# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
"""
Authors: Xavier Calle-Heredia & Xavier Hesselbach
Date: 2026-04-08

xrm.py — Extended Reality Manager
------------------------------------
Manages XR service lifecycle and routes XR service commands.

Modes
-----
core:
  Runs in pod-xrm. Manages XR service lifecycle only:
    - deploy / stop services on NDT pods (via RESTART_NDT to DTM)
    - maintain AgentRegistry of connected xrm-agents
    - does NOT participate in the command data path
  CLI via separate --mode cli process (kubectl exec, port 8103).

cli:
  Lightweight CLI connecting to a running core.
  Sends commands to core via UDP :8103, receives replies on :8102.
  No port binding conflicts with the running core.
  Usage: kubectl exec -it pod-xrm -n ndt -- python3 xrm.py --mode cli

agent:
  Runs inside each ndt-i pod alongside the XR services.
  Responsibilities:
    1. Lifecycle: register with XRM core, receive deploy/stop signals.
    2. Command routing: receive XR_SERVICE_CMD from ndt-event-cli via
       NodePort :8110+ndt_id, route to correct service (localhost),
       forward to DTM upstream for ON application.
    3. Context: receive ON_STATE_UPDATE broadcast from DTM upstream,
       store context, forward back to ndt-event-cli.
  Does NOT call kubectl — lifecycle is managed by XRM core + DTM.

1:1:
  Core + agent in one process. Runs in pod-dtm-ndt.
  Combines lifecycle and command routing for a single NDT.

Service lifecycle (XRM core only)
----------------------------------
  XRM core does NOT manage pod lifecycle directly.
  On deploy/stop:
    1. Updates /tmp/xrm-state/ndt-{id}/state.json with service list.
    2. Sends RESTART_NDT event to DTM relay UDP :7001.
    3. DTM writes restart_requested signal → watch_restarts.sh →
       restart_ndt.sh → pod recreated with updated service list.
  XRM has no dependency on kubectl.

Command data path (xrm-agent)
------------------------------
  ndt-event-cli → UDP NodePort :31100+ndt_id → pod :8110+ndt_id
      ↓
  xrm-agent: validates service, routes to service process (localhost)
      ↓
  actuator-control / node-scanner (localhost :9200/:9201)
      ↓
  xrm-agent forwards XR_SERVICE_CMD to DTM upstream UDP :7005
      ↓
  DTM upstream: AdmissionQueue (priority order, one-by-one)
      ↓
  ONM upstream: applies on ON (h1_1), sends ACK
      ↓
  DTM upstream: broadcast ON_STATE_UPDATE → xrm-agents UDP :8115+ndt_id
      ↓
  xrm-agent: stores context, forwards reply to ndt-event-cli

State persistence
-----------------
  /tmp/xrm-state/ndt-{id}/state.json  (hostPath on cluster node)
  {
    "ndt_id": N,
    "services": ["actuator-control"],
    "context": {<latest ON state>}
  }

Ports
-----
  8100  xrm-core → xrm-agent      (lifecycle: deploy/stop signals)
  8101  xrm-agent → xrm-core      (lifecycle: registration + results)
  8102  xrm-core → xrm-cli        (CLI replies)
  8103  xrm-cli → xrm-core        (CLI commands)
  8110+i  ndt-event-cli → xrm-agent  (XR service commands, pod internal)
  31100+i NodePort → xrm-agent :8110+i   (external access)
  8115+i  DTM upstream → xrm-agent   (ON_STATE_UPDATE broadcast)
  9200  xrm-agent → actuator-control  (localhost)
  9201  xrm-agent → node-scanner      (localhost)
  7001  xrm-core → DTM relay          (RESTART_NDT events)
  7005  xrm-agent → DTM upstream      (XR_SERVICE_CMD)
"""
import json
import os
import sys
import time
import uuid
import socket
import argparse
import threading
from typing import Dict, List, Optional

# ── Constants ────────────────────────────────────────────────
CONTROL_HOST      = "127.0.0.1"
CONTROL_PORT      = 6001

# XRM lifecycle ports
AGENT_RX_PORT     = 8100   # xrm-agent rx from core (lifecycle)
CORE_RX_PORT      = 8101   # xrm-core rx from agents
CLI_REPLY_PORT    = 8102   # xrm-core → xrm-cli replies
CLI_CMD_PORT      = 8103   # xrm-cli → xrm-core commands

# XR command data path ports
CMD_BASE_PORT     = 8110   # ndt-event-cli → xrm-agent (pod internal, +ndt_id)
BC_BASE_PORT      = 8115   # DTM upstream → xrm-agent broadcast (+ndt_id)
REPLY_BASE_PORT   = 9210   # ndt-event-cli reply port base (+ ndt_id)

# Service local ports (xrm-agent → service, localhost)
SERVICE_PORTS = {
    "actuator-control": 9200,
    "node-scanner":     9201,
}

# DTM upstream port
DTM_UPSTREAM_PORT = 7005   # xrm-agent → DTM upstream
DTM_RELAY_PORT    = 7001   # xrm-core → DTM relay (RESTART_NDT)

STATE_BASE_PATH   = "/tmp/xrm-state"
LOG_FILE_DEFAULT  = "/tmp/xrm.log"
NAMESPACE         = "ndt"

# Known service images
SERVICE_IMAGES = {
    "actuator-control": "actuator-control:1.0",
    "node-scanner":     "node-scanner:1.0",
}


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

_log_file: str = LOG_FILE_DEFAULT


def log_event(tag: str, data: dict) -> None:
    entry = json.dumps({"ts_ns": time.time_ns(), "tag": tag, **data})
    try:
        with open(_log_file, "a") as f:
            f.write(entry + "\n")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# State persistence
# ─────────────────────────────────────────────────────────────

def state_path(ndt_id: int) -> str:
    return os.path.join(STATE_BASE_PATH, f"ndt-{ndt_id}", "state.json")


def load_state(ndt_id: int) -> dict:
    try:
        with open(state_path(ndt_id)) as f:
            return json.load(f)
    except Exception:
        return {"ndt_id": ndt_id, "services": [], "context": {}}


def save_state(ndt_id: int, state: dict) -> None:
    path = state_path(ndt_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
    log_event("XRM_STATE_SAVED", {"ndt_id": ndt_id})


# ─────────────────────────────────────────────────────────────
# UDP helpers
# ─────────────────────────────────────────────────────────────

def udp_send(ip: str, port: int, msg: dict) -> None:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.sendto(json.dumps(msg).encode(), (ip, port))
        s.close()
    except Exception as exc:
        log_event("XRM_UDP_SEND_ERR",
                  {"dst": f"{ip}:{port}", "err": str(exc)})


def make_udp_rx(port: int, timeout: float = 1.0) -> socket.socket:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4 * 1024 * 1024)
    s.bind(("0.0.0.0", port))
    s.settimeout(timeout)
    return s


# ─────────────────────────────────────────────────────────────
# RESTART_NDT sender (XRM core → DTM relay)
# ─────────────────────────────────────────────────────────────

def send_restart_to_dtm(ndt_id: int, dtm_relay_ip: str,
                         dtm_relay_port: int) -> None:
    """
    Send RESTART_NDT event to DTM relay.
    DTM writes restart_requested signal → watch_restarts.sh → pod restart.
    XRM has no dependency on kubectl.
    """
    event_id = uuid.uuid4().hex[:12]
    ev = json.dumps({
        "type":     "RESTART_NDT",
        "event_id": event_id,
        "payload":  {"ndt_id": ndt_id},
    }).encode()
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.sendto(ev, (dtm_relay_ip, dtm_relay_port))
        s.close()
        log_event("XRM_RESTART_SENT", {
            "ndt_id":   ndt_id,
            "event_id": event_id,
            "dtm_ip":   dtm_relay_ip,
            "dtm_port": dtm_relay_port,
        })
        print(f"[XRM] RESTART_NDT sent to DTM for ndt-{ndt_id}",
              flush=True)
    except Exception as exc:
        log_event("XRM_RESTART_SEND_ERR",
                  {"ndt_id": ndt_id, "err": str(exc)})


# ─────────────────────────────────────────────────────────────
# Service lifecycle helpers (XRM core)
# ─────────────────────────────────────────────────────────────

def request_deploy_service(ndt_id: int, service_name: str,
                            dtm_relay_ip: str,
                            dtm_relay_port: int) -> dict:
    """
    Deploy a service: update xrm-state, send RESTART_NDT to DTM.
    """
    if service_name not in SERVICE_IMAGES:
        return {"ok": False,
                "err": f"Unknown service: {service_name}. "
                       f"Known: {list(SERVICE_IMAGES.keys())}"}
    state = load_state(ndt_id)
    if service_name in state.get("services", []):
        return {"ok": False,
                "err": f"Service {service_name} already deployed"}
    state.setdefault("services", []).append(service_name)
    save_state(ndt_id, state)
    send_restart_to_dtm(ndt_id, dtm_relay_ip, dtm_relay_port)
    log_event("XRM_DEPLOY_REQUESTED",
              {"ndt_id": ndt_id, "service": service_name})
    return {"ok": True}


def request_stop_service(ndt_id: int, service_name: str,
                          dtm_relay_ip: str,
                          dtm_relay_port: int) -> dict:
    """
    Stop a service: update xrm-state, send RESTART_NDT to DTM.
    """
    state = load_state(ndt_id)
    if service_name not in state.get("services", []):
        return {"ok": False,
                "err": f"Service {service_name} not deployed"}
    state["services"] = [s for s in state.get("services", [])
                         if s != service_name]
    save_state(ndt_id, state)
    send_restart_to_dtm(ndt_id, dtm_relay_ip, dtm_relay_port)
    log_event("XRM_STOP_REQUESTED",
              {"ndt_id": ndt_id, "service": service_name})
    return {"ok": True}


# ─────────────────────────────────────────────────────────────
# Agent registry (XRM core)
# ─────────────────────────────────────────────────────────────

class AgentRegistry:
    def __init__(self) -> None:
        self._lock   = threading.Lock()
        self._agents: Dict[str, dict] = {}

    def register(self, ndt_name: str, ip: str,
                 ndt_id: int, services: List[str]) -> None:
        with self._lock:
            self._agents[ndt_name] = {
                "ip":           ip,
                "ndt_id":       ndt_id,
                "services":     services,
                "last_seen_ns": time.time_ns(),
            }
        print(f"[XRM-CORE] registered: {ndt_name} "
              f"ip={ip} services={services}", flush=True)
        log_event("XRM_AGENT_REGISTERED", {
            "ndt_name": ndt_name, "ip": ip,
            "ndt_id": ndt_id, "services": services,
        })

    def update(self, ndt_name: str, services: List[str]) -> None:
        with self._lock:
            if ndt_name in self._agents:
                self._agents[ndt_name]["services"]     = services
                self._agents[ndt_name]["last_seen_ns"] = time.time_ns()

    def get_ndt_id(self, ndt_name: str) -> int:
        with self._lock:
            agent = self._agents.get(ndt_name)
            if agent:
                return int(agent["ndt_id"])
        try:
            return int(ndt_name.split("-")[1]) - 1
        except Exception:
            return -1

    def all(self) -> Dict[str, dict]:
        with self._lock:
            return dict(self._agents)

    def get(self, ndt_name: str) -> Optional[dict]:
        with self._lock:
            return self._agents.get(ndt_name)


# ─────────────────────────────────────────────────────────────
# Agent mode (new command data path)
# Receives XR_SERVICE_CMD from ndt-event-cli via NodePort.
# Routes to local service process (localhost).
# Forwards to DTM upstream for ON application.
# Receives ON_STATE_UPDATE broadcast, stores context,
# forwards reply to ndt-event-cli.
# ─────────────────────────────────────────────────────────────

def run_agent(args: argparse.Namespace) -> None:
    global _log_file
    _log_file = args.log_file

    ndt_id   = args.ndt_id
    pod_name = f"ndt-{ndt_id + 1}"

    cmd_port = CMD_BASE_PORT + ndt_id   # rx from ndt-event-cli
    bc_port  = BC_BASE_PORT  + ndt_id   # rx ON_STATE_UPDATE from DTM

    print(f"[XRM-AGENT ndt_id={ndt_id}] Starting", flush=True)
    print(f"[XRM-AGENT] cmd-rx :{cmd_port}  bc-rx :{bc_port}",
          flush=True)
    print(f"[XRM-AGENT] → DTM upstream {args.dtm_ip}:{DTM_UPSTREAM_PORT}",
          flush=True)

    # Command rx from ndt-event-cli
    cmd_sock = make_udp_rx(cmd_port, timeout=1.0)

    # Broadcast rx from DTM upstream
    bc_sock = make_udp_rx(bc_port, timeout=1.0)

    # Tx to DTM upstream
    dtm_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Tx to local service processes (localhost)
    svc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Tx replies to ndt-event-cli
    reply_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Pending: event_id → client addr
    _pending:      Dict[str, tuple] = {}
    _pending_lock  = threading.Lock()

    # Registration with XRM core
    state    = load_state(ndt_id)
    services = state.get("services", [])

    def _register() -> None:
        while True:
            udp_send(args.core_ip, CORE_RX_PORT, {
                "type":     "REGISTER",
                "ndt_name": pod_name,
                "ndt_id":   ndt_id,
                "services": services,
            })
            time.sleep(10)

    threading.Thread(target=_register, name="xrm-register",
                     daemon=True).start()

    def _bc_rx_loop() -> None:
        """
        Receive ON_STATE_UPDATE broadcast from DTM upstream.
        Store context locally and forward reply to ndt-event-cli.
        """
        while True:
            try:
                data, _ = bc_sock.recvfrom(65535)
            except socket.timeout:
                continue
            except Exception:
                continue

            t_rx_ns = time.time_ns()
            try:
                bc = json.loads(data)
            except Exception as exc:
                log_event("XRM_AGENT_BC_PARSE_ERR", {"err": str(exc)})
                continue

            event_id    = str(bc.get("processed_event_id", ""))
            your_status = bc.get("your_status", "PENDING")
            state_data  = bc.get("state", {})
            apply_ms    = bc.get("apply_ms", 0.0)
            t_create    = int(bc.get("t_create_ns") or 0)
            t_applied   = int(bc.get("t_applied_ns") or t_rx_ns)

            # Store ON context
            if state_data:
                ctx = load_state(ndt_id)
                ctx["context"] = {
                    "event_id":     event_id,
                    "your_status":  your_status,
                    "t_applied_ns": t_applied,
                    "t_rx_ns":      t_rx_ns,
                    **state_data,
                }
                save_state(ndt_id, ctx)

            e2e_ms = round((t_rx_ns - t_create) / 1e6, 3) \
                if t_create and your_status == "ACK" else None

            log_event("XRM_AGENT_BC_RX", {
                "event_id":    event_id,
                "your_status": your_status,
                "apply_ms":    apply_ms,
                "e2e_ms":      e2e_ms,
                "ndt_id":      ndt_id,
                "t_rx_ns":     t_rx_ns,
            })

            print(f"[XRM-AGENT ndt_id={ndt_id}] BC "
                  f"status={your_status} e2e_ms={e2e_ms}",
                  flush=True)

            # Forward reply to ndt-event-cli if this is our command
            with _pending_lock:
                client_addr = _pending.pop(event_id, None)
            if client_addr:
                reply = json.dumps({
                    "type":        "XR_SERVICE_REPLY",
                    "event_id":    event_id,
                    "ndt_id":      ndt_id,
                    "your_status": your_status,
                    "state":       state_data,
                    "apply_ms":    apply_ms,
                    "e2e_ms":      e2e_ms,
                    "t_create_ns": t_create,
                    "t_applied_ns": t_applied,
                    "t_rx_ns":     t_rx_ns,
                }).encode()
                try:
                    reply_sock.sendto(reply, client_addr)
                except Exception as exc:
                    log_event("XRM_AGENT_REPLY_ERR",
                              {"event_id": event_id, "err": str(exc)})

    threading.Thread(target=_bc_rx_loop,
                     name="xrm-agent-bc-rx", daemon=True).start()

    print(f"[XRM-AGENT ndt_id={ndt_id}] Ready", flush=True)

    # ── Main command receive loop ─────────────────────────────
    while True:
        try:
            data, addr = cmd_sock.recvfrom(65535)
        except socket.timeout:
            continue
        except Exception:
            continue

        t_rx_ns = time.time_ns()
        try:
            cmd = json.loads(data)
        except Exception as exc:
            log_event("XRM_AGENT_CMD_PARSE_ERR", {"err": str(exc)})
            continue

        ev_type  = cmd.get("type", "")
        event_id = str(cmd.get("event_id", ""))
        service  = cmd.get("service", "")

        log_event("XRM_AGENT_CMD_RX", {
            "event_id": event_id,
            "type":     ev_type,
            "service":  service,
            "ndt_id":   ndt_id,
            "from":     addr[0],
            "t_rx_ns":  t_rx_ns,
        })

        if ev_type == "XR_SERVICE_CMD":
            # Validate service
            if service not in SERVICE_PORTS:
                err_reply = json.dumps({
                    "type":        "XR_SERVICE_REPLY",
                    "event_id":    event_id,
                    "ndt_id":      ndt_id,
                    "your_status": "DISCARDED",
                    "err": f"Unknown service: {service}. "
                           f"Known: {list(SERVICE_PORTS.keys())}",
                }).encode()
                reply_sock.sendto(err_reply, addr)
                continue

            # Register pending for broadcast reply.
            # Use reply_ip + reply_port from cmd payload — the client
            # includes its own IP explicitly because in K8s UDP NAT
            # addr[0] seen by the pod is not the real client IP.
            client_ip   = cmd.get("reply_ip", addr[0])
            client_port = int(cmd.get("reply_port",
                                      REPLY_BASE_PORT + ndt_id))
            client_reply_addr = (client_ip, client_port)
            with _pending_lock:
                _pending[event_id] = client_reply_addr

            # Route to local service (localhost)
            svc_port = SERVICE_PORTS[service]
            svc_cmd  = json.dumps({
                **cmd,
                "ndt_id":       ndt_id,
                "agent_bc_port": bc_port,
                "t_create_ns":  cmd.get("t_create_ns", t_rx_ns),
            }).encode()
            try:
                svc_sock.sendto(svc_cmd,
                                (CONTROL_HOST, svc_port))
            except Exception as exc:
                log_event("XRM_AGENT_SVC_ROUTE_ERR",
                          {"event_id": event_id, "err": str(exc)})

            # Forward XR_SERVICE_CMD to DTM upstream
            upstream_cmd = json.dumps({
                "type":         "XR_SERVICE_CMD",
                "event_id":     event_id,
                "ndt_id":       ndt_id,
                "service":      service,
                "priority":     int(cmd.get("priority", 50)),
                "t_create_ns":  cmd.get("t_create_ns", t_rx_ns),
                "agent_bc_port": bc_port,
                "payload":      cmd.get("payload", {}),
            }).encode()
            try:
                dtm_sock.sendto(upstream_cmd,
                                (args.dtm_ip, DTM_UPSTREAM_PORT))
                log_event("XRM_AGENT_CMD_FORWARDED_DTM", {
                    "event_id": event_id,
                    "service":  service,
                    "ndt_id":   ndt_id,
                    "t_rx_ns":  t_rx_ns,
                })
            except Exception as exc:
                log_event("XRM_AGENT_DTM_FWD_ERR",
                          {"event_id": event_id, "err": str(exc)})
                with _pending_lock:
                    _pending.pop(event_id, None)
                reply_sock.sendto(json.dumps({
                    "type":        "XR_SERVICE_REPLY",
                    "event_id":    event_id,
                    "ndt_id":      ndt_id,
                    "your_status": "DISCARDED",
                    "err":         str(exc),
                }).encode(), addr)

        elif ev_type == "XR_SERVICE_GET":
            # Return current ON context
            ctx = load_state(ndt_id).get("context", {})
            reply_sock.sendto(json.dumps({
                "type":     "XR_SERVICE_STATE",
                "event_id": event_id,
                "ndt_id":   ndt_id,
                "context":  ctx,
            }).encode(), addr)

        else:
            log_event("XRM_AGENT_UNKNOWN_CMD",
                      {"event_id": event_id, "type": ev_type})


# ─────────────────────────────────────────────────────────────
# Core mode — lifecycle management only
# ─────────────────────────────────────────────────────────────

def run_core(args: argparse.Namespace) -> None:
    global _log_file
    _log_file = args.log_file

    registry     = AgentRegistry()
    rx_sock      = make_udp_rx(CORE_RX_PORT)
    _pending:    Dict[str, dict] = {}
    _pending_lock = threading.Lock()

    def _rx_loop() -> None:
        while True:
            try:
                data, addr = rx_sock.recvfrom(65535)
            except socket.timeout:
                continue
            except Exception:
                continue
            try:
                msg = json.loads(data)
            except Exception:
                continue

            msg_type = msg.get("type", "")
            if msg_type == "REGISTER":
                registry.register(
                    ndt_name=msg.get("ndt_name", ""),
                    ip=addr[0],
                    ndt_id=int(msg.get("ndt_id", -1)),
                    services=msg.get("services", []),
                )
            elif msg_type == "CMD_RESULT":
                req_id = msg.get("req_id", "")
                registry.update(msg.get("ndt_name", ""),
                                msg.get("services", []))
                with _pending_lock:
                    if req_id in _pending:
                        _pending[req_id]["result"] = msg
                        _pending[req_id]["event"].set()

    threading.Thread(target=_rx_loop, name="xrm-core-rx",
                     daemon=True).start()

    # CLI command listener (from --mode cli process)
    cli_sock = make_udp_rx(CLI_CMD_PORT, timeout=0.5)

    def _cli_cmd_loop() -> None:
        while True:
            try:
                data, addr = cli_sock.recvfrom(65535)
            except socket.timeout:
                continue
            except Exception:
                continue
            try:
                cmd = json.loads(data)
            except Exception:
                continue

            cmd_type   = cmd.get("type", "")
            reply_ip   = cmd.get("_reply_ip", addr[0])
            reply_port = int(cmd.get("_reply_port", CLI_REPLY_PORT))

            if cmd_type == "CLI_LIST":
                result = {"ok": True, "agents": registry.all()}

            elif cmd_type == "CLI_DEPLOY":
                ndt_name = cmd.get("ndt_name", "")
                service  = cmd.get("service", "")
                ndt_id   = registry.get_ndt_id(ndt_name)
                if ndt_id < 0:
                    result = {"ok": False,
                              "err": f"could not resolve {ndt_name}"}
                else:
                    result = request_deploy_service(
                        ndt_id, service,
                        args.dtm_relay_ip, args.dtm_relay_port,
                    )

            elif cmd_type == "CLI_STOP":
                ndt_name = cmd.get("ndt_name", "")
                service  = cmd.get("service", "")
                ndt_id   = registry.get_ndt_id(ndt_name)
                if ndt_id < 0:
                    result = {"ok": False,
                              "err": f"could not resolve {ndt_name}"}
                else:
                    result = request_stop_service(
                        ndt_id, service,
                        args.dtm_relay_ip, args.dtm_relay_port,
                    )

            elif cmd_type == "CLI_RESTART":
                ndt_name = cmd.get("ndt_name", "")
                ndt_id   = registry.get_ndt_id(ndt_name)
                if ndt_id < 0:
                    result = {"ok": False,
                              "err": f"could not resolve {ndt_name}"}
                else:
                    send_restart_to_dtm(ndt_id, args.dtm_relay_ip,
                                        args.dtm_relay_port)
                    result = {"ok": True}

            elif cmd_type == "CLI_STATE":
                ndt_name = cmd.get("ndt_name", "")
                agent    = registry.get(ndt_name)
                if not agent:
                    result = {"ok": False,
                              "err": f"{ndt_name} not registered"}
                else:
                    ndt_id = agent["ndt_id"]
                    ctx    = load_state(ndt_id).get("context", {})
                    result = {"ok": True,
                              "services": agent["services"],
                              "context":  ctx}

            else:
                result = {"ok": False,
                          "err": f"unknown cmd: {cmd_type}"}

            try:
                out_sock = socket.socket(socket.AF_INET,
                                         socket.SOCK_DGRAM)
                out_sock.sendto(json.dumps(result).encode(),
                                (reply_ip, reply_port))
                out_sock.close()
            except Exception:
                pass

    threading.Thread(target=_cli_cmd_loop, name="xrm-cli-rx",
                     daemon=True).start()

    print(f"[XRM-CORE] Ready — rx UDP :{CORE_RX_PORT}", flush=True)
    print(f"[XRM-CORE] CLI rx :{CLI_CMD_PORT}", flush=True)
    print("[XRM-CORE] Lifecycle only — command routing via xrm-agents",
          flush=True)

    while True:
        time.sleep(60)


# ─────────────────────────────────────────────────────────────
# CLI mode — connects to running core
# ─────────────────────────────────────────────────────────────

def run_cli(args: argparse.Namespace) -> None:
    global _log_file
    _log_file = args.log_file

    reply_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    reply_sock.bind(("0.0.0.0", CLI_REPLY_PORT))
    reply_sock.settimeout(10.0)
    core_ip = args.core_ip

    def send_and_wait(cmd: dict) -> dict:
        cmd["_reply_ip"]   = "127.0.0.1"
        cmd["_reply_port"] = CLI_REPLY_PORT
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(json.dumps(cmd).encode(), (core_ip, CLI_CMD_PORT))
            s.close()
            data, _ = reply_sock.recvfrom(65535)
            return json.loads(data)
        except socket.timeout:
            return {"ok": False, "err": "timeout — is core running?"}
        except Exception as exc:
            return {"ok": False, "err": str(exc)}

    print(f"[XRM-CLI] Connected to core at {core_ip}:{CLI_CMD_PORT}",
          flush=True)
    _print_help_core()

    while True:
        try:
            line = input("[XRM-CLI] > ").strip()
        except EOFError:
            break
        if not line:
            continue
        parts = line.split()
        cmd   = parts[0].lower()

        if cmd == "quit":
            break

        elif cmd == "list":
            result = send_and_wait({"type": "CLI_LIST"})
            if result.get("ok"):
                agents = result.get("agents", {})
                if not agents:
                    print("  No NDTs registered yet.")
                for name, info in sorted(agents.items()):
                    print(f"  {name}  ip={info.get('ip')}  "
                          f"services={info.get('services')}")
            else:
                print(f"  ✗ {result.get('err')}")

        elif cmd == "services":
            for name, image in SERVICE_IMAGES.items():
                print(f"  {name}: {image}")

        elif cmd == "state" and len(parts) == 2:
            result = send_and_wait({
                "type":     "CLI_STATE",
                "ndt_name": parts[1],
            })
            if result.get("ok"):
                print(f"  Services: {result.get('services', [])}")
                ctx = result.get("context", {})
                if ctx:
                    print("  ON context:")
                    for k, v in ctx.items():
                        print(f"    {k}: {v}")
                else:
                    print("  (no ON context yet)")
            else:
                print(f"  ✗ {result.get('err')}")

        elif cmd == "restart" and len(parts) == 2:
            result = send_and_wait({
                "type":     "CLI_RESTART",
                "ndt_name": parts[1],
            })
            if result.get("ok"):
                print(f"  ✓ RESTART_NDT sent to DTM for {parts[1]}")
            else:
                print(f"  ✗ {result.get('err')}")

        elif cmd == "deploy" and len(parts) >= 3:
            service   = parts[1]
            ndt_names = parts[2:]
            for ndt_name in ndt_names:
                result = send_and_wait({
                    "type":     "CLI_DEPLOY",
                    "ndt_name": ndt_name,
                    "service":  service,
                })
                if result.get("ok"):
                    print(f"  ✓ {ndt_name}: {service} deploy requested")
                else:
                    print(f"  ✗ {ndt_name}: {result.get('err')}")

        elif cmd == "stop" and len(parts) >= 3:
            service   = parts[1]
            ndt_names = parts[2:]
            for ndt_name in ndt_names:
                result = send_and_wait({
                    "type":     "CLI_STOP",
                    "ndt_name": ndt_name,
                    "service":  service,
                })
                if result.get("ok"):
                    print(f"  ✓ {ndt_name}: {service} stop requested")
                else:
                    print(f"  ✗ {ndt_name}: {result.get('err')}")

        else:
            _print_help_core()

    reply_sock.close()


# ─────────────────────────────────────────────────────────────
# 1:1 mode — core + agent combined
# ─────────────────────────────────────────────────────────────

def run_1to1(args: argparse.Namespace) -> None:
    global _log_file
    _log_file = args.log_file

    ndt_id = 0
    print(f"[XRM 1:1] Starting — ndt_id={ndt_id}", flush=True)

    state    = load_state(ndt_id)
    services = state.get("services", [])
    print(f"[XRM 1:1] Active services: {services}", flush=True)

    # Run agent loop in this thread
    run_agent(args)


# ─────────────────────────────────────────────────────────────
# CLI help
# ─────────────────────────────────────────────────────────────

def _print_help_core() -> None:
    print("Commands:")
    print("  list                        — show all NDTs and services")
    print("  deploy SERVICE NDT [NDT...] — deploy service (lifecycle)")
    print("  stop   SERVICE NDT [NDT...] — stop service (lifecycle)")
    print("  restart NDT                 — request pod restart via DTM")
    print("  state  NDT                  — show ON context of NDT")
    print("  services                    — list known service images")
    print("  quit")
    print(f"Known services: {list(SERVICE_IMAGES.keys())}\n")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="XRM — Extended Reality Manager"
    )
    ap.add_argument("--mode",
                    choices=["1:1", "agent", "core", "cli"],
                    required=True)
    ap.add_argument("--ndt-id",          type=int, default=0,
                    help="[agent/1:1] NDT identifier (0-based)")
    ap.add_argument("--core-ip",         default="127.0.0.1",
                    help="[agent/cli] XRM core IP")
    ap.add_argument("--dtm-ip",          default="127.0.0.1",
                    help="[agent] DTM upstream IP for XR_SERVICE_CMD")
    ap.add_argument("--dtm-relay-ip",    default="127.0.0.1",
                    help="[core/cli] DTM relay IP for RESTART_NDT events")
    ap.add_argument("--dtm-relay-port",  type=int, default=DTM_RELAY_PORT,
                    help="[core/cli] DTM relay port")
    ap.add_argument("--log-file",        default=LOG_FILE_DEFAULT)
    args = ap.parse_args()

    if args.mode == "1:1":
        run_1to1(args)
    elif args.mode == "agent":
        run_agent(args)
    elif args.mode == "core":
        run_core(args)
    elif args.mode == "cli":
        run_cli(args)


if __name__ == "__main__":
    main()