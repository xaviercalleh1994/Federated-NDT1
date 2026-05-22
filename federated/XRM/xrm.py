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
Manages XR services deployed on NDT pods.

Modes
-----
1:1:
  Runs inside pod-dtm-ndt alongside dtm (container name: xrm).
  Acts as both core and agent — no network registration needed.
  On startup: reads /tmp/xrm-state/ndt-0/state.json and
  re-applies any saved network state via on_ndt.py:6001.
  Accepts CLI commands directly (stdin) and via --script file.

agent:
  Runs inside each ndt-i pod (container name: xrm-agent).
  On startup: reads /tmp/xrm-state/ndt-{id}/state.json,
  re-applies saved network state via on_ndt.py:6001,
  then registers with XRM core via UDP.
  Listens for commands from XRM core on UDP :8100.
  Reports state back to XRM core.

core:
  Runs in pod-xrm (container name: xrm).
  Maintains registry of all connected xrm-agents.
  Accepts CLI commands (stdin) and --script file.
  Dispatches service commands to agents.

Service lifecycle (updated)
----------------------------
  The XRM does NOT manage pod lifecycle directly — that is the
  DTM's responsibility. When deploying or stopping a service:
    1. XRM core updates xrm-state/ndt-{id}/state.json with the
       new service list.
    2. XRM core sends a RESTART_NDT event to the DTM relay so
       the DTM can restart the pod with the updated service list.
    3. On pod restart, dtm-agent restores network state from
       dtm-state and xrm-agent restores XR service state from
       xrm-state — including launching any services listed there.
  The XRM has no dependency on kubectl.

State persistence (for re-launching an NDT with a new XR service)
-----------------
  /tmp/xrm-state/ndt-{id}/state.json  (hostPath on cluster node)
  {
    "ndt_id": N,
    "links": [
      {"a": "s1", "b": "s2", "delay_ms": 10,
       "loss_pct": 0, "bw_mbps": 100, "up": true}
    ],
    "services": ["actuator-control"]
  }

CLI commands (core and 1:1 modes)
----------------------------------
  list
      Show all registered NDTs and their active services.

  deploy SERVICE NDT [NDT ...]
      Deploy a service on one or more NDTs.
      Example: deploy actuator-control ndt-1 ndt-3
               deploy node-scanner ndt-1

  stop SERVICE NDT [NDT ...]
      Stop a service on one or more NDTs.
      Example: stop actuator-control ndt-1

  restart NDT
      Request DTM to restart an NDT pod (preserving network state).
      Example: restart ndt-1

  state NDT
      Show current network state of an NDT.
      Example: state ndt-2

  services
      List known service images.

  quit
      Exit.

Ports
-----
  8100  xrm-agent rx      (XRM core → agent)
  8101  XRM core rx       (agents → core, registration + state)
"""
import json
import os
import sys
import time
import uuid
import socket
import argparse
import threading
from collections import defaultdict
from typing import Dict, List, Optional

# ── Constants ────────────────────────────────────────────────
CONTROL_HOST     = "127.0.0.1"
CONTROL_PORT     = 6001
AGENT_RX_PORT    = 8100   # xrm-agent listens for commands from core
CORE_RX_PORT     = 8101   # xrm-core listens for registration + state
STATE_BASE_PATH  = "/tmp/xrm-state"
LOG_FILE_DEFAULT = "/tmp/xrm.log"
NAMESPACE        = "ndt"
# CLI mode reply port — used by cli mode to receive responses from core
CLI_REPLY_PORT   = 8102
# CLI mode command port — core listens for cli commands here
CLI_CMD_PORT     = 8103

# Known service images — add new services here
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
# on_ndt.py primitive caller (localhost:6001)
# ─────────────────────────────────────────────────────────────

def control_call(ev: dict) -> dict:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.settimeout(5.0)
        s.connect((CONTROL_HOST, CONTROL_PORT))
        s.sendall((json.dumps(ev) + "\n").encode())
        data = b""
        while b"\n" not in data:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
        s.close()
        return json.loads(data.split(b"\n")[0].decode()) if data \
            else {"ok": False, "err": "no response"}
    except Exception as e:
        return {"ok": False, "err": str(e)}


# ─────────────────────────────────────────────────────────────
# State persistence
# ─────────────────────────────────────────────────────────────

def state_path(ndt_id: int) -> str:
    return os.path.join(STATE_BASE_PATH, f"ndt-{ndt_id}", "state.json")


def load_state(ndt_id: int) -> dict:
    path = state_path(ndt_id)
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {"ndt_id": ndt_id, "links": [], "services": []}


def save_state(ndt_id: int, state: dict) -> None:
    path = state_path(ndt_id)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(state, f, indent=2)
    log_event("XRM_STATE_SAVED", {"ndt_id": ndt_id, "path": path})


# ─────────────────────────────────────────────────────────────
# Network state restore
# Reads saved links and re-applies them via on_ndt.py:6001.
# Called on agent/1:1 startup after Mininet is ready.
# ─────────────────────────────────────────────────────────────

def restore_network_state(ndt_id: int) -> None:
    state = load_state(ndt_id)
    links = state.get("links", [])
    if not links:
        print(f"[XRM] ndt-{ndt_id}: no saved network state", flush=True)
        return

    print(f"[XRM] ndt-{ndt_id}: restoring {len(links)} links...",
          flush=True)
    for link in links:
        resp = control_call({
            "type": "LINK_SET",
            "payload": {
                "a":        link["a"],
                "b":        link["b"],
                "delay_ms": link.get("delay_ms", 0.0),
                "loss_pct": link.get("loss_pct", 0.0),
                "bw_mbps":  link.get("bw_mbps", 1000.0),
                "up":       link.get("up", True),
            },
        })
        log_event("XRM_LINK_RESTORED", {
            "ndt_id": ndt_id,
            "link":   link,
            "ok":     resp.get("ok"),
        })
        print(f"[XRM]   {link['a']}↔{link['b']} ok={resp.get('ok')}",
              flush=True)


# ─────────────────────────────────────────────────────────────
# Network state snapshot
# Queries on_ndt.py for current link state of all known links.
# Returns list of link dicts.
# ─────────────────────────────────────────────────────────────

def snapshot_network_state(ndt_id: int) -> List[dict]:
    """
    Read current link state from the Mininet topology.
    Uses SNAPSHOT primitive if available, otherwise reads
    from saved state (conservative approach).
    """
    resp = control_call({"type": "SNAPSHOT"})
    if resp.get("ok") and resp.get("links"):
        links = resp["links"]
        log_event("XRM_SNAPSHOT_OK", {"ndt_id": ndt_id,
                                       "n_links": len(links)})
        return links

    # Fallback: return saved links
    state = load_state(ndt_id)
    return state.get("links", [])


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
# DTM relay sender (core side, new)
# Sends RESTART_NDT event to the DTM relay so the DTM can
# restart the pod. The XRM has no kubectl dependency — pod
# lifecycle is entirely the DTM's responsibility.
# ─────────────────────────────────────────────────────────────

def send_restart_to_dtm(ndt_id: int, dtm_relay_ip: str,
                         dtm_relay_port: int) -> None:
    """
    Send RESTART_NDT event to DTM relay.
    DTM will save network state and call restart_ndt.sh.
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
# Service management (core side, updated)
# The XRM updates xrm-state and sends RESTART_NDT to DTM.
# The DTM restarts the pod — the XRM has no kubectl dependency.
# ─────────────────────────────────────────────────────────────

def request_deploy_service(ndt_id: int, service_name: str,
                            dtm_relay_ip: str,
                            dtm_relay_port: int) -> dict:
    """
    Request deploy of a service on an NDT pod.
    Steps:
      1. Validate service name.
      2. Update xrm-state with new service.
      3. Send RESTART_NDT to DTM — DTM restarts the pod.
      4. On restart xrm-agent reads state.json and launches services.
    Returns {ok, err}.
    """
    if service_name not in SERVICE_IMAGES:
        return {"ok": False,
                "err": f"Unknown service: {service_name}. "
                       f"Known: {list(SERVICE_IMAGES.keys())}"}

    # Update xrm state with new service
    state = load_state(ndt_id)
    if service_name in state.get("services", []):
        return {"ok": False,
                "err": f"Service {service_name} already in state"}
    state.setdefault("services", []).append(service_name)
    save_state(ndt_id, state)

    # Request DTM to restart the pod
    send_restart_to_dtm(ndt_id, dtm_relay_ip, dtm_relay_port)

    log_event("XRM_DEPLOY_REQUESTED", {
        "ndt_id":  ndt_id,
        "service": service_name,
    })
    return {"ok": True}


def request_stop_service(ndt_id: int, service_name: str,
                          dtm_relay_ip: str,
                          dtm_relay_port: int) -> dict:
    """
    Request stop of a service on an NDT pod.
    Steps:
      1. Remove service from xrm-state.
      2. Send RESTART_NDT to DTM — DTM restarts the pod.
      3. On restart xrm-agent reads state.json (service not listed).
    Returns {ok, err}.
    """
    state = load_state(ndt_id)
    if service_name not in state.get("services", []):
        return {"ok": False,
                "err": f"Service {service_name} not in state"}

    state["services"] = [s for s in state.get("services", [])
                         if s != service_name]
    save_state(ndt_id, state)

    # Request DTM to restart the pod
    send_restart_to_dtm(ndt_id, dtm_relay_ip, dtm_relay_port)

    log_event("XRM_STOP_REQUESTED", {
        "ndt_id":  ndt_id,
        "service": service_name,
    })
    return {"ok": True}


# ─────────────────────────────────────────────────────────────
# Agent registry (core side)
# ─────────────────────────────────────────────────────────────

class AgentRegistry:
    """
    Tracks all registered xrm-agents.
    ndt_name → {ip, port, ndt_id, services, last_seen_ns}
    """
    def __init__(self) -> None:
        self._lock   = threading.Lock()
        self._agents: Dict[str, dict] = {}

    def register(self, ndt_name: str, ip: str,
                 ndt_id: int, services: List[str]) -> None:
        with self._lock:
            self._agents[ndt_name] = {
                "ip":           ip,
                "port":         AGENT_RX_PORT,
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
        """Resolve ndt_name (e.g. 'ndt-1') to 0-based ndt_id."""
        with self._lock:
            agent = self._agents.get(ndt_name)
            if agent:
                return int(agent["ndt_id"])
        # Fallback: parse from name (ndt-1 → 0, ndt-2 → 1 ...)
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
# 1:1 mode
# Core + agent in one. Runs in pod-dtm-ndt.
# ─────────────────────────────────────────────────────────────

def run_1to1(args: argparse.Namespace) -> None:
    global _log_file
    _log_file = args.log_file

    ndt_id = 0

    print(f"[XRM 1:1] Starting — ndt_id={ndt_id}", flush=True)

    # Wait for on_ndt.py to be ready
    _wait_for_on_ndt()

    # Restore saved network state
    restore_network_state(ndt_id)

    # Load current service list
    state    = load_state(ndt_id)
    services = state.get("services", [])
    print(f"[XRM 1:1] Active services: {services}", flush=True)

    if args.script:
        _run_script_1to1(args.script, ndt_id, args)
    else:
        _run_cli_1to1(ndt_id, args)


# ─────────────────────────────────────────────────────────────
# Agent mode
# Runs in each ndt-i pod. Registers with XRM core.
# ─────────────────────────────────────────────────────────────

def run_agent(args: argparse.Namespace) -> None:
    global _log_file
    _log_file = args.log_file

    ndt_id   = args.ndt_id
    pod_name = f"ndt-{ndt_id + 1}"   # ndt_id is 0-based, pod name is 1-based

    print(f"[XRM-AGENT ndt_id={ndt_id}] Starting", flush=True)

    # Wait for on_ndt.py to be ready
    _wait_for_on_ndt()

    # Restore saved network state
    restore_network_state(ndt_id)

    # Register with XRM core
    state    = load_state(ndt_id)
    services = state.get("services", [])

    rx_sock = make_udp_rx(AGENT_RX_PORT)

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

    print(f"[XRM-AGENT ndt_id={ndt_id}] Ready — "
          f"rx UDP :{AGENT_RX_PORT}", flush=True)

    # Command receive loop
    # Agent only handles STATE queries — deploy/stop are managed
    # by the core via DTM restart, not by the agent directly.
    while True:
        try:
            data, _ = rx_sock.recvfrom(65535)
        except socket.timeout:
            continue
        except Exception:
            continue

        try:
            cmd = json.loads(data)
        except Exception:
            continue

        cmd_type = cmd.get("type", "")
        req_id   = cmd.get("req_id", "")

        log_event("XRM_AGENT_CMD_RX", {
            "ndt_id":   ndt_id,
            "cmd_type": cmd_type,
            "req_id":   req_id,
        })

        if cmd_type == "STATE":
            links  = snapshot_network_state(ndt_id)
            state  = load_state(ndt_id)
            result = {"ok": True, "links": links,
                      "services": state.get("services", [])}
        else:
            result = {"ok": False, "err": f"Unknown cmd: {cmd_type}"}

        # Send result back to core
        udp_send(args.core_ip, CORE_RX_PORT, {
            "type":     "CMD_RESULT",
            "req_id":   req_id,
            "ndt_name": pod_name,
            "ndt_id":   ndt_id,
            **result,
        })

        print(f"[XRM-AGENT ndt_id={ndt_id}] "
              f"{cmd_type} ok={result.get('ok')}",
              flush=True)


# ─────────────────────────────────────────────────────────────
# Core mode
# Runs in pod-xrm. CLI + script. Manages all agents.
# ─────────────────────────────────────────────────────────────

def run_core(args: argparse.Namespace) -> None:
    global _log_file
    _log_file = args.log_file

    registry = AgentRegistry()
    rx_sock  = make_udp_rx(CORE_RX_PORT)

    # Pending command results: req_id → threading.Event + result
    _pending: Dict[str, dict] = {}
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
                    ndt_name = msg.get("ndt_name", ""),
                    ip       = addr[0],
                    ndt_id   = int(msg.get("ndt_id", -1)),
                    services = msg.get("services", []),
                )

            elif msg_type == "CMD_RESULT":
                req_id = msg.get("req_id", "")
                registry.update(
                    msg.get("ndt_name", ""),
                    msg.get("services", []),
                )
                with _pending_lock:
                    if req_id in _pending:
                        _pending[req_id]["result"] = msg
                        _pending[req_id]["event"].set()

    threading.Thread(target=_rx_loop, name="xrm-core-rx",
                     daemon=True).start()

    # CLI command listener — receives commands from cli mode instances
    # running in the same pod via kubectl exec. Uses a separate port
    # so cli mode does not conflict with the agent registration port.
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

            cmd_type  = cmd.get("type", "")
            reply_ip  = cmd.get("_reply_ip", addr[0])
            reply_port = int(cmd.get("_reply_port", CLI_REPLY_PORT))

            if cmd_type == "CLI_LIST":
                agents = registry.all()
                result = {"ok": True, "agents": agents}

            elif cmd_type == "CLI_STATE":
                ndt_name = cmd.get("ndt_name", "")
                result   = _send_cmd(ndt_name, "STATE", timeout=15.0)

            elif cmd_type == "CLI_RESTART":
                ndt_name = cmd.get("ndt_name", "")
                ndt_id   = registry.get_ndt_id(ndt_name)
                if ndt_id < 0:
                    result = {"ok": False,
                              "err": f"could not resolve {ndt_name}"}
                else:
                    send_restart_to_dtm(
                        ndt_id, args.dtm_relay_ip, args.dtm_relay_port)
                    result = {"ok": True}

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

            else:
                result = {"ok": False, "err": f"unknown cmd: {cmd_type}"}

            try:
                reply_sock_out = socket.socket(
                    socket.AF_INET, socket.SOCK_DGRAM)
                reply_sock_out.sendto(
                    json.dumps(result).encode(), (reply_ip, reply_port))
                reply_sock_out.close()
            except Exception:
                pass

    threading.Thread(target=_cli_cmd_loop, name="xrm-cli-rx",
                     daemon=True).start()

    print(f"[XRM-CORE] Ready — rx UDP :{CORE_RX_PORT}", flush=True)

    def _send_cmd(ndt_name: str, cmd_type: str,
                  timeout: float = 15.0) -> dict:
        """Send command to agent and wait for result."""
        agent = registry.get(ndt_name)
        if not agent:
            return {"ok": False,
                    "err": f"{ndt_name} not registered"}

        req_id = uuid.uuid4().hex[:12]
        ev     = threading.Event()
        with _pending_lock:
            _pending[req_id] = {"event": ev, "result": None}

        udp_send(agent["ip"], AGENT_RX_PORT, {
            "type":   cmd_type,
            "req_id": req_id,
        })

        ev.wait(timeout=timeout)
        with _pending_lock:
            entry = _pending.pop(req_id, {})

        result = entry.get("result")
        if result is None:
            return {"ok": False, "err": "timeout"}
        return result

    if args.script:
        _run_script_core(args.script, registry, _send_cmd, args)
    else:
        _run_cli_core(registry, _send_cmd, args)


# ─────────────────────────────────────────────────────────────
# CLI helpers
# ─────────────────────────────────────────────────────────────

def _wait_for_on_ndt(retries: int = 60, interval: float = 2.0) -> None:
    """Wait until on_ndt.py is accepting connections on :6001."""
    for _ in range(retries):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            s.connect((CONTROL_HOST, CONTROL_PORT))
            s.close()
            print("[XRM] on_ndt.py ready", flush=True)
            return
        except Exception:
            time.sleep(interval)
    print("[XRM] WARNING: on_ndt.py not ready after timeout",
          flush=True)


def _print_help_1to1() -> None:
    print("Commands:")
    print("  deploy SERVICE     — request deploy of service on this NDT")
    print("  stop   SERVICE     — request stop of service on this NDT")
    print("  state              — show network state")
    print("  services           — list known service images")
    print("  quit")
    print(f"Known services: {list(SERVICE_IMAGES.keys())}\n")


def _print_help_core() -> None:
    print("Commands:")
    print("  list                        — show all NDTs and services")
    print("  deploy SERVICE NDT [NDT...] — request deploy on NDTs via DTM")
    print("  stop   SERVICE NDT [NDT...] — request stop on NDTs via DTM")
    print("  restart NDT                 — request DTM to restart NDT")
    print("  state  NDT                  — show network state of NDT")
    print("  services                    — list known service images")
    print("  quit")
    print(f"Known services: {list(SERVICE_IMAGES.keys())}\n")


def _run_cli_1to1(ndt_id: int, args: argparse.Namespace) -> None:
    """Interactive CLI for 1:1 mode."""
    _print_help_1to1()
    while True:
        try:
            line = input("[XRM 1:1] > ").strip()
        except EOFError:
            break
        if not line:
            continue
        parts = line.split()
        cmd   = parts[0].lower()

        if cmd == "quit":
            break
        elif cmd == "services":
            for name, image in SERVICE_IMAGES.items():
                print(f"  {name}: {image}")
        elif cmd == "state":
            links = snapshot_network_state(ndt_id)
            state = load_state(ndt_id)
            print(f"  Links ({len(links)}):")
            for lk in links:
                print(f"    {lk['a']}↔{lk['b']} "
                      f"up={lk.get('up')} "
                      f"delay={lk.get('delay_ms')}ms "
                      f"bw={lk.get('bw_mbps')}Mbps")
            print(f"  Services: {state.get('services', [])}")
        elif cmd == "deploy" and len(parts) == 2:
            service = parts[1]
            print(f"  Requesting deploy of {service}...")
            result = request_deploy_service(
                ndt_id, service,
                args.dtm_relay_ip, args.dtm_relay_port,
            )
            if result["ok"]:
                print(f"  ✓ {service}: restart requested to DTM.")
            else:
                print(f"  ✗ {result.get('err')}")
        elif cmd == "stop" and len(parts) == 2:
            service = parts[1]
            print(f"  Requesting stop of {service}...")
            result = request_stop_service(
                ndt_id, service,
                args.dtm_relay_ip, args.dtm_relay_port,
            )
            if result["ok"]:
                print(f"  ✓ {service}: restart requested to DTM.")
            else:
                print(f"  ✗ {result.get('err')}")
        else:
            _print_help_1to1()


def _run_cli_core(registry: AgentRegistry, send_cmd,
                  args: argparse.Namespace) -> None:
    """Interactive CLI for core mode."""
    _print_help_core()
    while True:
        try:
            line = input("[XRM-CORE] > ").strip()
        except EOFError:
            break
        if not line:
            continue
        parts = line.split()
        cmd   = parts[0].lower()

        if cmd == "quit":
            break

        elif cmd == "list":
            agents = registry.all()
            if not agents:
                print("  No NDTs registered yet.")
            for name, info in sorted(agents.items()):
                print(f"  {name}  ip={info['ip']}  "
                      f"services={info['services']}")

        elif cmd == "services":
            for name, image in SERVICE_IMAGES.items():
                print(f"  {name}: {image}")

        elif cmd == "state" and len(parts) == 2:
            ndt_name = parts[1]
            print(f"  Querying {ndt_name}...")
            result = send_cmd(ndt_name, "STATE", timeout=15.0)
            if result.get("ok"):
                links = result.get("links", [])
                print(f"  Links ({len(links)}):")
                for lk in links:
                    print(f"    {lk['a']}↔{lk['b']} "
                          f"up={lk.get('up')} "
                          f"delay={lk.get('delay_ms')}ms "
                          f"bw={lk.get('bw_mbps')}Mbps")
                print(f"  Services: {result.get('services', [])}")
            else:
                print(f"  ✗ {result.get('err')}")

        elif cmd == "restart" and len(parts) == 2:
            # Send RESTART_NDT directly to DTM
            ndt_name = parts[1]
            ndt_id   = registry.get_ndt_id(ndt_name)
            if ndt_id < 0:
                print(f"  ✗ Could not resolve ndt_id for {ndt_name}")
            else:
                send_restart_to_dtm(
                    ndt_id, args.dtm_relay_ip, args.dtm_relay_port)
                print(f"  ✓ RESTART_NDT sent to DTM for {ndt_name}")

        elif cmd == "deploy" and len(parts) >= 3:
            service   = parts[1]
            ndt_names = parts[2:]
            for ndt_name in ndt_names:
                ndt_id = registry.get_ndt_id(ndt_name)
                if ndt_id < 0:
                    print(f"  ✗ {ndt_name}: could not resolve ndt_id")
                    continue
                print(f"  Requesting deploy of {service} on {ndt_name}...")
                result = request_deploy_service(
                    ndt_id, service,
                    args.dtm_relay_ip, args.dtm_relay_port,
                )
                if result.get("ok"):
                    print(f"  ✓ {ndt_name}: restart requested to DTM.")
                else:
                    print(f"  ✗ {ndt_name}: {result.get('err')}")

        elif cmd == "stop" and len(parts) >= 3:
            service   = parts[1]
            ndt_names = parts[2:]
            for ndt_name in ndt_names:
                ndt_id = registry.get_ndt_id(ndt_name)
                if ndt_id < 0:
                    print(f"  ✗ {ndt_name}: could not resolve ndt_id")
                    continue
                print(f"  Requesting stop of {service} on {ndt_name}...")
                result = request_stop_service(
                    ndt_id, service,
                    args.dtm_relay_ip, args.dtm_relay_port,
                )
                if result.get("ok"):
                    print(f"  ✓ {ndt_name}: restart requested to DTM.")
                else:
                    print(f"  ✗ {ndt_name}: {result.get('err')}")

        else:
            _print_help_core()


def _run_script_1to1(script_path: str, ndt_id: int,
                     args: argparse.Namespace) -> None:
    """Execute commands from script file (1:1 mode)."""
    with open(script_path) as f:
        lines = [l.strip() for l in f
                 if l.strip() and not l.startswith("#")]
    for line in lines:
        print(f"[XRM 1:1] > {line}", flush=True)
        parts = line.split()
        cmd   = parts[0].lower()
        if cmd == "deploy" and len(parts) == 2:
            result = request_deploy_service(
                ndt_id, parts[1],
                args.dtm_relay_ip, args.dtm_relay_port,
            )
            print(f"  {'✓' if result['ok'] else '✗'} "
                  f"{result.get('err', 'restart requested')}",
                  flush=True)
        elif cmd == "stop" and len(parts) == 2:
            result = request_stop_service(
                ndt_id, parts[1],
                args.dtm_relay_ip, args.dtm_relay_port,
            )
            print(f"  {'✓' if result['ok'] else '✗'} "
                  f"{result.get('err', 'restart requested')}",
                  flush=True)
        time.sleep(0.5)


def _run_script_core(script_path: str, registry: AgentRegistry,
                     send_cmd, args: argparse.Namespace) -> None:
    """Execute commands from script file (core mode)."""
    with open(script_path) as f:
        lines = [l.strip() for l in f
                 if l.strip() and not l.startswith("#")]
    for line in lines:
        print(f"[XRM-CORE] > {line}", flush=True)
        parts = line.split()
        cmd   = parts[0].lower()
        if cmd == "deploy" and len(parts) >= 3:
            service   = parts[1]
            ndt_names = parts[2:]
            for ndt_name in ndt_names:
                ndt_id = registry.get_ndt_id(ndt_name)
                if ndt_id < 0:
                    print(f"  ✗ {ndt_name}: could not resolve ndt_id",
                          flush=True)
                    continue
                result = request_deploy_service(
                    ndt_id, service,
                    args.dtm_relay_ip, args.dtm_relay_port,
                )
                print(f"  {ndt_name}: "
                      f"{'✓' if result['ok'] else '✗'} "
                      f"{result.get('err', 'restart requested')}",
                      flush=True)
        elif cmd == "stop" and len(parts) >= 3:
            service   = parts[1]
            ndt_names = parts[2:]
            for ndt_name in ndt_names:
                ndt_id = registry.get_ndt_id(ndt_name)
                if ndt_id < 0:
                    print(f"  ✗ {ndt_name}: could not resolve ndt_id",
                          flush=True)
                    continue
                result = request_stop_service(
                    ndt_id, service,
                    args.dtm_relay_ip, args.dtm_relay_port,
                )
                print(f"  {ndt_name}: "
                      f"{'✓' if result['ok'] else '✗'} "
                      f"{result.get('err', 'restart requested')}",
                      flush=True)
        elif cmd == "restart" and len(parts) == 2:
            ndt_name = parts[1]
            ndt_id   = registry.get_ndt_id(ndt_name)
            if ndt_id >= 0:
                send_restart_to_dtm(
                    ndt_id, args.dtm_relay_ip, args.dtm_relay_port)
                print(f"  {ndt_name}: ✓ restart requested", flush=True)
        time.sleep(0.5)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────
# CLI mode (new)
# Lightweight interactive CLI that connects to a running core.
# Does NOT bind the core port (8101) — uses an ephemeral reply
# port (8102) to receive responses from the core.
# Intended use: kubectl exec -it pod-xrm -n ndt -- \
#               python3 xrm.py --mode cli
# ─────────────────────────────────────────────────────────────

def run_cli(args: argparse.Namespace) -> None:
    """
    CLI mode — connects to a running XRM core.
    Sends commands to the core via UDP and receives replies.
    No ports bound except the ephemeral reply socket.
    """
    global _log_file
    _log_file = args.log_file

    # Reply socket — ephemeral port for receiving core responses
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
                links = result.get("links", [])
                print(f"  Links ({len(links)}):")
                for lk in links:
                    print(f"    {lk['a']}↔{lk['b']} "
                          f"up={lk.get('up')} "
                          f"delay={lk.get('delay_ms')}ms "
                          f"bw={lk.get('bw_mbps')}Mbps")
                print(f"  Services: {result.get('services', [])}")
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
                    print(f"  ✓ {ndt_name}: restart requested to DTM.")
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
                    print(f"  ✓ {ndt_name}: restart requested to DTM.")
                else:
                    print(f"  ✗ {ndt_name}: {result.get('err')}")

        else:
            _print_help_core()

    reply_sock.close()


def main() -> None:
    ap = argparse.ArgumentParser(
        description="XRM — Extended Reality Manager"
    )
    ap.add_argument("--mode",
                    choices=["1:1", "agent", "core", "cli"],
                    required=True)
    ap.add_argument("--ndt-id",          type=int, default=0,
                    help="[agent] NDT identifier (0-based)")
    ap.add_argument("--core-ip",         default="127.0.0.1",
                    help="[agent] XRM core IP")
    ap.add_argument("--script",          default="",
                    help="[1:1/core] path to command script file")
    ap.add_argument("--log-file",        default=LOG_FILE_DEFAULT)
    # DTM relay connection — needed to send RESTART_NDT events
    ap.add_argument("--dtm-relay-ip",    default="127.0.0.1",
                    help="[core/1:1] DTM relay IP for RESTART_NDT events")
    ap.add_argument("--dtm-relay-port",  type=int, default=7001,
                    help="[core/1:1] DTM relay port")
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