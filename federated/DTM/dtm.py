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

dtm.py — Digital Twin Manager
-------------------------------
Combines DTM core and DTM distributed agent logic.

Modes
-----
1:1 mode:
  Receives events from ONM via relay.
  Resolves LOGICAL_LINK_SET → physical segments.
  Applies events on local NDT via on_ndt.py primitives (127.0.0.1:6001).
  No fanout, no feedback port.

federated mode:
  Receives events from ONM via relay.
  On TOPOLOGY event: builds adjacency graph.
  Fans out events to N NDT instances (UdpFanoutSender/TcpFanoutSender).
  Tracks ACKs from NDTs via FeedbackController + AckTracker.

  DTM distributed agent runs embedded in each NDT Pod:
    - Receives events from DTM core
    - Resolves LOGICAL_LINK_SET → physical segments
    - Applies events on local NDT via on_ndt.py primitives (127.0.0.1:6001)
    - Sends UDP ACK back to DTM core

  In the NDT Pod the DTM distributed agent is started by setting
  --mode agent. The agent connects to on_ndt.py on localhost.

Threading model
---------------
  Main thread  : receives events from relay (ONM→DTM).
  log-writer   : drains log deque and writes to disk.
  fb-rx        : receives NDT upstream feedback (federated mode).
  fb-dispatch  : drains AdmissionQueue, forwards critical events to ONM.

NDT state persistence and restart (new)
----------------------------------------
  DtmStateManager tracks current network state of each NDT and persists
  it to /tmp/dtm-state/ndt-{id}/state.json via a hostPath volume.
  Updated on every LOGICAL_LINK_SET dispatched to NDTs.

  RESTART_NDT event: DTM writes restart_requested signal file.
  watch_restarts.sh on the host detects it and calls restart_ndt.sh.
  On pod restart dtm-agent reads state.json and re-applies all links.

  Agent startup: if --ndt-id is provided, the agent reads state.json
  and re-applies all saved links before entering the receive loop.

Upstream mode — XR service coordination (new)
----------------------------------------------
  The DTM upstream-core receives XR service commands from xrm-agents,
  serialises them via AdmissionQueue (priority order, one-by-one),
  forwards each to the ONM upstream server, waits for ACK, and
  broadcasts ON_STATE_UPDATE to ALL registered xrm-agents.

  Port layout (upstream, separate from Exp1):
    7005  DTM upstream-core rx    (xrm-agents → DTM)
    7006  DTM → ONM upstream      (DTM forwards to ONM)
    7007  DTM upstream ACK rx     (ONM → DTM)
    7008  DTM → xrm-agents bc     (DTM broadcasts to agents)

  xrm-agent status per broadcast:
    ACK       — this agent's command was applied on ON
    QUEUED    — this agent has a command pending in queue
    PENDING   — idle
    DISCARDED — this agent's command was dropped
"""
import gc
import os
import json as _json
import time
import math
import heapq
import itertools
import socket
import argparse
import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
import orjson


# ─────────────────────────────────────────────────────────────
# JSON helpers
# ─────────────────────────────────────────────────────────────

def _dumps(obj: object) -> bytes:
    return orjson.dumps(obj)

def _loads(data: bytes) -> dict:
    return orjson.loads(data)


# ─────────────────────────────────────────────────────────────
# File paths
# ─────────────────────────────────────────────────────────────

TOPO_FILE        = "/run/topology.json"
TOPO_CONFIG_FILE = "/run/topology_config.json"
LOG_FILE_DEFAULT = "/home/ubuntu/ndtm_events.log"
CONTROL_HOST     = "127.0.0.1"
CONTROL_PORT     = 6001

# State persistence base path — hostPath volume on cluster node
DTM_STATE_BASE = "/tmp/dtm-state"

# Upstream ports — XR service coordination (new)
DTM_UPSTREAM_RX_PORT  = 7005   # rx from xrm-agents
DTM_UPSTREAM_ONM_PORT = 7006   # tx to ONM upstream
DTM_UPSTREAM_ACK_PORT = 7007   # rx ACK from ONM
DTM_UPSTREAM_BC_PORT  = 7008   # tx broadcast to xrm-agents

run_ctx: Dict[str, object] = {}
_done_ids: set   = set()
_done_lock       = threading.Lock()
stop_event       = threading.Event()

_log_deque: deque      = deque()
_log_stop              = threading.Event()
_log_ctx_prefix: bytes = b""


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

def _build_log_prefix() -> None:
    global _log_ctx_prefix
    base = _dumps(run_ctx)
    _log_ctx_prefix = base[:-1]


def _log_writer() -> None:
    path = str(run_ctx.get("log_file", LOG_FILE_DEFAULT))
    tmp: deque = deque()
    with open(path, "wb") as f:
        while not _log_stop.is_set() or _log_deque:
            if _log_deque:
                tmp.clear()
                while _log_deque:
                    tmp.append(_log_deque.popleft())
                f.write(b"\n".join(tmp) + b"\n")
                f.flush()
            else:
                time.sleep(0.01)


def log_event(tag: str, data: dict) -> None:
    try:
        event_bytes = _dumps({"ts_ns": time.time_ns(), "tag": tag, **data})
        merged = _log_ctx_prefix + b"," + event_bytes[1:]
        _log_deque.append(merged)
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# Experiment control
# ─────────────────────────────────────────────────────────────

def count_done_event(event_id: str) -> None:
    limit = int(run_ctx.get("event_limit", 0) or 0)
    if limit <= 0 or not event_id:
        return
    with _done_lock:
        if event_id in _done_ids:
            return
        _done_ids.add(event_id)
        n = len(_done_ids)
    if run_ctx.get("print_progress") and n % 500 == 0:
        print(f"[DTM] done {n}/{limit}", flush=True)
    if n >= limit:
        stop_event.set()


def _flush_and_exit(reason: str) -> None:
    print(f"\n[DTM] {reason}. Flushing logs...", flush=True)
    _log_stop.set()
    deadline = time.monotonic() + 6.0
    while _log_deque and time.monotonic() < deadline:
        time.sleep(0.05)
    print("[DTM] Logs flushed. Exiting.", flush=True)
    gc.enable()
    os._exit(0)


def limit_reached() -> None:
    if stop_event.is_set():
        _flush_and_exit("Experiment limit reached")


# ─────────────────────────────────────────────────────────────
# on_ndt.py primitive caller (distributed agent)
# ─────────────────────────────────────────────────────────────

def recv_exact_tcp(sock: socket.socket, n: int) -> Optional[bytes]:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


def control_call(ev: dict) -> dict:
    """
    Send a primitive command to on_ndt.py on localhost:6001.
    Used by the DTM distributed agent to apply events on the NDT.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((CONTROL_HOST, CONTROL_PORT))
        s.sendall((_dumps(ev) + b"\n"))
        resp = b""
        while b"\n" not in resp:
            chunk = s.recv(4096)
            if not chunk:
                break
            resp += chunk
        s.close()
        return _loads(resp.split(b"\n")[0]) if resp else {"ok": False}
    except Exception as e:
        return {"ok": False, "err": str(e)}


# ─────────────────────────────────────────────────────────────
# Topology graph helpers (distributed agent)
# ─────────────────────────────────────────────────────────────

@dataclass
class Edge:
    delay_ms: float
    loss_pct: float
    bw_mbps:  float
    up:       bool


def is_switch(n: str) -> bool:
    return n.startswith("s")


def load_topo_graph() -> Dict[str, List[str]]:
    with open(TOPO_FILE) as f:
        t = orjson.loads(f.read())
    g: Dict[str, List[str]] = defaultdict(list)
    for a, b in t["links"]:
        g[a].append(b)
        g[b].append(a)
    return g


def build_graph_from_payload(payload: dict) -> Dict[str, List[str]]:
    g: Dict[str, List[str]] = defaultdict(list)
    for a, b in payload.get("links", []):
        g[a].append(b)
        g[b].append(a)
    return g


def path_only_switches_middle(
    g: Dict[str, List[str]], src: str, dst: str
) -> List[Tuple[str, str]]:
    q: deque = deque([src])
    prev: Dict[str, Optional[str]] = {src: None}
    while q:
        cur = q.popleft()
        if cur == dst:
            break
        for nxt in g[cur]:
            if nxt not in prev:
                prev[nxt] = cur
                q.append(nxt)
    if dst not in prev:
        return []
    path: List[str] = []
    cur = dst
    while cur is not None:
        path.append(cur)
        cur = prev[cur]
    path.reverse()
    if any(not is_switch(x) for x in path[1:-1]):
        return []
    return [(path[i], path[i + 1]) for i in range(len(path) - 1)]


def apply_logical_link(
    g: Dict[str, List[str]], a: str, b: str, e: Edge
) -> int:
    """
    Resolve logical link A↔B → physical segments.
    Apply each segment via on_ndt.py LINK_SET primitive.
    Returns number of segments applied.
    """
    phys = path_only_switches_middle(g, a, b)
    if not phys:
        return 0
    for x, y in phys:
        control_call({"type": "LINK_SET", "payload": {
            "a": x, "b": y, **e.__dict__
        }})
    # Reassert routes on LAN routers
    routers = [n for n in g if n.startswith("r") and n != "r_nat"]
    for r in routers:
        control_call({"type": "EXEC", "payload": {
            "node": r,
            "cmd":  "ip route replace default via 10.0.0.254",
        }})
    return len(phys)


def resolve_to_physical(
    g: Optional[Dict[str, List[str]]], ev: dict
) -> List[bytes]:
    if ev.get("type") != "LOGICAL_LINK_SET" or g is None:
        return [_dumps(ev)]
    p    = ev.get("payload", {})
    a, b = p.get("a"), p.get("b")
    segs = path_only_switches_middle(g, a, b)
    eid  = ev.get("event_id", "")
    if not segs:
        log_event("DTM_NO_PATH", {"event_id": eid, "a": a, "b": b})
        return []
    edge = {
        "delay_ms": float(p.get("delay_ms", 0.0)),
        "loss_pct": float(p.get("loss_pct", 0.0)),
        "bw_mbps":  float(p.get("bw_mbps", 1000.0)),
        "up":       bool(p.get("up", True)),
    }
    payloads: List[bytes] = []
    for x, y in segs:
        payloads.append(_dumps({
            "type":     "LINK_SET",
            "event_id": eid,
            "payload":  {"a": x, "b": y, **edge},
        }))
    return payloads


# ─────────────────────────────────────────────────────────────
# DTM State Manager (new)
# Persists per-NDT network state to hostPath volume.
# Updated on every LOGICAL_LINK_SET dispatched to NDTs.
# ─────────────────────────────────────────────────────────────

class DtmStateManager:
    def __init__(self) -> None:
        self._lock  = threading.Lock()
        self._cache: Dict[int, dict] = {}

    def _path(self, ndt_id: int) -> str:
        return os.path.join(DTM_STATE_BASE, f"ndt-{ndt_id}", "state.json")

    def update_link(self, ndt_id: int, link: dict) -> None:
        """Update a single link in the in-memory cache and persist."""
        with self._lock:
            state = self._cache.setdefault(ndt_id, {
                "ndt_id": ndt_id, "links": [], "services": []
            })
            links = state["links"]
            for i, lk in enumerate(links):
                if lk.get("a") == link.get("a") and \
                   lk.get("b") == link.get("b"):
                    links[i] = link
                    break
            else:
                links.append(link)
            state_copy = dict(state)
        try:
            path = self._path(ndt_id)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w") as f:
                _json.dump(state_copy, f, indent=2)
        except Exception as exc:
            log_event("DTM_STATE_PERSIST_ERR",
                      {"ndt_id": ndt_id, "err": str(exc)})


def request_ndt_restart(ndt_id: int) -> None:
    """
    Signal watch_restarts.sh to restart NDT ndt_id.
    Writes restart_requested in the hostPath directory.
    watch_restarts.sh on the host detects this file and calls
    restart_ndt.sh which performs kubectl delete + apply.
    The DTM has no dependency on kubectl or subprocess.
    """
    signal_path = os.path.join(
        DTM_STATE_BASE, f"ndt-{ndt_id}", "restart_requested")
    try:
        os.makedirs(os.path.dirname(signal_path), exist_ok=True)
        with open(signal_path, "w") as f:
            _json.dump({"ndt_id": ndt_id, "t_ns": time.time_ns()}, f)
        log_event("DTM_RESTART_REQUESTED", {"ndt_id": ndt_id})
        print(f"[DTM] Restart requested for ndt-{ndt_id}", flush=True)
    except Exception as exc:
        log_event("DTM_RESTART_SIGNAL_ERR",
                  {"ndt_id": ndt_id, "err": str(exc)})


# ─────────────────────────────────────────────────────────────
# NDT state restore (agent side, new)
# Called on agent startup before entering the receive loop.
# Reads saved state from hostPath and re-applies all links.
# ─────────────────────────────────────────────────────────────

def _restore_ndt_state(
        ndt_id: int,
        g: Optional[Dict[str, List[str]]] = None) -> None:
    # Restore saved network state after pod restart.
    # Uses apply_logical_link with the topology graph so logical links
    # are resolved to physical segments correctly.
    path = os.path.join(DTM_STATE_BASE, f"ndt-{ndt_id}", "state.json")
    try:
        with open(path) as f:
            state = _json.load(f)
    except FileNotFoundError:
        print(f"[AGENT] No saved state for ndt-{ndt_id}", flush=True)
        return
    except Exception as exc:
        print(f"[AGENT] Could not read state: {exc}", flush=True)
        return

    links = state.get("links", [])
    if not links:
        print(f"[AGENT] ndt-{ndt_id}: no links to restore", flush=True)
        return

    print(f"[AGENT] ndt-{ndt_id}: restoring {len(links)} links...",
          flush=True)
    for link in links:
        a = link["a"]
        b = link["b"]
        e = Edge(
            delay_ms=float(link.get("delay_ms", 0.0)),
            loss_pct=float(link.get("loss_pct", 0.0)),
            bw_mbps= float(link.get("bw_mbps", 1000.0)),
            up=      bool(link.get("up", True)),
        )
        if g is not None:
            segs = apply_logical_link(g, a, b, e)
            ok   = segs > 0
            err  = None if ok else "no path found"
        else:
            resp = control_call({
                "type": "LINK_SET",
                "payload": {
                    "a": a, "b": b,
                    "delay_ms": e.delay_ms,
                    "loss_pct": e.loss_pct,
                    "bw_mbps":  e.bw_mbps,
                    "up":       e.up,
                },
            })
            ok  = resp.get("ok", False)
            err = resp.get("err")
        log_event("AGENT_LINK_RESTORED", {
            "ndt_id": ndt_id, "link": link, "ok": ok, "err": err,
        })
        print(f"[AGENT]   {a}\u2194{b} ok={ok}"
              + (f" err={err}" if err else ""), flush=True)


# ─────────────────────────────────────────────────────────────
# DTM upstream — XR service coordination (new)
# Receives XR service commands from xrm-agents, serialises
# via AdmissionQueue (priority order), forwards to ONM,
# waits for ACK, broadcasts ON_STATE_UPDATE to all xrm-agents.
# ─────────────────────────────────────────────────────────────

class XrmAgentRegistry:
    """
    Tracks all xrm-agents that have sent at least one command.
    ndt_id → {ip, bc_port}
    Used for broadcasting ON_STATE_UPDATE.
    """
    def __init__(self) -> None:
        self._lock  = threading.Lock()
        self._agents: Dict[int, dict] = {}
        self._inflight: Dict[str, int] = {}  # event_id → ndt_id

    def register(self, ndt_id: int, ip: str, bc_port: int) -> None:
        with self._lock:
            self._agents[ndt_id] = {"ip": ip, "bc_port": bc_port}

    def set_inflight(self, event_id: str, ndt_id: int) -> None:
        with self._lock:
            self._inflight[event_id] = ndt_id

    def clear_inflight(self, event_id: str) -> None:
        with self._lock:
            self._inflight.pop(event_id, None)

    def all_agents(self) -> Dict[int, dict]:
        with self._lock:
            return dict(self._agents)

    def queued_ndt_ids(self, queued_ids: set) -> set:
        with self._lock:
            return {nid for eid, nid in self._inflight.items()
                    if eid in queued_ids}


def _broadcast_state_update(
    bc_sock: socket.socket,
    registry: XrmAgentRegistry,
    processed_event_id: str,
    processed_ndt_id: int,
    service: str,
    node: str,
    state: dict,
    apply_ms: float,
    t_applied_ns: int,
    t_create_ns: int,
    queued_ids: set,
    status_override: Optional[Dict[int, str]] = None,
) -> None:
    """
    Broadcast ON_STATE_UPDATE to all registered xrm-agents.
    Each agent receives its individual status:
      ACK       — this agent's command was applied
      QUEUED    — this agent has a pending command
      PENDING   — idle
      DISCARDED — this agent's command was dropped
    """
    queued_ndt_ids = registry.queued_ndt_ids(queued_ids)
    for ndt_id, info in registry.all_agents().items():
        if status_override and ndt_id in status_override:
            your_status = status_override[ndt_id]
        elif ndt_id == processed_ndt_id:
            your_status = "ACK"
        elif ndt_id in queued_ndt_ids:
            your_status = "QUEUED"
        else:
            your_status = "PENDING"

        msg = _dumps({
            "type":               "ON_STATE_UPDATE",
            "processed_event_id": processed_event_id,
            "processed_ndt_id":   processed_ndt_id,
            "service":            service,
            "node":               node,
            "state":              state,
            "apply_ms":           apply_ms,
            "t_applied_ns":       t_applied_ns,
            "t_create_ns":        t_create_ns,
            "your_status":        your_status,
            "ndt_id":             ndt_id,
        })
        try:
            bc_sock.sendto(msg, (info["ip"], info["bc_port"]))
        except Exception as exc:
            log_event("DTM_UPSTREAM_BC_ERR",
                      {"ndt_id": ndt_id, "err": str(exc)})

    log_event("DTM_UPSTREAM_BC_SENT", {
        "processed_event_id": processed_event_id,
        "processed_ndt_id":   processed_ndt_id,
        "service":            service,
        "n_agents":           len(registry.all_agents()),
        "apply_ms":           apply_ms,
    })


def run_upstream(args: argparse.Namespace) -> None:
    """
    DTM upstream mode — XR service coordination.
    Receives XR service commands from xrm-agents on UDP :7005.
    Serialises via AdmissionQueue (priority order, one-by-one).
    Forwards each to ONM upstream on UDP :7006.
    Waits for ACK from ONM on UDP :7007.
    Broadcasts ON_STATE_UPDATE to all xrm-agents on UDP :7008.
    """
    global run_ctx
    run_ctx = {
        "side":               "DTM_UPSTREAM",
        "onm_ip":             args.onm_ip,
        "log_file":           args.log_file,
        "event_limit":        args.event_limit,
        "max_queue":          args.max_queue,
        "disable_priorities": args.disable_priorities,
    }
    threading.Thread(target=_log_writer, name="log-writer",
                     daemon=True).start()
    _build_log_prefix()

    gc.disable()

    registry = XrmAgentRegistry()
    aq = AdmissionQueue(
        max_queue          = args.max_queue,
        drop_prio_if_full  = args.drop_prio_if_full,
        disable_priorities = args.disable_priorities,
    )

    # Receive XR service commands from xrm-agents
    rx_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                       64 * 1024 * 1024)
    rx_sock.bind(("0.0.0.0", DTM_UPSTREAM_RX_PORT))
    rx_sock.settimeout(0.5)

    # Forward to ONM upstream
    onm_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Receive ACK from ONM
    ack_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    ack_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                        8 * 1024 * 1024)
    ack_sock.bind(("0.0.0.0", DTM_UPSTREAM_ACK_PORT))
    ack_sock.settimeout(args.onm_ack_timeout)

    # Broadcast to xrm-agents
    bc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    print(f"[DTM-UPSTREAM] Ready", flush=True)
    print(f"[DTM-UPSTREAM] rx :{DTM_UPSTREAM_RX_PORT}  "
          f"ack :{DTM_UPSTREAM_ACK_PORT}  "
          f"bc :{DTM_UPSTREAM_BC_PORT}", flush=True)
    print(f"[DTM-UPSTREAM] → ONM {args.onm_ip}:{DTM_UPSTREAM_ONM_PORT}",
          flush=True)

    def _rx_loop() -> None:
        while True:
            try:
                data, addr = rx_sock.recvfrom(65535)
            except socket.timeout:
                continue
            except Exception:
                continue

            t_rx_ns = time.time_ns()
            try:
                ev = _loads(data)
            except Exception as exc:
                log_event("DTM_UPSTREAM_PARSE_ERR", {"err": str(exc)})
                continue

            ev_type  = ev.get("type", "")
            event_id = str(ev.get("event_id", ""))
            ndt_id   = int(ev.get("ndt_id", -1))
            pr       = int(ev.get("priority", 100))
            bc_port  = int(ev.get("agent_bc_port", DTM_UPSTREAM_BC_PORT))

            # Register agent on first contact
            registry.register(ndt_id, addr[0], bc_port)

            if ev_type != "XR_SERVICE_CMD":
                log_event("DTM_UPSTREAM_UNKNOWN",
                          {"event_id": event_id, "type": ev_type})
                continue

            log_event("DTM_UPSTREAM_RX", {
                "event_id":   event_id,
                "ndt_id":     ndt_id,
                "priority":   pr,
                "service":    ev.get("service", ""),
                "t_rx_ns":    t_rx_ns,
                "size_bytes": len(data),
            })

            registry.set_inflight(event_id, ndt_id)

            admitted = aq.push(
                {"ev": ev, "t_rx_ns": t_rx_ns, "priority": pr,
                 "event_id": event_id, "ndt_id": ndt_id,
                 "t_create_ns": int(ev.get("t_create_ns", 0))},
                pr, event_id, ev_type, "upstream",
            )

            if not admitted:
                registry.clear_inflight(event_id)
                _broadcast_state_update(
                    bc_sock, registry,
                    processed_event_id=event_id,
                    processed_ndt_id=ndt_id,
                    service=ev.get("service", ""),
                    node=ev.get("payload", {}).get("node", ""),
                    state={}, apply_ms=0.0,
                    t_applied_ns=time.time_ns(),
                    t_create_ns=int(ev.get("t_create_ns", 0)),
                    queued_ids=set(),
                    status_override={ndt_id: "DISCARDED"},
                )

    def _dispatch_loop() -> None:
        while True:
            limit_reached()
            items = aq.drain(max_items=1)
            for item in items:
                ev          = item["ev"]
                event_id    = item["event_id"]
                ndt_id      = item["ndt_id"]
                t_rx_ns     = item["t_rx_ns"]
                t_create_ns = item["t_create_ns"]
                service     = ev.get("service", "")
                node        = ev.get("payload", {}).get("node", "")

                # Forward to ONM upstream
                t_fwd_ns = time.time_ns()
                try:
                    onm_sock.sendto(
                        _dumps(ev),
                        (args.onm_ip, DTM_UPSTREAM_ONM_PORT),
                    )
                except Exception as exc:
                    log_event("DTM_UPSTREAM_FWD_ERR",
                              {"event_id": event_id, "err": str(exc)})
                    registry.clear_inflight(event_id)
                    _broadcast_state_update(
                        bc_sock, registry,
                        processed_event_id=event_id,
                        processed_ndt_id=ndt_id,
                        service=service, node=node,
                        state={}, apply_ms=0.0,
                        t_applied_ns=time.time_ns(),
                        t_create_ns=t_create_ns,
                        queued_ids=set(),
                        status_override={ndt_id: "DISCARDED"},
                    )
                    continue

                log_event("DTM_UPSTREAM_FORWARDED", {
                    "event_id": event_id,
                    "ndt_id":   ndt_id,
                    "service":  service,
                    "t_fwd_ns": t_fwd_ns,
                })

                # Wait for ACK from ONM
                state    = {}
                apply_ms = 0.0
                ok       = False
                try:
                    ack_data, _ = ack_sock.recvfrom(65535)
                    ack      = _loads(ack_data)
                    state    = ack.get("state", {})
                    apply_ms = float(ack.get("apply_ms", 0.0))
                    ok       = bool(ack.get("ok", False))
                except socket.timeout:
                    log_event("DTM_UPSTREAM_ACK_TIMEOUT",
                              {"event_id": event_id, "ndt_id": ndt_id})

                t_done_ns = time.time_ns()
                e2e_ms    = round((t_done_ns - t_create_ns) / 1e6, 3) \
                            if t_create_ns else None

                log_event("DTM_UPSTREAM_DONE", {
                    "event_id":    event_id,
                    "ndt_id":      ndt_id,
                    "service":     service,
                    "ok":          ok,
                    "apply_ms":    apply_ms,
                    "e2e_ms":      e2e_ms,
                    "t_create_ns": t_create_ns,
                    "t_rx_ns":     t_rx_ns,
                    "t_done_ns":   t_done_ns,
                    "queue_ms":    round((t_done_ns - t_rx_ns) / 1e6, 3),
                })

                registry.clear_inflight(event_id)

                # Get current queued event_ids for status assignment
                # (non-destructive peek — see AdmissionQueue.peek_ids)
                queued_ids = aq.peek_ids()

                _broadcast_state_update(
                    bc_sock, registry,
                    processed_event_id=event_id,
                    processed_ndt_id=ndt_id,
                    service=service,
                    node=node,
                    state=state,
                    apply_ms=apply_ms,
                    t_applied_ns=t_done_ns,
                    t_create_ns=t_create_ns,
                    queued_ids=queued_ids,
                )

                count_done_event(event_id)
                limit_reached()

    threading.Thread(target=_rx_loop,
                     name="upstream-rx", daemon=True).start()
    threading.Thread(target=_dispatch_loop,
                     name="upstream-dispatch", daemon=True).start()

    while True:
        limit_reached()
        time.sleep(0.1)


# ─────────────────────────────────────────────────────────────
# DTM distributed agent — apply events on local NDT
# ─────────────────────────────────────────────────────────────

def apply_event_on_ndt(
    ev: dict,
    g: Optional[Dict[str, List[str]]],
    relay_port: int,
    dtm_ip: str,
    ack_port: int,
) -> dict:
    """
    DTM distributed agent logic.
    Applies an event on the local NDT via on_ndt.py primitives.
    Sends UDP ACK to DTM core on completion.
    """
    event_id = str(ev.get("event_id", ""))
    ev_type  = ev.get("type", "")
    t_rx_ns  = time.time_ns()

    if ev_type == "LOGICAL_LINK_SET":
        p = ev.get("payload", {})
        try:
            e = Edge(
                delay_ms=float(p["delay_ms"]),
                loss_pct=float(p["loss_pct"]),
                bw_mbps= float(p["bw_mbps"]),
                up=      bool(p["up"]),
            )
        except Exception as exc:
            result = {"ok": False, "err": str(exc)}
        else:
            segs   = apply_logical_link(g, p.get("a"), p.get("b"), e)
            result = {"ok": segs > 0, "segments": segs}

    elif ev_type == "TOPOLOGY_UPDATE":
        # Write new topology config and trigger RELOAD on on_ndt.py
        new_topo = ev.get("payload", {}).get("topology")
        if new_topo:
            with open(TOPO_CONFIG_FILE, "w") as f:
                _json.dump(new_topo, f, indent=2)
            resp   = control_call({"type": "RELOAD"})
            result = resp
        else:
            result = {"ok": False, "err": "Missing topology payload"}

    elif ev_type == "EXEC_CMD":
        p    = ev.get("payload", {})
        resp = control_call({
            "type":    "EXEC",
            "payload": {"node": p.get("node"), "cmd": p.get("cmd")},
        })
        result = resp

    else:
        result = {"ok": False, "err": f"Unknown event type: {ev_type}"}

    t_apply_ns = time.time_ns()
    apply_ms   = (t_apply_ns - t_rx_ns) / 1e6

    log_event("NDT_EVENT_APPLIED", {
        "event_id":   event_id,
        "type":       ev_type,
        "t_rx_ns":    t_rx_ns,
        "t_apply_ns": t_apply_ns,
        "apply_ms":   apply_ms,
        "ok":         result.get("ok"),
    })

    # Send ACK to DTM core
    if dtm_ip:
        ack_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        ack = _dumps({
            "type":       "NDT_APPLY_ACK",
            "event_id":   event_id,
            "port":       relay_port,
            "ok":         result.get("ok", False),
            "t_apply_ns": t_apply_ns,
            "apply_ms":   apply_ms,
        })
        try:
            ack_sock.sendto(ack, (dtm_ip, ack_port))
        except Exception as exc:
            log_event("DTM_ACK_SEND_ERR", {"err": str(exc)})

    return result


# ─────────────────────────────────────────────────────────────
# Relay receivers
# ─────────────────────────────────────────────────────────────

def recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


class UdpRelayReceiver:
    def __init__(self, relay_ip: str, relay_port: int) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                              64 * 1024 * 1024)
        self._sock.settimeout(0.5)
        self._sock.bind(("0.0.0.0", relay_port))
        self._relay = (relay_ip, relay_port)
        threading.Thread(target=self._keepalive, daemon=True).start()

    def _keepalive(self) -> None:
        while True:
            try:
                self._sock.sendto(b"HELLO_NDT", self._relay)
            except Exception:
                pass
            time.sleep(5)

    def recv(self) -> bytes:
        try:
            return self._sock.recvfrom(65535)[0]
        except socket.timeout:
            return b""


class TcpRelayReceiver:
    def __init__(self, relay_ip: str, relay_port: int) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock.settimeout(0.5)
        self._sock.connect((relay_ip, relay_port))
        self._sock.sendall(b"NDT\n")

    def recv(self) -> bytes:
        try:
            hdr = recv_exact(self._sock, 4)
            if not hdr:
                return b""
            return recv_exact(self._sock,
                              int.from_bytes(hdr, "big")) or b""
        except socket.timeout:
            return b""


# ─────────────────────────────────────────────────────────────
# Fanout senders (federated mode)
# ─────────────────────────────────────────────────────────────

class UdpFanoutSender:
    def __init__(self, dst_ip: str, base_port: int, fanout: int) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF,
                              8 * 1024 * 1024)
        self._targets = [(dst_ip, base_port + i) for i in range(fanout)]

    def send_all(self, payload: bytes, idxs: List[int]) -> int:
        ok = 0
        for i in idxs:
            try:
                self._sock.sendto(payload, self._targets[i])
                ok += 1
            except Exception:
                pass
        return ok


class TcpFanoutSender:
    def __init__(self, dst_ip: str, base_port: int, fanout: int) -> None:
        self._ip    = dst_ip
        self._ports = [base_port + i for i in range(fanout)]
        self._socks: Dict[int, socket.socket] = {}

    def _get(self, port: int) -> Optional[socket.socket]:
        if port in self._socks:
            return self._socks[port]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.connect((self._ip, port))
            self._socks[port] = s
            return s
        except Exception:
            return None

    def send_all(self, payload: bytes, idxs: List[int]) -> int:
        frame = len(payload).to_bytes(4, "big") + payload
        ok    = 0
        for i in idxs:
            s = self._get(self._ports[i])
            if not s:
                continue
            try:
                s.sendall(frame)
                ok += 1
            except Exception:
                self._socks.pop(self._ports[i], None)
        return ok


# ─────────────────────────────────────────────────────────────
# Rate limiting + queue (federated mode)
# ─────────────────────────────────────────────────────────────

class TokenBucket:
    WINDOW_S = 0.008

    def __init__(self, rate: float) -> None:
        self._rate              = rate
        self._tokens_per_window = max(1, math.floor(rate * self.WINDOW_S))
        self._window_end        = time.perf_counter() + self.WINDOW_S
        self._remaining         = self._tokens_per_window
        print(
            f"[DTM] TokenBucket: {rate:.0f} ev/s | "
            f"window={self.WINDOW_S*1000:.0f}ms | "
            f"batch={self._tokens_per_window}",
            flush=True,
        )

    def next_batch(self) -> int:
        return self._remaining

    def consume(self, n: int) -> None:
        self._remaining -= n

    def wait_window_end(self) -> None:
        gap = self._window_end - time.perf_counter()
        if gap > 0:
            time.sleep(gap)
        self._window_end += self.WINDOW_S
        if self._window_end < time.perf_counter():
            self._window_end = time.perf_counter() + self.WINDOW_S
        self._remaining = self._tokens_per_window


class AdmissionQueue:
    # ── FIX (2026-07-19) ────────────────────────────────────────
    # Previously self._q was a plain deque: push() always appended
    # and drain()/drain_up_to() always popleft()'d, so dispatch order
    # was strict FIFO regardless of `priority` — priority only ever
    # affected admission (drop) decisions, never service order. This
    # meant "priority" mode and "fifo" mode produced identical e2e
    # latency, since nothing was ever actually reordered.
    #
    # Fix: replace the deque with a binary heap (heapq) keyed on
    # priority when priority mode is active, or on a monotonic
    # counter when disable_priorities/fifo mode is active (preserving
    # strict FIFO for that control condition). A monotonically
    # increasing tie-breaker (self._counter) is always included as
    # the second tuple element so heapq never has to compare the
    # `item` dicts themselves (which are not orderable) when two
    # entries share the same priority.
    # ─────────────────────────────────────────────────────────────
    def __init__(self, max_queue: int, drop_prio_if_full: int,
                 disable_priorities: bool, no_queue: bool = False) -> None:
        self._max       = max_queue
        self._drop_prio = drop_prio_if_full
        self._fifo      = disable_priorities
        self._no_queue  = no_queue
        self._heap: list = []
        self._counter    = itertools.count()
        self._lock       = threading.Lock()
        self._cond       = threading.Condition(self._lock)
        self._qsize      = 0
        self._dispatch_busy = False

    def set_busy(self, busy: bool) -> None:
        self._dispatch_busy = busy

    def push(self, item: dict, priority: int,
             event_id: str, ev_type: str, src: str) -> bool:
        drop_reason: Optional[str] = None
        with self._lock:
            if self._no_queue:
                if self._dispatch_busy or self._qsize > 0:
                    drop_reason = "no_queue_busy"
            elif self._max > 0 and self._qsize >= self._max:
                if self._fifo or priority >= self._drop_prio:
                    drop_reason = "tail_drop" if self._fifo else "prio_drop"
            if drop_reason is None:
                # order key: priority (lower = served first) in priority
                # mode, or arrival sequence in fifo mode — either way the
                # counter guarantees stable ordering and a valid heap
                # comparison without ever touching `item`.
                seq = next(self._counter)
                order_key = seq if self._fifo else priority
                heapq.heappush(self._heap, (order_key, seq, item))
                self._qsize += 1
                self._cond.notify()
        if drop_reason is not None:
            log_event("DTM_DROP", {
                "event_id": event_id, "type": ev_type,
                "priority": priority, "src": src,
                "reason": drop_reason, "qsize": self._qsize,
            })
            return False
        return True

    def drain(self, max_items: int = 2000) -> List[dict]:
        with self._lock:
            while not self._heap:
                self._cond.wait(timeout=0.0005)
            batch: List[dict] = []
            while self._heap and len(batch) < max_items:
                _, _, item = heapq.heappop(self._heap)
                batch.append(item)
                self._qsize -= 1
        return batch

    def drain_up_to(self, max_items: int) -> List[dict]:
        with self._lock:
            batch: List[dict] = []
            while self._heap and len(batch) < max_items:
                _, _, item = heapq.heappop(self._heap)
                batch.append(item)
                self._qsize -= 1
        return batch

    # ── FIX (2026-07-19) ────────────────────────────────────────
    # Non-destructive peek. run_upstream's _dispatch_loop needs to
    # know which event_ids are currently waiting (to broadcast
    # "QUEUED" status) without removing them from the heap.
    # drain_up_to() always pops — calling it with max_items=0 was
    # being used for this purpose but its loop condition
    # (len(batch) < max_items) is false immediately when
    # max_items=0, so it always returned an empty set and no
    # xrm-agent ever received "QUEUED" status. Use this instead.
    # ─────────────────────────────────────────────────────────────
    def peek_ids(self) -> set:
        with self._lock:
            return {item[2]["event_id"] for item in self._heap}


# ─────────────────────────────────────────────────────────────
# ACK tracker (Exp. 1)
# ─────────────────────────────────────────────────────────────

class AckTracker:
    def __init__(self, fanout: int) -> None:
        self._fanout   = fanout
        self._lock     = threading.Lock()
        self._acks:      Dict[str, Dict[int, int]] = defaultdict(dict)
        self._t_dispatch: Dict[str, int] = {}

    def register_dispatched(self, event_id: str, t_ns: int) -> None:
        with self._lock:
            self._t_dispatch[event_id] = t_ns

    def register_ack(self, event_id: str, port: int,
                     ok: bool, t_apply_ns: int) -> None:
        with self._lock:
            self._acks[event_id][port] = t_apply_ns
            n_acked  = len(self._acks[event_id])
            t_disp   = self._t_dispatch.get(event_id, 0)
            complete = (n_acked >= self._fanout)
        if complete:
            t_now = time.time_ns()
            log_event("DTM_EVENT_COMPLETE", {
                "event_id":      event_id,
                "fanout":        self._fanout,
                "acks":          n_acked,
                "t_dispatch_ns": t_disp,
                "t_complete_ns": t_now,
                "wall_ms": (t_now - t_disp) / 1e6 if t_disp else None,
            })
            count_done_event(event_id)
            limit_reached()


# ─────────────────────────────────────────────────────────────
# Feedback controller (federated mode)
# ─────────────────────────────────────────────────────────────

class FeedbackController:
    def __init__(self, listen_port: int,
                 onm_sender: Optional[UdpFanoutSender],
                 aq: AdmissionQueue,
                 token_bucket: Optional[TokenBucket] = None,
                 ack_tracker:  Optional[AckTracker]  = None) -> None:
        self._aq      = aq
        self._onm     = onm_sender
        self._tb      = token_bucket
        self._tracker = ack_tracker
        self._sock    = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                              64 * 1024 * 1024)
        self._sock.bind(("0.0.0.0", listen_port))
        threading.Thread(target=self._rx_loop,
                         name="fb-rx", daemon=True).start()
        threading.Thread(target=self._dispatch_loop,
                         name="fb-dispatch", daemon=True).start()
        print(f"[DTM] FeedbackController listening on UDP {listen_port}",
              flush=True)

    def _rx_loop(self) -> None:
        while True:
            try:
                data, _ = self._sock.recvfrom(65535)
            except Exception:
                continue
            t_rx_ns = time.time_ns()
            try:
                ev = _loads(data)
            except Exception:
                continue
            event_id = str(ev.get("event_id", ""))
            ev_type  = ev.get("type", "NA")
            pr       = int(ev.get("priority", 100))

            if ev_type == "NDT_APPLY_ACK" and self._tracker:
                self._tracker.register_ack(
                    event_id   = event_id,
                    port       = int(ev.get("port", 0)),
                    ok         = bool(ev.get("ok", False)),
                    t_apply_ns = int(ev.get("t_apply_ns", t_rx_ns)),
                )
                log_event("DTM_ACK_RX", {
                    "event_id": event_id,
                    "port":     ev.get("port"),
                    "ok":       ev.get("ok"),
                    "apply_ms": ev.get("apply_ms"),
                    "t_rx_ns":  t_rx_ns,
                })
                continue

            log_event("NDT_EVENT_RX", {
                "event_id":   event_id,
                "type":       ev_type,
                "t_rx_ns":    t_rx_ns,
                "size_bytes": len(data),
            })
            self._aq.push(
                {"ev": ev, "raw": data, "t_rx_ns": t_rx_ns,
                 "priority": pr, "event_id": event_id},
                pr, event_id, ev_type, "feedback",
            )

    def _process_item(self, item: dict) -> None:
        ev          = item["ev"]
        event_id    = item["event_id"]
        pr          = item["priority"]
        t_rx_ns     = item["t_rx_ns"]
        data: bytes = item["raw"]

        if pr <= 20 and self._onm:
            t0 = time.time_ns()
            try:
                self._onm.send_all(data, [0])
                log_event("DTM_ALERT_TO_ONM", {
                    "event_id":          event_id,
                    "priority":          pr,
                    "latency_to_onm_ns": time.time_ns() - t0,
                })
            except Exception:
                pass

        t_done_ns = time.time_ns()
        log_event("DTM_EVENT_DONE", {
            "event_id":  event_id,
            "type":      ev.get("type"),
            "src":       "feedback",
            "priority":  pr,
            "t_rx_ns":   t_rx_ns,
            "queue_ms":  (t_done_ns - t_rx_ns) / 1e6,
            "t_done_ns": t_done_ns,
        })
        count_done_event(event_id)
        limit_reached()

    def _dispatch_loop(self) -> None:
        tb = self._tb
        if tb is None:
            while True:
                for item in self._aq.drain():
                    if self._aq._no_queue:
                        self._aq.set_busy(True)
                    self._process_item(item)
                    if self._aq._no_queue:
                        self._aq.set_busy(False)
        else:
            while True:
                allowed    = tb.next_batch()
                items      = self._aq.drain(max_items=allowed)
                dispatched = 0
                for item in items:
                    self._process_item(item)
                    dispatched += 1
                tb.consume(dispatched)
                remaining = allowed - dispatched
                if remaining > 0:
                    for item in self._aq.drain_up_to(remaining):
                        self._process_item(item)
                        dispatched += 1
                tb.wait_window_end()


# ─────────────────────────────────────────────────────────────
# Fast dispatch (federated mode)
# ─────────────────────────────────────────────────────────────

def fast_dispatch(
    sender, fanout: int, group_size: int, wave_delay: float,
    ev: dict, payload: bytes,
    g: Optional[Dict[str, List[str]]] = None,
    tracker: Optional[AckTracker]     = None,
) -> Tuple[int, int]:
    is_crit = (
        ev.get("type") == "LOGICAL_LINK_SET"
        and not ev.get("payload", {}).get("up", True)
    )
    targets     = list(range(fanout))
    sent_ok     = 0
    dispatch_ns = 0
    payloads    = [_dumps(ev)]
    if not payloads:
        return 0, 0

    t_dispatch_start = time.time_ns()

    for pld in payloads:
        if is_crit:
            t0          = time.time_ns()
            sent_ok    += sender.send_all(pld, targets)
            dispatch_ns += time.time_ns() - t0
        else:
            gs = max(1, group_size)
            for i in range(0, fanout, gs):
                t0           = time.time_ns()
                sent_ok     += sender.send_all(pld, targets[i: i + gs])
                dispatch_ns += time.time_ns() - t0
                if wave_delay > 0:
                    time.sleep(wave_delay)

    if tracker and ev.get("event_id"):
        tracker.register_dispatched(ev["event_id"], t_dispatch_start)

    return sent_ok, dispatch_ns


# ─────────────────────────────────────────────────────────────
# Agent mode — DTM distributed agent running in NDT Pod
# ─────────────────────────────────────────────────────────────

def run_agent(args: argparse.Namespace) -> None:
    """
    DTM distributed agent mode.
    Runs inside the NDT Pod alongside on_ndt.py.
    Receives events from DTM core, applies them via on_ndt.py,
    sends ACKs back.
    On startup: restores saved network state from hostPath if present.
    """
    global run_ctx
    run_ctx = {
        "side":       "NDT_AGENT",
        "proto":      args.proto,
        "relay_ip":   args.relay_ip,
        "relay_port": args.relay_port,
        "log_file":   args.log_file,
    }
    threading.Thread(target=_log_writer, name="log-writer",
                     daemon=True).start()
    _build_log_prefix()

    # Wait for Mininet to be fully ready before restoring state.
    # topology.json is written only after Mininet has finished starting.
    print("[AGENT] Waiting for Mininet to be ready...", flush=True)
    for _ in range(90):
        if os.path.exists(TOPO_FILE):
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(1.0)
                s.connect((CONTROL_HOST, CONTROL_PORT))
                s.close()
                print("[AGENT] Mininet ready", flush=True)
                break
            except Exception:
                pass
        time.sleep(2)
    else:
        print("[AGENT] WARNING: Mininet not ready after timeout",
              flush=True)

    # Load local topology graph
    g: Optional[Dict[str, List[str]]] = None
    try:
        g = load_topo_graph()
        print(f"[AGENT] Topology loaded ({len(g)} nodes)", flush=True)
    except FileNotFoundError:
        print("[AGENT] topology.json not found — "
              "waiting for TOPOLOGY event", flush=True)

    # Restore saved network state if --ndt-id provided
    if args.ndt_id >= 0:
        _restore_ndt_state(args.ndt_id, g)

    # Connect to DTM core
    if args.proto == "udp":
        rx = UdpRelayReceiver(args.relay_ip, args.relay_port)
    else:
        rx = TcpRelayReceiver(args.relay_ip, args.relay_port)

    print(f"[AGENT] Ready — {args.proto.upper()} "
          f"relay={args.relay_ip}:{args.relay_port}", flush=True)
    print(f"[AGENT] ACK → {args.dtm_ip}:{args.ack_port}", flush=True)

    while True:
        data = rx.recv()
        if not data:
            # If topology not loaded yet, retry loading from file
            if g is None:
                try:
                    g = load_topo_graph()
                    print(f"[AGENT] Topology loaded from file "
                          f"({len(g)} nodes)", flush=True)
                except FileNotFoundError:
                    pass
            continue
        t_rx_ns = time.time_ns()
        try:
            ev = _loads(data)
        except Exception as exc:
            log_event("AGENT_PARSE_ERR",
                      {"err": str(exc), "size": len(data)})
            continue

        event_id = str(ev.get("event_id", ""))
        ev_type  = ev.get("type", "NA")

        log_event("NDT_EVENT_RX", {
            "event_id":   event_id,
            "type":       ev_type,
            "t_rx_ns":    t_rx_ns,
            "size_bytes": len(data),
        })

        # TOPOLOGY handshake — build graph, no ACK needed
        if ev_type == "TOPOLOGY":
            g = build_graph_from_payload(ev.get("payload", {}))
            print(f"[AGENT] Topology received ({len(g)} nodes)",
                  flush=True)
            log_event("AGENT_TOPOLOGY_RX", {
                "nodes":      len(g),
                "t_rx_ns":    t_rx_ns,
                "size_bytes": len(data),
            })
            continue

        # Apply event on local NDT
        result = apply_event_on_ndt(
            ev, g,
            relay_port = args.relay_port,
            dtm_ip     = args.dtm_ip,
            ack_port   = args.ack_port,
        )

        print(f"[AGENT] {ev_type} ok={result.get('ok')} "
              f"({(time.time_ns()-t_rx_ns)/1e6:.1f}ms)", flush=True)


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Digital Twin Manager")
    ap.add_argument("--proto",       required=True, choices=["udp", "tcp"])
    ap.add_argument("--relay-ip",    required=True)
    ap.add_argument("--relay-port",  type=int, default=7001)
    ap.add_argument("--onm-port",    type=int, default=7002)
    ap.add_argument("--mode",        choices=["1:1", "federated", "agent",
                                              "upstream"],
                    default="1:1")
    ap.add_argument("--fanout",      type=int, default=0)
    ap.add_argument("--base-port",   type=int, default=8001)
    ap.add_argument("--dst-ip",      default="127.0.0.1")
    ap.add_argument("--group-size",  type=int, default=5)
    ap.add_argument("--wave-delay",  type=float, default=0.0)
    ap.add_argument("--feedback-port", type=int, default=9000)
    ap.add_argument("--dtm-ip",      default="",
                    help="DTM core IP for agent ACKs")
    ap.add_argument("--ack-port",    type=int, default=9000)
    ap.add_argument("--max-queue",   type=int, default=0)
    ap.add_argument("--drop-prio-if-full", type=int, default=60)
    ap.add_argument("--disable-priorities", action="store_true")
    ap.add_argument("--no-queue",    action="store_true")
    ap.add_argument("--max-rate",    type=float, default=0.0)
    ap.add_argument("--log-file",    default=LOG_FILE_DEFAULT)
    ap.add_argument("--event-limit", type=int, default=0)
    ap.add_argument("--print-progress", action="store_true")
    ap.add_argument("--experiment-duration", type=float, default=0)
    # ndt-id for agent mode — used to restore state on startup
    ap.add_argument("--ndt-id",      type=int, default=-1,
                    help="[agent] NDT identifier (0-based) for state restore")
    # upstream mode args
    ap.add_argument("--onm-ip",      default="127.0.0.1",
                    help="[upstream] ONM IP for XR service commands")
    ap.add_argument("--onm-ack-timeout", type=float, default=10.0,
                    help="[upstream] seconds to wait for ONM ACK")
    args = ap.parse_args()

    # ── Upstream mode ─────────────────────────────────────────
    if args.mode == "upstream":
        run_upstream(args)
        return

    # ── Agent mode — runs in NDT Pod ──────────────────────────
    if args.mode == "agent":
        run_agent(args)
        return

    # ── Core modes (1:1 and federated) ───────────────────────
    global run_ctx
    run_ctx = {
        "side":               "NDT",
        "proto":              args.proto,
        "mode":               args.mode,
        "relay_ip":           args.relay_ip,
        "relay_port":         args.relay_port,
        "fanout":             args.fanout,
        "base_port":          args.base_port,
        "dst_ip":             args.dst_ip,
        "max_queue":          args.max_queue,
        "disable_priorities": args.disable_priorities,
        "log_file":           args.log_file,
        "event_limit":        args.event_limit,
        "print_progress":     args.print_progress,
    }
    threading.Thread(target=_log_writer, name="log-writer",
                     daemon=True).start()
    _build_log_prefix()

    if args.experiment_duration > 0:
        def _timer() -> None:
            print(f"[DTM] Timer: {args.experiment_duration}s", flush=True)
            time.sleep(args.experiment_duration)
            stop_event.set()
            _flush_and_exit("Experiment duration reached")
        threading.Thread(target=_timer, name="exp-timer",
                         daemon=True).start()

    gc.disable()

    g: Optional[Dict[str, List[str]]] = (
        load_topo_graph() if args.mode == "1:1" else None
    )

    rx = (
        UdpRelayReceiver(args.relay_ip, args.relay_port)
        if args.proto == "udp"
        else TcpRelayReceiver(args.relay_ip, args.relay_port)
    )

    fast_sender  = None
    ack_tracker  = None
    fc           = None
    state_mgr: Optional[DtmStateManager] = None

    if args.mode == "federated":
        if args.fanout <= 0:
            raise SystemExit("ERROR: --fanout > 0 required in federated mode")
        fast_sender = (
            UdpFanoutSender(args.dst_ip, args.base_port, args.fanout)
            if args.proto == "udp"
            else TcpFanoutSender(args.dst_ip, args.base_port, args.fanout)
        )
        onm_sender = UdpFanoutSender(args.relay_ip, args.onm_port, 1)
        aq = AdmissionQueue(
            max_queue          = args.max_queue,
            drop_prio_if_full  = args.drop_prio_if_full,
            disable_priorities = args.disable_priorities,
            no_queue           = args.no_queue,
        )
        tb = TokenBucket(args.max_rate) if args.max_rate > 0 else None
        fc = FeedbackController(
            listen_port  = args.feedback_port,
            onm_sender   = onm_sender,
            aq           = aq,
            token_bucket = tb,
            ack_tracker  = ack_tracker,
        )
        state_mgr = DtmStateManager()
        print(
            f"[DTM] Federated | fanout={args.fanout} "
            f"max_queue={args.max_queue}",
            flush=True,
        )

    print(f"[DTM] Ready ({args.proto.upper()}, mode={args.mode})",
          flush=True)

    while True:
        limit_reached()
        data = rx.recv()
        if not data:
            continue
        t_rx_ns = time.time_ns()
        try:
            ev = _loads(data)
        except Exception as exc:
            log_event("NDT_RX_PARSE_ERR",
                      {"err": str(exc), "size_bytes": len(data)})
            continue

        event_id = str(ev.get("event_id", ""))
        ev_type  = ev.get("type", "NA")

        # TOPOLOGY handshake (federated Exp. 1)
        if ev_type == "TOPOLOGY":
            if args.mode == "federated":
                g = build_graph_from_payload(ev.get("payload", {}))
                if ack_tracker is None and fc is not None:
                    ack_tracker = AckTracker(fanout=args.fanout)
                    fc._tracker = ack_tracker
                print(f"[DTM] Topology received ({len(g)} nodes)",
                      flush=True)
                log_event("DTM_TOPOLOGY_RX", {
                    "nodes":      len(g),
                    "size_bytes": len(data),
                    "t_rx_ns":    t_rx_ns,
                })
            continue

        log_event("NDT_EVENT_RX", {
            "event_id":   event_id,
            "type":       ev_type,
            "t_rx_ns":    t_rx_ns,
            "size_bytes": len(data),
        })

        # ── 1:1 mode ──────────────────────────────────────────
        if args.mode == "1:1":
            result = apply_event_on_ndt(
                ev, g,
                relay_port = args.relay_port,
                dtm_ip     = "",
                ack_port   = 0,
            )
            count_done_event(event_id)
            limit_reached()

        # ── Federated mode ────────────────────────────────────
        else:
            assert fast_sender is not None

            if ev_type == "LOGICAL_LINK_SET" and g is None:
                log_event("DTM_NO_TOPOLOGY", {"event_id": event_id})
                print("[DTM] WARNING: LOGICAL_LINK_SET before TOPOLOGY",
                      flush=True)
                continue

            # Update state manager on every link change
            if ev_type == "LOGICAL_LINK_SET" and state_mgr is not None:
                p = ev.get("payload", {})
                for ndt_id in range(args.fanout):
                    state_mgr.update_link(ndt_id, {
                        "a":        p.get("a"),
                        "b":        p.get("b"),
                        "delay_ms": float(p.get("delay_ms", 0.0)),
                        "loss_pct": float(p.get("loss_pct", 0.0)),
                        "bw_mbps":  float(p.get("bw_mbps", 1000.0)),
                        "up":       bool(p.get("up", True)),
                    })

            # RESTART_NDT — write signal file for watch_restarts.sh
            if ev_type == "RESTART_NDT":
                ndt_id = int(ev.get("payload", {}).get("ndt_id", -1))
                if ndt_id < 0 or ndt_id >= args.fanout:
                    log_event("DTM_RESTART_INVALID",
                              {"event_id": event_id, "ndt_id": ndt_id})
                else:
                    request_ndt_restart(ndt_id)
                continue

            t0 = time.time_ns()
            sent_ok, dispatch_ns = fast_dispatch(
                sender     = fast_sender,
                fanout     = args.fanout,
                group_size = args.group_size,
                wave_delay = args.wave_delay,
                ev         = ev,
                payload    = data,
                g          = g,
                tracker    = ack_tracker,
            )
            t1 = time.time_ns()
            log_event("NDT_EVENT_DISPATCHED", {
                "event_id":            event_id,
                "type":                ev_type,
                "fanout":              args.fanout,
                "sent_ok":             sent_ok,
                "dispatch_ns":         dispatch_ns,
                "t_dispatch_start_ns": t0,
                "t_dispatch_end_ns":   t1,
            })
            if int(sent_ok) > 0:
                count_done_event(event_id)
            limit_reached()


if __name__ == "__main__":
    main()