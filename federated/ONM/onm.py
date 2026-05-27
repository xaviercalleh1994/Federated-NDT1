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

Responsibilities:
  - Interactive CLI for the operator
  - Read topology graph from topology.json
  - Resolve logical links to physical segments (BFS)
  - Apply events locally on the ON via on_ndt.py primitives
  - Serialize and send events to DTM via relay/direct UDP or TCP
  - Handle topology updates (write new config + trigger RELOAD)

CLI commands:
  show
      Display current logical topology state.

  set A B up|down delay_ms loss_pct bw_mbps
      Apply a logical link change between nodes A and B.
      Example: set r1 r2 up 10 0 100
               set r1 r2 down 0 0 1000

  exec NODE "cmd"
      Run a shell command inside a Mininet node namespace.
      Example: exec r1 "ip route"

  topology <file.json>
      Load a new topology config, apply it to ON, send to DTM.
      Example: topology /app/topologies/3lan.json

  quit
      Exit the ONM.

Upstream server — XR service commands (new)
---------------------------------------------
  Activated with --upstream-server flag. Runs as a daemon thread
  alongside the interactive CLI without blocking it.

  Listens for XR_SERVICE_CMD from DTM upstream on UDP :7006.
  Applies command on the target node via on_ndt.py:6001:
    Writes state to /tmp/actuator/{node}/state.json
    Reads it back for verification.
  Sends ACK(event_id, ndt_id, ok, state, apply_ms) back to DTM
  on UDP dtm_ip:7007.

  Upstream port layout (new, separate from Exp1):
    7006  ONM rx from DTM upstream
    7007  ONM → DTM upstream ACK
"""
import json
import time
import uuid
import socket
import argparse
import threading
from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, deque

# ── File paths ────────────────────────────────────────────────
TOPO_FILE        = "/run/topology.json"
TOPO_CONFIG_FILE = "/run/topology_config.json"
LOG_FILE_DEFAULT = "/home/ubuntu/onm_events.log"

# ── on_ndt.py control server ──────────────────────────────────
CONTROL_HOST = "127.0.0.1"
CONTROL_PORT = 6001

# ── Upstream ports (new) ──────────────────────────────────────
ONM_UPSTREAM_RX_PORT  = 7006   # rx from DTM upstream
ONM_UPSTREAM_ACK_PORT = 7007   # tx ACK to DTM upstream

# ── Actuator state path convention ───────────────────────────
ACTUATOR_STATE_PATH = "/tmp/actuator/{node}/state.json"

# ── Runtime context for logging ──────────────────────────────
RUN_CTX: Dict[str, object] = {}


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

def log_event(tag: str, data: dict) -> None:
    entry = {"ts_ns": time.time_ns(), "tag": tag, **RUN_CTX, **data}
    with open(RUN_CTX.get("log_file", LOG_FILE_DEFAULT), "a") as f:
        f.write(json.dumps(entry) + "\n")


# ─────────────────────────────────────────────────────────────
# Logical edge representation
# ─────────────────────────────────────────────────────────────

@dataclass
class Edge:
    delay_ms: float
    loss_pct: float
    bw_mbps:  float
    up:       bool


# ─────────────────────────────────────────────────────────────
# Logical topology state (in-memory matrix)
# ─────────────────────────────────────────────────────────────

class Matrix:
    def __init__(self):
        self.idx: Dict[str, int] = {}
        self.rev: Dict[int, str] = {}
        self.m: List[List[Optional[Edge]]] = []

    def _ensure(self, n: str):
        if n in self.idx:
            return
        i = len(self.idx)
        self.idx[n] = i
        self.rev[i] = n
        for row in self.m:
            row.append(None)
        self.m.append([None] * (i + 1))

    def set(self, a: str, b: str, e: Edge):
        self._ensure(a)
        self._ensure(b)
        i, j = self.idx[a], self.idx[b]
        self.m[i][j] = e
        self.m[j][i] = e

    def show(self):
        print("\nLogical Nodes:")
        for n, i in self.idx.items():
            print(f"  {i}: {n}")
        print("\nLogical Links:")
        printed = set()
        for i in range(len(self.m)):
            for j in range(len(self.m)):
                e = self.m[i][j]
                if e is None:
                    continue
                a, b = self.rev[i], self.rev[j]
                key = tuple(sorted([a, b]))
                if key in printed:
                    continue
                printed.add(key)
                print(f"  {a} <-> {b} | up={e.up} "
                      f"delay={e.delay_ms}ms "
                      f"loss={e.loss_pct}% "
                      f"bw={e.bw_mbps}Mbps")


# ─────────────────────────────────────────────────────────────
# Topology graph helpers
# ─────────────────────────────────────────────────────────────

def is_switch(n: str) -> bool:
    return n.startswith("s")


def load_topo_graph() -> Tuple[List[str], Dict[str, List[str]]]:
    """Load Mininet-exported topology.json into adjacency graph."""
    with open(TOPO_FILE) as f:
        t = json.load(f)
    g = defaultdict(list)
    for a, b in t["links"]:
        g[a].append(b)
        g[b].append(a)
    return t["nodes"], g


def path_only_switches_middle(
    g: Dict[str, List[str]], src: str, dst: str
) -> List[Tuple[str, str]]:
    """BFS path where only switches are in the middle."""
    q    = deque([src])
    prev = {src: None}
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
    path = []
    cur  = dst
    while cur is not None:
        path.append(cur)
        cur = prev[cur]
    path.reverse()
    if any(not is_switch(x) for x in path[1:-1]):
        return []
    return [(path[i], path[i + 1]) for i in range(len(path) - 1)]


# ─────────────────────────────────────────────────────────────
# on_ndt.py primitive caller
# ─────────────────────────────────────────────────────────────

def control_call(ev: dict) -> dict:
    """
    Send a primitive command to on_ndt.py control server.
    on_ndt.py applies the physical change to Mininet.
    """
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
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
# ON event application
# ─────────────────────────────────────────────────────────────

def apply_logical_link(
    g: Dict[str, List[str]], a: str, b: str, e: Edge
) -> bool:
    """
    Resolve logical link A↔B to physical segments and apply via
    on_ndt.py LINK_SET primitives + ROUTE_REASSERT.
    """
    segs = path_only_switches_middle(g, a, b)
    if not segs:
        print(f"[ONM] No path between {a} and {b}")
        return False

    for x, y in segs:
        resp = control_call({
            "type":    "LINK_SET",
            "payload": {
                "a": x, "b": y,
                "up":       e.up,
                "delay_ms": e.delay_ms,
                "loss_pct": e.loss_pct,
                "bw_mbps":  e.bw_mbps,
            }
        })
        if not resp.get("ok"):
            print(f"[ONM] LINK_SET error on {x}↔{y}: {resp}")
            return False

    # Reassert routes on all LAN routers
    routers = [n for n in g if n.startswith("r") and n != "r_nat"]
    control_call({
        "type":    "EXEC",
        "payload": {"node": "r_nat", "cmd": "true"}
    })
    for r in routers:
        control_call({
            "type":    "EXEC",
            "payload": {
                "node": r,
                "cmd":  "ip route replace default via 10.0.0.254",
            }
        })
    return True


# ─────────────────────────────────────────────────────────────
# DTM sender
# ─────────────────────────────────────────────────────────────

class SyncSender:
    def send(self, data: bytes): raise NotImplementedError
    def close(self): pass


class UdpSender(SyncSender):
    def __init__(self, relay_ip: str, relay_port: int):
        self.addr = (relay_ip, relay_port)
        self.s    = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, data: bytes):
        self.s.sendto(data, self.addr)

    def close(self):
        self.s.close()


class TcpSender(SyncSender):
    def __init__(self, relay_ip: str, relay_port: int):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((relay_ip, relay_port))
        self.s.sendall(b"ON\n")

    def send(self, data: bytes):
        self.s.sendall(len(data).to_bytes(4, "big") + data)

    def close(self):
        self.s.close()


def send_topology_to_dtm(sender: SyncSender) -> None:
    """Send current topology to DTM (Exp. 1 handshake)."""
    with open(TOPO_FILE) as f:
        topo = json.load(f)
    ev      = {"type": "TOPOLOGY", "payload": topo}
    payload = json.dumps(ev).encode()
    sender.send(payload)
    print(f"[ONM] Topology sent to DTM "
          f"({len(topo['nodes'])} nodes, {len(topo['links'])} links)")
    log_event("ON_TOPOLOGY_SENT", {
        "nodes": len(topo["nodes"]),
        "links": len(topo["links"]),
        "size_bytes": len(payload),
    })


# ─────────────────────────────────────────────────────────────
# Topology update
# ─────────────────────────────────────────────────────────────

def apply_topology_update(
    new_topo_path: str,
    sender: SyncSender,
    mock_on: bool,
) -> bool:
    """
    Load new topology config, write to topology_config.json,
    trigger RELOAD on on_ndt.py, send TOPOLOGY_UPDATE to DTM.
    """
    try:
        with open(new_topo_path) as f:
            new_topo = json.load(f)
    except Exception as e:
        print(f"[ONM] Error loading {new_topo_path}: {e}")
        return False

    if "lans" not in new_topo or "backbone" not in new_topo:
        print("[ONM] Invalid topology: missing 'lans' or 'backbone'")
        return False

    with open(TOPO_CONFIG_FILE, "w") as f:
        json.dump(new_topo, f, indent=2)
    print(f"[ONM] New topology saved to {TOPO_CONFIG_FILE}")

    if not mock_on:
        resp = control_call({"type": "RELOAD"})
        if not resp.get("ok"):
            print(f"[ONM] RELOAD error: {resp}")
            return False
        print("[ONM] ON Mininet reloading...")
        for _ in range(90):
            time.sleep(1)
            try:
                with open(TOPO_FILE) as f:
                    topo = json.load(f)
                expected = len(new_topo["lans"])
                routers  = [n for n in topo["nodes"]
                            if n.startswith("r") and n != "r_nat"]
                if len(routers) == expected:
                    print(f"[ONM] ON reload complete "
                          f"({expected} LANs ready)")
                    break
            except Exception:
                pass
        else:
            print("[ONM] WARNING: ON reload timeout")

    event_id    = str(uuid.uuid4())
    t_create_ns = time.time_ns()
    ev = {
        "type":        "TOPOLOGY_UPDATE",
        "event_id":    event_id,
        "t_create_ns": t_create_ns,
        "payload":     {"topology": new_topo},
    }
    payload = json.dumps(ev).encode()
    t_tx_ns = time.time_ns()
    sender.send(payload)

    log_event("ON_TOPOLOGY_UPDATE_SENT", {
        "event_id":    event_id,
        "t_create_ns": t_create_ns,
        "t_tx_ns":     t_tx_ns,
        "lans":        len(new_topo["lans"]),
        "size_bytes":  len(payload),
    })
    print(f"[ONM] TOPOLOGY_UPDATE sent to DTM "
          f"({len(new_topo['lans'])} LANs)")
    return True


# ─────────────────────────────────────────────────────────────
# Upstream server — XR service commands (new)
# Daemon thread alongside the interactive CLI.
# Receives XR_SERVICE_CMD from DTM upstream on UDP :7006.
# Applies on target node via on_ndt.py:6001.
# Sends ACK back to DTM on UDP :7007.
# ─────────────────────────────────────────────────────────────

def _apply_xr_service_cmd(ev: dict) -> dict:
    """
    Apply XR_SERVICE_CMD on the target node via on_ndt.py:6001.
    Writes state to /tmp/actuator/{node}/state.json and reads
    it back for verification.
    Returns {ok, state, apply_ms}.
    """
    t0      = time.time_ns()
    payload = ev.get("payload", {})
    node    = payload.get("node", "h1_1")
    service = ev.get("service", "unknown")
    state   = {
        "event_id":     str(ev.get("event_id", "")),
        "ndt_id":       ev.get("ndt_id"),
        "service":      service,
        "t_applied_ns": time.time_ns(),
        **{k: v for k, v in payload.items() if k != "node"},
    }
    state_path = ACTUATOR_STATE_PATH.format(node=node)
    state_json = json.dumps(state).replace('"', '\\"')

    # Write state into node namespace via on_ndt.py EXEC
    write_resp = control_call({
        "type": "EXEC",
        "payload": {
            "node": node,
            "cmd": (
                f"mkdir -p /tmp/actuator/{node} && "
                f"echo \"{state_json}\" > {state_path}"
            ),
        },
    })

    if not write_resp.get("ok"):
        return {
            "ok":       False,
            "err":      f"write failed: {write_resp.get('err')}",
            "apply_ms": round((time.time_ns() - t0) / 1e6, 3),
            "state":    {},
        }

    # Read back for verification
    read_resp = control_call({
        "type": "EXEC",
        "payload": {"node": node, "cmd": f"cat {state_path}"},
    })

    verified_state = state
    if read_resp.get("ok") and read_resp.get("output"):
        try:
            verified_state = json.loads(read_resp["output"])
        except Exception:
            pass

    return {
        "ok":       True,
        "state":    verified_state,
        "apply_ms": round((time.time_ns() - t0) / 1e6, 3),
    }


def run_upstream_server(
    dtm_ip: str,
    mock_on: bool,
) -> None:
    """
    Upstream server thread — XR service coordination.
    Receives XR_SERVICE_CMD from DTM upstream on UDP :7006.
    Applies on ON via on_ndt.py:6001.
    Sends ACK back to DTM on UDP :7007.
    """
    rx_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx_sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF,
                       8 * 1024 * 1024)
    rx_sock.bind(("0.0.0.0", ONM_UPSTREAM_RX_PORT))
    rx_sock.settimeout(1.0)

    ack_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    print(f"[ONM-UPSTREAM] Ready — UDP :{ONM_UPSTREAM_RX_PORT}",
          flush=True)
    print(f"[ONM-UPSTREAM] ACK → {dtm_ip}:{ONM_UPSTREAM_ACK_PORT}",
          flush=True)

    while True:
        try:
            data, _ = rx_sock.recvfrom(65535)
        except socket.timeout:
            continue
        except Exception:
            continue

        t_rx_ns = time.time_ns()
        try:
            ev = json.loads(data)
        except Exception as exc:
            log_event("ONM_UPSTREAM_PARSE_ERR", {"err": str(exc)})
            continue

        event_id = str(ev.get("event_id", ""))
        ndt_id   = ev.get("ndt_id")
        service  = ev.get("service", "unknown")
        ev_type  = ev.get("type", "")

        if ev_type != "XR_SERVICE_CMD":
            log_event("ONM_UPSTREAM_UNKNOWN",
                      {"event_id": event_id, "type": ev_type})
            continue

        log_event("ONM_UPSTREAM_CMD_RX", {
            "event_id":   event_id,
            "ndt_id":     ndt_id,
            "service":    service,
            "t_rx_ns":    t_rx_ns,
            "size_bytes": len(data),
        })

        # Apply on ON
        if mock_on:
            result = {
                "ok":       True,
                "state":    ev.get("payload", {}),
                "apply_ms": 0.0,
            }
        else:
            result = _apply_xr_service_cmd(ev)

        t_done_ns = time.time_ns()

        log_event("ONM_UPSTREAM_CMD_APPLIED", {
            "event_id":  event_id,
            "ndt_id":    ndt_id,
            "service":   service,
            "ok":        result["ok"],
            "apply_ms":  result["apply_ms"],
            "t_rx_ns":   t_rx_ns,
            "t_done_ns": t_done_ns,
            "queue_ms":  round((t_done_ns - t_rx_ns) / 1e6, 3),
        })

        print(f"[ONM-UPSTREAM] {event_id} service={service} "
              f"ndt_id={ndt_id} ok={result['ok']} "
              f"apply_ms={result['apply_ms']}", flush=True)

        # Send ACK back to DTM upstream
        ack = json.dumps({
            "type":      "XR_SERVICE_ACK",
            "event_id":  event_id,
            "ndt_id":    ndt_id,
            "service":   service,
            "ok":        result["ok"],
            "state":     result.get("state", {}),
            "apply_ms":  result["apply_ms"],
            "t_done_ns": t_done_ns,
        }).encode()

        try:
            ack_sock.sendto(ack, (dtm_ip, ONM_UPSTREAM_ACK_PORT))
        except Exception as exc:
            log_event("ONM_UPSTREAM_ACK_ERR",
                      {"event_id": event_id, "err": str(exc)})


# ─────────────────────────────────────────────────────────────
# Main CLI
# ─────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(description="Original Network Manager")
    ap.add_argument("--proto",      choices=["udp", "tcp"], required=True)
    ap.add_argument("--relay-ip",   required=True,
                    help="DTM IP or hostname")
    ap.add_argument("--relay-port", type=int, default=7001)
    ap.add_argument("--relay-kind", choices=["router", "vps"], required=True)
    ap.add_argument("--log-file",   default=LOG_FILE_DEFAULT)
    ap.add_argument("--mock-on",    action="store_true",
                    help="Skip local ON application (no Mininet available)")
    ap.add_argument("--on-ip",      default="127.0.0.1",
                    help="IP of the ON Pod control server")
    # Upstream server — XR service commands (new)
    ap.add_argument("--upstream-server", action="store_true",
                    help="[upstream] Enable upstream server thread")
    ap.add_argument("--dtm-upstream-ip", default="",
                    help="[upstream] DTM IP to send ACK to")

    args = ap.parse_args()

    global RUN_CTX, CONTROL_HOST
    CONTROL_HOST = args.on_ip
    RUN_CTX = {
        "side":       "ON",
        "proto":      args.proto,
        "relay_kind": args.relay_kind,
        "relay_ip":   args.relay_ip,
        "relay_port": args.relay_port,
        "log_file":   args.log_file,
    }

    sender = (
        UdpSender(args.relay_ip, args.relay_port)
        if args.proto == "udp"
        else TcpSender(args.relay_ip, args.relay_port)
    )

    # Load topology graph
    try:
        nodes, g = load_topo_graph()
    except FileNotFoundError:
        print(f"[ONM] WARNING: {TOPO_FILE} not found.")
        nodes, g = [], defaultdict(list)

    # Send topology to DTM (Exp. 1 handshake)
    try:
        send_topology_to_dtm(sender)
    except Exception as e:
        print(f"[ONM] WARNING: Could not send topology: {e}")

    # Start upstream server thread if requested
    if args.upstream_server:
        if not args.dtm_upstream_ip:
            print("[ONM] ERROR: --dtm-upstream-ip required "
                  "with --upstream-server")
            return
        threading.Thread(
            target=run_upstream_server,
            args=(args.dtm_upstream_ip, args.mock_on),
            name="onm-upstream",
            daemon=True,
        ).start()

    M = Matrix()

    print("[ONM] Ready. Commands:")
    print("  show")
    print("  set A B up|down delay_ms loss_pct bw_mbps")
    print("    e.g.: set r1 r2 up 10 0 100")
    print("    e.g.: set r1 r2 down 0 0 1000")
    print('  exec NODE "cmd"')
    print('    e.g.: exec r1 "ip route"')
    print("  topology <file.json>")
    print("  quit\n")

    while True:
        try:
            line = input("> ").strip()
        except EOFError:
            break

        if not line:
            continue
        if line == "quit":
            break
        if line == "show":
            M.show()
            continue

        parts = line.split()
        cmd   = parts[0].lower()

        # ── set A B up|down delay loss bw ────────────────────
        if cmd == "set":
            if len(parts) != 7:
                print("Usage: set A B up|down delay_ms loss_pct bw_mbps")
                continue
            _, a, b, up_s, d, l, bw = parts
            e = Edge(
                delay_ms=float(d), loss_pct=float(l),
                bw_mbps=float(bw), up=(up_s.lower() == "up"),
            )
            M.set(a, b, e)
            event_id    = str(uuid.uuid4())
            t_create_ns = time.time_ns()
            log_event("ON_EVENT_CREATED", {
                "event_id": event_id, "a": a, "b": b,
                "params": asdict(e),
            })

            if not args.mock_on:
                ok = apply_logical_link(g, a, b, e)
                if not ok:
                    log_event("ON_EVENT_LOCAL_APPLY_FAIL",
                              {"event_id": event_id, "a": a, "b": b})
                    continue

            ev = {
                "type":        "LOGICAL_LINK_SET",
                "event_id":    event_id,
                "t_create_ns": t_create_ns,
                "payload":     {"a": a, "b": b, **asdict(e)},
            }
            payload = json.dumps(ev).encode()
            t_tx_ns = time.time_ns()
            sender.send(payload)
            log_event("ON_EVENT_SENT", {
                "event_id":    event_id,
                "t_create_ns": t_create_ns,
                "t_tx_ns":     t_tx_ns,
                "size_bytes":  len(payload),
            })
            if not args.mock_on:
                print("[ONM] Applied locally and sent to DTM")
            continue

        # ── exec NODE "cmd" ──────────────────────────────────
        if cmd == "exec":
            if len(parts) < 3:
                print('Usage: exec NODE "cmd"')
                continue
            node      = parts[1]
            shell_cmd = line.split(None, 2)[2].strip().strip('"')
            event_id    = str(uuid.uuid4())
            t_create_ns = time.time_ns()

            if not args.mock_on:
                resp = control_call({
                    "type":    "EXEC",
                    "payload": {"node": node, "cmd": shell_cmd},
                })
                print("[ONM EXEC]", resp)

            ev = {
                "type":        "EXEC_CMD",
                "event_id":    event_id,
                "t_create_ns": t_create_ns,
                "payload":     {"node": node, "cmd": shell_cmd},
            }
            sender.send(json.dumps(ev).encode())
            continue

        # ── topology <file.json> ─────────────────────────────
        if cmd == "topology":
            if len(parts) != 2:
                print("Usage: topology <file.json>")
                continue
            ok = apply_topology_update(parts[1], sender, args.mock_on)
            if ok and not args.mock_on:
                time.sleep(2)
                try:
                    nodes, g = load_topo_graph()
                    print(f"[ONM] Graph reloaded: {len(nodes)} nodes")
                except Exception as e:
                    print(f"[ONM] WARNING: Could not reload graph: {e}")
            continue

        print(f"Unknown command: {cmd}")

    sender.close()


if __name__ == "__main__":
    main()