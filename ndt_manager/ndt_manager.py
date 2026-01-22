#!/usr/bin/env python3
import json
import time
import socket
import argparse
from dataclasses import dataclass
from typing import Dict, List, Tuple
from collections import defaultdict, deque
import threading

TOPO_FILE = "/run/topology.json"
CONTROL_HOST = "127.0.0.1"
CONTROL_PORT = 6001

# Default log file for NDTM (can be overridden with --log-file)
LOG_FILE_DEFAULT = "/home/ubuntu/ndtm_events.log"

# Runtime context appended to every log line
RUN_CTX: Dict[str, object] = {}


# Log structured events for metrics extraction
def log_event(tag: str, data: dict):
    entry = {"ts_ns": time.time_ns(), "tag": tag, **RUN_CTX, **data}
    with open(RUN_CTX.get("log_file", LOG_FILE_DEFAULT), "a") as f:
        f.write(json.dumps(entry) + "\n")


# TCP helper: receive exactly N bytes for applying events
def recv_exact(sock: socket.socket, n: int):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# link between two nodes
@dataclass
class Edge:
    delay_ms: float
    loss_pct: float
    bw_mbps: float
    up: bool


# Identify switch nodes to exclude from graph
def is_switch(n: str) -> bool:
    return n.startswith("s")


# Build graph from exported topology
def load_topo_graph() -> Dict[str, List[str]]:
    with open(TOPO_FILE) as f:
        t = json.load(f)
    links = [(a, b) for a, b in t["links"]]

    g = defaultdict(list)

    for a, b in links:
        g[a].append(b)
        g[b].append(a)
    return g


# Find path with only switches as intermediate nodes
def path_only_switches_middle(g: Dict[str, List[str]], src: str, dst: str) -> List[Tuple[str, str]]:
    q = deque([src])
    prev = {src: None}

    while q:
        cur = q.popleft()
        if cur == dst:
            break
        for nxt in g[cur]:
            if nxt in prev:
                continue
            prev[nxt] = cur
            q.append(nxt)

    if dst not in prev:
        return []

    path_nodes = []
    cur = dst

    while cur is not None:
        path_nodes.append(cur)
        cur = prev[cur]

    path_nodes.reverse()

    mids = path_nodes[1:-1]

    if any(not is_switch(x) for x in mids):
        return []

    return [(path_nodes[i], path_nodes[i + 1]) for i in range(len(path_nodes) - 1)]


# Send event to local Mininet control server (TCP JSON line)
def control_call(ev: dict) -> dict:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((CONTROL_HOST, CONTROL_PORT))
    s.sendall((json.dumps(ev) + "\n").encode())

    data = b""

    while b"\n" not in data:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk

    s.close()

    if not data:
        return {"ok": False, "err": "no response"}
    return json.loads(data.split(b"\n")[0].decode())


# Apply logical link over physical segments in NDT (LINK_SET + ROUTE_REASSERT)
def apply_logical_link(g, a: str, b: str, e: Edge) -> int:

    phys_edges = path_only_switches_middle(g, a, b)

    if not phys_edges:
        print(f"[NDT] No logical path (only-switch intermediates) between {a} and {b}")
        return 0

    for x, y in phys_edges:
        resp = control_call({
            "type": "LINK_SET",
            "payload": {
                "a": x, "b": y,
                "up": e.up,
                "delay_ms": e.delay_ms,
                "loss_pct": e.loss_pct,
                "bw_mbps": e.bw_mbps
            }
        })

        if not resp.get("ok", False):
            print("[NDT] LINK_SET error:", resp)
            return 0

    rr = control_call({
        "type": "ROUTE_REASSERT",
        "payload": {"routers": ["r1", "r2"], "gateway": "10.0.0.254"}
    })
    if not rr.get("ok", True):
        print("[NDT] ROUTE_REASSERT warning:", rr)

    return len(phys_edges)


# ---------- UDP Receiver ----------
# NDT registers with relay via HELLO_NDT and keeps NAT mapping alive
class UdpReceiver:
    def __init__(self, relay_ip: str, relay_port: int, keepalive_s: int = 5):
        self.relay = (relay_ip, relay_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", relay_port))  # listen on same port 7001
        self.keepalive_s = keepalive_s
        self._start_keepalive()

    def _start_keepalive(self):
        def loop():
            while True:
                try:
                    self.sock.sendto(b"HELLO_NDT", self.relay)
                except Exception:
                    pass
                time.sleep(self.keepalive_s)

        t = threading.Thread(target=loop, daemon=True)
        t.start()

    def recv(self) -> bytes:
        data, _ = self.sock.recvfrom(65535)
        return data
    

# ---------- TCP Receiver ----------
class TcpReceiver:
    def __init__(self, relay_ip: str, relay_port: int):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((relay_ip, relay_port))
        self.sock.sendall(b"NDT\n")

    def recv(self) -> bytes:
        hdr = recv_exact(self.sock, 4)
        if hdr is None:
            return b""
        ln = int.from_bytes(hdr, "big")
        payload = recv_exact(self.sock, ln)

        return payload if payload else b""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proto", choices=["udp", "tcp"], required=True)
    ap.add_argument("--relay-ip", required=True)
    ap.add_argument("--relay-port", type=int, default=7001)
    ap.add_argument("--relay-kind", choices=["router", "vps"], required=True,
                    help="Type of relay used: local router VM or remote VPS server")
    ap.add_argument("--log-file", default=LOG_FILE_DEFAULT,
                    help="Path to NDT manager log file (JSONL)")
    args = ap.parse_args()

    global RUN_CTX
    RUN_CTX = {
        "side": "NDT",
        "proto": args.proto,
        "relay_kind": args.relay_kind,
        "relay_ip": args.relay_ip,
        "relay_port": args.relay_port,
        "log_file": args.log_file
    }

    g = load_topo_graph()

    if args.proto == "udp":
        rx = UdpReceiver(args.relay_ip, args.relay_port)
        print(f"[NDT] Ready (UDP), listening on {args.relay_port}")
    else:
        rx = TcpReceiver(args.relay_ip, args.relay_port)
        print("[NDT] Ready (TCP)")

    while True:
        data = rx.recv()
        if not data:
            continue

        t_rx_ns = time.time_ns()

        try:
            ev = json.loads(data.decode())
        except Exception as e:
            log_event("NDT_RX_PARSE_ERR", {"err": str(e), "size_bytes": len(data)})
            continue

        t = ev.get("type", "NA")

        if t == "LOGICAL_LINK_SET":
            event_id = ev.get("event_id", "NA")
            t_create_ns = ev.get("t_create_ns", None)
            p = ev.get("payload", {})

            print(f"[NDT] EVENT RECEIVED {event_id}", flush=True)

            log_event("NDT_EVENT_RX", {
                "event_id": event_id,
                "t_create_ns": t_create_ns,
                "t_rx_ns": t_rx_ns,
                "size_bytes": len(data)
            })

            try:
                e = Edge(
                    delay_ms=float(p["delay_ms"]),
                    loss_pct=float(p["loss_pct"]),
                    bw_mbps=float(p["bw_mbps"]),
                    up=bool(p["up"])
                )
            except Exception as ex:
                log_event("NDT_EVENT_BAD_PAYLOAD", {"event_id": event_id, "err": str(ex)})
                continue

            segments = apply_logical_link(g, p["a"], p["b"], e)
            t_apply_ns = time.time_ns()

            log_event("NDT_EVENT_APPLIED", {
                "event_id": event_id,
                "t_create_ns": t_create_ns,
                "t_rx_ns": t_rx_ns,
                "t_apply_ns": t_apply_ns,
                "segments": segments,
                "a": p.get("a"),
                "b": p.get("b"),
                "up": e.up
            })

            print(f"[NDT] EVENT APPLIED {event_id}", flush=True)
            continue

        if t == "EXEC_CMD":
            # Optional: replicate EXEC_CMD
            resp = control_call(ev)
            log_event("NDT_EXEC_CMD", {"ok": resp.get("ok", False)})
            continue

        log_event("NDT_UNKNOWN_EVENT", {"type": t, "size_bytes": len(data)})


if __name__ == "__main__":
    main()
