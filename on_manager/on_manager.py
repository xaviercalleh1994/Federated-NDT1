#!/usr/bin/env python3
import json
import time
import uuid
import socket
import argparse
from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, deque

TOPO_FILE = "/run/topology.json"
LOG_FILE = "/tmp/onm_events.log"

CONTROL_HOST = "127.0.0.1"
CONTROL_PORT = 6001


# ---------------- LOGGING ----------------

def log_event(tag: str, data: dict):
    entry = {
        "ts_ns": time.time_ns(),
        "tag": tag,
        **data
    }
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


# ---------------- DATA STRUCTURES ----------------

@dataclass
class Edge:
    delay_ms: float
    loss_pct: float
    bw_mbps: float
    up: bool


# ---------------- TOPOLOGY ----------------

def is_switch(n: str) -> bool:
    return n.startswith("s")


def load_topo_graph():
    with open(TOPO_FILE) as f:
        t = json.load(f)
    g = defaultdict(list)
    for a, b in t["links"]:
        g[a].append(b)
        g[b].append(a)
    return t["nodes"], g


def path_only_switches_middle(g, src, dst):
    q = deque([src])
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
    cur = dst
    while prev[cur] is not None:
        path.append((prev[cur], cur))
        cur = prev[cur]
    path.reverse()

    mids = [x for seg in path for x in seg][1:-1]
    if any(not is_switch(x) for x in mids):
        return []

    return path


# ---------------- CONTROL PLANE (ON local) ----------------

def control_call(ev: dict) -> dict:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((CONTROL_HOST, CONTROL_PORT))
    s.sendall((json.dumps(ev) + "\n").encode())
    data = b""
    while b"\n" not in data:
        data += s.recv(4096)
    s.close()
    return json.loads(data.split(b"\n")[0].decode())


def apply_logical_edge_to_on(g, a, b, e: Edge):
    phys = path_only_switches_middle(g, a, b)
    if not phys:
        return False

    for x, y in phys:
        control_call({
            "type": "LINK_SET",
            "payload": {
                "a": x, "b": y,
                "up": e.up,
                "delay_ms": e.delay_ms,
                "loss_pct": e.loss_pct,
                "bw_mbps": e.bw_mbps
            }
        })

    control_call({
        "type": "ROUTE_REASSERT",
        "payload": {"routers": ["r1", "r2"], "gateway": "10.0.0.254"}
    })
    return True


# ---------------- TRANSPORT ----------------

class SyncSender:
    def send(self, data: bytes): ...
    def close(self): ...


class UdpSender(SyncSender):
    def __init__(self, ip, port):
        self.addr = (ip, port)
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    def send(self, data: bytes):
        self.s.sendto(data, self.addr)

    def close(self):
        self.s.close()


class TcpSender(SyncSender):
    def __init__(self, ip, port):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((ip, port))
        self.s.sendall(b"ON")

    def send(self, data: bytes):
        self.s.sendall(len(data).to_bytes(4, "big") + data)

    def close(self):
        self.s.close()


# ---------------- MAIN ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proto", choices=["udp", "tcp"], required=True)
    ap.add_argument("--relay-ip", required=True)
    ap.add_argument("--relay-port", type=int, default=7001)
    args = ap.parse_args()

    sender = UdpSender(args.relay_ip, args.relay_port) if args.proto == "udp" \
        else TcpSender(args.relay_ip, args.relay_port)

    nodes, g = load_topo_graph()

    print("[ON] Ready. Type: set A B up|down delay loss bw")

    while True:
        line = input("> ").strip()
        if not line:
            continue
        if line == "quit":
            break

        _, a, b, up, d, l, bw = line.split()
        e = Edge(float(d), float(l), float(bw), up == "up")

        event_id = str(uuid.uuid4())
        t_create_ns = time.time_ns()

        log_event("ON_EVENT_CREATED", {
            "event_id": event_id,
            "a": a, "b": b,
            "params": asdict(e)
        })

        apply_logical_edge_to_on(g, a, b, e)

        ev = {
            "type": "LOGICAL_LINK_SET",
            "event_id": event_id,
            "t_create_ns": t_create_ns,
            "payload": {"a": a, "b": b, **asdict(e)}
        }
        data = json.dumps(ev).encode()
        t_tx_ns = time.time_ns()

        sender.send(data)

        log_event("ON_EVENT_SENT", {
            "event_id": event_id,
            "size_bytes": len(data),
            "t_create_ns": t_create_ns,
            "t_tx_ns": t_tx_ns,
            "proto": args.proto
        })

    sender.close()


if __name__ == "__main__":
    main()
