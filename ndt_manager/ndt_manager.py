#!/usr/bin/env python3
import json
import time
import socket
import argparse
from dataclasses import dataclass
from collections import defaultdict, deque

TOPO_FILE = "/run/topology.json"
CONTROL_HOST = "127.0.0.1"
CONTROL_PORT = 6001
LOG_FILE = "/tmp/ndtm_events.log"


def log_event(tag, data):
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps({"ts_ns": time.time_ns(), "tag": tag, **data}) + "\n")


@dataclass
class Edge:
    delay_ms: float
    loss_pct: float
    bw_mbps: float
    up: bool


def load_topo_graph():
    with open(TOPO_FILE) as f:
        t = json.load(f)
    g = defaultdict(list)
    for a, b in t["links"]:
        g[a].append(b)
        g[b].append(a)
    return g


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
    return path


def control_call(ev):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((CONTROL_HOST, CONTROL_PORT))
    s.sendall((json.dumps(ev) + "\n").encode())
    data = b""
    while b"\n" not in data:
        data += s.recv(4096)
    s.close()
    return json.loads(data.split(b"\n")[0].decode())


# ---------------- TRANSPORT ----------------

class UdpReceiver:
    def __init__(self, ip, port):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.s.bind(("0.0.0.0", 0))
        self.s.sendto(b"HELLO_NDT", (ip, port))

    def recv(self):
        return self.s.recvfrom(65535)[0]


class TcpReceiver:
    def __init__(self, ip, port):
        self.s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.s.connect((ip, port))
        self.s.sendall(b"NDT")

    def recv(self):
        ln = int.from_bytes(self.s.recv(4), "big")
        return self.s.recv(ln)


# ---------------- MAIN ----------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proto", choices=["udp", "tcp"], required=True)
    ap.add_argument("--relay-ip", required=True)
    ap.add_argument("--relay-port", type=int, default=7001)
    args = ap.parse_args()

    rx = UdpReceiver(args.relay_ip, args.relay_port) if args.proto == "udp" \
        else TcpReceiver(args.relay_ip, args.relay_port)

    g = load_topo_graph()
    print("[NDT] Ready")

    while True:
        data = rx.recv()
        t_rx_ns = time.time_ns()
        ev = json.loads(data.decode())

        event_id = ev["event_id"]
        t_create_ns = ev["t_create_ns"]
        p = ev["payload"]

        log_event("NDT_EVENT_RX", {
            "event_id": event_id,
            "t_create_ns": t_create_ns,
            "t_rx_ns": t_rx_ns,
            "size_bytes": len(data)
        })

        e = Edge(p["delay_ms"], p["loss_pct"], p["bw_mbps"], p["up"])
        phys = path_only_switches_middle(g, p["a"], p["b"])

        for x, y in phys:
            control_call({
                "type": "LINK_SET",
                "payload": {"a": x, "b": y, **p}
            })

        t_apply_ns = time.time_ns()
        log_event("NDT_EVENT_APPLIED", {
            "event_id": event_id,
            "t_create_ns": t_create_ns,
            "t_rx_ns": t_rx_ns,
            "t_apply_ns": t_apply_ns,
            "segments": len(phys)
        })


if __name__ == "__main__":
    main()
