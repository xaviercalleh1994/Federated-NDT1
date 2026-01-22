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

LOG_FILE_DEFAULT = "/home/ubuntu/onm_fanout_events.log"

CONTROL_HOST = "127.0.0.1"
CONTROL_PORT = 6001

RUN_CTX: Dict[str, object] = {}

def log_event(tag: str, data: dict):
    entry = {"ts_ns": time.time_ns(), "tag": tag, **RUN_CTX, **data}
    with open(RUN_CTX.get("log_file", LOG_FILE_DEFAULT), "a") as f:
        f.write(json.dumps(entry) + "\n")

@dataclass
class Edge:
    delay_ms: float
    loss_pct: float
    bw_mbps: float
    up: bool

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
        print("\nLogic Nodes:")
        for n, i in self.idx.items():
            print(f"  {i}: {n}")
        print("\nLOGICAL LINKS:")
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
                print(
                    f"  {a} <-> {b} | up={e.up} delay={e.delay_ms}ms "
                    f"loss={e.loss_pct}% bw={e.bw_mbps}Mbps"
                )

def is_switch(n: str) -> bool:
    return n.startswith("s")

def load_topo_graph() -> Tuple[List[str], Dict[str, List[str]]]:
    with open(TOPO_FILE) as f:
        t = json.load(f)
    g = defaultdict(list)
    for a, b in t["links"]:
        g[a].append(b)
        g[b].append(a)
    return t["nodes"], g

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

def apply_logical_edge_to_on(g, a: str, b: str, e: Edge) -> bool:
    phys_edges = path_only_switches_middle(g, a, b)
    if not phys_edges:
        print(f"[ON] No logical path between {a} and {b}")
        return False

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
            print("[ON] LINK_SET error:", resp)
            return False

    rr = control_call({
        "type": "ROUTE_REASSERT",
        "payload": {"routers": ["r1", "r2"], "gateway": "10.0.0.254"}
    })
    if not rr.get("ok", True):
        print("[ON] ROUTE_REASSERT warning:", rr)
    return True

class SyncSender:
    def send(self, data: bytes):
        raise NotImplementedError
    def close(self):
        pass

class UdpSender(SyncSender):
    def __init__(self, relay_ip: str, relay_port: int):
        self.addr = (relay_ip, relay_port)
        self.s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proto", choices=["udp", "tcp"], required=True)
    ap.add_argument("--relay-ip", required=True)
    ap.add_argument("--relay-kind", choices=["router", "vps"], required=True)
    ap.add_argument("--base-port", type=int, default=7001)
    ap.add_argument("--fanout", type=int, required=True, help="2,5,10,20 etc.")
    ap.add_argument("--log-file", default=LOG_FILE_DEFAULT)
    args = ap.parse_args()

    global RUN_CTX
    RUN_CTX = {
        "side": "ON",
        "proto": args.proto,
        "relay_kind": args.relay_kind,
        "relay_ip": args.relay_ip,
        "base_port": args.base_port,
        "fanout": args.fanout,
        "log_file": args.log_file
    }

    # Build N senders (ports: base_port ... base_port+fanout-1)
    senders: List[SyncSender] = []
    for i in range(args.fanout):
        port = args.base_port + i
        if args.proto == "udp":
            senders.append(UdpSender(args.relay_ip, port))
        else:
            senders.append(TcpSender(args.relay_ip, port))

    nodes, g = load_topo_graph()
    M = Matrix()

    print("[ON-FANOUT] Ready. Commands:")
    print("  show")
    print("  set A B up|down delay_ms loss_pct bw_mbps")
    print('  exec NODE "cmd ..."')
    print("  quit\n")

    while True:
        line = input("> ").strip()
        if not line:
            continue
        if line == "quit":
            break
        if line == "show":
            M.show()
            continue

        parts = line.split()
        cmd = parts[0].lower()

        if cmd == "set":
            if len(parts) != 7:
                print("Uso: set A B up|down delay_ms loss_pct bw_mbps")
                continue

            _, a, b, up_s, d, l, bw = parts
            up = up_s.lower() == "up"
            e = Edge(delay_ms=float(d), loss_pct=float(l), bw_mbps=float(bw), up=up)

            M.set(a, b, e)

            event_id = str(uuid.uuid4())
            t_create_ns = time.time_ns()

            log_event("ON_EVENT_CREATED", {
                "event_id": event_id,
                "a": a, "b": b,
                "params": asdict(e)
            })

            ok = apply_logical_edge_to_on(g, a, b, e)
            if not ok:
                print("[ON] Failed to apply locally")
                log_event("ON_EVENT_LOCAL_APPLY_FAIL", {"event_id": event_id, "a": a, "b": b})
                continue

            ev = {
                "type": "LOGICAL_LINK_SET",
                "event_id": event_id,
                "t_create_ns": t_create_ns,
                "payload": {"a": a, "b": b, **asdict(e)}
            }

            payload = json.dumps(ev).encode()

            # Fan-out send timing
            t_tx_start_ns = time.time_ns()
            for s in senders:
                s.send(payload)
            t_tx_end_ns = time.time_ns()

            log_event("ON_EVENT_FANOUT_SENT", {
                "event_id": event_id,
                "t_create_ns": t_create_ns,
                "t_tx_start_ns": t_tx_start_ns,
                "t_tx_end_ns": t_tx_end_ns,
                "fanout_send_ns": (t_tx_end_ns - t_tx_start_ns),
                "size_bytes": len(payload)
            })

            print("[ON-FANOUT] Event applied locally and sent to fanout ports")
            continue

        if cmd == "exec":
            if len(parts) < 3:
                print('Uso: exec NODE "cmd ..."')
                continue

            node = parts[1]
            shell_cmd = line.split(None, 2)[2].strip()
            if shell_cmd.startswith('"') and shell_cmd.endswith('"'):
                shell_cmd = shell_cmd[1:-1]

            ev = {"type": "EXEC_CMD", "payload": {"node": node, "cmd": shell_cmd}}

            resp = control_call(ev)
            print("[ON EXEC]", resp)

            payload = json.dumps(ev).encode()

            t_tx_start_ns = time.time_ns()
            for s in senders:
                s.send(payload)
            t_tx_end_ns = time.time_ns()

            log_event("ON_EXEC_FANOUT_SENT", {
                "node": node,
                "cmd": shell_cmd,
                "t_tx_start_ns": t_tx_start_ns,
                "t_tx_end_ns": t_tx_end_ns,
                "fanout_send_ns": (t_tx_end_ns - t_tx_start_ns),
                "size_bytes": len(payload)
            })
            continue

        print("Comando no reconocido.")

    for s in senders:
        try:
            s.close()
        except Exception:
            pass

if __name__ == "__main__":
    main()
