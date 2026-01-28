#!/usr/bin/env python3
import json
import time
import socket
import argparse
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional, Any
from collections import defaultdict, deque
import threading
from queue import PriorityQueue

TOPO_FILE = "/run/topology.json"
CONTROL_HOST = "127.0.0.1"
CONTROL_PORT = 6001

# Default log file for NDTM (can be overridden with --log-file)
LOG_FILE_DEFAULT = "/home/ubuntu/ndtm_events.log"

# Runtime context appended to every log line
RUN_CTX: Dict[str, object] = {}

# -------------------------
# Logging (unchanged style)
# -------------------------
def log_event(tag: str, data: dict):
    entry = {"ts_ns": time.time_ns(), "tag": tag, **RUN_CTX, **data}
    with open(RUN_CTX.get("log_file", LOG_FILE_DEFAULT), "a") as f:
        f.write(json.dumps(entry) + "\n")


# -------------------------
# TCP helper (unchanged)
# -------------------------
def recv_exact(sock: socket.socket, n: int):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# -------------------------
# Data model (unchanged)
# -------------------------
@dataclass
class Edge:
    delay_ms: float
    loss_pct: float
    bw_mbps: float
    up: bool


def is_switch(n: str) -> bool:
    return n.startswith("s")


def load_topo_graph() -> Dict[str, List[str]]:
    with open(TOPO_FILE) as f:
        t = json.load(f)
    links = [(a, b) for a, b in t["links"]]

    g = defaultdict(list)
    for a, b in links:
        g[a].append(b)
        g[b].append(a)
    return g


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


# -------------------------
# Receivers (same API)
# -------------------------
class UdpReceiver:
    """
    For the 1:1 case you used "bind to relay_port".
    Keep it identical so you don't break your current runs.
    Note: In real relay/NAT scenarios this is weird, but we preserve behavior.
    """
    def __init__(self, relay_ip: str, relay_port: int, keepalive_s: int = 5):
        self.relay = (relay_ip, relay_port)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.bind(("0.0.0.0", relay_port))
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


# -------------------------
# Federated control helpers
# -------------------------
def ev_key(ev: dict) -> Tuple[str, str]:
    p = ev.get("payload", {})
    a = str(p.get("a", ""))
    b = str(p.get("b", ""))
    return (a, b) if a <= b else (b, a)

def ev_priority(ev: dict) -> int:
    # lower => higher priority
    t = ev.get("type", "NA")
    if t == "LOGICAL_LINK_SET":
        up = bool(ev.get("payload", {}).get("up", True))
        return 0 if not up else 10  # failures first
    if t == "EXEC_CMD":
        return 50
    return 90

class TokenBucket:
    def __init__(self, rate_eps: float, burst: int):
        self.rate_eps = max(0.0, rate_eps)
        self.capacity = max(1, int(burst))
        self.tokens = float(self.capacity)
        self.last_s = time.time()
        self.lock = threading.Lock()

    def allow(self) -> bool:
        if self.rate_eps <= 0:
            return True  # unlimited
        with self.lock:
            now = time.time()
            dt = now - self.last_s
            self.last_s = now
            self.tokens = min(self.capacity, self.tokens + dt * self.rate_eps)
            if self.tokens >= 1.0:
                self.tokens -= 1.0
                return True
            return False

class DownstreamSender:
    def send_subset(self, payload: bytes, idxs: List[int]) -> int:
        raise NotImplementedError
    def close(self):
        pass

class UdpDownstreamSender(DownstreamSender):
    def __init__(self, dst_ip: str, base_port: int, fanout: int):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.targets = [(dst_ip, base_port + i) for i in range(fanout)]

    def send_subset(self, payload: bytes, idxs: List[int]) -> int:
        ok = 0
        for i in idxs:
            try:
                self.sock.sendto(payload, self.targets[i])
                ok += 1
            except Exception:
                pass
        return ok

    def close(self):
        try:
            self.sock.close()
        except Exception:
            pass

class TcpDownstreamSender(DownstreamSender):
    """
    Persistent TCP conns to each downstream port. Frames payload the same way as your ONM TcpSender.
    If a port isn't listening, it will be skipped and retried on next send.
    """
    def __init__(self, dst_ip: str, base_port: int, fanout: int, connect_timeout_s: float = 1.5):
        self.dst_ip = dst_ip
        self.ports = [base_port + i for i in range(fanout)]
        self.socks: Dict[int, socket.socket] = {}
        self.lock = threading.Lock()
        self.connect_timeout_s = connect_timeout_s

    def _connect(self, port: int) -> Optional[socket.socket]:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(self.connect_timeout_s)
        try:
            s.connect((self.dst_ip, port))
            s.settimeout(None)
            return s
        except Exception:
            try:
                s.close()
            except Exception:
                pass
            return None

    def _get(self, port: int) -> Optional[socket.socket]:
        with self.lock:
            s = self.socks.get(port)
            if s:
                return s
        s = self._connect(port)
        if s:
            with self.lock:
                self.socks[port] = s
        return s

    def send_subset(self, payload: bytes, idxs: List[int]) -> int:
        ok = 0
        for i in idxs:
            port = self.ports[i]
            s = self._get(port)
            if not s:
                continue
            try:
                s.sendall(len(payload).to_bytes(4, "big") + payload)
                ok += 1
            except Exception:
                try:
                    s.close()
                except Exception:
                    pass
                with self.lock:
                    if port in self.socks:
                        del self.socks[port]
        return ok

    def close(self):
        with self.lock:
            for s in self.socks.values():
                try:
                    s.close()
                except Exception:
                    pass
            self.socks.clear()

def make_groups(n: int, group_size: int) -> List[List[int]]:
    gs = max(1, int(group_size))
    return [list(range(i, min(i + gs, n))) for i in range(0, n, gs)]


class FederatedController:
    """
    Receives events (same as 1:1), applies control:
      - priority scheduling (down > up > exec > other)
      - dedup by (a,b) for LOGICAL_LINK_SET
      - admission control (token bucket) for non-critical
      - rolling update for non-critical; broadcast for critical
    """
    def __init__(self, sender: DownstreamSender, fanout: int,
                 rate_eps: float, burst: int,
                 group_size: int, wave_delay_s: float,
                 enable_dedup: bool = True):
        self.sender = sender
        self.fanout = fanout
        self.bucket = TokenBucket(rate_eps, burst)
        self.groups = make_groups(fanout, group_size)
        self.wave_delay_s = max(0.0, float(wave_delay_s))
        self.enable_dedup = enable_dedup

        self.q: "PriorityQueue[Tuple[int,int,dict]]" = PriorityQueue()
        self.seq = 0

        self.latest: Dict[Tuple[str, str], dict] = {}
        self.l_lock = threading.Lock()

        t = threading.Thread(target=self._loop, daemon=True)
        t.start()

    def submit(self, ev: dict, t_rx_ns: int, raw_size: int):
        pr = ev_priority(ev)
        self.seq += 1
        meta = {"t_rx_ns": t_rx_ns, "size_bytes": raw_size}

        # keep old tags style but with DTM_* (new)
        log_event("DTM_INGEST", {
            "type": ev.get("type", "NA"),
            "event_id": ev.get("event_id", "NA"),
            "priority": pr,
            **meta
        })

        if self.enable_dedup and ev.get("type") == "LOGICAL_LINK_SET":
            k = ev_key(ev)
            with self.l_lock:
                self.latest[k] = ev
            log_event("DTM_DEDUP_STORE", {"event_id": ev.get("event_id", "NA"), "key": list(k)})
            self.q.put((pr, self.seq, {"_dedup_key": k, "_meta": meta}))
        else:
            self.q.put((pr, self.seq, {"_event": ev, "_meta": meta}))

    def _is_critical(self, ev: dict) -> bool:
        if ev.get("type") != "LOGICAL_LINK_SET":
            return False
        up = bool(ev.get("payload", {}).get("up", True))
        return (not up)

    def _resolve(self, item: dict) -> Optional[Tuple[dict, dict]]:
        if "_dedup_key" in item:
            k = tuple(item["_dedup_key"])
            with self.l_lock:
                ev = self.latest.pop(k, None)
            if ev is None:
                return None
            return ev, item.get("_meta", {})
        return item["_event"], item.get("_meta", {})

    def _loop(self):
        while True:
            pr, seq, item = self.q.get()
            resolved = self._resolve(item)
            if not resolved:
                continue
            ev, meta = resolved
            t_deq_ns = time.time_ns()

            # Add orchestration metadata (doesn't break receivers; dummies ignore)
            ev_out = dict(ev)
            ev_out["dtm"] = {
                "mode": "federated",
                "policy": "prio+dedup+tokenbucket+rolling",
                "priority": pr,
                "seq": seq,
                "t_deq_ns": t_deq_ns
            }
            payload = json.dumps(ev_out).encode()

            critical = self._is_critical(ev)
            if not critical:
                if not self.bucket.allow():
                    log_event("DTM_DROP", {
                        "event_id": ev.get("event_id", "NA"),
                        "type": ev.get("type", "NA"),
                        "reason": "rate_limit",
                        "priority": pr,
                        **meta
                    })
                    continue

            # Dispatch
            if critical:
                idxs = list(range(self.fanout))
                t_tx_start_ns = time.time_ns()
                sent = self.sender.send_subset(payload, idxs)
                t_tx_end_ns = time.time_ns()

                log_event("DTM_BROADCAST", {
                    "event_id": ev.get("event_id", "NA"),
                    "type": ev.get("type", "NA"),
                    "priority": pr,
                    "sent_ok": sent,
                    "fanout": self.fanout,
                    "t_rx_ns": meta.get("t_rx_ns"),
                    "t_deq_ns": t_deq_ns,
                    "t_tx_start_ns": t_tx_start_ns,
                    "t_tx_end_ns": t_tx_end_ns,
                    "dispatch_ns": (t_tx_end_ns - t_tx_start_ns),
                    "size_bytes": len(payload)
                })
            else:
                # Rolling update across groups
                for wave_id, grp in enumerate(self.groups):
                    t_tx_start_ns = time.time_ns()
                    sent = self.sender.send_subset(payload, grp)
                    t_tx_end_ns = time.time_ns()

                    log_event("DTM_ROLLING_SEND", {
                        "event_id": ev.get("event_id", "NA"),
                        "type": ev.get("type", "NA"),
                        "priority": pr,
                        "wave_id": wave_id,
                        "targets": grp,
                        "sent_ok": sent,
                        "t_rx_ns": meta.get("t_rx_ns"),
                        "t_deq_ns": t_deq_ns,
                        "t_tx_start_ns": t_tx_start_ns,
                        "t_tx_end_ns": t_tx_end_ns,
                        "dispatch_ns": (t_tx_end_ns - t_tx_start_ns),
                        "size_bytes": len(payload)
                    })
                    if self.wave_delay_s > 0:
                        time.sleep(self.wave_delay_s)

            log_event("DTM_EVENT_DONE", {
                "event_id": ev.get("event_id", "NA"),
                "type": ev.get("type", "NA"),
                "priority": pr,
                "t_rx_ns": meta.get("t_rx_ns"),
                "t_done_ns": time.time_ns()
            })


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proto", choices=["udp", "tcp"], required=True)
    ap.add_argument("--relay-ip", required=True)
    ap.add_argument("--relay-port", type=int, default=7001)
    ap.add_argument("--relay-kind", choices=["router", "vps"], required=True,
                    help="Type of relay used: local router VM or remote VPS server")
    ap.add_argument("--log-file", default=LOG_FILE_DEFAULT,
                    help="Path to NDT manager log file (JSONL)")

    # New: operation mode
    ap.add_argument("--mode", choices=["1:1", "federated"], default="1:1",
                    help="1:1 = apply locally (original behavior). federated = orchestrate and fan-out.")
    # New: federated downstream settings
    ap.add_argument("--dst-ip", default="127.0.0.1",
                    help="Downstream IP where NDT dummies or NDTs listen (federated mode)")
    ap.add_argument("--base-port", type=int, default=8001,
                    help="Downstream base port (federated mode)")
    ap.add_argument("--fanout", type=int, default=0,
                    help="Number of downstream NDTs/ports (federated mode, required >0)")
    ap.add_argument("--group-size", type=int, default=5,
                    help="Rolling update group size (federated mode)")
    ap.add_argument("--wave-delay-s", type=float, default=0.2,
                    help="Delay between rolling waves (federated mode)")
    ap.add_argument("--rate-eps", type=float, default=0.0,
                    help="Admission control rate (events/sec). 0=unlimited (federated mode)")
    ap.add_argument("--burst", type=int, default=5,
                    help="Token bucket burst size (federated mode)")
    ap.add_argument("--no-dedup", action="store_true",
                    help="Disable dedup for LOGICAL_LINK_SET by (a,b)")

    args = ap.parse_args()

    global RUN_CTX
    RUN_CTX = {
        "side": "NDT",
        "proto": args.proto,
        "relay_kind": args.relay_kind,
        "relay_ip": args.relay_ip,
        "relay_port": args.relay_port,
        "mode": args.mode,
        "log_file": args.log_file
    }

    # Load graph (used only in 1:1 mode)
    g = load_topo_graph()

    # Receiver (unchanged prints)
    if args.proto == "udp":
        rx = UdpReceiver(args.relay_ip, args.relay_port)
        print(f"[NDT] Ready (UDP), listening on {args.relay_port}")
    else:
        rx = TcpReceiver(args.relay_ip, args.relay_port)
        print("[NDT] Ready (TCP)")

    # Federated controller init (only if mode=federated)
    controller: Optional[FederatedController] = None
    if args.mode == "federated":
        if args.fanout <= 0:
            raise SystemExit("ERROR: --mode federated requires --fanout > 0")
        RUN_CTX.update({
            "dst_ip": args.dst_ip,
            "base_port": args.base_port,
            "fanout": args.fanout,
            "group_size": args.group_size,
            "wave_delay_s": args.wave_delay_s,
            "rate_eps": args.rate_eps,
            "burst": args.burst,
            "dedup": (not args.no_dedup)
        })

        if args.proto == "udp":
            ds = UdpDownstreamSender(args.dst_ip, args.base_port, args.fanout)
        else:
            ds = TcpDownstreamSender(args.dst_ip, args.base_port, args.fanout)

        controller = FederatedController(
            sender=ds,
            fanout=args.fanout,
            rate_eps=args.rate_eps,
            burst=args.burst,
            group_size=args.group_size,
            wave_delay_s=args.wave_delay_s,
            enable_dedup=(not args.no_dedup),
        )

        print(f"[NDT] Federated control enabled: fanout={args.fanout} "
              f"base_port={args.base_port} group_size={args.group_size} "
              f"rate_eps={args.rate_eps} burst={args.burst} wave_delay_s={args.wave_delay_s}",
              flush=True)

    # Main loop (keeps original log tags & prints for 1:1)
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

            # KEEP PRINT (unchanged)
            print(f"[NDT] EVENT RECEIVED {event_id}", flush=True)

            # KEEP LOG TAG (unchanged)
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

            if args.mode == "1:1":
                segments = apply_logical_link(g, p["a"], p["b"], e)
                t_apply_ns = time.time_ns()

                # KEEP LOG TAG (unchanged)
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

                # KEEP PRINT (unchanged)
                print(f"[NDT] EVENT APPLIED {event_id}", flush=True)

            else:
                # Federated: do NOT apply locally; orchestrate + fan-out with control
                assert controller is not None
                controller.submit(ev, t_rx_ns=t_rx_ns, raw_size=len(data))

                # Keep a "print" that does not remove existing ones, but clarifies mode
                print(f"[NDT] EVENT QUEUED (FED) {event_id}", flush=True)

                # Optional extra log (new tag) but does not remove old ones
                log_event("DTM_FED_EVENT_QUEUED", {
                    "event_id": event_id,
                    "t_create_ns": t_create_ns,
                    "t_rx_ns": t_rx_ns,
                    "size_bytes": len(data),
                    "fanout": args.fanout
                })

            continue

        if t == "EXEC_CMD":
            if args.mode == "1:1":
                resp = control_call(ev)
                log_event("NDT_EXEC_CMD", {"ok": resp.get("ok", False)})
            else:
                assert controller is not None
                controller.submit(ev, t_rx_ns=t_rx_ns, raw_size=len(data))
                log_event("DTM_FED_EXEC_QUEUED", {"ok": True, "t_rx_ns": t_rx_ns})
            continue

        log_event("NDT_UNKNOWN_EVENT", {"type": t, "size_bytes": len(data)})


if __name__ == "__main__":
    main()
