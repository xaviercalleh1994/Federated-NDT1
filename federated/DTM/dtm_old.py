#!/usr/bin/env python3
import json
import time
import socket
import argparse
import threading
from queue import Queue
from typing import Dict, List

LOG_FILE = "/home/ubuntu/dtm_federated.log"

# ---------------- Logging ----------------
def log_event(tag, data):
    entry = {"ts_ns": time.time_ns(), "tag": tag, **data}
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")

# ---------------- Token bucket ----------------
class TokenBucket:
    def __init__(self, rate_eps, burst):
        self.rate = rate_eps
        self.capacity = burst
        self.tokens = burst
        self.ts = time.time()
        self.lock = threading.Lock()

    def allow(self):
        with self.lock:
            now = time.time()
            self.tokens += (now - self.ts) * self.rate
            self.tokens = min(self.capacity, self.tokens)
            self.ts = now
            if self.tokens >= 1:
                self.tokens -= 1
                return True
            return False

# ---------------- Fanout sender ----------------
class Fanout:
    def __init__(self, proto, ip, base_port, n):
        self.proto = proto
        self.targets = [(ip, base_port + i) for i in range(n)]
        self.socks = []

        if proto == "udp":
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        else:
            for ip, port in self.targets:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                try:
                    s.connect((ip, port))
                    self.socks.append(s)
                except:
                    pass

    def send_subset(self, payload: bytes, subset: List[int]):
        ok = 0
        if self.proto == "udp":
            for i in subset:
                self.sock.sendto(payload, self.targets[i])
                ok += 1
        else:
            for i in subset:
                try:
                    s = self.socks[i]
                    s.sendall(len(payload).to_bytes(4, "big") + payload)
                    ok += 1
                except:
                    pass
        return ok

# ---------------- Rolling scheduler ----------------
def rolling_groups(n, group_size):
    return [list(range(i, min(i + group_size, n)))
            for i in range(0, n, group_size)]

# ---------------- Main ----------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proto", choices=["udp", "tcp"], required=True)
    ap.add_argument("--listen-port", type=int, default=7001)
    ap.add_argument("--dst-ip", required=True)
    ap.add_argument("--base-port", type=int, default=8001)
    ap.add_argument("--fanout", type=int, required=True)
    ap.add_argument("--rate-eps", type=float, default=10)
    ap.add_argument("--burst", type=int, default=5)
    ap.add_argument("--group-size", type=int, default=5)
    args = ap.parse_args()

    bucket = TokenBucket(args.rate_eps, args.burst)
    fanout = Fanout(args.proto, args.dst_ip, args.base_port, args.fanout)
    groups = rolling_groups(args.fanout, args.group_size)

    # Receiver
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("0.0.0.0", args.listen_port))
    print(f"[DTM-FED] Listening on {args.listen_port}")

    while True:
        data, addr = s.recvfrom(65535)
        t_rx = time.time_ns()
        ev = json.loads(data.decode())

        etype = ev.get("type")
        payload = json.dumps(ev).encode()

        # ---- CLASSIFICATION ----
        critical = False
        if etype == "LOGICAL_LINK_SET":
            if not ev["payload"]["up"]:
                critical = True

        # ---- ADMISSION ----
        if not critical and not bucket.allow():
            log_event("DTM_DROP", {
                "event_id": ev.get("event_id"),
                "reason": "rate_limit"
            })
            continue

        # ---- DISPATCH ----
        if critical:
            sent = fanout.send_subset(payload, list(range(args.fanout)))
            log_event("DTM_BROADCAST", {
                "event_id": ev.get("event_id"),
                "sent": sent,
                "critical": True
            })
        else:
            for round_id, grp in enumerate(groups):
                sent = fanout.send_subset(payload, grp)
                log_event("DTM_ROLLING_SEND", {
                    "event_id": ev.get("event_id"),
                    "round": round_id,
                    "targets": grp,
                    "sent": sent
                })
                time.sleep(0.2)  # orchestration delay

        log_event("DTM_EVENT_DONE", {
            "event_id": ev.get("event_id"),
            "t_rx_ns": t_rx,
            "t_done_ns": time.time_ns()
        })

if __name__ == "__main__":
    main()
