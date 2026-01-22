#!/usr/bin/env python3
import socket
import threading
import argparse
import time
import csv
from collections import defaultdict

stats = defaultdict(lambda: {"pkts": 0, "bytes": 0, "conns": 0})
lock = threading.Lock()

def udp_listener(port: int):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("0.0.0.0", port))
    print(f"[DUMMY][UDP] Listening on 0.0.0.0:{port}")
    while True:
        data, _ = s.recvfrom(65535)
        with lock:
            stats[port]["pkts"] += 1
            stats[port]["bytes"] += len(data)

def tcp_consume(conn: socket.socket, port: int):
    try:
        while True:
            data = conn.recv(4096)
            if not data:
                break
            with lock:
                stats[port]["pkts"] += 1
                stats[port]["bytes"] += len(data)
    finally:
        conn.close()

def tcp_listener(port: int):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", port))
    s.listen(50)
    print(f"[DUMMY][TCP] Listening on 0.0.0.0:{port}")
    while True:
        conn, _ = s.accept()
        with lock:
            stats[port]["conns"] += 1
        threading.Thread(target=tcp_consume, args=(conn, port), daemon=True).start()

def reporter(ports, report_s: float, csv_path: str | None):
    last = {p: {"pkts": 0, "bytes": 0, "conns": 0} for p in ports}

    csv_file = None
    csv_writer = None
    if csv_path:
        csv_file = open(csv_path, "w", newline="")
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(["ts_epoch_s", "port", "pkts_total", "bytes_total", "pkts_delta", "bytes_delta", "conns_total"])

    while True:
        time.sleep(report_s)
        ts = time.time()

        with lock:
            snap = {p: dict(stats[p]) for p in ports}

        total_pkts = sum(snap[p]["pkts"] for p in ports)
        total_bytes = sum(snap[p]["bytes"] for p in ports)
        total_conns = sum(snap[p]["conns"] for p in ports)

        print(f"[DUMMY] totals: pkts={total_pkts} bytes={total_bytes} conns={total_conns}")

        for p in ports:
            pkts_d = snap[p]["pkts"] - last[p]["pkts"]
            bytes_d = snap[p]["bytes"] - last[p]["bytes"]
            if csv_writer:
                csv_writer.writerow([ts, p, snap[p]["pkts"], snap[p]["bytes"], pkts_d, bytes_d, snap[p]["conns"]])
            last[p] = snap[p]

        if csv_file:
            csv_file.flush()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proto", choices=["udp", "tcp"], required=True)
    ap.add_argument("--base-port", type=int, default=7001)
    ap.add_argument("--fanout", type=int, required=True)
    ap.add_argument("--report-s", type=float, default=1.0)
    ap.add_argument("--csv", default=None)
    args = ap.parse_args()

    ports = [args.base_port + i for i in range(args.fanout)]

    for p in ports:
        if args.proto == "udp":
            threading.Thread(target=udp_listener, args=(p,), daemon=True).start()
        else:
            threading.Thread(target=tcp_listener, args=(p,), daemon=True).start()

    print(f"[DUMMY] Started {args.proto.upper()} receivers on ports {ports[0]}..{ports[-1]}")
    reporter(ports, args.report_s, args.csv)

if __name__ == "__main__":
    main()