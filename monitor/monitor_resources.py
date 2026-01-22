#!/usr/bin/env python3
import psutil
import time
import csv
import argparse
import sys

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--pid", type=int, required=True,
                    help="PID to monitor (ONM or NDTM)")
    ap.add_argument("--iface", required=True,
                    help="Network interface to monitor (e.g., eth0, enp0s8)")
    ap.add_argument("--out", required=True,
                    help="Output CSV file")
    ap.add_argument("--interval", type=float, default=1.0,
                    help="Sampling interval in seconds (e.g., 0.2, 1.0)")
    args = ap.parse_args()

    try:
        proc = psutil.Process(args.pid)
    except psutil.NoSuchProcess:
        print("PID not found")
        sys.exit(1)

    # Prime CPU measurement
    proc.cpu_percent(interval=None)

    net = psutil.net_io_counters(pernic=True)
    if args.iface not in net:
        print(f"Interface not found: {args.iface}")
        sys.exit(1)

    prev_net = net[args.iface]

    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "ts_epoch_s",
            "cpu_percent",
            "rss_mb",
            "tx_kbps",
            "rx_kbps"
        ])

        while True:
            time.sleep(args.interval)

            ts = time.time()
            cpu = proc.cpu_percent(interval=None)
            rss_mb = proc.memory_info().rss / (1024 * 1024)

            net = psutil.net_io_counters(pernic=True)[args.iface]
            tx_kbps = ((net.bytes_sent - prev_net.bytes_sent) * 8.0 / 1000.0) / args.interval
            rx_kbps = ((net.bytes_recv - prev_net.bytes_recv) * 8.0 / 1000.0) / args.interval
            prev_net = net

            w.writerow([ts, cpu, rss_mb, tx_kbps, rx_kbps])
            f.flush()

if __name__ == "__main__":
    main()
