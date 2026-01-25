#!/usr/bin/env python3
import json
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# -------------------------
# Helpers
# -------------------------
def load_onm_log(path):
    rows = []
    with open(path) as f:
        for line in f:
            try:
                r = json.loads(line)
                if "fanout_send_ns" in r:
                    rows.append(r)
            except:
                pass
    return pd.DataFrame(rows)

def ci95(x):
    if len(x) < 2:
        return 0.0
    return 1.96 * np.std(x, ddof=1) / np.sqrt(len(x))

# -------------------------
# Plots
# -------------------------
def plot_fanout_latency(df, out):
    plt.figure()
    for proto in ["tcp", "udp"]:
        d = df[df.proto == proto]
        g = d.groupby("fanout")["fanout_send_ns"].mean() / 1e6
        plt.plot(g.index, g.values, marker="o", label=proto.upper())

    plt.xlabel("Fan-out (number of federated domains)")
    plt.ylabel("Fan-out send latency (ms)")
    plt.title("ONM fan-out latency vs federation size")
    plt.grid(True, ls="--", alpha=0.5)
    plt.legend()
    plt.savefig(out / "fanout_latency_vs_fanout.png", dpi=200)
    plt.close()

def plot_cost_per_domain(df, out):
    plt.figure()
    for proto in ["tcp", "udp"]:
        d = df[df.proto == proto].copy()
        d["cost_ms"] = (d["fanout_send_ns"] / d["fanout"]) / 1e6
        g = d.groupby("fanout")["cost_ms"].mean()
        plt.plot(g.index, g.values, marker="o", label=proto.upper())

    plt.xlabel("Fan-out")
    plt.ylabel("Cost per domain (ms)")
    plt.title("Per-domain synchronization cost")
    plt.grid(True, ls="--", alpha=0.5)
    plt.legend()
    plt.savefig(out / "cost_per_domain.png", dpi=200)
    plt.close()

def plot_fanout_cdf(df, out):
    plt.figure()
    for proto in ["tcp", "udp"]:
        vals = df[df.proto == proto]["fanout_send_ns"].values / 1e6
        vals = np.sort(vals)
        y = np.arange(1, len(vals) + 1) / len(vals)
        plt.plot(vals, y, label=proto.upper())

    plt.xlabel("Fan-out send latency (ms)")
    plt.ylabel("CDF")
    plt.title("CDF of fan-out send latency")
    plt.grid(True, ls="--", alpha=0.5)
    plt.legend()
    plt.savefig(out / "fanout_latency_cdf.png", dpi=200)
    plt.close()

def plot_msg_size(df, out):
    plt.figure()
    for proto in ["tcp", "udp"]:
        vals = df[df.proto == proto]["size_bytes"]
        plt.bar(proto.upper(), vals.mean(),
                yerr=ci95(vals),
                capsize=6)

    plt.ylabel("Bytes")
    plt.title("Synchronization message size (mean ± CI95)")
    plt.grid(True, axis="y", ls="--", alpha=0.5)
    plt.savefig(out / "sync_msg_size.png", dpi=200)
    plt.close()

def plot_relay_throughput(csv_path, out, label):
    df = pd.read_csv(csv_path)

    df["pkts_per_s"] = df["pkts_delta"]
    df["kbps"] = (df["bytes_delta"] * 8) / 1000

    fig, ax = plt.subplots(2, 1, sharex=True, figsize=(7,6))

    ax[0].plot(df.ts_epoch_s, df.pkts_per_s)
    ax[0].set_ylabel("Packets/s")
    ax[0].grid(True, ls="--", alpha=0.5)

    ax[1].plot(df.ts_epoch_s, df.kbps)
    ax[1].set_ylabel("kbps")
    ax[1].set_xlabel("Time (s)")
    ax[1].grid(True, ls="--", alpha=0.5)

    fig.suptitle(f"Relay load over time ({label})")
    plt.tight_layout()
    plt.savefig(out / f"relay_load_{label}.png", dpi=200)
    plt.close()

# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--onm-log", required=True)
    ap.add_argument("--tcp-csv", required=False)
    ap.add_argument("--udp-csv", required=False)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)

    df = load_onm_log(args.onm_log)

    plot_fanout_latency(df, out)
    plot_cost_per_domain(df, out)
    plot_fanout_cdf(df, out)
    plot_msg_size(df, out)

    if args.tcp_csv:
        plot_relay_throughput(args.tcp_csv, out, "tcp")

    if args.udp_csv:
        plot_relay_throughput(args.udp_csv, out, "udp")

    print(f"[OK] Federated plots written to {out}")

if __name__ == "__main__":
    main()
