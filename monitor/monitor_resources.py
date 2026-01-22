#!/usr/bin/env python3
import argparse
import pathlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

# -----------------------------
# Helpers
# -----------------------------
def p95(x):
    return float(np.percentile(x, 95))

def normalize_time(df):
    t0 = df["ts_epoch_s"].iloc[0]
    df = df.copy()
    df["t_rel_s"] = df["ts_epoch_s"] - t0
    return df

# -----------------------------
# 1:1 ANALYSIS
# -----------------------------
def analyze_1to1(csv_path, outdir):
    df = pd.read_csv(csv_path)
    df = normalize_time(df)

    metrics = {
        "cpu_mean": df.cpu_percent.mean(),
        "cpu_p95": p95(df.cpu_percent),
        "rss_mb_mean": df.rss_mb.mean(),
        "rss_mb_p95": p95(df.rss_mb),
        "tx_kbps_mean": df.tx_kbps.mean(),
        "rx_kbps_mean": df.rx_kbps.mean(),
    }

    # Save metrics
    pd.DataFrame([metrics]).to_csv(outdir / "metrics_1to1.csv", index=False)

    # ---- CPU plot ----
    plt.figure(figsize=(8,4))
    plt.plot(df.t_rel_s, df.cpu_percent)
    plt.xlabel("time (s)")
    plt.ylabel("CPU (%)")
    plt.title("NDT CPU usage (1:1)")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(outdir / "fig_cpu_1to1.png", dpi=300)
    plt.close()

    # ---- Memory plot ----
    plt.figure(figsize=(8,4))
    plt.plot(df.t_rel_s, df.rss_mb)
    plt.xlabel("time (s)")
    plt.ylabel("RSS (MB)")
    plt.title("NDT Memory usage (1:1)")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(outdir / "fig_mem_1to1.png", dpi=300)
    plt.close()

    # ---- Bandwidth plot ----
    plt.figure(figsize=(8,4))
    plt.plot(df.t_rel_s, df.tx_kbps, label="TX")
    plt.plot(df.t_rel_s, df.rx_kbps, label="RX")
    plt.xlabel("time (s)")
    plt.ylabel("kbps")
    plt.title("Synchronization bandwidth (1:1)")
    plt.legend()
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(outdir / "fig_bw_1to1.png", dpi=300)
    plt.close()

    print("[OK] 1:1 analysis completed")


# -----------------------------
# FEDERATED ANALYSIS
# -----------------------------
def analyze_federated(csv_paths, outdir, link_capacity_mbps=None):
    rows = []

    for csv in csv_paths:
        df = pd.read_csv(csv)
        fanout = int("".join(filter(str.isdigit, csv.stem)))
        rows.append({
            "fanout": fanout,
            "tx_kbps_mean": df.tx_kbps.mean(),
            "tx_kbps_p95": p95(df.tx_kbps)
        })

    m = pd.DataFrame(rows).sort_values("fanout")
    m.to_csv(outdir / "metrics_federated.csv", index=False)

    # Federation Overhead Ratio
    B1 = m[m.fanout == 1].tx_kbps_mean.iloc[0]
    m["FOR"] = m.tx_kbps_mean / B1

    if link_capacity_mbps:
        C = link_capacity_mbps * 1000
        m["link_util_pct"] = (m.tx_kbps_mean / C) * 100

    m.to_csv(outdir / "metrics_federated_with_for.csv", index=False)

    # ---- Bandwidth vs fanout ----
    plt.figure(figsize=(6,4))
    plt.plot(m.fanout, m.tx_kbps_mean, marker="o")
    plt.xlabel("Number of replicas (fanout)")
    plt.ylabel("TX kbps (mean)")
    plt.title("Synchronization bandwidth vs replicas")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(outdir / "fig_federated_bw.png", dpi=300)
    plt.close()

    # ---- FOR plot ----
    plt.figure(figsize=(6,4))
    plt.plot(m.fanout, m.FOR, marker="o")
    plt.xlabel("Number of replicas (fanout)")
    plt.ylabel("Federation Overhead Ratio")
    plt.title("Federation overhead scaling")
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.tight_layout()
    plt.savefig(outdir / "fig_federated_for.png", dpi=300)
    plt.close()

    print("[OK] Federated analysis completed")


# -----------------------------
# MAIN
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--mode", choices=["1to1", "federated"], required=True)
    ap.add_argument("--csv", nargs="+", required=True,
                    help="CSV(s) from monitor_resources.py")
    ap.add_argument("--outdir", default="out_monitor")
    ap.add_argument("--link-capacity-mbps", type=float, default=None,
                    help="Optional link capacity for utilization metric")
    args = ap.parse_args()

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    if args.mode == "1to1":
        if len(args.csv) != 1:
            raise SystemExit("1to1 mode expects exactly ONE CSV")
        analyze_1to1(pathlib.Path(args.csv[0]), outdir)

    else:
        csvs = [pathlib.Path(c) for c in args.csv]
        analyze_federated(csvs, outdir, args.link_capacity_mbps)


if __name__ == "__main__":
    main()
