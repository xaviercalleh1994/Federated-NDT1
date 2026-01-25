#!/usr/bin/env python3
import argparse
import pathlib
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# -----------------------------
# Utils
# -----------------------------
def ci95(x):
    x = np.asarray(x)
    if len(x) < 2:
        return 0.0
    return 1.96 * np.std(x, ddof=1) / np.sqrt(len(x))

def load_csv(path):
    df = pd.read_csv(path)
    required = ["cpu_percent", "rss_mb", "tx_kbps", "rx_kbps"]
    for c in required:
        if c not in df.columns:
            raise SystemExit(f"Missing column '{c}' in {path}")
    return df

def summarize(df):
    return {
        "cpu_mean": df["cpu_percent"].mean(),
        "cpu_ci": ci95(df["cpu_percent"]),
        "rss_mean": df["rss_mb"].mean(),
        "rss_ci": ci95(df["rss_mb"]),
        "tx_mean": df["tx_kbps"].mean(),
        "tx_ci": ci95(df["tx_kbps"]),
        "rx_mean": df["rx_kbps"].mean(),
        "rx_ci": ci95(df["rx_kbps"]),
    }

# -----------------------------
# Plot helpers
# -----------------------------
def point_ci(ax, x, mean, ci, label):
    ax.errorbar(
        x, mean, yerr=ci,
        fmt="o", capsize=6, linewidth=2, label=label
    )

def finish(ax, ylabel, title):
    ax.set_ylabel(ylabel)
    ax.set_title(title)
    ax.grid(True, axis="y", linestyle="--", alpha=0.5)

# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tcp", required=True, help="TCP CSV")
    ap.add_argument("--udp", required=True, help="UDP CSV")
    ap.add_argument("--outdir", default="resources_out")
    args = ap.parse_args()

    out = pathlib.Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)

    tcp = summarize(load_csv(args.tcp))
    udp = summarize(load_csv(args.udp))

    labels = ["TCP", "UDP"]
    x = np.arange(2)

    # ---- CPU ----
    fig, ax = plt.subplots(figsize=(6,4))
    point_ci(ax, x[0], tcp["cpu_mean"], tcp["cpu_ci"], "TCP")
    point_ci(ax, x[1], udp["cpu_mean"], udp["cpu_ci"], "UDP")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    finish(ax, "CPU usage (%)", "CPU usage (mean ± CI95)")
    fig.tight_layout()
    fig.savefig(out / "cpu_mean_ci95.png", dpi=200)
    plt.close(fig)

    # ---- Memory ----
    fig, ax = plt.subplots(figsize=(6,4))
    point_ci(ax, x[0], tcp["rss_mean"], tcp["rss_ci"], "TCP")
    point_ci(ax, x[1], udp["rss_mean"], udp["rss_ci"], "UDP")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    finish(ax, "RSS (MB)", "Memory usage (mean ± CI95)")
    fig.tight_layout()
    fig.savefig(out / "rss_mean_ci95.png", dpi=200)
    plt.close(fig)

    # ---- TX ----
    fig, ax = plt.subplots(figsize=(6,4))
    point_ci(ax, x[0], tcp["tx_mean"], tcp["tx_ci"], "TCP")
    point_ci(ax, x[1], udp["tx_mean"], udp["tx_ci"], "UDP")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    finish(ax, "TX rate (kbps)", "TX synchronization traffic (mean ± CI95)")
    fig.tight_layout()
    fig.savefig(out / "tx_kbps_mean_ci95.png", dpi=200)
    plt.close(fig)

    # ---- RX ----
    fig, ax = plt.subplots(figsize=(6,4))
    point_ci(ax, x[0], tcp["rx_mean"], tcp["rx_ci"], "TCP")
    point_ci(ax, x[1], udp["rx_mean"], udp["rx_ci"], "UDP")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    finish(ax, "RX rate (kbps)", "RX synchronization traffic (mean ± CI95)")
    fig.tight_layout()
    fig.savefig(out / "rx_kbps_mean_ci95.png", dpi=200)
    plt.close(fig)

    # ---- CSV summary ----
    pd.DataFrame.from_dict(
        {"TCP": tcp, "UDP": udp}, orient="index"
    ).to_csv(out / "resources_summary.csv")

    print("✔ Plots and summary written to:", out)

if __name__ == "__main__":
    main()
