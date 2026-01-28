#!/usr/bin/env python3
import argparse
import re
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# -------------------------
# Utils
# -------------------------
def ci95(x):
    if len(x) < 2:
        return 0.0
    return 1.96 * np.std(x, ddof=1) / np.sqrt(len(x))

def parse_proto_fanout(name):
    name = name.lower()
    proto = "udp" if "udp" in name else "tcp" if "tcp" in name else None

    # 1:1
    if "1to1" in name:
        return proto, 1

    m = re.search(r"f(\d+)", name)
    if m:
        return proto, int(m.group(1))

    return proto, None

# -------------------------
# Load CSVs
# -------------------------
def load_all_csvs(input_dir):
    rows = []

    for p in Path(input_dir).glob("*.csv"):
        proto, fanout = parse_proto_fanout(p.name)
        if proto is None or fanout is None:
            continue

        df = pd.read_csv(p)

        if not {"tx_kbps", "cpu_percent", "rss_mb"}.issubset(df.columns):
            continue

        rows.append({
            "proto": proto,
            "fanout": fanout,
            "tx_kbps_mean": df.tx_kbps.mean(),
            "tx_kbps_ci95": ci95(df.tx_kbps),
            "cpu_mean": df.cpu_percent.mean(),
            "rss_mb_mean": df.rss_mb.mean()
        })

    return pd.DataFrame(rows)

# -------------------------
# Plot helpers
# -------------------------
def plot_metric(df, metric, ylabel, out, fname):
    plt.figure()
    for proto in ["tcp", "udp"]:
        d = df[df.proto == proto].sort_values("fanout")
        if d.empty:
            continue
        plt.errorbar(
            d.fanout,
            d[metric],
            yerr=d[f"{metric}_ci95"] if f"{metric}_ci95" in d else None,
            marker="o",
            label=proto.upper()
        )

    plt.xlabel("Fan-out (number of federated domains)")
    plt.ylabel(ylabel)
    plt.grid(True, ls="--", alpha=0.5)
    plt.legend()
    plt.tight_layout()
    plt.savefig(out / fname, dpi=200)
    plt.close()

# -------------------------
# Main
# -------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--link-capacity-mbps", type=float, default=100)
    args = ap.parse_args()

    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)

    df = load_all_csvs(args.input_dir)

    if df.empty:
        raise RuntimeError("No valid CSVs found")

    # ---- normalize vs 1:1 per protocol
    df["tx_norm_vs_1to1"] = np.nan
    for proto in ["tcp", "udp"]:
        base = df[(df.proto == proto) & (df.fanout == 1)]
        if not base.empty:
            base_tx = base.tx_kbps_mean.iloc[0]
            df.loc[df.proto == proto, "tx_norm_vs_1to1"] = (
                df.loc[df.proto == proto, "tx_kbps_mean"] / base_tx
            )

    # ---- Save summary
    df.sort_values(["proto", "fanout"]).to_csv(
        out / "federated_resource_summary.csv", index=False
    )

    # ---- PLOTS (NOW THEY ALWAYS APPEAR)
    plot_metric(
        df,
        "tx_kbps_mean",
        "TX rate (kbps)",
        out,
        "tx_rate_vs_fanout.png"
    )

    plot_metric(
        df,
        "cpu_mean",
        "CPU usage (%)",
        out,
        "cpu_vs_fanout.png"
    )

    plot_metric(
        df,
        "rss_mb_mean",
        "RSS memory (MB)",
        out,
        "rss_vs_fanout.png"
    )

    if df.tx_norm_vs_1to1.notna().any():
        plt.figure()
        for proto in ["tcp", "udp"]:
            d = df[(df.proto == proto) & df.tx_norm_vs_1to1.notna()]
            plt.plot(d.fanout, d.tx_norm_vs_1to1, marker="o", label=proto.upper())
        plt.xlabel("Fan-out")
        plt.ylabel("TX normalized vs 1:1")
        plt.grid(True, ls="--", alpha=0.5)
        plt.legend()
        plt.tight_layout()
        plt.savefig(out / "tx_norm_vs_1to1.png", dpi=200)
        plt.close()

    print(f"[OK] Plots and CSV written to {out}")

if __name__ == "__main__":
    main()
