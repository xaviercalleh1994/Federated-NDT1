#!/usr/bin/env python3
import json
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import re
from pathlib import Path

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

    if "1to1" in name or "f1" in name:
        return proto, 1

    m = re.search(r"f(\d+)", name)
    if m:
        return proto, int(m.group(1))

    return proto, None

# -------------------------
# Load DTM log (JSONL)
# -------------------------
def load_dtm_log(path):
    rows = []
    with open(path) as f:
        for line in f:
            try:
                r = json.loads(line)
                if r.get("tag") in (
                    "DTM_BROADCAST",
                    "DTM_ROLLING_SEND",
                    "DTM_EVENT_DONE",
                ):
                    rows.append(r)
            except:
                pass
    return pd.DataFrame(rows)

# -------------------------
# Load resource CSVs
# -------------------------
def load_resource_csvs(input_dir):
    rows = []

    for p in Path(input_dir).glob("*.csv"):
        proto, fanout = parse_proto_fanout(p.name)
        if proto is None or fanout is None:
            continue

        df = pd.read_csv(p)
        if not {"cpu_percent", "rss_mb", "tx_kbps"}.issubset(df.columns):
            continue

        rows.append({
            "proto": proto,
            "fanout": fanout,
            "cpu_mean": df.cpu_percent.mean(),
            "cpu_ci95": ci95(df.cpu_percent),
            "rss_mb_mean": df.rss_mb.mean(),
            "tx_kbps_mean": df.tx_kbps.mean(),
            "tx_kbps_ci95": ci95(df.tx_kbps),
        })

    return pd.DataFrame(rows)

# -------------------------
# Plot helpers
# -------------------------
def plot_line(df, x, y, ylabel, out, fname, yerr=None):
    plt.figure()
    for proto in ["tcp", "udp"]:
        d = df[df.proto == proto].sort_values(x)
        if d.empty:
            continue
        plt.errorbar(
            d[x],
            d[y],
            yerr=d[yerr] if yerr and yerr in d else None,
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
    ap.add_argument("--dtm-log", required=True)
    ap.add_argument("--resource-dir", required=True)
    ap.add_argument("--outdir", required=True)
    args = ap.parse_args()

    out = Path(args.outdir)
    out.mkdir(parents=True, exist_ok=True)

    # ---- Load logs
    df_log = load_dtm_log(args.dtm_log)
    if df_log.empty:
        raise RuntimeError("DTM log is empty or invalid")

    # ---- Fan-out delay (dispatch)
    fanout_rows = []
    for tag in ["DTM_BROADCAST", "DTM_ROLLING_SEND"]:
        d = df_log[df_log.tag == tag]
        if d.empty:
            continue
        g = d.groupby(["proto", "fanout"])["dispatch_ns"].mean().reset_index()
        g["dispatch_ms"] = g.dispatch_ns / 1e6
        fanout_rows.append(g)

    df_fanout = pd.concat(fanout_rows, ignore_index=True)

    # ---- Orchestration delay
    d_done = df_log[df_log.tag == "DTM_EVENT_DONE"].copy()
    d_done["orch_ms"] = (d_done.t_done_ns - d_done.t_rx_ns) / 1e6
    df_orch = d_done.groupby(["proto", "fanout"])["orch_ms"].mean().reset_index()

    # ---- Resources
    df_res = load_resource_csvs(args.resource_dir)
    if df_res.empty:
        raise RuntimeError("No valid resource CSVs found")

    # ---- Save summaries
    df_fanout.to_csv(out / "fanout_delay_summary.csv", index=False)
    df_orch.to_csv(out / "orchestration_delay_summary.csv", index=False)
    df_res.to_csv(out / "resource_summary.csv", index=False)

    # ---- Plots
    plot_line(
        df_fanout,
        "fanout",
        "dispatch_ms",
        "Fan-out dispatch delay (ms)",
        out,
        "fanout_delay_vs_fanout.png"
    )

    plot_line(
        df_orch,
        "fanout",
        "orch_ms",
        "Orchestration delay (ms)",
        out,
        "orchestration_delay_vs_fanout.png"
    )

    plot_line(
        df_res,
        "fanout",
        "cpu_mean",
        "CPU usage (%)",
        out,
        "cpu_vs_fanout.png",
        yerr="cpu_ci95"
    )

    plot_line(
        df_res,
        "fanout",
        "rss_mb_mean",
        "RSS memory (MB)",
        out,
        "rss_vs_fanout.png"
    )

    plot_line(
        df_res,
        "fanout",
        "tx_kbps_mean",
        "TX rate (kbps)",
        out,
        "tx_rate_vs_fanout.png",
        yerr="tx_kbps_ci95"
    )

    print(f"[OK] All metrics and plots written to {out}")

if __name__ == "__main__":
    main()
