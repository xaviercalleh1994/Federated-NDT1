"""
NDT Federation Loss Analyser
==============================
Reads experiment folders named 1toN (N = number of NDTs) and produces:
  - Summary table (console + CSV)
  - Fig 1: Drop % vs number of NDTs
  - Fig 2: Processing latency (mean + P95) vs number of NDTs
  - Fig 3: Offered load vs capacity vs number of NDTs

Usage
-----
python3 analyse_losses.py \
    --logs /home/ubuntu/final_test/experiment2/losses/ \
    --out  /home/ubuntu/final_test/results/losses/ \
    --C    2000

Folder layout expected
----------------------
losses/
  1to1/   dtm.log   (or ndtm_events.log)
  1to2/   dtm.log
  1to5/   dtm.log
  ...
"""

import re
import argparse
from pathlib import Path
from typing import Optional, List

try:
    import orjson
    def _parse(line) -> dict:
        return orjson.loads(line)
    _MODE = "rb"
except ImportError:
    import json as _json  # type: ignore
    def _parse(line) -> dict:
        return _json.loads(line)
    _MODE = "r"

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from tabulate import tabulate

# ---------------------------------------------------------------------------
# IEEE style
# ---------------------------------------------------------------------------
plt.rcParams.update({
    "font.family":      "serif",
    "font.size":        10,
    "axes.titlesize":   10,
    "axes.labelsize":   10,
    "legend.fontsize":  9,
    "xtick.labelsize":  9,
    "ytick.labelsize":  9,
    "figure.figsize":   (5.5, 3.5),
    "axes.grid":        True,
    "grid.alpha":       0.4,
    "grid.linestyle":   "--",
    "savefig.dpi":      300,
    "savefig.bbox":     "tight",
})


# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------
def load_log(path: Path) -> pd.DataFrame:
    print(f"  Reading {path.name} ... ", end="", flush=True)
    rows = []
    with open(path, _MODE) as f:
        for raw in f:
            try:
                rows.append(_parse(raw))
            except Exception:
                pass
    print(f"OK ({len(rows):,} events)")
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "ts_ns" in df.columns:
        df["ts_ns"] = pd.to_numeric(df["ts_ns"], errors="coerce")
        df = df.dropna(subset=["ts_ns"]).sort_values("ts_ns").reset_index(drop=True)
    return df


def find_log(folder: Path) -> Optional[Path]:
    for name in ("dtm.log", "ndtm_events.log", "dtm_events.log"):
        p = folder / name
        if p.exists():
            return p
    return None


def extract_ndts(folder_name: str) -> Optional[int]:
    """Extract N from folder names like '1to1', '1to11', '1to20'."""
    m = re.search(r"1to(\d+)$", folder_name)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Metrics extraction
# ---------------------------------------------------------------------------
def compute_metrics(df: pd.DataFrame, n_ndts: int, C: float) -> Optional[dict]:
    if "tag" not in df.columns:
        return None

    rx   = df[df["tag"] == "NDT_EVENT_RX"]
    done = df[(df["tag"] == "DTM_EVENT_DONE") & (df.get("src", pd.Series("")) == "feedback")]
    drop = df[df["tag"] == "DTM_DROP"]

    total_rx   = len(rx)
    total_done = len(done)
    total_drop = len(drop)

    if total_rx == 0:
        return None

    # Offered load — based on arrival window
    t_rx = pd.to_numeric(rx["ts_ns"], errors="coerce").dropna()
    if len(t_rx) < 2:
        return None
    window_s = (t_rx.max() - t_rx.min()) / 1e9
    offered   = total_rx   / window_s if window_s > 0 else 0.0
    capacity  = total_done / window_s if window_s > 0 else 0.0
    drop_pct  = total_drop / total_rx * 100.0

    # Processing latency from DTM_EVENT_DONE mochila
    proc_lat_mean = proc_lat_p95 = float("nan")
    queue_mean    = queue_p95    = float("nan")

    if not done.empty:
        if {"t_rx_ns", "t_done_ns"}.issubset(done.columns):
            lats = (
                pd.to_numeric(done["t_done_ns"], errors="coerce") -
                pd.to_numeric(done["t_rx_ns"],   errors="coerce")
            ).dropna() / 1e6
            if not lats.empty:
                proc_lat_mean = float(lats.mean())
                proc_lat_p95  = float(lats.quantile(0.95))

        if "queue_ms" in done.columns:
            qms = pd.to_numeric(done["queue_ms"], errors="coerce").dropna()
            if not qms.empty:
                queue_mean = float(qms.mean())
                queue_p95  = float(qms.quantile(0.95))

    # Drop breakdown by priority
    drops_crit = drops_bg = 0
    if not drop.empty and "priority" in drop.columns:
        prio = pd.to_numeric(drop["priority"], errors="coerce").fillna(100)
        drops_crit = int((prio <= 20).sum())
        drops_bg   = int((prio > 20).sum())

    # Theoretical drop% from queuing theory
    theo_drop = max(0.0, (offered - C) / offered * 100.0) if offered > 0 else 0.0

    return {
        "NDTs":                n_ndts,
        "Offered (ev/s)":      round(offered,       1),
        "Capacity (ev/s)":     round(capacity,      1),
        "Drop (%)":            round(drop_pct,      2),
        "Theoretical Drop (%)":round(theo_drop,     2),
        "Queue Mean (ms)":     round(queue_mean,    3),
        "Queue P95 (ms)":      round(queue_p95,     3),
        "Proc Lat Mean (ms)":  round(proc_lat_mean, 3),
        "Proc Lat P95 (ms)":   round(proc_lat_p95,  3),
        "Drops Critical":      drops_crit,
        "Drops Background":    drops_bg,
    }


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------
def plot_drop(df: pd.DataFrame, C: float, outdir: Path) -> None:
    fig, ax = plt.subplots()
    ax.plot(df["NDTs"], df["Drop (%)"],
            marker="o", label="Measured drop %")
    ax.plot(df["NDTs"], df["Theoretical Drop (%)"],
            marker="s", linestyle="--", label="Theoretical drop %")
    ax.axvline(x=C / (C / df["NDTs"].max()) if C > 0 else 0,
               color="gray", linestyle=":", alpha=0.5)
    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Event drop ratio (%)")
    ax.set_title("Drop % vs federation size")
    ax.legend()
    out = outdir / "fig1_drop_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


def plot_latency(df: pd.DataFrame, outdir: Path) -> None:
    fig, ax = plt.subplots()
    ax.plot(df["NDTs"], df["Proc Lat Mean (ms)"],
            marker="o", label="Mean latency")
    ax.plot(df["NDTs"], df["Proc Lat P95 (ms)"],
            marker="s", linestyle="--", label="P95 latency")
    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Processing latency (ms)")
    ax.set_title("Processing latency vs federation size")
    ax.legend()
    out = outdir / "fig2_latency_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


def plot_throughput(df: pd.DataFrame, C: float, outdir: Path) -> None:
    fig, ax = plt.subplots()
    ax.plot(df["NDTs"], df["Offered (ev/s)"],
            marker="o", label="Offered load")
    ax.plot(df["NDTs"], df["Capacity (ev/s)"],
            marker="s", linestyle="--", label="Processed")
    if C > 0:
        ax.axhline(y=C, color="red", linestyle=":", alpha=0.7, label=f"C = {C:.0f} ev/s")
    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Event rate (ev/s)")
    ax.set_title("Throughput vs federation size")
    ax.legend()
    out = outdir / "fig3_throughput_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


def plot_queue(df: pd.DataFrame, outdir: Path) -> None:
    fig, ax = plt.subplots()
    ax.plot(df["NDTs"], df["Queue Mean (ms)"],
            marker="o", label="Queue mean")
    ax.plot(df["NDTs"], df["Queue P95 (ms)"],
            marker="s", linestyle="--", label="Queue P95")
    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Queue delay (ms)")
    ax.set_title("Queue delay vs federation size")
    ax.legend()
    out = outdir / "fig4_queue_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(description="NDT federation loss analyser")
    ap.add_argument("--logs", required=True,
                    help="Root folder containing 1toN experiment subfolders")
    ap.add_argument("--out",  required=True,
                    help="Output folder for PDFs and CSV")
    ap.add_argument("--C", type=float, default=2000.0,
                    help="Known DTM capacity C in ev/s (used for theoretical drop line)")
    args = ap.parse_args()

    logs_path = Path(args.logs)
    out_path  = Path(args.out)
    out_path.mkdir(parents=True, exist_ok=True)

    # Discover experiment folders
    folders = sorted(
        [d for d in logs_path.iterdir() if d.is_dir() and extract_ndts(d.name) is not None],
        key=lambda d: extract_ndts(d.name),
    )

    if not folders:
        print(f"[WARN] No 1toN folders found under {logs_path}")
        return

    print(f"\nFound {len(folders)} experiments: {[d.name for d in folders]}\n")

    results = []
    for folder in folders:
        n = extract_ndts(folder.name)
        log = find_log(folder)
        if log is None:
            print(f"  [SKIP] {folder.name}: no log file found")
            continue
        df = load_log(log)
        if df.empty:
            print(f"  [SKIP] {folder.name}: empty log")
            continue
        m = compute_metrics(df, n, args.C)
        if m is None:
            print(f"  [SKIP] {folder.name}: insufficient data")
            continue
        results.append(m)

    if not results:
        print("[ERROR] No valid experiments found.")
        return

    df_all = pd.DataFrame(results).sort_values("NDTs").reset_index(drop=True)

    # Console table
    print(f"\n{'='*20} RESULTS {'='*20}")
    display_cols = [
        "NDTs", "Offered (ev/s)", "Capacity (ev/s)",
        "Drop (%)", "Theoretical Drop (%)",
        "Proc Lat Mean (ms)", "Proc Lat P95 (ms)",
        "Queue Mean (ms)", "Queue P95 (ms)",
    ]
    print(tabulate(
        df_all[display_cols],
        headers="keys",
        tablefmt="psql",
        showindex=False,
        floatfmt=".2f",
    ))

    # CSV
    csv_path = out_path / "metrics_losses.csv"
    df_all.to_csv(csv_path, index=False)
    print(f"\n  -> {csv_path.name}")

    # Plots
    print("\nGenerating plots...")
    plot_drop(df_all, args.C, out_path)
    plot_latency(df_all, out_path)
    plot_throughput(df_all, args.C, out_path)
    plot_queue(df_all, out_path)

    print(f"\nDone. Results in: {out_path.resolve()}")


if __name__ == "__main__":
    main()