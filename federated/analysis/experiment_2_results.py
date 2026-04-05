"""
NDT Federation Loss Analyser — uniform vs Poisson comparison
=============================================================
Reads experiment logs and produces plots comparing uniform and Poisson
arrival modes side by side.

Folder layout expected
----------------------
losses/
  uniform/
    1to1/    dtm.log
    1to5/    dtm.log
    1to10/   dtm.log
    1to11/   dtm.log
    1to15/   dtm.log
    1to20/   dtm.log
  poisson/
    1to1/    dtm.log
    ...

Usage
-----
python3 analyse_losses.py \\
    --logs /home/ubuntu/final_test/experiment2/losses/ \\
    --out  /home/ubuntu/final_test/results/losses/ \\
    --C    2500

If only one mode is present (e.g. uniform/ only), the script will plot
that mode alone without error.
"""

import re
import argparse
from pathlib import Path
from typing import Optional, List, Dict

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

# Style per mode
MODE_STYLE = {
    "uniform": {"color": "#185FA5", "marker": "o", "linestyle": "-",  "label": "Uniform"},
    "poisson": {"color": "#D85A30", "marker": "s", "linestyle": "--", "label": "Poisson"},
}


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------
def load_log(path: Path) -> pd.DataFrame:
    print(f"    Reading {path.parent.name}/{path.name} ... ", end="", flush=True)
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
    m = re.search(r"1to(\d+)$", folder_name)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Metrics extraction
# ---------------------------------------------------------------------------
def compute_metrics(df: pd.DataFrame, n_ndts: int, C: float) -> Optional[dict]:
    if "tag" not in df.columns:
        return None

    rx   = df[df["tag"] == "NDT_EVENT_RX"]
    done_mask = df["tag"] == "DTM_EVENT_DONE"
    if "src" in df.columns:
        done_mask &= (df["src"] == "feedback")
    done = df[done_mask]
    drop = df[df["tag"] == "DTM_DROP"]

    total_rx   = len(rx)
    total_done = len(done)
    total_drop = len(drop)

    if total_rx == 0:
        return None

    t_rx = pd.to_numeric(rx["ts_ns"], errors="coerce").dropna()
    if len(t_rx) < 2:
        return None
    window_s  = (t_rx.max() - t_rx.min()) / 1e9
    offered   = total_rx   / window_s if window_s > 0 else 0.0
    capacity  = total_done / window_s if window_s > 0 else 0.0
    drop_pct  = total_drop / total_rx * 100.0
    theo_drop = max(0.0, (offered - C) / offered * 100.0) if offered > 0 else 0.0

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

    return {
        "NDTs":                 n_ndts,
        "Offered (ev/s)":       round(offered,       1),
        "Capacity (ev/s)":      round(capacity,      1),
        "Drop (%)":             round(drop_pct,      2),
        "Theoretical Drop (%)": round(theo_drop,     2),
        "Queue Mean (ms)":      round(queue_mean,    3),
        "Queue P95 (ms)":       round(queue_p95,     3),
        "Proc Lat Mean (ms)":   round(proc_lat_mean, 3),
        "Proc Lat P95 (ms)":    round(proc_lat_p95,  3),
    }


# ---------------------------------------------------------------------------
# Load all experiments for one arrival mode
# ---------------------------------------------------------------------------
def load_mode(mode_dir: Path, C: float) -> pd.DataFrame:
    if not mode_dir.exists():
        return pd.DataFrame()

    folders = sorted(
        [d for d in mode_dir.iterdir()
         if d.is_dir() and extract_ndts(d.name) is not None],
        key=lambda d: extract_ndts(d.name),
    )

    if not folders:
        print(f"  [WARN] No 1toN folders in {mode_dir}")
        return pd.DataFrame()

    results = []
    for folder in folders:
        n = extract_ndts(folder.name)
        log = find_log(folder)
        if log is None:
            print(f"  [SKIP] {folder.name}: no log file")
            continue
        df = load_log(log)
        if df.empty:
            print(f"  [SKIP] {folder.name}: empty log")
            continue
        m = compute_metrics(df, n, C)
        if m is None:
            print(f"  [SKIP] {folder.name}: insufficient data")
            continue
        results.append(m)

    if not results:
        return pd.DataFrame()

    return pd.DataFrame(results).sort_values("NDTs").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------
def _plot_line(ax, df: pd.DataFrame, col: str, style: dict) -> None:
    if df.empty or col not in df.columns:
        return
    valid = df.dropna(subset=[col])
    if valid.empty:
        return
    ax.plot(valid["NDTs"], valid[col],
            color=style["color"], marker=style["marker"],
            linestyle=style["linestyle"], label=style["label"],
            linewidth=1.5, markersize=5)


def plot_drop(data: Dict[str, pd.DataFrame], C: float, outdir: Path) -> None:
    fig, ax = plt.subplots()

    # Theoretical curve — same for both modes, use first available df
    ref = next((df for df in data.values() if not df.empty), None)
    if ref is not None:
        ax.plot(ref["NDTs"], ref["Theoretical Drop (%)"],
                color="gray", linestyle=":", linewidth=1.2,
                label="Theoretical (no buffer)")

    for mode, df in data.items():
        _plot_line(ax, df, "Drop (%)", MODE_STYLE[mode])

    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Event drop ratio (%)")
    ax.set_title("Drop % vs federation size")
    ax.legend()
    out = outdir / "fig1_drop_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


def plot_throughput(data: Dict[str, pd.DataFrame], C: float, outdir: Path) -> None:
    fig, ax = plt.subplots()

    # C line
    if C > 0:
        ax.axhline(y=C, color="red", linestyle=":", linewidth=1.2,
                   label=f"C = {C:.0f} ev/s")

    for mode, df in data.items():
        if df.empty:
            continue
        s = MODE_STYLE[mode]
        ax.plot(df["NDTs"], df["Offered (ev/s)"],
                color=s["color"], marker=s["marker"],
                linestyle="-", alpha=0.4,
                linewidth=1, markersize=4,
                label=f"Offered ({s['label']})")
        ax.plot(df["NDTs"], df["Capacity (ev/s)"],
                color=s["color"], marker=s["marker"],
                linestyle=s["linestyle"], linewidth=1.5, markersize=5,
                label=f"Processed ({s['label']})")

    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Event rate (ev/s)")
    ax.set_title("Throughput vs federation size")
    ax.legend(fontsize=7)
    out = outdir / "fig2_throughput_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


def plot_latency(data: Dict[str, pd.DataFrame], outdir: Path) -> None:
    fig, ax = plt.subplots()
    for mode, df in data.items():
        if df.empty:
            continue
        s = MODE_STYLE[mode]
        _plot_line(ax, df, "Proc Lat Mean (ms)",
                   {**s, "label": f"Mean ({s['label']})"})
        _plot_line(ax, df, "Proc Lat P95 (ms)",
                   {**s, "linestyle": ":", "label": f"P95 ({s['label']})"})
    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Processing latency (ms)")
    ax.set_title("Processing latency vs federation size")
    ax.legend(fontsize=7)
    out = outdir / "fig3_latency_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


def plot_queue(data: Dict[str, pd.DataFrame], outdir: Path) -> None:
    fig, ax = plt.subplots()
    for mode, df in data.items():
        if df.empty:
            continue
        s = MODE_STYLE[mode]
        _plot_line(ax, df, "Queue Mean (ms)",
                   {**s, "label": f"Mean ({s['label']})"})
        _plot_line(ax, df, "Queue P95 (ms)",
                   {**s, "linestyle": ":", "label": f"P95 ({s['label']})"})
    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Queue delay (ms)")
    ax.set_title("Queue delay vs federation size")
    ax.legend(fontsize=7)
    out = outdir / "fig4_queue_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(
        description="NDT loss analyser — uniform vs Poisson comparison"
    )
    ap.add_argument("--logs", required=True,
                    help="Root folder containing uniform/ and/or poisson/ subfolders")
    ap.add_argument("--out",  required=True,
                    help="Output folder for PDFs and CSV")
    ap.add_argument("--C", type=float, default=2500.0,
                    help="DTM capacity C in ev/s (default: 2500)")
    args = ap.parse_args()

    logs_path = Path(args.logs)
    out_path  = Path(args.out)
    out_path.mkdir(parents=True, exist_ok=True)

    # Load both modes
    data: Dict[str, pd.DataFrame] = {}
    for mode in ("uniform", "poisson"):
        mode_dir = logs_path / mode
        print(f"\nLoading mode: {mode} ({mode_dir})")
        df = load_mode(mode_dir, args.C)
        if not df.empty:
            data[mode] = df
            print(f"  Loaded {len(df)} experiments for {mode}")
        else:
            print(f"  No data for {mode} — skipping")

    if not data:
        print("\n[ERROR] No valid data found in any mode.")
        return

    # Console tables
    sep = "=" * 20
    for mode, df in data.items():
        print(f"\n[{sep} {mode.upper()} {sep}]")
        cols = ["NDTs", "Offered (ev/s)", "Capacity (ev/s)",
                "Drop (%)", "Theoretical Drop (%)",
                "Proc Lat Mean (ms)", "Proc Lat P95 (ms)",
                "Queue Mean (ms)", "Queue P95 (ms)"]
        print(tabulate(df[cols], headers="keys", tablefmt="psql",
                       showindex=False, floatfmt=".2f"))

    # CSV per mode
    for mode, df in data.items():
        csv_path = out_path / f"metrics_{mode}.csv"
        df.to_csv(csv_path, index=False)
        print(f"  -> {csv_path.name}")

    # Combined CSV
    frames = []
    for mode, df in data.items():
        tmp = df.copy()
        tmp.insert(0, "Mode", mode)
        frames.append(tmp)
    pd.concat(frames).to_csv(out_path / "metrics_combined.csv", index=False)

    # Plots
    print("\nGenerating plots...")
    plot_drop(data, args.C, out_path)
    plot_throughput(data, args.C, out_path)
    plot_latency(data, out_path)
    plot_queue(data, out_path)

    print(f"\nDone. Results in: {out_path.resolve()}")


if __name__ == "__main__":
    main()