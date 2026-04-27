"""
Authors: Xavier Calle-Heredia & Xavier Hesselbach
Date: 2026-04-08

Experiment 1 — Synchronization Latency Analyser
================================================
Reads DTM logs (and optionally ONM logs) from multiple runs and produces
plots with 95% confidence intervals, consistent with experiment_2 analysis.

Metrics extracted
-----------------
From dtm.log only:
  L_dispatch  — fanout time at the DTM (t_dispatch_end - t_dispatch_start)
  L_apply     — Mininet apply time per NDT (apply_ms from DTM_ACK_RX)
  T_conv      — convergence spread (wall_ms from DTM_EVENT_COMPLETE)
  F_sync      — sync fidelity (ok ACKs / dispatched × 100)

From dtm.log + onm.log (optional):
  L_sync      — end-to-end sync latency (t_apply_ns - t_tx_ns joined on event_id)
  L_relay     — relay latency (t_rx_ns at DTM - t_tx_ns at ONM)

Folder layout expected
----------------------
exp1/
  run1/
    1to1/    dtm.log  [onm.log]
    1to2/    dtm.log  [onm.log]
    1to5/    dtm.log  [onm.log]
    1to10/   dtm.log  [onm.log]
    1to20/   dtm.log  [onm.log]
  run2/
    ...
  run3/
    ...

Usage
-----
python3 experiment_1.py \\
    --logs /home/ubuntu/exp1/ \\
    --out  /home/ubuntu/exp1/results/
"""

import re
import argparse
from pathlib import Path
from typing import Optional, Dict, List

try:
    import orjson
    def _parse(line: bytes) -> dict: return orjson.loads(line)
    _MODE = "rb"
except ImportError:
    import json as _json
    def _parse(line) -> dict: return _json.loads(line)
    _MODE = "r"

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy import stats
from tabulate import tabulate

# ── IEEE-style plot settings (consistent with experiment_2) ──────────────────
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

STYLE = {"color": "#185FA5", "marker": "o", "linestyle": "-"}
CI    = 0.95


# ── IO helpers ────────────────────────────────────────────────────────────────

def load_log(path: Path) -> pd.DataFrame:
    rows = []
    with open(path, _MODE) as f:
        for raw in f:
            try:
                rows.append(_parse(raw))
            except Exception:
                pass
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "ts_ns" in df.columns:
        df["ts_ns"] = pd.to_numeric(df["ts_ns"], errors="coerce")
        df = df.dropna(subset=["ts_ns"]).sort_values("ts_ns").reset_index(drop=True)
    return df


def extract_ndts(folder_name: str) -> Optional[int]:
    m = re.search(r"1to(\d+)$", folder_name)
    return int(m.group(1)) if m else None


# ── Metric extraction for one (dtm.log, onm.log?) pair ───────────────────────

def compute_metrics(dtm_path: Path, onm_path: Optional[Path],
                    n_ndts: int) -> Optional[dict]:
    """
    Extract all metrics for one experiment folder.
    onm_path may be None — L_sync and L_relay are skipped in that case.
    """
    dtm = load_log(dtm_path)
    if dtm.empty or "tag" not in dtm.columns:
        return None

    # ── L_dispatch ────────────────────────────────────────────────────────────
    disp = dtm[dtm["tag"] == "NDT_EVENT_DISPATCHED"].copy()
    l_dispatch_mean = l_dispatch_p95 = float("nan")
    if not disp.empty and {"t_dispatch_start_ns", "t_dispatch_end_ns"}.issubset(disp.columns):
        d_ns = (
            pd.to_numeric(disp["t_dispatch_end_ns"],   errors="coerce") -
            pd.to_numeric(disp["t_dispatch_start_ns"], errors="coerce")
        ).dropna() / 1e6   # ms
        if not d_ns.empty:
            l_dispatch_mean = float(d_ns.mean())
            l_dispatch_p95  = float(d_ns.quantile(0.95))

    # ── L_apply (from DTM_ACK_RX.apply_ms) ───────────────────────────────────
    acks = dtm[dtm["tag"] == "DTM_ACK_RX"].copy()
    l_apply_mean = l_apply_p95 = float("nan")
    if not acks.empty and "apply_ms" in acks.columns:
        a_ms = pd.to_numeric(acks["apply_ms"], errors="coerce").dropna()
        if not a_ms.empty:
            l_apply_mean = float(a_ms.mean())
            l_apply_p95  = float(a_ms.quantile(0.95))

    # ── T_conv (convergence spread across NDTs) ───────────────────────────────
    # T_conv = max(t_apply_i) - min(t_apply_i) for i=1..N, per event_id.
    # Only meaningful for N>=2. For N=1 there is only one NDT so the spread
    # is always 0 by definition — we set NaN to exclude it from plots.
    t_conv_mean = t_conv_p95 = float("nan")
    if n_ndts == 1:
        # With a single NDT there is no spread between replicas by definition.
        # Set to 0 so the plot shows T_conv growing from 0 as N increases.
        t_conv_mean = 0.0
        t_conv_p95  = 0.0
    elif n_ndts >= 2 and not acks.empty and {"event_id", "t_rx_ns", "apply_ms"}.issubset(acks.columns):
        acks_t = acks[["event_id", "t_rx_ns", "apply_ms"]].copy()
        acks_t["t_rx_ns"]  = pd.to_numeric(acks_t["t_rx_ns"],  errors="coerce")
        acks_t["apply_ms"] = pd.to_numeric(acks_t["apply_ms"], errors="coerce")
        # Reconstruct t_apply_ns: the moment the NDT finished applying =
        # moment DTM received the ACK minus the apply time reported in the ACK
        acks_t["t_apply_ns"] = acks_t["t_rx_ns"] - (acks_t["apply_ms"] * 1e6)
        acks_t = acks_t.dropna(subset=["t_apply_ns"])
        # For each event_id compute spread across all NDT ACKs
        spread = acks_t.groupby("event_id")["t_apply_ns"].agg(
            lambda x: (x.max() - x.min()) / 1e6 if len(x) >= 2 else float("nan")
        ).dropna()
        if not spread.empty:
            t_conv_mean = float(spread.mean())
            t_conv_p95  = float(spread.quantile(0.95))

    # ── F_sync (% of ok ACKs) ─────────────────────────────────────────────────
    f_sync = float("nan")
    if not acks.empty and "ok" in acks.columns:
        ok_vals = acks["ok"].astype(bool)
        f_sync  = float(ok_vals.sum() / len(ok_vals) * 100.0)

    # ── L_relay and L_sync (require onm.log) ─────────────────────────────────
    l_relay_mean = l_relay_p95 = float("nan")
    l_sync_mean  = l_sync_p95  = float("nan")

    if onm_path is not None and onm_path.exists():
        onm = load_log(onm_path)
        if not onm.empty and "tag" in onm.columns:
            sent = onm[onm["tag"] == "ON_EVENT_SENT"].copy()

            if not sent.empty and "event_id" in sent.columns and "t_tx_ns" in sent.columns:
                sent = sent[["event_id", "t_tx_ns"]].copy()
                sent["t_tx_ns"] = pd.to_numeric(sent["t_tx_ns"], errors="coerce")
                sent = sent.dropna(subset=["t_tx_ns"])

                # ── L_relay: DTM NDT_EVENT_RX.t_rx_ns - ONM t_tx_ns ──────────
                rx = dtm[dtm["tag"] == "NDT_EVENT_RX"].copy()
                if not rx.empty and "event_id" in rx.columns:
                    rx = rx[["event_id", "t_rx_ns"]].copy()
                    rx["t_rx_ns"] = pd.to_numeric(rx["t_rx_ns"], errors="coerce")
                    merged_relay = sent.merge(rx, on="event_id", how="inner")
                    relay_ms = (merged_relay["t_rx_ns"] - merged_relay["t_tx_ns"]) / 1e6
                    relay_ms = relay_ms[relay_ms > 0].dropna()
                    if not relay_ms.empty:
                        l_relay_mean = float(relay_ms.mean())
                        l_relay_p95  = float(relay_ms.quantile(0.95))

                # ── L_sync: DTM_ACK_RX.t_apply_ns - ONM t_tx_ns ─────────────
                if not acks.empty and "event_id" in acks.columns and "t_apply_ns" in acks.columns:
                    acks_s = acks[["event_id", "t_apply_ns"]].copy()
                    acks_s["t_apply_ns"] = pd.to_numeric(acks_s["t_apply_ns"], errors="coerce")
                    merged_sync = sent.merge(acks_s, on="event_id", how="inner")
                    sync_ms = (merged_sync["t_apply_ns"] - merged_sync["t_tx_ns"]) / 1e6
                    sync_ms = sync_ms[sync_ms > 0].dropna()
                    if not sync_ms.empty:
                        l_sync_mean = float(sync_ms.mean())
                        l_sync_p95  = float(sync_ms.quantile(0.95))

    return {
        "NDTs":               n_ndts,
        "L_dispatch_mean_ms": round(l_dispatch_mean, 3),
        "L_dispatch_p95_ms":  round(l_dispatch_p95,  3),
        "L_apply_mean_ms":    round(l_apply_mean,     3),
        "L_apply_p95_ms":     round(l_apply_p95,      3),
        "T_conv_mean_ms":     round(t_conv_mean,       3),
        "T_conv_p95_ms":      round(t_conv_p95,        3),
        "F_sync_pct":         round(f_sync,            2),
        "L_relay_mean_ms":    round(l_relay_mean,      3),
        "L_relay_p95_ms":     round(l_relay_p95,       3),
        "L_sync_mean_ms":     round(l_sync_mean,       3),
        "L_sync_p95_ms":      round(l_sync_p95,        3),
    }


# ── Load all runs ─────────────────────────────────────────────────────────────

def load_all_runs(logs_path: Path) -> pd.DataFrame:
    """
    Walk logs_path/run*/1toN/ and collect metrics per run per N.
    """
    run_dirs = sorted([
        d for d in logs_path.iterdir()
        if d.is_dir() and d.name.startswith("run")
    ])

    if not run_dirs:
        # fallback: logs_path itself contains 1toN/ folders directly
        run_dirs = [logs_path]

    frames = []
    for run_dir in run_dirs:
        exp_dirs = sorted(
            [d for d in run_dir.iterdir()
             if d.is_dir() and extract_ndts(d.name) is not None],
            key=lambda d: extract_ndts(d.name),
        )
        for exp_dir in exp_dirs:
            n        = extract_ndts(exp_dir.name)
            dtm_path = exp_dir / "dtm.log"
            onm_path = exp_dir / "onm.log"  # optional
            if not dtm_path.exists():
                print(f"  [SKIP] {exp_dir}: no dtm.log")
                continue
            onm = onm_path if onm_path.exists() else None
            if onm is None:
                print(f"  [INFO] {exp_dir}: no onm.log — L_sync/L_relay skipped")
            m = compute_metrics(dtm_path, onm, n)
            if m is None:
                print(f"  [SKIP] {exp_dir}: empty or unparseable dtm.log")
                continue
            m["run"] = run_dir.name
            frames.append(m)
            print(f"  {run_dir.name}/{exp_dir.name}: N={n} ok")

    if not frames:
        return pd.DataFrame()
    return pd.DataFrame(frames).sort_values(["NDTs", "run"]).reset_index(drop=True)


# ── Aggregate with 95% CI ─────────────────────────────────────────────────────

def aggregate(df: pd.DataFrame, col: str) -> pd.DataFrame:
    results = []
    for ndts, group in df.groupby("NDTs"):
        vals = group[col].replace([float("inf"), float("-inf")], float("nan")).dropna().values
        n    = len(vals)
        if n == 0:
            continue
        mean = float(np.mean(vals))
        if n == 1:
            ci_low = ci_high = mean
        else:
            se      = stats.sem(vals)
            t       = stats.t.ppf((1 + CI) / 2, df=n - 1)
            ci_low  = mean - t * se
            ci_high = mean + t * se
        results.append({
            "NDTs":    ndts,
            "mean":    mean,
            "ci_low":  ci_low,
            "ci_high": ci_high,
            "n_runs":  n,
        })
    if not results:
        return pd.DataFrame()
    return pd.DataFrame(results).sort_values("NDTs").reset_index(drop=True)


# ── Plot helpers ──────────────────────────────────────────────────────────────

def _plot_ci(ax, agg: pd.DataFrame, color: str, marker: str,
             linestyle: str, label: str) -> None:
    if agg.empty:
        return
    yerr = np.array([
        agg["mean"].values - agg["ci_low"].values,
        agg["ci_high"].values - agg["mean"].values,
    ])
    ax.errorbar(
        agg["NDTs"], agg["mean"],
        yerr=yerr,
        color=color, marker=marker, linestyle=linestyle,
        label=label, linewidth=1.5, markersize=5,
        capsize=4, capthick=1.2, elinewidth=1.2,
    )


# ── Individual plots ──────────────────────────────────────────────────────────

def plot_t_conv(df: pd.DataFrame, outdir: Path) -> None:
    """T_conv (convergence spread) vs N — primary result figure."""
    fig, ax = plt.subplots()
    agg_mean = aggregate(df, "T_conv_mean_ms")
    agg_p95  = aggregate(df, "T_conv_p95_ms")
    _plot_ci(ax, agg_mean, "#185FA5", "o", "-",  "Mean")
    _plot_ci(ax, agg_p95,  "#D85A30", "s", "--", "P95")
    ax.set_xlabel("Number of NDTs (N)")
    ax.set_ylabel("Convergence spread $T_{conv}$ (ms)")
    ax.set_title("Convergence spread vs federation size (95% CI)")
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.legend()
    out = outdir / "fig1_t_conv_vs_ndts.pdf"
    fig.savefig(out); plt.close(fig)
    print(f"  -> {out.name}")


def plot_l_dispatch(df: pd.DataFrame, outdir: Path) -> None:
    """L_dispatch vs N — DTM fanout overhead."""
    fig, ax = plt.subplots()
    agg_mean = aggregate(df, "L_dispatch_mean_ms")
    agg_p95  = aggregate(df, "L_dispatch_p95_ms")
    _plot_ci(ax, agg_mean, "#D85A30", "o", "-",  "Mean")
    _plot_ci(ax, agg_p95,  "#534AB7", "s", "--", "P95")
    ax.set_xlabel("Number of NDTs (N)")
    ax.set_ylabel("Dispatch latency $L_{dispatch}$ (ms)")
    ax.set_title("DTM dispatch latency vs federation size (95% CI)")
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.legend()
    out = outdir / "fig2_l_dispatch_vs_ndts.pdf"
    fig.savefig(out); plt.close(fig)
    print(f"  -> {out.name}")


def plot_l_apply(df: pd.DataFrame, outdir: Path) -> None:
    """L_apply vs N — Mininet application overhead per NDT."""
    fig, ax = plt.subplots()
    agg_mean = aggregate(df, "L_apply_mean_ms")
    agg_p95  = aggregate(df, "L_apply_p95_ms")
    _plot_ci(ax, agg_mean, "#1D9E75", "o", "-",  "Mean")
    _plot_ci(ax, agg_p95,  "#BA7517", "s", "--", "P95")
    ax.set_xlabel("Number of NDTs (N)")
    ax.set_ylabel("Apply latency $L_{apply}$ (ms)")
    ax.set_title("NDT apply latency vs federation size (95% CI)")
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.legend()
    out = outdir / "fig3_l_apply_vs_ndts.pdf"
    fig.savefig(out); plt.close(fig)
    print(f"  -> {out.name}")


def plot_f_sync(df: pd.DataFrame, outdir: Path) -> None:
    """F_sync (%) vs N — synchronization fidelity."""
    fig, ax = plt.subplots()
    agg = aggregate(df, "F_sync_pct")
    _plot_ci(ax, agg, "#534AB7", "o", "-", "F_sync")
    ax.set_xlabel("Number of NDTs (N)")
    ax.set_ylabel("Synchronization fidelity $F_{sync}$ (%)")
    ax.set_title("Sync fidelity vs federation size (95% CI)")
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.set_ylim(0, 105)
    out = outdir / "fig4_f_sync_vs_ndts.pdf"
    fig.savefig(out); plt.close(fig)
    print(f"  -> {out.name}")


def plot_l_sync(df: pd.DataFrame, outdir: Path) -> None:
    """L_sync vs N — end-to-end synchronization latency (requires onm.log)."""
    col_mean = "L_sync_mean_ms"
    col_p95  = "L_sync_p95_ms"
    valid = df[col_mean].replace([float("inf"), float("-inf")], float("nan")).dropna()
    if valid.empty:
        print("  [SKIP] fig5_l_sync: no L_sync data (onm.log not found)")
        return
    fig, ax = plt.subplots()
    agg_mean = aggregate(df, col_mean)
    agg_p95  = aggregate(df, col_p95)
    _plot_ci(ax, agg_mean, "#BA7517", "o", "-",  "Mean")
    _plot_ci(ax, agg_p95,  "#185FA5", "s", "--", "P95")
    ax.set_xlabel("Number of NDTs (N)")
    ax.set_ylabel("Sync latency $L_{sync}$ (ms)")
    ax.set_title("End-to-end sync latency vs federation size (95% CI)")
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    ax.legend()
    out = outdir / "fig5_l_sync_vs_ndts.pdf"
    fig.savefig(out); plt.close(fig)
    print(f"  -> {out.name}")


def plot_l_relay(df: pd.DataFrame, outdir: Path) -> None:
    """L_relay vs N — relay transport latency (requires onm.log)."""
    col = "L_relay_mean_ms"
    valid = df[col].replace([float("inf"), float("-inf")], float("nan")).dropna()
    if valid.empty:
        print("  [SKIP] fig6_l_relay: no L_relay data (onm.log not found)")
        return
    fig, ax = plt.subplots()
    agg = aggregate(df, col)
    _plot_ci(ax, agg, "#993556", "o", "-", "L_relay mean")
    ax.set_xlabel("Number of NDTs (N)")
    ax.set_ylabel("Relay latency $L_{relay}$ (ms)")
    ax.set_title("Relay latency vs federation size (95% CI)")
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())
    out = outdir / "fig6_l_relay_vs_ndts.pdf"
    fig.savefig(out); plt.close(fig)
    print(f"  -> {out.name}")


def plot_decomposition(df: pd.DataFrame, outdir: Path) -> None:
    """
    Stacked bar: L_relay + L_dispatch + L_apply per N.
    Shows which component dominates synchronization latency.
    Skips L_relay if onm.log not available.
    """
    ndts_vals = sorted(df["NDTs"].unique())
    relay_means   = []
    dispatch_means = []
    apply_means   = []
    has_relay = False

    for n in ndts_vals:
        g = df[df["NDTs"] == n]
        dispatch_means.append(float(g["L_dispatch_mean_ms"].replace(
            [float("inf"), float("-inf")], float("nan")).dropna().mean() or 0))
        apply_means.append(float(g["L_apply_mean_ms"].replace(
            [float("inf"), float("-inf")], float("nan")).dropna().mean() or 0))
        r = g["L_relay_mean_ms"].replace(
            [float("inf"), float("-inf")], float("nan")).dropna()
        if not r.empty and not np.isnan(r.mean()):
            relay_means.append(float(r.mean()))
            has_relay = True
        else:
            relay_means.append(0.0)

    fig, ax = plt.subplots()
    x = np.arange(len(ndts_vals))
    w = 0.5

    bottom = np.zeros(len(ndts_vals))
    if has_relay:
        ax.bar(x, relay_means, w, bottom=bottom,
               label="$L_{relay}$", color="#993556")
        bottom += np.array(relay_means)

    ax.bar(x, dispatch_means, w, bottom=bottom,
           label="$L_{dispatch}$", color="#D85A30")
    bottom += np.array(dispatch_means)

    ax.bar(x, apply_means, w, bottom=bottom,
           label="$L_{apply}$", color="#1D9E75")

    ax.set_xticks(x)
    ax.set_xticklabels([str(n) for n in ndts_vals])
    ax.set_xlabel("Number of NDTs (N)  [log₂ scale]")
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Latency decomposition per component")
    ax.legend()
    out = outdir / "fig7_decomposition.pdf"
    fig.savefig(out); plt.close(fig)
    print(f"  -> {out.name}")


# ── Console summary table ─────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    cols_dtm = [
        ("L_dispatch_mean_ms", "L_disp mean (ms)"),
        ("L_apply_mean_ms",    "L_apply mean (ms)"),
        ("T_conv_mean_ms",     "T_conv mean (ms)"),
        ("T_conv_p95_ms",      "T_conv P95 (ms)"),
        ("F_sync_pct",         "F_sync (%)"),
    ]
    cols_onm = [
        ("L_relay_mean_ms", "L_relay mean (ms)"),
        ("L_sync_mean_ms",  "L_sync mean (ms)"),
        ("L_sync_p95_ms",   "L_sync P95 (ms)"),
    ]

    rows = []
    for ndts, group in df.groupby("NDTs"):
        row = {"NDTs": ndts, "runs": len(group)}
        for col, label in cols_dtm + cols_onm:
            vals = group[col].replace(
                [float("inf"), float("-inf")], float("nan")).dropna().values
            if len(vals) == 0:
                row[f"{label}"] = "—"
            elif len(vals) == 1:
                row[f"{label}"] = f"{vals[0]:.2f}"
            else:
                mean = np.mean(vals)
                se   = stats.sem(vals)
                t    = stats.t.ppf((1 + CI) / 2, df=len(vals) - 1)
                ci   = t * se
                row[f"{label}"] = f"{mean:.2f} ±{ci:.2f}"
        rows.append(row)

    if rows:
        print("\n" + "=" * 70)
        print(" EXPERIMENT 1 — SYNCHRONIZATION METRICS (mean ± 95% CI)")
        print("=" * 70)
        print(tabulate(pd.DataFrame(rows), headers="keys",
                       tablefmt="psql", showindex=False))


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="Experiment 1 — sync latency analyser (DTM + optional ONM logs)"
    )
    ap.add_argument("--logs", required=True,
                    help="Root folder containing run1/, run2/... with 1toN/ subfolders")
    ap.add_argument("--out",  required=True,
                    help="Output folder for PDFs and CSVs")
    args = ap.parse_args()

    logs_path = Path(args.logs)
    out_path  = Path(args.out)
    out_path.mkdir(parents=True, exist_ok=True)

    print(f"\nLoading logs from: {logs_path}")
    df = load_all_runs(logs_path)

    if df.empty:
        print("\n[ERROR] No valid data found. Check folder structure:")
        print("  exp1/run1/1to5/dtm.log")
        return

    n_runs = df["run"].nunique()
    print(f"\nLoaded {len(df)} experiments across {n_runs} run(s)")

    # Console summary
    print_summary(df)

    # CSV — raw per-run data
    csv_raw = out_path / "exp1_metrics_runs.csv"
    df.to_csv(csv_raw, index=False)
    print(f"\n  -> {csv_raw.name}")

    # CSV — aggregated mean + CI
    agg_rows = []
    all_cols = [
        "L_dispatch_mean_ms", "L_dispatch_p95_ms",
        "L_apply_mean_ms",    "L_apply_p95_ms",
        "T_conv_mean_ms",     "T_conv_p95_ms",
        "F_sync_pct",
        "L_relay_mean_ms",    "L_relay_p95_ms",
        "L_sync_mean_ms",     "L_sync_p95_ms",
    ]
    for ndts, group in df.groupby("NDTs"):
        row = {"NDTs": ndts, "n_runs": len(group)}
        for col in all_cols:
            vals = group[col].replace(
                [float("inf"), float("-inf")], float("nan")).dropna().values
            if len(vals) == 0:
                row[f"{col}_agg_mean"] = float("nan")
                row[f"{col}_agg_ci"]   = float("nan")
            else:
                mean = float(np.mean(vals))
                ci   = 0.0
                if len(vals) > 1:
                    se = stats.sem(vals)
                    t  = stats.t.ppf((1 + CI) / 2, df=len(vals) - 1)
                    ci = float(t * se)
                row[f"{col}_agg_mean"] = round(mean, 3)
                row[f"{col}_agg_ci"]   = round(ci,   3)
        agg_rows.append(row)

    csv_agg = out_path / "exp1_metrics_aggregated.csv"
    pd.DataFrame(agg_rows).to_csv(csv_agg, index=False)
    print(f"  -> {csv_agg.name}")

    # Plots
    print("\nGenerating plots...")
    plot_t_conv(df, out_path)
    plot_l_dispatch(df, out_path)
    plot_l_apply(df, out_path)
    plot_f_sync(df, out_path)
    plot_l_sync(df, out_path)
    plot_l_relay(df, out_path)
    plot_decomposition(df, out_path)

    print(f"\nDone. Results in: {out_path.resolve()}")


if __name__ == "__main__":
    main()