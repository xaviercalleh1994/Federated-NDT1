# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
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
  T_conv      — convergence spread: max(t_apply_i) - min(t_apply_i) per event
  F_sync      — sync fidelity (ok ACKs / dispatched × 100)

From dtm.log + onm.log (optional):
  L_relay     — relay latency (t_rx_ns at DTM - t_tx_ns at ONM)
  L_sync      — end-to-end sync latency (t_apply_ns - t_tx_ns joined on event_id)

Plots generated
---------------
  fig1_l_dispatch_vs_ndts.pdf  — dispatch latency vs N
  fig2_t_conv_vs_ndts.pdf      — convergence spread vs N
  fig3_f_sync_vs_ndts.pdf      — sync fidelity vs N

Folder layout expected
----------------------
exp1/
  run1/
    1to1/    dtm.log  [onm.log]
    1to2/    dtm.log  [onm.log]
    1to4/    dtm.log  [onm.log]
    1to8/    dtm.log  [onm.log]
    1to16/   dtm.log  [onm.log]
    1to32/   dtm.log  [onm.log]
    1to64/   dtm.log  [onm.log]
  run2/
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
from typing import Optional

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
import matplotlib.ticker
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from scipy import stats
from tabulate import tabulate


# ── IEEE-style plot settings ──────────────────────────────────────────────────
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

CI = 0.95


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


# ── Metric extraction ─────────────────────────────────────────────────────────

def compute_metrics(dtm_path: Path, onm_path: Optional[Path],
                    n_ndts: int) -> Optional[dict]:
    """
    Extract synchronization metrics for one experiment folder.
    onm_path is optional — L_relay and L_sync are skipped if absent.
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
        ).dropna() / 1e6
        if not d_ns.empty:
            l_dispatch_mean = float(d_ns.mean())
            l_dispatch_p95  = float(d_ns.quantile(0.95))

    # ── T_conv (convergence spread across NDTs) ───────────────────────────────
    # T_conv = max(t_apply_i) - min(t_apply_i) per event_id, for i = 1..N.
    # With N=1 there is only one NDT so no spread by definition — set to 0
    # so the plot clearly shows T_conv growing from 0 as N increases.
    acks = dtm[dtm["tag"] == "DTM_ACK_RX"].copy()
    t_conv_mean = t_conv_p95 = float("nan")
    if n_ndts == 1:
        t_conv_mean = 0.0
        t_conv_p95  = 0.0
    elif n_ndts >= 2 and not acks.empty and \
            {"event_id", "t_rx_ns", "apply_ms"}.issubset(acks.columns):
        acks_t = acks[["event_id", "t_rx_ns", "apply_ms"]].copy()
        acks_t["event_id"] = acks_t["event_id"].astype(str)
        acks_t["t_rx_ns"]  = pd.to_numeric(acks_t["t_rx_ns"],  errors="coerce")
        acks_t["apply_ms"] = pd.to_numeric(acks_t["apply_ms"], errors="coerce")
        # Reconstruct t_apply: moment DTM received the ACK minus apply time
        acks_t["t_apply_ns"] = acks_t["t_rx_ns"] - (acks_t["apply_ms"] * 1e6)
        acks_t = acks_t.dropna(subset=["t_apply_ns"])
        spread = acks_t.groupby("event_id")["t_apply_ns"].agg(
            lambda x: (x.max() - x.min()) / 1e6 if len(x) >= 2 else float("nan")
        ).dropna()
        if not spread.empty:
            t_conv_mean = float(spread.mean())
            t_conv_p95  = float(spread.quantile(0.95))

    # ── F_sync (synchronization fidelity) ────────────────────────────────────
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
            if not sent.empty and \
                    {"event_id", "t_tx_ns"}.issubset(sent.columns):
                sent = sent[["event_id", "t_tx_ns"]].copy()
                sent["t_tx_ns"] = pd.to_numeric(sent["t_tx_ns"], errors="coerce")
                sent = sent.dropna(subset=["t_tx_ns"])

                # L_relay
                rx = dtm[dtm["tag"] == "NDT_EVENT_RX"].copy()
                if not rx.empty and "event_id" in rx.columns:
                    rx = rx[["event_id", "t_rx_ns"]].copy()
                    rx["t_rx_ns"] = pd.to_numeric(rx["t_rx_ns"], errors="coerce")
                    merged = sent.merge(rx, on="event_id", how="inner")
                    relay_ms = (merged["t_rx_ns"] - merged["t_tx_ns"]) / 1e6
                    relay_ms = relay_ms[relay_ms > 0].dropna()
                    if not relay_ms.empty:
                        l_relay_mean = float(relay_ms.mean())
                        l_relay_p95  = float(relay_ms.quantile(0.95))

                # L_sync
                if not acks.empty and \
                        {"event_id", "t_apply_ns"}.issubset(acks.columns):
                    acks_s = acks[["event_id", "t_apply_ns"]].copy()
                    acks_s["t_apply_ns"] = pd.to_numeric(
                        acks_s["t_apply_ns"], errors="coerce")
                    merged_s = sent.merge(acks_s, on="event_id", how="inner")
                    sync_ms = (merged_s["t_apply_ns"] - merged_s["t_tx_ns"]) / 1e6
                    sync_ms = sync_ms[sync_ms > 0].dropna()
                    if not sync_ms.empty:
                        l_sync_mean = float(sync_ms.mean())
                        l_sync_p95  = float(sync_ms.quantile(0.95))

    return {
        "NDTs":               n_ndts,
        "L_dispatch_mean_ms": round(l_dispatch_mean, 3),
        "L_dispatch_p95_ms":  round(l_dispatch_p95,  3),
        "T_conv_mean_ms":     round(t_conv_mean,      3),
        "T_conv_p95_ms":      round(t_conv_p95,       3),
        "F_sync_pct":         round(f_sync,           2),
        "L_relay_mean_ms":    round(l_relay_mean,     3),
        "L_relay_p95_ms":     round(l_relay_p95,      3),
        "L_sync_mean_ms":     round(l_sync_mean,      3),
        "L_sync_p95_ms":      round(l_sync_p95,       3),
    }


# ── Load all runs ─────────────────────────────────────────────────────────────

def load_all_runs(logs_path: Path) -> pd.DataFrame:
    run_dirs = sorted([
        d for d in logs_path.iterdir()
        if d.is_dir() and d.name.startswith("run")
    ])
    if not run_dirs:
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
            onm_path = exp_dir / "onm.log"
            if not dtm_path.exists():
                print(f"  [SKIP] {exp_dir}: no dtm.log")
                continue
            onm = onm_path if onm_path.exists() else None
            if onm is None:
                print(f"  [INFO] {exp_dir}: no onm.log — L_relay/L_sync skipped")
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
        vals = group[col].replace(
            [float("inf"), float("-inf")], float("nan")).dropna().values
        n = len(vals)
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


# ── Plot helper ───────────────────────────────────────────────────────────────

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


def _set_log2_xaxis(ax) -> None:
    ax.set_xscale("log", base=2)
    ax.xaxis.set_major_formatter(matplotlib.ticker.ScalarFormatter())


# ── Plots ─────────────────────────────────────────────────────────────────────

def plot_l_dispatch(df: pd.DataFrame, outdir: Path) -> None:
    """L_dispatch vs N — DTM fanout overhead."""
    fig, ax = plt.subplots()
    _plot_ci(ax, aggregate(df, "L_dispatch_mean_ms"), "#D85A30", "o", "-",  "Mean")
    _plot_ci(ax, aggregate(df, "L_dispatch_p95_ms"),  "#534AB7", "s", "--", "P95")
    ax.set_xlabel("Number of NDTs (N)")
    ax.set_ylabel("Dispatch latency $L_{dispatch}$ (ms)")
    ax.set_title("DTM dispatch latency vs federation size (95% CI)")
    _set_log2_xaxis(ax)
    ax.legend()
    out = outdir / "fig1_l_dispatch_vs_ndts.pdf"
    fig.savefig(out); plt.close(fig)
    print(f"  -> {out.name}")


def plot_t_conv(df: pd.DataFrame, outdir: Path) -> None:
    """T_conv (convergence spread) vs N."""
    fig, ax = plt.subplots()
    _plot_ci(ax, aggregate(df, "T_conv_mean_ms"), "#185FA5", "o", "-",  "Mean")
    _plot_ci(ax, aggregate(df, "T_conv_p95_ms"),  "#D85A30", "s", "--", "P95")
    ax.set_xlabel("Number of NDTs (N)")
    ax.set_ylabel("Convergence spread $T_{conv}$ (ms)")
    ax.set_title("Convergence spread vs federation size (95% CI)")
    _set_log2_xaxis(ax)
    ax.legend()
    out = outdir / "fig2_t_conv_vs_ndts.pdf"
    fig.savefig(out); plt.close(fig)
    print(f"  -> {out.name}")


def plot_f_sync(df: pd.DataFrame, outdir: Path) -> None:
    """F_sync (%) vs N — synchronization fidelity."""
    fig, ax = plt.subplots()
    _plot_ci(ax, aggregate(df, "F_sync_pct"), "#534AB7", "o", "-", "$F_{sync}$")
    ax.set_xlabel("Number of NDTs (N)")
    ax.set_ylabel("Synchronization fidelity $F_{sync}$ (%)")
    ax.set_title("Sync fidelity vs federation size (95% CI)")
    _set_log2_xaxis(ax)
    ax.set_ylim(0, 105)
    ax.legend()
    out = outdir / "fig3_f_sync_vs_ndts.pdf"
    fig.savefig(out); plt.close(fig)
    print(f"  -> {out.name}")


# ── Console summary table ─────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    cols = [
        ("L_dispatch_mean_ms", "L_disp mean (ms)"),
        ("L_dispatch_p95_ms",  "L_disp P95 (ms)"),
        ("T_conv_mean_ms",     "T_conv mean (ms)"),
        ("T_conv_p95_ms",      "T_conv P95 (ms)"),
        ("F_sync_pct",         "F_sync (%)"),
        ("L_relay_mean_ms",    "L_relay mean (ms)"),
        ("L_sync_mean_ms",     "L_sync mean (ms)"),
    ]
    rows = []
    for ndts, group in df.groupby("NDTs"):
        row = {"NDTs": ndts, "runs": len(group)}
        for col, label in cols:
            vals = group[col].replace(
                [float("inf"), float("-inf")], float("nan")).dropna().values
            if len(vals) == 0:
                row[label] = "—"
            elif len(vals) == 1:
                row[label] = f"{vals[0]:.2f}"
            else:
                mean = np.mean(vals)
                se   = stats.sem(vals)
                t    = stats.t.ppf((1 + CI) / 2, df=len(vals) - 1)
                row[label] = f"{mean:.2f} ±{t * se:.2f}"
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
        description="Experiment 1 — sync latency analyser"
    )
    ap.add_argument("--logs", required=True,
                    help="Root folder with run1/, run2/... containing 1toN/ subfolders")
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
        print("  exp1/run1/1to8/dtm.log")
        return

    print(f"\nLoaded {len(df)} experiments across {df['run'].nunique()} run(s)")
    print_summary(df)

    # CSV — raw per-run data
    df.to_csv(out_path / "exp1_metrics_runs.csv", index=False)
    print(f"\n  -> exp1_metrics_runs.csv")

    # CSV — aggregated mean + CI
    all_cols = [
        "L_dispatch_mean_ms", "L_dispatch_p95_ms",
        "T_conv_mean_ms",     "T_conv_p95_ms",
        "F_sync_pct",
        "L_relay_mean_ms",    "L_relay_p95_ms",
        "L_sync_mean_ms",     "L_sync_p95_ms",
    ]
    agg_rows = []
    for ndts, group in df.groupby("NDTs"):
        row = {"NDTs": ndts, "n_runs": len(group)}
        for col in all_cols:
            vals = group[col].replace(
                [float("inf"), float("-inf")], float("nan")).dropna().values
            mean = float(np.mean(vals)) if len(vals) > 0 else float("nan")
            ci   = 0.0
            if len(vals) > 1:
                se = stats.sem(vals)
                t  = stats.t.ppf((1 + CI) / 2, df=len(vals) - 1)
                ci = float(t * se)
            row[f"{col}_mean"] = round(mean, 3)
            row[f"{col}_ci"]   = round(ci,   3)
        agg_rows.append(row)

    pd.DataFrame(agg_rows).to_csv(out_path / "exp1_metrics_aggregated.csv", index=False)
    print(f"  -> exp1_metrics_aggregated.csv")

    # Plots
    print("\nGenerating plots...")
    plot_l_dispatch(df, out_path)
    plot_t_conv(df, out_path)
    plot_f_sync(df, out_path)

    print(f"\nDone. Results in: {out_path.resolve()}")


if __name__ == "__main__":
    main()