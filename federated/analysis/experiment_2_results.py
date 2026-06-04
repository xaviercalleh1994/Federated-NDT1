# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
"""
Authors: Xavier Calle-Heredia & Xavier Hesselbach
Date: 2026-04-08

experiment_2_results.py — Experiment 2 Analysis
-------------------------------------------------
Analyses ndt_event_cli logs from Experiment 2 (XR service coordination).

Metrics computed per condition (rate × queue_size):
  - e2e_ms     : end-to-end latency (create → ACK received)
  - apply_ms   : application time on ON
  - wait_ms    : time in DTM queue + network
  - ACK rate   : fraction of commands successfully applied
  - DISCARD rate: fraction of commands dropped by DTM queue

Plots generated:
  1. e2e_ms vs rate — per NDT (priority effect)
  2. e2e_ms vs queue_size — per rate
  3. ACK / DISCARD rate vs rate × queue_size
  4. Fairness — e2e CDF per NDT per condition
  5. Heatmap — e2e_avg vs rate × queue_size

Usage:
  python3 experiment_2_results.py --logs /tmp/exp2 --out /tmp/exp2/results
  python3 experiment_2_results.py --logs /tmp/exp2 --out /tmp/exp2/results --show
"""
import os
import re
import json
import argparse
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Optional
from scipy import stats

# ── NDT priority mapping ──────────────────────────────────────
NDT_PRIORITIES = {0: 10, 1: 30, 2: 50, 3: 70, 4: 90}
FANOUT         = 5
COLORS         = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']


# ─────────────────────────────────────────────────────────────
# Log loading
# ─────────────────────────────────────────────────────────────

def parse_condition(cond_name: str) -> Tuple[float, int]:
    """Parse 'rate1_queue4' → (1.0, 4)."""
    m = re.match(r"rate([\d.]+)_queue(\d+)", cond_name)
    if not m:
        return 0.0, 0
    return float(m.group(1)), int(m.group(2))


def load_logs(logs_dir: str) -> pd.DataFrame:
    """
    Load all ndt*.log files from all runs and conditions.
    Returns a DataFrame with one row per command.
    """
    rows = []
    logs_path = Path(logs_dir)

    for run_dir in sorted(logs_path.glob("run*")):
        run_num = int(re.search(r"run(\d+)", run_dir.name).group(1))

        for cond_dir in sorted(run_dir.iterdir()):
            if not cond_dir.is_dir():
                continue
            rate, queue = parse_condition(cond_dir.name)

            for ndt_id in range(FANOUT):
                log_file = cond_dir / f"ndt{ndt_id}.log"
                if not log_file.exists():
                    continue

                with open(log_file) as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            rec = json.loads(line)
                        except Exception:
                            continue

                        rows.append({
                            "run":        run_num,
                            "rate":       rate,
                            "queue_size": queue,
                            "condition":  cond_dir.name,
                            "ndt_id":     ndt_id,
                            "priority":   NDT_PRIORITIES.get(ndt_id, 50),
                            "event_id":   rec.get("event_id", ""),
                            "status":     rec.get("your_status", "UNKNOWN"),
                            "e2e_ms":     rec.get("e2e_ms"),
                            "send_ms":    rec.get("send_ms"),
                            "wait_ms":    rec.get("wait_ms"),
                            "apply_ms":   rec.get("apply_ms"),
                            "t_create_ns":rec.get("t_create_ns", 0),
                        })

    if not rows:
        raise ValueError(f"No logs found in {logs_dir}")

    df = pd.DataFrame(rows)
    print(f"Loaded {len(df)} records from {logs_dir}")
    print(f"  Runs: {sorted(df['run'].unique())}")
    print(f"  Rates: {sorted(df['rate'].unique())}")
    print(f"  Queue sizes: {sorted(df['queue_size'].unique())}")
    print(f"  NDTs: {sorted(df['ndt_id'].unique())}")
    return df


# ─────────────────────────────────────────────────────────────
# Statistics helpers
# ─────────────────────────────────────────────────────────────

def mean_ci95(values: List[float]) -> Tuple[float, float]:
    """Return (mean, half-width of 95% CI)."""
    if len(values) < 2:
        return (values[0] if values else 0.0), 0.0
    m  = np.mean(values)
    se = stats.sem(values)
    ci = se * stats.t.ppf(0.975, df=len(values) - 1)
    return float(m), float(ci)


def aggregate(df: pd.DataFrame, groupby: List[str],
              metric: str) -> pd.DataFrame:
    """
    Aggregate metric over runs: mean ± 95% CI.
    Only includes ACK rows for latency metrics.
    """
    acked = df[df["status"] == "ACK"].copy()
    acked = acked.dropna(subset=[metric])

    out = []
    for keys, group in acked.groupby(groupby):
        vals  = group[metric].tolist()
        m, ci = mean_ci95(vals)
        row   = dict(zip(groupby, keys if isinstance(keys, tuple) else [keys]))
        row[f"{metric}_mean"] = m
        row[f"{metric}_ci95"] = ci
        row["n_acked"]        = len(vals)
        out.append(row)

    return pd.DataFrame(out)


def ack_rate(df: pd.DataFrame, groupby: List[str]) -> pd.DataFrame:
    """Compute ACK / DISCARD / TIMEOUT rates per group."""
    out = []
    for keys, group in df.groupby(groupby):
        total     = len(group)
        acked     = (group["status"] == "ACK").sum()
        discarded = (group["status"] == "DISCARDED").sum()
        timeout   = (group["status"] == "TIMEOUT").sum()
        row = dict(zip(groupby, keys if isinstance(keys, tuple) else [keys]))
        row["total"]        = total
        row["ack_rate"]     = acked / total if total > 0 else 0
        row["discard_rate"] = discarded / total if total > 0 else 0
        row["timeout_rate"] = timeout / total if total > 0 else 0
        out.append(row)
    return pd.DataFrame(out)


# ─────────────────────────────────────────────────────────────
# Quick summary (text)
# ─────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame) -> None:
    print("\n" + "="*70)
    print(" EXPERIMENT 2 SUMMARY")
    print("="*70)

    for (rate, queue), grp in df.groupby(["rate", "queue_size"]):
        print(f"\n  rate={rate} cmd/s  queue={queue}"
              f"  total={len(grp)}")
        for ndt_id in sorted(grp["ndt_id"].unique()):
            ngrp  = grp[grp["ndt_id"] == ndt_id]
            acked = ngrp[ngrp["status"] == "ACK"]
            disc  = (ngrp["status"] == "DISCARDED").sum()
            e2e   = acked["e2e_ms"].dropna().tolist()
            avg   = round(np.mean(e2e), 1) if e2e else None
            prio  = NDT_PRIORITIES.get(ndt_id, "?")
            print(f"    ndt-{ndt_id} (prio={prio:<3}): "
                  f"sent={len(ngrp):<4} "
                  f"ack={len(acked):<4} "
                  f"disc={disc:<3} "
                  f"e2e_avg={avg}ms")
    print()


# ─────────────────────────────────────────────────────────────
# Plot 1 — e2e_ms vs rate, per NDT (priority effect)
# ─────────────────────────────────────────────────────────────

def plot_e2e_vs_rate(df: pd.DataFrame, out_dir: Path,
                     queue_size: int = 0, show: bool = False) -> None:
    subset = df[df["queue_size"] == queue_size]
    agg    = aggregate(subset, ["rate", "ndt_id"], "e2e_ms")

    fig, ax = plt.subplots(figsize=(8, 5))
    for ndt_id in sorted(agg["ndt_id"].unique()):
        ngrp  = agg[agg["ndt_id"] == ndt_id].sort_values("rate")
        prio  = NDT_PRIORITIES.get(int(ndt_id), "?")
        ax.errorbar(ngrp["rate"], ngrp["e2e_ms_mean"],
                    yerr=ngrp["e2e_ms_ci95"],
                    marker="o", label=f"ndt-{ndt_id} (prio={prio})",
                    color=COLORS[int(ndt_id) % len(COLORS)],
                    capsize=4, linewidth=1.5)

    ax.set_xlabel("Rate (cmd/s per NDT)", fontsize=12)
    ax.set_ylabel("e2e latency (ms)", fontsize=12)
    ax.set_title(f"E2E Latency vs Rate — queue_size={queue_size}\n"
                 f"(mean ± 95% CI)", fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path = out_dir / f"e2e_vs_rate_queue{queue_size}.png"
    fig.savefig(path, dpi=150)
    print(f"  Saved: {path}")
    if show:
        plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 2 — e2e_ms vs queue_size, per rate
# ─────────────────────────────────────────────────────────────

def plot_e2e_vs_queue(df: pd.DataFrame, out_dir: Path,
                      show: bool = False) -> None:
    agg   = aggregate(df, ["rate", "queue_size"], "e2e_ms")
    rates = sorted(agg["rate"].unique())

    fig, ax = plt.subplots(figsize=(8, 5))
    for i, rate in enumerate(rates):
        rgrp = agg[agg["rate"] == rate].sort_values("queue_size")
        ax.errorbar(rgrp["queue_size"], rgrp["e2e_ms_mean"],
                    yerr=rgrp["e2e_ms_ci95"],
                    marker="s", label=f"rate={rate} cmd/s",
                    color=COLORS[i % len(COLORS)],
                    capsize=4, linewidth=1.5)

    ax.set_xlabel("Queue size (0=unlimited)", fontsize=12)
    ax.set_ylabel("e2e latency (ms)", fontsize=12)
    ax.set_title("E2E Latency vs Queue Size\n(mean ± 95% CI)", fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path = out_dir / "e2e_vs_queue.png"
    fig.savefig(path, dpi=150)
    print(f"  Saved: {path}")
    if show:
        plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 3 — ACK / DISCARD rate vs conditions
# ─────────────────────────────────────────────────────────────

def plot_ack_discard(df: pd.DataFrame, out_dir: Path,
                     show: bool = False) -> None:
    rates  = sorted(df["rate"].unique())
    queues = sorted(df["queue_size"].unique())
    ar     = ack_rate(df, ["rate", "queue_size"])

    x      = np.arange(len(rates))
    width  = 0.8 / len(queues)

    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    for j, queue in enumerate(queues):
        qgrp = ar[ar["queue_size"] == queue].sort_values("rate")
        offset = (j - len(queues)/2 + 0.5) * width
        axes[0].bar(x + offset, qgrp["ack_rate"] * 100,
                    width=width*0.9, label=f"queue={queue}",
                    color=COLORS[j % len(COLORS)], alpha=0.8)
        axes[1].bar(x + offset, qgrp["discard_rate"] * 100,
                    width=width*0.9, label=f"queue={queue}",
                    color=COLORS[j % len(COLORS)], alpha=0.8)

    for ax, title, ylabel in zip(
        axes,
        ["ACK Rate vs Rate × Queue", "DISCARD Rate vs Rate × Queue"],
        ["ACK rate (%)", "Discard rate (%)"]
    ):
        ax.set_xticks(x)
        ax.set_xticklabels([f"{r}" for r in rates])
        ax.set_xlabel("Rate (cmd/s per NDT)", fontsize=11)
        ax.set_ylabel(ylabel, fontsize=11)
        ax.set_title(title, fontsize=11)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3, axis="y")
        ax.set_ylim(0, 105)

    fig.tight_layout()
    path = out_dir / "ack_discard_rate.png"
    fig.savefig(path, dpi=150)
    print(f"  Saved: {path}")
    if show:
        plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 4 — CDF of e2e_ms per NDT (fairness)
# ─────────────────────────────────────────────────────────────

def plot_cdf_fairness(df: pd.DataFrame, out_dir: Path,
                      rate: float = 1.0, queue_size: int = 0,
                      show: bool = False) -> None:
    subset = df[(df["rate"] == rate) &
                (df["queue_size"] == queue_size) &
                (df["status"] == "ACK")].dropna(subset=["e2e_ms"])

    if subset.empty:
        print(f"  No ACK data for rate={rate} queue={queue_size} — skipping CDF")
        return

    fig, ax = plt.subplots(figsize=(8, 5))
    for ndt_id in sorted(subset["ndt_id"].unique()):
        vals = sorted(subset[subset["ndt_id"] == ndt_id]["e2e_ms"].tolist())
        if not vals:
            continue
        cdf  = np.arange(1, len(vals) + 1) / len(vals)
        prio = NDT_PRIORITIES.get(int(ndt_id), "?")
        ax.plot(vals, cdf,
                label=f"ndt-{ndt_id} (prio={prio})",
                color=COLORS[int(ndt_id) % len(COLORS)],
                linewidth=1.5)

    ax.set_xlabel("e2e latency (ms)", fontsize=12)
    ax.set_ylabel("CDF", fontsize=12)
    ax.set_title(f"Fairness — CDF of e2e latency per NDT\n"
                 f"rate={rate} cmd/s  queue={queue_size}", fontsize=12)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3)
    ax.set_ylim(0, 1.05)
    fig.tight_layout()
    path = out_dir / f"cdf_fairness_rate{rate}_queue{queue_size}.png"
    fig.savefig(path, dpi=150)
    print(f"  Saved: {path}")
    if show:
        plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 5 — Heatmap e2e_avg vs rate × queue_size
# ─────────────────────────────────────────────────────────────

def plot_heatmap(df: pd.DataFrame, out_dir: Path,
                 show: bool = False) -> None:
    agg    = aggregate(df, ["rate", "queue_size"], "e2e_ms")
    rates  = sorted(agg["rate"].unique())
    queues = sorted(agg["queue_size"].unique())

    matrix = np.full((len(queues), len(rates)), np.nan)
    for _, row in agg.iterrows():
        i = queues.index(row["queue_size"])
        j = rates.index(row["rate"])
        matrix[i, j] = row["e2e_ms_mean"]

    fig, ax = plt.subplots(figsize=(8, 5))
    im = ax.imshow(matrix, aspect="auto", cmap="YlOrRd")
    plt.colorbar(im, ax=ax, label="e2e_ms mean")

    ax.set_xticks(range(len(rates)))
    ax.set_xticklabels([str(r) for r in rates])
    ax.set_yticks(range(len(queues)))
    ax.set_yticklabels([str(q) for q in queues])
    ax.set_xlabel("Rate (cmd/s per NDT)", fontsize=12)
    ax.set_ylabel("Queue size (0=unlimited)", fontsize=12)
    ax.set_title("Heatmap — Mean e2e latency (ms)\n"
                 "rate × queue_size", fontsize=12)

    for i in range(len(queues)):
        for j in range(len(rates)):
            val = matrix[i, j]
            if not np.isnan(val):
                ax.text(j, i, f"{val:.1f}", ha="center", va="center",
                        fontsize=10, color="black")

    fig.tight_layout()
    path = out_dir / "heatmap_e2e.png"
    fig.savefig(path, dpi=150)
    print(f"  Saved: {path}")
    if show:
        plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 6 — Priority effect: e2e by NDT at fixed rate
# ─────────────────────────────────────────────────────────────

def plot_priority_effect(df: pd.DataFrame, out_dir: Path,
                         show: bool = False) -> None:
    rates  = sorted(df["rate"].unique())
    queues = sorted(df["queue_size"].unique())

    for queue in queues:
        fig, axes = plt.subplots(1, len(rates),
                                 figsize=(4 * len(rates), 5),
                                 sharey=True)
        if len(rates) == 1:
            axes = [axes]

        for ax, rate in zip(axes, rates):
            subset = df[(df["rate"] == rate) &
                        (df["queue_size"] == queue) &
                        (df["status"] == "ACK")].dropna(subset=["e2e_ms"])

            ndt_ids   = sorted(subset["ndt_id"].unique())
            means, cis = [], []
            for ndt_id in ndt_ids:
                vals = subset[subset["ndt_id"] == ndt_id]["e2e_ms"].tolist()
                m, ci = mean_ci95(vals) if vals else (0, 0)
                means.append(m)
                cis.append(ci)

            x     = np.arange(len(ndt_ids))
            prios = [NDT_PRIORITIES.get(int(n), "?") for n in ndt_ids]
            bars  = ax.bar(x, means, yerr=cis,
                           color=[COLORS[int(n) % len(COLORS)] for n in ndt_ids],
                           capsize=5, alpha=0.85)
            ax.set_xticks(x)
            ax.set_xticklabels([f"ndt-{n}\n(p={p})" for n, p in
                                 zip(ndt_ids, prios)], fontsize=9)
            ax.set_title(f"rate={rate}", fontsize=10)
            ax.set_xlabel("NDT (priority)", fontsize=10)
            ax.grid(True, alpha=0.3, axis="y")

        axes[0].set_ylabel("e2e latency (ms)", fontsize=11)
        fig.suptitle(f"Priority Effect on e2e Latency — queue={queue}",
                     fontsize=12)
        fig.tight_layout()
        path = out_dir / f"priority_effect_queue{queue}.png"
        fig.savefig(path, dpi=150)
        print(f"  Saved: {path}")
        if show:
            plt.show()
        plt.close(fig)


# ─────────────────────────────────────────────────────────────
# CSV export
# ─────────────────────────────────────────────────────────────

def export_csv(df: pd.DataFrame, out_dir: Path) -> None:
    # Full log
    df.to_csv(out_dir / "all_records.csv", index=False)
    print(f"  Saved: {out_dir / 'all_records.csv'}")

    # Aggregated summary
    agg_e2e  = aggregate(df, ["rate", "queue_size", "ndt_id"], "e2e_ms")
    agg_wait = aggregate(df, ["rate", "queue_size", "ndt_id"], "wait_ms")
    agg_app  = aggregate(df, ["rate", "queue_size", "ndt_id"], "apply_ms")
    ar       = ack_rate(df, ["rate", "queue_size", "ndt_id"])

    summary = agg_e2e.merge(
        agg_wait[["rate","queue_size","ndt_id","wait_ms_mean","wait_ms_ci95"]],
        on=["rate","queue_size","ndt_id"], how="left"
    ).merge(
        agg_app[["rate","queue_size","ndt_id","apply_ms_mean","apply_ms_ci95"]],
        on=["rate","queue_size","ndt_id"], how="left"
    ).merge(
        ar[["rate","queue_size","ndt_id","total","ack_rate","discard_rate"]],
        on=["rate","queue_size","ndt_id"], how="left"
    )
    summary["priority"] = summary["ndt_id"].map(NDT_PRIORITIES)
    summary = summary.sort_values(["rate","queue_size","ndt_id"])
    summary.to_csv(out_dir / "summary.csv", index=False)
    print(f"  Saved: {out_dir / 'summary.csv'}")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="experiment_2_results — Exp.2 analysis and plots"
    )
    ap.add_argument("--logs", required=True,
                    help="Root directory of experiment logs (e.g. /tmp/exp2)")
    ap.add_argument("--out",  required=True,
                    help="Output directory for plots and CSVs")
    ap.add_argument("--show", action="store_true",
                    help="Show plots interactively")
    ap.add_argument("--queue-for-cdf", type=int, default=0,
                    help="Queue size to use for CDF fairness plot (default: 0)")
    ap.add_argument("--rate-for-cdf", type=float, default=1.0,
                    help="Rate to use for CDF fairness plot (default: 1.0)")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load
    df = load_logs(args.logs)

    # Summary
    print_summary(df)

    print(f"\nGenerating plots → {out_dir}")

    # Plot 1 — e2e vs rate per NDT (all queue sizes)
    for queue in sorted(df["queue_size"].unique()):
        plot_e2e_vs_rate(df, out_dir, queue_size=int(queue), show=args.show)

    # Plot 2 — e2e vs queue per rate
    plot_e2e_vs_queue(df, out_dir, show=args.show)

    # Plot 3 — ACK/DISCARD rates
    plot_ack_discard(df, out_dir, show=args.show)

    # Plot 4 — CDF fairness
    plot_cdf_fairness(df, out_dir,
                      rate=args.rate_for_cdf,
                      queue_size=args.queue_for_cdf,
                      show=args.show)

    # Plot 5 — Heatmap
    plot_heatmap(df, out_dir, show=args.show)

    # Plot 6 — Priority effect
    plot_priority_effect(df, out_dir, show=args.show)

    # CSV export
    print("\nExporting CSVs...")
    export_csv(df, out_dir)

    print(f"\nDone. Results in: {out_dir}")
    print("Files:")
    for f in sorted(out_dir.iterdir()):
        print(f"  {f.name}")


if __name__ == "__main__":
    main()