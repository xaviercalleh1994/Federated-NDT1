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

Key results demonstrated:
  1. E2E latency vs load (rate) — system saturation point
  2. Throughput analysis — effective cmd/s from DTM upstream logs
  3. Queue wait time (queue_ms) — DTM coordination overhead
  4. System stability — ACK rate, 0 DISCARDs under load
  5. Priority vs FIFO comparison (if both modes present)
  6. Per-NDT fairness — CDF of e2e_ms

Usage:
  python3 experiment_2_results.py --logs /tmp/exp2 --out /tmp/exp2/results
  python3 experiment_2_results.py --logs /tmp/exp2 --out /tmp/exp2/results --show

# ── FIX (2026-07-22) ────────────────────────────────────────────
# This file previously contained its entire contents duplicated
# (everything from the header down to `if __name__ == "__main__":
# main()` appeared twice, back to back). Since the duplicate also
# ended in its own `if __name__ == "__main__": main()`, running the
# script executed main() TWICE per invocation — printing the
# EXPERIMENT 2 SUMMARY block twice and regenerating identical plots
# and CSVs twice (harmless to the output files themselves, since the
# second pass just overwrites the first with the same data, but it
# doubled runtime and made terminal output confusing to read/paste).
# Deduplicated back down to a single copy of every function and a
# single `if __name__` entry point.
# ─────────────────────────────────────────────────────────────────
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
from typing import Dict, List, Tuple, Optional
from scipy import stats

# ── Style ────────────────────────────────────────────────────
# Journal-appropriate look: serif font metrically compatible with
# Times (the IEEE/Elsevier default body font), larger base sizes
# tuned for a 2-column print layout, muted spines, and a
# colorblind-safe qualitative palette (Okabe-Ito) instead of
# matplotlib's default tab10 — holds up in grayscale print too when
# combined with the hatch patterns defined below.
plt.rcParams.update({
    'font.family':       'serif',
    'font.serif':        ['Liberation Serif', 'Times New Roman', 'DejaVu Serif'],
    'font.size':         12,
    'axes.titlesize':    13,
    'axes.labelsize':    12,
    'legend.fontsize':   10.5,
    'xtick.labelsize':   11,
    'ytick.labelsize':   11,
    'figure.dpi':        150,
    'savefig.dpi':       300,
    'axes.spines.top':   False,
    'axes.spines.right': False,
    'axes.linewidth':    0.9,
    'axes.edgecolor':    '#333333',
    'axes.grid':         True,
    'grid.alpha':        0.25,
    'grid.linewidth':    0.6,
    'legend.frameon':    True,
    'legend.facecolor':  'white',
    'legend.edgecolor':  '#cccccc',
    'legend.framealpha': 0.92,
    'axes.axisbelow':    True,
})

NDT_PRIORITIES = {0: 10, 1: 30, 2: 50, 3: 70}
# Okabe-Ito palette — colorblind-safe, standard in Q1 venues
COLORS  = ['#0072B2', '#E69F00', '#009E73', '#D55E00', '#CC79A7']
MARKERS = ['o', 's', '^', 'D', 'v']
HATCHES = ['', '///', '\\\\\\', 'xxx', '...']  # per-NDT, B&W-safe redundancy


def _save(fig, out_dir: Path, stem: str, also_pdf: bool = True) -> None:
    """Save a figure as high-res PNG for screen/quick review and, by
    default, also as a vector PDF — camera-ready venues want vector
    figures, and PDF never loses quality when the layout team rescales
    it into a 2-column template.

    # FIX (2026-07-30): PNG and PDF used to land side by side in the
    # same flat folder, which looked like every figure was duplicated
    # when browsing the output dir. They now go into png/ and pdf/
    # subfolders so the top-level listing shows one entry per figure.
    """
    png_dir = out_dir / "png"
    png_dir.mkdir(parents=True, exist_ok=True)
    png_path = png_dir / f"{stem}.png"
    fig.savefig(png_path, dpi=300, bbox_inches="tight")
    print(f"  Saved: {png_path}")
    if also_pdf:
        pdf_dir = out_dir / "pdf"
        pdf_dir.mkdir(parents=True, exist_ok=True)
        pdf_path = pdf_dir / f"{stem}.pdf"
        fig.savefig(pdf_path, bbox_inches="tight")
        print(f"  Saved: {pdf_path}")


# ─────────────────────────────────────────────────────────────
# Log loading
# ─────────────────────────────────────────────────────────────

def parse_condition(name: str) -> Tuple[float, int, str]:
    """Parse 'rate20_queue0_priority' → (20.0, 0, 'priority')."""
    m = re.match(r"rate([\d.]+)_queue(\d+)_(\w+)", name)
    if m:
        return float(m.group(1)), int(m.group(2)), m.group(3)
    m2 = re.match(r"rate([\d.]+)_queue(\d+)", name)
    if m2:
        return float(m2.group(1)), int(m2.group(2)), "priority"
    return 0.0, 0, "priority"


def _iter_run_dirs(logs_dir: str):
    """Yield (source_idx, run_num, run_dir_path) across one or more
    comma-separated log roots, so results from separate invocations
    of run_experiment2.sh (e.g. a rate x queue sweep AND a separate
    long single-condition run) can be combined into one analysis.
    source_idx disambiguates 'run1' colliding between roots.
    """
    roots = [r.strip() for r in logs_dir.split(",") if r.strip()]
    for src_idx, root in enumerate(roots):
        for run_dir in sorted(Path(root).glob("run*")):
            m = re.search(r"run(\d+)", run_dir.name)
            run_num = int(m.group(1)) if m else 0
            yield src_idx, run_num, run_dir


def load_client_logs(logs_dir: str) -> pd.DataFrame:
    """Load ndt*.log files — one row per command. logs_dir may be a
    single path or a comma-separated list of paths (see
    _iter_run_dirs)."""
    rows = []
    for src_idx, run_num, run_dir in _iter_run_dirs(logs_dir):
        for cond_dir in sorted(run_dir.iterdir()):
            if not cond_dir.is_dir():
                continue
            rate, queue, prio_mode = parse_condition(cond_dir.name)
            for ndt_id in range(10):
                log_file = cond_dir / f"ndt{ndt_id}.log"
                if not log_file.exists():
                    continue
                for line in open(log_file):
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rec = json.loads(line)
                    except Exception:
                        continue
                    rows.append({
                        "run":       f"{src_idx}-{run_num}",
                        "rate":      rate,
                        "queue":     queue,
                        "prio_mode": prio_mode,
                        "condition": cond_dir.name,
                        "ndt_id":    ndt_id,
                        "priority":  NDT_PRIORITIES.get(ndt_id, 50 + ndt_id*20),
                        "status":    rec.get("your_status", "UNKNOWN"),
                        "e2e_ms":    rec.get("e2e_ms"),
                        "wait_ms":   rec.get("wait_ms"),
                        "apply_ms":  rec.get("apply_ms"),
                        "t_create":  rec.get("t_create_ns", 0),
                    })
    if not rows:
        raise ValueError(f"No client logs found in {logs_dir}")
    df = pd.DataFrame(rows)
    print(f"Loaded {len(df):,} client records")
    print(f"  Rates: {sorted(df['rate'].unique())}")
    print(f"  Queues: {sorted(df['queue'].unique())}")
    print(f"  Modes: {sorted(df['prio_mode'].unique())}")
    return df


def load_dtm_logs(logs_dir: str) -> pd.DataFrame:
    """Load dtm_upstream.log files — one row per processed command.
    logs_dir may be a single path or a comma-separated list of paths."""
    rows = []
    for src_idx, run_num, run_dir in _iter_run_dirs(logs_dir):
        for cond_dir in sorted(run_dir.iterdir()):
            if not cond_dir.is_dir():
                continue
            rate, queue, prio_mode = parse_condition(cond_dir.name)
            log_file = cond_dir / "dtm_upstream.log"
            if not log_file.exists():
                continue
            for line in open(log_file):
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except Exception:
                    continue
                tag = rec.get("tag", "")
                rows.append({
                    "run":       f"{src_idx}-{run_num}",
                    "rate":      rate,
                    "queue":     queue,
                    "prio_mode": prio_mode,
                    "tag":       tag,
                    "ndt_id":    rec.get("ndt_id"),
                    "apply_ms":  rec.get("apply_ms"),
                    "queue_ms":  rec.get("queue_ms"),
                    "e2e_ms":    rec.get("e2e_ms"),
                    "ok":        rec.get("ok"),
                    "reason":    rec.get("reason"),
                    "max_queue": rec.get("max_queue", 0),
                    "disable_p": rec.get("disable_priorities", False),
                    "t_done_ns": rec.get("t_done_ns", 0),
                    "t_rx_ns":   rec.get("t_rx_ns", 0),
                })
    if not rows:
        print("  WARNING: No DTM upstream logs found")
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    print(f"Loaded {len(df):,} DTM upstream records")
    return df


# ─────────────────────────────────────────────────────────────
# Statistics
# ─────────────────────────────────────────────────────────────

def mean_ci95(values: List[float]) -> Tuple[float, float]:
    if len(values) < 2:
        return (float(values[0]) if values else 0.0), 0.0
    m  = np.mean(values)
    ci = stats.sem(values) * stats.t.ppf(0.975, df=len(values)-1)
    return float(m), float(ci)


def agg_metric(df: pd.DataFrame, groupby: List[str],
               metric: str, status_filter: str = "ACK") -> pd.DataFrame:
    if status_filter:
        df = df[df["status"] == status_filter]
    df = df.dropna(subset=[metric])
    rows = []
    for keys, grp in df.groupby(groupby):
        vals = grp[metric].tolist()
        m, ci = mean_ci95(vals)
        row = dict(zip(groupby, keys if isinstance(keys, tuple) else [keys]))
        row[f"{metric}_mean"] = m
        row[f"{metric}_ci95"] = ci
        row["n"] = len(vals)
        rows.append(row)
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────

def print_summary(df: pd.DataFrame, dtm: pd.DataFrame) -> None:
    print("\n" + "="*70)
    print(" EXPERIMENT 2 SUMMARY")
    print("="*70)
    for (rate, queue, mode), grp in df.groupby(["rate","queue","prio_mode"]):
        total = len(grp)
        print(f"\n  rate={rate} cmd/s  queue={queue}  mode={mode}  total={total}")
        for ndt_id in sorted(grp["ndt_id"].unique()):
            ng    = grp[grp["ndt_id"]==ndt_id]
            acked = ng[ng["status"]=="ACK"]
            disc  = (ng["status"]=="DISCARDED").sum()
            e2e   = acked["e2e_ms"].dropna().tolist()
            avg   = round(np.mean(e2e),1) if e2e else None
            prio  = NDT_PRIORITIES.get(ndt_id,"?")
            print(f"    ndt-{ndt_id} (prio={prio:<3}): "
                  f"sent={len(ng):<6} ack={len(acked):<6} disc={disc:<4} "
                  f"e2e_avg={avg}ms")

    if not dtm.empty:
        print("\n── DTM Upstream Analysis ──")
        done = dtm[dtm["tag"]=="DTM_UPSTREAM_DONE"]
        drops = dtm[dtm["tag"]=="DTM_DROP"]
        for (rate,queue,mode), grp in done.groupby(["rate","queue","prio_mode"]):
            qms = grp["queue_ms"].dropna().tolist()
            d = drops[(drops["rate"]==rate)&(drops["queue"]==queue)&
                      (drops["prio_mode"]==mode)]
            print(f"  rate={rate} queue={queue} mode={mode}: "
                  f"processed={len(grp)} drops={len(d)} "
                  f"queue_ms_avg={round(np.mean(qms),1) if qms else 'N/A'}ms "
                  f"queue_ms_max={round(max(qms),1) if qms else 'N/A'}ms")


# ─────────────────────────────────────────────────────────────
# Plot 1 — E2E latency vs rate (main result)
# ─────────────────────────────────────────────────────────────

def plot_e2e_vs_rate(df: pd.DataFrame, out_dir: Path,
                     queue: int = 0, show: bool = False) -> None:
    """E2E latency vs rate — shows system saturation point."""
    sub  = df[(df["queue"]==queue) & (df["status"]=="ACK")]
    modes = sorted(sub["prio_mode"].unique())

    fig, ax = plt.subplots(figsize=(8,5))

    for mi, mode in enumerate(modes):
        agg = agg_metric(
            sub[sub["prio_mode"]==mode],
            ["rate"], "e2e_ms"
        ).sort_values("rate")
        ls = "-" if mode == "priority" else "--"
        ax.errorbar(agg["rate"], agg["e2e_ms_mean"],
                    yerr=agg["e2e_ms_ci95"],
                    marker=MARKERS[mi], linestyle=ls,
                    color=COLORS[mi], capsize=4, linewidth=2,
                    label=f"Mode: {mode}")

    ax.set_xlabel("Command rate per NDT (cmd/s)", fontsize=12)
    ax.set_ylabel("E2E latency (ms)", fontsize=12)
    ax.set_title(f"E2E Latency vs Load — queue={queue}\n"
                 f"(mean ± 95% CI, {df['run'].nunique()} runs)", fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    _save(fig, out_dir, f"e2e_vs_rate_queue{queue}")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 2 — Priority effect: e2e per NDT per rate
# ─────────────────────────────────────────────────────────────

def plot_priority_effect(df: pd.DataFrame, out_dir: Path,
                         queue: int = 0, show: bool = False) -> None:
    """Shows priority effect on e2e per NDT at each rate.

    # FIX (2026-07-22): under strict priority + saturation, the
    # lowest-priority NDT's e2e mean can be 1-2 orders of magnitude
    # above the others in the same panel (e.g. rate=150: ~7ms vs
    # ~3300ms), which flattened every other bar to invisible on a
    # linear axis. Each panel now switches to a log y-axis when its
    # own bars span more than a 20x ratio, and every bar gets its
    # mean value annotated on top (via ax.annotate with an
    # offset-points textcoord, so it stays legible on both linear
    # and log axes) so the numbers are readable even when the bar
    # itself is visually tiny.
    """
    modes = sorted(df["prio_mode"].unique())
    rates = sorted(df["rate"].unique())

    for mode in modes:
        sub = df[(df["queue"]==queue) &
                 (df["prio_mode"]==mode) &
                 (df["status"]=="ACK")]

        fig, axes = plt.subplots(1, len(rates),
                                 figsize=(4*len(rates), 5), sharey=False)
        if len(rates) == 1:
            axes = [axes]

        for ax, rate in zip(axes, rates):
            rgrp   = sub[sub["rate"]==rate]
            ndts   = sorted(rgrp["ndt_id"].unique())
            means, cis = [], []
            for ndt_id in ndts:
                vals = rgrp[rgrp["ndt_id"]==ndt_id]["e2e_ms"].dropna().tolist()
                m, ci = mean_ci95(vals) if vals else (0, 0)
                means.append(m)
                cis.append(ci)

            x     = np.arange(len(ndts))
            prios = [NDT_PRIORITIES.get(n, "?") for n in ndts]
            bars  = ax.bar(x, means, yerr=cis,
                   color=[COLORS[n % len(COLORS)] for n in ndts],
                   hatch=[HATCHES[n % len(HATCHES)] for n in ndts],
                   edgecolor="black", linewidth=0.8,
                   capsize=5, alpha=0.9,
                   error_kw={"linewidth": 1.2, "ecolor": "#222222"})
            ax.set_xticks(x)
            ax.set_xticklabels([f"ndt-{n}\n(p={p})"
                                 for n,p in zip(ndts,prios)], fontsize=9)
            ax.set_title(f"rate={rate} cmd/s", fontsize=10.5, fontweight="bold")
            ax.grid(True, alpha=0.25, axis="y")

            # adaptive log scale: only when this panel's own bars
            # span more than ~20x, so low-load panels (flat, all
            # similar) stay linear and readable
            nonzero = [m for m in means if m > 0]
            if nonzero and max(nonzero) / min(nonzero) > 20:
                ax.set_yscale('log')

            # annotate each bar with its mean value regardless of
            # scale — offset in points, not data units, so it works
            # the same on linear and log axes
            for xi, m in zip(x, means):
                if m <= 0:
                    continue
                ax.annotate(f"{m:.1f}", xy=(xi, m), xytext=(0, 3),
                            textcoords="offset points",
                            ha="center", fontsize=8)

        axes[0].set_ylabel("E2E latency (ms)", fontsize=11)
        fig.suptitle(f"Priority Effect on E2E Latency — mode={mode} queue={queue}",
                     fontsize=12)
        fig.tight_layout()
        _save(fig, out_dir, f"priority_effect_{mode}_queue{queue}")
        if show: plt.show()
        plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 3 — Queue wait time from DTM logs
# ─────────────────────────────────────────────────────────────

def plot_queue_ms(dtm: pd.DataFrame, out_dir: Path,
                  show: bool = False) -> None:
    """Queue wait time vs rate — shows DTM coordination overhead."""
    if dtm.empty:
        print("  Skipping queue_ms plot — no DTM logs")
        return

    done  = dtm[dtm["tag"]=="DTM_UPSTREAM_DONE"].dropna(subset=["queue_ms"])
    modes = sorted(done["prio_mode"].unique())

    fig, ax = plt.subplots(figsize=(8,5))

    for mi, mode in enumerate(modes):
        sub = done[done["prio_mode"]==mode]
        agg = []
        for rate, grp in sub.groupby("rate"):
            vals = grp["queue_ms"].tolist()
            m, ci = mean_ci95(vals)
            agg.append({"rate": rate, "mean": m, "ci": ci})
        agg = pd.DataFrame(agg).sort_values("rate")

        ls = "-" if mode == "priority" else "--"
        ax.errorbar(agg["rate"], agg["mean"], yerr=agg["ci"],
                    marker=MARKERS[mi], linestyle=ls,
                    color=COLORS[mi], capsize=4, linewidth=2,
                    label=f"Mode: {mode}")

    ax.set_xlabel("Command rate per NDT (cmd/s)", fontsize=12)
    ax.set_ylabel("Queue wait time (ms)", fontsize=12)
    ax.set_title("DTM AdmissionQueue Wait Time vs Load\n"
                 "(mean ± 95% CI)", fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    _save(fig, out_dir, "queue_ms_vs_rate")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 4 — Effective throughput from DTM logs
# ─────────────────────────────────────────────────────────────

def plot_throughput(dtm: pd.DataFrame, out_dir: Path,
                    show: bool = False) -> None:
    """Effective throughput processed by DTM upstream vs offered load."""
    if dtm.empty:
        print("  Skipping throughput plot — no DTM logs")
        return

    done  = dtm[dtm["tag"]=="DTM_UPSTREAM_DONE"]
    modes = sorted(done["prio_mode"].unique())

    fig, ax = plt.subplots(figsize=(8,5))

    for mi, mode in enumerate(modes):
        sub = done[done["prio_mode"]==mode]
        agg = []
        for (rate, run), grp in sub.groupby(["rate","run"]):
            if grp["t_done_ns"].max() > 0 and grp["t_rx_ns"].min() > 0:
                duration_s = (grp["t_done_ns"].max() -
                              grp["t_rx_ns"].min()) / 1e9
                if duration_s > 0:
                    tput = len(grp) / duration_s
                    agg.append({"rate": rate, "throughput": tput})

        if not agg:
            # Fallback: use count/120s
            for rate, grp in sub.groupby("rate"):
                agg.append({"rate": rate,
                             "throughput": len(grp) / (grp["run"].nunique() * 120)})

        agg_df = pd.DataFrame(agg)
        grp_agg = []
        for rate, g in agg_df.groupby("rate"):
            m, ci = mean_ci95(g["throughput"].tolist())
            grp_agg.append({"rate": rate, "mean": m, "ci": ci})
        grp_agg = pd.DataFrame(grp_agg).sort_values("rate")

        ls = "-" if mode == "priority" else "--"
        ax.errorbar(grp_agg["rate"], grp_agg["mean"], yerr=grp_agg["ci"],
                    marker=MARKERS[mi], linestyle=ls,
                    color=COLORS[mi], capsize=4, linewidth=2,
                    label=f"Mode: {mode}")

    # Ideal line (y=x × NDTs)
    rates = sorted(done["rate"].unique())
    n_ndts = dtm["ndt_id"].nunique() if "ndt_id" in dtm else 4
    ax.plot(rates, [r * n_ndts for r in rates], 'k--',
            alpha=0.4, label=f"Ideal ({n_ndts} NDTs)", linewidth=1)

    ax.set_xlabel("Command rate per NDT (cmd/s)", fontsize=12)
    ax.set_ylabel("Effective throughput (cmd/s)", fontsize=12)
    ax.set_title("Effective Throughput vs Offered Load\n"
                 "(mean ± 95% CI)", fontsize=12)
    ax.legend()
    ax.grid(True, alpha=0.3)
    ax.set_ylim(bottom=0)
    fig.tight_layout()
    _save(fig, out_dir, "throughput_vs_rate")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 5a — ACK rate, aggregate (system-level stability overview)
# ─────────────────────────────────────────────────────────────

def plot_ack_rate(df: pd.DataFrame, out_dir: Path,
                  show: bool = False) -> None:
    """Aggregate ACK rate vs rate — system-level stability overview.

    NOTE: this averages across all 4 NDTs, so under strict priority
    it can look deceptively healthy (e.g. ~85% overall) while masking
    that ndt-0/ndt-1 are at ~100% and ndt-3 is near 0%. Use this only
    as a system-wide sanity check; see plot_success_rate_per_ndt for
    the metric that actually shows the fairness story.
    """
    modes = sorted(df["prio_mode"].unique())
    fig, ax = plt.subplots(figsize=(8,5))

    for mi, mode in enumerate(modes):
        sub = df[df["prio_mode"]==mode]
        agg = []
        for rate, grp in sub.groupby("rate"):
            total  = len(grp)
            acked  = (grp["status"]=="ACK").sum()
            agg.append({"rate": rate,
                         "ack_rate": acked/total*100 if total else 0})
        agg = pd.DataFrame(agg).sort_values("rate")
        ls = "-" if mode == "priority" else "--"
        ax.plot(agg["rate"], agg["ack_rate"],
                marker=MARKERS[mi], linestyle=ls,
                color=COLORS[mi], linewidth=2, label=f"Mode: {mode}")

    ax.set_xlabel("Command rate per NDT (cmd/s)", fontsize=12)
    ax.set_ylabel("ACK rate (%)", fontsize=12)
    ax.set_title("System Stability — Aggregate ACK Rate vs Load\n"
                 "(see success_rate_per_ndt for per-NDT breakdown)",
                 fontsize=11)
    ax.set_ylim(0, 105)
    ax.axhline(100, color='gray', linestyle=':', alpha=0.5)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    _save(fig, out_dir, "ack_rate_vs_rate")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 5b — Success rate PER NDT (the real fairness metric)
# ─────────────────────────────────────────────────────────────

def plot_success_rate_per_ndt(df: pd.DataFrame, out_dir: Path,
                              queue: int = 0, show: bool = False) -> None:
    """Success rate per NDT — ack / (ack + discarded), i.e. share of
    RESOLVED outcomes that succeeded. Denominator excludes commands
    still pending/queued when the condition's time window ended
    (censored by the experiment cutoff, not a real outcome), so this
    isolates true admission-control success from the client-side
    'sent' count, which reflects attempt volume rather than success
    and can be misleading on its own (a starved NDT often shows a
    LARGE 'sent' because every quick rejection is immediately
    retried — see per-NDT drop counts in dtm_summary / ops_summary
    for the DTM-side, non-ambiguous cross-check of this same story).
    """
    modes = sorted(df["prio_mode"].unique())
    rates = sorted(df["rate"].unique())

    fig, axes = plt.subplots(1, len(modes), figsize=(6*len(modes), 5),
                             sharey=True)
    if len(modes) == 1:
        axes = [axes]

    for ax, mode in zip(axes, modes):
        sub = df[(df["queue"]==queue) & (df["prio_mode"]==mode)]
        ndts = sorted(sub["ndt_id"].unique())
        width = 0.8 / max(len(ndts), 1)

        for ni, ndt_id in enumerate(ndts):
            prio = NDT_PRIORITIES.get(ndt_id, "?")
            rates_x, rates_y = [], []
            for rate in rates:
                g = sub[(sub["ndt_id"]==ndt_id) & (sub["rate"]==rate)]
                ack  = (g["status"]=="ACK").sum()
                disc = (g["status"]=="DISCARDED").sum()
                resolved = ack + disc
                pct = 100.0 * ack / resolved if resolved else np.nan
                rates_x.append(rate)
                rates_y.append(pct)
            x = np.arange(len(rates)) + ni*width
            bars = ax.bar(x, rates_y, width=width,
                   color=COLORS[ndt_id % len(COLORS)],
                   hatch=HATCHES[ndt_id % len(HATCHES)],
                   edgecolor="black", linewidth=0.8,
                   label=f"ndt-{ndt_id} (p={prio})")
            for xi, y in zip(x, rates_y):
                if np.isnan(y):
                    continue
                ax.annotate(f"{y:.0f}%", xy=(xi, y), xytext=(0, 3),
                            textcoords="offset points",
                            ha="center", fontsize=7.5)

        ax.set_xticks(np.arange(len(rates)) + width*(len(ndts)-1)/2)
        ax.set_xticklabels([str(r) for r in rates])
        ax.set_xlabel("Command rate per NDT (cmd/s)", fontsize=11)
        ax.set_title(f"Mode: {mode}", fontsize=11.5, fontweight="bold")
        ax.set_ylim(0, 112)
        ax.grid(True, alpha=0.25, axis="y")
        ax.legend(fontsize=8, loc="lower left")

    axes[0].set_ylabel("Success rate (%) — ACK / (ACK + DISCARDED)",
                       fontsize=11)
    fig.suptitle(f"Per-NDT Success Rate vs Load — queue={queue}",
                fontsize=12)
    fig.tight_layout()
    _save(fig, out_dir, f"success_rate_per_ndt_queue{queue}")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 5c — Latency percentiles per NDT (P50/P95/P99)
# ─────────────────────────────────────────────────────────────

def plot_latency_percentiles(df: pd.DataFrame, out_dir: Path,
                             rate: float, queue: int,
                             show: bool = False) -> None:
    """P50/P95/P99 e2e latency per NDT — the mean alone hides
    starvation tails (a distribution with a short body and a long
    unbounded tail can have a deceptively low mean). Log y-axis is
    used unconditionally here since percentile spread under
    saturation routinely spans 2-3 orders of magnitude between the
    highest- and lowest-priority NDT.
    """
    modes = sorted(df["prio_mode"].unique())
    fig, axes = plt.subplots(1, len(modes), figsize=(6*len(modes), 5),
                             sharey=True)
    if len(modes) == 1:
        axes = [axes]

    percentiles = [50, 95, 99]
    pctl_colors  = ["#009E73", "#E69F00", "#D55E00"]  # Okabe-Ito subset
    pctl_hatches = ['', '///', 'xxx']

    for ax, mode in zip(axes, modes):
        sub = df[(df["rate"]==rate) & (df["queue"]==queue) &
                 (df["prio_mode"]==mode) & (df["status"]=="ACK")
                 ].dropna(subset=["e2e_ms"])
        ndts = sorted(sub["ndt_id"].unique())
        width = 0.8 / max(len(percentiles), 1)
        x_base = np.arange(len(ndts))

        for pi, p in enumerate(percentiles):
            vals = []
            for ndt_id in ndts:
                v = sub[sub["ndt_id"]==ndt_id]["e2e_ms"]
                vals.append(np.percentile(v, p) if len(v) else np.nan)
            x = x_base + pi*width
            ax.bar(x, vals, width=width, color=pctl_colors[pi],
                   hatch=pctl_hatches[pi], edgecolor="black", linewidth=0.7,
                   label=f"P{p}", alpha=0.95)

        ax.set_yscale("log")
        ax.set_xticks(x_base + width*(len(percentiles)-1)/2)
        ax.set_xticklabels([
            f"ndt-{n}\n(p={NDT_PRIORITIES.get(n,'?')})" for n in ndts
        ], fontsize=9)
        ax.set_title(f"Mode: {mode}", fontsize=11.5, fontweight="bold")
        ax.grid(True, alpha=0.25, axis="y")
        ax.legend(fontsize=9)

    axes[0].set_ylabel("E2E latency (ms, log scale)", fontsize=11)
    fig.suptitle(f"Latency Percentiles per NDT — "
                f"rate={rate} cmd/s, queue={queue}", fontsize=12)
    fig.tight_layout()
    _save(fig, out_dir, f"latency_percentiles_rate{rate}_queue{queue}")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 5d — Starvation regime map: drop rate vs wait time
# ─────────────────────────────────────────────────────────────

def plot_starvation_regime(df: pd.DataFrame, out_dir: Path,
                           show: bool = False) -> None:
    """Scatter of (drop rate %, mean queue wait ms) per (NDT, rate,
    queue) point in priority mode. Point size encodes offered load
    (rate); color encodes the NDT's priority class; each point is
    labeled directly with its rate so the plot is self-explanatory
    without relying on a legend lookup.

    # FIX (2026-07-30): the earlier version shaded fixed background
    # zones (x<15%, x>50%) and floated quoted "fast death"/"slow
    # death" text in the plot corners regardless of where the actual
    # data fell. With few conditions (e.g. a single-rate dataset)
    # this produced 2-4 sparse dots with confusing colored bands and
    # text that didn't clearly relate to any specific point. Replaced
    # with direct per-point rate labels and a single annotation drawn
    # only near the points that actually exhibit each regime (found
    # by ranking observed drop rates), so the labels always describe
    # real data instead of a fixed, potentially-empty region. Needs
    # at least 2 distinct (rate, queue) conditions per NDT to be
    # informative — for a single-condition dataset, combine it with
    # a broader sweep via a comma-separated --logs (see
    # _iter_run_dirs) to get enough points to actually form a "map".
    """
    sub = df[df["prio_mode"]=="priority"]
    if sub.empty:
        print("  Skipping starvation_regime plot — no priority-mode data")
        return

    n_conditions = sub.groupby(["rate","queue"]).ngroups
    if n_conditions < 2:
        print("  NOTE: starvation_regime_map has only 1 (rate,queue) "
              "condition — this figure needs several conditions to form "
              "a meaningful map. Combine datasets via a comma-separated "
              "--logs path (e.g. the full sweep + a long single-condition "
              "run) for a richer version.")

    fig, ax = plt.subplots(figsize=(8,6))

    all_points = []  # (drop_pct, mean_wait, ndt_id, rate) for extremum labeling
    label_candidates = []  # (drop_pct, mean_wait, rate) for collision-avoided labels

    for ndt_id in sorted(sub["ndt_id"].unique()):
        prio = NDT_PRIORITIES.get(ndt_id, "?")
        xs, ys, sizes, rate_labels = [], [], [], []
        for (rate, queue), g in sub[sub["ndt_id"]==ndt_id].groupby(
                ["rate","queue"]):
            ack  = (g["status"]=="ACK").sum()
            disc = (g["status"]=="DISCARDED").sum()
            resolved = ack + disc
            if resolved == 0:
                continue
            drop_pct = 100.0 * disc / resolved
            wait_vals = g[g["status"]=="ACK"]["wait_ms"].dropna()
            mean_wait = wait_vals.mean() if len(wait_vals) else np.nan
            if np.isnan(mean_wait):
                # fall back to e2e_ms if wait_ms wasn't populated
                e2e_vals = g[g["status"]=="ACK"]["e2e_ms"].dropna()
                mean_wait = e2e_vals.mean() if len(e2e_vals) else 0.0
            mean_wait = max(mean_wait, 0.01)  # avoid log(0)
            xs.append(drop_pct)
            ys.append(mean_wait)
            sizes.append(60 + rate*1.8)
            rate_labels.append(rate)
            all_points.append((drop_pct, mean_wait, ndt_id, rate))

        if xs:
            ax.scatter(xs, ys, s=sizes,
                       color=COLORS[ndt_id % len(COLORS)],
                       alpha=0.8, edgecolors="black", linewidths=0.7,
                       zorder=3,
                       label=f"ndt-{ndt_id} (p={prio})")
            # rate labels are placed after ALL points are scattered
            # (see collision-avoidance pass below) rather than here,
            # so nearby points from different NDTs don't stack labels
            # on top of each other.
            label_candidates.extend(zip(xs, ys, rate_labels))

    # ── FIX (2026-08-15) ────────────────────────────────────────
    # Placing a rate label at a fixed offset next to every point
    # (as before) caused labels to overlap into unreadable stacks
    # whenever several (ndt, rate) points landed close together in
    # (drop%, wait) space — common at low load, where most NDTs
    # cluster near (0%, low ms). Fix: sort candidates by y (so the
    # most visually prominent/high points get first claim on label
    # space) and skip a label if it would land within a small
    # neighborhood (in log-wait / drop-pct space) of an already
    # placed label — keeps the point itself visible via the marker
    # even when its numeric label is skipped for space.
    # ─────────────────────────────────────────────────────────────
    placed: list = []  # (drop_pct, log10(wait)) of labels already drawn
    label_candidates.sort(key=lambda p: -p[1])
    x_thresh, y_thresh = 4.0, 0.12  # drop-% units, log10(ms) units
    for xi, yi, r in label_candidates:
        log_yi = np.log10(yi)
        too_close = any(
            abs(xi - px) < x_thresh and abs(log_yi - py) < y_thresh
            for px, py in placed
        )
        if too_close:
            continue
        ax.annotate(f"{r:g}", xy=(xi, yi), xytext=(5, 4),
                    textcoords="offset points", fontsize=7.5,
                    color="#333333")
        placed.append((xi, log_yi))

    ax.set_yscale("log")
    ax.set_xlim(-5, 105)

    # Annotate the two extreme points actually present in the data
    # (highest drop-rate point and highest-wait/lowest-drop point)
    # instead of shading fixed regions — always describes real data.
    if all_points:
        by_drop = sorted(all_points, key=lambda p: -p[0])
        by_wait = sorted(all_points, key=lambda p: (p[0], -p[1]))
        fast_pt = by_drop[0]
        slow_pt = by_wait[0]
        if fast_pt[0] > 30:  # only label if genuinely drop-dominated
            ax.annotate("rejected at admission\n(\"fast\" exclusion)",
                        xy=(fast_pt[0], fast_pt[1]),
                        xytext=(-90, 18), textcoords="offset points",
                        fontsize=8.5, color="#D55E00",
                        arrowprops=dict(arrowstyle="->", color="#D55E00",
                                        lw=0.9))
        if slow_pt[0] < 10 and slow_pt[1] > 50:  # genuinely wait-dominated
            ax.annotate("admitted, never served\n(\"slow\" exclusion)",
                        xy=(slow_pt[0], slow_pt[1]),
                        xytext=(15, -12), textcoords="offset points",
                        fontsize=8.5, color="#0072B2",
                        arrowprops=dict(arrowstyle="->", color="#0072B2",
                                        lw=0.9))

    ax.set_xlabel("Drop rate (%) — share of resolved commands discarded",
                  fontsize=12)
    ax.set_ylabel("Mean wait time (ms, log scale)", fontsize=12)
    ax.set_title("Starvation Regime Map (priority mode)\n"
                 "marker size scales with offered rate; labels show rate (cmd/s)",
                 fontsize=12, fontweight="bold")
    ax.legend(fontsize=9, loc="center right")
    ax.grid(True, alpha=0.25)
    fig.tight_layout()
    _save(fig, out_dir, "starvation_regime_map")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 5e — Headline figure: 3-panel composite for the main text
# ─────────────────────────────────────────────────────────────

def plot_headline_summary(df: pd.DataFrame, out_dir: Path,
                          rate: float, queue: int,
                          show: bool = False) -> None:
    """A single wide 3-panel figure meant to anchor the results
    section: (a) per-NDT success rate at this rate/queue in priority
    mode, (b) P50/P95/P99 latency per NDT (log scale), (c) the
    starvation regime map. Individual full-detail versions of each
    panel (all rates/queues, both modes) stay available as
    supplementary figures — this is the one figure a reader should
    remember without reading the caption twice.
    """
    sub = df[(df["rate"]==rate) & (df["queue"]==queue) &
             (df["prio_mode"]=="priority")]
    if sub.empty:
        print("  Skipping headline_summary — no data for that rate/queue")
        return
    ndts = sorted(sub["ndt_id"].unique())

    fig = plt.figure(figsize=(15, 4.6))
    gs  = fig.add_gridspec(1, 3, wspace=0.32)
    ax1, ax2, ax3 = (fig.add_subplot(gs[0, i]) for i in range(3))

    # (a) success rate
    for ndt_id in ndts:
        prio = NDT_PRIORITIES.get(ndt_id, "?")
        g = sub[sub["ndt_id"]==ndt_id]
        ack, disc = (g["status"]=="ACK").sum(), (g["status"]=="DISCARDED").sum()
        resolved = ack + disc
        pct = 100.0*ack/resolved if resolved else 0
        bar = ax1.bar(ndt_id, pct, color=COLORS[ndt_id % len(COLORS)],
                      hatch=HATCHES[ndt_id % len(HATCHES)],
                      edgecolor="black", linewidth=0.8)
        ax1.annotate(f"{pct:.0f}%", xy=(ndt_id, pct), xytext=(0, 3),
                     textcoords="offset points", ha="center", fontsize=9)
    ax1.set_xticks(ndts)
    ax1.set_xticklabels([f"ndt-{n}\n(p={NDT_PRIORITIES.get(n,'?')})"
                         for n in ndts], fontsize=9)
    ax1.set_ylim(0, 112)
    ax1.set_ylabel("Success rate (%)", fontsize=11)
    ax1.set_title("(a) Command success rate", fontsize=11, fontweight="bold")
    ax1.grid(True, alpha=0.25, axis="y")

    # (b) latency percentiles
    percentiles  = [50, 95, 99]
    pctl_colors  = ["#009E73", "#E69F00", "#D55E00"]
    pctl_hatches = ['', '///', 'xxx']
    width = 0.8 / len(percentiles)
    x_base = np.arange(len(ndts))
    acked = sub[sub["status"]=="ACK"].dropna(subset=["e2e_ms"])
    for pi, p in enumerate(percentiles):
        vals = [np.percentile(acked[acked["ndt_id"]==n]["e2e_ms"], p)
                if len(acked[acked["ndt_id"]==n]) else np.nan for n in ndts]
        ax2.bar(x_base + pi*width, vals, width=width, color=pctl_colors[pi],
               hatch=pctl_hatches[pi], edgecolor="black", linewidth=0.7,
               label=f"P{p}")
    ax2.set_yscale("log")
    ax2.set_xticks(x_base + width)
    ax2.set_xticklabels([f"ndt-{n}\n(p={NDT_PRIORITIES.get(n,'?')})"
                         for n in ndts], fontsize=9)
    ax2.set_ylabel("E2E latency (ms, log)", fontsize=11)
    ax2.set_title("(b) Latency tail per NDT", fontsize=11, fontweight="bold")
    ax2.legend(fontsize=8.5)
    ax2.grid(True, alpha=0.25, axis="y")

    # (c) Paired bars: drop rate (%) and mean wait (ms) per NDT.
    # A scatter of (drop rate, wait) makes sense once many conditions
    # are combined into a real "map" (see plot_starvation_regime for
    # that multi-condition version) — but for a single rate/queue
    # condition it's only 4 isolated points, which reads as unclear
    # dots rather than a pattern. Paired bars read directly: for each
    # NDT, how much got rejected (left axis) vs how long the rest
    # waited (right axis, log) — no legend lookup required.
    drop_pcts, mean_waits = [], []
    for ndt_id in ndts:
        g = sub[sub["ndt_id"]==ndt_id]
        ack, disc = (g["status"]=="ACK").sum(), (g["status"]=="DISCARDED").sum()
        resolved = ack + disc
        drop_pct = 100.0*disc/resolved if resolved else 0.0
        wait_vals = g[g["status"]=="ACK"]["wait_ms"].dropna()
        mean_wait = wait_vals.mean() if len(wait_vals) else \
                    g[g["status"]=="ACK"]["e2e_ms"].dropna().mean()
        mean_wait = max(mean_wait if pd.notna(mean_wait) else 0.01, 0.01)
        drop_pcts.append(drop_pct)
        mean_waits.append(mean_wait)

    x3 = np.arange(len(ndts))
    bw = 0.36
    bars_drop = ax3.bar(x3 - bw/2, drop_pcts, width=bw,
                        color="#D55E00", hatch="xxx",
                        edgecolor="black", linewidth=0.7,
                        label="Drop rate (%)")
    for xi, v in zip(x3 - bw/2, drop_pcts):
        ax3.annotate(f"{v:.0f}%", xy=(xi, v), xytext=(0, 3),
                     textcoords="offset points", ha="center", fontsize=7.5)
    ax3.set_ylim(0, 112)
    ax3.set_ylabel("Drop rate (%)", fontsize=10.5, color="#D55E00")
    ax3.tick_params(axis="y", labelcolor="#D55E00")

    ax3b = ax3.twinx()
    bars_wait = ax3b.bar(x3 + bw/2, mean_waits, width=bw,
                         color="#0072B2", hatch="...",
                         edgecolor="black", linewidth=0.7,
                         label="Mean wait (ms)")
    for xi, v in zip(x3 + bw/2, mean_waits):
        ax3b.annotate(f"{v:.0f}", xy=(xi, v), xytext=(0, 3),
                      textcoords="offset points", ha="center", fontsize=7.5)
    ax3b.set_yscale("log")
    # headroom so the annotation on the tallest bar never collides
    # with the legend above it, regardless of how extreme the max
    # wait time is (e.g. starvation cases reaching 10^4+ ms)
    wait_max = max(mean_waits) if mean_waits else 1.0
    wait_min = min(mean_waits) if mean_waits else 0.1
    ax3b.set_ylim(max(wait_min / 3, 0.1), wait_max * 6)
    ax3b.set_ylabel("Mean wait (ms, log)", fontsize=10.5, color="#0072B2")
    ax3b.tick_params(axis="y", labelcolor="#0072B2")
    ax3b.grid(False)

    ax3.set_xticks(x3)
    ax3.set_xticklabels([f"ndt-{n}\n(p={NDT_PRIORITIES.get(n,'?')})"
                         for n in ndts], fontsize=9)
    ax3.set_title("(c) Rejection vs. wait, per NDT", fontsize=11, fontweight="bold")
    h1, l1 = ax3.get_legend_handles_labels()
    h2, l2 = ax3b.get_legend_handles_labels()
    ax3.legend(h1+h2, l1+l2, fontsize=8, loc="upper center")
    ax3.grid(True, alpha=0.2, axis="y")

    fig.suptitle(f"Priority-Aware XR Coordination Under Sustained Load "
                f"— rate={rate} cmd/s, queue={queue}",
                fontsize=13, fontweight="bold", y=1.03)
    _save(fig, out_dir, f"headline_summary_rate{rate}_queue{queue}")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 6 — CDF fairness
# ─────────────────────────────────────────────────────────────

def plot_cdf(df: pd.DataFrame, out_dir: Path,
             rate: float, queue: int, show: bool = False) -> None:
    """CDF of e2e_ms per NDT — shows fairness between priorities.

    # FIX (2026-07-22): under strict priority + saturation (e.g.
    # rate=150), the lowest-priority NDT's tail can stretch out to
    # multiple seconds while the rest sit in single-digit
    # milliseconds. On a linear x-axis that renders as a vertical
    # line pinned at 0 with an invisible point far to the right —
    # unreadable. Each panel now switches to a log x-axis when the
    # ratio between the largest and smallest observed e2e_ms in that
    # panel exceeds 50x; low-load panels (all values within the same
    # order of magnitude) stay linear, which is more readable there.
    """
    modes = sorted(df["prio_mode"].unique())

    fig, axes = plt.subplots(1, len(modes),
                              figsize=(6*len(modes), 5), sharey=True)
    if len(modes) == 1:
        axes = [axes]

    for ax, mode in zip(axes, modes):
        sub = df[(df["rate"]==rate) & (df["queue"]==queue) &
                 (df["prio_mode"]==mode) & (df["status"]=="ACK")
                 ].dropna(subset=["e2e_ms"])

        vals_all = sub["e2e_ms"]
        if len(vals_all) and vals_all.min() > 0 and \
           (vals_all.max() / vals_all.min()) > 50:
            ax.set_xscale('log')

        for ndt_id in sorted(sub["ndt_id"].unique()):
            vals = sorted(sub[sub["ndt_id"]==ndt_id]["e2e_ms"].tolist())
            if not vals:
                continue
            cdf  = np.arange(1, len(vals)+1) / len(vals)
            prio = NDT_PRIORITIES.get(ndt_id, "?")
            ax.plot(vals, cdf,
                    color=COLORS[ndt_id % len(COLORS)],
                    label=f"ndt-{ndt_id} (p={prio})",
                    linewidth=1.5)

        ax.set_xlabel("E2E latency (ms)", fontsize=11)
        ax.set_title(f"Mode: {mode}", fontsize=11)
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)
        ax.set_ylim(0, 1.05)

    axes[0].set_ylabel("CDF", fontsize=11)
    fig.suptitle(f"Fairness — CDF of E2E Latency per NDT\n"
                 f"rate={rate} cmd/s  queue={queue}", fontsize=12)
    fig.tight_layout()
    _save(fig, out_dir, f"cdf_rate{rate}_queue{queue}")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 7 — Heatmap e2e vs rate × mode
# ─────────────────────────────────────────────────────────────

def plot_heatmap(df: pd.DataFrame, out_dir: Path,
                 queue: int = 0, show: bool = False) -> None:
    """Heatmap of mean e2e_ms vs rate × prio_mode.

    Note: mode is already on its own axis here (y-axis rows), so
    fifo and priority are visually separated within a single
    figure per queue size — no split needed, left as-is.
    """
    sub   = df[(df["queue"]==queue) & (df["status"]=="ACK")].dropna(subset=["e2e_ms"])
    rates = sorted(sub["rate"].unique())
    modes = sorted(sub["prio_mode"].unique())

    matrix = np.full((len(modes), len(rates)), np.nan)
    for _, row in sub.groupby(["rate","prio_mode"])["e2e_ms"].mean().reset_index().iterrows():
        i = modes.index(row["prio_mode"])
        j = rates.index(row["rate"])
        matrix[i, j] = row["e2e_ms"]

    fig, ax = plt.subplots(figsize=(8,4))
    im = ax.imshow(matrix, aspect="auto", cmap="YlOrRd")
    plt.colorbar(im, ax=ax, label="Mean e2e_ms")
    ax.set_xticks(range(len(rates)))
    ax.set_xticklabels([str(r) for r in rates])
    ax.set_yticks(range(len(modes)))
    ax.set_yticklabels(modes)
    ax.set_xlabel("Rate (cmd/s per NDT)", fontsize=12)
    ax.set_ylabel("Priority Mode", fontsize=12)
    ax.set_title(f"Heatmap — Mean E2E Latency (ms)\nqueue={queue}", fontsize=12)
    for i in range(len(modes)):
        for j in range(len(rates)):
            if not np.isnan(matrix[i,j]):
                ax.text(j, i, f"{matrix[i,j]:.1f}",
                        ha="center", va="center", fontsize=10)
    fig.tight_layout()
    _save(fig, out_dir, f"heatmap_queue{queue}")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# CSV export
# ─────────────────────────────────────────────────────────────

def export_csv(df: pd.DataFrame, dtm: pd.DataFrame,
               out_dir: Path) -> None:
    # Full client log
    df.to_csv(out_dir / "all_records.csv", index=False)
    print(f"  Saved: {out_dir / 'all_records.csv'}")

    # Per-condition summary
    rows = []
    for (rate,queue,mode,ndt_id), grp in df.groupby(
            ["rate","queue","prio_mode","ndt_id"]):
        acked = grp[grp["status"]=="ACK"]
        disc  = (grp["status"]=="DISCARDED").sum()
        resolved = len(acked) + disc
        pending  = len(grp) - resolved  # censored by condition cutoff
        e2e   = acked["e2e_ms"].dropna().tolist()
        wait  = acked["wait_ms"].dropna().tolist()
        apply = acked["apply_ms"].dropna().tolist()
        m_e2e, ci_e2e   = mean_ci95(e2e)   if e2e   else (None, None)
        m_wait, ci_wait = mean_ci95(wait)  if wait  else (None, None)
        m_app, ci_app   = mean_ci95(apply) if apply else (None, None)
        rows.append({
            "rate": rate, "queue": queue, "prio_mode": mode,
            "ndt_id": ndt_id, "priority": NDT_PRIORITIES.get(ndt_id,"?"),
            "sent": len(grp), "acked": len(acked), "discarded": disc,
            "pending_at_cutoff": pending,
            # ack_rate: legacy denominator (ack/sent) — kept for
            # backward compatibility with earlier CSVs, but 'sent'
            # includes commands still pending when the condition's
            # time window ended, which understates true success.
            "ack_rate": len(acked)/len(grp) if len(grp) else 0,
            # success_rate: the metric to report in the paper —
            # ack / (ack + discarded), i.e. share of RESOLVED
            # outcomes that succeeded, excluding censored/pending.
            "success_rate": len(acked)/resolved if resolved else None,
            "e2e_mean": m_e2e, "e2e_ci95": ci_e2e,
            "e2e_p50": np.percentile(e2e, 50) if e2e else None,
            "e2e_p95": np.percentile(e2e, 95) if e2e else None,
            "e2e_p99": np.percentile(e2e, 99) if e2e else None,
            "wait_mean": m_wait, "wait_ci95": ci_wait,
            "apply_mean": m_app, "apply_ci95": ci_app,
        })
    pd.DataFrame(rows).to_csv(out_dir / "summary.csv", index=False)
    print(f"  Saved: {out_dir / 'summary.csv'}")

    # DTM summary
    if not dtm.empty:
        done = dtm[dtm["tag"]=="DTM_UPSTREAM_DONE"]
        drops = dtm[dtm["tag"]=="DTM_DROP"]
        dtm_rows = []
        for (rate,queue,mode), grp in done.groupby(["rate","queue","prio_mode"]):
            d = drops[(drops["rate"]==rate)&(drops["queue"]==queue)&
                      (drops["prio_mode"]==mode)]
            qms = grp["queue_ms"].dropna().tolist()
            ams = grp["apply_ms"].dropna().tolist()
            dtm_rows.append({
                "rate": rate, "queue": queue, "prio_mode": mode,
                "processed": len(grp), "drops": len(d),
                "queue_ms_mean": round(np.mean(qms),2) if qms else None,
                "queue_ms_p99":  round(np.percentile(qms,99),2) if qms else None,
                "queue_ms_max":  round(max(qms),2) if qms else None,
                "apply_ms_mean": round(np.mean(ams),2) if ams else None,
            })
        pd.DataFrame(dtm_rows).to_csv(out_dir / "dtm_summary.csv", index=False)
        print(f"  Saved: {out_dir / 'dtm_summary.csv'}")


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="experiment_2_results — Exp.2 analysis and plots"
    )
    ap.add_argument("--logs",          required=True)
    ap.add_argument("--out",           required=True)
    ap.add_argument("--show",          action="store_true")
    ap.add_argument("--queue",         type=int,   default=0,
                    help="Queue size for per-plot filtering (default: 0)")
    ap.add_argument("--rate-for-cdf",  type=float, default=None,
                    help="Rate for CDF plot (default: highest rate)")
    args = ap.parse_args()

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load
    df  = load_client_logs(args.logs)
    dtm = load_dtm_logs(args.logs)

    # Summary
    print_summary(df, dtm)

    # Rate for CDF
    cdf_rate = args.rate_for_cdf or float(sorted(df["rate"].unique())[-1])

    print(f"\nGenerating plots → {out_dir}")

    # All queue sizes found
    queues = sorted(df["queue"].unique())
    rates  = sorted(df["rate"].unique())

    # ── FIX (2026-07-30) ────────────────────────────────────────
    # e2e_vs_rate / queue_ms_vs_rate / throughput_vs_rate / the
    # aggregate ack_rate plot, and the heatmap, are trend-over-rate
    # figures — with a single-condition dataset (e.g. a long
    # starvation-characterization run at one fixed rate/queue) they
    # degenerate to 1-2 isolated points per line, which reads as a
    # broken chart rather than "not applicable to this dataset".
    # Skip them here with an explicit message instead of emitting a
    # near-empty figure; run this same script against a multi-rate
    # sweep (e.g. the full rate x queue matrix) to get these.
    # ─────────────────────────────────────────────────────────────
    multi_rate = len(rates) >= 2
    if not multi_rate:
        print(f"  NOTE: only one rate value ({rates[0]}) in this dataset — "
              "skipping e2e_vs_rate, queue_ms_vs_rate, throughput_vs_rate, "
              "aggregate ack_rate_vs_rate, and heatmap (all degenerate to "
              "1-2 points with a single rate). Run against a multi-rate "
              "sweep for those.")

    for q in queues:
        if multi_rate:
            plot_e2e_vs_rate(df, out_dir, queue=int(q), show=args.show)
            plot_heatmap(df, out_dir, queue=int(q), show=args.show)
        plot_priority_effect(df, out_dir, queue=int(q), show=args.show)
        plot_cdf(df, out_dir, rate=cdf_rate, queue=int(q), show=args.show)
        plot_success_rate_per_ndt(df, out_dir, queue=int(q), show=args.show)
        for r in rates:
            plot_latency_percentiles(df, out_dir, rate=r, queue=int(q),
                                     show=args.show)

    if multi_rate:
        plot_ack_rate(df, out_dir, show=args.show)
        plot_queue_ms(dtm, out_dir, show=args.show)
        plot_throughput(dtm, out_dir, show=args.show)
    plot_starvation_regime(df, out_dir, show=args.show)
    for q in queues:
        plot_headline_summary(df, out_dir, rate=float(sorted(df["rate"].unique())[-1]),
                              queue=int(q), show=args.show)

    print("\nExporting CSVs...")
    export_csv(df, dtm, out_dir)

    print(f"\nDone. Results in: {out_dir}")
    print("Files:")
    for f in sorted(out_dir.iterdir()):
        print(f"  {f.name}")


if __name__ == "__main__":
    main()