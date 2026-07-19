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
plt.rcParams.update({
    'font.family':     'DejaVu Sans',
    'font.size':       11,
    'axes.titlesize':  12,
    'axes.labelsize':  11,
    'legend.fontsize': 10,
    'figure.dpi':      100,
})

NDT_PRIORITIES = {0: 10, 1: 30, 2: 50, 3: 70}
COLORS = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
MARKERS = ['o', 's', '^', 'D', 'v']


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


def load_client_logs(logs_dir: str) -> pd.DataFrame:
    """Load ndt*.log files — one row per command."""
    rows = []
    for run_dir in sorted(Path(logs_dir).glob("run*")):
        run_num = int(re.search(r"run(\d+)", run_dir.name).group(1))
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
                        "run":       run_num,
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
    """Load dtm_upstream.log files — one row per processed command."""
    rows = []
    for run_dir in sorted(Path(logs_dir).glob("run*")):
        run_num = int(re.search(r"run(\d+)", run_dir.name).group(1))
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
                    "run":       run_num,
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
    path = out_dir / f"e2e_vs_rate_queue{queue}.png"
    fig.savefig(path, dpi=150)
    print(f"  Saved: {path}")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 2 — Priority effect: e2e per NDT per rate
# ─────────────────────────────────────────────────────────────

def plot_priority_effect(df: pd.DataFrame, out_dir: Path,
                         queue: int = 0, show: bool = False) -> None:
    """Shows priority effect on e2e per NDT at each rate."""
    modes = sorted(df["prio_mode"].unique())
    rates = sorted(df["rate"].unique())

    for mode in modes:
        sub = df[(df["queue"]==queue) &
                 (df["prio_mode"]==mode) &
                 (df["status"]=="ACK")]

        fig, axes = plt.subplots(1, len(rates),
                                 figsize=(4*len(rates), 5), sharey=True)
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
            ax.bar(x, means, yerr=cis,
                   color=[COLORS[n % len(COLORS)] for n in ndts],
                   capsize=5, alpha=0.85)
            ax.set_xticks(x)
            ax.set_xticklabels([f"ndt-{n}\n(p={p})"
                                 for n,p in zip(ndts,prios)], fontsize=9)
            ax.set_title(f"rate={rate} cmd/s", fontsize=10)
            ax.grid(True, alpha=0.3, axis="y")

        axes[0].set_ylabel("E2E latency (ms)", fontsize=11)
        fig.suptitle(f"Priority Effect on E2E Latency — mode={mode} queue={queue}",
                     fontsize=12)
        fig.tight_layout()
        path = out_dir / f"priority_effect_{mode}_queue{queue}.png"
        fig.savefig(path, dpi=150)
        print(f"  Saved: {path}")
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
    path = out_dir / "queue_ms_vs_rate.png"
    fig.savefig(path, dpi=150)
    print(f"  Saved: {path}")
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
    path = out_dir / "throughput_vs_rate.png"
    fig.savefig(path, dpi=150)
    print(f"  Saved: {path}")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 5 — ACK rate (stability)
# ─────────────────────────────────────────────────────────────

def plot_ack_rate(df: pd.DataFrame, out_dir: Path,
                  show: bool = False) -> None:
    """ACK rate vs rate — system stability under load."""
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
    ax.set_title("System Stability — ACK Rate vs Load", fontsize=12)
    ax.set_ylim(0, 105)
    ax.axhline(100, color='gray', linestyle=':', alpha=0.5)
    ax.legend()
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    path = out_dir / "ack_rate_vs_rate.png"
    fig.savefig(path, dpi=150)
    print(f"  Saved: {path}")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 6 — CDF fairness
# ─────────────────────────────────────────────────────────────

def plot_cdf(df: pd.DataFrame, out_dir: Path,
             rate: float, queue: int, show: bool = False) -> None:
    """CDF of e2e_ms per NDT — shows fairness between priorities."""
    modes = sorted(df["prio_mode"].unique())

    fig, axes = plt.subplots(1, len(modes),
                              figsize=(6*len(modes), 5), sharey=True)
    if len(modes) == 1:
        axes = [axes]

    for ax, mode in zip(axes, modes):
        sub = df[(df["rate"]==rate) & (df["queue"]==queue) &
                 (df["prio_mode"]==mode) & (df["status"]=="ACK")
                 ].dropna(subset=["e2e_ms"])

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
    path = out_dir / f"cdf_rate{rate}_queue{queue}.png"
    fig.savefig(path, dpi=150)
    print(f"  Saved: {path}")
    if show: plt.show()
    plt.close(fig)


# ─────────────────────────────────────────────────────────────
# Plot 7 — Heatmap e2e vs rate × mode
# ─────────────────────────────────────────────────────────────

def plot_heatmap(df: pd.DataFrame, out_dir: Path,
                 queue: int = 0, show: bool = False) -> None:
    """Heatmap of mean e2e_ms vs rate × prio_mode."""
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
    path = out_dir / f"heatmap_queue{queue}.png"
    fig.savefig(path, dpi=150)
    print(f"  Saved: {path}")
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
            "ack_rate": len(acked)/len(grp) if len(grp) else 0,
            "e2e_mean": m_e2e, "e2e_ci95": ci_e2e,
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
    for q in queues:
        plot_e2e_vs_rate(df, out_dir, queue=int(q), show=args.show)
        plot_priority_effect(df, out_dir, queue=int(q), show=args.show)
        plot_heatmap(df, out_dir, queue=int(q), show=args.show)
        plot_cdf(df, out_dir, rate=cdf_rate, queue=int(q), show=args.show)

    plot_ack_rate(df, out_dir, show=args.show)
    plot_queue_ms(dtm, out_dir, show=args.show)
    plot_throughput(dtm, out_dir, show=args.show)

    print("\nExporting CSVs...")
    export_csv(df, dtm, out_dir)

    print(f"\nDone. Results in: {out_dir}")
    print("Files:")
    for f in sorted(out_dir.iterdir()):
        print(f"  {f.name}")


if __name__ == "__main__":
    main()