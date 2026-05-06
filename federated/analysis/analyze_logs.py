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
Log analyser — parses JSONL event logs and produces:
  Experiment 1 (downstream)  : orchestration latency vs federation size
  Experiment 2 (upstream)    : throughput / drop / latency vs offered load

Usage
-----
python analyse.py --logs /path/to/logs --out /path/to/output

Log folder layout expected
--------------------------
logs/
  1toN/          ← downstream experiments (N = fanout)
    ndtm_events.log
  rateR/         ← upstream experiments  (R = offered load)
    ndtm_events.log

Output
------
  metrics_exp1_downstream.csv
  metrics_exp2_upstream.csv
  fig1_downstream_latency.pdf
  fig2_upstream_throughput.pdf
  fig3_upstream_latency.pdf
  fig4_upstream_drops.pdf
"""

import re
import argparse
from pathlib import Path
from typing import Optional

try:
    import orjson

    def _parse_line(line) -> dict:
        return orjson.loads(line)

    _OPEN_MODE = "rb"
except ImportError:
    import json as _json  # type: ignore

    def _parse_line(line) -> dict:
        return _json.loads(line)

    _OPEN_MODE = "r"

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate


# ---------------------------------------------------------------------------
# Plot style (IEEE Communications Magazine)
# ---------------------------------------------------------------------------

plt.rcParams.update({
    "font.family": "serif",
    "font.size": 10,
    "axes.titlesize": 10,
    "axes.labelsize": 10,
    "legend.fontsize": 9,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "figure.figsize": (5.5, 3.5),
    "axes.grid": True,
    "grid.alpha": 0.4,
    "grid.linestyle": "--",
    "savefig.dpi": 300,
    "savefig.bbox": "tight",
})


# ---------------------------------------------------------------------------
# IO
# ---------------------------------------------------------------------------

def load_jsonl(path: Path) -> pd.DataFrame:
    print(f"⏳ Reading {path.name} … ", end="", flush=True)
    rows = []
    with open(path, _OPEN_MODE) as f:
        for raw in f:
            try:
                rows.append(_parse_line(raw))
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


# ---------------------------------------------------------------------------
# Experiment 1 — Downstream (ONM → DTM → NDTs)
# ---------------------------------------------------------------------------

def compute_downstream(df: pd.DataFrame, exp_name: str) -> Optional[dict]:
    """
    Latency = t_dispatch_end_ns (from NDT_EVENT_DISPATCHED)
              − t_rx_ns         (from NDT_EVENT_RX)
    Both columns exist in NDT_EVENT_DISPATCHED via the log_event call in dtm.py.
    We merge on event_id, using explicit column names to avoid suffix ambiguity.
    """
    if "tag" not in df.columns:
        return None

    rx = df[df["tag"] == "NDT_EVENT_RX"][["event_id", "t_rx_ns"]].copy()
    disp = df[df["tag"] == "NDT_EVENT_DISPATCHED"][
        ["event_id", "t_dispatch_end_ns", "sent_ok", "fanout"]
    ].copy()

    if rx.empty or disp.empty:
        return None

    # Coerce numeric
    rx["t_rx_ns"] = pd.to_numeric(rx["t_rx_ns"], errors="coerce")
    disp["t_dispatch_end_ns"] = pd.to_numeric(disp["t_dispatch_end_ns"], errors="coerce")
    disp["sent_ok"] = pd.to_numeric(disp["sent_ok"], errors="coerce")
    disp["fanout"] = pd.to_numeric(disp["fanout"], errors="coerce")

    merged = rx.merge(disp, on="event_id", how="inner")
    if merged.empty:
        return None

    latencies_ms = (merged["t_dispatch_end_ns"] - merged["t_rx_ns"]) / 1e6

    sent_total = disp["sent_ok"].sum()
    fanout_total = disp["fanout"].sum()
    eff = float(sent_total / fanout_total * 100.0) if fanout_total > 0 else 0.0

    # Extract fanout value from directory name (e.g. "1to32" → 32)
    match = re.search(r"(\d+)$", exp_name.replace("1to", ""))
    fanout_val = int(match.group(1)) if match else 1

    return {
        "Experiment": exp_name,
        "Fanout": fanout_val,
        "Dispatch Eff (%)": round(eff, 2),
        "Orchestration Lat Mean (ms)": round(float(latencies_ms.mean()), 3),
        "Orchestration Lat P95 (ms)": round(float(latencies_ms.quantile(0.95)), 3),
        "N events": len(merged),
    }


# ---------------------------------------------------------------------------
# Experiment 2 — Upstream (NDTs → DTM feedback)
# ---------------------------------------------------------------------------

def compute_upstream(df: pd.DataFrame, exp_name: str) -> Optional[dict]:
    """
    Uses the single DTM_EVENT_DONE rows (the 'mochila') for per-event latency.
    No merge needed — all timing fields are embedded in the DONE record.
    """
    if "tag" not in df.columns:
        return None

    done = df[
        (df["tag"] == "DTM_EVENT_DONE") & (df.get("src", pd.Series("")) == "feedback")
    ].copy()
    drops = df[df["tag"] == "DTM_DROP"].copy()

    total_ingested = len(done) + len(drops)
    if total_ingested == 0:
        return None

    # ---- Offered load (based on arrival timestamps of all events) ----------
    t_rx_done = pd.to_numeric(done["t_rx_ns"], errors="coerce")
    t_rx_drops = pd.to_numeric(drops["ts_ns"], errors="coerce")
    all_rx = pd.concat([t_rx_done, t_rx_drops]).dropna()

    if all_rx.empty:
        return None

    window_s = (all_rx.max() - all_rx.min()) / 1e9
    offered_load = total_ingested / window_s if window_s > 0 else 0.0

    # ---- Processing capacity (completed events only) -----------------------
    if not done.empty and "ts_ns" in done.columns:
        t_proc = pd.to_numeric(done["ts_ns"], errors="coerce").dropna()
        proc_window_s = (t_proc.max() - t_proc.min()) / 1e9 if len(t_proc) > 1 else window_s
        capacity = len(done) / proc_window_s if proc_window_s > 0 else 0.0
    else:
        capacity = 0.0

    proc_eff = len(done) / total_ingested * 100.0
    drop_pct = len(drops) / total_ingested * 100.0

    # ---- Queue delay (from mochila) ----------------------------------------
    q_ms = pd.to_numeric(done.get("queue_ms", pd.Series(dtype=float)), errors="coerce").dropna()
    q_mean = float(q_ms.mean()) if not q_ms.empty else float("nan")
    q_p95 = float(q_ms.quantile(0.95)) if not q_ms.empty else float("nan")

    # ---- End-to-end processing latency -------------------------------------
    if not done.empty and {"t_rx_ns", "t_done_ns"}.issubset(done.columns):
        t_rx = pd.to_numeric(done["t_rx_ns"], errors="coerce")
        t_dn = pd.to_numeric(done["t_done_ns"], errors="coerce")
        proc_lats = ((t_dn - t_rx) / 1e6).dropna()
        proc_lat_mean = float(proc_lats.mean()) if not proc_lats.empty else float("nan")
        proc_lat_p95 = float(proc_lats.quantile(0.95)) if not proc_lats.empty else float("nan")
    else:
        proc_lat_mean = proc_lat_p95 = float("nan")

    # ---- Drop analysis by priority -----------------------------------------
    drops_critical = drops_background = 0
    if not drops.empty and "priority" in drops.columns:
        prio = pd.to_numeric(drops["priority"], errors="coerce").fillna(100)
        drops_critical = int((prio <= 20).sum())
        drops_background = int((prio > 20).sum())

    return {
        "Experiment": exp_name,
        "Offered Load (ev/s)": round(offered_load, 2),
        "Capacity (ev/s)": round(capacity, 2),
        "Processing Eff (%)": round(proc_eff, 2),
        "Drop (%)": round(drop_pct, 2),
        "Queue Mean (ms)": round(q_mean, 3),
        "Queue P95 (ms)": round(q_p95, 3),
        "Proc Lat Mean (ms)": round(proc_lat_mean, 3),
        "Proc Lat P95 (ms)": round(proc_lat_p95, 3),
        "Drops Critical (<=20)": drops_critical,
        "Drops Background (>20)": drops_background,
    }


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------

def plot_downstream(df: pd.DataFrame, outdir: Path) -> None:
    if df.empty:
        return
    df = df.sort_values("Fanout").reset_index(drop=True)

    plt.figure()
    plt.plot(df["Fanout"], df["Orchestration Lat Mean (ms)"],
             marker="o", label="Mean latency")
    plt.plot(df["Fanout"], df["Orchestration Lat P95 (ms)"],
             marker="s", linestyle="--", label="P95 latency")
    plt.xscale("log", base=2)
    plt.xticks(df["Fanout"], df["Fanout"].astype(str))
    plt.xlabel("Federation size (number of NDTs)")
    plt.ylabel("Orchestration latency (ms)")
    plt.title("Exp 1: DTM state synchronisation delay")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outdir / "fig1_downstream_latency.pdf")
    plt.close()
    print("  → fig1_downstream_latency.pdf")


def plot_upstream(df: pd.DataFrame, outdir: Path) -> None:
    if df.empty:
        return
    df = df.sort_values("Offered Load (ev/s)").reset_index(drop=True)
    x = df["Offered Load (ev/s)"]

    # 1. Throughput
    plt.figure()
    plt.plot(x, df["Capacity (ev/s)"], marker="o", label="Processed")
    top = max(x.max(), df["Capacity (ev/s)"].max())
    plt.plot([0, top], [0, top], "k--", alpha=0.5, label="Ideal 1:1")
    plt.xlabel("Offered telemetry load (events/s)")
    plt.ylabel("DTM processing capacity (events/s)")
    plt.title("Exp 2: Upstream throughput")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outdir / "fig2_upstream_throughput.pdf")
    plt.close()
    print("  → fig2_upstream_throughput.pdf")

    # 2. Latency
    plt.figure()
    plt.plot(x, df["Proc Lat Mean (ms)"], marker="o", label="Mean")
    plt.plot(x, df["Proc Lat P95 (ms)"], marker="s", linestyle="--", label="P95")
    plt.xlabel("Offered telemetry load (events/s)")
    plt.ylabel("Processing latency (ms)")
    plt.title("Exp 2: DTM telemetry processing delay")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outdir / "fig3_upstream_latency.pdf")
    plt.close()
    print("  → fig3_upstream_latency.pdf")

    # 3. Drop ratio
    plt.figure()
    plt.plot(x, df["Drop (%)"], marker="^", color="red")
    plt.xlabel("Offered telemetry load (events/s)")
    plt.ylabel("Event drop ratio (%)")
    plt.title("Exp 2: DTM resilience (drop policy)")
    plt.tight_layout()
    plt.savefig(outdir / "fig4_upstream_drops.pdf")
    plt.close()
    print("  → fig4_upstream_drops.pdf")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="NDT federation log analyser")
    ap.add_argument("--logs", required=True, help="Root folder containing experiment sub-folders")
    ap.add_argument("--out", required=True, help="Output folder for PDFs and CSVs")
    args = ap.parse_args()

    input_path = Path(args.logs)
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    # Discover log files (support both naming conventions)
    log_files = (
        list(input_path.rglob("dtm.log"))
        + list(input_path.rglob("ndtm_events.log"))
    )

    if not log_files:
        print(f"[WARN] No log files found under {input_path}")
        return

    results_down, results_up = [], []

    for log_path in log_files:
        exp_name = log_path.parent.name
        df = load_jsonl(log_path)
        if df.empty:
            print(f"  [SKIP] {exp_name}: empty dataframe")
            continue

        if "1to" in exp_name:
            m = compute_downstream(df, exp_name)
            if m:
                results_down.append(m)
            else:
                print(f"  [SKIP] {exp_name}: insufficient downstream data")
        elif "rate" in exp_name:
            m = compute_upstream(df, exp_name)
            if m:
                results_up.append(m)
            else:
                print(f"  [SKIP] {exp_name}: insufficient upstream data")
        else:
            print(f"  [INFO] {exp_name}: folder name not recognised, skipping")

    sep = "=" * 24

    if results_down:
        df_down = pd.DataFrame(results_down)
        print(f"\n[{sep} EXP 1: DOWNSTREAM (SYNC) {sep}]")
        print(tabulate(df_down, headers="keys", tablefmt="psql", showindex=False))
        plot_downstream(df_down, outdir)
        csv_path = outdir / "metrics_exp1_downstream.csv"
        df_down.to_csv(csv_path, index=False)
        print(f"  → {csv_path.name}")

    if results_up:
        df_up = pd.DataFrame(results_up)
        print(f"\n[{sep} EXP 2: UPSTREAM (TELEMETRY) {sep}]")
        print(tabulate(df_up, headers="keys", tablefmt="psql", showindex=False))
        plot_upstream(df_up, outdir)
        csv_path = outdir / "metrics_exp2_upstream.csv"
        df_up.to_csv(csv_path, index=False)
        print(f"  → {csv_path.name}")

    if not results_down and not results_up:
        print("[WARN] No valid experiments found. Check folder names (expect '1toN' or 'rateR').")

    print(f"\n✅ Analysis complete. Results in: {outdir.resolve()}")


if __name__ == "__main__":
    main()