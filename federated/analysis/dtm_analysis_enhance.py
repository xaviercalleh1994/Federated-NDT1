import json
import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# ============================================================
# IEEE-like style
# ============================================================
plt.rcParams.update({
    "font.size": 10,
    "axes.titlesize": 10,
    "axes.labelsize": 10,
    "legend.fontsize": 9,
    "xtick.labelsize": 9,
    "ytick.labelsize": 9,
    "figure.figsize": (5.2, 3.2),
    "axes.grid": True,
    "grid.alpha": 0.35,
    "grid.linestyle": "--",
    "savefig.dpi": 300,
})


# ============================================================
# Helpers
# ============================================================
def load_jsonl(path: Path) -> pd.DataFrame:
    rows = []
    with open(path) as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except Exception:
                pass

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    if "ts_ns" in df.columns:
        df["ts_ns"] = pd.to_numeric(df["ts_ns"], errors="coerce")
        df = df.dropna(subset=["ts_ns"])
        df = df.sort_values("ts_ns")

    return df


def detect_experiment_type(name: str):
    if re.fullmatch(r"rate_.+", name):
        return "load"
    if re.fullmatch(r"1to\d+", name):
        return "ndts"
    return None


def parse_load_value(name: str):
    return float(name.replace("rate_", "").replace("_", "."))


def parse_ndt_value(name: str):
    return int(name.replace("1to", ""))


def compute_window_from_ingest(df: pd.DataFrame):
    ing = df[df.tag == "DTM_INGEST"]
    if ing.empty:
        return None, None, None

    t0 = ing.ts_ns.min()
    t1 = ing.ts_ns.max()
    window_s = (t1 - t0) / 1e9

    if window_s <= 0:
        return None, None, None

    return int(t0), int(t1), float(window_s)


def compute_metrics(df: pd.DataFrame):
    if df.empty:
        return None

    t0, t1, window_s = compute_window_from_ingest(df)
    if window_s is None:
        return None

    ing = df[(df.tag == "DTM_INGEST") & (df.ts_ns >= t0) & (df.ts_ns <= t1)]
    deq = df[df.tag == "DTM_DEQUEUE"]
    done = df[df.tag == "DTM_EVENT_DONE"]
    disp = df[df.tag == "NDT_EVENT_DISPATCHED"]

    event_ids = set(ing.event_id.dropna().astype(str))
    deq = deq[deq.event_id.astype(str).isin(event_ids)] if not deq.empty and "event_id" in deq.columns else pd.DataFrame()
    done = done[done.event_id.astype(str).isin(event_ids)] if not done.empty and "event_id" in done.columns else pd.DataFrame()

    offered_load = len(ing) / window_s
    processing_capacity = len(deq) / window_s

    processing_eff = (len(deq) / len(ing)) if len(ing) > 0 else np.nan
    drop_pct = max(0.0, (1.0 - processing_eff) * 100.0) if np.isfinite(processing_eff) else np.nan

    queue_delays = pd.to_numeric(deq.get("queue_ms", pd.Series(dtype=float)), errors="coerce").dropna()
    qmean = queue_delays.mean() if not queue_delays.empty else np.nan
    qp95 = queue_delays.quantile(0.95) if not queue_delays.empty else np.nan
    qp99 = queue_delays.quantile(0.99) if not queue_delays.empty else np.nan

    # DTM internal latency approximation from INGEST and DONE for common event_ids
    total_lat_ms = np.nan
    latency_series = pd.Series(dtype=float)
    if not ing.empty and not done.empty and "event_id" in ing.columns and "event_id" in done.columns:
        ing2 = ing[["event_id", "ts_ns"]].copy()
        ing2["event_id"] = ing2["event_id"].astype(str)
        ing2 = ing2.groupby("event_id", as_index=False)["ts_ns"].min()

        done2 = done[["event_id", "ts_ns"]].copy()
        done2["event_id"] = done2["event_id"].astype(str)
        done2 = done2.groupby("event_id", as_index=False)["ts_ns"].max()

        merged = ing2.merge(done2, on="event_id", suffixes=("_ing", "_done"))
        if not merged.empty:
            latency_series = (merged["ts_ns_done"] - merged["ts_ns_ing"]) / 1e6
            total_lat_ms = latency_series.mean()

    # Dispatch efficiency
    dispatch_eff = np.nan
    if not disp.empty and "sent_ok" in disp.columns and "fanout" in disp.columns:
        sent = pd.to_numeric(disp["sent_ok"], errors="coerce").fillna(0).sum()
        expected = pd.to_numeric(disp["fanout"], errors="coerce").fillna(0).sum()
        dispatch_eff = (sent / expected) if expected > 0 else np.nan

    return {
        "window_s": window_s,
        "offered_load": offered_load,
        "processing_capacity": processing_capacity,
        "processing_eff": processing_eff,
        "drop_pct": drop_pct,
        "queue_mean_ms": qmean,
        "queue_p95_ms": qp95,
        "queue_p99_ms": qp99,
        "latency_mean_ms": total_lat_ms,
        "dispatch_eff": dispatch_eff,
        "latency_series_ms": latency_series,
    }


# ============================================================
# Plot helpers
# ============================================================
def save_plot(fig, path: Path):
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)


def plot_vs_load(df: pd.DataFrame, outdir: Path):
    sub = df[df["mode"] == "load"].sort_values("offered_load")
    if sub.empty:
        return

    # 1) Throughput vs Offered Load
    fig = plt.figure()
    plt.plot(sub["offered_load"], sub["processing_capacity"], marker="o", label="Processed")
    lim = max(sub["offered_load"].max(), sub["processing_capacity"].max()) * 1.05
    plt.plot([0, lim], [0, lim], linestyle="--", label="Ideal")
    plt.xlabel("Offered Load (events/s)")
    plt.ylabel("Processing Capacity (events/s)")
    plt.title("DTM Throughput")
    plt.legend(frameon=False)
    save_plot(fig, outdir / "fig1_throughput_vs_load.pdf")

    # 2) Processing Efficiency
    fig = plt.figure()
    plt.plot(sub["offered_load"], sub["processing_eff"], marker="o")
    plt.xlabel("Offered Load (events/s)")
    plt.ylabel("Processing Efficiency")
    plt.title("DTM Processing Efficiency")
    plt.ylim(0, 1.05)
    save_plot(fig, outdir / "fig2_processing_efficiency_vs_load.pdf")

    # 3) Queue Delay
    fig = plt.figure()
    plt.plot(sub["offered_load"], sub["queue_mean_ms"], marker="o", label="Mean")
    plt.plot(sub["offered_load"], sub["queue_p95_ms"], marker="s", label="P95")
    plt.plot(sub["offered_load"], sub["queue_p99_ms"], marker="^", label="P99")
    plt.xlabel("Offered Load (events/s)")
    plt.ylabel("Queue Delay (ms)")
    plt.title("Queueing Delay")
    plt.legend(frameon=False)
    save_plot(fig, outdir / "fig3_queue_delay_vs_load.pdf")

    # 4) Drop vs Load
    fig = plt.figure()
    plt.plot(sub["offered_load"], sub["drop_pct"], marker="o")
    plt.xlabel("Offered Load (events/s)")
    plt.ylabel("Drop Ratio (%)")
    plt.title("Packet/Event Drop Ratio")
    save_plot(fig, outdir / "fig4_drop_vs_load.pdf")

    # 5) Latency CDF
    latencies = []
    for _, row in sub.iterrows():
        s = row["latency_series_ms"]
        if isinstance(s, pd.Series) and not s.empty:
            latencies.extend(s.tolist())

    if latencies:
        lat = np.sort(np.array(latencies))
        y = np.arange(1, len(lat) + 1) / len(lat)

        fig = plt.figure()
        plt.plot(lat, y)
        plt.xlabel("DTM Event Latency (ms)")
        plt.ylabel("CDF")
        plt.title("Latency Distribution")
        save_plot(fig, outdir / "fig5_latency_cdf.pdf")


def plot_vs_ndts(df: pd.DataFrame, outdir: Path):
    sub = df[df["mode"] == "ndts"].sort_values("ndts")
    if sub.empty:
        return

    # 6) Capacity vs NDT count
    fig = plt.figure()
    plt.plot(sub["ndts"], sub["processing_capacity"], marker="o")
    plt.xscale("log", base=2)
    plt.xlabel("Number of NDTs")
    plt.ylabel("Processing Capacity (events/s)")
    plt.title("Scalability of the DTM")
    save_plot(fig, outdir / "fig6_capacity_vs_ndts.pdf")

    # 7) Efficiency vs NDT count
    fig = plt.figure()
    plt.plot(sub["ndts"], sub["processing_eff"], marker="o")
    plt.xscale("log", base=2)
    plt.xlabel("Number of NDTs")
    plt.ylabel("Processing Efficiency")
    plt.title("Efficiency under Federation Scale")
    plt.ylim(0, 1.05)
    save_plot(fig, outdir / "fig7_efficiency_vs_ndts.pdf")

    # 8) Queue p95 vs NDT count
    fig = plt.figure()
    plt.plot(sub["ndts"], sub["queue_p95_ms"], marker="s")
    plt.xscale("log", base=2)
    plt.xlabel("Number of NDTs")
    plt.ylabel("Queue Delay P95 (ms)")
    plt.title("Tail Queueing Delay vs Federation Size")
    save_plot(fig, outdir / "fig8_queue_p95_vs_ndts.pdf")


# ============================================================
# Main
# ============================================================
def main():
    ap = argparse.ArgumentParser(description="Deterministic DTM log analysis for IEEE-style figures")
    ap.add_argument("--logs", required=True, help="Root directory containing experiment folders")
    ap.add_argument("--out", required=True, help="Directory where figures and CSV will be saved")
    args = ap.parse_args()

    root = Path(args.logs)
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    rows = []

    for exp in sorted(root.iterdir()):
        if not exp.is_dir():
            continue

        mode = detect_experiment_type(exp.name)
        if mode is None:
            continue

        log = exp / "dtm.log"
        if not log.exists():
            continue

        df = load_jsonl(log)
        metrics = compute_metrics(df)
        if metrics is None:
            continue

        row = {
            "experiment": exp.name,
            "mode": mode,
            "offered_load": metrics["offered_load"],
            "processing_capacity": metrics["processing_capacity"],
            "processing_eff": metrics["processing_eff"],
            "drop_pct": metrics["drop_pct"],
            "queue_mean_ms": metrics["queue_mean_ms"],
            "queue_p95_ms": metrics["queue_p95_ms"],
            "queue_p99_ms": metrics["queue_p99_ms"],
            "latency_mean_ms": metrics["latency_mean_ms"],
            "dispatch_eff": metrics["dispatch_eff"],
            "window_s": metrics["window_s"],
            "latency_series_ms": metrics["latency_series_ms"],
        }

        if mode == "load":
            row["configured_rate"] = parse_load_value(exp.name)
            row["ndts"] = np.nan
        else:
            row["configured_rate"] = np.nan
            row["ndts"] = parse_ndt_value(exp.name)

        rows.append(row)

    if not rows:
        print("No valid experiments found.")
        return

    df = pd.DataFrame(rows)
    df_no_series = df.drop(columns=["latency_series_ms"])
    df_no_series.to_csv(outdir / "metrics.csv", index=False)

    print("\nMetrics summary:\n")
    print(df_no_series.to_string(index=False))

    plot_vs_load(df, outdir)
    plot_vs_ndts(df, outdir)

    print(f"\nSaved results in: {outdir}")


if __name__ == "__main__":
    main()