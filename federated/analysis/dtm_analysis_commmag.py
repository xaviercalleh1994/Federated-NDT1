import json
import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate


# ============================================================
# STYLE
# ============================================================
plt.rcParams.update({
    "font.size": 10,
    "figure.figsize": (5.4, 3.4),
    "axes.grid": True,
})


# ============================================================
# HELPERS
# ============================================================
def load_jsonl(path: Path):
    rows = []
    with open(path) as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except:
                pass

    df = pd.DataFrame(rows)

    if "ts_ns" in df.columns:
        df["ts_ns"] = pd.to_numeric(df["ts_ns"], errors="coerce")
        df = df.dropna(subset=["ts_ns"])
        df = df.sort_values("ts_ns")

    return df


def detect_experiment_type(name):
    if name.startswith("rate_"):
        return "load"
    if name.startswith("1to"):
        return "ndts"
    return None


def parse_ndt_value(name):
    return int(name.replace("1to", "").split("_")[0])


def save_plot(fig, path):
    fig.tight_layout()
    fig.savefig(path)
    plt.close(fig)


# ============================================================
# METRICS
# ============================================================
def compute_metrics(df):
    if df.empty:
        return None

    ing = df[df["tag"] == "DTM_INGEST"].copy()
    deq = df[df["tag"] == "DTM_DEQUEUE"].copy()
    done = df[df["tag"] == "DTM_EVENT_DONE"].copy()

    if ing.empty or "t_rx_ns" not in ing.columns:
        return None

    ing["t_rx_ns"] = pd.to_numeric(ing["t_rx_ns"], errors="coerce")
    ing = ing.dropna(subset=["t_rx_ns"])

    t0 = ing["t_rx_ns"].min()
    t1 = ing["t_rx_ns"].max()
    window_s = (t1 - t0) / 1e9

    if window_s <= 0:
        return None

    offered_load = len(ing) / window_s
    proc_capacity = len(deq) / window_s if not deq.empty else 0.0
    proc_eff = len(deq) / len(ing) if len(ing) > 0 else np.nan
    drop_pct = (1 - proc_eff) * 100 if np.isfinite(proc_eff) else np.nan

    # =========================
    # QUEUE LATENCY
    # =========================
    queue = pd.to_numeric(deq.get("queue_ms"), errors="coerce").dropna()

    q_mean = queue.mean()
    q_p95 = queue.quantile(0.95)
    q_p99 = queue.quantile(0.99)

    # =========================
    # SENDQ LATENCY (🔥 NUEVO)
    # =========================
    sendq = pd.to_numeric(done.get("sendq_wait_ms"), errors="coerce").dropna()

    sendq_mean = sendq.mean()
    sendq_p95 = sendq.quantile(0.95)
    sendq_p99 = sendq.quantile(0.99)

    # =========================
    # E2E LATENCY
    # =========================
    latency = pd.Series(dtype=float)

    if not done.empty and "t_done_ns" in done.columns:
        ing2 = ing.groupby("event_id")["t_rx_ns"].min()
        done2 = done.groupby("event_id")["t_done_ns"].max()

        merged = pd.concat([ing2, done2], axis=1).dropna()
        latency = (merged["t_done_ns"] - merged["t_rx_ns"]) / 1e6

    return {
        "window_s": window_s,
        "offered_load": offered_load,
        "proc_capacity": proc_capacity,
        "proc_eff": proc_eff,
        "drop_pct": drop_pct,

        "q_mean_ms": q_mean,
        "q_p95_ms": q_p95,
        "q_p99_ms": q_p99,

        "sendq_mean_ms": sendq_mean,
        "sendq_p95_ms": sendq_p95,
        "sendq_p99_ms": sendq_p99,

        "e2e_mean_ms": latency.mean(),
        "e2e_p95_ms": latency.quantile(0.95),
        "e2e_p99_ms": latency.quantile(0.99),
    }


# ============================================================
# BUILD ROW
# ============================================================
def build_row(exp, mode, metrics):
    row = {
        "exp": exp.name,
        "mode": mode,
        "w_s": metrics["window_s"],
        "offered_load": metrics["offered_load"],
        "proc_capacity": metrics["proc_capacity"],
        "proc_eff": metrics["proc_eff"],
        "drop_pct": metrics["drop_pct"],

        "q_mean_ms": metrics["q_mean_ms"],
        "q_p95_ms": metrics["q_p95_ms"],
        "q_p99_ms": metrics["q_p99_ms"],

        "sendq_mean_ms": metrics["sendq_mean_ms"],
        "sendq_p95_ms": metrics["sendq_p95_ms"],
        "sendq_p99_ms": metrics["sendq_p99_ms"],

        "e2e_mean_ms": metrics["e2e_mean_ms"],
        "e2e_p95_ms": metrics["e2e_p95_ms"],
        "e2e_p99_ms": metrics["e2e_p99_ms"],
    }

    if mode == "ndts":
        row["ndts"] = parse_ndt_value(exp.name)
    else:
        row["ndts"] = np.nan

    return row


# ============================================================
# PLOTS
# ============================================================
def plot_vs_ndts(df, outdir):
    sub = df[df["mode"] == "ndts"].sort_values("ndts")

    fig = plt.figure()
    plt.plot(sub["ndts"], sub["proc_capacity"], marker="o")
    plt.xscale("log", base=2)
    plt.xlabel("NDTs")
    plt.ylabel("Capacity")
    save_plot(fig, outdir / "capacity_vs_ndts.pdf")

    fig = plt.figure()
    plt.plot(sub["ndts"], sub["e2e_mean_ms"], marker="o", label="E2E")
    plt.plot(sub["ndts"], sub["q_mean_ms"], marker="s", label="Queue")
    plt.plot(sub["ndts"], sub["sendq_mean_ms"], marker="^", label="SendQ")

    plt.legend()
    plt.xscale("log", base=2)
    plt.xlabel("NDTs")
    plt.ylabel("Latency (ms)")
    save_plot(fig, outdir / "latency_vs_ndts.pdf")


def plot_core_comparison(df, outdir):
    sub = df[df["mode"] == "ndts"]

    fig = plt.figure()
    for core, g in sub.groupby("core"):
        g = g.sort_values("ndts")
        plt.plot(g["ndts"], g["proc_capacity"], marker="o", label=core)

    plt.legend()
    plt.xscale("log", base=2)
    plt.xlabel("NDTs")
    plt.ylabel("Capacity")
    save_plot(fig, outdir / "compare_capacity.pdf")

    fig = plt.figure()
    for core, g in sub.groupby("core"):
        g = g.sort_values("ndts")
        plt.plot(g["ndts"], g["e2e_mean_ms"], marker="o", label=f"{core}-E2E")
        plt.plot(g["ndts"], g["sendq_mean_ms"], marker="s", linestyle="--", label=f"{core}-SendQ")

    plt.legend()
    plt.xscale("log", base=2)
    plt.xlabel("NDTs")
    plt.ylabel("Latency")
    save_plot(fig, outdir / "compare_latency.pdf")


# ============================================================
# MAIN
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs", required=True)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    root = Path(args.logs)
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    core_dirs = [d for d in root.iterdir() if d.is_dir() and "core" in d.name]
    multi_core = len(core_dirs) > 1

    rows = []

    if multi_core:
        for cdir in core_dirs:
            for exp in cdir.iterdir():
                log = exp / "dtm.log"
                if not log.exists():
                    continue

                df = load_jsonl(log)
                metrics = compute_metrics(df)
                if metrics is None:
                    continue

                row = build_row(exp, detect_experiment_type(exp.name), metrics)
                row["core"] = cdir.name
                rows.append(row)
    else:
        for exp in root.iterdir():
            log = exp / "dtm.log"
            if not log.exists():
                continue

            df = load_jsonl(log)
            metrics = compute_metrics(df)
            if metrics is None:
                continue

            row = build_row(exp, detect_experiment_type(exp.name), metrics)
            rows.append(row)

    if not rows:
        print("No valid experiments found.")
        return

    df = pd.DataFrame(rows)

    if "ndts" in df.columns:
        df = df.sort_values(by="ndts", na_position="last")

    cols = [
        "exp", "w_s", "mode", "ndts",
        "offered_load", "proc_capacity", "proc_eff", "drop_pct",
        "q_mean_ms", "q_p95_ms", "q_p99_ms",
        "sendq_mean_ms", "sendq_p95_ms", "sendq_p99_ms",
        "e2e_mean_ms", "e2e_p95_ms", "e2e_p99_ms",
    ]
    cols = [c for c in cols if c in df.columns]
    df = df[cols]

    print("\nMetrics summary:\n")
    print(tabulate(df, headers="keys", tablefmt="grid", floatfmt=".3f", showindex=False))

    if multi_core:
        plot_core_comparison(df, outdir)
    else:
        plot_vs_ndts(df, outdir)

    df.to_csv(outdir / "metrics.csv", index=False)


if __name__ == "__main__":
    main()