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
            except Exception:
                pass

    df = pd.DataFrame(rows)

    if df.empty:
        return df

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


def summarize_series(s: pd.Series, prefix: str):
    s = pd.to_numeric(s, errors="coerce").dropna()
    if s.empty:
        return {
            f"{prefix}_mean_ms": np.nan,
            f"{prefix}_p50_ms": np.nan,
            f"{prefix}_p95_ms": np.nan,
            f"{prefix}_p99_ms": np.nan,
        }

    return {
        f"{prefix}_mean_ms": s.mean(),
        f"{prefix}_p50_ms": s.quantile(0.50),
        f"{prefix}_p95_ms": s.quantile(0.95),
        f"{prefix}_p99_ms": s.quantile(0.99),
    }


# ============================================================
# METRICS
# ============================================================
def compute_metrics(df):
    if df.empty:
        return None

    ing = df[df["tag"] == "DTM_INGEST"].copy()
    deq = df[df["tag"] == "DTM_DEQUEUE"].copy()
    done = df[df["tag"] == "DTM_EVENT_DONE"].copy()

    if ing.empty or done.empty or "t_rx_ns" not in ing.columns or "t_done_ns" not in done.columns:
        return None

    # -----------------------------
    # Clean timestamps / ids
    # -----------------------------
    ing["t_rx_ns"] = pd.to_numeric(ing["t_rx_ns"], errors="coerce")
    done["t_done_ns"] = pd.to_numeric(done["t_done_ns"], errors="coerce")

    ing = ing.dropna(subset=["t_rx_ns", "event_id"]).copy()
    done = done.dropna(subset=["t_done_ns", "event_id"]).copy()

    ing["event_id"] = ing["event_id"].astype(str)
    done["event_id"] = done["event_id"].astype(str)

    if not deq.empty and "event_id" in deq.columns:
        deq = deq.dropna(subset=["event_id"]).copy()
        deq["event_id"] = deq["event_id"].astype(str)
    else:
        deq = pd.DataFrame()

    # -----------------------------
    # Match only valid events
    # -----------------------------
    valid_ids = set(ing["event_id"]).intersection(set(done["event_id"]))
    if not valid_ids:
        return None

    ing = ing[ing["event_id"].isin(valid_ids)].copy()
    done = done[done["event_id"].isin(valid_ids)].copy()
    if not deq.empty:
        deq = deq[deq["event_id"].isin(valid_ids)].copy()

    # -----------------------------
    # Use one row per event for ingest/done
    # -----------------------------
    ing_ev = ing.groupby("event_id", as_index=False)["t_rx_ns"].min()
    done_ev = done.groupby("event_id", as_index=False)["t_done_ns"].max()

    # window based on valid ingest events
    t0 = ing_ev["t_rx_ns"].min()
    t1 = ing_ev["t_rx_ns"].max()
    window_s = (t1 - t0) / 1e9

    if window_s <= 0:
        return None

    # -----------------------------
    # Throughput / load / efficiency
    # -----------------------------
    offered_load = len(ing_ev) / window_s
    proc_capacity = len(done_ev) / window_s
    proc_eff = len(done_ev) / len(ing_ev) if len(ing_ev) > 0 else np.nan
    drop_pct = (1.0 - proc_eff) * 100.0 if np.isfinite(proc_eff) else np.nan
    if np.isfinite(drop_pct):
        drop_pct = max(0.0, drop_pct)

    # -----------------------------
    # Queue latency
    # -----------------------------
    if not deq.empty and "queue_ms" in deq.columns:
        deq["queue_ms"] = pd.to_numeric(deq["queue_ms"], errors="coerce")
        deq_valid = (
            deq.dropna(subset=["queue_ms"])
               .groupby("event_id", as_index=False)["queue_ms"]
               .min()
        )
        queue = deq_valid["queue_ms"]
    else:
        queue = pd.Series(dtype=float)

    q_stats = summarize_series(queue, "q")

    # -----------------------------
    # SendQ latency
    # stored in DONE as sendq_wait_ms
    # -----------------------------
    if "sendq_wait_ms" in done.columns:
        done["sendq_wait_ms"] = pd.to_numeric(done["sendq_wait_ms"], errors="coerce")
        sendq_valid = (
            done.dropna(subset=["sendq_wait_ms"])
                .groupby("event_id", as_index=False)["sendq_wait_ms"]
                .max()
        )
        sendq = sendq_valid["sendq_wait_ms"]
    else:
        sendq = pd.Series(dtype=float)

    sendq_stats = summarize_series(sendq, "sendq")

    # -----------------------------
    # E2E latency
    # -----------------------------
    merged = ing_ev.merge(done_ev, on="event_id", how="inner")
    latency = (merged["t_done_ns"] - merged["t_rx_ns"]) / 1e6

    e2e_stats = summarize_series(latency, "e2e")

    return {
        "window_s": window_s,
        "offered_load": offered_load,
        "proc_capacity": proc_capacity,
        "proc_eff": proc_eff,
        "drop_pct": drop_pct,

        **q_stats,
        **sendq_stats,
        **e2e_stats,
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
        "q_p50_ms": metrics["q_p50_ms"],
        "q_p95_ms": metrics["q_p95_ms"],
        "q_p99_ms": metrics["q_p99_ms"],

        "sendq_mean_ms": metrics["sendq_mean_ms"],
        "sendq_p50_ms": metrics["sendq_p50_ms"],
        "sendq_p95_ms": metrics["sendq_p95_ms"],
        "sendq_p99_ms": metrics["sendq_p99_ms"],

        "e2e_mean_ms": metrics["e2e_mean_ms"],
        "e2e_p50_ms": metrics["e2e_p50_ms"],
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
    if sub.empty:
        return

    # Capacity
    fig = plt.figure()
    plt.plot(sub["ndts"], sub["proc_capacity"], marker="o")
    plt.xscale("log", base=2)
    plt.xlabel("NDTs")
    plt.ylabel("Capacity")
    plt.title("Capacity vs NDTs")
    save_plot(fig, outdir / "capacity_vs_ndts.pdf")

    # Mean latency decomposition
    fig = plt.figure()
    plt.plot(sub["ndts"], sub["e2e_mean_ms"], marker="o", label="E2E mean")
    plt.plot(sub["ndts"], sub["q_mean_ms"], marker="s", label="Queue mean")
    plt.plot(sub["ndts"], sub["sendq_mean_ms"], marker="^", label="SendQ mean")
    plt.legend()
    plt.xscale("log", base=2)
    plt.xlabel("NDTs")
    plt.ylabel("Latency (ms)")
    plt.title("Mean Latency vs NDTs")
    save_plot(fig, outdir / "latency_vs_ndts.pdf")

    # Tail latency
    fig = plt.figure()
    plt.plot(sub["ndts"], sub["q_p95_ms"], marker="s", label="Queue p95")
    plt.plot(sub["ndts"], sub["sendq_p95_ms"], marker="^", label="SendQ p95")
    plt.plot(sub["ndts"], sub["e2e_p95_ms"], marker="o", label="E2E p95")
    plt.legend()
    plt.xscale("log", base=2)
    plt.xlabel("NDTs")
    plt.ylabel("Latency (ms)")
    plt.title("P95 Latency vs NDTs")
    save_plot(fig, outdir / "latency_p95_vs_ndts.pdf")

    # P50 latency
    fig = plt.figure()
    plt.plot(sub["ndts"], sub["q_p50_ms"], marker="s", label="Queue p50")
    plt.plot(sub["ndts"], sub["sendq_p50_ms"], marker="^", label="SendQ p50")
    plt.plot(sub["ndts"], sub["e2e_p50_ms"], marker="o", label="E2E p50")
    plt.legend()
    plt.xscale("log", base=2)
    plt.xlabel("NDTs")
    plt.ylabel("Latency (ms)")
    plt.title("P50 Latency vs NDTs")
    save_plot(fig, outdir / "latency_p50_vs_ndts.pdf")


def plot_core_comparison(df, outdir):
    sub = df[df["mode"] == "ndts"].copy()
    if sub.empty or "core" not in sub.columns:
        return

    # Capacity comparison
    fig = plt.figure()
    for core, g in sub.groupby("core"):
        g = g.sort_values("ndts")
        plt.plot(g["ndts"], g["proc_capacity"], marker="o", label=core)
    plt.legend()
    plt.xscale("log", base=2)
    plt.xlabel("NDTs")
    plt.ylabel("Capacity")
    plt.title("Capacity Comparison")
    save_plot(fig, outdir / "compare_capacity.pdf")

    # E2E mean + SendQ mean comparison
    fig = plt.figure()
    for core, g in sub.groupby("core"):
        g = g.sort_values("ndts")
        plt.plot(g["ndts"], g["e2e_mean_ms"], marker="o", label=f"{core}-E2E mean")
        plt.plot(g["ndts"], g["sendq_mean_ms"], marker="s", linestyle="--", label=f"{core}-SendQ mean")
    plt.legend()
    plt.xscale("log", base=2)
    plt.xlabel("NDTs")
    plt.ylabel("Latency")
    plt.title("Latency Comparison")
    save_plot(fig, outdir / "compare_latency.pdf")

    # p95 comparison
    fig = plt.figure()
    for core, g in sub.groupby("core"):
        g = g.sort_values("ndts")
        plt.plot(g["ndts"], g["e2e_p95_ms"], marker="o", label=f"{core}-E2E p95")
        plt.plot(g["ndts"], g["sendq_p95_ms"], marker="^", linestyle="--", label=f"{core}-SendQ p95")
    plt.legend()
    plt.xscale("log", base=2)
    plt.xlabel("NDTs")
    plt.ylabel("Latency")
    plt.title("P95 Latency Comparison")
    save_plot(fig, outdir / "compare_latency_p95.pdf")


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
                if not exp.is_dir():
                    continue

                log = exp / "dtm.log"
                if not log.exists():
                    continue

                mode = detect_experiment_type(exp.name)
                if mode is None:
                    continue

                df = load_jsonl(log)
                metrics = compute_metrics(df)
                if metrics is None:
                    continue

                row = build_row(exp, mode, metrics)
                row["core"] = cdir.name
                rows.append(row)
    else:
        for exp in root.iterdir():
            if not exp.is_dir():
                continue

            log = exp / "dtm.log"
            if not log.exists():
                continue

            mode = detect_experiment_type(exp.name)
            if mode is None:
                continue

            df = load_jsonl(log)
            metrics = compute_metrics(df)
            if metrics is None:
                continue

            row = build_row(exp, mode, metrics)
            rows.append(row)

    if not rows:
        print("No valid experiments found.")
        return

    df_all = pd.DataFrame(rows)

    if "ndts" in df_all.columns:
        df_all = df_all.sort_values(by="ndts", na_position="last")

    cols = [
        "exp", "w_s", "mode", "ndts",
        "offered_load", "proc_capacity", "proc_eff", "drop_pct",
        "q_mean_ms", "q_p50_ms", "q_p95_ms", "q_p99_ms",
        "sendq_mean_ms", "sendq_p50_ms", "sendq_p95_ms", "sendq_p99_ms",
        "e2e_mean_ms", "e2e_p50_ms", "e2e_p95_ms", "e2e_p99_ms",
    ]
    if "core" in df_all.columns:
        cols = ["core"] + cols

    cols = [c for c in cols if c in df_all.columns]
    df_print = df_all[cols]

    print("\nMetrics summary:\n")
    print(tabulate(df_print, headers="keys", tablefmt="grid", floatfmt=".3f", showindex=False))

    if multi_core:
        plot_core_comparison(df_all, outdir)
    else:
        plot_vs_ndts(df_all, outdir)

    df_all.to_csv(outdir / "metrics.csv", index=False)


if __name__ == "__main__":
    main()