import json
import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


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

    df = pd.DataFrame(rows)

    if not df.empty and "ts_ns" in df.columns:
        df["ts_ns"] = pd.to_numeric(df["ts_ns"], errors="coerce")
        df = df.dropna(subset=["ts_ns"])
        df = df.sort_values("ts_ns")

    return df


def numeric(x):
    return pd.to_numeric(x, errors="coerce")


# ============================================================
# Stable window
# ============================================================

def fixed_window(df, warmup_s, cooldown_s):

    t_min = df["ts_ns"].min()
    t_max = df["ts_ns"].max()

    t0 = t_min + warmup_s * 1e9
    t1 = t_max - cooldown_s * 1e9

    if t1 <= t0:
        return None, None, np.nan

    return int(t0), int(t1), (t1 - t0) / 1e9


# ============================================================
# Metrics
# ============================================================

def extract_metrics(dtm_df, ndts, warmup_s, cooldown_s):

    if dtm_df.empty:
        return None

    t0, t1, window_s = fixed_window(dtm_df, warmup_s, cooldown_s)
    if not np.isfinite(window_s):
        return None

    w = dtm_df[(dtm_df["ts_ns"] >= t0) &
               (dtm_df["ts_ns"] <= t1)]

    ing = w[w["tag"] == "DTM_INGEST"]
    deq = w[w["tag"] == "DTM_DEQUEUE"]

    out = {}

    # Coordination capacity
    out["coord_capacity_eps"] = len(deq) / window_s

    # Coordination latency
    if "queue_ms" in deq.columns:
        out["coord_latency_mean_ms"] = \
            numeric(deq["queue_ms"]).mean()
    else:
        out["coord_latency_mean_ms"] = np.nan

    # Packet loss percentage
    if len(ing) > 0:
        drop = 1 - (len(deq) / len(ing))
        drop = max(0.0, min(1.0, drop))
        out["drop_pct"] = drop * 100
    else:
        out["drop_pct"] = np.nan

    return out


# ============================================================
# Plot with CI (log Y)
# ============================================================

def plot_with_ci(df, metric, ylabel, outpath, logy=True):

    plt.figure(figsize=(6,4))

    for cores in sorted(df["cores"].unique()):

        sub = df[df["cores"] == cores]
        sub = sub.sort_values("ndts")

        x = sub["ndts"].values
        y = sub[f"{metric}_mean"].values
        ci = sub[f"{metric}_ci95"].values

        if logy:
            y = np.maximum(y, 1e-9)

        ci = np.nan_to_num(ci)

        plt.errorbar(
            x,
            y,
            yerr=ci,
            marker="o",
            capsize=4,
            linewidth=2,
            label=f"{cores} cores"
        )

    plt.xscale("log", base=2)

    if logy:
        plt.yscale("log")

    plt.xlabel("Number of NDTs (log₂ scale)")
    plt.ylabel(ylabel)

    if not logy:
        plt.ylim(0, 100)

    plt.grid(True, which="both", linestyle="--", alpha=0.4)
    plt.legend(frameon=False)
    plt.tight_layout()
    plt.savefig(outpath, dpi=300)
    plt.close()


# ============================================================
# Main
# ============================================================

def main():

    ap = argparse.ArgumentParser()
    ap.add_argument("root", help="Folder containing run1, run2, ...")
    ap.add_argument("outdir", help="Output directory")
    ap.add_argument("--warmup", type=float, default=5.0)
    ap.add_argument("--cooldown", type=float, default=5.0)
    args = ap.parse_args()

    root = Path(args.root)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    pat_core = re.compile(r"(\d+)_core")
    pat_exp = re.compile(r"1to(\d+)_(tcp|udp)")

    rows = []

    for run in root.iterdir():

        if not run.name.startswith("run"):
            continue

        logs = run / "logs"
        if not logs.exists():
            continue

        for core_dir in logs.iterdir():

            mcore = pat_core.match(core_dir.name)
            if not mcore:
                continue

            cores = int(mcore.group(1))

            for exp in core_dir.iterdir():

                mexp = pat_exp.match(exp.name)
                if not mexp:
                    continue

                ndts = int(mexp.group(1))
                proto = mexp.group(2)

                dtm_log = exp / "dtm.log"
                if not dtm_log.exists():
                    continue

                df = load_jsonl(dtm_log)

                met = extract_metrics(
                    df,
                    ndts,
                    args.warmup,
                    args.cooldown
                )

                if met is None:
                    continue

                met["run"] = run.name
                met["cores"] = cores
                met["ndts"] = ndts
                met["proto"] = proto

                rows.append(met)

    df = pd.DataFrame(rows)

    if df.empty:
        raise SystemExit("No valid experiments found.")

    grouped = df.groupby(["cores", "ndts", "proto"])

    agg = grouped.agg(
        coord_capacity_eps_mean=("coord_capacity_eps", "mean"),
        coord_capacity_eps_std=("coord_capacity_eps", "std"),
        coord_latency_mean_ms_mean=("coord_latency_mean_ms", "mean"),
        coord_latency_mean_ms_std=("coord_latency_mean_ms", "std"),
        drop_pct_mean=("drop_pct", "mean"),
        drop_pct_std=("drop_pct", "std"),
    ).reset_index()

    n = df["run"].nunique()

    agg["coord_capacity_eps_ci95"] = \
        1.96 * agg["coord_capacity_eps_std"] / np.sqrt(n)

    agg["coord_latency_mean_ms_ci95"] = \
        1.96 * agg["coord_latency_mean_ms_std"] / np.sqrt(n)

    agg["drop_pct_ci95"] = \
        1.96 * agg["drop_pct_std"] / np.sqrt(n)

    agg.sort_values("ndts", inplace=True)

    agg.to_csv(outdir / "summary_with_ci.csv", index=False)

    tcp_df = agg[agg["proto"] == "tcp"]

    plot_with_ci(
        tcp_df,
        "coord_capacity_eps",
        "Coordination capacity (events/s)",
        outdir / "capacity_tcp_logy.png",
        logy=True
    )

    plot_with_ci(
        tcp_df,
        "coord_latency_mean_ms",
        "Coordination latency (ms)",
        outdir / "latency_tcp_logy.png",
        logy=True
    )

    # NEW: Packet loss plot (linear Y)
    plot_with_ci(
        tcp_df,
        "drop_pct",
        "Packet loss (%)",
        outdir / "drop_pct_tcp.png",
        logy=False
    )

    print("[OK] Multi-run analysis complete →", outdir)


if __name__ == "__main__":
    main()