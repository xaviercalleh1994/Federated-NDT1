"""
NDT Federation Loss Analyser — uniform vs Poisson, multiple runs
================================================================
Reads multiple experimental runs and produces plots with 95% confidence
intervals.

Folder layout expected
----------------------
losses/
  run1/
    uniform/
      1to1/    dtm.log
      1to5/    dtm.log
      1to10/   dtm.log
      1to11/   dtm.log
      1to15/   dtm.log
      1to20/   dtm.log
    poisson/
      1to1/    dtm.log
      ...
  run2/
    uniform/
      ...
    poisson/
      ...
  run3/
    ...

Usage
-----
python3 analyse_losses.py \\
    --logs /home/ubuntu/final_test/experiment2/losses/ \\
    --out  /home/ubuntu/final_test/results/losses/ \\
    --C    2000
"""

import re
import argparse
from pathlib import Path
from typing import Optional, Dict, List

try:
    import orjson
    def _parse(line) -> dict: return orjson.loads(line)
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

# ---------------------------------------------------------------------------
# IEEE style
# ---------------------------------------------------------------------------
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

MODE_STYLE = {
    "uniform": {"color": "#185FA5", "marker": "o", "linestyle": "-",  "label": "Uniform"},
    "poisson": {"color": "#D85A30", "marker": "s", "linestyle": "--", "label": "Poisson"},
}

CI = 0.95  # confidence level


# ---------------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------------
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


def find_log(folder: Path) -> Optional[Path]:
    for name in ("dtm.log", "ndtm_events.log", "dtm_events.log"):
        p = folder / name
        if p.exists():
            return p
    return None


def extract_ndts(folder_name: str) -> Optional[int]:
    m = re.search(r"1to(\d+)$", folder_name)
    return int(m.group(1)) if m else None


# ---------------------------------------------------------------------------
# Metrics for one log file
# ---------------------------------------------------------------------------
def compute_metrics(df: pd.DataFrame, n_ndts: int, C: float) -> Optional[dict]:
    if "tag" not in df.columns:
        return None

    rx        = df[df["tag"] == "NDT_EVENT_RX"]
    done_mask = df["tag"] == "DTM_EVENT_DONE"
    if "src" in df.columns:
        done_mask &= (df["src"] == "feedback")
    done = df[done_mask]
    drop = df[df["tag"] == "DTM_DROP"]

    total_rx   = len(rx)
    total_done = len(done)
    total_drop = len(drop)

    if total_rx == 0:
        return None

    t_rx = pd.to_numeric(rx["ts_ns"], errors="coerce").dropna()
    if len(t_rx) < 2:
        return None

    window_s  = (t_rx.max() - t_rx.min()) / 1e9
    offered   = total_rx   / window_s if window_s > 0 else 0.0
    capacity  = total_done / window_s if window_s > 0 else 0.0
    drop_pct  = total_drop / total_rx * 100.0
    theo_drop = max(0.0, (offered - C) / offered * 100.0) if offered > 0 else 0.0

    proc_lat_mean = proc_lat_p95 = float("nan")
    queue_mean    = queue_p95    = float("nan")

    if not done.empty:
        if {"t_rx_ns", "t_done_ns"}.issubset(done.columns):
            lats = (
                pd.to_numeric(done["t_done_ns"], errors="coerce") -
                pd.to_numeric(done["t_rx_ns"],   errors="coerce")
            ).dropna() / 1e6
            if not lats.empty:
                proc_lat_mean = float(lats.mean())
                proc_lat_p95  = float(lats.quantile(0.95))

        if "queue_ms" in done.columns:
            qms = pd.to_numeric(done["queue_ms"], errors="coerce").dropna()
            if not qms.empty:
                queue_mean = float(qms.mean())
                queue_p95  = float(qms.quantile(0.95))

    return {
        "NDTs":                 n_ndts,
        "Offered (ev/s)":       round(offered,       1),
        "Capacity (ev/s)":      round(capacity,      1),
        "Drop (%)":             round(drop_pct,      2),
        "Theoretical Drop (%)": round(theo_drop,     2),
        "Queue Mean (ms)":      round(queue_mean,    3),
        "Queue P95 (ms)":       round(queue_p95,     3),
        "Proc Lat Mean (ms)":   round(proc_lat_mean, 3),
        "Proc Lat P95 (ms)":    round(proc_lat_p95,  3),
    }


# ---------------------------------------------------------------------------
# Load one run / one mode
# ---------------------------------------------------------------------------
def load_run_mode(mode_dir: Path, C: float, run_name: str) -> pd.DataFrame:
    if not mode_dir.exists():
        return pd.DataFrame()

    folders = sorted(
        [d for d in mode_dir.iterdir()
         if d.is_dir() and extract_ndts(d.name) is not None],
        key=lambda d: extract_ndts(d.name),
    )

    results = []
    for folder in folders:
        n   = extract_ndts(folder.name)
        log = find_log(folder)
        if log is None:
            continue
        df = load_log(log)
        if df.empty:
            continue
        m = compute_metrics(df, n, C)
        if m is None:
            continue
        m["run"] = run_name
        results.append(m)

    if not results:
        return pd.DataFrame()
    return pd.DataFrame(results).sort_values("NDTs").reset_index(drop=True)


# ---------------------------------------------------------------------------
# Load all runs for one mode — returns DataFrame with all runs stacked
# ---------------------------------------------------------------------------
def load_all_runs(logs_path: Path, mode: str, C: float) -> pd.DataFrame:
    run_dirs = sorted([
        d for d in logs_path.iterdir()
        if d.is_dir() and (d / mode).exists()
    ])

    if not run_dirs:
        # fallback: maybe logs_path itself contains the mode folder directly
        mode_dir = logs_path / mode
        if mode_dir.exists():
            df = load_run_mode(mode_dir, C, "run1")
            if not df.empty:
                print(f"  Found single run for {mode}")
            return df

    frames = []
    for run_dir in run_dirs:
        mode_dir = run_dir / mode
        df = load_run_mode(mode_dir, C, run_dir.name)
        if not df.empty:
            frames.append(df)
            print(f"  {run_dir.name}/{mode}: {len(df)} experiments")

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


# ---------------------------------------------------------------------------
# Aggregate: mean + 95% CI per NDT value
# ---------------------------------------------------------------------------
def aggregate(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """Returns DataFrame with columns: NDTs, mean, ci_low, ci_high."""
    results = []
    for ndts, group in df.groupby("NDTs"):
        vals = group[col].dropna().values
        n    = len(vals)
        if n == 0:
            continue
        mean = float(np.mean(vals))
        if n == 1:
            ci_low = ci_high = mean
        else:
            se = stats.sem(vals)
            t  = stats.t.ppf((1 + CI) / 2, df=n - 1)
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


# ---------------------------------------------------------------------------
# Y-axis in multiples of C
# ---------------------------------------------------------------------------
def set_yaxis_C_ticks(ax, C: float, max_val: float) -> None:
    fracs  = [i / 2 for i in range(0, int(max_val / C * 2) + 3)]
    ticks  = [f * C for f in fracs if f * C <= max_val * 1.1]
    labels = []
    for f in fracs:
        if f * C > max_val * 1.1:
            break
        if f == 0:
            labels.append("0")
        elif f == 1:
            labels.append("$C$")
        elif f.is_integer():
            labels.append(f"${int(f)}C$")
        else:
            num = int(f * 2)
            labels.append(f"$\\frac{{{num}}}{{2}}C$")
    ax.set_yticks(ticks)
    ax.set_yticklabels(labels)


# ---------------------------------------------------------------------------
# Plot helpers
# ---------------------------------------------------------------------------
def _plot_ci(ax, agg: pd.DataFrame, style: dict, label: str) -> None:
    if agg.empty:
        return
    import numpy as _np
    yerr = _np.array([
        agg["mean"].values - agg["ci_low"].values,
        agg["ci_high"].values - agg["mean"].values,
    ])
    ax.errorbar(
        agg["NDTs"], agg["mean"],
        yerr=yerr,
        color=style["color"],
        marker=style["marker"],
        linestyle=style["linestyle"],
        label=label,
        linewidth=1.5,
        markersize=5,
        capsize=4,
        capthick=1.2,
        elinewidth=1.2,
    )


# ---------------------------------------------------------------------------
# Plots
# ---------------------------------------------------------------------------
def plot_drop(data: Dict[str, pd.DataFrame], C: float, outdir: Path) -> None:
    fig, ax = plt.subplots()

    # theoretical curve from first available mode
    ref = next((df for df in data.values() if not df.empty), None)
    if ref is not None:
        theo = aggregate(ref, "Theoretical Drop (%)")
        if not theo.empty:
            ax.plot(theo["NDTs"], theo["mean"],
                    color="gray", linestyle=":", linewidth=1.2,
                    label="Theoretical (no buffer)")

    for mode, df in data.items():
        agg = aggregate(df, "Drop (%)")
        _plot_ci(ax, agg, MODE_STYLE[mode], MODE_STYLE[mode]["label"])

    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Event drop ratio (%)")
    ax.set_title(f"Drop % vs federation size (95% CI)")
    ax.legend()
    out = outdir / "fig1_drop_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


def plot_throughput(data: Dict[str, pd.DataFrame], C: float, outdir: Path) -> None:
    fig, ax = plt.subplots()

    all_vals = []
    for df in data.values():
        if df.empty:
            continue
        all_vals += df["Offered (ev/s)"].tolist()
        all_vals += df["Capacity (ev/s)"].tolist()
    max_val = max(all_vals) if all_vals else C * 2

    ax.axhline(y=C, color="red", linestyle=":", linewidth=1.2, label="$C$")

    for mode, df in data.items():
        s = MODE_STYLE[mode]
        agg_off = aggregate(df, "Offered (ev/s)")
        agg_cap = aggregate(df, "Capacity (ev/s)")
        if not agg_off.empty:
            ax.plot(agg_off["NDTs"], agg_off["mean"],
                    color=s["color"], marker=s["marker"],
                    linestyle="-", alpha=0.4, linewidth=1, markersize=4,
                    label=f"Offered ({s['label']})")
        if not agg_cap.empty:
            _plot_ci(ax, agg_cap, s, f"Processed ({s['label']})")

    set_yaxis_C_ticks(ax, C, max_val)
    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Event rate")
    ax.set_title("Throughput vs federation size (95% CI)")
    ax.legend(fontsize=7)
    out = outdir / "fig2_throughput_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


def plot_latency(data: Dict[str, pd.DataFrame], outdir: Path) -> None:
    fig, ax = plt.subplots()
    for mode, df in data.items():
        s = MODE_STYLE[mode]
        agg_mean = aggregate(df, "Proc Lat Mean (ms)")
        agg_p95  = aggregate(df, "Proc Lat P95 (ms)")
        _plot_ci(ax, agg_mean, s, f"Mean ({s['label']})")
        _plot_ci(ax, agg_p95,  {**s, "linestyle": ":"}, f"P95 ({s['label']})")
    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Processing latency (ms)")
    ax.set_title("Processing latency vs federation size (95% CI)")
    ax.legend(fontsize=7)
    out = outdir / "fig3_latency_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


def plot_queue(data: Dict[str, pd.DataFrame], outdir: Path) -> None:
    fig, ax = plt.subplots()
    for mode, df in data.items():
        s = MODE_STYLE[mode]
        agg_mean = aggregate(df, "Queue Mean (ms)")
        agg_p95  = aggregate(df, "Queue P95 (ms)")
        _plot_ci(ax, agg_mean, s, f"Mean ({s['label']})")
        _plot_ci(ax, agg_p95,  {**s, "linestyle": ":"}, f"P95 ({s['label']})")
    ax.set_xlabel("Number of NDTs")
    ax.set_ylabel("Queue delay (ms)")
    ax.set_title("Queue delay vs federation size (95% CI)")
    ax.legend(fontsize=7)
    out = outdir / "fig4_queue_vs_ndts.pdf"
    fig.savefig(out)
    plt.close(fig)
    print(f"  -> {out.name}")


# ---------------------------------------------------------------------------
# Console summary table with CI
# ---------------------------------------------------------------------------
def print_summary(df: pd.DataFrame, mode: str, C: float) -> None:
    cols = ["Drop (%)", "Capacity (ev/s)", "Queue Mean (ms)",
            "Queue P95 (ms)", "Proc Lat Mean (ms)"]
    rows = []
    for ndts, group in df.groupby("NDTs"):
        row = {"NDTs": ndts, "runs": len(group)}
        for col in cols:
            vals = group[col].dropna().values
            if len(vals) == 0:
                row[f"{col} mean"] = float("nan")
                row[f"{col} CI±"]  = float("nan")
            elif len(vals) == 1:
                row[f"{col} mean"] = round(float(vals[0]), 2)
                row[f"{col} CI±"]  = 0.0
            else:
                mean = float(np.mean(vals))
                se   = stats.sem(vals)
                t    = stats.t.ppf((1 + CI) / 2, df=len(vals) - 1)
                row[f"{col} mean"] = round(mean, 2)
                row[f"{col} CI±"]  = round(t * se, 2)
        rows.append(row)

    if not rows:
        return
    result_df = pd.DataFrame(rows)
    print(f"\n[{'='*20} {mode.upper()} {'='*20}]")
    print(tabulate(result_df, headers="keys", tablefmt="psql",
                   showindex=False, floatfmt=".2f"))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    ap = argparse.ArgumentParser(
        description="NDT loss analyser — uniform vs Poisson, multiple runs with 95% CI"
    )
    ap.add_argument("--logs", required=True,
                    help="Root folder containing run1/, run2/... or uniform/poisson/ directly")
    ap.add_argument("--out",  required=True,
                    help="Output folder for PDFs and CSV")
    ap.add_argument("--C",   type=float, default=2000.0,
                    help="DTM capacity C in ev/s (default: 2000)")
    args = ap.parse_args()

    logs_path = Path(args.logs)
    out_path  = Path(args.out)
    out_path.mkdir(parents=True, exist_ok=True)

    data: Dict[str, pd.DataFrame] = {}
    for mode in ("uniform", "poisson"):
        print(f"\nLoading mode: {mode}")
        df = load_all_runs(logs_path, mode, args.C)
        if not df.empty:
            n_runs = df["run"].nunique()
            data[mode] = df
            print(f"  Total: {len(df)} experiments across {n_runs} run(s)")
        else:
            print(f"  No data for {mode} — skipping")

    if not data:
        print("\n[ERROR] No valid data found.")
        return

    # console summary
    for mode, df in data.items():
        print_summary(df, mode, args.C)

    # CSV — raw per-run data
    for mode, df in data.items():
        df.to_csv(out_path / f"metrics_{mode}_runs.csv", index=False)
        print(f"  -> metrics_{mode}_runs.csv")

    # CSV — aggregated mean + CI
    agg_frames = []
    for mode, df in data.items():
        cols = ["Drop (%)", "Capacity (ev/s)", "Offered (ev/s)",
                "Queue Mean (ms)", "Queue P95 (ms)",
                "Proc Lat Mean (ms)", "Proc Lat P95 (ms)"]
        rows = []
        for ndts, group in df.groupby("NDTs"):
            row = {"Mode": mode, "NDTs": ndts, "n_runs": len(group)}
            for col in cols:
                vals = group[col].dropna().values
                if len(vals) == 0:
                    row[f"{col}_mean"] = float("nan")
                    row[f"{col}_ci"]   = float("nan")
                    continue
                mean = float(np.mean(vals))
                ci   = 0.0
                if len(vals) > 1:
                    se = stats.sem(vals)
                    t  = stats.t.ppf((1 + CI) / 2, df=len(vals) - 1)
                    ci = float(t * se)
                row[f"{col}_mean"] = round(mean, 3)
                row[f"{col}_ci"]   = round(ci, 3)
            rows.append(row)
        agg_frames.append(pd.DataFrame(rows))

    pd.concat(agg_frames).to_csv(out_path / "metrics_aggregated.csv", index=False)
    print(f"  -> metrics_aggregated.csv")

    # plots
    print("\nGenerating plots...")
    plot_drop(data, args.C, out_path)
    plot_throughput(data, args.C, out_path)
    plot_latency(data, out_path)
    plot_queue(data, out_path)

    print(f"\nDone. Results in: {out_path.resolve()}")


if __name__ == "__main__":
    main()