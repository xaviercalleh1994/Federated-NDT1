#!/usr/bin/env python3
import argparse
import json
import math
import pathlib
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


# -----------------------------
# Loading
# -----------------------------
def iter_log_files(logdir: str) -> List[pathlib.Path]:
    d = pathlib.Path(logdir)
    if not d.exists():
        raise SystemExit(f"logdir does not exist: {logdir}")

    patterns = ["*.log", "*.logs", "*.log.*", "*.jsonl", "*.txt"]
    files = []
    for pat in patterns:
        files.extend(d.glob(pat))
    # unique + stable order
    files = sorted(set(files))
    return files


def load_dir(logdir: str) -> pd.DataFrame:
    rows = []
    files = iter_log_files(logdir)
    if not files:
        raise SystemExit(f"No log-like files found in: {logdir}")

    for p in files:
        try:
            with p.open("r", encoding="utf-8", errors="ignore") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        rows.append(json.loads(line))
                    except Exception:
                        # ignore non-json lines
                        pass
        except Exception as e:
            print(f"[WARN] Could not read {p}: {e}")

    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("No valid JSON lines found in provided directory.")

    # Normalize expected columns (if missing, create)
    for col in ["tag", "event_id", "proto", "relay_kind", "side"]:
        if col not in df.columns:
            df[col] = np.nan

    return df


# -----------------------------
# Metrics computation
# -----------------------------
def _to_num(s: pd.Series) -> pd.Series:
    return pd.to_numeric(s, errors="coerce")


def correlate_events(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build per-event table by event_id:
      - create time: prefer ON_EVENT_SENT.t_create_ns (your ON code logs it there),
        fallback to ON_EVENT_CREATED.ts_ns if needed.
      - tx time: ON_EVENT_SENT.t_tx_ns
      - rx time: NDT_EVENT_RX.t_rx_ns
      - apply time: NDT_EVENT_APPLIED.t_apply_ns
    """
    on_sent = df[df["tag"] == "ON_EVENT_SENT"].copy()
    ndt_rx = df[df["tag"] == "NDT_EVENT_RX"].copy()
    ndt_ap = df[df["tag"] == "NDT_EVENT_APPLIED"].copy()
    on_created = df[df["tag"] == "ON_EVENT_CREATED"].copy()

    # numeric
    for tcol in ["t_create_ns", "t_tx_ns", "t_rx_ns", "t_apply_ns", "ts_ns", "size_bytes"]:
        if tcol in on_sent.columns:
            on_sent[tcol] = _to_num(on_sent[tcol])
        if tcol in ndt_rx.columns:
            ndt_rx[tcol] = _to_num(ndt_rx[tcol])
        if tcol in ndt_ap.columns:
            ndt_ap[tcol] = _to_num(ndt_ap[tcol])
        if tcol in on_created.columns:
            on_created[tcol] = _to_num(on_created[tcol])

    # base table = ON_EVENT_SENT (because it definitely has t_tx_ns and usually t_create_ns)
    base = on_sent[["event_id", "proto", "relay_kind", "t_create_ns", "t_tx_ns", "size_bytes"]].copy()

    # fallback create time from ON_EVENT_CREATED.ts_ns if t_create_ns missing
    if not on_created.empty and "ts_ns" in on_created.columns:
        c_fallback = on_created[["event_id", "ts_ns"]].rename(columns={"ts_ns": "t_create_fallback_ns"})
        base = base.merge(c_fallback, on="event_id", how="left")
        base["t_create_ns"] = base["t_create_ns"].fillna(base["t_create_fallback_ns"])

    # merge rx/apply
    rx_tbl = ndt_rx[["event_id", "t_rx_ns"]].copy()
    ap_tbl = ndt_ap[["event_id", "t_apply_ns"]].copy()

    m = base.merge(rx_tbl, on="event_id", how="inner").merge(ap_tbl, on="event_id", how="inner")

    # compute latencies (ms)
    m["t_create_ns"] = _to_num(m["t_create_ns"])
    m["t_tx_ns"] = _to_num(m["t_tx_ns"])
    m["t_rx_ns"] = _to_num(m["t_rx_ns"])
    m["t_apply_ns"] = _to_num(m["t_apply_ns"])
    m["size_bytes"] = _to_num(m["size_bytes"])

    m["lat_net_ms"] = (m["t_rx_ns"] - m["t_tx_ns"]) / 1e6
    m["lat_apply_ms"] = (m["t_apply_ns"] - m["t_rx_ns"]) / 1e6
    m["lat_e2e_ms"] = (m["t_apply_ns"] - m["t_create_ns"]) / 1e6

    # clean impossible values
    for col in ["lat_net_ms", "lat_apply_ms", "lat_e2e_ms"]:
        m.loc[~np.isfinite(m[col]), col] = np.nan
        # drop negative outliers (clock issues / missing fields)
        m.loc[m[col] < 0, col] = np.nan

    # keep only rows with e2e available for E2E plots; keep others for partial plots too
    return m


def bootstrap_ci_mean(x: np.ndarray, n_boot: int = 2000, ci: float = 0.95, seed: int = 1) -> Tuple[float, float, float]:
    """
    Returns (mean, lo, hi) for the mean using bootstrap percentile CI.
    """
    x = x[np.isfinite(x)]
    if len(x) == 0:
        return (float("nan"), float("nan"), float("nan"))
    if len(x) == 1:
        v = float(x[0])
        return (v, v, v)

    rng = np.random.default_rng(seed)
    means = []
    n = len(x)
    for _ in range(n_boot):
        sample = x[rng.integers(0, n, size=n)]
        means.append(float(np.mean(sample)))
    means = np.array(means)

    alpha = (1.0 - ci) / 2.0
    lo = float(np.quantile(means, alpha))
    hi = float(np.quantile(means, 1.0 - alpha))
    return (float(np.mean(x)), lo, hi)


def summarize_by_group(m: pd.DataFrame) -> pd.DataFrame:
    groups = []
    for (proto, relay_kind), g in m.groupby(["proto", "relay_kind"], dropna=False):
        proto_s = str(proto)
        relay_s = str(relay_kind)
        label = f"{proto_s.upper()}-{relay_s}"

        # arrays
        e2e = g["lat_e2e_ms"].to_numpy()
        net = g["lat_net_ms"].to_numpy()
        app = g["lat_apply_ms"].to_numpy()
        sz = g["size_bytes"].to_numpy()

        e2e_mean, e2e_lo, e2e_hi = bootstrap_ci_mean(e2e)
        net_mean, net_lo, net_hi = bootstrap_ci_mean(net)
        app_mean, app_lo, app_hi = bootstrap_ci_mean(app)
        sz_mean, sz_lo, sz_hi = bootstrap_ci_mean(sz)

        def p50(a): 
            a = a[np.isfinite(a)]
            return float(np.percentile(a, 50)) if len(a) else float("nan")

        def p95(a):
            a = a[np.isfinite(a)]
            return float(np.percentile(a, 95)) if len(a) else float("nan")

        groups.append({
            "proto": proto_s,
            "relay_kind": relay_s,
            "label": label,
            "events_total": int(len(g)),
            "events_e2e": int(np.isfinite(e2e).sum()),
            "events_net": int(np.isfinite(net).sum()),
            "events_apply": int(np.isfinite(app).sum()),
            "events_size": int(np.isfinite(sz).sum()),
            "lat_e2e_mean": e2e_mean,
            "lat_e2e_ci_lo": e2e_lo,
            "lat_e2e_ci_hi": e2e_hi,
            "lat_e2e_p50": p50(e2e),
            "lat_e2e_p95": p95(e2e),
            "lat_net_mean": net_mean,
            "lat_net_ci_lo": net_lo,
            "lat_net_ci_hi": net_hi,
            "lat_apply_mean": app_mean,
            "lat_apply_ci_lo": app_lo,
            "lat_apply_ci_hi": app_hi,
            "size_bytes_mean": sz_mean,
            "size_bytes_ci_lo": sz_lo,
            "size_bytes_ci_hi": sz_hi,
        })

    out = pd.DataFrame(groups)
    # order: TCP then UDP by default, and stable
    order = {"tcp": 0, "udp": 1}
    out["proto_order"] = out["proto"].map(lambda x: order.get(str(x).lower(), 99))
    out = out.sort_values(["relay_kind", "proto_order", "proto"]).drop(columns=["proto_order"])
    return out


# -----------------------------
# Plotting helpers
# -----------------------------
def point_ci_plot(df: pd.DataFrame, mean_col: str, lo_col: str, hi_col: str,
                  title: str, ylabel: str, outpath: pathlib.Path):
    labels = df["label"].tolist()
    means = df[mean_col].to_numpy(dtype=float)
    lo = df[lo_col].to_numpy(dtype=float)
    hi = df[hi_col].to_numpy(dtype=float)

    x = np.arange(len(labels))
    yerr = np.vstack([means - lo, hi - means])
    # If any NaN, matplotlib can choke on yerr shapes; mask them
    mask = np.isfinite(means)
    plt.figure(figsize=(9, 4.5))
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.title(title)
    plt.ylabel(ylabel)
    plt.xticks(x, labels)

    if mask.any():
        plt.errorbar(x[mask], means[mask], yerr=yerr[:, mask], fmt="o", capsize=6)
        # make y-limits tight and sane
        ymin = np.nanmin(lo[mask])
        ymax = np.nanmax(hi[mask])
        if np.isfinite(ymin) and np.isfinite(ymax):
            pad = 0.08 * (ymax - ymin) if ymax > ymin else 1.0
            plt.ylim(max(0, ymin - pad), ymax + pad)
    else:
        plt.text(0.5, 0.5, "No data to plot (missing timestamps in logs)", ha="center", va="center")
        plt.ylim(0, 1)

    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def decomposition_plot(df: pd.DataFrame, outpath: pathlib.Path):
    """
    Mean decomposition: E2E ≈ Net + Apply (+ other local overhead not measured).
    Here we plot Net mean + Apply mean stacked. Not a “truth proof”, but a useful split.
    """
    labels = df["label"].tolist()
    net = df["lat_net_mean"].to_numpy(dtype=float)
    app = df["lat_apply_mean"].to_numpy(dtype=float)

    x = np.arange(len(labels))
    plt.figure(figsize=(9, 4.5))
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.title("Latency decomposition (mean)")
    plt.ylabel("Latency (ms)")
    plt.xticks(x, labels)

    net2 = np.nan_to_num(net, nan=0.0)
    app2 = np.nan_to_num(app, nan=0.0)

    plt.bar(x, net2, label="Network / relay")
    plt.bar(x, app2, bottom=net2, label="NDT apply")

    ymax = float(np.nanmax(net2 + app2)) if len(net2) else 1.0
    plt.ylim(0, ymax * 1.15 if ymax > 0 else 1.0)

    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def cdf_plot(m: pd.DataFrame, outpath: pathlib.Path):
    plt.figure(figsize=(9, 4.5))
    plt.grid(True, linestyle="--", alpha=0.4)
    plt.title("End-to-end latency CDF")
    plt.xlabel("End-to-end latency (ms)")
    plt.ylabel("CDF")

    plotted = False
    for (proto, relay_kind), g in m.groupby(["proto", "relay_kind"], dropna=False):
        label = f"{str(proto).upper()}-{str(relay_kind)}"
        x = g["lat_e2e_ms"].to_numpy(dtype=float)
        x = x[np.isfinite(x)]
        if len(x) < 2:
            continue
        x = np.sort(x)
        y = np.arange(1, len(x) + 1) / len(x)
        plt.plot(x, y, label=label)
        plotted = True

    if plotted:
        # tight x limits
        allx = m["lat_e2e_ms"].to_numpy(dtype=float)
        allx = allx[np.isfinite(allx)]
        if len(allx):
            xmin, xmax = float(np.min(allx)), float(np.max(allx))
            pad = 0.05 * (xmax - xmin) if xmax > xmin else 1.0
            plt.xlim(max(0, xmin - pad), xmax + pad)
        plt.ylim(0, 1.0)
        plt.legend()
    else:
        plt.text(0.5, 0.5, "Not enough E2E samples for CDF", ha="center", va="center")

    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


def p95_plot(df: pd.DataFrame, outpath: pathlib.Path):
    labels = df["label"].tolist()
    p95 = df["lat_e2e_p95"].to_numpy(dtype=float)

    x = np.arange(len(labels))
    plt.figure(figsize=(9, 4.5))
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.title("End-to-end latency (p95)")
    plt.ylabel("Latency (ms)")
    plt.xticks(x, labels)

    mask = np.isfinite(p95)
    if mask.any():
        plt.bar(x[mask], p95[mask])
        ymax = float(np.nanmax(p95[mask]))
        plt.ylim(0, ymax * 1.15 if ymax > 0 else 1.0)
    else:
        plt.text(0.5, 0.5, "No valid p95 (missing E2E samples)", ha="center", va="center")
        plt.ylim(0, 1)

    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()


# -----------------------------
# Main
# -----------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logdir", required=True, help="Directory containing ON/NDT logs (JSONL lines)")
    ap.add_argument("--outdir", default="out_sync", help="Output directory")
    args = ap.parse_args()

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = load_dir(args.logdir)
    m = correlate_events(df)
    g = summarize_by_group(m)

    # Save tables
    m.to_csv(outdir / "events_correlated.csv", index=False)
    g.to_csv(outdir / "metrics_grouped.csv", index=False)

    # Plots (points + CI95)
    point_ci_plot(
        g,
        mean_col="lat_e2e_mean", lo_col="lat_e2e_ci_lo", hi_col="lat_e2e_ci_hi",
        title="End-to-end latency (mean ± CI95)",
        ylabel="Latency (ms)",
        outpath=outdir / "fig_e2e_mean_ci95.png",
    )
    p95_plot(g, outdir / "fig_e2e_p95.png")
    cdf_plot(m, outdir / "fig_e2e_cdf.png")

    point_ci_plot(
        g,
        mean_col="lat_net_mean", lo_col="lat_net_ci_lo", hi_col="lat_net_ci_hi",
        title="Network latency (mean ± CI95)",
        ylabel="Latency (ms)",
        outpath=outdir / "fig_net_mean_ci95.png",
    )
    point_ci_plot(
        g,
        mean_col="lat_apply_mean", lo_col="lat_apply_ci_lo", hi_col="lat_apply_ci_hi",
        title="NDT apply latency (mean ± CI95)",
        ylabel="Latency (ms)",
        outpath=outdir / "fig_ndt_apply_mean_ci95.png",
    )
    point_ci_plot(
        g,
        mean_col="size_bytes_mean", lo_col="size_bytes_ci_lo", hi_col="size_bytes_ci_hi",
        title="Sync message size (mean ± CI95)",
        ylabel="Bytes",
        outpath=outdir / "fig_size_mean_ci95.png",
    )

    decomposition_plot(g, outdir / "fig_latency_decomposition_mean.png")

    print("Wrote:")
    print(" -", outdir / "events_correlated.csv")
    print(" -", outdir / "metrics_grouped.csv")
    print(" - figures in", outdir)


if __name__ == "__main__":
    main()
