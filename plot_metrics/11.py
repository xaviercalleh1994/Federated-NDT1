#!/usr/bin/env python3
import argparse
import json
import pathlib
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

def load_dir(logdir: str) -> pd.DataFrame:
    rows = []
    for p in pathlib.Path(logdir).glob("*.log"):
        with open(p) as f:
            for line in f:
                try:
                    rows.append(json.loads(line))
                except Exception:
                    pass
    df = pd.DataFrame(rows)
    if df.empty:
        raise SystemExit("No valid JSON lines found in *.log")
    return df

def correlate(df: pd.DataFrame) -> pd.DataFrame:
    on_created = df[df["tag"] == "ON_EVENT_CREATED"][["event_id","t_create_ns","proto","relay_kind"]]
    on_sent = df[df["tag"] == "ON_EVENT_SENT"][["event_id","t_tx_ns","size_bytes","proto","relay_kind"]]
    ndt_rx = df[df["tag"] == "NDT_EVENT_RX"][["event_id","t_rx_ns","proto","relay_kind"]]
    ndt_ap = df[df["tag"] == "NDT_EVENT_APPLIED"][["event_id","t_apply_ns","proto","relay_kind"]]

    # Merge by event_id. Prefer ON columns for grouping (proto, relay_kind).
    m = on_created.merge(on_sent, on="event_id", suffixes=("_c","_s"))
    m = m.merge(ndt_rx[["event_id","t_rx_ns"]], on="event_id")
    m = m.merge(ndt_ap[["event_id","t_apply_ns"]], on="event_id")

    m["lat_e2e_ms"] = (m["t_apply_ns"] - m["t_create_ns"]) / 1e6
    m["lat_net_ms"] = (m["t_rx_ns"] - m["t_tx_ns"]) / 1e6
    m["lat_apply_ms"] = (m["t_apply_ns"] - m["t_rx_ns"]) / 1e6
    return m

def agg(m: pd.DataFrame) -> pd.DataFrame:
    def p95(x): return float(np.percentile(x, 95))
    def p50(x): return float(np.percentile(x, 50))

    g = (m.groupby(["proto","relay_kind"])
           .agg(
               events=("event_id","count"),
               lat_e2e_mean=("lat_e2e_ms","mean"),
               lat_e2e_p50=("lat_e2e_ms", p50),
               lat_e2e_p95=("lat_e2e_ms", p95),
               lat_net_mean=("lat_net_ms","mean"),
               lat_apply_mean=("lat_apply_ms","mean"),
               size_bytes_mean=("size_bytes","mean")
           )
           .reset_index())
    return g

def barplot(df, ycol, title, outpath):
    labels = [f"{r.proto.upper()}-{r.relay_kind}" for _, r in df.iterrows()]
    vals = df[ycol].tolist()
    plt.figure(figsize=(8,4))
    plt.bar(labels, vals)
    plt.ylabel(ycol.replace("_"," "))
    plt.title(title)
    plt.grid(axis="y", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()

def stacked_breakdown(df, outpath):
    labels = [f"{r.proto.upper()}-{r.relay_kind}" for _, r in df.iterrows()]
    net = df["lat_net_mean"].to_numpy()
    app = df["lat_apply_mean"].to_numpy()
    plt.figure(figsize=(8,4))
    plt.bar(labels, net, label="Network/relay")
    plt.bar(labels, app, bottom=net, label="NDT apply")
    plt.ylabel("ms")
    plt.title("Latency decomposition (mean)")
    plt.legend()
    plt.tight_layout()
    plt.savefig(outpath, dpi=200)
    plt.close()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logdir", required=True, help="Directory with *.log (onm + ndtm)")
    ap.add_argument("--outdir", default="out_sync", help="Output directory")
    args = ap.parse_args()

    outdir = pathlib.Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    df = load_dir(args.logdir)
    m = correlate(df)
    g = agg(m)

    g.to_csv(outdir / "metrics_sync.csv", index=False)

    barplot(g, "lat_e2e_mean", "End-to-end latency (mean)", outdir / "fig_latency_mean.png")
    barplot(g, "lat_e2e_p95", "End-to-end latency (p95)", outdir / "fig_latency_p95.png")
    stacked_breakdown(g, outdir / "fig_latency_breakdown_mean.png")

    print("Wrote:", outdir / "metrics_sync.csv")
    print("Wrote figures in:", outdir)

if __name__ == "__main__":
    main()
