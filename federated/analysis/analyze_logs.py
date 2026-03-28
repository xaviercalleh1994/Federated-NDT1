import os
import argparse
import re
from pathlib import Path

# Fallback robusto
try:
    import orjson


    def parse_line(line):
        return orjson.loads(line)


    open_mode = "rb"
except ImportError:
    import json


    def parse_line(line):
        return json.loads(line)


    open_mode = "r"

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from tabulate import tabulate

# ============================================================
# IEEE Communications Magazine Plot Style
# ============================================================
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
    "savefig.bbox": "tight"
})


def load_jsonl(path: Path) -> pd.DataFrame:
    print(f"⏳ Leyendo {path.name}... ", end="", flush=True)
    rows = []
    with open(path, open_mode) as f:
        for line in f:
            try:
                rows.append(parse_line(line))
            except Exception:
                pass
    print(f"¡Listo! ({len(rows)} eventos)")
    if not rows: return pd.DataFrame()
    df = pd.DataFrame(rows)
    if "ts_ns" in df.columns:
        df["ts_ns"] = pd.to_numeric(df["ts_ns"], errors="coerce")
        df = df.dropna(subset=["ts_ns"]).sort_values("ts_ns")
    return df


def compute_downstream(df: pd.DataFrame, exp_name: str):
    """Métricas para el Experimento 1 (ONM -> NDTs) usando el Fast-Path"""
    if "tag" not in df.columns: return None

    # En el Fast-Path, los eventos entran como NDT_EVENT_RX y salen como NDT_EVENT_DISPATCHED
    rx = df[df.tag == "NDT_EVENT_RX"]
    disp = df[df.tag == "NDT_EVENT_DISPATCHED"]

    if rx.empty or disp.empty: return None

    # Dispatch Efficiency
    sent = pd.to_numeric(disp.get("sent_ok", pd.Series([0]))).sum()
    fanout = pd.to_numeric(disp.get("fanout", pd.Series([1]))).sum()
    eff = (sent / fanout * 100.0) if fanout > 0 else 0.0

    # Orchestration Latency
    merged = rx.merge(disp, on="event_id", suffixes=("_rx", "_disp"))
    if merged.empty: return None

    # EL FIX: Usamos los nombres con los sufijos _disp y _rx generados por Pandas
    latencies = (pd.to_numeric(merged["t_dispatch_end_ns_disp"]) - pd.to_numeric(merged["t_rx_ns_rx"])) / 1e6

    match = re.search(r'\d+', exp_name.replace("1to", ""))
    fanout_val = int(match.group()) if match else 1

    return {
        "Experiment": exp_name,
        "Fanout": fanout_val,
        "Dispatch Eff (%)": round(eff, 2),
        "Orchestration Lat Mean (ms)": round(latencies.mean(), 3),
        "Orchestration Lat P95 (ms)": round(latencies.quantile(0.95), 3)
    }


def compute_upstream(df: pd.DataFrame, exp_name: str):
    """Métricas para el Experimento 2 (Telemetría NDTs -> DTM) con Traza Única (Mochila)"""
    if "tag" not in df.columns: return None

    src_col = df.get("src", pd.Series([None] * len(df)))

    # Extraemos directamente los eventos procesados (la mochila) y los descartados
    done = df[(df.tag == "DTM_EVENT_DONE") & (src_col == "feedback")]
    drops = df[df.tag == "DTM_DROP"]

    total_ingested = len(done) + len(drops)
    if total_ingested == 0: return None

    # 1. Cronómetro de Red (Offered Load): t_rx_ns para exitosos, ts_ns para drops
    t_rx_series = pd.concat([
        pd.to_numeric(done.get("t_rx_ns", pd.Series(dtype=float))),
        pd.to_numeric(drops.get("ts_ns", pd.Series(dtype=float)))
    ]).dropna()

    if t_rx_series.empty: return None

    t0_rx, t1_rx = t_rx_series.min(), t_rx_series.max()
    window_rx_s = (t1_rx - t0_rx) / 1e9

    offered_load = total_ingested / window_rx_s if window_rx_s > 0 else 0.0

    # 2. Cronómetro de CPU (Capacity)
    if not done.empty:
        t0_proc, t1_proc = pd.to_numeric(done["ts_ns"]).min(), pd.to_numeric(done["ts_ns"]).max()
        window_proc_s = (t1_proc - t0_proc) / 1e9
        if window_proc_s <= 0: window_proc_s = window_rx_s
        processing_capacity = len(done) / window_proc_s if window_proc_s > 0 else 0.0
    else:
        processing_capacity = 0.0

    processing_eff = (len(done) / total_ingested * 100.0)
    drop_pct = (len(drops) / total_ingested * 100.0)

    # Queue Delay: Extraído de la mochila sin merge
    q_delays = pd.to_numeric(done.get("queue_ms", pd.Series([np.nan]))).dropna()
    q_mean = q_delays.mean() if not q_delays.empty else np.nan
    q_p95 = q_delays.quantile(0.95) if not q_delays.empty else np.nan

    # Processing Latency Total: t_done_ns - t_rx_ns dentro del mismo evento DONE
    if not done.empty and "t_rx_ns" in done.columns and "t_done_ns" in done.columns:
        proc_lats = (pd.to_numeric(done["t_done_ns"]) - pd.to_numeric(done["t_rx_ns"])) / 1e6
        proc_lat_mean = proc_lats.mean() if not proc_lats.empty else np.nan
        proc_lat_p95 = proc_lats.quantile(0.95) if not proc_lats.empty else np.nan
    else:
        proc_lat_mean, proc_lat_p95 = np.nan, np.nan

    # NUEVO: Análisis de Drops por Prioridad (Crucial para el Paper)
    # Suponiendo que las prioridades son 10, 20 (Críticas) y 50, 100 (Fondo)
    drops_prio_baja = 0
    drops_prio_alta = 0
    if not drops.empty and "priority" in drops.columns:
        prio_series = pd.to_numeric(drops["priority"], errors="coerce").fillna(100)
        # Consideramos <= 20 como Crítico (Alta), > 20 como Fondo (Baja)
        drops_prio_alta = len(prio_series[prio_series <= 20])
        drops_prio_baja = len(prio_series[prio_series > 20])

    return {
        "Experiment": exp_name,
        "Offered Load (ev/s)": round(offered_load, 2),
        "Capacity (ev/s)": round(processing_capacity, 2),
        "Processing Eff (%)": round(processing_eff, 2),
        "Drop (%)": round(drop_pct, 2),
        "Queue Mean (ms)": round(q_mean, 3),
        "Queue P95 (ms)": round(q_p95, 3),
        "Proc Lat Mean (ms)": round(proc_lat_mean, 3),
        "Proc Lat P95 (ms)": round(proc_lat_p95, 3),
        "Drops (Critical <=20)": drops_prio_alta,  # NUEVA COLUMNA
        "Drops (Background >20)": drops_prio_baja  # NUEVA COLUMNA
    }


def plot_downstream(df: pd.DataFrame, outdir: Path):
    if df.empty: return
    df = df.sort_values("Fanout")

    plt.figure()
    plt.plot(df["Fanout"], df["Orchestration Lat Mean (ms)"], marker="o", label="Mean Latency")
    plt.plot(df["Fanout"], df["Orchestration Lat P95 (ms)"], marker="s", linestyle="--", label="P95 Latency")
    plt.xscale("log", base=2)
    plt.xticks(df["Fanout"], df["Fanout"])
    plt.xlabel("Federation Size (Number of NDTs)")
    plt.ylabel("Orchestration Latency (ms)")
    plt.title("Exp 1: DTM State Synchronization Delay")
    plt.legend()
    plt.savefig(outdir / "fig1_downstream_latency.pdf")
    plt.close()


def plot_upstream(df: pd.DataFrame, outdir: Path):
    if df.empty: return
    df = df.sort_values("Offered Load (ev/s)")

    # 1. Capacity vs Load
    plt.figure()
    plt.plot(df["Offered Load (ev/s)"], df["Capacity (ev/s)"], marker="o", label="Processed")
    max_val = max(df["Offered Load (ev/s)"].max(), df["Capacity (ev/s)"].max())
    plt.plot([0, max_val], [0, max_val], "k--", alpha=0.5, label="Ideal")
    plt.xlabel("Offered Telemetry Load (Events/s)")
    plt.ylabel("DTM Processing Capacity (Events/s)")
    plt.title("Exp 2: Upstream Throughput")
    plt.legend()
    plt.savefig(outdir / "fig2_upstream_throughput.pdf")
    plt.close()

    # 2. Latency vs Load
    plt.figure()
    plt.plot(df["Offered Load (ev/s)"], df["Proc Lat Mean (ms)"], marker="o", label="Mean Latency")
    plt.plot(df["Offered Load (ev/s)"], df["Proc Lat P95 (ms)"], marker="s", linestyle="--", label="P95 Latency")
    plt.xlabel("Offered Telemetry Load (Events/s)")
    plt.ylabel("Processing Latency (ms)")
    plt.title("Exp 2: DTM Telemetry Processing Delay")
    plt.legend()
    plt.savefig(outdir / "fig3_upstream_latency.pdf")
    plt.close()

    # 3. Drops
    plt.figure()
    plt.plot(df["Offered Load (ev/s)"], df["Drop (%)"], marker="^", color="red")
    plt.xlabel("Offered Telemetry Load (Events/s)")
    plt.ylabel("Event Drop Ratio (%)")
    plt.title("Exp 2: DTM Resilience (Drop Policy)")
    plt.savefig(outdir / "fig4_upstream_drops.pdf")
    plt.close()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--logs", required=True, help="Carpeta con los logs")
    ap.add_argument("--out", required=True, help="Carpeta de salida para PDFs")
    args = ap.parse_args()

    input_path = Path(args.logs)
    outdir = Path(args.out)
    outdir.mkdir(parents=True, exist_ok=True)

    res_down, res_up = [], []

    # Leemos la estructura recursivamente por si hay subcarpetas
    log_files = list(input_path.rglob("dtm.log")) + list(input_path.rglob("ndtm_events.log"))

    for log in log_files:
        exp_name = log.parent.name
        df = load_jsonl(log)

        if "1to" in exp_name:
            metrics = compute_downstream(df, exp_name)
            if metrics: res_down.append(metrics)
        elif "rate" in exp_name:
            metrics = compute_upstream(df, exp_name)
            if metrics: res_up.append(metrics)

    if res_down:
        df_down = pd.DataFrame(res_down)
        print(f"\n[{'=' * 20} EXPERIMENTO 1: DOWNSTREAM (SYNC) {'=' * 20}]")
        print(tabulate(df_down, headers="keys", tablefmt="psql", showindex=False))
        plot_downstream(df_down, outdir)
        df_down.to_csv(outdir / "metrics_exp1_downstream.csv", index=False)

    if res_up:
        df_up = pd.DataFrame(res_up)
        print(f"\n[{'=' * 20} EXPERIMENTO 2: UPSTREAM (TELEMETRY) {'=' * 20}]")
        print(tabulate(df_up, headers="keys", tablefmt="psql", showindex=False))
        plot_upstream(df_up, outdir)
        df_up.to_csv(outdir / "metrics_exp2_upstream.csv", index=False)

    print(f"\n✅ Análisis completado. Resultados en: {outdir.absolute()}")


if __name__ == "__main__":
    main()