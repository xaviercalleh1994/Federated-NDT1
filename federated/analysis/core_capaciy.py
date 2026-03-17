import json
import argparse
from pathlib import Path
import pandas as pd


def load_jsonl(path):
    rows = []
    with open(path) as f:
        for line in f:
            try:
                rows.append(json.loads(line))
            except:
                pass
    return pd.DataFrame(rows)


def analyze_log(path):

    df = load_jsonl(path)

    if df.empty:
        return None

    ing = df[df.tag == "DTM_INGEST"]
    deq = df[df.tag == "DTM_DEQUEUE"]

    if ing.empty:
        return None

    # ventana basada en INGEST
    t0 = ing.ts_ns.min()
    t1 = ing.ts_ns.max()

    window_s = (t1 - t0) / 1e9

    if window_s <= 0:
        return None

    # eventos dentro de la ventana
    ing_window = ing[(ing.ts_ns >= t0) & (ing.ts_ns <= t1)]

    event_ids = set(ing_window.event_id)

    # contar solo DEQUEUE de esos eventos
    deq_window = deq[deq.event_id.isin(event_ids)]

    ingest_rate = len(ing_window) / window_s
    capacity = len(deq_window) / window_s

    drop = (1 - len(deq_window) / len(ing_window)) * 100
    drop = max(0, drop)

    return ingest_rate, capacity, drop


def main():

    ap = argparse.ArgumentParser()
    ap.add_argument("root", help="folder with rate_* experiments")

    args = ap.parse_args()

    root = Path(args.root)

    results = []

    for exp in sorted(root.iterdir()):

        if not exp.name.startswith("rate_"):
            continue

        #rate = float(exp.name.split("_")[1].replace("_", "."))
        rate = float(exp.name.replace("rate_", "").replace("_", "."))

        log = exp / "dtm.log"

        if not log.exists():
            continue

        r = analyze_log(log)

        if r is None:
            continue

        ingest_rate, capacity, drop = r

        results.append({
            "rate": rate,
            "offered_load": ingest_rate,
            "capacity": capacity,
            "drop_pct": drop
        })

    df = pd.DataFrame(results)

    if df.empty:
        print("No experiments found")
        return

    df = df.sort_values("offered_load")

    print("\nResults:")
    print(df.to_string(index=False))

    stable = df[df.drop_pct == 0]

    if stable.empty:
        print("\nNo loss-free region found.")
        return

    best = stable.loc[stable.capacity.idxmax()]

    print("\nMaximum loss-free capacity:")
    print(f"{best.capacity:.2f} events/s")

    print("\nAt offered load:")
    print(f"{best.offered_load:.2f} events/s")


if __name__ == "__main__":
    main()