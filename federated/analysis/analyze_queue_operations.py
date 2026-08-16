# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
"""
analyze_queue_operations.py — Operation-count / complexity scalability
------------------------------------------------------------------------
Reconstructs AdmissionQueue occupancy and operation throughput PURELY
from dtm_upstream.log timestamps (t_rx_ns already logged on every
DTM_UPSTREAM_RX / DTM_UPSTREAM_DONE / DTM_DROP event). No changes to
dtm.py, no hardware/CPU sampling — this is event-sourcing over the
existing log stream, so it can run against a live pod (tailed) or a
completed log file with the exact same code path.

Why this instead of CPU/memory sampling:
  - Hardware-independent: op counts and queue-depth reconstruction
    don't change if you run this on a laptop or a beefy server.
  - Non-invasive: reads a log stream that already exists; dtm.py
    stays exactly as validated.
  - Directly maps to the theoretical complexity claim (AdmissionQueue
    push/pop is O(log q)): by reconstructing the queue depth q at the
    moment of every push, we get an empirical (depth, latency) series
    that can be checked against a log-shaped trend, instead of having
    to trust the Big-O claim on paper alone.

Two modes:
  --stream   read newline-delimited JSON from stdin (for live use,
             piped from `kubectl exec -n ndt pod-dtm -- tail -f
             -n +1 /tmp/dtm_upstream.log`)
  --log PATH read a completed log file (post-hoc, e.g. after
             run_experiment2.sh has collected logs to the host)

Usage (live, run in a second terminal WHILE run_experiment2.sh runs):
  kubectl exec -n ndt pod-dtm -- tail -f -n +1 /tmp/dtm_upstream.log \\
    | python3 analyze_queue_operations.py --stream \\
        --out /tmp/exp2_starvation_long/live_ops

Usage (post-hoc, on a completed condition's log):
  python3 analyze_queue_operations.py \\
    --log /tmp/exp2_full/run1/rate150_queue4_priority/dtm_upstream.log \\
    --out /tmp/exp2_full/run1/rate150_queue4_priority/ops_analysis

Outputs:
  - queue_depth_timeline.csv   : t_s, event, ndt_id, priority, depth_after
  - depth_vs_wait.csv          : observed depth at admission vs the
                                  queue_ms that command eventually
                                  incurred (empirical complexity curve)
  - ops_summary.txt            : operation counts, ops/sec, per-NDT
                                  breakdown, printed live every
                                  --report-every seconds in --stream
                                  mode and written once at EOF
"""
import sys
import json
import argparse
import time
from pathlib import Path
from collections import defaultdict
from typing import Optional


NDT_PRIORITIES = {0: 10, 1: 30, 2: 50, 3: 70}


class QueueReconstructor:
    """
    Event-sourced reconstruction of AdmissionQueue occupancy.
    Consumes DTM_UPSTREAM_RX (arrival — push), DTM_UPSTREAM_DONE
    (departure after successful processing — pop+serve), and
    DTM_DROP (arrival that was immediately rejected — never
    entered the heap) events, in the order they appear in the log
    (which is already time-ordered since it's a single-threaded
    writer draining one deque).
    """

    def __init__(self) -> None:
        self.depth = 0
        self.rx_count      = 0   # raw arrivals, BEFORE admission decision
        self.admitted_count = 0  # rx_count - drop_count (settled once EOF/log ends)
        self.pop_count  = 0
        self.drop_count = 0
        self.per_ndt_rx    = defaultdict(int)
        self.per_ndt_drop  = defaultdict(int)
        self.per_ndt_pop   = defaultdict(int)
        # event_id -> (t_rx_ns, ndt_id, priority, depth_at_rx) for commands
        # seen at RX whose admit/drop outcome isn't resolved yet
        self._pending: dict = {}
        self.t0_ns: Optional[int] = None
        self.timeline: list = []      # (t_s, event, ndt_id, priority, depth)
        self.depth_vs_wait: list = [] # (depth_at_admission, queue_ms)

    def _t_s(self, t_ns: int) -> float:
        if self.t0_ns is None:
            self.t0_ns = t_ns
        return (t_ns - self.t0_ns) / 1e9

    # ── FIX (2026-07-25) ────────────────────────────────────────────
    # DTM_UPSTREAM_RX is logged in dtm.py BEFORE the AdmissionQueue
    # decides to admit or reject the command — so treating every RX
    # as a real queue push (as the previous version did) let depth
    # grow without bound for a queue that is actually capped at
    # --max-queue (a reconstructed depth of 44994 against
    # --max-queue 4 was the giveaway that this was wrong). On top of
    # that, DTM_DROP does not carry ndt_id in dtm.py's log_event call
    # (only `priority`), so per-NDT drop attribution silently
    # collapsed to zero everywhere even though the total was correct.
    #
    # Fix: treat RX as an "optimistic" push (increment depth, stash
    # the record — including its ndt_id — in self._pending keyed by
    # event_id, which DTM_DROP *does* carry). If a DTM_DROP arrives
    # for that event_id, undo the optimistic push (decrement depth
    # back down, don't count it as admitted) and attribute the drop
    # using the ndt_id stashed at RX time, not the (absent) one on
    # the DROP record itself. If DTM_UPSTREAM_DONE arrives instead,
    # the item really was admitted — resolve normally. Net effect:
    # correct depth bounded by --max-queue, and correct per-NDT drop
    # counts, in both --log (file) and --stream (live) modes.
    # ─────────────────────────────────────────────────────────────
    def handle(self, rec: dict) -> None:
        tag = rec.get("tag", "")
        ndt_id = rec.get("ndt_id")
        prio   = NDT_PRIORITIES.get(ndt_id, rec.get("priority"))

        if tag == "DTM_UPSTREAM_RX":
            t_ns = rec.get("t_rx_ns", 0)
            if not t_ns:
                return
            event_id = rec.get("event_id")
            self.depth += 1
            self.rx_count += 1
            self.per_ndt_rx[ndt_id] += 1
            self._pending[event_id] = (t_ns, ndt_id, prio, self.depth)
            self.timeline.append(
                (self._t_s(t_ns), "push", ndt_id, prio, self.depth))

        elif tag == "DTM_DROP":
            event_id = rec.get("event_id")
            self.drop_count += 1
            entry = self._pending.pop(event_id, None)
            if entry is not None:
                t_rx_ns, entry_ndt_id, entry_prio, _ = entry
                self.per_ndt_drop[entry_ndt_id] += 1
                if self.depth > 0:
                    self.depth -= 1  # undo the optimistic push — never really admitted
                self.timeline.append(
                    (self._t_s(t_rx_ns), "drop_undo",
                     entry_ndt_id, entry_prio, self.depth))
            # if entry is missing (shouldn't happen given the DTM's
            # single-threaded, time-ordered log writer), we still
            # count the drop in the total but can't attribute it —
            # depth is left untouched rather than risk going negative

        elif tag == "DTM_UPSTREAM_DONE":
            event_id = rec.get("event_id")
            t_ns = rec.get("t_rx_ns", 0) or rec.get("t_done_ns", 0)
            queue_ms = rec.get("queue_ms")
            entry = self._pending.pop(event_id, None)
            self.pop_count += 1
            self.per_ndt_pop[ndt_id] += 1
            if self.depth > 0:
                self.depth -= 1
            if entry is not None and queue_ms is not None:
                _, _, _, depth_at_admission = entry
                self.depth_vs_wait.append((depth_at_admission, queue_ms))
            if t_ns:
                self.timeline.append(
                    (self._t_s(t_ns), "pop", ndt_id, prio, self.depth))

    def summary_text(self, elapsed_s: float) -> str:
        self.admitted_count = self.rx_count - self.drop_count
        lines = []
        lines.append(f"elapsed_s={elapsed_s:.1f}")
        lines.append(f"rx_ops={self.rx_count} "
                      f"admitted_ops={self.admitted_count} "
                      f"pop_ops={self.pop_count} "
                      f"drop_ops={self.drop_count} "
                      f"current_depth={self.depth} "
                      f"(bounded by --max-queue — flag if this exceeds it)")
        if elapsed_s > 0:
            total_ops = self.rx_count + self.pop_count + self.drop_count
            lines.append(f"ops_per_sec={total_ops/elapsed_s:.1f}")
        lines.append("per-NDT: rx (arrivals) / pop (completed) / drop (rejected)")
        for ndt_id in sorted(set(list(self.per_ndt_rx) +
                                  list(self.per_ndt_pop) +
                                  list(self.per_ndt_drop))):
            prio = NDT_PRIORITIES.get(ndt_id, "?")
            lines.append(
                f"  ndt-{ndt_id} (p={prio}): "
                f"rx={self.per_ndt_rx.get(ndt_id,0)} "
                f"pop={self.per_ndt_pop.get(ndt_id,0)} "
                f"drop={self.per_ndt_drop.get(ndt_id,0)}"
            )
        return "\n".join(lines)


def write_outputs(rec_state: QueueReconstructor, out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)

    with open(out_dir / "queue_depth_timeline.csv", "w") as f:
        f.write("t_s,event,ndt_id,priority,depth_after\n")
        for t_s, ev, ndt_id, prio, depth in rec_state.timeline:
            f.write(f"{t_s:.4f},{ev},{ndt_id},{prio},{depth}\n")

    with open(out_dir / "depth_vs_wait.csv", "w") as f:
        f.write("depth_at_admission,queue_ms\n")
        for depth, qms in rec_state.depth_vs_wait:
            f.write(f"{depth},{qms}\n")

    elapsed = rec_state.timeline[-1][0] if rec_state.timeline else 0.0
    summary = rec_state.summary_text(elapsed)
    with open(out_dir / "ops_summary.txt", "w") as f:
        f.write(summary + "\n")

    print(f"\nSaved: {out_dir / 'queue_depth_timeline.csv'}")
    print(f"Saved: {out_dir / 'depth_vs_wait.csv'}")
    print(f"Saved: {out_dir / 'ops_summary.txt'}")
    print("\n" + summary)


def run_stream(out_dir: Path, report_every: float) -> None:
    rec_state = QueueReconstructor()
    start = time.monotonic()
    last_report = start
    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            rec_state.handle(rec)

            now = time.monotonic()
            if now - last_report >= report_every:
                elapsed = rec_state.timeline[-1][0] if rec_state.timeline else 0.0
                print("\n" + "="*60)
                print(rec_state.summary_text(elapsed))
                sys.stdout.flush()
                last_report = now
    except KeyboardInterrupt:
        print("\n[analyze_queue_operations] Interrupted — writing outputs...")
    finally:
        write_outputs(rec_state, out_dir)


def run_file(log_path: Path, out_dir: Path) -> None:
    rec_state = QueueReconstructor()
    with open(log_path) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue
            rec_state.handle(rec)
    write_outputs(rec_state, out_dir)


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Non-invasive, log-based AdmissionQueue operation/"
                     "complexity scalability analyzer"
    )
    ap.add_argument("--stream", action="store_true",
                    help="Read newline-delimited JSON from stdin "
                         "(pipe from `kubectl exec ... tail -f`)")
    ap.add_argument("--log", type=str, default=None,
                    help="Path to a completed dtm_upstream.log file "
                         "(post-hoc mode)")
    ap.add_argument("--out", type=str, required=True)
    ap.add_argument("--report-every", type=float, default=5.0,
                    help="Seconds between live summaries in --stream mode")
    args = ap.parse_args()

    out_dir = Path(args.out)

    if args.stream:
        run_stream(out_dir, args.report_every)
    elif args.log:
        run_file(Path(args.log), out_dir)
    else:
        print("ERROR: pass either --stream or --log PATH", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()