# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.
"""
Authors: Xavier Calle-Heredia & Xavier Hesselbach
Date: 2026-04-08

NDT Simulator

listen   : passive sink for Experiment 1 (fanout downstream).
generate : clock-disciplined alert generator for Experiment 2 (upstream load).

Arrival modes
-------------
uniform  : deterministic inter-arrival (original behaviour, unchanged).
           Each NDT fires at exactly equal intervals. Period must be >= 5ms
           on VirtualBox (max reliable rate per NDT = ~200 ev/s).

poisson  : exponentially-distributed inter-arrival times with mean 1/rate.
           Each NDT fires randomly (M/D/1 model). Produces realistic
           bursty traffic. No period constraint — precision_sleep handles
           any interval naturally.

Usage
-----
# Uniform — 10 NDTs x 200 ev/s = 2000 ev/s total
python3 ndt_sim.py --mode generate \
    --fanout 10 --rate 2000 --arrival-mode uniform \
    --feedback-ip 192.168.99.1 --feedback-port 9000 \
    --priority-mode paper_mix --duration 60 --start-delay 10

# Poisson — same average rate, random inter-arrivals
python3 ndt_sim.py --mode generate \
    --fanout 10 --rate 2000 --arrival-mode poisson \
    --feedback-ip 192.168.99.1 --feedback-port 9000 \
    --priority-mode paper_mix --duration 60 --start-delay 10

NOTE: --rate is TOTAL events/s across all NDTs.
      Each NDT process sends at rate/fanout ev/s independently.
"""

import gc
import sys
import time
import random
import socket
import argparse
import threading
import multiprocessing as mp
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Precision sleep — absolute target, bounded spin
# ---------------------------------------------------------------------------

def precision_sleep(target_perf: float) -> None:
    gap = target_perf - time.perf_counter()
    if gap > 0.003:
        time.sleep(gap - 0.002)
    spin_deadline = target_perf + 0.005
    while time.perf_counter() < target_perf:
        if time.perf_counter() > spin_deadline:
            break


# ---------------------------------------------------------------------------
# Payload builder — f-string, no serialiser overhead
# ---------------------------------------------------------------------------

def _build_payload(ndt_id: int, tick: int, pr: int, now_ns: int) -> bytes:
    return (
        f'{{"type":"NDT_ALERT","event_id":"{ndt_id}-{tick}",'
        f'"ndt_id":{ndt_id},"priority":{pr},'
        f'"t_create_ns":{now_ns},"payload":{{"msg":"dummy_alert"}}}}'
    ).encode()


def _get_priority(mode: str, sent: int, fixed_priority: Optional[int],
                  prio_min: int, prio_max: int) -> int:
    if mode == "paper_mix":
        mod = sent % 100
        return 10 if mod < 5 else (30 if mod < 30 else 80)
    if mode == "per_event_random":
        return random.randint(prio_min, prio_max)
    return fixed_priority  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Per-NDT worker — uniform arrivals (original behaviour, unchanged)
# ---------------------------------------------------------------------------

def _ndt_worker_uniform(
    ndt_id: int,
    dst: Tuple[str, int],
    rate_per_ndt: float,
    duration: float,
    start_wall: float,
    mode: str,
    fixed_priority: Optional[int],
    prio_min: int,
    prio_max: int,
    result_q: mp.Queue,
) -> None:
    gc.disable()

    period = 1.0 / rate_per_ndt
    ticks = int(round(rate_per_ndt * duration))

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2 * 1024 * 1024)

    now_wall = time.time()
    now_perf = time.perf_counter()
    start_perf = now_perf + (start_wall - now_wall)

    precision_sleep(start_perf)
    t_actual_start = time.perf_counter()

    fired = skipped = sent = 0

    for tick in range(ticks):
        target = start_perf + tick * period
        now_perf = time.perf_counter()

        if now_perf > target + period:
            skipped += 1
            continue

        precision_sleep(target)
        now_ns = time.time_ns()
        fired += 1

        pr = _get_priority(mode, sent, fixed_priority, prio_min, prio_max)
        payload = _build_payload(ndt_id, tick, pr, now_ns)
        try:
            sock.sendto(payload, dst)
            sent += 1
        except Exception:
            pass

    t_actual_end = time.perf_counter()
    actual_duration = t_actual_end - t_actual_start
    actual_rate = sent / actual_duration if actual_duration > 0 else 0.0

    sock.close()
    gc.enable()

    result_q.put({
        "ndt_id": ndt_id,
        "arrival_mode": "uniform",
        "fired": fired,
        "skipped": skipped,
        "sent": sent,
        "actual_dur_s": round(actual_duration, 3),
        "actual_rate": round(actual_rate, 1),
    })


# ---------------------------------------------------------------------------
# Per-NDT worker — Poisson arrivals
#
# Inter-arrival times drawn from Exp(rate_per_ndt).
# No tick concept — next event time advances by a random exponential interval.
# ---------------------------------------------------------------------------

def _ndt_worker_poisson(
    ndt_id: int,
    dst: Tuple[str, int],
    rate_per_ndt: float,
    duration: float,
    start_wall: float,
    mode: str,
    fixed_priority: Optional[int],
    prio_min: int,
    prio_max: int,
    result_q: mp.Queue,
) -> None:
    gc.disable()

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2 * 1024 * 1024)

    now_wall = time.time()
    now_perf = time.perf_counter()
    start_perf = now_perf + (start_wall - now_wall)
    end_perf = start_perf + duration

    precision_sleep(start_perf)
    t_actual_start = time.perf_counter()

    fired = sent = 0
    next_event = start_perf

    while True:
        next_event += random.expovariate(rate_per_ndt)
        if next_event > end_perf:
            break

        precision_sleep(next_event)
        now_ns = time.time_ns()
        fired += 1

        pr = _get_priority(mode, sent, fixed_priority, prio_min, prio_max)
        payload = _build_payload(ndt_id, fired, pr, now_ns)
        try:
            sock.sendto(payload, dst)
            sent += 1
        except Exception:
            pass

    t_actual_end = time.perf_counter()
    actual_duration = t_actual_end - t_actual_start
    actual_rate = sent / actual_duration if actual_duration > 0 else 0.0

    sock.close()
    gc.enable()

    result_q.put({
        "ndt_id": ndt_id,
        "arrival_mode": "poisson",
        "fired": fired,
        "skipped": 0,
        "sent": sent,
        "actual_dur_s": round(actual_duration, 3),
        "actual_rate": round(actual_rate, 1),
    })


# ---------------------------------------------------------------------------
# Listen mode — passive sinks for Experiment 1
# ---------------------------------------------------------------------------

def _udp_sink(port: int) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024 * 1024)
    s.bind(("0.0.0.0", port))
    while True:
        try:
            s.recvfrom(65535)
        except Exception:
            pass


def _tcp_handler(conn: socket.socket) -> None:
    while True:
        try:
            if not conn.recv(65535):
                break
        except Exception:
            break
    conn.close()


def _tcp_sink(port: int) -> None:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", port))
    s.listen(32)
    while True:
        try:
            conn, _ = s.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            threading.Thread(target=_tcp_handler, args=(conn,), daemon=True).start()
        except Exception:
            pass


def run_listen(args: argparse.Namespace) -> None:
    print(f"[NDT-SIM] LISTEN mode | fanout={args.fanout} proto={args.proto}")
    for i in range(args.fanout):
        port = args.base_port + i
        fn = _udp_sink if args.proto == "udp" else _tcp_sink
        threading.Thread(target=fn, args=(port,), daemon=True).start()
        print(f"  Sink on {args.proto.upper()} :{port}")
    print("[NDT-SIM] Ready. Ctrl-C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[NDT-SIM] Stopped.")
        sys.exit(0)


# ---------------------------------------------------------------------------
# Generate mode — one OS process per NDT
# ---------------------------------------------------------------------------

def run_generate(args: argparse.Namespace) -> None:
    if args.rate <= 0:
        raise ValueError("--rate must be > 0")
    if args.duration <= 0:
        raise ValueError("--duration must be > 0")
    if args.fanout <= 0:
        raise ValueError("--fanout must be > 0")

    fanout = args.fanout
    rate_per_ndt = args.rate / fanout
    period_ms = 1000.0 / rate_per_ndt
    arrival = args.arrival_mode
    dst = (args.feedback_ip, args.feedback_port)
    mode = args.priority_mode

    if arrival == "uniform" and period_ms < 5.0:
        print(f"[WARN] period={period_ms:.2f}ms < 5ms — VM may skip ticks.")
        print(f"[WARN] For stable uniform arrivals use --fanout >= {int(args.rate/200)+1}")

    fixed_priorities: List[Optional[int]] = []
    if mode == "per_domain_fixed_random":
        fixed_priorities = [
            random.randint(args.base_prio_min, args.base_prio_max)
            for _ in range(fanout)
        ]
    elif mode == "per_domain_fixed_value":
        fixed_priorities = [args.base_prio_min] * fanout
    else:
        fixed_priorities = [None] * fanout

    start_wall = time.time() + args.start_delay

    print(f"[GEN] arrival_mode={arrival} | fanout={fanout} | "
          f"total_rate={args.rate} ev/s | per_ndt={rate_per_ndt:.4f} ev/s | "
          f"period={period_ms:.3f} ms")
    print(f"[GEN] duration={args.duration}s | priority_mode={mode}")
    print(f"[GEN] architecture: {fanout} independent OS processes (no GIL)")
    print(f"[GEN] shared start_wall={start_wall:.3f}")
    print(f"[GEN] Launching processes, firing in {args.start_delay}s…")

    worker = _ndt_worker_uniform if arrival == "uniform" else _ndt_worker_poisson

    result_q: mp.Queue = mp.Queue()
    processes = []

    for ndt_id in range(fanout):
        p = mp.Process(
            target=worker,
            args=(
                ndt_id, dst, rate_per_ndt, args.duration,
                start_wall, mode, fixed_priorities[ndt_id],
                args.prio_min, args.prio_max, result_q,
            ),
            daemon=True,
            name=f"ndt-{ndt_id}",
        )
        p.start()
        processes.append(p)

    print(f"[GEN] All {fanout} processes started. Waiting for completion…")

    timeout_per = args.duration + args.start_delay + 15.0
    results = []
    for _ in processes:
        try:
            results.append(result_q.get(timeout=timeout_per))
        except Exception:
            pass

    for p in processes:
        p.join(timeout=5.0)
        if p.is_alive():
            p.terminate()

    if not results:
        print("[GEN] ERROR: no results collected — all processes may have failed.")
        return

    total_sent    = sum(r["sent"]    for r in results)
    total_fired   = sum(r["fired"]   for r in results)
    total_skipped = sum(r["skipped"] for r in results)
    durations     = [r["actual_dur_s"] for r in results]
    rates         = [r["actual_rate"]  for r in results]

    dur_mean = sum(durations) / len(durations)
    dur_min  = min(durations)
    dur_max  = max(durations)
    rate_mean = sum(rates) / len(rates)
    rate_min  = min(rates)
    rate_max  = max(rates)
    total_actual_rate = total_sent / dur_mean if dur_mean > 0 else 0.0

    print(f"\n[GEN] Done.")
    print(f"  arrival_mode    = {arrival}")
    print(f"  processes       = {fanout}")
    print(f"  total_fired     = {total_fired}  (skipped={total_skipped})")
    print(f"  total_sent      = {total_sent}")
    print(f"  duration        = {dur_mean:.3f}s mean  [{dur_min:.3f}, {dur_max:.3f}]")
    print(f"  per_ndt_rate    = {rate_mean:.1f} ev/s  [{rate_min:.1f}, {rate_max:.1f}]")
    print(f"  total_rate      = {total_actual_rate:.1f} ev/s  (target={args.rate:.1f})")

    if total_skipped > 0 and arrival == "uniform":
        skip_pct = 100.0 * total_skipped / max(1, total_fired + total_skipped)
        print(f"  [WARN] {total_skipped} ticks skipped ({skip_pct:.1f}%)")
        if skip_pct > 5.0:
            print(f"  [WARN] >5% skipped — reduce --rate or increase --fanout")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    mp.set_start_method("spawn", force=True)

    ap = argparse.ArgumentParser(
        description="NDT Simulator: passive sink (Exp 1) or per-NDT generator (Exp 2)"
    )
    ap.add_argument("--mode", choices=["listen", "generate"], required=True)
    ap.add_argument("--fanout", type=int, required=True)

    ap.add_argument("--proto", choices=["udp", "tcp"], default="udp")
    ap.add_argument("--base-port", type=int, default=8001)

    ap.add_argument("--feedback-ip", default="127.0.0.1")
    ap.add_argument("--feedback-port", type=int, default=9000)
    ap.add_argument("--rate", type=float, default=2000.0)
    ap.add_argument("--duration", type=float, default=60.0)
    ap.add_argument("--start-delay", type=float, default=10.0)
    ap.add_argument(
        "--arrival-mode",
        choices=["uniform", "poisson"],
        default="uniform",
        help="uniform: fixed inter-arrival (D model). poisson: exponential inter-arrival (M model).",
    )
    ap.add_argument(
        "--priority-mode",
        choices=["per_event_random", "per_domain_fixed_random",
                 "per_domain_fixed_value", "paper_mix"],
        default="paper_mix",
    )
    ap.add_argument("--prio-min", type=int, default=5)
    ap.add_argument("--prio-max", type=int, default=80)
    ap.add_argument("--base-prio-min", type=int, default=10)
    ap.add_argument("--base-prio-max", type=int, default=60)

    args = ap.parse_args()

    if args.mode == "listen":
        run_listen(args)
    else:
        run_generate(args)


if __name__ == "__main__":
    main()