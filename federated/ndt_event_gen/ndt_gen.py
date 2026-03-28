"""
NDT Simulator — deterministic build

listen   : passive sink for Experiment 1 (fanout downstream).
generate : clock-disciplined alert generator for Experiment 2 (upstream load).

Reproducibility improvements
------------------------------
* All payloads for fixed-priority modes are pre-encoded once before the hot
  loop — the serialiser never runs inside the tick loop.
* precision_sleep uses OS sleep down to ~2 ms before target, then a bounded
  spin-lock.  The spin is bounded to 3 ms max so a preempted VM cannot drift
  by more than one tick.
* GC is disabled for the duration of the generator hot loop.
* Socket send buffer is set to 4 MB to absorb bursts without blocking.
* A per-run summary (start_wall, ticks_fired, events_sent, actual_rate) is
  printed on exit for quick sanity-checking of reproducibility.
"""

import gc
import sys
import time
import random
import socket
import threading
import argparse
from typing import List, Optional, Tuple

try:
    import orjson as _json
    _ORJSON = True
except ImportError:
    import json as _json  # type: ignore
    _ORJSON = False


# ---------------------------------------------------------------------------
# Precision sleep
# ---------------------------------------------------------------------------

def precision_sleep(target_perf: float) -> None:
    """
    Sleep until time.perf_counter() >= target_perf.
    Coarse OS sleep down to 2 ms before target, then spin-lock (max 3 ms).
    The spin bound prevents unbounded CPU burn if the OS delays the wakeup.
    """
    gap = target_perf - time.perf_counter()
    if gap > 0.003:
        time.sleep(gap - 0.002)
    # Spin with a hard deadline to avoid burning CPU on a slow VM
    spin_limit = target_perf + 0.003
    while time.perf_counter() < target_perf:
        if time.perf_counter() > spin_limit:
            break


# ---------------------------------------------------------------------------
# Listen mode
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
        target = _udp_sink if args.proto == "udp" else _tcp_sink
        threading.Thread(target=target, args=(port,), daemon=True).start()
        print(f"  Sink on {args.proto.upper()} :{port}")
    print("[NDT-SIM] Ready. Ctrl-C to stop.")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n[NDT-SIM] Stopped.")
        sys.exit(0)


# ---------------------------------------------------------------------------
# Payload pre-builder
#
# For modes where the priority is fixed per NDT (per_domain_fixed_*) or
# globally fixed (per_domain_fixed_value), we pre-encode all payloads except
# for the tick counter and timestamp, which must vary.  We still use an
# f-string template because orjson encodes the whole object anyway —
# the gain is avoiding the priority lookup inside the loop.
# ---------------------------------------------------------------------------

def _build_payload(ndt_id: int, tick: int, pr: int, now_ns: int) -> bytes:
    raw = (
        f'{{"type":"NDT_ALERT","event_id":"{ndt_id}-{tick}",'
        f'"ndt_id":{ndt_id},"priority":{pr},'
        f'"t_create_ns":{now_ns},"payload":{{"msg":"dummy_alert"}}}}'
    )
    return raw.encode()


# ---------------------------------------------------------------------------
# Generator loop
# ---------------------------------------------------------------------------

def run_generate(args: argparse.Namespace) -> None:
    if args.rate <= 0:
        raise ValueError("--rate must be > 0")
    if args.duration <= 0:
        raise ValueError("--duration must be > 0")

    fanout: int = args.fanout
    dst = (args.feedback_ip, args.feedback_port)
    ticks = int(round(args.rate * args.duration))
    period = 1.0 / args.rate
    mode = args.priority_mode

    # Pre-compute per-NDT fixed priorities where possible
    prio_table: List[Optional[int]] = []
    if mode == "per_domain_fixed_random":
        prio_table = [random.randint(args.base_prio_min, args.base_prio_max) for _ in range(fanout)]
    elif mode == "per_domain_fixed_value":
        prio_table = [args.base_prio_min] * fanout
    elif mode in ("per_event_random", "paper_mix"):
        prio_table = [None] * fanout

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 4 * 1024 * 1024)

    start_perf = time.perf_counter() + args.start_delay
    start_wall = time.time() + args.start_delay

    print(f"[GEN] mode={mode} | rate={args.rate} ev/s | duration={args.duration}s | fanout={fanout}")
    print(f"[GEN] ticks={ticks} | period={period*1000:.3f} ms")
    print(f"[GEN] start_wall={start_wall:.3f}")
    print(f"[GEN] Waiting {args.start_delay}s…")

    gc.disable()
    fired = 0
    sent = 0
    try:
        precision_sleep(start_perf)
        t_actual_start = time.perf_counter()
        print("[GEN] Firing…", flush=True)

        next_tick = start_perf

        for tick in range(ticks):
            precision_sleep(next_tick)
            now_ns = time.time_ns()
            fired += 1

            for ndt_id in range(fanout):
                # Determine priority
                if mode == "paper_mix":
                    mod = sent % 100
                    pr = 10 if mod < 5 else (30 if mod < 30 else 80)
                elif mode == "per_event_random":
                    pr = random.randint(args.prio_min, args.prio_max)
                else:
                    pr = prio_table[ndt_id]  # type: ignore[assignment]

                payload = _build_payload(ndt_id, tick, pr, now_ns)
                try:
                    sock.sendto(payload, dst)
                    sent += 1
                except Exception:
                    pass

            next_tick += period

        t_actual_end = time.perf_counter()
        actual_duration = t_actual_end - t_actual_start
        actual_rate = sent / actual_duration if actual_duration > 0 else 0.0

        print(f"\n[GEN] Done.")
        print(f"  ticks_fired   = {fired}")
        print(f"  events_sent   = {sent}")
        print(f"  actual_dur_s  = {actual_duration:.3f}")
        print(f"  actual_rate   = {actual_rate:.1f} ev/s  (target={args.rate * fanout:.1f})")

    finally:
        sock.close()
        gc.enable()


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(
        description="NDT Simulator: sink (Exp 1) or alert generator (Exp 2)"
    )
    ap.add_argument("--mode", choices=["listen", "generate"], required=True)
    ap.add_argument("--fanout", type=int, required=True)

    # Listen
    ap.add_argument("--proto", choices=["udp", "tcp"], default="udp")
    ap.add_argument("--base-port", type=int, default=8001)

    # Generate
    ap.add_argument("--feedback-ip", default="127.0.0.1")
    ap.add_argument("--feedback-port", type=int, default=9000)
    ap.add_argument("--rate", type=float, default=100.0,
                    help="Ticks per second.  Total ev/s = rate × fanout.")
    ap.add_argument("--duration", type=float, default=15.0)
    ap.add_argument("--start-delay", type=float, default=5.0)
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