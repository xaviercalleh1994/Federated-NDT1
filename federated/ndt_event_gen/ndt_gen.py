import socket
import argparse
import time
import json
import random


def build_priority_table(fanout, mode, prio_min, prio_max, base_prio_min, base_prio_max):
    table = []

    if mode == "per_event_random":
        table = [None] * fanout

    elif mode == "per_domain_fixed_random":
        for _ in range(fanout):
            table.append(random.randint(base_prio_min, base_prio_max))

    elif mode == "per_domain_fixed_value":
        for _ in range(fanout):
            table.append(int(base_prio_min))

    return table


def main():

    ap = argparse.ArgumentParser()

    ap.add_argument("--fanout", type=int, required=True)
    ap.add_argument("--feedback-ip", required=True)
    ap.add_argument("--feedback-port", type=int, required=True)

    ap.add_argument("--rate", type=float, required=True)
    ap.add_argument("--duration", type=float, required=True)
    ap.add_argument("--start-delay", type=float, default=5.0)

    ap.add_argument("--priority-mode", choices=[
        "per_event_random",
        "per_domain_fixed_random",
        "per_domain_fixed_value"
    ], default="per_domain_fixed_random")

    ap.add_argument("--prio-min", type=int, default=5)
    ap.add_argument("--prio-max", type=int, default=80)

    ap.add_argument("--base-prio-min", type=int, default=10)
    ap.add_argument("--base-prio-max", type=int, default=60)

    args = ap.parse_args()

    if args.rate <= 0:
        raise ValueError("rate must be >0")

    if args.duration <= 0:
        raise ValueError("duration must be >0")

    fanout = args.fanout
    dst = (args.feedback_ip, args.feedback_port)

    ticks = int(round(args.rate * args.duration))
    period = 1.0 / args.rate

    start_time = time.time() + args.start_delay
    end_time = start_time + args.duration

    print(f"[GEN] start_time={start_time:.6f}")
    print(f"[GEN] end_time={end_time:.6f}")
    print(f"[GEN] duration={args.duration}s")
    print(f"[GEN] rate={args.rate} events/s per NDT")
    print(f"[GEN] ticks={ticks}")
    print(f"[GEN] total_theoretical_events={ticks * fanout}")

    # UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # deterministic priority assignment
    prio_table = build_priority_table(
        fanout,
        args.priority_mode,
        args.prio_min,
        args.prio_max,
        args.base_prio_min,
        args.base_prio_max
    )

    for i, p in enumerate(prio_table):
        if p is not None:
            print(f"[GEN] domain {i} base_priority={p}")

    # wait until start
    while True:
        now = time.time()
        if now >= start_time:
            break
        time.sleep(min(0.001, start_time - now))

    sent = 0
    first_send = None
    last_send = None

    for tick in range(ticks):

        tick_time = start_time + tick * period

        now = time.time()

        if now < tick_time:
            time.sleep(tick_time - now)

        now = time.time()

        if now >= end_time:
            break

        for ndt_id in range(fanout):

            if args.priority_mode == "per_event_random":
                pr = random.randint(args.prio_min, args.prio_max)
            else:
                pr = prio_table[ndt_id]

            ev = {
                "type": "NDT_ALERT",
                "event_id": f"{ndt_id}-{tick}",
                "ndt_id": ndt_id,
                "priority": pr,
                "t_create_ns": time.time_ns(),
                "payload": {"msg": "dummy_alert"}
            }

            payload = json.dumps(ev).encode()

            try:
                sock.sendto(payload, dst)

                if first_send is None:
                    first_send = time.time()

                last_send = time.time()

                sent += 1

            except Exception as e:
                print(f"[GEN] send failed: {e}")

    sock.close()

    print("\n[GEN] experiment finished")
    print(f"[GEN] events_sent={sent}")

    if first_send and last_send:
        window = last_send - first_send
        print(f"[GEN] effective_window={window:.6f}s")
        print(f"[GEN] effective_rate={sent/window:.2f} ev/s")


if __name__ == "__main__":
    main()