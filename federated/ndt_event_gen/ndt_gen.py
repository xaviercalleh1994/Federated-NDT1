import socket
import argparse
import time
import random
import threading
import sys

# CAMBIO ESENCIAL: Usar orjson para velocidad extrema
try:
    import orjson as json

    _USING_ORJSON = True
except ImportError:
    import json

    _USING_ORJSON = False


# =================================================================
# MODO LISTEN (EXPERIMENTO 1) - Actúa como Sumidero (Sink)
# =================================================================
def udp_listener(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024 * 1024)
    s.bind(("0.0.0.0", port))
    while True:
        try:
            s.recvfrom(65535)
        except Exception:
            pass


def tcp_listener(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("0.0.0.0", port))
    s.listen(5)
    while True:
        try:
            conn, _ = s.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            threading.Thread(target=tcp_handler, args=(conn,), daemon=True).start()
        except Exception:
            pass


def tcp_handler(conn):
    while True:
        try:
            data = conn.recv(65535)
            if not data: break
        except Exception:
            break
    conn.close()


# =================================================================
# MODO GENERATE (EXPERIMENTO 2) - Lógica Original Optimizada
# =================================================================
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


def generator_loop(args):
    if args.rate <= 0: raise ValueError("rate must be >0")
    if args.duration <= 0: raise ValueError("duration must be >0")

    fanout = args.fanout
    dst = (args.feedback_ip, args.feedback_port)
    ticks = int(round(args.rate * args.duration))
    period = 1.0 / args.rate

    start_time_wall = time.time() + args.start_delay
    start_time_perf = time.perf_counter() + args.start_delay

    print(f"[GEN] start_time={start_time_wall:.6f}")
    print(f"[GEN] theoretical_events={ticks * fanout}")
    print(f"[GEN] priority_mode={args.priority_mode}")
    if not _USING_ORJSON:
        print("[GEN] WARNING: orjson no detectado. Usando json estándar (más lento).")

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 2097152)

    # Solo precalculamos si NO es paper_mix
    prio_table = []
    if args.priority_mode != "paper_mix":
        prio_table = build_priority_table(fanout, args.priority_mode, args.prio_min, args.prio_max, args.base_prio_min,
                                          args.base_prio_max)

    # Espera inicial precisa
    while time.perf_counter() < start_time_perf:
        pass

    sent = 0
    next_tick_perf = start_time_perf

    for tick in range(ticks):
        # Híbrido Sleep/Spin-Wait
        sleep_time = next_tick_perf - time.perf_counter()
        if sleep_time > 0.002:
            time.sleep(sleep_time - 0.001)
        while time.perf_counter() < next_tick_perf:
            pass

        now_ns = time.time_ns()

        for ndt_id in range(fanout):
            # AQUÍ ESTÁ LA MAGIA DEL PAPER_MIX
            if args.priority_mode == "paper_mix":
                mod = sent % 100
                if mod < 5:
                    pr = 10  # 5% pasan la cola y van al ONM
                elif mod < 30:
                    pr = 30  # 25% pasan la cola pero mueren en el DTM
                else:
                    pr = 80  # 70% son dropeables si la cola se llena
            elif args.priority_mode == "per_event_random":
                pr = random.randint(args.prio_min, args.prio_max)
            else:
                pr = prio_table[ndt_id]

            ev = {
                "type": "NDT_ALERT",
                "event_id": f"{ndt_id}-{tick}",
                "ndt_id": ndt_id,
                "priority": pr,
                "t_create_ns": now_ns,
                "payload": {"msg": "dummy_alert"}
            }

            payload = json.dumps(ev)
            if not isinstance(payload, bytes):
                payload = payload.encode()

            try:
                sock.sendto(payload, dst)
                sent += 1
            except Exception as e:
                print(f"[GEN] send failed: {e}")

        next_tick_perf += period

    sock.close()
    print(f"\n[GEN] experiment finished. events_sent={sent}")


# =================================================================
# MAIN
# =================================================================
def main():
    ap = argparse.ArgumentParser(description="Unificado: Sumidero NDT (Exp1) y Generador de Telemetría (Exp2)")

    ap.add_argument("--mode", choices=["listen", "generate"], required=True, help="Modo de operación")
    ap.add_argument("--fanout", type=int, required=True, help="Cantidad de NDTs")

    # Argumentos para MODO LISTEN
    ap.add_argument("--proto", choices=["udp", "tcp"], default="tcp", help="Protocolo de escucha (Exp 1)")
    ap.add_argument("--base-port", type=int, default=8001, help="Puerto inicial (Exp 1)")

    # Argumentos para MODO GENERATE
    ap.add_argument("--feedback-ip", default="127.0.0.1")
    ap.add_argument("--feedback-port", type=int, default=9000)
    ap.add_argument("--rate", type=float, default=100.0)
    ap.add_argument("--duration", type=float, default=15.0)
    ap.add_argument("--start-delay", type=float, default=5.0)

    # AÑADIDO paper_mix a los choices AQUÍ
    ap.add_argument("--priority-mode",
                    choices=["per_event_random", "per_domain_fixed_random", "per_domain_fixed_value", "paper_mix"],
                    default="per_domain_fixed_random")
    ap.add_argument("--prio-min", type=int, default=5)
    ap.add_argument("--prio-max", type=int, default=80)
    ap.add_argument("--base-prio-min", type=int, default=10)
    ap.add_argument("--base-prio-max", type=int, default=60)

    args = ap.parse_args()

    if args.mode == "listen":
        print(f"[NDT-SIM] MODO ESCUCHA ACTIVO (Experimento 1)")
        for i in range(args.fanout):
            port = args.base_port + i
            if args.proto == "udp":
                threading.Thread(target=udp_listener, args=(port,), daemon=True).start()
            else:
                threading.Thread(target=tcp_listener, args=(port,), daemon=True).start()
        print("[NDT-SIM] Listo para recibir eventos. Ctrl+C para salir.")
        try:
            while True: time.sleep(1)
        except KeyboardInterrupt:
            print("\n[NDT-SIM] Apagando listeners...")
            sys.exit(0)

    elif args.mode == "generate":
        print(f"[NDT-SIM] MODO GENERACIÓN ACTIVO (Experimento 2)")
        generator_loop(args)


if __name__ == "__main__":
    main()