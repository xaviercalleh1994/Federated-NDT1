import time
import socket
import argparse
import threading
import sys
from dataclasses import dataclass
from typing import Dict, List, Tuple
from collections import defaultdict
import queue
from collections import deque
import os
import gc
import multiprocessing as mp

# Intentar usar orjson para máxima velocidad, fallback a json estándar
try:
    import orjson as json
except ImportError:
    import json

# ============================================================
# ASYNC LOGGING
# ============================================================
# Aumentamos el maxsize drásticamente para soportar tormentas de 5000 ev/s
log_queue = queue.Queue(maxsize=2000000)
log_stop = threading.Event()

# ============================================================
# CONSTANTS
# ============================================================
topo_file = "/run/topology.json"
control_host = "127.0.0.1"
control_port = 6001
log_file_default = "/home/ubuntu/ndtm_events.log"

# variable to globally store the arguments used to launch the DTM
run_ctx: Dict[str, object] = {}

# ============================================================
# NEW: deterministic stop mechanism (event-limit)
# - Tracks unique completed event_ids
# - Stops once count reaches --event-limit
# ============================================================
done_event_ids = set()
done_lock = threading.Lock()
stop_event = threading.Event()


def count_done_event(
        event_id: str,
        *,
        require_full_fanout: bool = False,
        sent_ok: int = None,
        fanout: int = None,
):
    """
    Function to mark events either form ONM or NDTs ad completed.

    Events are marked as completed if:
        - ONM events (fanout == false) have been totally dispatched to all NDTs
        - NDT events (fanout == true) completion when DTM_EVENT_DONE is
          logged (dequeued events).
    """
    if not event_id:
        return

    limit = int(run_ctx.get("event_limit", 0) or 0)
    if limit <= 0:
        return

    if require_full_fanout:
        if sent_ok is None or fanout is None:
            return
        if sent_ok != fanout:
            return

    with done_lock:
        if event_id in done_event_ids:
            return
        done_event_ids.add(event_id)
        done_n = len(done_event_ids)

    # Optional debug line (reducimos la impresión a cada 500 para no ahogar la terminal):
    if run_ctx.get("print_progress", False) and done_n % 500 == 0:
        print(f"[DTM] done {done_n}/{limit} (event_id={event_id})", flush=True)

    if done_n >= limit:
        stop_event.set()


def limit_reached():
    """Exit cleanly once stop_event is set."""
    if stop_event.is_set():
        time.sleep(0.15)
        print(f"\n[DTM] experiment duration reached. Flushing logs...", flush=True)
        log_stop.set()

        # Drenamos la cola, pero con un límite máximo de 5 segundos de seguridad.
        # Si la red está en silencio (como debe ser), esto tomará 0.1 segundos.
        timeout = 50  # 50 * 0.1s = 5 segundos
        while not log_queue.empty() and timeout > 0:
            time.sleep(0.1)
            timeout -= 1

        if timeout <= 0:
            print("[DTM] Advertencia: Se forzó el cierre para evitar un loop infinito.", flush=True)
        else:
            print("[DTM] Logs saved successfully. Exiting.", flush=True)

        gc.enable()
        os._exit(0)


# ============================================================
# LOGGING (OPTIMIZADO CON BATCHING Y ESCRITURA BINARIA LIMPIA)
# ============================================================
def log_writer():
    path = run_ctx.get("log_file", log_file_default)
    try:
        with open(path, "wb") as f:
            while not log_stop.is_set() or not log_queue.empty():
                batch = []
                try:
                    for _ in range(5000):
                        batch.append(log_queue.get_nowait())
                except queue.Empty:
                    pass

                if batch:
                    lines = []
                    for entry in batch:
                        try:
                            line = json.dumps(entry)
                            if not isinstance(line, bytes):
                                line = line.encode()
                            lines.append(line)
                        except Exception:
                            # Si un evento viene corrupto, lo ignoramos. ¡EL HILO NUNCA MUERE!
                            pass

                    if lines:
                        f.write(b"\n".join(lines) + b"\n")
                else:
                    time.sleep(0.05)
    except Exception as e:
        print(f"\n[FATAL] El hilo de logs murió por un error de disco: {e}", flush=True)


def log_event(tag: str, data: dict):
    entry = {"ts_ns": time.time_ns(), "tag": tag, **run_ctx, **data}
    try:
        log_queue.put_nowait(entry)
    except queue.Full:
        pass


# ============================================================
# TCP HELPER
# ============================================================
def recv_exact(sock: socket.socket, n: int):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# ============================================================
# DATA MODEL
# ============================================================
@dataclass
class Edge:
    delay_ms: float
    loss_pct: float
    bw_mbps: float
    up: bool


# ============================================================
# TOPOLOGY + LOCAL APPLY
# ============================================================
def is_switch(n: str) -> bool:
    return n.startswith("s")


# Function to load the adjacency matrix topology obtained once the NDT runs
def load_topo_graph() -> Dict[str, List[str]]:
    with open(topo_file) as f:
        content = f.read()
        # Soporte cruzado para json/orjson
        if hasattr(json, 'loads') and not isinstance(content, bytes) and 'orjson' in str(json):
            t = json.loads(content.encode())
        else:
            t = json.loads(content)

    g = defaultdict(list)
    for a, b in t["links"]:
        g[a].append(b)
        g[b].append(a)
    return g


def path_only_switches_middle(g, src, dst):
    q = deque([src])
    prev = {src: None}
    while q:
        cur = q.popleft()
        if cur == dst:
            break
        for nxt in g[cur]:
            if nxt not in prev:
                prev[nxt] = cur
                q.append(nxt)
    if dst not in prev:
        return []

    path = []
    cur = dst
    while cur:
        path.append(cur)
        cur = prev[cur]
    path.reverse()

    if any(not is_switch(x) for x in path[1:-1]):
        return []

    return [(path[i], path[i + 1]) for i in range(len(path) - 1)]


def control_call(ev: dict) -> dict:
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((control_host, control_port))
    data = json.dumps(ev)
    s.sendall((data if isinstance(data, bytes) else data.encode()) + b"\n")
    resp_data = b""
    while b"\n" not in resp_data:
        chunk = s.recv(4096)
        if not chunk:
            break
        resp_data += chunk
    s.close()
    return json.loads(resp_data.split(b"\n")[0]) if resp_data else {"ok": False}


def apply_logical_link(g, a, b, e: Edge) -> int:
    phys = path_only_switches_middle(g, a, b)
    if not phys:
        print(f"[NDT] No logical path {a}-{b}")
        return 0

    for x, y in phys:
        control_call({"type": "LINK_SET", "payload": {"a": x, "b": y, **e.__dict__}})

    control_call(
        {
            "type": "ROUTE_REASSERT",
            "payload": {"routers": ["r1", "r2"], "gateway": "10.0.0.254"},
        }
    )

    return len(phys)


# ============================================================
# RECEIVERS
# ============================================================
class UdpReceiver:
    def __init__(self, relay_ip, relay_port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Timeout para que el DTM no se quede en coma y pueda apagarse
        self.sock.settimeout(0.5)
        # Optimization:
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024 * 1024)
        self.sock.bind(("0.0.0.0", relay_port))
        self.relay = (relay_ip, relay_port)
        threading.Thread(target=self._keepalive, daemon=True).start()

    def _keepalive(self):
        while True:
            try:
                self.sock.sendto(b"HELLO_NDT", self.relay)
            except Exception:
                pass
            time.sleep(5)

    def recv(self) -> bytes:
        try:
            return self.sock.recvfrom(65535)[0]
        except socket.timeout:
            return b""


class TcpReceiver:
    def __init__(self, relay_ip, relay_port):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(0.5)
        self.sock.connect((relay_ip, relay_port))
        self.sock.sendall(b"NDT\n")

    def recv(self) -> bytes:
        try:
            hdr = recv_exact(self.sock, 4)
            if not hdr:
                return b""
            ln = int.from_bytes(hdr, "big")
            payload = recv_exact(self.sock, ln)
            return payload if payload else b""
        except socket.timeout:
            return b""


# ============================================================
# FEEDBACK RECEIVER
# ============================================================
class FeedbackReceiver:
    """
    Class to receive NDT events via UDP, prepare them and send them
    to the DTM scheduler
    """

    def __init__(self, listen_port: int, controller):
        self.controller = controller
        # It creates an UDP socket to receive events
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # Increase the buffer size
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024 * 1024)
        # bind the receiver
        self.sock.bind(("0.0.0.0", listen_port))

        # NUEVA LÓGICA: Colección rápida (deque + Condition) en lugar de Queue
        self.q = deque()
        self.cond = threading.Condition()

        # It defines the main thread just to receive events
        threading.Thread(target=self._rx_loop, daemon=True).start()
        # It defines a thread to process what has been received
        for _ in range(4):  # Subimos a 4 para aguantar la carga masiva
            threading.Thread(target=self._worker_loop, daemon=True).start()
        print(f"[DTM] FeedbackReceiver listening on UDP {listen_port}")

    def _rx_loop(self):
        """
        Loop to receive incoming packets
        """
        while True:
            try:
                data, _ = self.sock.recvfrom(65535)
                t_rx_ns = time.time_ns()
                with self.cond:
                    self.q.append((data, t_rx_ns))
                    self.cond.notify()
            except Exception:
                continue

    def _worker_loop(self):
        """
        Loop to process received packets
        """
        while True:
            # Vaciado en bloque (Batching) para eliminar la contención del Lock
            with self.cond:
                while not self.q:
                    self.cond.wait()
                batch = list(self.q)
                self.q.clear()

            for data, t_rx_ns in batch:
                try:
                    ev = json.loads(data)
                except Exception:
                    continue

                # Pasamos raw_data para omitir re-serialización json en el envío (Zero-Copy)
                self.controller.submit(ev, t_rx_ns, src="feedback", raw_data=data)


# ============================================================
# DOWNSTREAM SENDERS (RETURN sent_ok)
# ============================================================
class UdpSender:
    def __init__(self, ip, base_port, fanout):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.targets = [(ip, base_port + i) for i in range(fanout)]

    def send(self, payload, idxs) -> int:
        ok = 0
        for i in idxs:
            try:
                self.sock.sendto(payload, self.targets[i])
                ok += 1
            except Exception:
                pass
        return ok


class TcpSender:
    def __init__(self, ip, base_port, fanout):
        self.ip = ip
        self.ports = [base_port + i for i in range(fanout)]
        self.socks = {}
        self.lock = threading.Lock()

    def _get(self, port: int):
        with self.lock:
            if port in self.socks:
                return self.socks[port]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.ip, port))
            with self.lock:
                self.socks[port] = s
            return s
        except Exception:
            return None

    def send(self, payload, idxs) -> int:
        ok = 0
        frame = len(payload).to_bytes(4, "big") + payload
        for i in idxs:
            port = self.ports[i]
            s = self._get(port)
            if not s:
                continue
            try:
                s.sendall(frame)
                ok += 1
            except Exception:
                try:
                    s.close()
                except Exception:
                    pass
                with self.lock:
                    self.socks.pop(port, None)
        return ok


# ============================================================
# FEDERATED DISPATCH (FAST PATH FOR ONM EVENTS)
#   - ONM events: NO QUEUE, NO DTM_* (only NDT_EVENT_RX + NDT_EVENT_DISPATCHED)
#   - Feedback: controller queue will handle + full DTM logs
# ============================================================
def is_critical(ev: dict) -> bool:
    """
    Defines logical link set events as critical
    """
    return ev.get("type") == "LOGICAL_LINK_SET" and not bool(
        ev.get("payload", {}).get("up", True)
    )


def fast_dispatch_to_fanout(
        sender, fanout: int, group_size: int, wave_delay: float, ev: dict
) -> Tuple[int, int]:
    """
    Dispatch ONM events immediately (no queue). Returns:
      (sent_ok_total, dispatch_ns_total)
    """
    targets = list(range(fanout))
    data = json.dumps(ev)
    if not isinstance(data, bytes):
        data = data.encode()

    sent_ok_total = 0
    dispatch_ns_total = 0

    # Just logical link set are critical
    if is_critical(ev):
        t0 = time.time_ns()
        sent_ok_total = sender.send(data, targets)
        dispatch_ns_total = time.time_ns() - t0
        return sent_ok_total, dispatch_ns_total

    # rolling groups
    gs = max(1, group_size)
    for i in range(0, fanout, gs):
        grp = targets[i: i + gs]
        t0 = time.time_ns()
        sent_ok_total += sender.send(data, grp)
        dispatch_ns_total += time.time_ns() - t0
        if wave_delay > 0:
            time.sleep(wave_delay)

    return sent_ok_total, dispatch_ns_total


def ev_priority(ev):
    """
    Function to get the priorities of ON or NDT events
    """
    t = ev.get("type", "NA")
    if t == "LOGICAL_LINK_SET":
        up = bool(ev.get("payload", {}).get("up", True))
        return 0 if not up else 10
    if t == "EXEC_CMD":
        return 50
    if t == "NDT_ALERT":
        # smaller number = higher priority
        return int(ev.get("priority", 30))
    return 100


def link_key(ev):
    p = ev.get("payload", {})
    a = str(p.get("a", ""))
    b = str(p.get("b", ""))
    return (a, b) if a <= b else (b, a)


class FederatedController:
    """
    Class acting as a federation scheduler. Among its main functions are:
    Receive NDT events, decide if events are enqueued or dropped, dequeue events, logg events.
    It is conceived to work with the ON events as well with a deduplication policy (applied in
    logical link set events to preserve the latest command)
    """

    def __init__(
            self,
            sender,
            onm_sender,
            fanout,
            group_size,
            wave_delay,
            max_queue: int,
            drop_prio_if_full: int,
            disable_priorities: bool = False,
            send_workers: int = 4,
    ):
        self.sender = sender
        self.onm_sender = onm_sender  # Nuevo sender hacia el ONM para cerrar el bucle
        self.fanout = fanout
        self.group_size = max(1, group_size)
        self.wave_delay = max(0.0, wave_delay)

        self.max_queue = max_queue
        self.drop_prio_if_full = drop_prio_if_full
        self.disable_priorities = disable_priorities

        """
            Queue structure:
            Previously it was being used PriorityQueue(), but it was not efficient.
            deque() creates the event queue which uses methods append() and popleft()

            While threading.Lock() protects the queue against concurrent access, a thread
            might insert events while another thread may deque events
        """
        self.q = deque()
        self.q_lock = threading.Lock()

        # NEW: queue for sending events using deque and Condition instead of mp.Queue
        self.send_q = deque()
        self.send_cond = threading.Condition()

        self.latest = {}
        # sequential event counter
        self._seq = 0
        # lock to protect sequential counter
        self._lock = threading.Lock()

        # define NDT targets:
        self.targets = list(range(self.fanout))

        # Creates the Scheduler thread with the main loop
        threading.Thread(target=self._loop, daemon=True).start()

        # EMPAQUETAMOS los parámetros para que coincidan con el worker
        dst_ip = run_ctx.get("dst_ip", "127.0.0.1")
        base_port = run_ctx.get("base_port", 8001)
        params = (dst_ip, base_port, self.fanout)

        # OPTIMIZACIÓN: Bajamos los hilos de 10 a 2 para evitar colisión de GIL en Python
        for _ in range(2):
            t = threading.Thread(
                target=self.send_loop_worker,
                args=(params, self.onm_sender, self.targets, self.group_size, self.wave_delay, self.fanout)
            )
            t.daemon = True
            t.start()

    def submit(self, ev, t_rx_ns: int, src: str, raw_data: bytes = None):
        """
        Function to receive events and prepare them for processing
        """
        # REGISTRAMOS EL 100% DE LOS EVENTOS PARA TENER MÉTRICAS REALES EN EL PAPER
        should_log = True

        pr = ev_priority(ev)
        event_id = ev.get("event_id")

        # Guarantee thet each event has its unique sequence counter
        with self._lock:
            self._seq += 1
            seq = self._seq

        # Read the current queue size
        with self.q_lock:
            qsize_now = len(self.q)

        # ==========================================================
        # POLÍTICA DE DESCARTE (ADMISSION CONTROL)
        # ==========================================================
        if self.max_queue > 0 and qsize_now >= self.max_queue:

            if self.disable_priorities:
                # MODO FIFO ESTRICTO (Tail Drop)
                # La cola está llena -> Bota TODO, sin importar la prioridad
                if should_log:
                    log_event(
                        "DTM_DROP",
                        {
                            "event_id": event_id,
                            "type": ev.get("type"),
                            "priority": pr,
                            "src": src,
                            "reason": "queue_full_fifo_drop",
                            "qsize": qsize_now,
                        },
                    )
                return
            else:
                # MODO GESTIÓN ACTIVA POR PRIORIDAD (QoS)
                # La cola está llena -> Solo bota los paquetes de prioridad baja
                if pr >= self.drop_prio_if_full:
                    if should_log:
                        log_event(
                            "DTM_DROP",
                            {
                                "event_id": event_id,
                                "type": ev.get("type"),
                                "priority": pr,
                                "src": src,
                                "reason": "queue_full_prio_drop",
                                "qsize": qsize_now,
                            },
                        )
                    return

        # Constructs the item to further process it (LA MOCHILA DEL EVENTO)
        item = {"ev": ev, "t_rx_ns": t_rx_ns, "src": src, "priority": pr, "should_log": should_log,
                "raw_data": raw_data}

        """
        Decides how to queue the events. If it's an ON LOGICAL_LINK_SET event 
        avoids duplicated events, if it's another type of event just queues it
        """
        with self.q_lock:
            if ev.get("type") == "LOGICAL_LINK_SET":
                k = link_key(ev)
                self.latest[k] = item
                self.q.append((pr, seq, ("DEDUP", k)))
            else:
                self.q.append((pr, seq, ("EV", item)))

    def _loop(self):
        """
        Loop launched with the thread to continuously deque events
        """
        while True:
            # Batch draining to avoid lock contention
            with self.q_lock:
                if not self.q:
                    batch = []
                else:
                    batch = list(self.q)
                    self.q.clear()

            if not batch:
                time.sleep(0.001)
                continue

            send_batch = []
            for item_q in batch:
                pr, seq, payload = item_q

                if payload[0] == "DEDUP":
                    k = payload[1]
                    item = self.latest.pop(k, None)
                    if not item:
                        continue
                else:
                    item = payload[1]

                # extract event information including deque time
                should_log = item.get("should_log", False)
                ev = item["ev"]
                event_id = ev.get("event_id")
                t_deq_ns = time.time_ns()
                queue_ms = (t_deq_ns - int(item["t_rx_ns"])) / 1e6

                # LOG SILENCIADO: DTM_DEQUEUE. Guardamos en la mochila
                item["queue_ms"] = queue_ms

                # The sender queue is pushed
                t_sendq_in_ns = time.time_ns()
                send_batch.append((ev, item, t_sendq_in_ns, should_log))

            if send_batch:
                with self.send_cond:
                    self.send_q.extend(send_batch)
                    self.send_cond.notify_all()

    # @staticmethod
    def send_loop_worker(self, sender_params, onm_sender, targets, group_size, wave_delay, fanout):
        # Desempaqueta la tupla que creamos arriba
        dst_ip, base_port, fanout_val = sender_params
        worker_sender = UdpSender(dst_ip, base_port, fanout_val)

        """
        This is the main loop for sending the response to
        the NDTs without blocking the main scheduler thread
        """
        while True:
            with self.send_cond:
                while not self.send_q:
                    self.send_cond.wait()
                batch = list(self.send_q)
                self.send_q.clear()

            for res in batch:
                ev, item, t_sendq_in_ns, should_log = res

                # Aseguramos que data sea bytes para el socket sin re-serializar
                data = item.get("raw_data") or json.dumps(ev)
                if not isinstance(data, bytes):
                    data = data.encode()

                t_send_start_ns = time.time_ns()
                sendq_wait_ms = (t_send_start_ns - t_sendq_in_ns) / 1e6
                event_id = ev.get("event_id")

                # LOG SILENCIADO: DTM_SENDQ_DEQUEUE. Guardamos en la mochila
                item["sendq_wait_ms"] = sendq_wait_ms

                # =======================================================
                # EXPERIMENTO 2 (UPSTREAM): NDT FEEDBACK -> DTM -> ONM
                # =======================================================
                if item["src"] == "feedback":
                    prioridad = int(ev.get("priority", 100))

                    # 1. Filtrado: Solo alertamos al ONM si es crítico (ej. <= 20)
                    if prioridad <= 20 and onm_sender:
                        t_send_onm = time.time_ns()
                        try:
                            onm_sender.send(data, [0])
                            if should_log:
                                log_event("DTM_ALERT_TO_ONM", {
                                    "event_id": event_id,
                                    "priority": prioridad,
                                    "latency_to_onm_ns": time.time_ns() - t_send_onm
                                })
                        except Exception:
                            pass

                    # 2. Terminamos el procesamiento sin hacer Broadcast a NDTs
                    t_done_ns = time.time_ns()
                    if should_log:
                        log_event("DTM_EVENT_DONE", {
                            "event_id": event_id,
                            "type": ev.get("type"),
                            "src": item.get("src"),
                            "priority": item.get("priority", 100),
                            "t_rx_ns": item.get("t_rx_ns"),
                            "queue_ms": item.get("queue_ms", 0),
                            "sendq_wait_ms": item.get("sendq_wait_ms", 0),
                            "t_done_ns": t_done_ns
                        })
                    count_done_event(str(event_id))
                    continue  # <-- Detiene el flujo aquí

                # =======================================================
                # EXPERIMENTO 1 (DOWNSTREAM): ONM -> NDTs
                # =======================================================
                if is_critical(ev):
                    t0 = time.time_ns()
                    # USAMOS el worker_sender local
                    sent_ok = worker_sender.send(data, targets)
                    dispatch_ns = time.time_ns() - t0
                    if should_log:
                        log_event(
                            "DTM_BROADCAST",
                            {
                                "event_id": event_id,
                                "fanout": fanout,
                                "sent_ok": sent_ok,
                                "dispatch_ns": dispatch_ns,
                            },
                        )
                else:
                    wave_id = 0
                    for i in range(0, fanout, group_size):
                        # events are sent serially to the NDTs
                        grp = targets[i: i + group_size]
                        t0 = time.time_ns()
                        try:
                            # USAMOS el worker_sender local
                            sent_ok = worker_sender.send(data, grp)
                        except Exception as e:
                            log_event("DTM_SEND_ERR", {"event_id": event_id, "err": str(e)})
                            continue
                        dispatch_ns = time.time_ns() - t0
                        if should_log:
                            log_event(
                                "DTM_ROLLING_SEND",
                                {
                                    "event_id": event_id,
                                    "fanout": fanout,
                                    "wave_id": wave_id,
                                    "group_size": len(grp),
                                    "sent_ok": sent_ok,
                                    "dispatch_ns": dispatch_ns,
                                },
                            )
                        wave_id += 1
                        if wave_delay > 0:
                            time.sleep(wave_delay)

                # Logging DONE events.
                t_done_ns = time.time_ns()
                if should_log:
                    log_event(
                        "DTM_EVENT_DONE",
                        {
                            "event_id": event_id,
                            "type": ev.get("type"),
                            "src": item.get("src"),
                            "priority": item.get("priority", 100),
                            "t_rx_ns": item.get("t_rx_ns"),
                            "queue_ms": item.get("queue_ms", 0),
                            "sendq_wait_ms": item.get("sendq_wait_ms", 0),
                            "t_done_ns": t_done_ns
                        },
                    )

                # NEW: count completion for queued (feedback) events
                count_done_event(str(event_id))


# ============================================================
# MAIN
# ============================================================
def main():
    """
    Here are defined the arguments that characterize the behavior of DTM:
    1. proto: TCP or UDP
    2. IP address of the relay node which communicates the ONM and the DTM
    3. Port in which the ONM and DTM are connected through the relay
    4. Mode: 1:1 -> No federation in which the DTM applies
        the events locally in the NDT, federated -> 1:N in which the DTM forwards the ON
        events to the NDTs
    5. In 1:N is the number of NDT instances that the DTM must handle.
    6. Port to forward the ON events to the NDTs
    7. IP of the VM which deploys the NDTs: 192.168.99.2
    8. Defines the number of NDT groups to transmit the events. Burst send.
    9.
    10. Port in which the DTM receives the NDT events
    11. Queue size. If no queue is defined, queue has no limit
    12. Queue drop policy based on NDT event priorities.
    13. Force to enqueue ONM events.
    14. Path where the DTM log file will be saved
    15.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--proto", required=True, choices=["udp", "tcp"])
    ap.add_argument("--relay-ip", required=True)
    ap.add_argument("--relay-port", type=int, default=7001)

    # NUEVO: Puerto para reenviar telemetría crítica de vuelta al ONM
    ap.add_argument("--onm-port", type=int, default=7002)

    ap.add_argument("--mode", choices=["1:1", "federated"], default="1:1")
    # federated downstream fanout
    ap.add_argument("--fanout", type=int, default=0)
    ap.add_argument("--base-port", type=int, default=8001)
    ap.add_argument("--dst-ip", default="127.0.0.1")
    ap.add_argument("--group-size", type=int, default=5)
    ap.add_argument("--wave-delay", type=float, default=0.2)
    # feedback
    ap.add_argument("--feedback-port", type=int, default=9000)
    # queue/drop tuning (optional)
    ap.add_argument(
        "--max-queue",
        type=int,
        default=0,
        help="0 disables queue limit. If >0, drop policy can trigger when full.",
    )
    ap.add_argument(
        "--drop-prio-if-full",
        type=int,
        default=60,
        help="If queue full, drop events with priority >= this value (lower is higher priority).",
    )

    # ========================================================
    # NUEVO PARÁMETRO: DESACTIVAR PRIORIDADES (MODO FIFO)
    # ========================================================
    ap.add_argument(
        "--disable-priorities",
        action="store_true",
        help="Si se activa, el DTM ignora las prioridades y aplica estricto Tail Drop (FIFO) cuando la cola se llena.",
    )

    # keep functionality: if you ever want to enqueue ONM events again
    ap.add_argument(
        "--enqueue-onm",
        action="store_true",
        help="If set, ONM events also go through the controller queue (old behavior). Default: OFF.",
    )
    ap.add_argument("--log-file", default=log_file_default)

    # NEW: deterministic end
    ap.add_argument(
        "--event-limit",
        type=int,
        default=0,
        help="If >0, exit after this many UNIQUE event_ids are completed.",
    )
    ap.add_argument(
        "--require-full-fanout",
        action="store_true",
        help="Only count ONM fast-path events as completed if sent_ok == fanout.",
    )
    ap.add_argument(
        "--print-progress",
        action="store_true",
        help="Print done count while running (debug).",
    )

    # NEW: deterministic experiment duration
    ap.add_argument(
        "--experiment-duration",
        type=float,
        default=0,
        help="If >0, the DTM will automatically stop after this many seconds.",
    )

    # Arguments are parsed
    args = ap.parse_args()

    # Set the parsed arguments in run_ctx global variable which will be used through all the code
    global run_ctx
    run_ctx = vars(args)

    # ============================================================
    # Start the logger
    # ============================================================
    threading.Thread(target=log_writer, daemon=True).start()

    # ============================================================
    # NEW: deterministic experiment timer
    # ============================================================
    if args.experiment_duration > 0:
        def stop_after_duration():
            print(
                f"[DTM] experiment timer started ({args.experiment_duration}s)",
                flush=True,
            )
            time.sleep(args.experiment_duration)
            print("[DTM] experiment duration reached. Stopping.", flush=True)
            stop_event.set()

        # Once the DTM is launched, starts a thread with the experiment
        # duration to stop the DTM.
        threading.Thread(target=stop_after_duration, daemon=True).start()

    gc.disable()

    # Loads the DTM adjacency matrix (Just in 1:1 Mode)
    if args.mode == "1:1":
        g = load_topo_graph()
    else:
        g = None

    # Defines the receptor which can be either TCP and UDP for ONM
    rx = (
        UdpReceiver(args.relay_ip, args.relay_port)
        if args.proto == "udp"
        else TcpReceiver(args.relay_ip, args.relay_port)
    )

    # Initializes the controllers
    controller = None
    sender = None
    onm_sender = None

    if args.mode == "federated":
        if args.fanout <= 0:
            raise SystemExit("ERROR: federated mode requires --fanout > 0")

        # Defines the sender UDP or TCP to forward ON events to the NDTs
        sender = (
            UdpSender(args.dst_ip, args.base_port, args.fanout)
            if args.proto == "udp"
            else TcpSender(args.dst_ip, args.base_port, args.fanout)
        )

        # Sender hacia el ONM para el bucle cerrado
        onm_sender = (
            UdpSender(args.relay_ip, args.onm_port, 1)
            if args.proto == "udp"
            else TcpSender(args.relay_ip, args.onm_port, 1)
        )

        # Federated controller is the DTM scheduler in the federated approach
        # to handle events received from the NDTs
        controller = FederatedController(
            sender=sender,
            onm_sender=onm_sender,
            fanout=args.fanout,
            group_size=args.group_size,
            wave_delay=args.wave_delay,
            max_queue=args.max_queue,
            drop_prio_if_full=args.drop_prio_if_full,
            disable_priorities=args.disable_priorities,
        )

        # It is the interface for receiving the NDT events
        FeedbackReceiver(args.feedback_port, controller)
        print(f"[NDT] Federated mode (fanout={args.fanout}, Disable Priorities={args.disable_priorities})", flush=True)

    print(f"[NDT] Ready ({args.proto.upper()}, mode={args.mode})", flush=True)

    while True:
        # Check if the limit of the experiment has been reached
        limit_reached()
        # Extract the ONM event data
        data = rx.recv()
        if not data:
            continue
        t_rx_ns = time.time_ns()

        try:
            ev = json.loads(data.decode() if isinstance(data, bytes) else data)
        except Exception as e:
            log_event("NDT_RX_PARSE_ERR", {"err": str(e), "size_bytes": len(data)})
            continue

        event_id = ev.get("event_id")
        ev_type = ev.get("type", "NA")

        log_event(
            "NDT_EVENT_RX",
            {
                "event_id": event_id,
                "type": ev_type,
                "t_rx_ns": t_rx_ns,
                "size_bytes": len(data),
            },
        )

        if args.mode == "1:1":
            # locally apply the events in the mininet NDT
            if ev_type == "LOGICAL_LINK_SET":
                p = ev.get("payload", {})
                try:
                    e = Edge(
                        delay_ms=float(p["delay_ms"]),
                        loss_pct=float(p["loss_pct"]),
                        bw_mbps=float(p["bw_mbps"]),
                        up=bool(p["up"]),
                    )
                except Exception as ex:
                    log_event(
                        "NDT_EVENT_BAD_PAYLOAD", {"event_id": event_id, "err": str(ex)}
                    )
                    continue

                segments = apply_logical_link(g, p.get("a"), p.get("b"), e)
                t_apply_ns = time.time_ns()

                log_event(
                    "NDT_EVENT_APPLIED",
                    {
                        "event_id": event_id,
                        "type": ev_type,
                        "t_rx_ns": t_rx_ns,
                        "t_apply_ns": t_apply_ns,
                        "segments": segments,
                        "a": p.get("a"),
                        "b": p.get("b"),
                        "up": e.up,
                    },
                )

                print(f"[NDT] EVENT APPLIED {event_id}", flush=True)

                # NEW: for 1:1, consider completion after applied
                count_done_event(str(event_id))
                limit_reached()

            elif ev_type == "EXEC_CMD":
                resp = control_call(ev)
                t_apply_ns = time.time_ns()

                log_event(
                    "NDT_EXEC_CMD",
                    {
                        "event_id": event_id,
                        "t_rx_ns": t_rx_ns,
                        "t_apply_ns": t_apply_ns,
                        "ok": resp.get("ok", False),
                    },
                )

                print(f"[NDT] EXEC APPLIED {event_id}", flush=True)

                # NEW: completion after exec applied
                count_done_event(str(event_id))
                limit_reached()

            else:
                # Unrecognized event
                log_event(
                    "NDT_UNKNOWN_EVENT",
                    {"event_id": event_id, "type": ev_type, "size_bytes": len(data)},
                )
        else:
            """
            FEDERATED MODE
              - ONM events: fast-dispatch only (2 logs: RX + DISPATCHED)
              - Feedback: goes to queue (handled in FeedbackReceiver)
            """
            if args.enqueue_onm:
                # Added the option to queue ON events
                controller.submit(ev, t_rx_ns, src="onm")
                print(f"[NDT] EVENT QUEUED {event_id}", flush=True)
            else:
                # Fast path: send ONM events immediately to NDTs
                if sender is None:
                    log_event("NDT_FED_NO_SENDER", {"event_id": event_id})
                    continue

                t0 = time.time_ns()
                # send the event to the NDTs
                sent_ok, dispatch_ns = fast_dispatch_to_fanout(
                    sender=sender,
                    fanout=args.fanout,
                    group_size=args.group_size,
                    wave_delay=args.wave_delay,
                    ev=ev,
                )
                t1 = time.time_ns()

                log_event(
                    "NDT_EVENT_DISPATCHED",
                    {
                        "event_id": event_id,
                        "type": ev_type,
                        "fanout": args.fanout,
                        "sent_ok": sent_ok,
                        "dispatch_ns": dispatch_ns,
                        "t_dispatch_start_ns": t0,
                        "t_dispatch_end_ns": t1,
                    },
                )

                # print(f"[NDT] EVENT DISPATCHED {event_id}", flush=True)

                # NEW: count completion for ONM fast-path events
                # Default (require_full_fanout=False): any real send (sent_ok>0) counts.
                if args.require_full_fanout:
                    count_done_event(
                        str(event_id),
                        require_full_fanout=True,
                        sent_ok=int(sent_ok),
                        fanout=int(args.fanout),
                    )
                else:
                    # "envío real": sent_ok > 0
                    if int(sent_ok) > 0:
                        count_done_event(str(event_id))

                limit_reached()


if __name__ == "__main__":
    main()