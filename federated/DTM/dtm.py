"""
DTM — Digital Twin Manager (deterministic / reproducible build)

Threading model
---------------
Main thread   : relay RX → fast-dispatch (federated) or local apply (1:1)
log-writer    : async JSONL writer (daemon)
fb-rx         : single UDP recv thread for NDT feedback
fb-dispatch   : single dispatch thread — drains AdmissionQueue, logs, forwards

Rate limiting — TokenBucket window pacer
-----------------------------------------
Window = 8ms → batch = floor(C * 0.008) events per window.
  C=2500 → batch=20, recommended --max-queue=25 (batch + 5 margin)
  C=2000 → batch=16, recommended --max-queue=21
  C=3000 → batch=24, recommended --max-queue=29

The dispatcher processes up to `batch` events per window then sleeps 8ms.
sleep(8ms) is well within Linux scheduler resolution and releases the GIL
so fb-rx runs freely during the sleep.

Why this works with the original ndt_sim (no batch mode):
  Each NDT sends 1 event per period independently. With 10 NDTs at 250 ev/s
  each, events arrive spread across the 8ms window (~2 per ms on average),
  not in a burst. The queue stays well below max_queue during normal operation.

Why Poisson gets more drops than Uniform:
  Poisson arrivals cluster randomly — sometimes 8-10 events arrive in 1ms,
  filling the queue. Uniform arrivals are spread evenly, queue stays low.
  With max_queue=25 and batch=20, Poisson queue overflows more often.
  Result: Poisson drop% > Uniform drop% at the same offered load. ✓
"""

import gc
import os
import sys
import time
import math
import socket
import argparse
import threading
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

try:
    import orjson

    def _dumps(obj: object) -> bytes:
        return orjson.dumps(obj)

    def _loads(data: bytes) -> dict:
        return orjson.loads(data)

except ImportError:
    sys.exit(
        "[FATAL] orjson is required.\n"
        "Install: pip install orjson --break-system-packages"
    )

TOPO_FILE        = "/run/topology.json"
CONTROL_HOST     = "127.0.0.1"
CONTROL_PORT     = 6001
LOG_FILE_DEFAULT = "/home/ubuntu/ndtm_events.log"

run_ctx: Dict[str, object] = {}

_done_ids: set = set()
_done_lock      = threading.Lock()
stop_event      = threading.Event()


def count_done_event(event_id: str) -> None:
    limit = int(run_ctx.get("event_limit", 0) or 0)
    if limit <= 0 or not event_id:
        return
    with _done_lock:
        if event_id in _done_ids:
            return
        _done_ids.add(event_id)
        n = len(_done_ids)
    if run_ctx.get("print_progress") and n % 500 == 0:
        print(f"[DTM] done {n}/{limit}", flush=True)
    if n >= limit:
        stop_event.set()


def _flush_and_exit(reason: str) -> None:
    print(f"\n[DTM] {reason}. Flushing logs...", flush=True)
    _log_stop.set()
    deadline = time.monotonic() + 6.0
    while _log_deque and time.monotonic() < deadline:
        time.sleep(0.05)
    print("[DTM] Logs flushed. Exiting.", flush=True)
    gc.enable()
    os._exit(0)


def limit_reached() -> None:
    if stop_event.is_set():
        _flush_and_exit("Experiment limit reached")


_log_deque: deque      = deque()
_log_stop              = threading.Event()
_log_ctx_prefix: bytes = b""


def _build_log_prefix() -> None:
    global _log_ctx_prefix
    base = _dumps(run_ctx)
    _log_ctx_prefix = base[:-1]


def _log_writer() -> None:
    path = str(run_ctx.get("log_file", LOG_FILE_DEFAULT))
    tmp: deque = deque()
    with open(path, "wb") as f:
        while not _log_stop.is_set() or _log_deque:
            if _log_deque:
                tmp.clear()
                while _log_deque:
                    tmp.append(_log_deque.popleft())
                f.write(b"\n".join(tmp) + b"\n")
                f.flush()
            else:
                time.sleep(0.01)


def log_event(tag: str, data: dict) -> None:
    try:
        event_bytes = _dumps({"ts_ns": time.time_ns(), "tag": tag, **data})
        merged = _log_ctx_prefix + b"," + event_bytes[1:]
        _log_deque.append(merged)
    except Exception:
        pass


def recv_exact(sock: socket.socket, n: int) -> Optional[bytes]:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


@dataclass
class Edge:
    delay_ms: float
    loss_pct: float
    bw_mbps:  float
    up:       bool


def is_switch(n: str) -> bool:
    return n.startswith("s")


def load_topo_graph() -> Dict[str, List[str]]:
    with open(TOPO_FILE) as f:
        t = orjson.loads(f.read())
    g: Dict[str, List[str]] = defaultdict(list)
    for a, b in t["links"]:
        g[a].append(b)
        g[b].append(a)
    return g


def path_only_switches_middle(
    g: Dict[str, List[str]], src: str, dst: str
) -> List[Tuple[str, str]]:
    q: deque = deque([src])
    prev: Dict[str, Optional[str]] = {src: None}
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
    path: List[str] = []
    cur = dst
    while cur is not None:
        path.append(cur)
        cur = prev[cur]  # type: ignore[assignment]
    path.reverse()
    if any(not is_switch(x) for x in path[1:-1]):
        return []
    return [(path[i], path[i + 1]) for i in range(len(path) - 1)]


def control_call(ev: dict) -> dict:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((CONTROL_HOST, CONTROL_PORT))
        s.sendall(_dumps(ev) + b"\n")
        resp = b""
        while b"\n" not in resp:
            chunk = s.recv(4096)
            if not chunk:
                break
            resp += chunk
    return _loads(resp.split(b"\n")[0]) if resp else {"ok": False}


def apply_logical_link(g: Dict[str, List[str]], a: str, b: str, e: Edge) -> int:
    phys = path_only_switches_middle(g, a, b)
    if not phys:
        return 0
    for x, y in phys:
        control_call({"type": "LINK_SET", "payload": {"a": x, "b": y, **e.__dict__}})
    control_call({
        "type":    "ROUTE_REASSERT",
        "payload": {"routers": ["r1", "r2"], "gateway": "10.0.0.254"},
    })
    return len(phys)


class UdpRelayReceiver:
    def __init__(self, relay_ip: str, relay_port: int) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024 * 1024)
        self._sock.settimeout(0.5)
        self._sock.bind(("0.0.0.0", relay_port))
        self._relay = (relay_ip, relay_port)
        threading.Thread(target=self._keepalive, daemon=True).start()

    def _keepalive(self) -> None:
        while True:
            try:
                self._sock.sendto(b"HELLO_NDT", self._relay)
            except Exception:
                pass
            time.sleep(5)

    def recv(self) -> bytes:
        try:
            return self._sock.recvfrom(65535)[0]
        except socket.timeout:
            return b""


class TcpRelayReceiver:
    def __init__(self, relay_ip: str, relay_port: int) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self._sock.settimeout(0.5)
        self._sock.connect((relay_ip, relay_port))
        self._sock.sendall(b"NDT\n")

    def recv(self) -> bytes:
        try:
            hdr = recv_exact(self._sock, 4)
            if not hdr:
                return b""
            return recv_exact(self._sock, int.from_bytes(hdr, "big")) or b""
        except socket.timeout:
            return b""


class UdpFanoutSender:
    def __init__(self, dst_ip: str, base_port: int, fanout: int) -> None:
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 8 * 1024 * 1024)
        self._targets = [(dst_ip, base_port + i) for i in range(fanout)]

    def send_all(self, payload: bytes, idxs: List[int]) -> int:
        ok = 0
        for i in idxs:
            try:
                self._sock.sendto(payload, self._targets[i])
                ok += 1
            except Exception:
                pass
        return ok


class TcpFanoutSender:
    def __init__(self, dst_ip: str, base_port: int, fanout: int) -> None:
        self._ip    = dst_ip
        self._ports = [base_port + i for i in range(fanout)]
        self._socks: Dict[int, socket.socket] = {}

    def _get(self, port: int) -> Optional[socket.socket]:
        if port in self._socks:
            return self._socks[port]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.connect((self._ip, port))
            self._socks[port] = s
            return s
        except Exception:
            return None

    def send_all(self, payload: bytes, idxs: List[int]) -> int:
        frame = len(payload).to_bytes(4, "big") + payload
        ok = 0
        for i in idxs:
            s = self._get(self._ports[i])
            if not s:
                continue
            try:
                s.sendall(frame)
                ok += 1
            except Exception:
                self._socks.pop(self._ports[i], None)
        return ok


# ---------------------------------------------------------------------------
# TokenBucket — 8ms window batch pacer
#
# window=8ms chosen because:
#   - sleep(8ms) is reliable on Linux/VirtualBox (well above 1ms min)
#   - C=2500 → batch=20, fits in queue=25 with 5 slots margin
#   - GIL released ~125x/second for 8ms each → fb-rx runs freely
#
# Usage: --max-rate 2500 --max-queue 25
# ---------------------------------------------------------------------------
class TokenBucket:
    WINDOW_S = 0.008  # 8ms window

    def __init__(self, rate: float) -> None:
        self._rate              = rate
        self._tokens_per_window = max(1, math.floor(rate * self.WINDOW_S))
        self._window_end        = time.perf_counter() + self.WINDOW_S
        self._remaining         = self._tokens_per_window
        print(
            f"[DTM] TokenBucket: {rate:.0f} ev/s | "
            f"window={self.WINDOW_S*1000:.0f}ms | "
            f"batch={self._tokens_per_window} ev/window | "
            f"recommended --max-queue={self._tokens_per_window + 5}",
            flush=True,
        )

    def next_batch(self) -> int:
        return self._remaining

    def consume(self, n: int) -> None:
        self._remaining -= n

    def wait_window_end(self) -> None:
        gap = self._window_end - time.perf_counter()
        if gap > 0:
            time.sleep(gap)
        self._window_end += self.WINDOW_S
        if self._window_end < time.perf_counter():
            self._window_end = time.perf_counter() + self.WINDOW_S
        self._remaining = self._tokens_per_window


class AdmissionQueue:
    def __init__(
        self,
        max_queue:          int,
        drop_prio_if_full:  int,
        disable_priorities: bool,
        no_queue:           bool = False,
    ) -> None:
        self._max       = max_queue
        self._drop_prio = drop_prio_if_full
        self._fifo      = disable_priorities
        self._no_queue  = no_queue
        self._q: deque  = deque()
        self._lock      = threading.Lock()
        self._cond      = threading.Condition(self._lock)
        self._qsize     = 0
        self._dispatch_busy = False

    def set_busy(self, busy: bool) -> None:
        self._dispatch_busy = busy

    def push(self, item: dict, priority: int,
             event_id: str, ev_type: str, src: str) -> bool:
        drop_reason: Optional[str] = None
        with self._lock:
            if self._no_queue:
                if self._dispatch_busy or self._qsize > 0:
                    drop_reason = "no_queue_busy"
            elif self._max > 0 and self._qsize >= self._max:
                if self._fifo or priority >= self._drop_prio:
                    drop_reason = "tail_drop" if self._fifo else "prio_drop"
            if drop_reason is None:
                self._q.append(item)
                self._qsize += 1
                self._cond.notify()
        if drop_reason is not None:
            log_event("DTM_DROP", {
                "event_id": event_id,
                "type":     ev_type,
                "priority": priority,
                "src":      src,
                "reason":   drop_reason,
                "qsize":    self._qsize,
            })
            return False
        return True

    def drain(self, max_items: int = 2000) -> List[dict]:
        with self._lock:
            while not self._q:
                self._cond.wait(timeout=0.0005)
            batch: List[dict] = []
            while self._q and len(batch) < max_items:
                batch.append(self._q.popleft())
                self._qsize -= 1
        return batch

    def drain_up_to(self, max_items: int) -> List[dict]:
        with self._lock:
            batch: List[dict] = []
            while self._q and len(batch) < max_items:
                batch.append(self._q.popleft())
                self._qsize -= 1
        return batch


class FeedbackController:
    def __init__(
        self,
        listen_port:  int,
        onm_sender:   Optional[UdpFanoutSender],
        aq:           AdmissionQueue,
        token_bucket: Optional[TokenBucket] = None,
    ) -> None:
        self._aq   = aq
        self._onm  = onm_sender
        self._tb   = token_bucket
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 64 * 1024 * 1024)
        self._sock.bind(("0.0.0.0", listen_port))
        threading.Thread(target=self._rx_loop,       name="fb-rx",       daemon=True).start()
        threading.Thread(target=self._dispatch_loop, name="fb-dispatch", daemon=True).start()
        print(f"[DTM] FeedbackController listening on UDP {listen_port}", flush=True)

    def _rx_loop(self) -> None:
        while True:
            try:
                data, _ = self._sock.recvfrom(65535)
            except Exception:
                continue
            t_rx_ns = time.time_ns()
            try:
                ev = _loads(data)
            except Exception:
                continue
            event_id = str(ev.get("event_id", ""))
            ev_type  = ev.get("type", "NA")
            pr       = int(ev.get("priority", 100))
            log_event("NDT_EVENT_RX", {
                "event_id":   event_id,
                "type":       ev_type,
                "t_rx_ns":    t_rx_ns,
                "size_bytes": len(data),
            })
            self._aq.push(
                {"ev": ev, "raw": data, "t_rx_ns": t_rx_ns,
                 "priority": pr, "event_id": event_id},
                pr, event_id, ev_type, "feedback",
            )

    def _process_item(self, item: dict) -> None:
        ev          = item["ev"]
        event_id    = item["event_id"]
        pr          = item["priority"]
        t_rx_ns     = item["t_rx_ns"]
        data: bytes = item["raw"]

        if pr <= 20 and self._onm:
            t0 = time.time_ns()
            try:
                self._onm.send_all(data, [0])
                log_event("DTM_ALERT_TO_ONM", {
                    "event_id":          event_id,
                    "priority":          pr,
                    "latency_to_onm_ns": time.time_ns() - t0,
                })
            except Exception:
                pass

        t_done_ns = time.time_ns()
        log_event("DTM_EVENT_DONE", {
            "event_id":  event_id,
            "type":      ev.get("type"),
            "src":       "feedback",
            "priority":  pr,
            "t_rx_ns":   t_rx_ns,
            "queue_ms":  (t_done_ns - t_rx_ns) / 1e6,
            "t_done_ns": t_done_ns,
        })
        count_done_event(event_id)
        limit_reached()

    def _dispatch_loop(self) -> None:
        tb = self._tb
        if tb is None:
            while True:
                for item in self._aq.drain():
                    if self._aq._no_queue:
                        self._aq.set_busy(True)
                    self._process_item(item)
                    if self._aq._no_queue:
                        self._aq.set_busy(False)
        else:
            while True:
                allowed    = tb.next_batch()
                items      = self._aq.drain(max_items=allowed)
                dispatched = 0
                for item in items:
                    self._process_item(item)
                    dispatched += 1
                tb.consume(dispatched)
                remaining = allowed - dispatched
                if remaining > 0:
                    for item in self._aq.drain_up_to(remaining):
                        self._process_item(item)
                        dispatched += 1
                tb.wait_window_end()


def fast_dispatch(
    sender,
    fanout:     int,
    group_size: int,
    wave_delay: float,
    ev:         dict,
    payload:    bytes,
) -> Tuple[int, int]:
    is_crit = (
        ev.get("type") == "LOGICAL_LINK_SET"
        and not ev.get("payload", {}).get("up", True)
    )
    targets     = list(range(fanout))
    sent_ok     = 0
    dispatch_ns = 0
    if is_crit:
        t0          = time.time_ns()
        sent_ok     = sender.send_all(payload, targets)
        dispatch_ns = time.time_ns() - t0
    else:
        gs = max(1, group_size)
        for i in range(0, fanout, gs):
            t0           = time.time_ns()
            sent_ok     += sender.send_all(payload, targets[i: i + gs])
            dispatch_ns += time.time_ns() - t0
            if wave_delay > 0:
                time.sleep(wave_delay)
    return sent_ok, dispatch_ns


def main() -> None:
    ap = argparse.ArgumentParser(description="Digital Twin Manager")
    ap.add_argument("--proto",       required=True, choices=["udp", "tcp"])
    ap.add_argument("--relay-ip",    required=True)
    ap.add_argument("--relay-port",  type=int, default=7001)
    ap.add_argument("--onm-port",    type=int, default=7002)
    ap.add_argument("--mode",        choices=["1:1", "federated"], default="1:1")
    ap.add_argument("--fanout",      type=int, default=0)
    ap.add_argument("--base-port",   type=int, default=8001)
    ap.add_argument("--dst-ip",      default="127.0.0.1")
    ap.add_argument("--group-size",  type=int, default=5)
    ap.add_argument("--wave-delay",  type=float, default=0.0)
    ap.add_argument("--feedback-port", type=int, default=9000)
    ap.add_argument("--max-queue",   type=int, default=0)
    ap.add_argument("--drop-prio-if-full", type=int, default=60)
    ap.add_argument("--disable-priorities", action="store_true")
    ap.add_argument("--no-queue",    action="store_true")
    ap.add_argument("--max-rate",    type=float, default=0.0,
                    help="Hard rate cap C ev/s (0=disabled)")
    ap.add_argument("--enqueue-onm", action="store_true")
    ap.add_argument("--log-file",    default=LOG_FILE_DEFAULT)
    ap.add_argument("--event-limit", type=int, default=0)
    ap.add_argument("--print-progress", action="store_true")
    ap.add_argument("--experiment-duration", type=float, default=0)
    args = ap.parse_args()

    global run_ctx
    run_ctx = {
        "side":               "NDT",
        "proto":              args.proto,
        "mode":               args.mode,
        "relay_ip":           args.relay_ip,
        "relay_port":         args.relay_port,
        "fanout":             args.fanout,
        "base_port":          args.base_port,
        "dst_ip":             args.dst_ip,
        "max_queue":          args.max_queue,
        "disable_priorities": args.disable_priorities,
        "log_file":           args.log_file,
        "event_limit":        args.event_limit,
        "print_progress":     args.print_progress,
    }

    threading.Thread(target=_log_writer, name="log-writer", daemon=True).start()
    _build_log_prefix()

    if args.experiment_duration > 0:
        def _timer() -> None:
            print(f"[DTM] Timer: {args.experiment_duration}s", flush=True)
            time.sleep(args.experiment_duration)
            stop_event.set()
            _flush_and_exit("Experiment duration reached")
        threading.Thread(target=_timer, name="exp-timer", daemon=True).start()

    gc.disable()

    g = load_topo_graph() if args.mode == "1:1" else None

    rx = (
        UdpRelayReceiver(args.relay_ip, args.relay_port)
        if args.proto == "udp"
        else TcpRelayReceiver(args.relay_ip, args.relay_port)
    )

    fast_sender = None

    if args.mode == "federated":
        if args.fanout <= 0:
            raise SystemExit("ERROR: --fanout > 0 required in federated mode")

        fast_sender = (
            UdpFanoutSender(args.dst_ip, args.base_port, args.fanout)
            if args.proto == "udp"
            else TcpFanoutSender(args.dst_ip, args.base_port, args.fanout)
        )

        onm_sender = UdpFanoutSender(args.relay_ip, args.onm_port, 1)
        aq = AdmissionQueue(
            max_queue=args.max_queue,
            drop_prio_if_full=args.drop_prio_if_full,
            disable_priorities=args.disable_priorities,
            no_queue=args.no_queue,
        )

        if args.no_queue:
            print("[DTM] Mode: NO-QUEUE — drop immediately if busy", flush=True)
        elif args.max_queue > 0:
            print(f"[DTM] Mode: BOUNDED QUEUE — max_queue={args.max_queue}", flush=True)
        else:
            print("[DTM] Mode: UNLIMITED QUEUE — no drops", flush=True)

        tb = TokenBucket(args.max_rate) if args.max_rate > 0 else None

        FeedbackController(
            listen_port=args.feedback_port,
            onm_sender=onm_sender,
            aq=aq,
            token_bucket=tb,
        )
        print(
            f"[NDT] Federated | fanout={args.fanout} "
            f"max_queue={args.max_queue} "
            f"disable_priorities={args.disable_priorities}",
            flush=True,
        )

    print(f"[DTM] Ready ({args.proto.upper()}, mode={args.mode})", flush=True)

    while True:
        limit_reached()
        data = rx.recv()
        if not data:
            continue
        t_rx_ns = time.time_ns()
        try:
            ev = _loads(data)
        except Exception as exc:
            log_event("NDT_RX_PARSE_ERR", {"err": str(exc), "size_bytes": len(data)})
            continue

        event_id = str(ev.get("event_id", ""))
        ev_type  = ev.get("type", "NA")

        log_event("NDT_EVENT_RX", {
            "event_id":   event_id,
            "type":       ev_type,
            "t_rx_ns":    t_rx_ns,
            "size_bytes": len(data),
        })

        if args.mode == "1:1":
            if ev_type == "LOGICAL_LINK_SET":
                p = ev.get("payload", {})
                try:
                    e = Edge(
                        delay_ms=float(p["delay_ms"]),
                        loss_pct=float(p["loss_pct"]),
                        bw_mbps= float(p["bw_mbps"]),
                        up=      bool(p["up"]),
                    )
                except Exception as exc:
                    log_event("NDT_EVENT_BAD_PAYLOAD",
                              {"event_id": event_id, "err": str(exc)})
                    continue
                segs    = apply_logical_link(g, p.get("a"), p.get("b"), e)
                t_apply = time.time_ns()
                log_event("NDT_EVENT_APPLIED", {
                    "event_id":   event_id,
                    "type":       ev_type,
                    "t_rx_ns":    t_rx_ns,
                    "t_apply_ns": t_apply,
                    "segments":   segs,
                    "a":          p.get("a"),
                    "b":          p.get("b"),
                    "up":         e.up,
                })
                count_done_event(event_id)
                limit_reached()
            elif ev_type == "EXEC_CMD":
                resp    = control_call(ev)
                t_apply = time.time_ns()
                log_event("NDT_EXEC_CMD", {
                    "event_id":   event_id,
                    "t_rx_ns":    t_rx_ns,
                    "t_apply_ns": t_apply,
                    "ok":         resp.get("ok", False),
                })
                count_done_event(event_id)
                limit_reached()
            else:
                log_event("NDT_UNKNOWN_EVENT", {
                    "event_id":   event_id,
                    "type":       ev_type,
                    "size_bytes": len(data),
                })
        else:
            assert fast_sender is not None
            t0 = time.time_ns()
            sent_ok, dispatch_ns = fast_dispatch(
                sender=fast_sender,
                fanout=args.fanout,
                group_size=args.group_size,
                wave_delay=args.wave_delay,
                ev=ev,
                payload=data,
            )
            t1 = time.time_ns()
            log_event("NDT_EVENT_DISPATCHED", {
                "event_id":            event_id,
                "type":                ev_type,
                "fanout":              args.fanout,
                "sent_ok":             sent_ok,
                "dispatch_ns":         dispatch_ns,
                "t_dispatch_start_ns": t0,
                "t_dispatch_end_ns":   t1,
            })
            if int(sent_ok) > 0:
                count_done_event(event_id)
            limit_reached()


if __name__ == "__main__":
    main()