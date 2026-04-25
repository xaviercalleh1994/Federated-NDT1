"""
Authors: Xavier Calle-Heredia & Xavier Hesselbach
Date: 2026-04-08

DTM — Digital Twin Manager

Overview
--------
The DTM coordinates multiple Network Digital Twin (NDT) instances in a
federated 1:N deployment. It operates in two modes:

  1:1 mode      — Manages a single local NDT (built with Mininet). Receives
                  topology events from the Original Network Manager (ONM) via
                  a relay node and applies them to the local Mininet-based NDT.

  federated mode — Does NOT manage a local NDT. Instead, it receives upstream
                  feedback events from N emulated NDT instances, processes
                  them through an admission queue, and forwards them to the ONM.
                  This mode is used for Experiment 2 (upstream load analysis).

  federated mode (Exp. 1) — Same as federated but NDTs are real Mininet
                  instances on a second machine. The ONM sends a TOPOLOGY
                  event as its first message; the DTM uses it to resolve
                  LOGICAL_LINK_SET into physical LINK_SET segments before
                  fanning out. Each NDT sends a UDP ACK after applying;
                  DTM logs DTM_EVENT_COMPLETE when all N ACKs arrive.

Threading model
---------------
  Main thread   : Receives events from the relay (ONM→DTM direction).
                  In 1:1 mode: applies topology changes to the local NDT.
                  In federated mode: dispatches events to NDT instances.
  log-writer    : Daemon thread. Drains the log deque and writes to disk
                  asynchronously so that the main thread and fb-rx are never
                  blocked by I/O.
  fb-rx         : Single UDP receive thread for NDT upstream feedback.
                  Parses incoming JSON events and pushes them to the
                  AdmissionQueue.
  fb-dispatch   : Single dispatch thread. Drains the AdmissionQueue, logs
                  processing latency, and forwards critical events to the ONM.
                  If a TokenBucket is configured, paces dispatch to C ev/s.
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
import orjson


def _dumps(obj: object) -> bytes:
    """Serialize Py objects to JSON with orjson which is
     faster than json"""
    return orjson.dumps(obj)

def _loads(data: bytes) -> dict:
    """Deserialize JSON bytes to a Python dict using orjson."""
    return orjson.loads(data)


########## Variables used for running DTM in mode 1:1 ############
# Contains the list of mininet nodes, used by the DTM to find logical paths
TOPO_FILE        = "/run/topology.json"
# HOST and PORT: to locally apply modifications in the mininet NDT
CONTROL_HOST     = "127.0.0.1"
CONTROL_PORT     = 6001
##################################################################
LOG_FILE_DEFAULT = "/home/ubuntu/ndtm_events.log"
# ctx to save the context of the variables used to run the DTM
run_ctx: Dict[str, object] = {}

# variable to cound complete events
_done_ids: set = set()
# variable to protect events from concurrent access
_done_lock      = threading.Lock()
stop_event      = threading.Event()


def count_done_event(event_id: str) -> None:
    """
    Optional function --> requires event_limit active. It is used to count
    the number of events that have been completed.
    """
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
    """
    Function to flush pending logs and exit cleanly the program.
    First _log_stop to stop the log writer thread, then waits 6 seconds to
    deque to pending events
    """
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


# Used for async loging
_log_deque: deque      = deque()
# Used to block the writing of new logs
_log_stop              = threading.Event()
_log_ctx_prefix: bytes = b""


def _build_log_prefix() -> None:
    """
    Function called just once at the beginning of the program to delete the '}'
    and append events to the same log line
    """
    global _log_ctx_prefix
    base = _dumps(run_ctx)
    _log_ctx_prefix = base[:-1]


def _log_writer() -> None:
    """
    Function to write logs with a separated thread.avoiding I/O blocking.
    The thread is started at the beginning of the program and keeps running untill
    experiment limit is reached.
    """
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
    """
    Appends the log event to the async log buffer (_log_deque)
    """
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
    """
    Class used to define the parameters of a logical link for 1:1 mode.
    When the DTM receives a LOGICAL_LINK_SET event from the ONM it extracts
    the link information to apply in the NDT mininet link segments.
    """
    delay_ms: float
    loss_pct: float
    bw_mbps:  float
    up:       bool


def is_switch(n: str) -> bool:
    """
    Used in 1:1 mode to determine if a node is a switch or not.
    It is further used to validate that a logical path contains at least 1 switch
    """
    return n.startswith("s")


def load_topo_graph() -> Dict[str, List[str]]:
    """
    Used in 1:1 Reads the topology contained in the topo file as adjacency matrix.
    Therefore, the 1:1 mode requires the NDT mininet running in order to save the topology
    """
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
    """
    Used when APPLY_LOGICAL_LINK_SET is received from the ONM.
    This function seeks for the middle switches in the logical path.
    """
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
    """
    Used as a control function to send the event to the controller in the
    mininet script
    """
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
    """
    Apply a logical link between two nodes in a given graph.

    This function determines the physical path between nodes `a` and `b` in the
    graph `g`, consisting only of switch nodes in the middle. If a physical path
    exists, the function establishes the link for each segment of the path using
    specified edge properties and reasserts routing configurations.
    """
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


# ── NEW (Exp. 1): build adjacency graph from TOPOLOGY event payload ───────────

def build_graph_from_payload(payload: dict) -> Dict[str, List[str]]:
    """
    Build adjacency graph from a TOPOLOGY event payload sent by the ONM.
    Called once when {"type":"TOPOLOGY","payload":{nodes,links}} arrives.
    Same structure as load_topo_graph() but reads from the event, not disk.
    """
    g: Dict[str, List[str]] = defaultdict(list)
    for a, b in payload.get("links", []):
        g[a].append(b)
        g[b].append(a)
    return g


def resolve_to_physical(
    g: Dict[str, List[str]],
    ev: dict,
) -> List[bytes]:
    """
    Resolve a LOGICAL_LINK_SET event into physical LINK_SET + ROUTE_REASSERT
    payloads ready to fan out to the NDTs (Exp. 1).

    Each NDT receives adjacent-node LINK_SET events that handle_event()
    can apply directly — same as what the DTM sends in 1:1 mode.

    Returns the original payload unchanged for any non-LOGICAL_LINK_SET type,
    or when g is None (Exp. 2 passive sinks — full backward compatibility).
    """
    if ev.get("type") != "LOGICAL_LINK_SET" or g is None:
        return [_dumps(ev)]

    p    = ev.get("payload", {})
    a, b = p.get("a"), p.get("b")
    segs = path_only_switches_middle(g, a, b)
    eid  = ev.get("event_id", "")

    if not segs:
        log_event("DTM_NO_PATH", {"event_id": eid, "a": a, "b": b})
        return []

    edge = {
        "delay_ms": float(p.get("delay_ms", 0.0)),
        "loss_pct": float(p.get("loss_pct", 0.0)),
        "bw_mbps":  float(p.get("bw_mbps", 1000.0)),
        "up":       bool(p.get("up", True)),
    }

    payloads: List[bytes] = []
    for x, y in segs:
        payloads.append(_dumps({
            "type":     "LINK_SET",
            "event_id": eid,
            "payload":  {"a": x, "b": y, **edge},
        }))
    payloads.append(_dumps({
        "type":     "ROUTE_REASSERT",
        "event_id": eid,
        "payload":  {"routers": ["r1", "r2"], "gateway": "10.0.0.254"},
    }))
    return payloads


# ── NEW (Exp. 1): ACK completion tracker ─────────────────────────────────────

class AckTracker:
    """
    Tracks UDP ACKs sent by each NDT after applying an event (Exp. 1).
    When all N NDTs have ACKed the same event_id, logs DTM_EVENT_COMPLETE.

    Created only when the TOPOLOGY event arrives (i.e. only for Exp. 1).
    For Exp. 2 passive sinks no TOPOLOGY is sent so ack_tracker stays None
    and FeedbackController behaviour is identical to the original.
    """
    def __init__(self, fanout: int) -> None:
        self._fanout  = fanout
        self._lock    = threading.Lock()
        # event_id → {port: t_apply_ns}
        self._acks:      Dict[str, Dict[int, int]] = defaultdict(dict)
        # event_id → t_dispatch_ns
        self._t_dispatch: Dict[str, int] = {}

    def register_dispatched(self, event_id: str, t_ns: int) -> None:
        with self._lock:
            self._t_dispatch[event_id] = t_ns

    def register_ack(self, event_id: str, port: int,
                     ok: bool, t_apply_ns: int) -> None:
        """Called by FeedbackController when an NDT_APPLY_ACK arrives."""
        with self._lock:
            self._acks[event_id][port] = t_apply_ns
            n_acked = len(self._acks[event_id])
            t_disp  = self._t_dispatch.get(event_id, 0)
            complete = (n_acked >= self._fanout)

        if complete:
            t_now = time.time_ns()
            log_event("DTM_EVENT_COMPLETE", {
                "event_id":      event_id,
                "fanout":        self._fanout,
                "acks":          n_acked,
                "t_dispatch_ns": t_disp,
                "t_complete_ns": t_now,
                "wall_ms":       (t_now - t_disp) / 1e6 if t_disp else None,
            })
            count_done_event(event_id)
            limit_reached()

# ─────────────────────────────────────────────────────────────────────────────


class UdpRelayReceiver:
    """
    Communicates the ONM and DTM via UDP connection
    """
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
    """
        Communicates the ONM and DTM via TCP connection
        """
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
    """
    Sends the ONM payload to the N NDTs via UDP
    """
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
    """
    Sends the ONM payload to the N NDTs via TCP
    """
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


class TokenBucket:
    """
    Implements a token bucket rate-limiting mechanism.

    This class provides a token bucket algorithm that allows users to consume
    tokens up to a certain rate, with tokens being replenished at fixed intervals.
    It is designed to regulate events or actions to ensure they do not exceed
    a defined rate.

    Attributes:
        WINDOW_S (float): A fixed window size in seconds used as the fundamental
            time frame for token allocation.

    Methods:
        next_batch: Returns the current number of tokens available for consumption.
        consume: Decreases the available tokens by a specified amount.
        wait_window_end: Waits for the current time window to end and replenishes
            tokens for the next time window.

    Usage: --max-rate 2500 --max-queue 25
    """
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
    """
    Class to manage the queue of events to be processed.

    This class facilitates queue management, including handling prioritized
    items, dealing with queue overflows, and providing mechanisms to control
    dispatch and draining of items. It supports both FIFO and priority-based
    queue behavior, along with configurations to disable queuing. It is
    designed for thread-safe operations.

    Attributes:
        max_queue (int): Maximum size of the queue. Zero means no limit.
        drop_prio_if_full (int): Minimum priority level at which items
            will be dropped if the queue is full and priority queuing
            is enabled.
        disable_priorities (bool): Disables priority sorting, enforcing
            FIFO behavior.
        no_queue (bool): If set to True, disables queuing entirely. Items
            will only be processed if no other items are present in the
            queue and the dispatch is not busy.
    """
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
    """
    Class for handling the events received by the NDTs. Reception is done via UDP

    Attributes:
        listen_port (int): The UDP port on which the controller listens for incoming feedback events.
        onm_sender (Optional[UdpFanoutSender]): Optional sender used for relaying specific feedback
            data under certain conditions.
        aq (AdmissionQueue): Queue used to manage the admission and prioritization of events.
        token_bucket (Optional[TokenBucket]): Optional token bucket to enforce rate limiting for
            processing events or managing throughput.
        ack_tracker (Optional[AckTracker]): NEW — tracks NDT_APPLY_ACK events for Exp. 1.
            None for Exp. 2 (passive sinks), created when TOPOLOGY arrives.
    """
    def __init__(
        self,
        listen_port:  int,
        onm_sender:   Optional[UdpFanoutSender],
        aq:           AdmissionQueue,
        token_bucket: Optional[TokenBucket] = None,
        ack_tracker:  Optional[AckTracker]  = None,   # NEW (Exp. 1)
    ) -> None:
        self._aq      = aq
        self._onm     = onm_sender
        self._tb      = token_bucket
        self._tracker = ack_tracker   # NEW (Exp. 1)
        self._sock    = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
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

            # NEW (Exp. 1): intercept NDT_APPLY_ACK before the AdmissionQueue
            # so the tracker can mark completion without queueing delay.
            # When tracker is None (Exp. 2) this branch is never taken.
            if ev_type == "NDT_APPLY_ACK" and self._tracker:
                self._tracker.register_ack(
                    event_id   = event_id,
                    port       = int(ev.get("port", 0)),
                    ok         = bool(ev.get("ok", False)),
                    t_apply_ns = int(ev.get("t_apply_ns", t_rx_ns)),
                )
                log_event("DTM_ACK_RX", {
                    "event_id": event_id,
                    "port":     ev.get("port"),
                    "ok":       ev.get("ok"),
                    "apply_ms": ev.get("apply_ms"),
                    "t_rx_ns":  t_rx_ns,
                })
                continue   # ACKs are not forwarded to the ONM

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
    g:          Optional[Dict[str, List[str]]] = None,    # NEW (Exp. 1)
    tracker:    Optional[AckTracker]           = None,    # NEW (Exp. 1)
) -> Tuple[int, int]:
    """
    FUnction to dispatch the ON events sent by the ONM towards the N NDTs.

    NEW (Exp. 1): if g is provided, LOGICAL_LINK_SET is resolved into
    physical LINK_SET + ROUTE_REASSERT segments before fanout so each NDT
    receives adjacent-node events that handle_event() can apply directly.
    If g is None (Exp. 2 passive sinks) behaviour is identical to original.
    """
    is_crit = (
        ev.get("type") == "LOGICAL_LINK_SET"
        and not ev.get("payload", {}).get("up", True)
    )
    targets     = list(range(fanout))
    sent_ok     = 0
    dispatch_ns = 0

    # Each NDT resolves LOGICAL_LINK_SET locally (same as DTM 1:1 mode).
    # The DTM sends the raw payload unchanged — no resolution needed here.
    payloads = [_dumps(ev)]
    if not payloads:
        return 0, 0

    t_dispatch_start = time.time_ns()

    for pld in payloads:
        if is_crit:
            t0          = time.time_ns()
            sent_ok    += sender.send_all(pld, targets)
            dispatch_ns += time.time_ns() - t0
        else:
            gs = max(1, group_size)
            for i in range(0, fanout, gs):
                t0           = time.time_ns()
                sent_ok     += sender.send_all(pld, targets[i: i + gs])
                dispatch_ns += time.time_ns() - t0
                if wave_delay > 0:
                    time.sleep(wave_delay)

    # NEW (Exp. 1): register dispatch time for ACK completion tracking
    if tracker and ev.get("event_id"):
        tracker.register_dispatched(ev["event_id"], t_dispatch_start)

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

    # Define the global context dictionary
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
    # Start the log independent thread
    threading.Thread(target=_log_writer, name="log-writer", daemon=True).start()
    _build_log_prefix()
    # To control the exact time for executing the experiment
    if args.experiment_duration > 0:
        def _timer() -> None:
            print(f"[DTM] Timer: {args.experiment_duration}s", flush=True)
            time.sleep(args.experiment_duration)
            stop_event.set()
            _flush_and_exit("Experiment duration reached")
        threading.Thread(target=_timer, name="exp-timer", daemon=True).start()

    gc.disable()

    # 1:1 mode loads topology from local file at startup (NDT Mininet already up).
    # federated mode receives topology from the ONM via a TOPOLOGY event.
    g: Optional[Dict[str, List[str]]] = (
        load_topo_graph() if args.mode == "1:1" else None
    )

    rx = (
        UdpRelayReceiver(args.relay_ip, args.relay_port)
        if args.proto == "udp"
        else TcpRelayReceiver(args.relay_ip, args.relay_port)
    )

    fast_sender  = None
    ack_tracker  = None   # NEW (Exp. 1) — created when TOPOLOGY arrives
    fc           = None   # reference kept to wire tracker after creation

    if args.mode == "federated":
        if args.fanout <= 0:
            raise SystemExit("ERROR: --fanout > 0 required in federated mode")
        # Define UDP/TCP sender for the NDTs
        fast_sender = (
            UdpFanoutSender(args.dst_ip, args.base_port, args.fanout)
            if args.proto == "udp"
            else TcpFanoutSender(args.dst_ip, args.base_port, args.fanout)
        )
        # Define the dispatch sender
        onm_sender = UdpFanoutSender(args.relay_ip, args.onm_port, 1)
        # Define the admision queue
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
        # Instantiate the event controller
        # ack_tracker starts as None — wired in when TOPOLOGY arrives (Exp. 1).
        # For Exp. 2 passive sinks no TOPOLOGY is sent so it stays None.
        fc = FeedbackController(
            listen_port=args.feedback_port,
            onm_sender=onm_sender,
            aq=aq,
            token_bucket=tb,
            ack_tracker=ack_tracker,
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

        # NEW (Exp. 1): TOPOLOGY handshake.
        # The ONM sends {"type":"TOPOLOGY","payload":{nodes,links}} as its
        # very first message after connecting. The DTM builds the adjacency
        # graph and wires up the AckTracker. Not forwarded to NDTs.
        # In 1:1 mode or Exp. 2 this event is never sent so the branch
        # is never taken.
        if ev_type == "TOPOLOGY":
            if args.mode == "federated":
                g = build_graph_from_payload(ev.get("payload", {}))
                # Create AckTracker now that we know this is Exp. 1
                if ack_tracker is None and fc is not None:
                    ack_tracker = AckTracker(fanout=args.fanout)
                    fc._tracker = ack_tracker
                print(
                    f"[DTM] Topology received from ONM ({len(g)} nodes) — ready",
                    flush=True,
                )
                log_event("DTM_TOPOLOGY_RX", {
                    "nodes":      len(g),
                    "size_bytes": len(data),
                    "t_rx_ns":    t_rx_ns,
                })
            continue   # not forwarded to NDTs

        log_event("NDT_EVENT_RX", {
            "event_id":   event_id,
            "type":       ev_type,
            "t_rx_ns":    t_rx_ns,
            "size_bytes": len(data),
        })

        if args.mode == "1:1":
            # Logic to apply the changes in the NDT
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

            # NEW (Exp. 1): guard — drop LOGICAL_LINK_SET if topology not yet received
            if ev_type == "LOGICAL_LINK_SET" and g is None:
                log_event("DTM_NO_TOPOLOGY", {"event_id": event_id})
                print("[DTM] WARNING: LOGICAL_LINK_SET before TOPOLOGY — dropped",
                      flush=True)
                continue

            t0 = time.time_ns()
            sent_ok, dispatch_ns = fast_dispatch(
                sender=fast_sender,
                fanout=args.fanout,
                group_size=args.group_size,
                wave_delay=args.wave_delay,
                ev=ev,
                payload=data,
                g=g,                 # NEW (Exp. 1): None for Exp. 2
                tracker=ack_tracker, # NEW (Exp. 1): None for Exp. 2
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