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

ndt_event_cli.py — Generic NDT XR Service Client
--------------------------------------------------
External client that sends commands to any XR service deployed in
an NDT pod. Connects to xrm-agent via NodePort. Does not require
kubectl — runs on the host or experiment VM.

The client is service-agnostic: it sends key=val fields to the
xrm-agent which routes them to the appropriate service process.

Flow
----
  ndt-event-cli (host)
      ↓ UDP NodePort :8110+ndt_id
  xrm-agent (pod ndt-i)
      ↓ localhost → service process
  actuator-control / node-scanner / ...
      ↓
  xrm-agent → DTM upstream (priority queue)
      ↓
  ONM → ON (h1_1)
      ↓
  DTM broadcast → xrm-agents
      ↓
  ndt-event-cli ← XR_SERVICE_REPLY

Modes
-----
cli:
  Interactive prompt. Operator types commands manually.

  Commands:
    set key=val [key=val ...]
        Send XR_SERVICE_CMD to the service.
        Example (actuator): set action=move angle=45 speed=10
        Example (scanner):  set scan_type=all
                            set scan_type=node node=r1
                            set scan_type=links
                            set scan_type=actuator

    get
        Request current ON context from xrm-agent.

    quit / exit
        Exit the CLI.

auto:
  Automated experiment mode. Sends commands at a given rate
  for a given duration. All replies are logged with full metrics.

  Designed to run in parallel for N NDTs:
    for i in 0 1 2 3; do
        python3 ndt_event_cli.py \\
            --mode auto --service actuator-control \\
            --ndt-id $i --node-ip 10.4.47.50 \\
            --rate 2 --duration 60 --priority 50 \\
            --log-file /tmp/exp2/ndt${i}.log &
    done
    wait

  Command script file (--script):
    Lines of key=val pairs:
      action=move angle=45 speed=10
      action=grip status=busy
      action=release status=idle

Metrics logged per command (auto mode)
---------------------------------------
  {
    "event_id":    "...",
    "ndt_id":      N,
    "service":     "actuator-control",
    "your_status": "ACK|QUEUED|DISCARDED|TIMEOUT",
    "t_create_ns": ...,
    "t_sent_ns":   ...,
    "t_reply_ns":  ...,
    "e2e_ms":      ...,    # t_reply_ns - t_create_ns
    "send_ms":     ...,    # t_sent_ns - t_create_ns
    "wait_ms":     ...,    # t_reply_ns - t_sent_ns
    "apply_ms":    ...,    # time to apply on ON (from DTM/ONM)
    "state":       {...},  # ON state after apply
  }

Ports
-----
  8110+ndt_id  tx to xrm-agent (NodePort)
  9210+ndt_id  rx replies from xrm-agent (local ephemeral)
"""
import json
import os
import time
import uuid
import socket
import argparse
import threading
from typing import Optional

# ── Constants ────────────────────────────────────────────────
XRM_AGENT_BASE_PORT = 8110   # xrm-agent pod internal port base (+ ndt_id)
REPLY_BASE_PORT     = 9210   # local reply port base (+ ndt_id)
TIMEOUT_S           = 30.0   # max wait for reply
LOG_FILE_DEFAULT    = "/tmp/ndt_event_cli.log"


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

_log_file: str = LOG_FILE_DEFAULT
_log_lock       = threading.Lock()


def log_result(data: dict) -> None:
    line = json.dumps(data)
    with _log_lock:
        try:
            with open(_log_file, "a") as f:
                f.write(line + "\n")
        except Exception:
            pass


# ─────────────────────────────────────────────────────────────
# UDP client
# ─────────────────────────────────────────────────────────────

class XrmAgentClient:
    """
    UDP client for any XR service via xrm-agent NodePort.
    Sends XR_SERVICE_CMD and waits for XR_SERVICE_REPLY.
    One instance per NDT — each binds its own reply port.
    """
    def __init__(self, node_ip: str, ndt_id: int,
                 service: str,
                 agent_port: Optional[int] = None,
                 pod_ip: Optional[str] = None) -> None:
        # pod_ip: connect directly to pod via Calico (bypasses NodePort)
        # node_ip: fallback via NodePort (may be blocked by Calico UDP NAT)
        self._node_ip    = pod_ip if pod_ip else node_ip
        self._ndt_id     = ndt_id
        self._service    = service
        self._agent_port = agent_port or (XRM_AGENT_BASE_PORT + ndt_id)
        self._reply_port = REPLY_BASE_PORT + ndt_id

        self._tx_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._rx_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self._rx_sock.setsockopt(
            socket.SOL_SOCKET, socket.SO_RCVBUF, 2 * 1024 * 1024)
        self._rx_sock.setsockopt(
            socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._rx_sock.bind(("0.0.0.0", self._reply_port))
        self._rx_sock.settimeout(TIMEOUT_S)

        # Pending replies: event_id → {event, result}
        self._pending:      dict = {}
        self._pending_lock  = threading.Lock()

        threading.Thread(target=self._rx_loop,
                         name=f"cli-rx-{ndt_id}", daemon=True).start()

    def _rx_loop(self) -> None:
        while True:
            try:
                data, _ = self._rx_sock.recvfrom(65535)
            except socket.timeout:
                continue
            except Exception:
                continue
            try:
                msg = json.loads(data)
            except Exception:
                continue
            event_id = str(msg.get("event_id", ""))
            with self._pending_lock:
                entry = self._pending.get(event_id)
            if entry:
                entry["result"] = msg
                entry["event"].set()

    def send_cmd(self, fields: dict,
                 priority: int = 50,
                 t_create_ns: Optional[int] = None) -> dict:
        """
        Send XR_SERVICE_CMD to xrm-agent and wait for reply.
        Returns result dict with full latency metrics.
        """
        event_id  = uuid.uuid4().hex[:16]
        t_create  = t_create_ns or time.time_ns()
        ev        = threading.Event()

        with self._pending_lock:
            self._pending[event_id] = {"event": ev, "result": None}

        # Include client IP so xrm-agent can reply directly
        # (in K8s UDP the source addr seen by the pod may be NAT'd)
        # Use node_ip (not pod_ip) to find the correct outgoing interface
        import socket as _socket
        try:
            _s = _socket.socket(_socket.AF_INET, _socket.SOCK_DGRAM)
            _s.connect(("8.8.8.8", 80))
            client_ip = _s.getsockname()[0]
            _s.close()
        except Exception:
            client_ip = "127.0.0.1"

        cmd = json.dumps({
            "type":        "XR_SERVICE_CMD",
            "event_id":    event_id,
            "service":     self._service,
            "ndt_id":      self._ndt_id,
            "priority":    priority,
            "t_create_ns": t_create,
            "reply_port":  self._reply_port,
            "reply_ip":    client_ip,
            "payload":     fields,
        }).encode()

        t_sent = time.time_ns()
        self._tx_sock.sendto(cmd, (self._node_ip, self._agent_port))

        ev.wait(timeout=TIMEOUT_S)

        with self._pending_lock:
            entry = self._pending.pop(event_id, {})

        result  = entry.get("result")
        t_reply = time.time_ns()

        if result is None:
            return {
                "event_id":    event_id,
                "ndt_id":      self._ndt_id,
                "service":     self._service,
                "your_status": "TIMEOUT",
                "t_create_ns": t_create,
                "t_sent_ns":   t_sent,
                "t_reply_ns":  t_reply,
                "e2e_ms":      round((t_reply - t_create) / 1e6, 3),
                "send_ms":     round((t_sent  - t_create) / 1e6, 3),
                "wait_ms":     round((t_reply - t_sent)   / 1e6, 3),
                "apply_ms":    None,
                "state":       {},
            }

        return {
            "event_id":    event_id,
            "ndt_id":      self._ndt_id,
            "service":     self._service,
            "your_status": result.get("your_status", "UNKNOWN"),
            "t_create_ns": t_create,
            "t_sent_ns":   t_sent,
            "t_reply_ns":  t_reply,
            "e2e_ms":      result.get("e2e_ms") or
                           round((t_reply - t_create) / 1e6, 3),
            "send_ms":     round((t_sent  - t_create) / 1e6, 3),
            "wait_ms":     round((t_reply - t_sent)   / 1e6, 3),
            "apply_ms":    result.get("apply_ms"),
            "state":       result.get("state", {}),
        }

    def get_context(self) -> dict:
        """Request current ON context from xrm-agent."""
        event_id = uuid.uuid4().hex[:16]
        ev       = threading.Event()

        with self._pending_lock:
            self._pending[event_id] = {"event": ev, "result": None}

        cmd = json.dumps({
            "type":       "XR_SERVICE_GET",
            "event_id":   event_id,
            "service":    self._service,
            "ndt_id":     self._ndt_id,
            "reply_port": self._reply_port,
        }).encode()

        self._tx_sock.sendto(cmd, (self._node_ip, self._agent_port))
        ev.wait(timeout=5.0)

        with self._pending_lock:
            entry = self._pending.pop(event_id, {})

        result = entry.get("result")
        return result.get("context", {}) if result else {}

    def close(self) -> None:
        self._tx_sock.close()
        self._rx_sock.close()


# ─────────────────────────────────────────────────────────────
# CLI mode
# ─────────────────────────────────────────────────────────────

def run_cli(args: argparse.Namespace) -> None:
    client = XrmAgentClient(args.node_ip, args.ndt_id,
                            args.service, args.agent_port,
                            pod_ip=args.pod_ip)

    target = args.pod_ip if args.pod_ip else args.node_ip
    print(f"[NDT-CLI ndt_id={args.ndt_id}] "
          f"service={args.service}")
    print(f"  → {target}:{client._agent_port}  "
          f"reply :{client._reply_port}")
    print("  Commands:")
    print("    set key=val [key=val ...]")
    print("      actuator: set action=move angle=45 speed=10")
    print("      scanner:  set scan_type=all")
    print("                set scan_type=node node=r1")
    print("                set scan_type=links")
    print("                set scan_type=actuator")
    print("    get   — show current ON context")
    print("    quit\n")

    while True:
        try:
            line = input(f"[ndt-{args.ndt_id}|{args.service}] > ").strip()
        except EOFError:
            break
        if not line:
            continue

        parts = line.split()
        cmd   = parts[0].lower()

        if cmd in ("quit", "exit"):
            break

        elif cmd == "get":
            ctx = client.get_context()
            if ctx:
                print(f"  ON context (ndt-{args.ndt_id}):")
                for k, v in ctx.items():
                    print(f"    {k}: {v}")
            else:
                print("  (no ON context yet)")

        elif cmd == "set":
            if len(parts) < 2:
                print("  Usage: set key=val [key=val ...]")
                continue
            fields = _parse_kv(parts[1:])
            if not fields:
                continue
            print(f"  Sending...", end=" ", flush=True)
            result = client.send_cmd(fields, priority=args.priority)
            status = result["your_status"]
            e2e    = result["e2e_ms"]
            apply  = result["apply_ms"]
            print(f"{status}  e2e={e2e}ms  apply={apply}ms")
            if result.get("state"):
                for k, v in result["state"].items():
                    if k not in ("event_id", "ndt_id", "service",
                                 "t_applied_ns", "t_create_ns"):
                        print(f"    {k}: {v}")
        else:
            print("  Unknown command. Use: set / get / quit")

    client.close()
    print(f"[NDT-CLI ndt_id={args.ndt_id}] Bye.")


# ─────────────────────────────────────────────────────────────
# Auto mode — experiment
# ─────────────────────────────────────────────────────────────

def run_auto(args: argparse.Namespace) -> None:
    global _log_file
    _log_file = args.log_file

    client   = XrmAgentClient(args.node_ip, args.ndt_id,
                              args.service, args.agent_port)
    interval = 1.0 / args.rate if args.rate > 0 else 1.0
    start    = time.monotonic()
    sent     = 0
    acked    = 0
    discarded = 0
    timeouts = 0

    # Load commands from script file or use service defaults
    commands = _load_commands(args.script) if args.script \
               else _default_commands(args.service)

    print(f"[NDT-AUTO ndt_id={args.ndt_id}] "
          f"service={args.service}", flush=True)
    print(f"  rate={args.rate}/s  duration={args.duration}s  "
          f"priority={args.priority}", flush=True)
    print(f"  log → {_log_file}", flush=True)

    cmd_idx = 0
    while True:
        elapsed = time.monotonic() - start
        if args.duration > 0 and elapsed >= args.duration:
            break

        t_create = time.time_ns()
        fields   = commands[cmd_idx % len(commands)]
        cmd_idx += 1

        result = client.send_cmd(fields, priority=args.priority,
                                 t_create_ns=t_create)
        sent += 1
        status = result["your_status"]
        if status == "ACK":
            acked += 1
        elif status == "DISCARDED":
            discarded += 1
        elif status == "TIMEOUT":
            timeouts += 1

        log_result(result)

        if sent % 10 == 0:
            print(f"[NDT-AUTO ndt_id={args.ndt_id}] "
                  f"sent={sent} ack={acked} disc={discarded} "
                  f"timeout={timeouts} elapsed={elapsed:.1f}s",
                  flush=True)

        # Rate control
        next_send = start + sent * interval
        sleep_s   = next_send - time.monotonic()
        if sleep_s > 0:
            time.sleep(sleep_s)

    client.close()

    # Summary
    print(f"\n[NDT-AUTO ndt_id={args.ndt_id}] Done", flush=True)
    print(f"  sent={sent} ack={acked} "
          f"discarded={discarded} timeout={timeouts}", flush=True)
    if acked > 0:
        try:
            e2e_vals = []
            with open(_log_file) as f:
                for line in f:
                    r = json.loads(line)
                    if r.get("your_status") == "ACK" and \
                       r.get("e2e_ms") is not None:
                        e2e_vals.append(float(r["e2e_ms"]))
            if e2e_vals:
                avg = round(sum(e2e_vals) / len(e2e_vals), 3)
                mn  = round(min(e2e_vals), 3)
                mx  = round(max(e2e_vals), 3)
                print(f"  e2e_ms: avg={avg}  min={mn}  max={mx}",
                      flush=True)
        except Exception:
            pass
    print(f"  log → {_log_file}", flush=True)


# ─────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────

def _parse_kv(parts: list) -> dict:
    fields = {}
    for kv in parts:
        if "=" not in kv:
            print(f"  [WARN] ignoring malformed param: {kv}")
            continue
        k, v = kv.split("=", 1)
        try:
            fields[k] = float(v) if "." in v else int(v)
        except ValueError:
            fields[k] = v
    return fields


def _load_commands(script_path: str) -> list:
    """Load commands from script file (one set of key=val per line)."""
    commands = []
    with open(script_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) >= 1:
                # Support both "set key=val" and just "key=val"
                if parts[0].lower() == "set":
                    parts = parts[1:]
                fields = _parse_kv(parts)
                if fields:
                    commands.append(fields)
    return commands or _default_commands("unknown")


def _default_commands(service: str) -> list:
    """Default command sequence per service."""
    if service == "actuator-control":
        return [
            {"action": "move",    "angle": 45,  "speed": 10},
            {"action": "grip",    "status": "busy"},
            {"action": "release", "status": "idle"},
            {"action": "move",    "angle": 0,   "speed": 5},
        ]
    elif service == "node-scanner":
        return [
            {"scan_type": "all"},
            {"scan_type": "links"},
            {"scan_type": "actuator"},
        ]
    else:
        return [{"action": "ping"}]


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="ndt_event_cli — generic NDT XR service client"
    )
    ap.add_argument("--mode",       choices=["cli", "auto"],
                    default="cli")
    ap.add_argument("--service",    required=True,
                    help="XR service name: actuator-control, node-scanner, ...")
    ap.add_argument("--ndt-id",     type=int, default=0,
                    help="NDT identifier (0-based)")
    ap.add_argument("--node-ip",    default="10.4.47.50",
                    help="Kubernetes node IP")
    ap.add_argument("--pod-ip",     default="",
                    help="Pod IP for direct Calico connection (bypasses NodePort)")
    ap.add_argument("--agent-port", type=int, default=None,
                    help="Override xrm-agent NodePort "
                         "(default: 8110+ndt_id)")
    ap.add_argument("--priority",   type=int, default=50,
                    help="[auto] Command priority (lower = higher)")
    ap.add_argument("--rate",       type=float, default=1.0,
                    help="[auto] Commands per second")
    ap.add_argument("--duration",   type=float, default=60.0,
                    help="[auto] Duration in seconds (0=infinite)")
    ap.add_argument("--script",     default="",
                    help="[auto] Command script file path")
    ap.add_argument("--log-file",   default=LOG_FILE_DEFAULT,
                    help="[auto] Output log file")
    args = ap.parse_args()

    if args.mode == "auto":
        run_auto(args)
    else:
        run_cli(args)


if __name__ == "__main__":
    main()