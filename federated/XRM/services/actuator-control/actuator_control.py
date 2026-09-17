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

actuator_control.py — XR Service: Actuator Control
----------------------------------------------------
XR service daemon running inside each NDT pod.
Deployed and managed by the XRM (lifecycle: deploy/stop/restart).

Responsibilities
----------------
  - Receive XR_SERVICE_CMD from xrm-agent on UDP :9200 (localhost)
  - Apply the command on h1_1 in local Mininet via on_ndt.py:6001
  - Store state in /tmp/actuator/h1_1/state.json
  - The xrm-agent handles forwarding to DTM upstream and broadcasting

The xrm-agent is the only entry point for commands — this service
only communicates locally with the xrm-agent and on_ndt.py.

Architecture
------------
  ndt-event-cli (host)
      ↓ UDP NodePort
  xrm-agent (pod)
      ↓ UDP localhost :9200
  actuator-control (pod)          ← this service
      ↓ TCP localhost :6001
  on_ndt.py → h1_1 Mininet

State file
----------
  /tmp/actuator/h1_1/state.json
  {
    "event_id": "...",
    "ndt_id": N,
    "service": "actuator-control",
    "t_applied_ns": ...,
    <command payload fields>
  }

Ports
-----
  9200  rx from xrm-agent (localhost only)
"""
import json
import os
import time
import socket
import argparse

# ── Constants ────────────────────────────────────────────────
CONTROL_HOST       = "127.0.0.1"
CONTROL_PORT       = 6001
CMD_RX_PORT        = 9200        # rx from xrm-agent (localhost)
ACTUATOR_NODE      = "h1_1"
ACTUATOR_STATE_DIR = f"/tmp/actuator/{ACTUATOR_NODE}"
ACTUATOR_STATE_PATH = f"{ACTUATOR_STATE_DIR}/state.json"
LOG_FILE_DEFAULT   = "/tmp/actuator_control.log"


# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────

_log_file: str = LOG_FILE_DEFAULT


def log_event(tag: str, data: dict) -> None:
    entry = json.dumps({"ts_ns": time.time_ns(), "tag": tag, **data})
    try:
        with open(_log_file, "a") as f:
            f.write(entry + "\n")
    except Exception:
        pass


# ─────────────────────────────────────────────────────────────
# on_ndt.py primitive caller (localhost:6001)
# ─────────────────────────────────────────────────────────────

def control_call(ev: dict) -> dict:
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.settimeout(5.0)
        s.connect((CONTROL_HOST, CONTROL_PORT))
        s.sendall((json.dumps(ev) + "\n").encode())
        data = b""
        while b"\n" not in data:
            chunk = s.recv(4096)
            if not chunk:
                break
            data += chunk
        s.close()
        return json.loads(data.split(b"\n")[0].decode()) if data \
            else {"ok": False, "err": "no response"}
    except Exception as e:
        return {"ok": False, "err": str(e)}


# ─────────────────────────────────────────────────────────────
# Actuator command application
# Applies command on h1_1 via on_ndt.py EXEC primitive.
# Stores state to /tmp/actuator/h1_1/state.json.
# ─────────────────────────────────────────────────────────────

def apply_cmd(ndt_id: int, event_id: str, payload: dict) -> dict:
    """
    Apply actuator command on h1_1 via on_ndt.py:6001.
    Writes state file via EXEC in h1_1 namespace.
    Returns {ok, state, apply_ms}.
    """
    t0 = time.time_ns()

    state = {
        "event_id":     event_id,
        "ndt_id":       ndt_id,
        "service":      "actuator-control",
        "node":         ACTUATOR_NODE,
        "t_applied_ns": time.time_ns(),
        **{k: v for k, v in payload.items() if k != "node"},
    }

    os.makedirs(ACTUATOR_STATE_DIR, exist_ok=True)
    state_json = json.dumps(state).replace('"', '\\"')

    # Write state into h1_1 namespace via EXEC
    write_resp = control_call({
        "type": "EXEC",
        "payload": {
            "node": ACTUATOR_NODE,
            "cmd": (
                f"mkdir -p {ACTUATOR_STATE_DIR} && "
                f"echo \"{state_json}\" > {ACTUATOR_STATE_PATH}"
            ),
        },
    })

    if not write_resp.get("ok"):
        return {
            "ok":       False,
            "err":      f"write failed: {write_resp.get('err')}",
            "apply_ms": round((time.time_ns() - t0) / 1e6, 3),
            "state":    {},
        }

    # Write local copy
    try:
        with open(ACTUATOR_STATE_PATH, "w") as f:
            json.dump(state, f, indent=2)
    except Exception as exc:
        log_event("ACTUATOR_STATE_WRITE_ERR", {"err": str(exc)})

    apply_ms = round((time.time_ns() - t0) / 1e6, 3)

    log_event("ACTUATOR_CMD_APPLIED", {
        "event_id": event_id,
        "ndt_id":   ndt_id,
        "payload":  payload,
        "apply_ms": apply_ms,
    })

    return {"ok": True, "state": state, "apply_ms": apply_ms}


# ─────────────────────────────────────────────────────────────
# Main daemon
# ─────────────────────────────────────────────────────────────

def run(args: argparse.Namespace) -> None:
    global _log_file
    _log_file = args.log_file

    ndt_id = args.ndt_id

    # Only accept connections from localhost (xrm-agent in same pod)
    rx_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    rx_sock.bind(("127.0.0.1", CMD_RX_PORT))
    rx_sock.settimeout(1.0)

    print(f"[ACTUATOR-CTRL ndt_id={ndt_id}] Ready — "
          f"rx localhost:{CMD_RX_PORT}", flush=True)

    while True:
        try:
            data, addr = rx_sock.recvfrom(65535)
        except socket.timeout:
            continue
        except Exception:
            continue

        t_rx_ns = time.time_ns()
        try:
            cmd = json.loads(data)
        except Exception as exc:
            log_event("ACTUATOR_PARSE_ERR", {"err": str(exc)})
            continue

        event_id = str(cmd.get("event_id", ""))
        ev_type  = cmd.get("type", "")
        payload  = cmd.get("payload", {})

        log_event("ACTUATOR_CMD_RX", {
            "event_id": event_id,
            "type":     ev_type,
            "payload":  payload,
            "t_rx_ns":  t_rx_ns,
        })

        if ev_type == "XR_SERVICE_CMD":
            result = apply_cmd(ndt_id, event_id, payload)
            print(f"[ACTUATOR-CTRL] {event_id} "
                  f"ok={result['ok']} "
                  f"apply_ms={result['apply_ms']}", flush=True)

        else:
            log_event("ACTUATOR_UNKNOWN_CMD",
                      {"event_id": event_id, "type": ev_type})


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="actuator_control — XR service daemon for h1_1"
    )
    ap.add_argument("--ndt-id",   type=int, default=0,
                    help="NDT identifier (0-based)")
    ap.add_argument("--log-file", default=LOG_FILE_DEFAULT)
    args = ap.parse_args()
    run(args)


if __name__ == "__main__":
    main()