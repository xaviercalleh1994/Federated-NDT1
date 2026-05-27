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

node_scanner.py — XR Service: Node Scanner
--------------------------------------------
XR service daemon running inside each NDT pod.
Deployed and managed by the XRM (lifecycle: deploy/stop/restart).

Responsibilities
----------------
  - Receive XR_SERVICE_CMD from xrm-agent on UDP :9201 (localhost)
  - Execute scan commands on Mininet nodes via on_ndt.py:6001
  - Return scan results to xrm-agent
  - The xrm-agent handles forwarding to DTM upstream and broadcasting

Supported scan commands (in payload)
--------------------------------------
  scan_type: "all"          — scan all Mininet nodes
  scan_type: "node"         — scan a specific node (node field required)
  scan_type: "links"        — return current link state
  scan_type: "actuator"     — read actuator state from h1_1

Architecture
------------
  ndt-event-cli --service node-scanner (host)
      ↓ UDP NodePort
  xrm-agent (pod)
      ↓ UDP localhost :9201
  node-scanner (pod)               ← this service
      ↓ TCP localhost :6001
  on_ndt.py → Mininet nodes

Ports
-----
  9201  rx from xrm-agent (localhost only)
"""
import json
import os
import time
import socket
import argparse
from typing import Dict, List

# ── Constants ────────────────────────────────────────────────
CONTROL_HOST     = "127.0.0.1"
CONTROL_PORT     = 6001
CMD_RX_PORT      = 9201        # rx from xrm-agent (localhost)
TOPO_FILE        = "/run/topology.json"
LOG_FILE_DEFAULT = "/tmp/node_scanner.log"


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
# Topology loader
# ─────────────────────────────────────────────────────────────

def load_nodes() -> List[str]:
    """Load list of Mininet nodes from topology.json."""
    try:
        with open(TOPO_FILE) as f:
            t = json.load(f)
        return t.get("nodes", [])
    except Exception:
        return []


# ─────────────────────────────────────────────────────────────
# Scan commands
# ─────────────────────────────────────────────────────────────

def scan_node(node: str) -> dict:
    """Scan a single Mininet node: IP, routes, interfaces."""
    result = {"node": node}

    # IP addresses
    ip_resp = control_call({
        "type":    "EXEC",
        "payload": {"node": node, "cmd": "ip addr show"},
    })
    result["ip_addr"] = ip_resp.get("output", "") \
        if ip_resp.get("ok") else ip_resp.get("err", "")

    # Routes
    route_resp = control_call({
        "type":    "EXEC",
        "payload": {"node": node, "cmd": "ip route show"},
    })
    result["routes"] = route_resp.get("output", "") \
        if route_resp.get("ok") else route_resp.get("err", "")

    return result


def scan_all(ndt_id: int) -> dict:
    """Scan all Mininet nodes in the NDT."""
    t0    = time.time_ns()
    nodes = load_nodes()
    scan  = {}
    for node in nodes:
        scan[node] = scan_node(node)
    apply_ms = round((time.time_ns() - t0) / 1e6, 3)
    return {
        "ok":       True,
        "ndt_id":   ndt_id,
        "n_nodes":  len(nodes),
        "scan":     scan,
        "apply_ms": apply_ms,
    }


def scan_links(ndt_id: int) -> dict:
    """Return current link state via on_ndt.py SNAPSHOT."""
    t0   = time.time_ns()
    resp = control_call({"type": "SNAPSHOT"})
    apply_ms = round((time.time_ns() - t0) / 1e6, 3)
    if resp.get("ok"):
        return {
            "ok":       True,
            "ndt_id":   ndt_id,
            "links":    resp.get("links", []),
            "apply_ms": apply_ms,
        }
    return {
        "ok":       False,
        "err":      resp.get("err", "SNAPSHOT failed"),
        "apply_ms": apply_ms,
    }


def scan_actuator(ndt_id: int) -> dict:
    """Read actuator state from h1_1 state file."""
    t0 = time.time_ns()
    resp = control_call({
        "type":    "EXEC",
        "payload": {
            "node": "h1_1",
            "cmd":  "cat /tmp/actuator/h1_1/state.json 2>/dev/null "
                    "|| echo '{}'",
        },
    })
    apply_ms = round((time.time_ns() - t0) / 1e6, 3)
    if resp.get("ok"):
        try:
            state = json.loads(resp.get("output", "{}"))
        except Exception:
            state = {}
        return {
            "ok":       True,
            "ndt_id":   ndt_id,
            "actuator": state,
            "apply_ms": apply_ms,
        }
    return {
        "ok":       False,
        "err":      resp.get("err", ""),
        "apply_ms": apply_ms,
    }


def run_scan(ndt_id: int, payload: dict) -> dict:
    """Route scan command to the appropriate scan function."""
    scan_type = payload.get("scan_type", "all")

    if scan_type == "all":
        return scan_all(ndt_id)

    elif scan_type == "node":
        node = payload.get("node", "")
        if not node:
            return {"ok": False, "err": "node field required"}
        t0 = time.time_ns()
        result = scan_node(node)
        return {
            "ok":       True,
            "ndt_id":   ndt_id,
            "scan":     {node: result},
            "apply_ms": round((time.time_ns() - t0) / 1e6, 3),
        }

    elif scan_type == "links":
        return scan_links(ndt_id)

    elif scan_type == "actuator":
        return scan_actuator(ndt_id)

    else:
        return {
            "ok":  False,
            "err": f"Unknown scan_type: {scan_type}. "
                   f"Known: all, node, links, actuator",
        }


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

    print(f"[NODE-SCANNER ndt_id={ndt_id}] Ready — "
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
            log_event("SCANNER_PARSE_ERR", {"err": str(exc)})
            continue

        event_id  = str(cmd.get("event_id", ""))
        ev_type   = cmd.get("type", "")
        payload   = cmd.get("payload", {})

        log_event("SCANNER_CMD_RX", {
            "event_id":  event_id,
            "type":      ev_type,
            "scan_type": payload.get("scan_type", ""),
            "t_rx_ns":   t_rx_ns,
        })

        if ev_type == "XR_SERVICE_CMD":
            result   = run_scan(ndt_id, payload)
            apply_ms = result.get("apply_ms", 0.0)
            print(f"[NODE-SCANNER] {event_id} "
                  f"scan_type={payload.get('scan_type', 'all')} "
                  f"ok={result['ok']} apply_ms={apply_ms}", flush=True)
            log_event("SCANNER_CMD_APPLIED", {
                "event_id": event_id,
                "ok":       result["ok"],
                "apply_ms": apply_ms,
            })

        else:
            log_event("SCANNER_UNKNOWN_CMD",
                      {"event_id": event_id, "type": ev_type})


# ─────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="node_scanner — XR service daemon for NDT node scanning"
    )
    ap.add_argument("--ndt-id",   type=int, default=0,
                    help="NDT identifier (0-based)")
    ap.add_argument("--log-file", default=LOG_FILE_DEFAULT)
    args = ap.parse_args()
    run(args)


if __name__ == "__main__":
    main()