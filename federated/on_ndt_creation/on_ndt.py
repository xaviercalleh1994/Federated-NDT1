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

on_ndt.py — Mininet Keeper
--------------------------
Single responsibility: start and maintain a Mininet network.
All control logic (event handling, routing decisions, QoS policy)
lives in onm.py (ON role) and dtm.py (NDT role).

This script exposes a minimal TCP API on 127.0.0.1:6001
so that onm.py and dtm.py can apply physical changes:

  LINK_SET  — bring interface up/down and apply tc QoS
  EXEC      — run a command inside a Mininet node namespace
  SNAPSHOT  — re-export topology.json
  RELOAD    — stop Mininet, reload topology_config.json, restart

The topology is defined in topology_config.json (--topo-file).
If the file is not found, a default 2-LAN topology is used.

Architecture:
  onm.py  →  127.0.0.1:6001  →  on_ndt.py  →  Mininet (ON)
  dtm.py  →  127.0.0.1:6001  →  on_ndt.py  →  Mininet (NDT)
"""
import os
import json
import time
import socket
import threading
import argparse
from typing import Optional, List, Tuple

from mininet.net import Mininet
from mininet.node import OVSController, Node
from mininet.nodelib import NAT
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink, Intf

# ── File paths ────────────────────────────────────────────────
PID_FILE         = "/run/mininet.pid"
TOPO_FILE        = "/run/topology.json"         # exported Mininet state
TOPO_CONFIG_FILE = "/run/topology_config.json"  # input config (LANs/hosts)

# ── Control server ────────────────────────────────────────────
CONTROL_HOST = "0.0.0.0"
CONTROL_PORT = 6001

# ── Default topology (fallback) ───────────────────────────────
DEFAULT_TOPOLOGY = {
    "description": "Default 2-LAN topology",
    "backbone": {"nat_ip": "10.0.0.254/24"},
    "lans": [
        {"id": 1, "subnet": "192.168.10", "hosts": 2},
        {"id": 2, "subnet": "192.168.20", "hosts": 2},
    ]
}

# ── Global state ──────────────────────────────────────────────
_current_net: Optional[Mininet] = None
_net_lock = threading.Lock()


# ─────────────────────────────────────────────────────────────
# Topology helpers
# ─────────────────────────────────────────────────────────────

def load_topology_config(path: str) -> dict:
    try:
        with open(path) as f:
            topo = json.load(f)
        n_hosts = sum(l["hosts"] for l in topo["lans"])
        info(f"*** [TOPO] Loaded {path} — "
             f"{len(topo['lans'])} LANs, {n_hosts} hosts\n")
        return topo
    except FileNotFoundError:
        info(f"*** [TOPO] {path} not found — using default topology\n")
        return DEFAULT_TOPOLOGY
    except Exception as e:
        info(f"*** [TOPO] Error loading {path}: {e} — using default\n")
        return DEFAULT_TOPOLOGY


def save_topology_config(path: str, topo: dict) -> None:
    os.makedirs(os.path.dirname(path) or "/run", exist_ok=True)
    with open(path, "w") as f:
        json.dump(topo, f, indent=2)
    info(f"*** [TOPO] Saved new config to {path}\n")


def export_topology(net: Mininet, filename: str = TOPO_FILE) -> None:
    """Export current Mininet topology (nodes + links) to JSON."""
    nodes = [n.name for n in net.hosts + net.switches]
    links = [[l.intf1.node.name, l.intf2.node.name] for l in net.links]
    os.makedirs("/run", exist_ok=True)
    with open(filename, "w") as f:
        json.dump({"nodes": nodes, "links": links}, f, indent=2)
    info(f"*** [TOPO] Exported topology to {filename}\n")


# ─────────────────────────────────────────────────────────────
# Mininet primitives
# ─────────────────────────────────────────────────────────────

def _find_link(net: Mininet, a: str, b: str):
    for l in net.links:
        n1 = l.intf1.node.name
        n2 = l.intf2.node.name
        if (n1 == a and n2 == b) or (n1 == b and n2 == a):
            return l
    return None


def _apply_tc(intf: str, delay_ms: float,
              loss_pct: float, bw_mbps: float) -> None:
    os.system(f"tc qdisc del dev {intf} root 2>/dev/null")
    rate = max(0.1, float(bw_mbps))
    os.system(
        f"tc qdisc add dev {intf} root handle 1: tbf "
        f"rate {rate}mbit burst 64kbit latency 400ms 2>/dev/null"
    )
    os.system(
        f"tc qdisc add dev {intf} parent 1:1 handle 10: netem "
        f"delay {max(0.0, float(delay_ms))}ms "
        f"loss {max(0.0, float(loss_pct))}% 2>/dev/null"
    )


def _configurar_router(router: Node) -> None:
    router.cmd("sysctl -w net.ipv4.ip_forward=1")


# ─────────────────────────────────────────────────────────────
# Primitive API handlers
# ─────────────────────────────────────────────────────────────

def _handle_link_set(net: Mininet, p: dict) -> dict:
    """
    LINK_SET — bring a physical link up/down and apply QoS.
    Called by onm.py and dtm.py after resolving logical paths.

    payload: {a, b, up, delay_ms, loss_pct, bw_mbps}
    """
    a        = p["a"]
    b        = p["b"]
    up       = bool(p.get("up", True))
    delay_ms = float(p.get("delay_ms", 0.0))
    loss_pct = float(p.get("loss_pct", 0.0))
    bw_mbps  = float(p.get("bw_mbps", 1000.0))

    l = _find_link(net, a, b)
    if l is None:
        return {"ok": False, "err": f"No physical link between {a} and {b}"}

    l.intf1.ifconfig("up" if up else "down")
    l.intf2.ifconfig("up" if up else "down")

    if up:
        _apply_tc(str(l.intf1), delay_ms, loss_pct, bw_mbps)
        _apply_tc(str(l.intf2), delay_ms, loss_pct, bw_mbps)
    else:
        os.system(f"tc qdisc del dev {str(l.intf1)} root 2>/dev/null")
        os.system(f"tc qdisc del dev {str(l.intf2)} root 2>/dev/null")

    return {"ok": True, "a": a, "b": b, "up": up}


def _handle_exec(net: Mininet, p: dict) -> dict:
    """
    EXEC — run a shell command inside a Mininet node namespace.
    Called by onm.py and dtm.py for diagnostic or config commands.

    payload: {node, cmd}
    """
    node_name = p.get("node")
    cmd       = p.get("cmd")
    if not node_name or not cmd:
        return {"ok": False, "err": "Missing node or cmd"}
    try:
        out = net.get(node_name).cmd(cmd)
        return {"ok": True, "node": node_name, "output": out.strip()}
    except Exception as e:
        return {"ok": False, "err": str(e)}


def _handle_snapshot(net: Mininet) -> dict:
    """
    SNAPSHOT — re-export current topology to topology.json.
    Useful after manual changes or to sync state.
    """
    export_topology(net)
    return {"ok": True, "file": TOPO_FILE}


def _handle_reload(topo_config_file: str, role: str,
                   bare_metal: bool, controller_port: int) -> dict:
    """
    RELOAD — stop current Mininet, reload topology_config.json, restart.
    Called after onm.py or dtm.py writes a new topology_config.json.
    Runs in a background thread so the control server stays responsive.
    """
    def _do_reload():
        global _current_net
        info("*** [RELOAD] Stopping current Mininet...\n")
        try:
            with _net_lock:
                if _current_net:
                    _current_net.stop()
        except Exception as e:
            info(f"*** [RELOAD] Stop error (ignored): {e}\n")
        os.system("mn -c > /dev/null 2>&1")
        time.sleep(2)
        info("*** [RELOAD] Restarting with new topology...\n")
        _start_mininet(
            role=role,
            bare_metal=bare_metal,
            controller_port=controller_port,
            topo_config_file=topo_config_file,
        )

    threading.Thread(target=_do_reload, daemon=True).start()
    return {"ok": True, "msg": "reload initiated"}


# ─────────────────────────────────────────────────────────────
# Control server — primitive API on 127.0.0.1:6001
# ─────────────────────────────────────────────────────────────

def _control_server(role: str, bare_metal: bool,
                    controller_port: int,
                    topo_config_file: str) -> None:
    """
    Minimal TCP server (JSON lines) on 127.0.0.1:6001.
    Dispatches to primitive handlers.
    onm.py and dtm.py connect here to apply physical changes.

    Protocol:
      send: {"type": "LINK_SET"|"EXEC"|"SNAPSHOT"|"RELOAD", "payload": {...}}\n
      recv: {"ok": true|false, ...}\n
    """
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((CONTROL_HOST, CONTROL_PORT))
    srv.listen(5)
    info(f"*** [API] Control server ready on {CONTROL_HOST}:{CONTROL_PORT}\n")

    while True:
        conn, _ = srv.accept()
        threading.Thread(
            target=_handle_conn,
            args=(conn, role, bare_metal, controller_port, topo_config_file),
            daemon=True,
        ).start()


def _handle_conn(conn, role, bare_metal, controller_port, topo_config_file):
    with conn:
        buf = b""
        while True:
            data = conn.recv(4096)
            if not data:
                break
            buf += data
            while b"\n" in buf:
                line, buf = buf.split(b"\n", 1)
                if not line.strip():
                    continue
                try:
                    ev   = json.loads(line.decode())
                    resp = _dispatch(
                        ev, role, bare_metal,
                        controller_port, topo_config_file,
                    )
                except Exception as e:
                    resp = {"ok": False, "err": str(e)}
                conn.sendall((json.dumps(resp) + "\n").encode())


def _dispatch(ev: dict, role: str, bare_metal: bool,
              controller_port: int, topo_config_file: str) -> dict:
    et  = ev.get("type")
    p   = ev.get("payload", {})
    net = _current_net

    if net is None:
        return {"ok": False, "err": "Mininet not ready"}

    if et == "LINK_SET":
        return _handle_link_set(net, p)

    if et == "EXEC":
        return _handle_exec(net, p)

    if et == "SNAPSHOT":
        return _handle_snapshot(net)

    if et == "RELOAD":
        return _handle_reload(topo_config_file, role,
                              bare_metal, controller_port)

    return {"ok": False, "err": f"Unknown primitive: {et}"}


# ─────────────────────────────────────────────────────────────
# Mininet startup
# ─────────────────────────────────────────────────────────────

def _start_mininet(role: str, bare_metal: bool,
                   controller_port: int,
                   topo_config_file: str,
                   relay_port: int = 0,
                   proto: str = "udp") -> None:
    """
    Build and start a Mininet network from topology_config.json.
    Saves reference to _current_net for the control server.
    In ON role: opens CLI (blocking).
    In NDT role: returns immediately (event loop is in dtm.py).
    """
    global _current_net

    topo_cfg = load_topology_config(topo_config_file)
    lans     = topo_cfg["lans"]
    nat_ip   = topo_cfg["backbone"]["nat_ip"]

    if role == "ndt":
        WAN_INTF     = "enp0s8"
        WAN_IP       = "172.16.2.2/30"
        OTHER_NET    = "172.16.1.0/24"
        VM_ROUTER_GW = "172.16.2.1"
        K8S_ENABLE   = True
        K8S_INTF     = "enp0s9"
        K8S_IP       = "192.168.99.1/30"
    else:
        WAN_INTF     = "enp0s8"
        WAN_IP       = "172.16.1.2/30"
        OTHER_NET    = "172.16.2.0/24"
        VM_ROUTER_GW = "172.16.1.1"
        K8S_ENABLE   = False

    # Physical interface flush — VM mode only
    if not bare_metal:
        os.system(f"ip addr flush dev {WAN_INTF}")
        if K8S_ENABLE:
            os.system(f"ip addr flush dev {K8S_INTF}")

    # Build network
    net = Mininet(controller=OVSController, link=TCLink)
    net.addController("c0", controller=OVSController, port=controller_port)

    info("*** Creating Backbone\n")
    s_core = net.addSwitch("s_core", dpid="100")
    r_nat  = net.addNAT("r_nat", ip=nat_ip, connect=True)
    r_nat.cmd("sysctl -w net.ipv4.ip_forward=1")
    net.addLink(r_nat, s_core)

    routers_lan = []
    for lan in lans:
        lan_id    = lan["id"]
        subnet    = lan["subnet"]
        num_hosts = lan["hosts"]
        info(f"*** LAN {lan_id}: {subnet}.x, {num_hosts} hosts\n")

        router = net.addHost(f"r{lan_id}", cls=Node)
        routers_lan.append((lan_id, router, subnet))
        switch = net.addSwitch(f"s{lan_id}")
        net.addLink(router, switch)
        net.addLink(router, s_core)

        for h in range(1, num_hosts + 1):
            h_node = net.addHost(
                f"h{lan_id}_{h}",
                ip=f"{subnet}.{h + 10}/24",
                defaultRoute=f"via {subnet}.1",
            )
            net.addLink(h_node, switch)

    info("*** Starting network\n")
    net.start()

    with _net_lock:
        _current_net = net

    # Physical interface binding — VM mode only
    if not bare_metal:
        info(f"*** Binding {WAN_INTF} to r_nat\n")
        Intf(WAN_INTF, node=r_nat)
        r_nat.cmd(f"ip addr add {WAN_IP} dev {WAN_INTF}")
        r_nat.cmd(f"ip link set {WAN_INTF} up")
        if K8S_ENABLE:
            info(f"*** Binding {K8S_INTF} to r_nat\n")
            Intf(K8S_INTF, node=r_nat)
            r_nat.cmd(f"ip addr add {K8S_IP} dev {K8S_INTF}")
            r_nat.cmd(f"ip link set {K8S_INTF} up")
        r_nat.cmd(f"ip route add {OTHER_NET} via {VM_ROUTER_GW}")

    # Internal routes — always needed
    for lan_id, _, subnet in routers_lan:
        r_nat.cmd(f"ip route add {subnet}.0/24 via 10.0.0.{lan_id}")

    # NAT
    r_nat.cmd("iptables -t nat -A POSTROUTING -s 192.168.0.0/16 -j MASQUERADE")

    # LAN router config
    for lan_id, router, subnet in routers_lan:
        router.cmd("sysctl -w net.ipv4.ip_forward=1")
        router.cmd(f"ifconfig r{lan_id}-eth0 {subnet}.1/24 up")
        router.cmd(f"ifconfig r{lan_id}-eth1 10.0.0.{lan_id}/24 up")
        router.cmd("route add default gw 10.0.0.254")

    # DNS
    for host in net.hosts:
        if host.name.startswith("h"):
            host.cmd("echo 'nameserver 8.8.8.8' > /etc/resolv.conf")

    export_topology(net)

    if role == "on":
        if bare_metal:
            # In container mode there is no interactive stdin.
            # Keep process alive — onm.py drives everything via localhost:6001.
            info("*** [ON] Mininet ready — keeper running (container mode)\n")
            while True:
                time.sleep(1)
        else:
            # VM mode — open interactive Mininet CLI
            info("*** [ON] Mininet ready — CLI started\n")
            CLI(net)
            net.stop()
    else:
        info("*** [NDT] Mininet ready — waiting for dtm-agent\n")


# ─────────────────────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(
        description="on_ndt.py — Mininet keeper. "
                    "Control logic lives in onm.py (ON) and dtm.py (NDT)."
    )
    ap.add_argument("--role",            choices=["on", "ndt"], required=True)
    ap.add_argument("--bare-metal",      action="store_true",
                    help="Skip WAN/K8S interface config (container mode)")
    ap.add_argument("--controller-port", type=int, default=6653,
                    help="OVS controller port (unique per container)")
    ap.add_argument("--topo-file",       default=TOPO_CONFIG_FILE,
                    help="Path to topology config JSON")
    args = ap.parse_args()

    os.makedirs("/run", exist_ok=True)
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    # Start control server in background
    threading.Thread(
        target=_control_server,
        args=(args.role, args.bare_metal,
              args.controller_port, args.topo_file),
        daemon=True,
    ).start()

    # Start Mininet (blocks in ON role via CLI, returns in NDT role)
    _start_mininet(
        role            = args.role,
        bare_metal      = args.bare_metal,
        controller_port = args.controller_port,
        topo_config_file= args.topo_file,
    )

    # NDT role: keep process alive (dtm.py drives everything)
    if args.role == "ndt":
        info("*** [NDT] Keeper running. dtm.py controls this node.\n")
        while True:
            time.sleep(1)


if __name__ == "__main__":
    setLogLevel("info")
    main()