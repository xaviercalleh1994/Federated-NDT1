"""
Authors: Xavier Calle-Heredia & Xavier Hesselbach
Date: 2026-04-08

Changes vs original (NDT role only — ON role is completely unchanged)
----------------------------------------------------------------------
- Added --relay-port, --proto, --dtm-ip, --ack-port, --bare-metal,
  --controller-port arguments.
- Added TcpServerReceiver: NDT binds and waits for DTM to connect (TCP).
- Added UdpReceiver: NDT binds a UDP port (UDP mode).
- Added event_loop(): receives physical LINK_SET / ROUTE_REASSERT / EXEC_CMD
  from the DTM, calls handle_event(), sends UDP ACK back to DTM.
- --bare-metal: skips WAN/K8S interface config (enp0s8/enp0s9 do not exist
  on the second bare-metal machine).
- --controller-port: each container gets a unique OVS controller port to
  avoid conflicts when running N instances on the same host.
"""
import os
import json
import time
import socket
import threading
import argparse
from typing import Dict, Any

from mininet.net import Mininet
from mininet.node import OVSController, Node
from mininet.nodelib import NAT
from mininet.cli import CLI
from mininet.log import setLogLevel, info
from mininet.link import TCLink, Intf

PID_FILE  = "/run/mininet.pid"
TOPO_FILE = "/run/topology.json"

CONTROL_HOST = "127.0.0.1"
CONTROL_PORT = 6001


def configurar_router(router: Node):
    router.cmd("sysctl -w net.ipv4.ip_forward=1")


def export_topology(net: Mininet, filename: str = TOPO_FILE):
    """
    Exporta topología de Mininet:
      - nodes: hosts + switches
      - links: pares [a,b]
    """
    nodes = [n.name for n in net.hosts + net.switches]
    links = []
    for l in net.links:
        links.append([l.intf1.node.name, l.intf2.node.name])

    # Define en donde se va a guardar la topología
    os.makedirs("/run", exist_ok=True)
    with open(filename, "w") as f:
        json.dump({"nodes": nodes, "links": links}, f, indent=2)

    info(f"*** [MININET] Topology exported in {filename}\n")


def apply_tc(intf: str, delay_ms: float, loss_pct: float, bw_mbps: float):
    """
    Aplica políticas de QoS (tc traffic control, qdisc colas) para enlaces cuando se generan eventos:
      - rate (bw) con tbf (token bucket filter para control de ancho de banda)
      - delay/loss con netem (network emulator: simula imperfecciones en el enlace)
    """

    # Se debe recrear el enlace
    os.system(f"tc qdisc del dev {intf} root 2>/dev/null")

    rate = max(0.1, float(bw_mbps))
    burst = "64kbit"
    latency = "400ms"
    # Se controlan los parametros para el enlace
    os.system(
        f"tc qdisc add dev {intf} root handle 1: tbf "
        f"rate {rate}mbit burst {burst} latency {latency} 2>/dev/null"
    )

    d = max(0.0, float(delay_ms))
    l = max(0.0, float(loss_pct))
    # añade delay y packet losses
    os.system(
        f"tc qdisc add dev {intf} parent 1:1 handle 10: netem "
        f"delay {d}ms loss {l}% 2>/dev/null"
    )


def find_link(net: Mininet, a: str, b: str):
    """
    Encuentra un link físico en net.links.
    """
    for l in net.links:
        n1 = l.intf1.node.name
        n2 = l.intf2.node.name
        if (n1 == a and n2 == b) or (n1 == b and n2 == a):
            return l
    return None


def handle_event(net: Mininet, ev: Dict[str, Any]) -> Dict[str, Any]:
    """
    Función para ejecutar los eventos en ON y NDT:
      - SNAPSHOT: volver a exportar la topología
      - LINK_SET: set up/down + tc on a PHYSICAL adjacent link
          payload: {a,b, up, delay_ms, loss_pct, bw_mbps}
      - ROUTE_REASSERT: reponer default route / ip_forward en routers
          payload: {routers: [...], gateway: "10.0.0.254"}
      - EXEC_CMD: ejecutar un comando dentro del namespace del nodo Mininet
          payload: {node: "r1", cmd: "ip route"}
    """

    # tipo de evento
    et = ev.get("type")
    # payload
    p = ev.get("payload", {})

    if et == "SNAPSHOT":
        export_topology(net)
        return {"ok": True, "msg": "snapshot exported", "file": TOPO_FILE}

    if et == "LINK_SET":
        a = p["a"]
        b = p["b"]
        # bajar o subir el enlace
        up = bool(p.get("up", True))
        delay_ms = float(p.get("delay_ms", 0.0))
        loss_pct = float(p.get("loss_pct", 0.0))
        bw_mbps = float(p.get("bw_mbps", 1000.0))

        # se llama a la función para verificar si hay enlace directo entre ambos nodos
        l = find_link(net, a, b)
        if l is None:
            return {"ok": False, "err": f"There is no physical link between nodes {a} y {b}"}

        l.intf1.ifconfig("up" if up else "down")
        l.intf2.ifconfig("up" if up else "down")

        if up:
            # llama a la función que aplica QoS en el enlace
            apply_tc(str(l.intf1), delay_ms, loss_pct, bw_mbps)
            apply_tc(str(l.intf2), delay_ms, loss_pct, bw_mbps)
        else:
            # limpia qdisc para evitar acumualaciones
            os.system(f"tc qdisc del dev {str(l.intf1)} root 2>/dev/null")
            os.system(f"tc qdisc del dev {str(l.intf2)} root 2>/dev/null")

        return {"ok": True, "msg": "applied", "applied": p}

    if et == "ROUTE_REASSERT":
        routers = p.get("routers", ["r1", "r2"])
        # gatewaw por defecto de ambos routers
        gw = p.get("gateway", "10.0.0.254")

        for r in routers:
            try:
                node = net.get(r)
                node.cmd("sysctl -w net.ipv4.ip_forward=1")
                node.cmd(f"ip route replace default via {gw}")
            except Exception as e:
                return {"ok": False, "err": f"{r}: {e}"}

        return {"ok": True, "msg": "routes reasserted", "routers": routers, "gateway": gw}

    if et == "EXEC_CMD":
        node_name = p["node"]
        cmd = p["cmd"]
        try:
            out = net.get(node_name).cmd(cmd)
            return {"ok": True, "node": node_name, "cmd": cmd, "output": out.strip()}
        except Exception as e:
            return {"ok": False, "err": str(e)}

    return {"ok": False, "err": f"Unknown event type: {et}"}


def control_server(net: Mininet):
    """
    Servidor TCP local (JSON por línea):
      - envías: {"type": "...", "payload": {...}}\n
      - recibes: {"ok": true/false, ...}\n
    """
    # Crea un scoket TCP para garantizar orden y entregas. La comunicación ON-NDT es UDP
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # permite reutilizar el puerto y evitar bloqueos
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # els ervidor escuchará solo localmente
    srv.bind((CONTROL_HOST, CONTROL_PORT))
    # socket en modo server
    srv.listen(5)
    info(f"*** Control server {CONTROL_HOST}:{CONTROL_PORT}\n")

    while True:
        conn, _ = srv.accept()
        with conn:
            buf = b""
            while True:
                data = conn.recv(4096)
                if not data:
                    break
                buf += data
                while b"\n" in buf:
                    # separar los JSON que terminan en \n
                    line, buf = buf.split(b"\n", 1)
                    if not line.strip():
                        continue
                    try:
                        # parsea el evento
                        ev = json.loads(line.decode())
                        resp = handle_event(net, ev)
                    except Exception as e:
                        resp = {"ok": False, "err": str(e)}
                    conn.sendall((json.dumps(resp) + "\n").encode())


# ── NEW (Exp. 1 — NDT role only) ─────────────────────────────────────────────

def recv_framed(sock: socket.socket) -> bytes:
    """Read a [4-byte length][payload] TCP frame."""
    hdr = b""
    while len(hdr) < 4:
        chunk = sock.recv(4 - len(hdr))
        if not chunk:
            return b""
        hdr += chunk
    size = int.from_bytes(hdr, "big")
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            return b""
        data += chunk
    return data


class TcpServerReceiver:
    """
    TCP server: the NDT binds a port and waits for the DTM to connect.
    Each accepted connection is handled in its own daemon thread.
    Received frames are put into self.queue for event_loop() to consume.
    """
    def __init__(self, port: int) -> None:
        import queue
        self.queue: queue.Queue = queue.Queue()
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", port))
        srv.listen(5)
        info(f"*** [NDT] TCP server listening on port {port}\n")
        threading.Thread(target=self._accept_loop,
                         args=(srv,), daemon=True).start()

    def _accept_loop(self, srv):
        while True:
            conn, addr = srv.accept()
            info(f"*** [NDT] DTM connected from {addr}\n")
            threading.Thread(target=self._recv_loop,
                             args=(conn,), daemon=True).start()

    def _recv_loop(self, conn):
        while True:
            data = recv_framed(conn)
            if not data:
                break
            self.queue.put(data)


class UdpReceiver:
    """
    UDP receiver: the NDT binds a port and receives datagrams from the DTM.
    Received payloads are put into self.queue for event_loop() to consume.
    """
    def __init__(self, port: int) -> None:
        import queue
        self.queue: queue.Queue = queue.Queue()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 8 * 1024 * 1024)
        sock.bind(("0.0.0.0", port))
        info(f"*** [NDT] UDP receiver listening on port {port}\n")
        threading.Thread(target=self._recv_loop,
                         args=(sock,), daemon=True).start()

    def _recv_loop(self, sock):
        while True:
            data, _ = sock.recvfrom(65535)
            if data:
                self.queue.put(data)


def event_loop(net: Mininet, relay_port: int, proto: str,
               dtm_ip: str, ack_port: int) -> None:
    """
    Main loop for the NDT role (Exp. 1).

    Receives physical LINK_SET / ROUTE_REASSERT / EXEC_CMD events from the
    DTM, calls handle_event(), and sends a UDP ACK back to the DTM
    FeedbackController so it can track completion.

    Uses TcpServerReceiver or UdpReceiver depending on --proto.
    """
    import queue as qmod
    import orjson

    # Create the receiver — DTM connects to us (TCP) or sends datagrams (UDP)
    if proto == "tcp":
        rx = TcpServerReceiver(relay_port)
    else:
        rx = UdpReceiver(relay_port)

    # UDP socket to send ACKs back to the DTM FeedbackController
    ack_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    info(f"*** [NDT] ACK -> {dtm_ip}:{ack_port}\n")
    info(f"*** [NDT] Event loop ready — {proto.upper()} port={relay_port}\n")

    while True:
        try:
            data = rx.queue.get(timeout=1.0)
        except qmod.Empty:
            continue

        t_rx_ns = time.time_ns()
        try:
            ev = orjson.loads(data)
        except Exception:
            continue

        event_id = str(ev.get("event_id", ""))

        # Apply the physical event in this Mininet instance
        result = handle_event(net, ev)

        t_apply_ns = time.time_ns()
        apply_ms   = (t_apply_ns - t_rx_ns) / 1e6

        info(f"*** [NDT] {ev.get('type')} ok={result.get('ok')} "
             f"apply={apply_ms:.1f}ms\n")

        # Send UDP ACK to DTM FeedbackController
        if dtm_ip:
            ack = orjson.dumps({
                "type":       "NDT_APPLY_ACK",
                "event_id":   event_id,
                "port":       relay_port,
                "ok":         result.get("ok", False),
                "t_apply_ns": t_apply_ns,
                "apply_ms":   apply_ms,
            })
            try:
                ack_sock.sendto(ack, (dtm_ip, ack_port))
            except Exception as exc:
                info(f"*** [NDT] ACK send error: {exc}\n")

# ─────────────────────────────────────────────────────────────────────────────


def redComplejaFinal(role: str, relay_port: int = 7001, proto: str = "tcp",
                     dtm_ip: str = "", ack_port: int = 9000,
                     bare_metal: bool = False,
                     controller_port: int = 6653):
    """
    APlicable tanto para ON y NDT, debe recibir el rol.
    Configura rutas en routers y host, asegura nat para acceso a redes externas

    Nuevo: control_server en thread y export_topology al inicio.

    NEW (Exp. 1):
      - bare_metal: skip WAN/K8S interface config (enp0s8/enp0s9 absent on
        the second bare-metal machine).
      - controller_port: unique OVS controller port per container instance.
      - relay_port, proto, dtm_ip, ack_port: passed to event_loop() for NDT.
    """
    if role == "ndt":
        print("--- MODE: DIGITAL TWIN (DT) ---")
        WAN_INTF = "enp0s8"
        WAN_IP = "172.16.2.2/30"
        OTHER_NET = "172.16.1.0/24"
        VM_ROUTER_GW = "172.16.2.1"
        K8S_ENABLE = True
        K8S_INTF = "enp0s9"
        K8S_IP = "192.168.99.1/30"
    else:
        print("--- MODE: ORIGINAL NETWORK (ON) ---")
        WAN_INTF = "enp0s8"
        WAN_IP = "172.16.1.2/30"
        OTHER_NET = "172.16.2.0/24"
        VM_ROUTER_GW = "172.16.1.1"
        K8S_ENABLE = False

    # NEW (Exp. 1): on bare-metal the WAN/K8S interfaces do not exist —
    # skip all physical interface config. The DTM connects to this machine
    # directly via its LAN IP; no relay interface is needed.
    if not bare_metal:
        # Limpiar las interfaces que conectan ON-NDT y la interfaz para K8S en DT
        os.system(f"ip addr flush dev {WAN_INTF}")
        if K8S_ENABLE:
            os.system(f"ip addr flush dev {K8S_INTF}")

    # Crea el controlador para la red para que se controle mediante software
    # NEW (Exp. 1): controller_port allows each Docker container to use a
    # different OVS port so N instances do not collide on port 6653.
    net = Mininet(controller=OVSController, link=TCLink) # TCLink permite usar TC para los eventos
    net.addController("c0", controller=OVSController, port=controller_port)

    # Crear el switch principal
    info("*** Creating Backbone\n")
    s_core = net.addSwitch("s_core", dpid="100")

    # Se va a crear el router principal r_nat que no forma parte ni de ON ni NDT. Por decisión
    # r_nat es parte del ONM y DTM. r_nat conecta ON-NDT y da salida a internet
    r_nat = net.addNAT("r_nat", ip="10.0.0.254/24", connect=True)
    # Activar el forwarding
    configurar_router(r_nat)
    # Conectar al backbone
    net.addLink(r_nat, s_core)

    # Creación de LANs
    routers_lan = []
    for i in range(1, 3):
        # subredes en rango 10 o 20
        subnet_val = i * 10
        info(f"*** Creating LAN {i} (192.168.{subnet_val}.x)\n")

        # Crea el router y el switch
        router = net.addHost(f"r{i}", cls=Node)
        routers_lan.append(router)
        switch = net.addSwitch(f"s{i}")

        # Conecta el router con el switch local
        net.addLink(router, switch)
        # Conecta el router con el switch del backbone
        net.addLink(router, s_core)

        # Crea los hosts de cada LAN
        for h in range(1, 3):
            host_name = f"h{i}_{h}"
            host_ip = f"192.168.{subnet_val}.{h + 10}/24"
            gateway = f"192.168.{subnet_val}.1"
            h_node = net.addHost(host_name, ip=host_ip, defaultRoute=f"via {gateway}")
            net.addLink(h_node, switch)

    # Inicia la red: se establecen interfaces y namespaces para cada nodo
    info("*** Starting network\n")
    net.start()

    # NEW (Exp. 1): only bind physical interfaces when they exist on this host
    if not bare_metal:
        # Conectar r_nat a las intefaces "físicas" de la VM
        info(f"*** Connecting {WAN_INTF} to r_nat\n")
        Intf(WAN_INTF, node=r_nat)
        r_nat.cmd(f"ip addr add {WAN_IP} dev {WAN_INTF}")
        r_nat.cmd(f"ip link set {WAN_INTF} up")

        if K8S_ENABLE:
            info(f"*** Conectando {K8S_INTF} a r_nat\n")
            Intf(K8S_INTF, node=r_nat)
            r_nat.cmd(f"ip addr add {K8S_IP} dev {K8S_INTF}")
            r_nat.cmd(f"ip link set {K8S_INTF} up")

        # Agregar las rutas en r_nat
        r_nat.cmd("ip route add 192.168.10.0/24 via 10.0.0.1")
        r_nat.cmd("ip route add 192.168.20.0/24 via 10.0.0.2")
        info(f"*** Adding route to {OTHER_NET} via {VM_ROUTER_GW}\n")
        r_nat.cmd(f"ip route add {OTHER_NET} via {VM_ROUTER_GW}")

    # Nateo para salida externa
    r_nat.cmd("iptables -t nat -A POSTROUTING -s 192.168.0.0/16 -j MASQUERADE")

    # COnfiguracion de cada router LAN
    for i, router in enumerate(routers_lan, start=1):
        subnet_val = i * 10
        configurar_router(router)
        router.cmd(f"ifconfig r{i}-eth0 192.168.{subnet_val}.1/24 up")
        router.cmd(f"ifconfig r{i}-eth1 10.0.0.{i}/24 up")
        router.cmd("route add default gw 10.0.0.254")

    # Añadir DNS en hosts
    for host in net.hosts:
        if host.name.startswith("h"):
            host.cmd("echo 'nameserver 8.8.8.8' > /etc/resolv.conf")

    # llama a la función para exportar la topología para crear la matriz
    export_topology(net)

    # Control server local mediante hilos
    threading.Thread(target=control_server, args=(net,), daemon=True).start()

    if role == "ndt":
        # NEW (Exp. 1): enter event loop — receives events from DTM and applies them
        event_loop(net, relay_port, proto, dtm_ip, ack_port)
    else:
        # para debuggear:
        info("*** CLI started\n")
        CLI(net)
        net.stop()


if __name__ == "__main__":
    setLogLevel("info")
    ap = argparse.ArgumentParser()
    ap.add_argument("--role",            choices=["on", "ndt"], required=True)
    ap.add_argument("--relay-port",      type=int, default=7001,
                    help="Port this NDT listens on (TCP server or UDP)")
    ap.add_argument("--proto",           choices=["tcp", "udp"], default="tcp",
                    help="Transport protocol for DTM→NDT events")
    ap.add_argument("--dtm-ip",          default="",
                    help="DTM IP for UDP ACK replies (empty = disabled)")
    ap.add_argument("--ack-port",        type=int, default=9000,
                    help="DTM FeedbackController UDP port for ACKs")
    ap.add_argument("--bare-metal",      action="store_true",
                    help="Skip WAN/K8S interface config (bare-metal, not VM)")
    ap.add_argument("--controller-port", type=int, default=6653,
                    help="OVS controller port (unique per container instance)")
    args = ap.parse_args()

    os.makedirs("/run", exist_ok=True)
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

    redComplejaFinal(
        role            = args.role,
        relay_port      = args.relay_port,
        proto           = args.proto,
        dtm_ip          = args.dtm_ip,
        ack_port        = args.ack_port,
        bare_metal      = args.bare_metal,
        controller_port = args.controller_port,
    )