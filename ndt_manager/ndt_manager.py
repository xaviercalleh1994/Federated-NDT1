#!/usr/bin/env python3
import json
import time
import socket
import argparse
from dataclasses import dataclass
from typing import Dict, List, Tuple
from collections import defaultdict, deque

# Ruta de la topología exportada durante la creación de la red
TOPO_FILE = "/run/topology.json"
# Control TCP para replicar eventos de la ON en la NDT
CONTROL_HOST = "127.0.0.1"
CONTROL_PORT = 6001
# Puerto UDP donde escucha los eventos
UDP_PORT = 7001

LOG_FILE = "/tmp/ndtm_events.log"


def log_event(tag: str, data: dict):
    entry = {
        "ts_ns": time.time_ns(),
        "tag": tag,
        **data
    }
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


# define la clase para los enlaces 
@dataclass
class Edge:
    delay_ms: float
    loss_pct: float
    bw_mbps: float
    up: bool


# identifica si el nodo es switch para saltarlo
def is_switch(n: str) -> bool:
    return n.startswith("s")


# Construye el grafo a partir del json guardado 
def load_topo_graph() -> Dict[str, List[str]]:
    with open(TOPO_FILE) as f:
        t = json.load(f)
    links = [(a, b) for a, b in t["links"]]
    g = defaultdict(list)
    for a, b in links:
        g[a].append(b)
        g[b].append(a)
    return g


# define el camino lógico entre nodos
def path_only_switches_middle(g: Dict[str, List[str]], src: str, dst: str) -> List[Tuple[str, str]]:
    q = deque([src])
    prev = {src: None}

    while q:
        cur = q.popleft()
        if cur == dst:
            break
        for nxt in g[cur]:
            if nxt in prev:
                continue
            prev[nxt] = cur
            q.append(nxt)

    if dst not in prev:
        return []

    path_nodes = []
    cur = dst
    while cur is not None:
        path_nodes.append(cur)
        cur = prev[cur]
    path_nodes.reverse()

    mids = path_nodes[1:-1]
    if any(not is_switch(x) for x in mids):
        return []

    return [(path_nodes[i], path_nodes[i + 1]) for i in range(len(path_nodes) - 1)]


# Aplica el evento con control TCP
def control_call(ev: dict) -> dict:
    # Crea el scoket TCP
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((CONTROL_HOST, CONTROL_PORT))
    s.sendall((json.dumps(ev) + "\n").encode())
    data = b""
    while b"\n" not in data:
        chunk = s.recv(4096)
        if not chunk:
            break
        data += chunk
    s.close()
    if not data:
        return {"ok": False, "err": "no response"}
    return json.loads(data.split(b"\n")[0].decode())


# APlicar el evento en la NDT
def apply_logical_link(g, a: str, b: str, e: Edge) -> int:
    # encontrar el cmaino lógico
    phys_edges = path_only_switches_middle(g, a, b)
    if not phys_edges:
        print(f"[NDT] No existe enlace lógico directo (solo switches intermedios) entre {a} y {b}")
        return

    for x, y in phys_edges:
        resp = control_call({
            "type": "LINK_SET",
            "payload": {
                "a": x, "b": y,
                "up": e.up,
                "delay_ms": e.delay_ms,
                "loss_pct": e.loss_pct,
                "bw_mbps": e.bw_mbps
            }
        })
        if not resp.get("ok"):
            print("[NDT] Error aplicando tramo", x, y, resp)
            return

    # Igual que en ON: reassert rutas
    rr = control_call({
        "type": "ROUTE_REASSERT",
        "payload": {"routers": ["r1", "r2"], "gateway": "10.0.0.254"}
    })
    if not rr.get("ok"):
        print("[NDT] Warning: ROUTE_REASSERT falló:", rr)

    print(f"[NDT] Replicado {a}<->{b} sobre {len(phys_edges)} tramos físicos.")

    return len(phys_edges)

    #log_event("NDT_EVENT_APPLIED", {
        #"a": a,
        #"b": b,
        #"up": e.up
    #})


def main():
    g = load_topo_graph()

    u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    u.bind(("0.0.0.0", UDP_PORT))

    #s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #s.bind(("127.0.0.1", 7001))
    #s.listen(5)
    #print("[NDT_MANAGER] Escuchando TCP 7001")

    #while True:
        #conn, addr = s.accept()
        #with conn:
            #data = conn.recv(65535)
            #if not data:
                #continue
            #ev = json.loads(data.decode())
            # sigue igual ↓

    print(f"[NDT_MANAGER] Escuchando UDP {UDP_PORT}")

    while True:
        #data, addr = u.recvfrom(65535)
        #ev = json.loads(data.decode())

        data, addr = u.recvfrom(65535)
        t_rx_ns = time.time_ns()
        ev = json.loads(data.decode())

        t = ev.get("type")

        event_id = ev.get("event_id", "NA")
        t_create_ns = ev.get("t_create_ns", None)


        if t == "LOGICAL_LINK_SET":
            p = ev["payload"]
            e = Edge(delay_ms=p["delay_ms"], loss_pct=p["loss_pct"], bw_mbps=p["bw_mbps"], up=p["up"])

            #log_event("NDTM_EVENT_RECEIVED", {
                #"event": "LOGICAL_LINK_SET",
                #"payload": p
            #})

            log_event("NDTM_EVENT_RX", {
                "event_id": event_id,
                "t_create_ns": t_create_ns,
                "t_rx_ns": t_rx_ns,
                "src": addr[0],
                "src_port": addr[1],
                "size_bytes": len(data)
            })            

            #apply_logical_link(g, p["a"], p["b"], e)

            #t_apply_ns = time.time_ns()
            #log_event("NDT_EVENT_APPLIED", {
                #"event_id": event_id,
                #"t_create_ns": t_create_ns,
                #"t_rx_ns": t_rx_ns,
                #"t_apply_ns": t_apply_ns,
                #"a": a,
                #"b": b,
                #"up": e.up,
                #"segments": len(phys_edges)
            #})            

            segments = apply_logical_link(g, p["a"], p["b"], e)

            t_apply_ns = time.time_ns()
            log_event("NDT_EVENT_APPLIED", {
                "event_id": event_id,
                "t_create_ns": t_create_ns,
                "t_rx_ns": t_rx_ns,
                "t_apply_ns": t_apply_ns,
                "a": p["a"],
                "b": p["b"],
                "up": e.up,
                "segments": segments
            })

            #log_event("NDT_EVENT_APPLIED", {
                #"a": a,
                #"b": b,
                #"up": e.up
            #})

            continue

        if t == "EXEC_CMD":
            print(f"[NDT] EXEC_CMD recibido: {ev}")
            resp = control_call(ev)
            print("[NDT EXEC]", resp)
            continue

        print("[NDT] Evento desconocido:", ev)


if __name__ == "__main__":
    main()