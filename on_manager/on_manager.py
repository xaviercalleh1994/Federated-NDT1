#!/usr/bin/env python3
import json
import time
import uuid
import socket
import argparse
from dataclasses import dataclass, asdict
from typing import Dict, List, Tuple, Optional
from collections import defaultdict, deque

TOPO_FILE = "/run/topology.json"
LOG_FILE = "/tmp/onm_events.log"


def log_event(tag: str, data: dict):
    entry = {
        "ts_ns": time.time_ns(),
        "tag": tag,
        **data
    }
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")


# Como se definió en el control de eventos del script para levantar la red
# Control por TCP
CONTROL_HOST = "127.0.0.1"
CONTROL_PORT = 6001

# COmunicación con la NDT por UDP
UDP_PORT = 7001  


# Define una class para los enlaces para tener un grafo ponderado en lugar de una matriz binaria
@dataclass
class Edge:
    delay_ms: float
    loss_pct: float
    bw_mbps: float
    up: bool


# Define una clase para implementar la matriz lógica
class Matrix:
    """
    Matriz de adyacencia lógica:
      - nodos lógicos: no switches (r_nat, r1, r2, h1_1, ...)
      - aristas lógicas: existe un camino con SOLO switches como intermediarios
        (ej: r1 - s_core - r2 => link lógico r1<->r2)
    """
    def __init__(self):
        self.idx: Dict[str, int] = {} # índice del nodo [i][j]
        self.rev: Dict[int, str] = {} # nombre del nodo
        self.m: List[List[Optional[Edge]]] = [] # matriz, cada celda tiene Edge o None

    # Función para garantizar que el nodo está en la matriz
    def _ensure(self, n: str):
        # si el nodo existe no hace nada
        if n in self.idx:
            return
        # asigna un nuevo índice
        i = len(self.idx)
        self.idx[n] = i
        self.rev[i] = n
        for row in self.m:
            row.append(None)
        self.m.append([None] * (i + 1))

    # definir el enlace entre los nodos a y b
    def set(self, a: str, b: str, e: Edge):
        # ver que los nodos existen
        self._ensure(a); self._ensure(b)
        i, j = self.idx[a], self.idx[b]
        # enlaces simétricos para un grafo no dirijido por ahora
        self.m[i][j] = e
        self.m[j][i] = e  

    # Devuelve el enlace
    def get(self, a: str, b: str) -> Optional[Edge]:
        if a not in self.idx or b not in self.idx:
            return None
        return self.m[self.idx[a]][self.idx[b]]

    # Imprime la matriz sin repetir enlaces a-b b-a
    def show(self):
        print("\nLogic Nodes:")
        for n, i in self.idx.items():
            print(f"  {i}: {n}")

        print("\nLINKS (derivados de la matriz):")
        printed = set()
        for i in range(len(self.m)):
            for j in range(len(self.m)):
                e = self.m[i][j]
                if e is None:
                    continue
                a, b = self.rev[i], self.rev[j]
                key = tuple(sorted([a, b]))
                if key in printed:
                    continue
                printed.add(key)
                print(f"  {a} <-> {b} | up={e.up} delay={e.delay_ms}ms loss={e.loss_pct}% bw={e.bw_mbps}Mbps")


# Esta función se usa para evitar poner switches en la matriz de adyacencia
def is_switch(n: str) -> bool:
    return n.startswith("s")


# Convierte el json en un grafo
def load_topo_graph() -> Tuple[List[str], List[Tuple[str, str]], Dict[str, List[str]]]:
    with open(TOPO_FILE) as f:
        t = json.load(f)
    nodes = t["nodes"]
    links = [(a, b) for a, b in t["links"]]
    g = defaultdict(list)
    for a, b in links:
        g[a].append(b)
        g[b].append(a)
    return nodes, links, g


def path_only_switches_middle(g: Dict[str, List[str]], src: str, dst: str) -> List[Tuple[str, str]]:
    """
    Retorna camino físico (lista de aristas adyacentes) de src a dst
    PERO solo si los nodos intermedios son switches.
    Ejemplos:
      r1 -> s_core -> r2  (OK)
      h1_1 -> s1 -> r1    (OK)
      r_nat -> s_core -> r1 (OK)
      r_nat -> s_core -> r1 -> s1 -> h1_1 (NO, porque r1 es intermedio y no es switch)
    """
    q = deque([src])
    prev = {src: None}

    while q:
        cur = q.popleft()
        if cur == dst:
            break
        for nxt in g[cur]:
            if nxt in prev:
                continue
            # si nxt va a ser intermedio (no destino), debe ser switch
            # y si cur va a ser intermedio, debe ser switch (excepto src)
            prev[nxt] = cur
            q.append(nxt)

    if dst not in prev:
        return []

    # reconstruir nodos
    path_nodes = []
    cur = dst
    while cur is not None:
        path_nodes.append(cur)
        cur = prev[cur]
    path_nodes.reverse()

    # validar intermediarios: SOLO switches
    mids = path_nodes[1:-1]
    if any(not is_switch(x) for x in mids):
        return []

    return [(path_nodes[i], path_nodes[i + 1]) for i in range(len(path_nodes) - 1)]


# Función para enviar eventos hacia el script mininet que tiene el event handler
# Control TCP
def control_call(ev: dict) -> dict:
    # Crear el scoket TCP
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


# Función para aplicar los cambios a la matriz y la ON
def apply_logical_edge_to_on(g, a: str, b: str, e: Edge) -> bool:
    """
    Convierte un enlace lógico (a<->b) a una secuencia de tramos físicos adyacentes
    (a - switch - ... - b), y aplica LINK_SET en cada tramo.
    """

    # Encuentra el camino lógico entre los nodos a y b
    phys_edges = path_only_switches_middle(g, a, b)
    if not phys_edges:
        print(f"[ON] There is no logic path between {a} and {b}")
        return False

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
            print("[ON] Error aplicando tramo físico", x, y, resp)
            return False

    log_event("ON_EVENT_APPLIED", {
        "a": a,
        "b": b,
        "up": e.up
    })

    # Reponer la ruta por defecto cuando se baja un enlace
    rr = control_call({
        "type": "ROUTE_REASSERT",
        "payload": {"routers": ["r1", "r2"], "gateway": "10.0.0.254"}
    })
    if not rr.get("ok"):
        print("[ON] Warning: ROUTE_REASSERT falló:", rr)

    return True


# ENvía el evento por UDP hacia la NDT
def send_udp(ndt_ip: str, ev: dict):
    # Define un socket UDP
    u = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    u.sendto(json.dumps(ev).encode(), (ndt_ip, UDP_PORT))
    u.close()
    #s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #s.connect((ndt_ip, 7001))
    #s.sendall(json.dumps(ev).encode())
    #s.close()


# Crear la matriz inicial
def build_initial_matrix(nodes: List[str], g) -> Matrix:
    logical = [n for n in nodes if not is_switch(n)]
    # llamar a la clase Matrix
    M = Matrix()

    # Añadir únicamente enlaces lógicos (solo switches intermedios)
    for i in range(len(logical)):
        for j in range(i + 1, len(logical)):
            a, b = logical[i], logical[j]
            phys = path_only_switches_middle(g, a, b)
            if phys:
                M.set(a, b, Edge(delay_ms=0.0, loss_pct=0.0, bw_mbps=1000.0, up=True))

    return M


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--ndt-ip", required=True, help="IP reachable NDT VM (ej. 172.16.2.2)")
    args = ap.parse_args()

    # Crear la matriz
    nodes, _, g = load_topo_graph()
    M = build_initial_matrix(nodes, g)

    print("[ON_MANAGER] Initial adjacency matrix created.")
    # LLamo al método show para mostrar la matriz inicial
    M.show()

    print("\nComandos:")
    print("  show")
    print("  set A B up|down delay_ms loss_pct bw_mbps")
    print('     ej: set r1 r2 up 50 1.0 10')
    print('     ej: set r1 r2 down 0 0 1000')
    print('  exec NODE \"comando\"')
    print('     ej: exec r1 "ip route"')
    print("  quit\n")

    while True:
        line = input("DTM> ").strip()
        if not line:
            continue

        parts = line.split()
        cmd = parts[0].lower()

        if cmd == "quit":
            break

        if cmd == "show":
            M.show()
            continue

        if cmd == "set":
            # set A B up|down delay loss bw  => 7 tokens
            if len(parts) != 7:
                print("Uso: set A B up|down delay_ms loss_pct bw_mbps")
                continue

            a, b = parts[1], parts[2]
            up = parts[3].lower() == "up"
            e = Edge(
                delay_ms=float(parts[4]),
                loss_pct=float(parts[5]),
                bw_mbps=float(parts[6]),
                up=up
            )

            # 1) actualizar matriz
            M.set(a, b, e)

            #######################################
            ############ LOGS EVENTS ##############
            #######################################
            event_id = str(uuid.uuid4())
            t_create_ns = time.time_ns()


            log_event("ONM_EVENT_CREATED", {
                "event_id": event_id,
                "event": "LOGICAL_LINK_SET",
                "a": a,
                "b": b,
                "params": asdict(e)
            })

            # 2) aplicar en ON (físico)
            ok = apply_logical_edge_to_on(g, a, b, e)
            if not ok:
                print("[ON_MANAGER] No se pudo aplicar en ON (enlace lógico inválido).")
                continue

            # 3) replicar en NDT
            #send_udp(args.ndt_ip, {
                #"type": "LOGICAL_LINK_SET",
                #"payload": {"a": a, "b": b, **asdict(e)}
            #})

            ev = {
                "type": "LOGICAL_LINK_SET",
                "event_id": event_id,
                "t_create_ns": t_create_ns,
                "payload": {"a": a, "b": b, **asdict(e)}
            }
            send_udp(args.ndt_ip, ev)


            log_event("ONM_EVENT_SENT", {
                "event_id": event_id,
                "a": a,
                "b": b,
                "up": e.up,
                "t_create_ns": t_create_ns,
                "size_bytes": len(json.dumps(ev).encode()),
                "dst": args.ndt_ip,
                "udp_port": UDP_PORT
            })


            print("[SYNC] Evento enviado a NDT")
            continue

        if cmd == "exec":
            # exec NODE "cmd ..."
            if len(parts) < 3:
                print('Uso: exec NODE "comando"')
                continue
            node = parts[1]
            shell_cmd = line.split(None, 2)[2].strip()
            if shell_cmd.startswith('"') and shell_cmd.endswith('"'):
                shell_cmd = shell_cmd[1:-1]

            ev = {"type": "EXEC_CMD", "payload": {"node": node, "cmd": shell_cmd}}

            # ejecutar en ON
            resp = control_call(ev)
            print("[ON EXEC]", resp)

            # replicar en NDT
            send_udp(args.ndt_ip, ev)
            print("[SYNC] EXEC_CMD enviado a NDT")
            continue

        print("Comando no reconocido.")


if __name__ == "__main__":
    main()