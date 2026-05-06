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
"""

import argparse
import socket
import threading
import time

# ---------- helpers ----------

def recv_line(sock):
    buf = b""
    while True:
        c = sock.recv(1)
        if not c:
            return None
        if c == b"\n":
            return buf.decode().strip()
        buf += c


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            return None
        buf += chunk
    return buf


# ---------- TCP RELAY ----------

def tcp_forward(on_sock, ndt_sock):
    print("[RELAY] Forwarding enabled (ON → NDT)")
    while True:
        hdr = recv_exact(on_sock, 4)
        if not hdr:
            print("[RELAY] ON disconnected")
            break

        ln = int.from_bytes(hdr, "big")
        payload = recv_exact(on_sock, ln)
        if not payload:
            print("[RELAY] Incomplete payload")
            break

        ndt_sock.sendall(hdr + payload)
        print(f"[RELAY] Forwarded {ln} bytes")


def tcp_relay(port):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("0.0.0.0", port))
    srv.listen(5)
    print(f"[RELAY] TCP listening on {port}")

    on_sock = None
    ndt_sock = None

    while True:
        conn, addr = srv.accept()
        role = recv_line(conn)

        if role == "ON":
            on_sock = conn
            print(f"[RELAY] ON connected from {addr}")
        elif role == "NDT":
            ndt_sock = conn
            print(f"[RELAY] NDT connected from {addr}")
        else:
            print(f"[RELAY] Unknown role '{role}' from {addr}")
            conn.close()
            continue

        if on_sock and ndt_sock:
            tcp_forward(on_sock, ndt_sock)
            break


# ---------- UDP RELAY ----------

def udp_relay(port):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind(("0.0.0.0", port))
    print(f"[RELAY] UDP listening on {port}")

    ndt_addr = None

    while True:
        data, addr = s.recvfrom(65535)

        if data == b"HELLO_NDT":
            ndt_addr = addr
            print(f"[RELAY] NDT registered from {addr}")
            continue

        if not ndt_addr:
            continue

        s.sendto(data, ndt_addr)
        print(f"[RELAY] UDP forwarded {len(data)} bytes")


# ---------- main ----------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--proto", choices=["tcp", "udp"], required=True)
    ap.add_argument("--port", type=int, default=7001)
    args = ap.parse_args()

    if args.proto == "tcp":
        tcp_relay(args.port)
    else:
        udp_relay(args.port)


if __name__ == "__main__":
    main()