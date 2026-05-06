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

import sys
import time
import argparse


def main():
    ap = argparse.ArgumentParser(description="Inyector de alta precisión para ONM")
    ap.add_argument("--file", default="onm_commands.txt", help="Archivo con comandos")
    ap.add_argument("--rate", type=float, required=True, help="Eventos por segundo (ev/s)")
    ap.add_argument("--duration", type=float, required=True, help="Duración del experimento en segundos")
    args = ap.parse_args()

    try:
        with open(args.file, "r") as f:
            cmds = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    except FileNotFoundError:
        print(f"[INJECTOR] Error: Archivo {args.file} no encontrado.", file=sys.stderr)
        sys.exit(1)

    if not cmds:
        print("[INJECTOR] Error: El archivo de comandos está vacío.", file=sys.stderr)
        sys.exit(1)

    # Imprimimos logs informativos a stderr para que no se mezclen en la tubería (pipe)
    print(f"[INJECTOR] Iniciando inyección: {args.rate} ev/s durante {args.duration} segundos...", file=sys.stderr)

    interval = 1.0 / args.rate
    end_time = time.time() + args.duration
    idx = 0
    events_sent = 0

    # Pausa inicial para darle tiempo al ONM de levantar
    time.sleep(2)

    # Bucle de alta precisión
    next_time = time.perf_counter()
    while time.time() < end_time:
        # Se imprime el comando a stdout (que viaja por el pipe hacia el ONM)
        # flush=True es CRÍTICO para que no se quede atascado en el buffer del SO
        print(cmds[idx], flush=True)

        idx = (idx + 1) % len(cmds)
        events_sent += 1

        # Calcular el tiempo exacto a dormir para mantener la tasa constante
        next_time += interval
        sleep_time = next_time - time.perf_counter()
        if sleep_time > 0:
            time.sleep(sleep_time)

    # Damos un segundo para que los buffers terminen de vaciarse
    time.sleep(1)

    # Cerramos el ONM limpiamente
    print("quit", flush=True)
    print(f"[INJECTOR] Inyección finalizada. {events_sent} comandos enviados en total.", file=sys.stderr)


if __name__ == "__main__":
    main()