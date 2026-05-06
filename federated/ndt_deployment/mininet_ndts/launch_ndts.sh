#!/bin/bash

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

# ============================================================
# launch_ndts.sh — Experiment 1
#
# Launches N NDT Mininet containers using Docker bridge networking.
# Each container has its own network namespace — no veth collisions.
# ACK packets reach the DTM via a route added in entrypoint.sh.
#
# Usage:
#   ./launch_ndts.sh N DTM_IP [BASE_PORT] [PROTO] [ACK_PORT]
# ============================================================

set -euo pipefail

N=${1:-2}
DTM_IP=${2:?"Usage: ./launch_ndts.sh N DTM_IP [BASE_PORT] [PROTO] [ACK_PORT]"}
BASE_PORT=${3:-8001}
PROTO=${4:-tcp}
ACK_PORT=${5:-9000}
MAX_WAIT=120

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE="ndt-mininet"
PREFIX="exp1-ndt"
LOG_DIR="${SCRIPT_DIR}/logs/current"

mkdir -p "${LOG_DIR}"

# ── Check prerequisites ──────────────────────────────────────
if ! docker image inspect "${IMAGE}" > /dev/null 2>&1; then
    echo "ERROR: image '${IMAGE}' not found."
    echo "Build it: docker build -t ${IMAGE} ${SCRIPT_DIR}"
    exit 1
fi

if [ ! -f "${SCRIPT_DIR}/on_ndt.py" ]; then
    echo "ERROR: on_ndt.py not found"
    exit 1
fi

# ── Clean previous state ─────────────────────────────────────
echo "Cleaning up..."
for i in $(seq 1 "${N}"); do
    docker rm -f "${PREFIX}-${i}" > /dev/null 2>&1 || true
done
sudo mn -c > /dev/null 2>&1 || true
sleep 1

echo "=============================================="
echo " Launching ${N} NDT containers"
echo "   Ports    : ${BASE_PORT} — $(( BASE_PORT + N - 1 ))"
echo "   Protocol : ${PROTO}"
echo "   DTM IP   : ${DTM_IP}"
echo "   ACK port : ${ACK_PORT}"
echo "=============================================="
echo ""

# ── Launch containers and wait for each before starting next ─
# Containers use Docker bridge (isolated network namespace per container).
# This prevents Mininet veth name collisions between instances.
# The entrypoint adds a host route so ACK UDP packets reach the DTM.
ALL_OK=true

for i in $(seq 1 "${N}"); do
    PORT=$(( BASE_PORT + i - 1 ))
    CTRL_PORT=$(( 6653 + i - 1 ))
    LOG_JSON="${LOG_DIR}/ndt_${i}.log"
    LOG_MN="${LOG_DIR}/ndt_${i}_mininet.log"

    touch "${LOG_JSON}" "${LOG_MN}" 2>/dev/null || true
    chmod 666 "${LOG_JSON}" "${LOG_MN}" 2>/dev/null || true

    echo "  Starting NDT ${i}  port=${PORT}  ctrl=${CTRL_PORT}..."

    # Bridge networking: each container gets own namespace.
    # Port mapping exposes NDT_PORT to the host so the DTM can connect.
    # ACK route to DTM is added inside entrypoint.sh.
    docker run -d \
        --name "${PREFIX}-${i}" \
        --privileged \
        -p "${PORT}:${PORT}/tcp" \
        -p "${PORT}:${PORT}/udp" \
        -e NDT_PORT="${PORT}" \
        -e NDT_PROTO="${PROTO}" \
        -e DTM_IP="${DTM_IP}" \
        -e ACK_PORT="${ACK_PORT}" \
        -e CTRL_PORT="${CTRL_PORT}" \
        -v "${SCRIPT_DIR}/on_ndt.py":/app/on_ndt.py:ro \
        "${IMAGE}" > /dev/null

    # Wait for this NDT to be fully ready before launching the next.
    # Since each container has its own namespace, the wait is just for
    # Mininet to finish starting — not to avoid race conditions.
    elapsed=0
    until docker logs "${PREFIX}-${i}" 2>&1 | grep -q "Event loop ready"; do
        sleep 1
        elapsed=$(( elapsed + 1 ))
        if [ "${elapsed}" -ge "${MAX_WAIT}" ]; then
            echo "  NDT ${i} TIMEOUT — check: docker logs ${PREFIX}-${i}"
            ALL_OK=false
            break
        fi
    done

    if docker logs "${PREFIX}-${i}" 2>&1 | grep -q "Event loop ready"; then
        echo "  NDT ${i} (port=${PORT}): ready (${elapsed}s)"
        docker logs "${PREFIX}-${i}" > "${LOG_MN}" 2>&1 || true
    fi
    echo ""
done

if [ "${ALL_OK}" = false ]; then
    echo "ERROR: one or more NDTs failed. Run: ./stop_ndts.sh"
    exit 1
fi

# ── Print commands ────────────────────────────────────────────
HOST_IP=$(hostname -I | awk '{print $1}')
echo "All ${N} NDTs ready."
echo ""
echo "Step 1 — VM DTM:"
echo "──────────────────────────────────────────────────"
echo "mkdir -p /home/ubuntu/exp1/1to${N}"
echo "python3 dtm.py \\"
echo "    --proto ${PROTO} \\"
echo "    --relay-ip 172.16.2.1 \\"
echo "    --relay-port 7001 \\"
echo "    --mode federated \\"
echo "    --fanout ${N} \\"
echo "    --dst-ip ${HOST_IP} \\"
echo "    --base-port ${BASE_PORT} \\"
echo "    --feedback-port ${ACK_PORT} \\"
echo "    --experiment-duration 75 \\"
echo "    --log-file /home/ubuntu/exp1/1to${N}/dtm.log"
echo ""
echo "Step 2 — VM ON:"
echo "──────────────────────────────────────────────────"
echo "sudo python3 on_ndt.py --role on"
echo ""
echo "Step 3 — VM ON (once Mininet is up):"
echo "──────────────────────────────────────────────────"
echo "python3 onm.py --proto ${PROTO} --relay-ip 172.16.1.1 --relay-port 7001 --relay-kind router"
echo ""
echo "When done: ./stop_ndts.sh ./logs/run1/1to${N}"