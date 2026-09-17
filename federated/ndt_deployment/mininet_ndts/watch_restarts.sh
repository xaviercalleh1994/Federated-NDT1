#!/bin/bash

# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# ============================================================
# watch_restarts.sh — NDT restart daemon
#
# Runs on the host alongside the testbed. Watches for restart
# signal files written by the DTM core:
#   /tmp/dtm-state/ndt-{id}/restart_requested
#
# When a signal file is detected, calls restart_ndt.sh to
# perform the pod restart via kubectl. The DTM container has
# no dependency on kubectl — this daemon is the only component
# that calls kubectl for pod restarts.
#
# Started by launch_testbed.sh in background.
# Stopped by stop_testbed.sh via PID file.
#
# Usage (managed by launch_testbed.sh):
#   ./watch_restarts.sh DTM_IP BASE_PORT [PROTO] [ACK_PORT] &
#
# PID file: /tmp/watch_restarts.pid
# ============================================================

set -euo pipefail

DTM_IP=${1:?"Usage: watch_restarts.sh DTM_IP BASE_PORT [PROTO] [ACK_PORT]"}
BASE_PORT=${2:?"Usage: watch_restarts.sh DTM_IP BASE_PORT [PROTO] [ACK_PORT]"}
PROTO=${3:-udp}
ACK_PORT=${4:-9000}

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PID_FILE="/tmp/watch_restarts.pid"
STATE_BASE="/tmp/dtm-state"
POLL_INTERVAL=1   # seconds between checks

echo "[WATCH] NDT restart daemon starting"
echo "[WATCH] DTM_IP=${DTM_IP} BASE_PORT=${BASE_PORT} PROTO=${PROTO}"
echo "[WATCH] Watching ${STATE_BASE}/ndt-*/restart_requested"
echo "[WATCH] PID=$$"

echo $$ > "${PID_FILE}"

# ── Main loop ────────────────────────────────────────────────
while true; do
    # Check all ndt-* directories for restart_requested signal
    for signal_file in "${STATE_BASE}"/ndt-*/restart_requested; do
        [ -f "${signal_file}" ] || continue

        # Extract NDT_ID from path (e.g. /tmp/dtm-state/ndt-0/restart_requested → 0)
        NDT_ID=$(basename "$(dirname "${signal_file}")" | grep -o '[0-9]*')

        echo "[WATCH] Restart signal detected for ndt-${NDT_ID}"

        # Remove signal file before restart to avoid double-processing
        rm -f "${signal_file}"

        # Use XRM env vars passed from launch_testbed.sh
        # XRM and XRM_CORE_IP are set in the environment if --xrm was used
        _XRM="${XRM:-0}"
        _XRM_CORE_IP="${XRM_CORE_IP:-${DTM_IP}}"

        # Call restart_ndt.sh
        XRM="${_XRM}" \
        XRM_CORE_IP="${_XRM_CORE_IP}" \
        bash "${SCRIPT_DIR}/restart_ndt.sh" \
            "${NDT_ID}" "${DTM_IP}" "${BASE_PORT}" \
            "${PROTO}" "${ACK_PORT}" \
            >> /tmp/watch_restarts.log 2>&1 &

        echo "[WATCH] restart_ndt.sh launched for ndt-${NDT_ID} (background)"
    done

    sleep "${POLL_INTERVAL}"
done