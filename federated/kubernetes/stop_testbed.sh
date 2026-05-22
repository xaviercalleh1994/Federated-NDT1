#!/bin/bash

# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# ============================================================
# stop_testbed.sh — Stop all testbed pods and services
#
# Stops all pods and services in the ndt namespace.
# Optionally copies logs to a destination directory.
#
# Usage:
#   ./stop_testbed.sh [LOG_DIR]
#
# Example:
#   ./stop_testbed.sh ./logs/run1/1to4
# ============================================================

set -euo pipefail

LOG_DIR="${1:-}"
NAMESPACE="ndt"

echo "=============================================="
echo " Stopping testbed"
echo "=============================================="

# ── Optionally collect logs ───────────────────────────────────
if [ -n "${LOG_DIR}" ]; then
    mkdir -p "${LOG_DIR}"
    echo "Collecting logs to ${LOG_DIR}..."

    # DTM log
    kubectl cp pod-dtm:/tmp/ndt-run/dtm.log \
        "${LOG_DIR}/dtm.log" 2>/dev/null || \
    kubectl logs pod-dtm -n "${NAMESPACE}" -c dtm \
        > "${LOG_DIR}/dtm.log" 2>/dev/null || true

    # ONM log
    kubectl logs pod-onm -n "${NAMESPACE}" \
        > "${LOG_DIR}/onm.log" 2>/dev/null || true

    # Per-NDT agent logs
    for pod in $(kubectl get pods -n "${NAMESPACE}" \
        --selector="app=ndt-exp1" --no-headers \
        -o custom-columns=":metadata.name" 2>/dev/null || true); do
        kubectl logs "${pod}" -n "${NAMESPACE}" -c dtm-agent \
            > "${LOG_DIR}/${pod}_agent.log" 2>/dev/null || true
    done

    echo "  Logs saved to ${LOG_DIR}"
fi

# ── Delete all pods ───────────────────────────────────────────
echo "Deleting pods..."
kubectl delete pods --all -n "${NAMESPACE}" \
    --grace-period=5 --wait=false 2>/dev/null || true

# ── Delete all services (except kubernetes) ───────────────────
echo "Deleting services..."
kubectl get services -n "${NAMESPACE}" --no-headers \
    -o custom-columns=":metadata.name" 2>/dev/null | \
    grep -v "^kubernetes$" | \
    xargs -r kubectl delete service -n "${NAMESPACE}" \
    --ignore-not-found 2>/dev/null || true

echo ""
echo "=============================================="
echo " Testbed stopped."
echo "=============================================="