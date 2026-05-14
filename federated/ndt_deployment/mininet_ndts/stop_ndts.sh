#!/bin/bash

# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# ============================================================
# stop_ndts.sh — Kubernetes version
#
# Stops and removes all NDT pods from the 'ndt' namespace.
# Optionally saves logs to a destination directory before deletion.
#
# Usage:
#   ./stop_ndts.sh [LOG_DST_DIR]
#
# Examples:
#   ./stop_ndts.sh                          # just stop
#   ./stop_ndts.sh ./logs/run1/1to8        # save logs then stop
# ============================================================

set -euo pipefail

LOG_DST=${1:-""}
NAMESPACE="ndt"
PREFIX="exp1-ndt"

# ── Save logs if destination provided ───────────────────────
if [ -n "${LOG_DST}" ]; then
    mkdir -p "${LOG_DST}"
    echo "Saving NDT logs to ${LOG_DST}..."

    PODS=$(kubectl get pods -n "${NAMESPACE}" \
        --selector="app=ndt-exp1" \
        --no-headers \
        -o custom-columns=":metadata.name" 2>/dev/null || true)

    for pod in ${PODS}; do
        INDEX=$(kubectl get pod "${pod}" -n "${NAMESPACE}" \
            -o jsonpath='{.metadata.labels.ndt-index}' 2>/dev/null || echo "x")
        LOG_FILE="${LOG_DST}/ndt_${INDEX}_mininet.log"
        echo "  Saving logs of ${pod} → ${LOG_FILE}"
        kubectl logs "${pod}" -n "${NAMESPACE}" > "${LOG_FILE}" 2>/dev/null || true
    done

    echo "Logs saved."
    echo ""
fi

# ── Delete all NDT pods and services ─────────────────────────
echo "Stopping all NDT pods in namespace '${NAMESPACE}'..."

kubectl delete pods \
    --selector="app=ndt-exp1" \
    --namespace="${NAMESPACE}" \
    --grace-period=0 \
    --force \
    --ignore-not-found=true 2>/dev/null || true

echo "Deleting all NDT services in namespace '${NAMESPACE}'..."

kubectl delete services \
    --selector="app=ndt-exp1" \
    --namespace="${NAMESPACE}" \
    --ignore-not-found=true 2>/dev/null || true

# ── Wait until all pods are gone ────────────────────────────
echo "Waiting for pods to terminate..."
MAX_WAIT=30
elapsed=0
until [ -z "$(kubectl get pods -n "${NAMESPACE}" \
    --selector="app=ndt-exp1" \
    --no-headers 2>/dev/null)" ] || [ "${elapsed}" -ge "${MAX_WAIT}" ]; do
    sleep 1
    elapsed=$(( elapsed + 1 ))
done

if [ "${elapsed}" -ge "${MAX_WAIT}" ]; then
    echo "WARNING: some pods may still be terminating."
    kubectl get pods -n "${NAMESPACE}" --selector="app=ndt-exp1" || true
else
    echo "All NDT pods stopped."
fi

# ── Clean leftover Mininet state (optional, same as Docker version) ──
# Only if mn is available on the host
if command -v mn > /dev/null 2>&1; then
    echo "Cleaning Mininet state..."
    sudo mn -c > /dev/null 2>&1 || true
fi

echo "Done."