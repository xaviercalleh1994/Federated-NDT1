#!/bin/bash

# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# ============================================================
# stop_testbed.sh — stops all testbed pods and services
#
# Works for both 1:1 and federated modes.
# Detects automatically which pods are running.
#
# Usage:
#   ./stop_testbed.sh
#   ./stop_testbed.sh ./logs/run1
# ============================================================

set -euo pipefail

LOG_DST=${1:-""}
NAMESPACE="ndt"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Save logs ────────────────────────────────────────────────
if [ -n "${LOG_DST}" ]; then
    mkdir -p "${LOG_DST}"
    echo "Saving logs to ${LOG_DST}..."

    for pod_container in \
        "pod-on-onm:on" \
        "pod-on-onm:onm" \
        "pod-dtm-ndt:ndt" \
        "pod-dtm-ndt:dtm" \
        "pod-dtm:dtm"; do
        pod="${pod_container%%:*}"
        container="${pod_container##*:}"
        if kubectl get pod "${pod}" -n "${NAMESPACE}" \
           > /dev/null 2>&1; then
            kubectl logs "${pod}" -n "${NAMESPACE}" \
                -c "${container}" \
                > "${LOG_DST}/${pod}-${container}.log" 2>/dev/null || true
            echo "  ${pod}/${container} → ${LOG_DST}/${pod}-${container}.log"
        fi
    done

    # NDT pods (federated)
    NDTS=$(kubectl get pods -n "${NAMESPACE}" \
        --selector="app=ndt-exp1" \
        --no-headers \
        -o custom-columns=":metadata.name" 2>/dev/null || true)
    for pod in ${NDTS}; do
        kubectl logs "${pod}" -n "${NAMESPACE}" -c ndt \
            > "${LOG_DST}/${pod}-ndt.log" 2>/dev/null || true
        kubectl logs "${pod}" -n "${NAMESPACE}" -c dtm-agent \
            > "${LOG_DST}/${pod}-dtm-agent.log" 2>/dev/null || true
        echo "  ${pod} → ${LOG_DST}/${pod}-*.log"
    done
    echo "Logs saved."
    echo ""
fi

# ── Stop NDT pods (federated mode) ───────────────────────────
if kubectl get pods -n "${NAMESPACE}" \
   --selector="app=ndt-exp1" --no-headers 2>/dev/null | grep -q .; then
    echo "Stopping NDT pods..."
    "${REPO_ROOT}/ndt_deployment/mininet_ndts/stop_ndts.sh"
fi

# ── Stop core pods ───────────────────────────────────────────
echo "Stopping core pods..."
for pod in pod-on pod-onm pod-on-onm pod-dtm pod-dtm-ndt; do
    kubectl delete pod "${pod}" -n "${NAMESPACE}" \
        --ignore-not-found \
        --grace-period=0 \
        --force > /dev/null 2>&1 || true
done

# ── Stop services ─────────────────────────────────────────────
echo "Stopping services..."
kubectl delete service svc-dtm svc-on -n "${NAMESPACE}" \
    --ignore-not-found > /dev/null 2>&1 || true

echo "Done."