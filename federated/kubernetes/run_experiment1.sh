#!/bin/bash
# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# ============================================================
# run_experiment1.sh — Automated Experiment 1
# ============================================
# Measures L_dispatch and T_conv vs federation size (N NDTs).
#
# For each N in FANOUT_LIST:
#   1. Launch testbed with --fanout N, DTM log saved to hostPath
#   2. Inject events via on_events_gen.py | onm.py
#   3. Copy dtm.log to results directory
#   4. Stop testbed
#
# Repeat RUN_COUNT times to get multiple runs for 95% CI.
#
# Results layout:
#   <OUT_DIR>/
#     run1/
#       1to1/dtm.log
#       1to2/dtm.log
#       ...
#     run2/
#       ...
#
# Usage:
#   ./run_experiment1.sh
#   ./run_experiment1.sh --runs 5 --rate 1 --duration 60 --out /tmp/exp1
#
# Then analyse:
#   python3 ../analysis/experiment_1.py \
#       --logs /tmp/exp1 \
#       --out  /tmp/exp1/results
# ============================================================

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────
FANOUT_LIST=(1 2 4 8 16 32 64)  # N values to test — edit as needed
RUN_COUNT=5               # number of runs per N for 95% CI
EVENT_RATE=0.5            # events per second injected
DURATION=60               # seconds per run
OUT_DIR="/tmp/exp1"       # results directory on the host
NAMESPACE="ndt"
RELAY_IP="127.0.0.1"
RELAY_PORT="7001"
PROTO="udp"
DTM_LOG_CONTAINER="/run/dtm_exp1.log"    # inside DTM container (via hostPath)
DTM_LOG_HOST="/tmp/ndt-run/dtm_exp1.log" # same file on host

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Parse arguments ──────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --runs)     RUN_COUNT="$2";  shift 2 ;;
        --rate)     EVENT_RATE="$2"; shift 2 ;;
        --duration) DURATION="$2";   shift 2 ;;
        --out)      OUT_DIR="$2";    shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# ── Helpers ───────────────────────────────────────────────────

wait_pods_ready() {
    local timeout=180
    echo "    Waiting for all pods Running..."
    for i in $(seq 1 $timeout); do
        not_ready=$(kubectl get pods -n "${NAMESPACE}" \
            --no-headers 2>/dev/null \
            | grep -vc "Running" || true)
        if [ "${not_ready}" -eq 0 ]; then
            echo "    All pods Running (${i}s)"
            return 0
        fi
        sleep 1
    done
    echo "    ERROR: pods not ready after ${timeout}s"
    kubectl get pods -n "${NAMESPACE}"
    exit 1
}

get_on_ip() {
    kubectl get pod pod-on -n "${NAMESPACE}" \
        -o jsonpath='{.status.podIP}' 2>/dev/null || echo "127.0.0.1"
}

inject_events() {
    local on_ip=$1
    echo "    Injecting events (rate=${EVENT_RATE} ev/s, duration=${DURATION}s)..."
    kubectl exec -i pod-onm -n "${NAMESPACE}" -- bash -c \
        "python3 /app/on_events_gen.py \
            --file /app/onm_commands.txt \
            --rate ${EVENT_RATE} \
            --duration ${DURATION} \
         | python3 /app/onm.py \
            --proto ${PROTO} \
            --relay-ip ${RELAY_IP} \
            --relay-port ${RELAY_PORT} \
            --relay-kind router \
            --on-ip ${on_ip} \
            --log-file /tmp/onm.log" 2>&1 | tail -3
    echo "    Injection complete."
}

collect_dtm_log() {
    local dest=$1
    echo "    Collecting dtm.log..."
    if [ -f "${DTM_LOG_HOST}" ]; then
        cp "${DTM_LOG_HOST}" "${dest}/dtm.log"
        local lines
        lines=$(wc -l < "${dest}/dtm.log")
        echo "    dtm.log saved (${lines} lines)"
    else
        echo "    WARNING: ${DTM_LOG_HOST} not found, falling back to kubectl logs"
        kubectl logs pod-dtm -n "${NAMESPACE}" 2>/dev/null \
            | grep '^{' > "${dest}/dtm.log" || true
        local lines
        lines=$(wc -l < "${dest}/dtm.log")
        echo "    dtm.log saved via kubectl logs (${lines} lines)"
    fi
}

cleanup_dtm_log() {
    sudo rm -f "${DTM_LOG_HOST}" 2>/dev/null || true
}

# ── Main ─────────────────────────────────────────────────────
echo "=============================================="
echo " Experiment 1 — L_dispatch and T_conv vs N"
echo "   Fanout values : ${FANOUT_LIST[*]}"
echo "   Runs          : ${RUN_COUNT}"
echo "   Event rate    : ${EVENT_RATE} ev/s"
echo "   Duration      : ${DURATION}s"
echo "   Output        : ${OUT_DIR}"
echo "=============================================="
echo ""

mkdir -p "${OUT_DIR}"

for run in $(seq 1 "${RUN_COUNT}"); do
    echo "══════════════════════════════════════════════"
    echo " RUN ${run}/${RUN_COUNT}"
    echo "══════════════════════════════════════════════"

    for N in "${FANOUT_LIST[@]}"; do
        echo ""
        echo "  ── N=${N} NDTs ─────────────────────────────"

        LOG_DIR="${OUT_DIR}/run${run}/1to${N}"
        mkdir -p "${LOG_DIR}"

        # Clean previous DTM log
        cleanup_dtm_log

        # 1. Launch testbed
        echo "    Launching testbed (fanout=${N})..."
        "${SCRIPT_DIR}/launch_testbed.sh" \
            --mode federated \
            --fanout "${N}" \
            --dtm-log "${DTM_LOG_CONTAINER}" \
            2>&1 | grep -E "ready|ERROR|TIMEOUT|NDTs|DTM|ONM" || true

        # 2. Wait for pods
        wait_pods_ready

        # 3. Give DTM time to initialize
        sleep 3

        # 4. Get ON IP and inject events
        ON_IP=$(get_on_ip)
        inject_events "${ON_IP}"

        # 5. Wait for ACKs to flush
        sleep 3

        # 6. Collect DTM log
        collect_dtm_log "${LOG_DIR}"

        # 7. Stop testbed
        echo "    Stopping testbed..."
        "${SCRIPT_DIR}/stop_testbed.sh" > /dev/null 2>&1 || true
        sleep 3

        echo "  ── N=${N} done ✓"
    done

    echo ""
    echo " Run ${run}/${RUN_COUNT} complete."
    echo ""
done

echo "=============================================="
echo " All runs complete."
echo " Results in: ${OUT_DIR}"
echo ""
echo " Analyse with:"
echo "   python3 ${REPO_ROOT}/analysis/experiment_1.py \\"
echo "       --logs ${OUT_DIR} \\"
echo "       --out  ${OUT_DIR}/results"
echo "=============================================="