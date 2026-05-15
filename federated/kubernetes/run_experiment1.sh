#!/bin/bash
# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# ============================================================
# run_experiment1.sh — Automated Experiment 1
# ============================================
# Measures L_dispatch, T_conv, F_sync and apply_ms vs federation size (N NDTs).
#
# For each N in FANOUT_LIST:
#   1. Launch testbed with --fanout N, DTM log saved to hostPath
#   2. Inject events via on_events_gen.py | onm.py
#   3. Optionally monitor CPU/RAM via kubectl top in background (--monitor)
#   4. Copy dtm.log (and resources.log) to results directory
#   5. Stop testbed
#
# Repeat RUN_COUNT times to get multiple runs for 95% CI.
#
# Results layout:
#   <OUT_DIR>/
#     run1/
#       1to1/dtm.log  [resources.log]
#       1to2/dtm.log  [resources.log]
#       ...
#     run2/
#       ...
#
# Usage:
#   ./run_experiment1.sh                         # default: no monitor
#   ./run_experiment1.sh --monitor               # enable kubectl top monitor
#   ./run_experiment1.sh --runs 5 --rate 2 --duration 120 --out /tmp/exp1
#
# Then analyse:
#   python3 ../analysis/experiment_1.py \
#       --logs /tmp/exp1 \
#       --out  /tmp/exp1/results
# ============================================================

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────
FANOUT_LIST=(1 2 4 8 16 32)     # N values to test — edit as needed
RUN_COUNT=5                      # number of runs per N for 95% CI
EVENT_RATE=0.5                   # events per second injected
DURATION=60                      # seconds per run
OUT_DIR="/tmp/exp1"              # results directory on the host
NAMESPACE="ndt"
RELAY_IP="127.0.0.1"
RELAY_PORT="7001"
PROTO="udp"
MONITOR=false                    # set to true with --monitor flag
MONITOR_INTERVAL=5               # seconds between kubectl top samples
DTM_LOG_CONTAINER="/run/dtm_exp1.log"
DTM_LOG_HOST="/tmp/ndt-run/dtm_exp1.log"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# ── Parse arguments ──────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --runs)     RUN_COUNT="$2";  shift 2 ;;
        --rate)     EVENT_RATE="$2"; shift 2 ;;
        --duration) DURATION="$2";   shift 2 ;;
        --out)      OUT_DIR="$2";    shift 2 ;;
        --monitor)  MONITOR=true;    shift 1 ;;
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

monitor_resources() {
    local dest=$1
    local res_log="${dest}/resources.log"
    > "${res_log}"
    while true; do
        local ts
        ts=$(date +%s%N)
        while read -r pod container cpu mem; do
            local cpu_m="${cpu%m}"
            local mem_mi="${mem%Mi}"
            echo "{\"ts_ns\":${ts},\"pod\":\"${pod}\",\"container\":\"${container}\",\"cpu_m\":${cpu_m},\"mem_mi\":${mem_mi}}" >> "${res_log}"
        done < <(kubectl top pods -n "${NAMESPACE}" --containers --no-headers 2>/dev/null)
        sleep "${MONITOR_INTERVAL}"
    done
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
echo " Experiment 1 — L_dispatch, T_conv, apply_ms"
echo "   Fanout values : ${FANOUT_LIST[*]}"
echo "   Runs          : ${RUN_COUNT}"
echo "   Event rate    : ${EVENT_RATE} ev/s"
echo "   Duration      : ${DURATION}s"
echo "   Monitor       : ${MONITOR}"
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

        # 2. Wait for all pods Running
        wait_pods_ready

        # 3. Wait for pod-onm to be Running and ready
        echo "    Waiting for pod-onm..."
        until kubectl get pod pod-onm -n "${NAMESPACE}" --no-headers 2>/dev/null | grep -q "Running"; do
            sleep 1
        done
        sleep 5

        # 4. Get ON IP
        ON_IP=$(get_on_ip)

        # 5. Start resource monitor in background (only if --monitor)
        MONITOR_PID=""
        if [ "${MONITOR}" = "true" ]; then
            echo "    Starting resource monitor (every ${MONITOR_INTERVAL}s)..."
            monitor_resources "${LOG_DIR}" &
            MONITOR_PID=$!
        fi

        # 6. Inject events (foreground — blocks for DURATION seconds)
        inject_events "${ON_IP}"

        # 7. Wait for ACKs to flush
        sleep 10

        # 8. Stop resource monitor if running
        if [ -n "${MONITOR_PID}" ]; then
            kill "${MONITOR_PID}" 2>/dev/null || true
            wait "${MONITOR_PID}" 2>/dev/null || true
            local_lines=$(wc -l < "${LOG_DIR}/resources.log" 2>/dev/null || echo 0)
            echo "    resources.log saved (${local_lines} samples)"
        fi

        # 9. Collect DTM log
        collect_dtm_log "${LOG_DIR}"

        # 10. Stop testbed and release ports
        echo "    Stopping testbed..."
        "${SCRIPT_DIR}/stop_testbed.sh" > /dev/null 2>&1 || true
        sudo fuser -k 7001/udp 2>/dev/null || true
        sudo fuser -k 7002/udp 2>/dev/null || true
        sudo fuser -k 9000/udp 2>/dev/null || true
        sleep 10

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