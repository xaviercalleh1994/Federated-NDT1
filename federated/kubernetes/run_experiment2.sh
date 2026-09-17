#!/bin/bash
# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# ============================================================
# run_experiment2.sh — Automated Experiment 2
# ============================================
# Measures XR service coordination performance with N NDTs
# competing concurrently to apply actuator commands on the ON.
#
# Fixed parameters:
#   FANOUT=5 NDTs
#   Service: actuator-control
#   Each NDT has a different base priority (10, 30, 50, 70, 90)
#   Each NDT alternates high/medium/low urgency commands
#
# Variable parameters (iterated):
#   RATE_LIST    — commands per second per NDT
#   QUEUE_LIST   — DTM upstream AdmissionQueue max size (0=unlimited)
#
# Metrics collected per run (from ndt_event_cli --mode auto logs):
#   e2e_ms       — end-to-end latency (create → ACK from ONM)
#   wait_ms      — time waiting in DTM queue
#   apply_ms     — application time on ON
#   your_status  — ACK / QUEUED / DISCARDED / TIMEOUT
#
# Results layout:
#   <OUT_DIR>/
#     run1/
#       rate0.5_queue0/
#         ndt0.log ndt1.log ndt2.log ndt3.log ndt4.log
#         dtm_upstream.log onm.log
#       rate1_queue4/
#         ...
#     run2/
#       ...
#
# Usage:
#   ./run_experiment2.sh
#   ./run_experiment2.sh --runs 3 --out /tmp/exp2
#   ./run_experiment2.sh --runs 1 --rate-list "0.5 1" --queue-list "0 4"
#
# Then analyse:
#   python3 ../analysis/experiment_2_results.py \
#       --logs /tmp/exp2 \
#       --out  /tmp/exp2/results
# ============================================================

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────
FANOUT=4                          # fixed number of NDTs
SERVICE="actuator-control"
RUN_COUNT=3                       # runs per condition for 95% CI
DURATION=60                       # seconds per condition
OUT_DIR="/tmp/exp2"
NAMESPACE="ndt"
PROTO="udp"

# Variable parameters — space-separated lists
RATE_LIST=(0.5 1 2 4)            # commands/second per NDT
QUEUE_LIST=(0 2 4 8)             # DTM upstream queue size (0=unlimited)
PRIORITY_MODES=(priority fifo)   # priority=AdmissionQueue, fifo=--disable-priorities

# NDT base priorities (ndt-0=highest, ndt-3=lowest)
# Priority per NDT: 10 30 50 70
NDT_PRIORITIES=(10 30 50 70)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NDT_SCRIPT_DIR="${REPO_ROOT}/ndt_deployment/mininet_ndts"
COMMANDS_DIR="${SCRIPT_DIR}"     # commands_ndt{0..4}.txt live here
ANALYSIS_DIR="${REPO_ROOT}/analysis"

# ── Parse arguments ──────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --runs)       RUN_COUNT="$2";        shift 2 ;;
        --duration)   DURATION="$2";         shift 2 ;;
        --out)        OUT_DIR="$2";          shift 2 ;;
        --rate-list)      IFS=' ' read -r -a RATE_LIST      <<< "$2"; shift 2 ;;
        --queue-list)     IFS=' ' read -r -a QUEUE_LIST     <<< "$2"; shift 2 ;;
        --priority-modes) IFS=' ' read -r -a PRIORITY_MODES <<< "$2"; shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

# ── Helpers ───────────────────────────────────────────────────

wait_pods_ready() {
    local timeout=180
    echo "    Waiting for all pods Running..."
    for i in $(seq 1 ${timeout}); do
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

get_pod_ip() {
    # Get pod IP directly for Calico-direct communication
    # Avoids NodePort UDP NAT issues
    local pod_name=$1
    kubectl get pod "${pod_name}" -n "${NAMESPACE}" \
        -o jsonpath='{.status.podIP}' 2>/dev/null || echo ""
}

start_dtm_upstream() {
    # Start DTM upstream mode — nohup + </dev/null ensures process survives kubectl exec session
    # Args: queue_size prio_mode (priority|fifo)
    local queue_size=$1
    local prio_mode=${2:-priority}
    local log_file="/tmp/dtm_upstream_exp2.log"
    local max_queue_arg=""
    local disable_prio_arg=""
    if [ "${queue_size}" -gt 0 ]; then
        max_queue_arg="--max-queue ${queue_size}"
    fi
    if [ "${prio_mode}" = "fifo" ]; then
        disable_prio_arg="--disable-priorities"
    fi
    echo "    Starting DTM upstream (queue=${queue_size} mode=${prio_mode})..."
    kubectl exec pod-dtm -n "${NAMESPACE}" -- bash -c \
        "nohup python3 /app/dtm.py \
            --proto udp \
            --relay-ip 0.0.0.0 \
            --mode upstream \
            --onm-ip 127.0.0.1 \
            --log-file ${log_file} \
            ${max_queue_arg} \
            ${disable_prio_arg} \
            </dev/null >/tmp/dtm_upstream_stderr.log 2>&1 & echo \$! > /tmp/dtm_upstream.pid"
    # Wait for DTM upstream to be ready
    for i in $(seq 1 20); do
        if kubectl exec pod-dtm -n "${NAMESPACE}" --             grep -q "DTM-UPSTREAM.*Ready" /tmp/dtm_upstream_stderr.log 2>/dev/null; then
            echo "    DTM upstream ready (${i}s)"
            break
        fi
        sleep 1
    done
}

stop_dtm_upstream() {
    # Kill DTM upstream process in pod-dtm
    kubectl exec pod-dtm -n "${NAMESPACE}" -- bash -c \
        "pkill -f '/app/dtm.py.*upstream' 2>/dev/null; pkill -f 'dtm.py.*upstream' 2>/dev/null; true" 2>/dev/null || true
}

start_onm_upstream() {
    # Start ONM with upstream server — nohup + </dev/null ensures process survives kubectl exec
    local on_ip=$1
    echo "    Starting ONM with upstream server..."
    kubectl exec pod-onm -n "${NAMESPACE}" -- bash -c \
        "nohup python3 /app/onm.py \
            --proto ${PROTO} \
            --relay-ip 127.0.0.1 \
            --relay-port 7001 \
            --relay-kind router \
            --on-ip ${on_ip} \
            --upstream-server \
            --dtm-upstream-ip 127.0.0.1 \
            --log-file /tmp/onm_exp2.log \
            </dev/null >/tmp/onm_stderr.log 2>&1 & echo \$! > /tmp/onm.pid"
    # Wait for ONM upstream to be ready (message goes to log file)
    for i in $(seq 1 20); do
        if kubectl exec pod-onm -n "${NAMESPACE}" --             grep -q "ONM-UPSTREAM.*Ready" /tmp/onm_exp2.log 2>/dev/null; then
            echo "    ONM upstream ready (${i}s)"
            break
        fi
        sleep 1
    done
}

stop_onm_upstream() {
    kubectl exec pod-onm -n "${NAMESPACE}" -- bash -c \
        "pkill -f 'onm.py' 2>/dev/null || true" 2>/dev/null || true
}

deploy_service_all_ndts() {
    # Deploy actuator-control on all NDTs via XRM
    echo "    Deploying ${SERVICE} on all ${FANOUT} NDTs..."
    # Ensure state dirs are writable — pods run as root and create dirs owned by root
    sudo chmod -R 777 /tmp/xrm-state /tmp/dtm-state 2>/dev/null || true
    # Write xrm-state for each NDT directly (avoids "already deployed" error)
    for i in $(seq 0 $(( FANOUT - 1 ))); do
        mkdir -p "/tmp/xrm-state/ndt-${i}"
        echo "{\"ndt_id\":${i},\"services\":[\"${SERVICE}\"],\"context\":{}}" \
            > "/tmp/xrm-state/ndt-${i}/state.json"
    done

    # Restart all NDTs so they pick up the service
    for i in $(seq 0 $(( FANOUT - 1 ))); do
        echo "    Triggering restart for ndt-$(( i + 1 ))..."
        # Write restart signal directly
        mkdir -p "/tmp/dtm-state/ndt-${i}"
        echo "{\"ndt_id\":${i}}" > "/tmp/dtm-state/ndt-${i}/restart_requested"
    done

    # Wait for all NDTs to be ready with service container
    echo "    Waiting for NDTs to restart with service..."
    sleep 5
    local timeout=120
    for i in $(seq 1 ${timeout}); do
        all_ready=true
        for j in $(seq 1 ${FANOUT}); do
            containers=$(kubectl get pod "ndt-${j}" -n "${NAMESPACE}" \
                --no-headers -o custom-columns=":status.containerStatuses[*].ready" \
                2>/dev/null || echo "")
            # Count ready containers — expect 4 (ndt + dtm-agent + xrm-agent + service)
            ready_count=$(echo "${containers}" | tr ',' '\n' | grep -c "true" || true)
            if [ "${ready_count}" -lt 4 ]; then
                all_ready=false
                break
            fi
        done
        if [ "${all_ready}" = true ]; then
            echo "    All NDTs ready with service (${i}s)"
            return 0
        fi
        sleep 2
    done
    echo "    WARNING: NDTs may not have all 4 containers ready"
}

run_clients() {
    # Run ndt_event_cli in auto mode for all NDTs in parallel
    # Each NDT has its own rate, priority and command script
    local rate=$1
    local log_dir=$2
    local pids=()

    echo "    Launching ${FANOUT} ndt-event-cli processes (rate=${rate}/s)..."

    for i in $(seq 0 $(( FANOUT - 1 ))); do
        local pod_ip
        pod_ip=$(get_pod_ip "ndt-$(( i + 1 ))")
        if [ -z "${pod_ip}" ]; then
            echo "    WARNING: could not get IP for ndt-$(( i + 1 )), skipping"
            continue
        fi

        local priority=${NDT_PRIORITIES[$i]}
        local cmd_script="${COMMANDS_DIR}/commands_ndt${i}.txt"
        local log_file="${log_dir}/ndt${i}.log"

        python3 "${ANALYSIS_DIR}/ndt_event_cli.py" \
            --mode auto \
            --service "${SERVICE}" \
            --ndt-id "${i}" \
            --node-ip "$(kubectl get nodes \
                -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')" \
            --pod-ip "${pod_ip}" \
            --priority "${priority}" \
            --rate "${rate}" \
            --duration "${DURATION}" \
            --script "${cmd_script}" \
            --log-file "${log_file}" &

        pids+=($!)
        echo "    ndt-${i} started (pod_ip=${pod_ip} prio=${priority} pid=${pids[-1]})"
    done

    # Wait for all clients to finish
    echo "    Waiting for all clients to complete (${DURATION}s)..."
    for pid in "${pids[@]}"; do
        wait "${pid}" 2>/dev/null || true
    done
    echo "    All clients finished"
}

collect_logs() {
    local log_dir=$1
    echo "    Collecting logs..."

    # DTM upstream log
    kubectl exec pod-dtm -n "${NAMESPACE}" -- \
        cat /tmp/dtm_upstream.log 2>/dev/null \
        > "${log_dir}/dtm_upstream.log" || true

    # ONM log
    kubectl exec pod-onm -n "${NAMESPACE}" -- \
        cat /tmp/onm_exp2.log 2>/dev/null \
        > "${log_dir}/onm.log" || true

    # Per-NDT DTM agent logs
    for j in $(seq 1 ${FANOUT}); do
        kubectl logs "ndt-${j}" -n "${NAMESPACE}" -c dtm-agent \
            > "${log_dir}/dtm_agent_ndt${j}.log" 2>/dev/null || true
    done

    local total_lines=0
    for i in $(seq 0 $(( FANOUT - 1 ))); do
        if [ -f "${log_dir}/ndt${i}.log" ]; then
            lines=$(wc -l < "${log_dir}/ndt${i}.log" 2>/dev/null || echo 0)
            total_lines=$(( total_lines + lines ))
        fi
    done
    echo "    Logs collected (${total_lines} total client log lines)"
}

print_quick_summary() {
    local log_dir=$1
    local rate=$2
    local queue=$3
    local prio_mode=${4:-priority}
    echo ""
    echo "    ── Quick summary (rate=${rate} queue=${queue} mode=${prio_mode}) ──"
    python3 - "${log_dir}" << 'PYEOF'
import sys, json, os, glob

log_dir = sys.argv[1]
results = {}
for i in range(5):
    log_file = f"{log_dir}/ndt{i}.log"
    if not os.path.exists(log_file):
        continue
    lines = [json.loads(l) for l in open(log_file) if l.strip()]
    if not lines:
        continue
    acked     = [l for l in lines if l.get("your_status") == "ACK"]
    discarded = [l for l in lines if l.get("your_status") == "DISCARDED"]
    e2e_vals  = [l["e2e_ms"] for l in acked if l.get("e2e_ms")]
    results[i] = {
        "sent":      len(lines),
        "acked":     len(acked),
        "discarded": len(discarded),
        "e2e_avg":   round(sum(e2e_vals)/len(e2e_vals), 1) if e2e_vals else None,
        "e2e_max":   round(max(e2e_vals), 1) if e2e_vals else None,
    }

print(f"    {'NDT':<6} {'Sent':<6} {'ACK':<6} {'DISC':<6} {'e2e_avg':<10} {'e2e_max'}")
for i, r in sorted(results.items()):
    print(f"    ndt-{i:<2} {r['sent']:<6} {r['acked']:<6} {r['discarded']:<6} "
          f"{str(r['e2e_avg'])+'ms':<10} {r['e2e_max']}ms")
PYEOF
    echo ""
}

# ── Main ─────────────────────────────────────────────────────
echo "=============================================="
echo " Experiment 2 — XR Service Coordination"
echo "   Fanout       : ${FANOUT} NDTs (fixed)"
echo "   Service      : ${SERVICE}"
echo "   Rate list    : ${RATE_LIST[*]} cmd/s per NDT"
echo "   Queue list   : ${QUEUE_LIST[*]} (0=unlimited)"
echo "   Duration     : ${DURATION}s per condition"
echo "   Runs         : ${RUN_COUNT}"
echo "   Priorities   : ${NDT_PRIORITIES[*]}"
echo "   Output       : ${OUT_DIR}"
echo "=============================================="
echo ""

mkdir -p "${OUT_DIR}"

# ── Pre-flight checks ────────────────────────────────────────
echo "Pre-flight checks..."

# Check command scripts exist
for i in $(seq 0 $(( FANOUT - 1 ))); do
    if [ ! -f "${COMMANDS_DIR}/commands_ndt${i}.txt" ]; then
        echo "ERROR: missing ${COMMANDS_DIR}/commands_ndt${i}.txt"
        exit 1
    fi
done
echo "  Command scripts: OK"

# Check ndt_event_cli.py exists
if [ ! -f "${ANALYSIS_DIR}/ndt_event_cli.py" ]; then
    echo "ERROR: ${ANALYSIS_DIR}/ndt_event_cli.py not found"
    exit 1
fi
echo "  ndt_event_cli.py: OK"
echo ""

# ── Experiment loops ─────────────────────────────────────────
TOTAL=$(( RUN_COUNT * ${#RATE_LIST[@]} * ${#QUEUE_LIST[@]} * ${#PRIORITY_MODES[@]} ))
CURRENT=0

for run in $(seq 1 "${RUN_COUNT}"); do
    echo "══════════════════════════════════════════════"
    echo " RUN ${run}/${RUN_COUNT}"
    echo "══════════════════════════════════════════════"

    for prio_mode in "${PRIORITY_MODES[@]}"; do
        for queue in "${QUEUE_LIST[@]}"; do
            for rate in "${RATE_LIST[@]}"; do
                CURRENT=$(( CURRENT + 1 ))
                CONDITION="rate${rate}_queue${queue}_${prio_mode}"
                LOG_DIR="${OUT_DIR}/run${run}/${CONDITION}"
            mkdir -p "${LOG_DIR}"

            echo ""
            echo "  ── condition ${CURRENT}/${TOTAL}: rate=${rate} queue=${queue} mode=${prio_mode} ──"

            # 1. Launch testbed with XRM
            echo "    Launching testbed (fanout=${FANOUT} --xrm)..."
            # Build launch args with queue and priority mode
            LAUNCH_EXTRA=""
            [ "${queue}" -gt 0 ] && LAUNCH_EXTRA="${LAUNCH_EXTRA} --max-queue ${queue}"
            [ "${prio_mode}" = "fifo" ] && LAUNCH_EXTRA="${LAUNCH_EXTRA} --disable-priorities"

            "${SCRIPT_DIR}/launch_testbed.sh" \
                --mode federated \
                --fanout "${FANOUT}" \
                --xrm \
                --upstream \
                ${LAUNCH_EXTRA} \
                2>&1 | grep -E "ready|ERROR|TIMEOUT|NDTs|DTM|XRM|ONM|queue|fifo" || true

            # 2. Wait for all pods Running
            wait_pods_ready

            # 3. Get ON IP
            ON_IP=$(kubectl get pod pod-on -n "${NAMESPACE}" \
                -o jsonpath='{.status.podIP}' 2>/dev/null || echo "127.0.0.1")
            echo "    ON IP: ${ON_IP}"

            # 4. Service already deployed at launch via --upstream flag
            #    NDTs start with 4 containers (ndt + dtm-agent + xrm-agent + actuator-control)

            # 5+6. ONM upstream and DTM upstream start automatically with testbed
            #       --max-queue and --disable-priorities passed via launch_testbed.sh
            #       No manual start needed — avoids port conflict with testbed process

            # 7. Run all NDT clients in parallel
            run_clients "${rate}" "${LOG_DIR}"

            # 8. Wait for broadcasts to flush
            sleep 5

            # 9. Collect logs
            collect_logs "${LOG_DIR}"

            # 10. Print quick summary
            print_quick_summary "${LOG_DIR}" "${rate}" "${queue}" "${prio_mode}"

            # 11. Upstream processes stop with testbed (stop_testbed.sh)

            # 12. Stop testbed
            echo "    Stopping testbed..."
            "${SCRIPT_DIR}/stop_testbed.sh" > /dev/null 2>&1 || true
            sudo fuser -k 7001/udp 2>/dev/null || true
            sudo fuser -k 7005/udp 2>/dev/null || true
            sudo fuser -k 7006/udp 2>/dev/null || true
            sudo fuser -k 7007/udp 2>/dev/null || true
            sleep 10

            echo "  ── condition done ✓"
        done
    done
    done

    echo ""
    echo " Run ${run}/${RUN_COUNT} complete."
    echo ""
done

echo " All runs complete."
echo " Results in: ${OUT_DIR}"
echo ""
echo " Analyse with:"
echo "   python3 ${ANALYSIS_DIR}/experiment_2_results.py \\"
echo "       --logs ${OUT_DIR} \\"
echo "       --out  ${OUT_DIR}/results"
echo "=============================================="