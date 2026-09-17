#!/bin/bash
# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# ============================================================
# run_experiment2_bounded.sh — bounded-but-admitted validation
# ============================================================
# Companion to run_experiment2.sh. Runs ONLY the bounded-tier
# ("third state") condition: rate=150 cmd/s, queue=4, priority
# mode, with a per-tenant backlog bound applied to ndt-2 (the
# tenant that starves in the unbounded baseline).
#
# Reuses run_experiment2.sh's exact helper functions
# (wait_pods_ready, get_pod_ip, run_clients, collect_logs,
# print_quick_summary, stop_dtm_upstream) unmodified. The only
# new logic: after the testbed's own auto-started DTM comes up
# via launch_testbed.sh --upstream, this script stops it and
# restarts the DTM manually with --tenant-bounds added, using
# the start_dtm_upstream() pattern already present (but unused
# in the main loop) in run_experiment2.sh.
#
# FIX vs run_experiment2.sh: start_dtm_upstream() there writes
# to /tmp/dtm_upstream_exp2.log, but collect_logs() reads from
# /tmp/dtm_upstream.log — those never matched. Fixed here so the
# DTM log for this run is actually collected.
#
# Usage:
#   ./run_experiment2_bounded.sh
#   ./run_experiment2_bounded.sh --runs 5 --bound 8 --target-ndt 2
#   ./run_experiment2_bounded.sh --out /tmp/exp2_bounded_std8
#
# Then analyse exactly as with the baseline (condition folder
# name still matches "rate<R>_queue<Q>..." so
# experiment_2_results.py's regex parses it unchanged):
#   python3 ../analysis/experiment_2_results.py \
#       --logs /tmp/exp2_bounded_std8 \
#       --out  /tmp/exp2_bounded_std8/results
# ============================================================

set -euo pipefail

# ── Defaults — matches the rate=150/queue=4/priority baseline ──
FANOUT=4
SERVICE="actuator-control"
RUN_COUNT=5
DURATION=60
OUT_DIR="/tmp/exp2_bounded"
NAMESPACE="ndt"
PROTO="udp"

RATE=150
QUEUE=4
TARGET_NDT=2          # ndt-2 (priority 50) — the tenant that starves today
BOUND=8                # max pending requests for TARGET_NDT before backpressure

NDT_PRIORITIES=(10 30 50 70)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
COMMANDS_DIR="${SCRIPT_DIR}"
ANALYSIS_DIR="${REPO_ROOT}/analysis"

# ── Parse arguments ──────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --runs)       RUN_COUNT="$2";  shift 2 ;;
        --duration)   DURATION="$2";   shift 2 ;;
        --out)        OUT_DIR="$2";    shift 2 ;;
        --rate)       RATE="$2";       shift 2 ;;
        --queue)      QUEUE="$2";      shift 2 ;;
        --target-ndt) TARGET_NDT="$2"; shift 2 ;;
        --bound)      BOUND="$2";      shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

TENANT_BOUNDS_JSON="{\"${TARGET_NDT}\": ${BOUND}}"

# ── Helpers — copied verbatim from run_experiment2.sh ───────────

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
    local pod_name=$1
    kubectl get pod "${pod_name}" -n "${NAMESPACE}" \
        -o jsonpath='{.status.podIP}' 2>/dev/null || echo ""
}

stop_dtm_upstream() {
    kubectl exec pod-dtm -n "${NAMESPACE}" -- bash -c \
        "pkill -f '/app/dtm.py.*upstream' 2>/dev/null; pkill -f 'dtm.py.*upstream' 2>/dev/null; true" 2>/dev/null || true
}

# ── NEW — extends run_experiment2.sh's start_dtm_upstream() with
#         --tenant-bounds, and fixes the log_file path so
#         collect_logs() actually picks it up (see header note).
start_dtm_upstream_bounded() {
    local queue_size=$1
    local tenant_bounds_json=$2
    local log_file="/tmp/dtm_upstream.log"   # FIX: was /tmp/dtm_upstream_exp2.log
    local max_queue_arg=""
    if [ "${queue_size}" -gt 0 ]; then
        max_queue_arg="--max-queue ${queue_size}"
    fi
    echo "    Starting DTM upstream (queue=${queue_size} mode=priority tenant-bounds=${tenant_bounds_json})..."
    kubectl exec pod-dtm -n "${NAMESPACE}" -- bash -c \
        "nohup python3 /app/dtm.py \
            --proto udp \
            --relay-ip 0.0.0.0 \
            --mode upstream \
            --onm-ip 127.0.0.1 \
            --log-file ${log_file} \
            ${max_queue_arg} \
            --tenant-bounds '${tenant_bounds_json}' \
            </dev/null >/tmp/dtm_upstream_stderr.log 2>&1 & echo \$! > /tmp/dtm_upstream.pid"
    for i in $(seq 1 20); do
        if kubectl exec pod-dtm -n "${NAMESPACE}" -- \
            grep -q "DTM-UPSTREAM.*Ready" /tmp/dtm_upstream_stderr.log 2>/dev/null; then
            echo "    DTM upstream ready (${i}s)"
            break
        fi
        sleep 1
    done
    # Sanity check: confirm the flag was actually parsed, not silently ignored
    if kubectl exec pod-dtm -n "${NAMESPACE}" -- \
        grep -q "bad --tenant-bounds" /tmp/dtm_upstream_stderr.log 2>/dev/null; then
        echo "    ERROR: DTM rejected --tenant-bounds — check dtm.py has the patch. Aborting."
        kubectl exec pod-dtm -n "${NAMESPACE}" -- grep "tenant-bounds" /tmp/dtm_upstream_stderr.log || true
        exit 1
    fi
}

run_clients() {
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

    echo "    Waiting for all clients to complete (${DURATION}s)..."
    for pid in "${pids[@]}"; do
        wait "${pid}" 2>/dev/null || true
    done
    echo "    All clients finished"
}

collect_logs() {
    local log_dir=$1
    echo "    Collecting logs..."

    kubectl exec pod-dtm -n "${NAMESPACE}" -- \
        cat /tmp/dtm_upstream.log 2>/dev/null \
        > "${log_dir}/dtm_upstream.log" || true

    kubectl exec pod-onm -n "${NAMESPACE}" -- \
        cat /tmp/onm_exp2.log 2>/dev/null \
        > "${log_dir}/onm.log" || true

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

    # NEW — quick visibility into how many requests actually hit the
    # new bound, without waiting for the full analysis pipeline.
    local n
    n=$(grep -c "tenant_bound_exceeded" "${log_dir}/dtm_upstream.log" 2>/dev/null || echo 0)
    echo "    tenant_bound_exceeded occurrences for ndt-${TARGET_NDT}: ${n}"
}

print_quick_summary() {
    local log_dir=$1
    echo ""
    echo "    ── Quick summary (rate=${RATE} queue=${QUEUE} mode=priority bounded=${TENANT_BOUNDS_JSON}) ──"
    python3 - "${log_dir}" << 'PYEOF'
import sys, json, os

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
echo " Experiment 2 — Bounded-But-Admitted Validation"
echo "   Fanout       : ${FANOUT} NDTs (fixed)"
echo "   Rate         : ${RATE} cmd/s per NDT"
echo "   Queue (K)    : ${QUEUE}"
echo "   Mode         : priority"
echo "   Bound (B)    : ndt-${TARGET_NDT} -> ${BOUND} pending"
echo "   Duration     : ${DURATION}s per run"
echo "   Runs         : ${RUN_COUNT}"
echo "   Output       : ${OUT_DIR}"
echo "=============================================="
echo ""

mkdir -p "${OUT_DIR}"

echo "Pre-flight checks..."
for i in $(seq 0 $(( FANOUT - 1 ))); do
    if [ ! -f "${COMMANDS_DIR}/commands_ndt${i}.txt" ]; then
        echo "ERROR: missing ${COMMANDS_DIR}/commands_ndt${i}.txt"
        exit 1
    fi
done
echo "  Command scripts: OK"
if [ ! -f "${ANALYSIS_DIR}/ndt_event_cli.py" ]; then
    echo "ERROR: ${ANALYSIS_DIR}/ndt_event_cli.py not found"
    exit 1
fi
echo "  ndt_event_cli.py: OK"
echo ""

for run in $(seq 1 "${RUN_COUNT}"); do
    echo "══════════════════════════════════════════════"
    echo " RUN ${run}/${RUN_COUNT}"
    echo "══════════════════════════════════════════════"

    CONDITION="rate${RATE}.0_queue${QUEUE}_priority_bounded"
    LOG_DIR="${OUT_DIR}/run${run}/${CONDITION}"
    mkdir -p "${LOG_DIR}"

    echo ""
    echo "  ── condition: rate=${RATE} queue=${QUEUE} mode=priority bound=${TENANT_BOUNDS_JSON} ──"

    # 1. Launch testbed exactly as the baseline (priority mode, same K)
    echo "    Launching testbed (fanout=${FANOUT} --xrm)..."
    "${SCRIPT_DIR}/launch_testbed.sh" \
        --mode federated \
        --fanout "${FANOUT}" \
        --xrm \
        --upstream \
        --max-queue "${QUEUE}" \
        2>&1 | grep -E "ready|ERROR|TIMEOUT|NDTs|DTM|XRM|ONM|queue|fifo" || true

    # 2. Wait for all pods Running
    wait_pods_ready

    # 3. Swap the auto-started DTM for one with --tenant-bounds.
    #    This is the only behavioral difference from the baseline run.
    echo "    Restarting DTM with bounded-but-admitted third state..."
    stop_dtm_upstream
    sleep 2
    start_dtm_upstream_bounded "${QUEUE}" "${TENANT_BOUNDS_JSON}"

    # 4. Run all NDT clients in parallel
    run_clients "${RATE}" "${LOG_DIR}"

    # 5. Wait for broadcasts to flush
    sleep 5

    # 6. Collect logs
    collect_logs "${LOG_DIR}"

    # 7. Print quick summary
    print_quick_summary "${LOG_DIR}"

    # 8. Stop testbed
    echo "    Stopping testbed..."
    "${SCRIPT_DIR}/stop_testbed.sh" > /dev/null 2>&1 || true
    sudo fuser -k 7001/udp 2>/dev/null || true
    sudo fuser -k 7005/udp 2>/dev/null || true
    sudo fuser -k 7006/udp 2>/dev/null || true
    sudo fuser -k 7007/udp 2>/dev/null || true
    sleep 10

    echo "  ── run done ✓"
    echo ""
done

echo " All runs complete."
echo " Results in: ${OUT_DIR}"
echo ""
echo " Analyse with:"
echo "   python3 ${ANALYSIS_DIR}/experiment_2_results.py \\"
echo "       --logs ${OUT_DIR} \\"
echo "       --out  ${OUT_DIR}/results"
echo ""
echo " Then compare ndt-${TARGET_NDT}'s e2e_ms P50/P95/P99 and drop_rate in"
echo " ${OUT_DIR}/results/summary.csv against the equivalent row from your"
echo " existing unbounded baseline (rate=150.0, queue=4, ndt_id=${TARGET_NDT})."
echo "=============================================="