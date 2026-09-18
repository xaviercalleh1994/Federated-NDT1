#!/bin/bash

# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# ============================================================
# launch_testbed.sh — Federated NDT Testbed (Kubernetes)
#
# Supports 1:1 and federated modes.
#
# Mode 1:1:
#   pod-on-onm  : ON (Mininet) + ONM
#   pod-dtm-ndt : NDT (Mininet) + DTM
#                 + XRM (if --xrm)
#
# Mode federated:
#   pod-on      : ON (Mininet)
#   pod-onm     : ONM
#   pod-dtm     : DTM core  (hostNetwork, dtm-state hostPath mounted)
#   ndt-i       : NDT (Mininet) + DTM agent
#                 + XRM agent (if --xrm)
#   pod-xrm     : XRM core (if --xrm)
#   watch_restarts.sh : daemon on host watching for DTM restart signals
#
# Changes vs v1.6:
#   - NODE_IP obtained generically from first cluster node.
#   - pod-dtm mounts dtm-state hostPath so DTM can persist NDT state.
#   - watch_restarts.sh started as background daemon — detects restart
#     signal files written by DTM and calls restart_ndt.sh.
#   - --xrm flag enables XRM containers (v2.0).
#   - All other logic identical to v1.6.
#
# Usage:
#   v1.6 — no XRM:
#     ./launch_testbed.sh --mode 1:1
#     ./launch_testbed.sh --mode federated --fanout N
#
#   v2.0 — with XRM:
#     ./launch_testbed.sh --mode federated --fanout N --xrm
#
#   Full options:
#     ./launch_testbed.sh --mode federated --fanout N \
#                         --base-port 30001 --proto udp \
#                         --ack-port 9000 --dtm-log /run/dtm.log \
#                         [--xrm]
# ============================================================

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────
MODE=""
FANOUT=2
BASE_PORT=30001
PROTO="udp"
ACK_PORT=9000
DTM_LOG="/dev/stderr"
XRM=0
UPSTREAM=0       # --upstream: start ONM upstream server + DTM upstream
UPSTREAM_MAX_QUEUE=0    # --max-queue N: DTM upstream AdmissionQueue size (0=unlimited)
UPSTREAM_FIFO=0         # --disable-priorities: DTM upstream FIFO mode

# ── Parse arguments ──────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)       MODE="$2";      shift 2 ;;
        --fanout)     FANOUT="$2";    shift 2 ;;
        --base-port)  BASE_PORT="$2"; shift 2 ;;
        --proto)      PROTO="$2";     shift 2 ;;
        --ack-port)   ACK_PORT="$2";  shift 2 ;;
        --dtm-log)    DTM_LOG="$2";   shift 2 ;;
        --xrm)                XRM=1;                  shift   ;;
        --upstream)           UPSTREAM=1;             shift   ;;
        --max-queue)          UPSTREAM_MAX_QUEUE="$2"; shift 2 ;;
        --disable-priorities) UPSTREAM_FIFO=1;         shift   ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [ -z "${MODE}" ]; then
    echo "Usage: ./launch_testbed.sh --mode 1:1|federated [--fanout N]"
    echo "                           [--base-port PORT] [--proto udp|tcp]"
    echo "                           [--ack-port PORT] [--dtm-log PATH]"
    echo "                           [--xrm]"
    exit 1
fi

if [ "${MODE}" != "1:1" ] && [ "${MODE}" != "federated" ]; then
    echo "ERROR: --mode must be '1:1' or 'federated'"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NDT_SCRIPTS="${REPO_ROOT}/ndt_deployment/mininet_ndts"
NAMESPACE="ndt"

# Generic node IP — works on any cluster regardless of node name
NODE_IP=$(kubectl get nodes \
    -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' \
    2>/dev/null || hostname -I | awk '{print $1}')

echo "=============================================="
echo " Federated NDT Testbed — Kubernetes"

# ── Ensure state directories exist with correct permissions ──
# Pods run as root and write to these hostPath volumes.
# Pre-create them as the current user to avoid permission errors.
sudo mkdir -p /tmp/dtm-state /tmp/xrm-state /tmp/ndt-run
sudo chmod -R 777 /tmp/dtm-state /tmp/xrm-state /tmp/ndt-run
echo "   Mode     : ${MODE}"
echo "   Node IP  : ${NODE_IP}"
[ "${MODE}" = "federated" ] && \
    echo "   NDTs     : ${FANOUT} (ports ${BASE_PORT}–$(( BASE_PORT + FANOUT - 1 )))"
echo "   Protocol : ${PROTO}"
echo "   DTM log  : ${DTM_LOG}"
echo "   XRM      : $([ "${XRM}" = "1" ] && echo "enabled (v2.0)" || echo "disabled (v1.6)")"
echo "=============================================="
echo ""

# ── Check images ─────────────────────────────────────────────
echo "Checking images..."
REQUIRED_IMAGES="dtm:1.0 onm:1.0 on-mininet:1.0 ndt-mininet:1.0"
[ "${XRM}" = "1" ] && REQUIRED_IMAGES="${REQUIRED_IMAGES} xrm:1.0"

for img in ${REQUIRED_IMAGES}; do
    name="${img%:*}"
    tag="${img#*:}"
    if ! sudo crictl images 2>/dev/null | \
       grep -E "${name}[[:space:]].*${tag}|${name}:${tag}" | grep -q .; then
        echo "ERROR: image '${img}' not found in containerd."
        echo "Build and import it first:"
        echo "  docker build -t ${img} <path>"
        echo "  docker save ${img} | sudo ctr -n k8s.io images import -"
        exit 1
    fi
done
echo "  All images present."
echo ""

# ── Namespace ────────────────────────────────────────────────
echo "[1/4] Namespace..."
kubectl apply -f "${SCRIPT_DIR}/namespace.yaml"

# ── ConfigMaps ───────────────────────────────────────────────
echo "[2/4] ConfigMaps..."

kubectl create configmap on-ndt-script \
    --from-file=on_ndt.py="${REPO_ROOT}/on_ndt_creation/on_ndt.py" \
    --namespace="${NAMESPACE}" \
    --dry-run=client -o yaml | kubectl apply -f -

sudo mkdir -p /tmp/ndt-run
sudo cp "${REPO_ROOT}/topology.json" /tmp/ndt-run/topology_config.json

kubectl create configmap topology-config \
    --from-file=topology.json="${REPO_ROOT}/topology.json" \
    --namespace="${NAMESPACE}" \
    --dry-run=client -o yaml | kubectl apply -f -

echo "  ConfigMaps synced."

# ── Pod ON ───────────────────────────────────────────────────
echo "[3/4] Launching ON + ONM..."

kubectl delete pod pod-on pod-onm pod-on-onm -n "${NAMESPACE}" \
    --ignore-not-found --grace-period=0 --force > /dev/null 2>&1 || true
kubectl delete service svc-on -n "${NAMESPACE}" \
    --ignore-not-found > /dev/null 2>&1 || true

kubectl apply -f "${SCRIPT_DIR}/on-pod.yaml"

echo "  Waiting for ON Mininet..."
elapsed=0
until kubectl logs pod-on -n "${NAMESPACE}" 2>/dev/null | \
      grep -q "Mininet ready"; do
    sleep 2
    elapsed=$(( elapsed + 2 ))
    PHASE=$(kubectl get pod pod-on -n "${NAMESPACE}" \
        --no-headers -o custom-columns=":status.phase" 2>/dev/null \
        || echo "Unknown")
    if [ "${PHASE}" = "Failed" ]; then
        echo "  ERROR: pod-on failed."
        echo "  Logs: kubectl logs pod-on -n ${NAMESPACE}"
        exit 1
    fi
    [ "${elapsed}" -ge 120 ] && echo "  TIMEOUT" && exit 1
done
echo "  ON ready (${elapsed}s)"

ON_POD_IP=$(kubectl get pod pod-on -n "${NAMESPACE}" \
    -o jsonpath='{.status.podIP}' 2>/dev/null || echo "127.0.0.1")
echo "  ON Pod IP: ${ON_POD_IP}"

# pod-onm — if --upstream: starts ONM directly with --upstream-server
#            otherwise: sleep infinity (operator launches ONM manually)
if [ "${UPSTREAM}" = "1" ]; then
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-onm
  namespace: ${NAMESPACE}
  labels:
    app: onm
spec:
  restartPolicy: Never
  hostNetwork: true
  volumes:
    - name: run-data
      hostPath:
        path: /tmp/ndt-run
        type: DirectoryOrCreate
    - name: logs
      emptyDir: {}
  containers:
    - name: onm
      image: onm:1.0
      imagePullPolicy: Never
      stdin: true
      command: ["python3", "/app/onm.py"]
      tty: true
      args:
        - "--proto"
        - "${PROTO}"
        - "--relay-ip"
        - "127.0.0.1"
        - "--relay-port"
        - "7001"
        - "--relay-kind"
        - "router"
        - "--on-ip"
        - "${ON_POD_IP}"
        - "--upstream-server"
        - "--dtm-upstream-ip"
        - "127.0.0.1"
        - "--log-file"
        - "/tmp/onm.log"
      volumeMounts:
        - name: run-data
          mountPath: /run
        - name: logs
          mountPath: /app/logs
EOF

# Wait for ONM upstream to be ready
echo "  Waiting for ONM upstream..."
for i in $(seq 1 30); do
    if kubectl exec pod-onm -n "${NAMESPACE}" -- \
        grep -q "ONM-UPSTREAM.*Ready" /tmp/onm.log 2>/dev/null; then
        echo "  ONM ready (${i}s)"
        break
    fi
    sleep 1
done
else
    # Original behavior — sleep infinity, operator launches ONM manually
    kubectl apply -f "${SCRIPT_DIR}/onm-pod.yaml"
    echo "  ONM ready."
fi

# ── DTM + NDT(s) + XRM ───────────────────────────────────────
echo "[4/4] Launching DTM$([ "${XRM}" = "1" ] && echo " + XRM" || echo "")..."

if [ "${MODE}" = "1:1" ]; then

    kubectl delete pod pod-dtm-ndt pod-xrm -n "${NAMESPACE}" \
        --ignore-not-found --grace-period=0 --force > /dev/null 2>&1 || true
    kubectl delete service svc-dtm -n "${NAMESPACE}" \
        --ignore-not-found > /dev/null 2>&1 || true

    if [ "${XRM}" = "1" ]; then
        mkdir -p /tmp/xrm-state/ndt-0

        # XRM core — same pattern as the federated branch below.
        kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-xrm
  namespace: ${NAMESPACE}
  labels:
    app: xrm
    mode: core
spec:
  restartPolicy: Never
  hostNetwork: true
  volumes:
    - name: xrm-state
      hostPath:
        path: /tmp/xrm-state
        type: DirectoryOrCreate
  containers:
    - name: xrm
      image: xrm:1.0
      imagePullPolicy: Never
      stdin: true
      tty: true
      args:
        - "--mode"
        - "core"
        - "--log-file"
        - "/tmp/xrm_core.log"
        - "--dtm-relay-ip"
        - "${NODE_IP}"
      volumeMounts:
        - name: xrm-state
          mountPath: /tmp/xrm-state
EOF
        echo "  XRM core launched."

        # pod-dtm-ndt, with an xrm-agent container appended.
        # Identical to dtm-ndt-pod.yaml otherwise — keep the two in sync
        # if you edit one.
        kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-dtm-ndt
  namespace: ${NAMESPACE}
  labels:
    app: dtm-ndt
    mode: "1-1"
spec:
  restartPolicy: Never
  volumes:
    - name: on-ndt-script
      configMap:
        name: on-ndt-script
        defaultMode: 493
    - name: topology-config
      configMap:
        name: topology-config
    - name: logs
      emptyDir: {}
    - name: run-data
      emptyDir: {}
    - name: xrm-state
      hostPath:
        path: /tmp/xrm-state/ndt-0
        type: DirectoryOrCreate
  containers:
    - name: ndt
      image: on-mininet:1.0
      imagePullPolicy: Never
      securityContext:
        privileged: true
      env:
        - name: CTRL_PORT
          value: "6653"
      volumeMounts:
        - name: on-ndt-script
          mountPath: /app/on_ndt.py
          subPath: on_ndt.py
        - name: topology-config
          mountPath: /run/topology_config.json
          subPath: topology.json
        - name: logs
          mountPath: /app/logs
    - name: dtm
      image: dtm:1.0
      imagePullPolicy: Never
      args:
        - "--proto"
        - "udp"
        - "--relay-ip"
        - "0.0.0.0"
        - "--relay-port"
        - "7001"
        - "--mode"
        - "agent"
        - "--dtm-ip"
        - ""
        - "--log-file"
        - "/app/logs/dtm-agent.log"
      volumeMounts:
        - name: logs
          mountPath: /app/logs
    - name: xrm-agent
      image: xrm:1.0
      imagePullPolicy: Never
      args:
        - "--mode"
        - "agent"
        - "--ndt-id"
        - "0"
        - "--core-ip"
        - "${NODE_IP}"
        - "--dtm-ip"
        - "127.0.0.1"
        - "--log-file"
        - "/tmp/xrm_agent_0.log"
      volumeMounts:
        - name: run-data
          mountPath: /run
        - name: xrm-state
          mountPath: /tmp/xrm-state/ndt-0
---
apiVersion: v1
kind: Service
metadata:
  name: svc-dtm
  namespace: ${NAMESPACE}
  labels:
    app: dtm-ndt
spec:
  type: ClusterIP
  selector:
    app: dtm-ndt
  ports:
    - name: relay-udp
      protocol: UDP
      port: 7001
      targetPort: 7001
EOF
    else
        kubectl apply -f "${SCRIPT_DIR}/dtm-ndt-pod.yaml"
    fi

    echo "  Waiting for NDT Mininet..."
    elapsed=0
    until kubectl logs pod-dtm-ndt -n "${NAMESPACE}" -c ndt 2>/dev/null | \
          grep -q "Mininet ready"; do
        sleep 2
        elapsed=$(( elapsed + 2 ))
        PHASE=$(kubectl get pod pod-dtm-ndt -n "${NAMESPACE}" \
            --no-headers -o custom-columns=":status.phase" 2>/dev/null \
            || echo "Unknown")
        if [ "${PHASE}" = "Failed" ]; then
            echo "  ERROR: pod-dtm-ndt failed."
            exit 1
        fi
        [ "${elapsed}" -ge 120 ] && echo "  TIMEOUT" && exit 1
    done
    echo "  DTM + NDT$([ "${XRM}" = "1" ] && echo " + XRM") ready (${elapsed}s)"

else

    kubectl delete pod pod-dtm pod-xrm -n "${NAMESPACE}" \
        --ignore-not-found --grace-period=0 --force > /dev/null 2>&1 || true
    kubectl delete service svc-dtm -n "${NAMESPACE}" \
        --ignore-not-found > /dev/null 2>&1 || true

    # Ensure dtm-state base directory exists on host
    mkdir -p /tmp/dtm-state

    # pod-dtm — mounts dtm-state hostPath so DTM can persist NDT state
    kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-dtm
  namespace: ${NAMESPACE}
  labels:
    app: dtm
    mode: federated
spec:
  restartPolicy: Never
  hostNetwork: true
  volumes:
    - name: run-data
      hostPath:
        path: /tmp/ndt-run
        type: DirectoryOrCreate
    - name: dtm-state
      hostPath:
        path: /tmp/dtm-state
        type: DirectoryOrCreate
  containers:
    - name: dtm
      image: dtm:1.0
      imagePullPolicy: Never
      args:
        - "--proto"
        - "${PROTO}"
        - "--relay-ip"
        - "0.0.0.0"
        - "--relay-port"
        - "7001"
        - "--onm-port"
        - "7002"
        - "--mode"
        - "federated"
        - "--fanout"
        - "${FANOUT}"
        - "--dst-ip"
        - "${NODE_IP}"
        - "--base-port"
        - "${BASE_PORT}"
        - "--feedback-port"
        - "9000"
        - "--experiment-duration"
        - "0"
        - "--log-file"
        - "${DTM_LOG}"
      volumeMounts:
        - name: run-data
          mountPath: /run
        - name: dtm-state
          mountPath: /tmp/dtm-state
EOF

    echo "  DTM core launched."

    # Start DTM upstream — only if --upstream flag is set
    # Runs as background process via kubectl exec with setsid to survive session
    # Waits for pod-dtm to be Running first
    echo "  Waiting for pod-dtm Running..."
    for i in $(seq 1 30); do
        phase=$(kubectl get pod pod-dtm -n "${NAMESPACE}" \
            --no-headers -o custom-columns=":status.phase" 2>/dev/null || echo "")
        [ "${phase}" = "Running" ] && break
        sleep 1
    done
    if [ "${UPSTREAM}" = "1" ]; then
        # Build optional args for DTM upstream
        DTM_UPSTREAM_EXTRA=""
        [ "${UPSTREAM_MAX_QUEUE}" -gt 0 ] &&             DTM_UPSTREAM_EXTRA="${DTM_UPSTREAM_EXTRA} --max-queue ${UPSTREAM_MAX_QUEUE}"
        [ "${UPSTREAM_FIFO}" = "1" ] &&             DTM_UPSTREAM_EXTRA="${DTM_UPSTREAM_EXTRA} --disable-priorities"
        echo "  DTM upstream args: queue=${UPSTREAM_MAX_QUEUE} fifo=${UPSTREAM_FIFO}"
        kubectl exec pod-dtm -n "${NAMESPACE}" -- bash -c \
            "setsid python3 /app/dtm.py \
                --proto udp \
                --relay-ip 0.0.0.0 \
                --mode upstream \
                --onm-ip 127.0.0.1 \
                --log-file /tmp/dtm_upstream.log \
                ${DTM_UPSTREAM_EXTRA} \
                </dev/null >/tmp/dtm_upstream_stderr.log 2>&1 &
             echo \$! > /tmp/dtm_upstream.pid" 2>/dev/null || true
        # Wait for DTM upstream to be ready
        echo "  Waiting for DTM upstream..."
        for i in $(seq 1 20); do
            if kubectl exec pod-dtm -n "${NAMESPACE}" -- \
                grep -q "DTM-UPSTREAM.*Ready" /tmp/dtm_upstream_stderr.log 2>/dev/null; then
                echo "  DTM upstream ready (${i}s)"
                break
            fi
            sleep 1
        done
    fi

    # pod-xrm — only if --xrm
    # stdin + tty keep the pod alive and allow kubectl exec -it to attach
    if [ "${XRM}" = "1" ]; then
        kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: pod-xrm
  namespace: ${NAMESPACE}
  labels:
    app: xrm
    mode: core
spec:
  restartPolicy: Never
  hostNetwork: true
  volumes:
    - name: xrm-state
      hostPath:
        path: /tmp/xrm-state
        type: DirectoryOrCreate
  containers:
    - name: xrm
      image: xrm:1.0
      imagePullPolicy: Never
      stdin: true
      tty: true
      args:
        - "--mode"
        - "core"
        - "--log-file"
        - "/tmp/xrm_core.log"
        - "--dtm-relay-ip"
        - "${NODE_IP}"
      volumeMounts:
        - name: xrm-state
          mountPath: /tmp/xrm-state
EOF
        echo "  XRM core launched."
    fi

    echo ""
    echo "  Launching ${FANOUT} NDT pods..."
    XRM="${XRM}" \
    XRM_CORE_IP="${NODE_IP}" \
    UPSTREAM="${UPSTREAM}" \
    DTM_IP="${NODE_IP}" \
    "${NDT_SCRIPTS}/launch_ndts.sh" \
        "${FANOUT}" "${NODE_IP}" "${BASE_PORT}" "${PROTO}" "${ACK_PORT}"

    # Start watch_restarts.sh daemon on host
    # Watches for DTM restart signals and calls restart_ndt.sh
    WATCH_PID_FILE="/tmp/watch_restarts.pid"
    if [ -f "${WATCH_PID_FILE}" ]; then
        OLD_PID=$(cat "${WATCH_PID_FILE}")
        kill "${OLD_PID}" 2>/dev/null || true
        rm -f "${WATCH_PID_FILE}"
    fi
    XRM="${XRM}" XRM_CORE_IP="${NODE_IP}" \
    bash "${NDT_SCRIPTS}/watch_restarts.sh" \
        "${NODE_IP}" "${BASE_PORT}" "${PROTO}" "${ACK_PORT}" \
        > /tmp/watch_restarts.log 2>&1 &
    echo "  watch_restarts.sh started (PID=$!)"
fi

# ── Summary ──────────────────────────────────────────────────
echo ""
echo "=============================================="
echo " Testbed ready — mode=${MODE}$([ "${XRM}" = "1" ] && echo " + XRM (v2.0)" || echo " (v1.6)")"
echo "=============================================="
kubectl get pods -n "${NAMESPACE}" -o wide
echo ""
echo "── Commands ──────────────────────────────────"
echo ""
echo "ONM interactive CLI:"
echo "  kubectl exec -it pod-onm -n ${NAMESPACE} -- python3 onm.py \\"
echo "      --proto ${PROTO} --relay-ip 127.0.0.1 \\"
echo "      --relay-port 7001 --relay-kind router \\"
echo "      --on-ip ${ON_POD_IP} \\"
echo "      --log-file /tmp/onm.log"
echo ""
echo "Access ON Mininet:"
echo "  kubectl exec -it pod-on -n ${NAMESPACE} -- bash"
echo ""
if [ "${XRM}" = "1" ]; then
    echo "XRM CLI:"
    echo "  kubectl exec -it pod-xrm -n ${NAMESPACE} -- python3 xrm.py --mode cli"
    echo ""
fi
echo "watch_restarts log: tail -f /tmp/watch_restarts.log"
echo ""
echo "Stop testbed:"
echo "  ./stop_testbed.sh"