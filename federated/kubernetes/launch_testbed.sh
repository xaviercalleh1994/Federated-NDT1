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
#   pod-dtm     : DTM core  (hostNetwork, scripts hostPath mounted)
#   ndt-i       : NDT (Mininet) + DTM agent
#                 + XRM agent (if --xrm)
#   pod-xrm     : XRM core (if --xrm)
#
# Changes vs v1.6:
#   - NODE_IP obtained generically from first cluster node.
#   - pod-dtm mounts scripts hostPath so DTM can call
#     restart_ndt.sh via subprocess.
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

# ── Parse arguments ──────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)       MODE="$2";      shift 2 ;;
        --fanout)     FANOUT="$2";    shift 2 ;;
        --base-port)  BASE_PORT="$2"; shift 2 ;;
        --proto)      PROTO="$2";     shift 2 ;;
        --ack-port)   ACK_PORT="$2";  shift 2 ;;
        --dtm-log)    DTM_LOG="$2";   shift 2 ;;
        --xrm)        XRM=1;          shift   ;;
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
NAMESPACE="ndt"

# Generic node IP — works on any cluster regardless of node name
NODE_IP=$(kubectl get nodes \
    -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' \
    2>/dev/null || hostname -I | awk '{print $1}')

echo "=============================================="
echo " Federated NDT Testbed — Kubernetes"
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

kubectl apply -f "${SCRIPT_DIR}/onm-pod.yaml"
echo "  ONM ready."

# ── DTM + NDT(s) + XRM ───────────────────────────────────────
echo "[4/4] Launching DTM$([ "${XRM}" = "1" ] && echo " + XRM" || echo "")..."

if [ "${MODE}" = "1:1" ]; then

    [ "${XRM}" = "1" ] && mkdir -p /tmp/xrm-state/ndt-0

    kubectl delete pod pod-dtm-ndt -n "${NAMESPACE}" \
        --ignore-not-found --grace-period=0 --force > /dev/null 2>&1 || true
    kubectl delete service svc-dtm -n "${NAMESPACE}" \
        --ignore-not-found > /dev/null 2>&1 || true

    kubectl apply -f "${SCRIPT_DIR}/dtm-ndt-pod.yaml"

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

    # pod-dtm mounts scripts hostPath so DTM can call restart_ndt.sh
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
    - name: scripts
      hostPath:
        path: ${REPO_ROOT}/ndt_deployment/mininet_ndts
        type: Directory
    - name: dtm-state
      hostPath:
        path: /tmp/dtm-state
        type: DirectoryOrCreate
    - name: kubectl-bin
      hostPath:
        path: /usr/bin/kubectl
        type: File
    - name: kubeconfig
      hostPath:
        path: /root/.kube
        type: Directory
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
        - "--restart-script"
        - "/scripts/restart_ndt.sh"
        - "--experiment-duration"
        - "0"
        - "--log-file"
        - "${DTM_LOG}"
      volumeMounts:
        - name: run-data
          mountPath: /run
        - name: scripts
          mountPath: /scripts
        - name: dtm-state
          mountPath: /tmp/dtm-state
        - name: kubectl-bin
          mountPath: /usr/local/bin/kubectl
        - name: kubeconfig
          mountPath: /root/.kube
EOF

    echo "  DTM core launched."

    # pod-xrm — only if --xrm
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
EOF
        echo "  XRM core launched."
    fi

    echo ""
    echo "  Launching ${FANOUT} NDT pods..."
    XRM="${XRM}" \
    XRM_CORE_IP="${NODE_IP}" \
    "${REPO_ROOT}/ndt_deployment/mininet_ndts/launch_ndts.sh" \
        "${FANOUT}" "${NODE_IP}" "${BASE_PORT}" "${PROTO}" "${ACK_PORT}"
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
echo "ONM automated injection:"
echo "  kubectl exec -i pod-onm -n ${NAMESPACE} -- bash -c \\"
echo "      'python3 /app/on_events_gen.py --file /app/onm_commands.txt --rate 0.5 --duration 60 | \\"
echo "       python3 /app/onm.py --proto ${PROTO} --relay-ip 127.0.0.1 \\"
echo "       --relay-port 7001 --relay-kind router --on-ip ${ON_POD_IP} --log-file /tmp/onm.log'"
echo ""
echo "Access ON Mininet:"
echo "  kubectl exec -it pod-on -n ${NAMESPACE} -- bash"
echo ""
if [ "${MODE}" = "1:1" ]; then
    echo "Access NDT Mininet:"
    echo "  kubectl exec -it pod-dtm-ndt -n ${NAMESPACE} -c ndt -- bash"
    echo ""
fi
if [ "${XRM}" = "1" ]; then
    echo "XRM CLI:"
    echo "  kubectl exec -it pod-xrm -n ${NAMESPACE} -c xrm -- \\"
    echo "      python3 xrm.py --mode core"
    echo ""
    echo "  Restart NDT via XRM (sends RESTART_NDT event to DTM):"
    echo "  Example from xrm CLI: restart ndt-1"
    echo ""
fi
echo "Stop testbed:"
echo "  ./stop_testbed.sh"