#!/bin/bash

# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# ============================================================
# launch_testbed.sh — Federated NDT Testbed (Kubernetes)
#
# Supports 1:1 and federated modes.
#
# Mode 1:1:
#   pod-on-onm  : ON (Mininet) + ONM
#   pod-dtm-ndt : NDT (Mininet) + DTM agent
#
# Mode federated:
#   pod-on-onm  : ON (Mininet) + ONM
#   pod-dtm     : DTM core
#   exp1-ndt-i  : NDT (Mininet) + DTM agent  (x N)
#
# Usage:
#   ./launch_testbed.sh --mode 1:1
#   ./launch_testbed.sh --mode federated --fanout N
#   ./launch_testbed.sh --mode federated --fanout N \
#                       --base-port 30001 --proto udp --ack-port 9000
# ============================================================

set -euo pipefail

# ── Defaults ─────────────────────────────────────────────────
MODE=""
FANOUT=2
BASE_PORT=30001
PROTO="udp"
ACK_PORT=9000

# ── Parse arguments ──────────────────────────────────────────
while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)       MODE="$2";      shift 2 ;;
        --fanout)     FANOUT="$2";    shift 2 ;;
        --base-port)  BASE_PORT="$2"; shift 2 ;;
        --proto)      PROTO="$2";     shift 2 ;;
        --ack-port)   ACK_PORT="$2";  shift 2 ;;
        *) echo "Unknown argument: $1"; exit 1 ;;
    esac
done

if [ -z "${MODE}" ]; then
    echo "Usage: ./launch_testbed.sh --mode 1:1|federated [--fanout N]"
    echo "                           [--base-port PORT] [--proto udp|tcp]"
    echo "                           [--ack-port PORT]"
    exit 1
fi

if [ "${MODE}" != "1:1" ] && [ "${MODE}" != "federated" ]; then
    echo "ERROR: --mode must be '1:1' or 'federated'"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
NAMESPACE="ndt"
NODE_IP=$(kubectl get node xcalle \
    -o jsonpath='{.status.addresses[?(@.type=="InternalIP")].address}' \
    2>/dev/null || hostname -I | awk '{print $1}')

echo "=============================================="
echo " Federated NDT Testbed — Kubernetes"
echo "   Mode     : ${MODE}"
echo "   Node IP  : ${NODE_IP}"
[ "${MODE}" = "federated" ] && \
    echo "   NDTs     : ${FANOUT} (ports ${BASE_PORT}–$(( BASE_PORT + FANOUT - 1 )))"
echo "   Protocol : ${PROTO}"
echo "=============================================="
echo ""

# ── Check images ─────────────────────────────────────────────
echo "Checking images..."
for img in dtm:1.0 onm:1.0 on-mininet:1.0 ndt-mininet:1.0; do
    name="${img%:*}"
    tag="${img#*:}"
    if ! sudo crictl images 2>/dev/null | grep -E "${name}[[:space:]].*${tag}|${name}:${tag}" | grep -q .; then
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

kubectl create configmap topology-config \
    --from-file=topology.json="${REPO_ROOT}/topology.json" \
    --namespace="${NAMESPACE}" \
    --dry-run=client -o yaml | kubectl apply -f -

echo "  ConfigMaps synced."

# ── Pod ON-ONM ───────────────────────────────────────────────
echo "[3/4] Launching ON + ONM..."

# Delete old pods
kubectl delete pod pod-on pod-onm pod-on-onm -n "${NAMESPACE}" \
    --ignore-not-found --grace-period=0 --force > /dev/null 2>&1 || true
kubectl delete service svc-on -n "${NAMESPACE}" \
    --ignore-not-found > /dev/null 2>&1 || true

# Launch ON Pod
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

# Get ON Pod IP
ON_POD_IP=$(kubectl get pod pod-on -n "${NAMESPACE}" \
    -o jsonpath='{.status.podIP}' 2>/dev/null || echo "127.0.0.1")
echo "  ON Pod IP: ${ON_POD_IP}"

# Launch ONM Pod (hostNetwork: true — can reach DTM on 127.0.0.1)
kubectl apply -f "${SCRIPT_DIR}/onm-pod.yaml"
echo "  ONM ready."

# ── DTM + NDT(s) ─────────────────────────────────────────────
echo "[4/4] Launching DTM..."

if [ "${MODE}" = "1:1" ]; then

    # ── Mode 1:1 — DTM agent + NDT in same Pod ───────────────
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
            echo "  Logs: kubectl logs pod-dtm-ndt -n ${NAMESPACE} -c ndt"
            exit 1
        fi
        [ "${elapsed}" -ge 120 ] && echo "  TIMEOUT" && exit 1
    done
    echo "  DTM + NDT ready (${elapsed}s)"

else

    # ── Mode federated — DTM core + N NDT Pods ───────────────
    kubectl delete pod pod-dtm -n "${NAMESPACE}" \
        --ignore-not-found --grace-period=0 --force > /dev/null 2>&1 || true
    kubectl delete service svc-dtm -n "${NAMESPACE}" \
        --ignore-not-found > /dev/null 2>&1 || true

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
    - name: logs
      emptyDir: {}
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
        - "/dev/stderr"
      volumeMounts:
        - name: logs
          mountPath: /app/logs
EOF

    echo "  DTM core launched."
    echo ""
    echo "  Launching ${FANOUT} NDT pods..."
    "${REPO_ROOT}/ndt_deployment/mininet_ndts/launch_ndts.sh" \
        "${FANOUT}" "${NODE_IP}" "${BASE_PORT}" "${PROTO}" "${ACK_PORT}"
fi

# ── Summary ──────────────────────────────────────────────────
echo ""
echo "=============================================="
echo " Testbed ready — mode=${MODE}"
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
if [ "${MODE}" = "1:1" ]; then
    echo "Access NDT Mininet:"
    echo "  kubectl exec -it pod-dtm-ndt -n ${NAMESPACE} -c ndt -- bash"
    echo ""
fi
echo "Stop testbed:"
echo "  ./stop_testbed.sh [./logs/run1]"