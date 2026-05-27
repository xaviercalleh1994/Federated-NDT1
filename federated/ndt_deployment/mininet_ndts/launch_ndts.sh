#!/bin/bash

# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# ============================================================
# launch_ndts.sh — Experiment 1 (Kubernetes NodePort version)
#
# Launches N NDT Mininet pods in Kubernetes.
# Each NDT Pod gets its own network namespace (no hostNetwork).
# Each Pod is exposed via a NodePort Service so the DTM on PC1
# can reach it on NODE_IP:NODE_PORT.
#
# Port mapping:
#   DTM --base-port BASE_PORT
#   NDT i → NodePort (BASE_PORT + i - 1) → Pod internal port 8001
#
# ACK packets (NDT → DTM) go out directly from the Pod via
# Calico — no NodePort needed for that direction.
#
# Changes vs v1.6:
#   - dtm-agent receives --ndt-id for state restore on startup.
#   - dtm-state hostPath volume mounted in dtm-agent so the DTM
#     core can persist state and the agent can restore it.
#   - scripts hostPath volume mounted so DTM can call
#     restart_ndt.sh via subprocess without kubectl in container.
#   - XRM=1: adds xrm-agent container and xrm-state hostPath.
#
# Usage:
#   v1.6-compatible:
#     ./launch_ndts.sh N DTM_IP BASE_PORT [PROTO] [ACK_PORT]
#
#   v2.0 with XRM:
#     XRM=1 XRM_CORE_IP=<ip> \
#     ./launch_ndts.sh N DTM_IP BASE_PORT [PROTO] [ACK_PORT]
#
# Examples:
#   ./launch_ndts.sh 8 10.4.47.53 30001 udp 9000
#   XRM=1 XRM_CORE_IP=10.4.47.53 ./launch_ndts.sh 4 10.4.47.53 30001 udp 9000
# ============================================================

set -euo pipefail

N=${1:-2}
DTM_IP=${2:?"Usage: ./launch_ndts.sh N DTM_IP BASE_PORT [PROTO] [ACK_PORT]"}
BASE_PORT=${3:?"Usage: ./launch_ndts.sh N DTM_IP BASE_PORT [PROTO] [ACK_PORT]"}
PROTO=${4:-udp}
ACK_PORT=${5:-9000}
MAX_WAIT=120

# XRM parameters — only used when XRM=1
XRM=${XRM:-0}
XRM_CORE_IP=${XRM_CORE_IP:-${DTM_IP}}

# Internal port that every NDT container listens on (fixed)
NDT_INTERNAL_PORT=8001

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
IMAGE="ndt-mininet:1.0"
DTM_IMAGE="dtm:1.0"
XRM_IMAGE="xrm:1.0"
PREFIX="ndt"
NAMESPACE="ndt"
MANIFEST_DIR="${SCRIPT_DIR}/manifests/current"
LOG_DIR="${SCRIPT_DIR}/logs/current"

mkdir -p "${MANIFEST_DIR}" "${LOG_DIR}"

# ── Check prerequisites ──────────────────────────────────────
if ! kubectl version --client > /dev/null 2>&1; then
    echo "ERROR: kubectl not found or not configured."
    exit 1
fi

if ! sudo crictl images 2>/dev/null | \
   grep -E "ndt-mininet[[:space:]].*1\.0|ndt-mininet:1\.0" | grep -q .; then
    echo "ERROR: image 'ndt-mininet:1.0' not found in containerd."
    echo "Import it with:"
    echo "  docker save ndt-mininet:1.0 | sudo ctr -n k8s.io images import -"
    exit 1
fi

if [ "${XRM}" = "1" ]; then
    if ! sudo crictl images 2>/dev/null | \
       grep -E "xrm[[:space:]].*1\.0|xrm:1\.0" | grep -q .; then
        echo "ERROR: image 'xrm:1.0' not found in containerd."
        exit 1
    fi
fi

# ── Validate NodePort range ──────────────────────────────────
LAST_PORT=$(( BASE_PORT + N - 1 ))
if [ "${BASE_PORT}" -lt 30000 ] || [ "${LAST_PORT}" -gt 32767 ]; then
    echo "ERROR: NodePort range must be within 30000-32767."
    echo "       Requested: ${BASE_PORT} — ${LAST_PORT}"
    exit 1
fi

# ── Create namespace if needed ───────────────────────────────
if ! kubectl get namespace "${NAMESPACE}" > /dev/null 2>&1; then
    echo "Creating namespace '${NAMESPACE}'..."
    kubectl create namespace "${NAMESPACE}"
fi

# ── Clean previous pods and services ────────────────────────
echo "Cleaning up previous NDT pods and services..."
for i in $(seq 1 "${N}"); do
    kubectl delete pod     "${PREFIX}-${i}" -n "${NAMESPACE}" \
        --ignore-not-found --grace-period=0 --force > /dev/null 2>&1 || true
    kubectl delete service "svc-${PREFIX}-${i}" -n "${NAMESPACE}" \
        --ignore-not-found > /dev/null 2>&1 || true
done
for res in $(kubectl get pods -n "${NAMESPACE}" \
    --selector="app=ndt-exp1" --no-headers \
    -o custom-columns=":metadata.name" 2>/dev/null || true); do
    kubectl delete pod "${res}" -n "${NAMESPACE}" \
        --ignore-not-found --grace-period=0 --force > /dev/null 2>&1 || true
done
sleep 1

PROTO_UPPER=$(echo "${PROTO}" | tr '[:lower:]' '[:upper:]')

echo "=============================================="
echo " Launching ${N} NDT pods in Kubernetes"
echo "   Namespace : ${NAMESPACE}"
echo "   NodePorts : ${BASE_PORT} — ${LAST_PORT}"
echo "   Protocol  : ${PROTO}"
echo "   DTM IP    : ${DTM_IP}"
echo "   XRM       : $([ "${XRM}" = "1" ] && echo "enabled — core ${XRM_CORE_IP}" || echo "disabled")"
echo "=============================================="
echo ""

ALL_OK=true

for i in $(seq 1 "${N}"); do
    NODE_PORT=$(( BASE_PORT + i - 1 ))
    CTRL_PORT=$(( 6653 + i - 1 ))
    NDT_ID=$(( i - 1 ))
    MANIFEST="${MANIFEST_DIR}/${PREFIX}-${i}.yaml"

    # Ensure state directories exist on host
    mkdir -p "/tmp/dtm-state/ndt-${NDT_ID}"
    [ "${XRM}" = "1" ] && mkdir -p "/tmp/xrm-state/ndt-${NDT_ID}"

    echo "  Starting NDT ${i}  nodeport=${NODE_PORT}  ctrl=${CTRL_PORT}..."

    # XRM optional blocks
    XRM_VOLUME=""
    XRM_CONTAINER=""
    if [ "${XRM}" = "1" ]; then
        XRM_VOLUME=$(cat <<XRMVOL

    - name: xrm-state
      hostPath:
        path: /tmp/xrm-state/ndt-${NDT_ID}
        type: DirectoryOrCreate
XRMVOL
)
        XRM_CONTAINER=$(cat <<XRMBLOCK

    # XRM agent — registers with XRM core, manages XR service lifecycle
    - name: xrm-agent
      image: ${XRM_IMAGE}
      imagePullPolicy: Never
      args:
        - "--mode"
        - "agent"
        - "--ndt-id"
        - "${NDT_ID}"
        - "--core-ip"
        - "${XRM_CORE_IP}"
        - "--log-file"
        - "/tmp/xrm_agent_${NDT_ID}.log"
      volumeMounts:
        - name: run-data
          mountPath: /run
        - name: xrm-state
          mountPath: /tmp/xrm-state/ndt-${NDT_ID}
XRMBLOCK
)
    fi

    # ── Generate Pod + Service manifest ─────────────────────
    #
    # Pod:
    #   - No hostNetwork → each Pod gets its own network namespace.
    #     Mininet/OVS interfaces are isolated per Pod, no collisions.
    #   - privileged: true → required for OVS + Mininet.
    #   - NDT_PORT is always NDT_INTERNAL_PORT (8001) inside the Pod.
    #   - DTM_IP is passed so the NDT knows where to send ACK packets.
    #   - dtm-state hostPath: DTM persists state here; agent restores on startup.
    #   - scripts hostPath: restart_ndt.sh mounted for subprocess calls.
    #
    # Service (NodePort):
    #   - Exposes the Pod on NODE_PORT of the host.
    #   - DTM uses --base-port BASE_PORT, so NDT i is reachable at
    #     NODE_IP:(BASE_PORT + i - 1).
    cat > "${MANIFEST}" <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${PREFIX}-${i}
  namespace: ${NAMESPACE}
  labels:
    app: ndt-exp1
    ndt-index: "${i}"
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
    - name: run-data
      emptyDir: {}    # shared /run between ndt and dtm-agent
    - name: dtm-state
      hostPath:
        path: /tmp/dtm-state/ndt-${NDT_ID}
        type: DirectoryOrCreate
    - name: scripts
      hostPath:
        path: ${SCRIPT_DIR}
        type: Directory${XRM_VOLUME}
  containers:
    - name: ndt
      image: ${IMAGE}
      imagePullPolicy: Never
      securityContext:
        privileged: true
      env:
        - name: NDT_PORT
          value: "${NDT_INTERNAL_PORT}"
        - name: NDT_PROTO
          value: "${PROTO}"
        - name: CTRL_PORT
          value: "${CTRL_PORT}"
      ports:
        - containerPort: ${NDT_INTERNAL_PORT}
          protocol: UDP
        - containerPort: ${NDT_INTERNAL_PORT}
          protocol: TCP
      volumeMounts:
        - name: on-ndt-script
          mountPath: /app/on_ndt.py
          subPath: on_ndt.py
        - name: topology-config
          mountPath: /app/topology_config.json
          subPath: topology.json
        - name: run-data
          mountPath: /run
      resources:
        requests:
          memory: "512Mi"
          cpu: "250m"
        limits:
          memory: "512Mi"
          cpu: "500m"

    # DTM distributed agent — receives events from DTM core,
    # applies them on local NDT via localhost:6001 (on_ndt.py),
    # restores saved network state on startup from dtm-state volume
    - name: dtm-agent
      image: ${DTM_IMAGE}
      imagePullPolicy: Never
      args:
        - "--proto"
        - "${PROTO}"
        - "--relay-ip"
        - "0.0.0.0"
        - "--relay-port"
        - "${NDT_INTERNAL_PORT}"
        - "--mode"
        - "agent"
        - "--ndt-id"
        - "${NDT_ID}"
        - "--dtm-ip"
        - "${DTM_IP}"
        - "--ack-port"
        - "${ACK_PORT}"
        - "--log-file"
        - "/tmp/agent.log"
      volumeMounts:
        - name: run-data
          mountPath: /run
        - name: dtm-state
          mountPath: /tmp/dtm-state/ndt-${NDT_ID}
        - name: scripts
          mountPath: /scripts${XRM_CONTAINER}
---
apiVersion: v1
kind: Service
metadata:
  name: svc-${PREFIX}-${i}
  namespace: ${NAMESPACE}
  labels:
    app: ndt-exp1
    ndt-index: "${i}"
spec:
  type: NodePort
  selector:
    app: ndt-exp1
    ndt-index: "${i}"
  ports:
    - name: ndt-${PROTO}
      protocol: ${PROTO_UPPER}
      port: ${NDT_INTERNAL_PORT}
      targetPort: ${NDT_INTERNAL_PORT}
      nodePort: ${NODE_PORT}
    - name: xrm-agent-cmd
      protocol: UDP
      port: 8110
      targetPort: 8110
      nodePort: $(( 31100 + NDT_ID ))
EOF

    kubectl apply -f "${MANIFEST}" > /dev/null

    # ── Wait for Pod to be ready ─────────────────────────────
    elapsed=0
    ready=false

    until [ "${ready}" = true ]; do
        sleep 1
        elapsed=$(( elapsed + 1 ))

        PHASE=$(kubectl get pod "${PREFIX}-${i}" -n "${NAMESPACE}" \
            --no-headers -o custom-columns=":status.phase" 2>/dev/null \
            || echo "Unknown")

        if [ "${PHASE}" = "Failed" ] || \
           kubectl get pod "${PREFIX}-${i}" -n "${NAMESPACE}" 2>/dev/null | \
           grep -qE "CrashLoopBackOff|OOMKilled"; then
            echo "  NDT ${i} FAILED (phase=${PHASE})"
            echo "  Logs: kubectl logs ${PREFIX}-${i} -n ${NAMESPACE}"
            ALL_OK=false
            break
        fi

        # Check ndt container (Mininet keeper) is ready
        # and dtm-agent is ready
        NDT_READY=$(kubectl logs "${PREFIX}-${i}" \
            -n "${NAMESPACE}" -c ndt 2>/dev/null | \
            grep -c "Mininet ready" || true)
        AGENT_READY=$(kubectl logs "${PREFIX}-${i}" \
            -n "${NAMESPACE}" -c dtm-agent 2>/dev/null | \
            grep -c "AGENT.*Ready" || true)

        if [ "${NDT_READY}" -gt 0 ] && [ "${AGENT_READY}" -gt 0 ]; then
            ready=true
        fi

        if [ "${elapsed}" -ge "${MAX_WAIT}" ]; then
            echo "  NDT ${i} TIMEOUT after ${MAX_WAIT}s"
            echo "  Logs: kubectl logs ${PREFIX}-${i} -n ${NAMESPACE}"
            ALL_OK=false
            break
        fi
    done

    if [ "${ready}" = true ]; then
        echo "  NDT ${i} (nodeport=${NODE_PORT}): ready (${elapsed}s)"
    fi
    echo ""
done

# ── Summary ──────────────────────────────────────────────────
if [ "${ALL_OK}" = false ]; then
    echo "ERROR: one or more NDTs failed. Run: ./stop_ndts.sh"
    exit 1
fi

echo "All ${N} NDTs ready."
echo ""
kubectl get pods,services -n "${NAMESPACE}" -o wide
echo ""

NODE_IP=$(kubectl get nodes \
    -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}' \
    2>/dev/null || hostname -I | awk '{print $1}')

echo ""
echo "Step 1 — VM DTM:"
echo "──────────────────────────────────────────────────"
echo "python3 dtm.py \\"
echo "    --proto ${PROTO} \\"
echo "    --relay-ip 172.16.2.1 \\"
echo "    --relay-port 7001 \\"
echo "    --mode federated \\"
echo "    --fanout ${N} \\"
echo "    --dst-ip ${NODE_IP} \\"
echo "    --base-port ${BASE_PORT} \\"
echo "    --feedback-port ${ACK_PORT} \\"
echo "    --restart-script /scripts/restart_ndt.sh \\"
echo "    --experiment-duration 75 \\"
echo "    --log-file /home/ubuntu/exp1/1to${N}/dtm.log"
echo ""
echo "Step 2 — VM ON:"
echo "──────────────────────────────────────────────────"
echo "sudo python3 on_ndt.py --role on"
echo ""
echo "Step 3 — VM ON (once Mininet is up):"
echo "──────────────────────────────────────────────────"
echo "python3 onm.py --proto ${PROTO} --relay-ip 172.16.1.1 --relay-port 7001 --relay-kind router"
echo ""
echo "When done: ./stop_ndts.sh [./logs/run1/1to${N}]"