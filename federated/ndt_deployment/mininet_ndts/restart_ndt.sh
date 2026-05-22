#!/bin/bash

# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# ============================================================
# restart_ndt.sh — NDT pod restart with state preservation
#
# Called by dtm.py via subprocess when a RESTART_NDT event is
# received. The DTM has already persisted the current network
# state to /tmp/dtm-state/ndt-{id}/state.json before calling
# this script. On restart, dtm-agent reads that file and
# re-applies all links via on_ndt.py:6001.
#
# This script is the only component that calls kubectl.
# It is mounted into the DTM container via a hostPath volume
# at /scripts/restart_ndt.sh so the container can call it
# via subprocess without having kubectl installed.
#
# Usage (called by dtm.py):
#   bash restart_ndt.sh NDT_ID DTM_IP BASE_PORT PROTO ACK_PORT
#
# Environment variables (optional, set by dtm.py):
#   XRM=1            include xrm-agent container in restarted pod
#   XRM_CORE_IP=...  XRM core IP for xrm-agent registration
#
# Direct usage (operator):
#   ./restart_ndt.sh 0 10.4.47.53 30001
#   XRM=1 XRM_CORE_IP=10.4.47.53 ./restart_ndt.sh 0 10.4.47.53 30001
# ============================================================

set -euo pipefail

NDT_ID=${1:?"Usage: restart_ndt.sh NDT_ID DTM_IP BASE_PORT [PROTO] [ACK_PORT]"}
DTM_IP=${2:?"Usage: restart_ndt.sh NDT_ID DTM_IP BASE_PORT [PROTO] [ACK_PORT]"}
BASE_PORT=${3:?"Usage: restart_ndt.sh NDT_ID DTM_IP BASE_PORT [PROTO] [ACK_PORT]"}
PROTO=${4:-udp}
ACK_PORT=${5:-9000}
XRM=${XRM:-0}
XRM_CORE_IP=${XRM_CORE_IP:-${DTM_IP}}
MAX_WAIT=120

POD_NAME="ndt-$(( NDT_ID + 1 ))"
NODE_PORT=$(( BASE_PORT + NDT_ID ))
CTRL_PORT=$(( 6653 + NDT_ID ))
NDT_INTERNAL_PORT=8001
NAMESPACE="ndt"
IMAGE="ndt-mininet:1.0"
DTM_IMAGE="dtm:1.0"
XRM_IMAGE="xrm:1.0"
PROTO_UPPER=$(echo "${PROTO}" | tr '[:lower:]' '[:upper:]')

echo "[RESTART] ${POD_NAME} requested by DTM" >&2

# ── XRM optional blocks ──────────────────────────────────────
XRM_VOLUME=""
XRM_CONTAINER=""
if [ "${XRM}" = "1" ]; then
    mkdir -p "/tmp/xrm-state/ndt-${NDT_ID}"
    XRM_VOLUME="
    - name: xrm-state
      hostPath:
        path: /tmp/xrm-state/ndt-${NDT_ID}
        type: DirectoryOrCreate"
    XRM_CONTAINER="
    - name: xrm-agent
      image: ${XRM_IMAGE}
      imagePullPolicy: Never
      args:
        - \"--mode\"
        - \"agent\"
        - \"--ndt-id\"
        - \"${NDT_ID}\"
        - \"--core-ip\"
        - \"${XRM_CORE_IP}\"
        - \"--log-file\"
        - \"/tmp/xrm_agent_${NDT_ID}.log\"
      volumeMounts:
        - name: run-data
          mountPath: /run
        - name: xrm-state
          mountPath: /tmp/xrm-state/ndt-${NDT_ID}"
fi

# ── Delete existing pod ───────────────────────────────────────
kubectl delete pod "${POD_NAME}" -n "${NAMESPACE}" \
    --ignore-not-found --grace-period=5 --wait=true

# ── Recreate pod ─────────────────────────────────────────────
kubectl apply -f - <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: ndt-exp1
    ndt-index: "$(( NDT_ID + 1 ))"
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
      emptyDir: {}
    - name: dtm-state
      hostPath:
        path: /tmp/dtm-state/ndt-${NDT_ID}
        type: DirectoryOrCreate
    - name: scripts
      hostPath:
        path: $(dirname "$(readlink -f "$0")")
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
  name: svc-${POD_NAME}
  namespace: ${NAMESPACE}
  labels:
    app: ndt-exp1
    ndt-index: "$(( NDT_ID + 1 ))"
spec:
  type: NodePort
  selector:
    app: ndt-exp1
    ndt-index: "$(( NDT_ID + 1 ))"
  ports:
    - name: ndt-${PROTO}
      protocol: ${PROTO_UPPER}
      port: ${NDT_INTERNAL_PORT}
      targetPort: ${NDT_INTERNAL_PORT}
      nodePort: ${NODE_PORT}
EOF

# ── Wait for pod to be ready ──────────────────────────────────
elapsed=0
until [ "${elapsed}" -ge "${MAX_WAIT}" ]; do
    sleep 1; elapsed=$(( elapsed + 1 ))
    PHASE=$(kubectl get pod "${POD_NAME}" -n "${NAMESPACE}" \
        --no-headers -o custom-columns=":status.phase" 2>/dev/null \
        || echo "Unknown")
    if [ "${PHASE}" = "Failed" ] || \
       kubectl get pod "${POD_NAME}" -n "${NAMESPACE}" 2>/dev/null | \
       grep -qE "CrashLoopBackOff|OOMKilled"; then
        echo "[RESTART] ERROR: ${POD_NAME} failed" >&2
        exit 1
    fi
    NDT_READY=$(kubectl logs "${POD_NAME}" -n "${NAMESPACE}" \
        -c ndt 2>/dev/null | grep -c "Mininet ready" || true)
    AGENT_READY=$(kubectl logs "${POD_NAME}" -n "${NAMESPACE}" \
        -c dtm-agent 2>/dev/null | grep -c "AGENT.*Ready" || true)
    if [ "${NDT_READY}" -gt 0 ] && [ "${AGENT_READY}" -gt 0 ]; then
        echo "[RESTART] ${POD_NAME} ready (${elapsed}s)" >&2
        exit 0
    fi
done

echo "[RESTART] TIMEOUT: ${POD_NAME} not ready after ${MAX_WAIT}s" >&2
exit 1