# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# ============================================================
# Dockerfile — ONM (Original Network Manager)
#
# Runs onm.py which contains full ONM logic:
#   - Interactive CLI (set, exec, topology, show, quit)
#   - Topology graph resolution (logical → physical)
#   - Calls on_ndt.py primitives via localhost:6001
#   - Sends events to DTM via UDP/TCP
#
# This container always runs in the same Pod as the ON container
# (on_ndt.py --role on) so that localhost:6001 is reachable.
#
# No privileged mode needed.
#
# Build:
#   docker build -t onm:1.0 .
#
# Import to containerd:
#   docker save onm:1.0 | sudo ctr -n k8s.io images import -
#
# Usage (kubectl exec into the onm container):
#   kubectl exec -it pod-on-onm -n ndt -c onm -- python3 onm.py \
#       --proto udp --relay-ip svc-dtm \
#       --relay-port 7001 --relay-kind router
#
# Automated experiment:
#   kubectl exec -it pod-on-onm -n ndt -c onm -- bash -c \
#       "python3 on_events_gen.py --rate 0.5 --duration 120 | \
#        python3 onm.py --proto udp --relay-ip svc-dtm \
#                       --relay-port 7001 --relay-kind router"
# ============================================================

FROM python:3.10-slim

WORKDIR /app

COPY onm.py .
COPY on_events_gen.py .
COPY onm_commands.txt .
COPY send_events.sh .
COPY sned_events_mass.sh .

RUN chmod +x send_events.sh sned_events_mass.sh

# Keep container alive for kubectl exec
CMD ["sleep", "infinity"]