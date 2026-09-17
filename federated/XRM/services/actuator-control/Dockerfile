# XRM/services/actuator-control/Dockerfile
# XR Service — Actuator Control
# Image: actuator-control:1.0
#
# Daemon that runs inside NDT pods. Listens for ACTUATOR_CMD
# on UDP :9200, forwards upstream to dtm-agent, receives
# ON_STATE_UPDATE broadcast on UDP :9203, stores context and
# replies to ndt-event-cli.
#
# Build (from federated/ root):
#   docker build -t actuator-control:1.0 \
#       -f XRM/services/actuator-control/Dockerfile \
#       XRM/services/actuator-control/
#
# Import to containerd:
#   docker save actuator-control:1.0 | \
#       sudo ctr -n k8s.io images import -

FROM python:3.11-slim

WORKDIR /app

# No extra dependencies — only stdlib + on_ndt.py:6001 (localhost)
COPY actuator_control.py .

ENTRYPOINT ["python3", "actuator_control.py"]