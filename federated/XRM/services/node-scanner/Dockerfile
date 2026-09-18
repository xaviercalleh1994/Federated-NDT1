# XRM/services/node-scanner/Dockerfile
# XR Service — Node Scanner
# Image: node-scanner:1.0
#
# Daemon that runs inside NDT pods. Listens for XR_SERVICE_CMD
# on UDP :9201 (localhost only, from xrm-agent). Executes scan
# commands (scan_type: all/node/links/actuator) against the
# local Mininet instance via on_ndt.py:6001.
#
# Build (from federated/ root):
#   docker build -t node-scanner:1.0 \
#       -f XRM/services/node-scanner/Dockerfile \
#       XRM/services/node-scanner/
#
# Import to containerd:
#   docker save node-scanner:1.0 | \
#       sudo ctr -n k8s.io images import -

FROM python:3.11-slim

WORKDIR /app

# No extra dependencies — only stdlib + on_ndt.py:6001 (localhost)
COPY node_scanner.py .

ENTRYPOINT ["python3", "node_scanner.py"]