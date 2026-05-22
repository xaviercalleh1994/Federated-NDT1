# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# ============================================================
# Dockerfile — DTM (Digital Twin Manager)
#
# Used for three roles depending on --mode argument:
#
#   --mode 1:1        DTM core, 1:1 with a single local NDT.
#                     Applies events via on_ndt.py on localhost:6001.
#
#   --mode federated  DTM core, fans out events to N NDT Pods.
#                     Tracks ACKs via FeedbackController.
#
#   --mode agent      DTM distributed agent. Runs inside an NDT Pod
#                     alongside on_ndt.py. Receives events from DTM
#                     core, applies them via localhost:6001, sends ACKs.
#
# No privileged mode needed — pure network logic.
# Dependencies: Python 3.10 + orjson only.
#
# Build:
#   docker build -t dtm:1.0 .
#
# Import to containerd:
#   docker save dtm:1.0 | sudo ctr -n k8s.io images import -
# ============================================================

# xrm/Dockerfile
# Image: xrm:1.0
#
# Build:
#   docker build -t xrm:1.0 -f xrm/Dockerfile .
#
# Import to containerd:
#   docker save xrm:1.0 | sudo ctr -n k8s.io images import -
 
FROM python:3.10-slim
 
WORKDIR /app
 
COPY xrm.py .
 
ENTRYPOINT ["python3", "xrm.py"]
CMD ["--mode", "core"]
