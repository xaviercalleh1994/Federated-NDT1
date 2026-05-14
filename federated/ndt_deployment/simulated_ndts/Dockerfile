# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# ============================================================
# Dockerfile — NDT Simulator (Experiment 2)
#
# Passive sink (listen mode) or upstream event generator
# (generate mode). No privileged mode needed.
#
# Build:
#   docker build -t ndt-sim:1.0 .
#
# Import to containerd:
#   docker save ndt-sim:1.0 | sudo ctr -n k8s.io images import -
#
# Listen mode (Exp. 2 — passive sinks replacing ndt-mininet):
#   python3 ndt_gen.py --mode listen \
#       --fanout 8 --proto udp --base-port 8001
#
# Generate mode (upstream load analysis):
#   python3 ndt_gen.py --mode generate \
#       --fanout 8 --rate 2000 --arrival-mode uniform \
#       --feedback-ip svc-dtm --feedback-port 9000 \
#       --duration 60 --start-delay 10
# ============================================================

FROM python:3.10-slim

WORKDIR /app

COPY ndt_gen.py .

ENTRYPOINT ["python3", "ndt_gen.py"]