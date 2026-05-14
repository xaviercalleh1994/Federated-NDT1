# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# ============================================================
# Dockerfile — NDT (Network Digital Twin, Mininet keeper)
#
# Runs on_ndt.py --role ndt --bare-metal via entrypoint.sh.
# on_ndt.py only responsibility: start and maintain Mininet.
# All NDT event logic lives in dtm.py --mode agent
# (runs as a second container in the same Pod).
#
# Exposes control API on localhost:6001 for dtm-agent to call
# LINK_SET, EXEC, SNAPSHOT, RELOAD primitives.
#
# Requires privileged mode (OVS + Mininet kernel namespaces).
#
# Build:
#   docker build -t ndt-mininet:1.0 .
#
# Import to containerd:
#   docker save ndt-mininet:1.0 | sudo ctr -n k8s.io images import -
#
# Note: on_ndt.py and topology_config.json are mounted as
#       ConfigMaps at runtime — no need to COPY them here.
# ============================================================

FROM ubuntu:24.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    openvswitch-switch \
    openvswitch-common \
    openvswitch-testcontroller \
    python3 \
    python3-pip \
    mininet \
    net-tools \
    iproute2 \
    iptables \
    iputils-ping \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Allow ovs-controller for Mininet
RUN ln -sf /usr/bin/ovs-testcontroller /usr/local/bin/ovs-controller

# Install orjson
RUN pip3 install --no-cache-dir --break-system-packages orjson==3.10.1

WORKDIR /app

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# on_ndt.py and topology_config.json injected via ConfigMap at runtime

ENTRYPOINT ["/entrypoint.sh"]