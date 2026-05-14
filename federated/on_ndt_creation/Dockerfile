# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# ============================================================
# Dockerfile — ON (Original Network, Mininet keeper)
#
# Runs on_ndt.py --role on --bare-metal.
# on_ndt.py only responsibility: start and maintain Mininet.
# All ON logic lives in onm.py (runs in the same Pod).
#
# Exposes control API on localhost:6001 for onm.py to call
# LINK_SET, EXEC, SNAPSHOT, RELOAD primitives.
#
# Requires privileged mode (OVS + Mininet kernel namespaces).
#
# Build:
#   docker build -t on-mininet:1.0 .
#
# Import to containerd:
#   docker save on-mininet:1.0 | sudo ctr -n k8s.io images import -
#
# Note: on_ndt.py is mounted as a ConfigMap at runtime
#       (same pattern as ndt-mininet containers).
#       topology.json is mounted as a ConfigMap at /run/topology_config.json.
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

COPY entrypoint_on.sh /entrypoint_on.sh
RUN chmod +x /entrypoint_on.sh

# on_ndt.py is injected via ConfigMap at /app/on_ndt.py

ENTRYPOINT ["/entrypoint_on.sh"]