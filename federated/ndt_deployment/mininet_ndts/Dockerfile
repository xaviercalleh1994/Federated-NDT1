# ============================================================
# Dockerfile — NDT Mininet container
# Base: ubuntu:24.04 (matches Linux Mint 22.3 kernel)
#
# Build (once):
#   docker build -t ndt-mininet .
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

# Allow ovs-controller for mininet
RUN ln -sf /usr/bin/ovs-testcontroller /usr/local/bin/ovs-controller

# Install orjson for log collection
RUN pip3 install --no-cache-dir --break-system-packages orjson

WORKDIR /app

# PRepare entry point file which deploys a single NDT
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]