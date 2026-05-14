#!/bin/bash
# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# ============================================================
# entrypoint.sh — NDT Mininet container
#
# 1. Starts OVS
# 2. Launches on_ndt.py --role ndt --bare-metal (Mininet keeper)
#
# on_ndt.py --role ndt starts Mininet and exposes the control
# API on localhost:6001. It does NOT handle events — that is
# the responsibility of dtm.py --mode agent running alongside.
#
# The container prints "NDT keeper running" when ready.
# launch_ndts.sh waits for this string before proceeding.
# ============================================================

set -e

NDT_PORT=${NDT_PORT:-8001}
NDT_PROTO=${NDT_PROTO:-udp}
CTRL_PORT=${CTRL_PORT:-6653}

echo "[entrypoint] NDT starting port=${NDT_PORT} proto=${NDT_PROTO} ctrl=${CTRL_PORT}"

# ── Start OVS ────────────────────────────────────────────────
mkdir -p /var/run/openvswitch /etc/openvswitch /var/log/openvswitch

if [ ! -f /etc/openvswitch/conf.db ]; then
    ovsdb-tool create /etc/openvswitch/conf.db \
        /usr/share/openvswitch/vswitch.ovsschema
fi

ovsdb-server /etc/openvswitch/conf.db \
    --remote=punix:/var/run/openvswitch/db.sock \
    --remote=db:Open_vSwitch,Open_vSwitch,manager_options \
    --pidfile --detach \
    --log-file=/var/log/openvswitch/ovsdb.log

ovs-vsctl --no-wait init

ovs-vswitchd --pidfile --detach \
    --log-file=/var/log/openvswitch/vswitchd.log

for i in $(seq 1 15); do
    ovs-vsctl show > /dev/null 2>&1 \
        && echo "[entrypoint] OVS ready (${i}s)" && break
    sleep 1
    [ "$i" -eq 15 ] && echo "[entrypoint] ERROR: OVS timeout" && exit 1
done

# ── Launch NDT Mininet keeper ─────────────────────────────────
# --role ndt    : starts Mininet, exposes API on localhost:6001
# --bare-metal  : skip enp0s8/enp0s9 (not present in container)
# --topo-file   : reads topology from ConfigMap mount
# --controller-port: unique OVS port per container instance
exec python3 /app/on_ndt.py \
    --role ndt \
    --bare-metal \
    --topo-file /app/topology_config.json \
    --controller-port "${CTRL_PORT}"