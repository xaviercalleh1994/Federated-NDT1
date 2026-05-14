#!/bin/bash
# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# ============================================================
# entrypoint_on.sh — ON Mininet container
#
# 1. Starts OVS (same as ndt entrypoint.sh)
# 2. Launches on_ndt.py --role on --bare-metal
#
# on_ndt.py --role on opens the Mininet CLI (blocking).
# The container stays alive waiting for kubectl exec from onm.py.
#
# --bare-metal: skips enp0s8/enp0s9 config (not present in container).
# topology is read from /run/topology_config.json (ConfigMap mount).
# ============================================================

set -e

echo "[entrypoint-on] ON container starting"

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
        && echo "[entrypoint-on] OVS ready (${i}s)" && break
    sleep 1
    [ "$i" -eq 15 ] && echo "[entrypoint-on] ERROR: OVS timeout" && exit 1
done

# ── Launch ON Mininet keeper ──────────────────────────────────
# --role on    : starts Mininet + CLI (blocking)
# --bare-metal : skip enp0s8/enp0s9 (not present in container)
# --topo-file  : reads topology from ConfigMap mount
exec python3 /app/on_ndt.py \
    --role on \
    --bare-metal \
    --topo-file /app/topology_config.json