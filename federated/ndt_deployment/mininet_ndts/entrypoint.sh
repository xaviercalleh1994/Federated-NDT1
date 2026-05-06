#!/bin/bash

# Copyright (C) 2026 Xavier Calle-Heredia, Xavier Hesselbach
# Universitat Politècnica de Catalunya (UPC)
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <https://www.gnu.org/licenses/>.

set -e

NDT_PORT=${NDT_PORT:-8001}
NDT_PROTO=${NDT_PROTO:-tcp}
DTM_IP=${DTM_IP:-""}
ACK_PORT=${ACK_PORT:-9000}
CTRL_PORT=${CTRL_PORT:-6653}

echo "[entrypoint] NDT starting port=${NDT_PORT} proto=${NDT_PROTO} ctrl=${CTRL_PORT}"

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
    ovs-vsctl show > /dev/null 2>&1 && echo "[entrypoint] OVS ready (${i}s)" && break
    sleep 1
    [ "$i" -eq 15 ] && echo "[entrypoint] ERROR: OVS timeout" && exit 1
done

exec python3 /app/on_ndt.py \
    --role ndt \
    --relay-port "${NDT_PORT}" \
    --proto "${NDT_PROTO}" \
    --dtm-ip "${DTM_IP}" \
    --ack-port "${ACK_PORT}" \
    --bare-metal \
    --controller-port "${CTRL_PORT}"