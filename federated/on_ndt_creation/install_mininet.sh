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

set -euo pipefail

sudo apt update
sudo apt install -y \
    mininet \
    openvswitch-switch \
    openvswitch-common \
    openvswitch-testcontroller

# Symlink
sudo ln -sf /usr/bin/ovs-testcontroller /usr/local/bin/ovs-controller

# activate OVS
sudo systemctl enable openvswitch-switch
sudo systemctl start  openvswitch-switch

# check
sudo python3 -c "import mininet; print('mininet OK')"
ovs-controller --version && echo "ovs-controller OK"