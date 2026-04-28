#!/bin/bash

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