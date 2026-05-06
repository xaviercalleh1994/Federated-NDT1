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

ONM_CMD_FILE="onm_commands.txt"
ITER=15
DELAY=2   # segundos entre comandos

sleep 10

for i in $(seq 1 $ITER); do
  #echo "[SEND] Iteration $i"

  while IFS= read -r cmd; do
    echo "$cmd"
    sleep $DELAY
  done < "$ONM_CMD_FILE"

done