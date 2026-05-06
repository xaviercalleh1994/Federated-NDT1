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

# 2500 iteraciones * 4 comandos = 10,000 eventos en total
ITER=2500

# 0.001 segundos = 1 milisegundo (1000 eventos por segundo)
DELAY=0.001

# Pausa inicial para asegurar que el ONM en Python esté listo
sleep 10

for i in $(seq 1 $ITER); do
  while IFS= read -r cmd; do
    echo "$cmd"
    sleep $DELAY
  done < "$ONM_CMD_FILE"
done

# Damos un segundo de margen para que se vacíen las tuberías (pipes)
sleep 10
echo "quit"