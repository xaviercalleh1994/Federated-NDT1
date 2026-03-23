#!/bin/bash

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