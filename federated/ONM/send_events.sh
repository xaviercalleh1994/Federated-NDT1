#!/bin/bash

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