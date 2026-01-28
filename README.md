# NDT-PoC

This is a PoC for testing Event-driven synchronization between ON-NDT

## Architecture:

    1. VM ON
        Puente: enp0s3: 192.168.1.98/24 
        Red interna (ON) enp0s8: 172.16.1.2/30

    2. VM NDT
        Puente: enp0s3: 192.168.1.124/24 
        Red interna (DT) enp0s8: 172.16.2.2/30
        Red interna (K8S) enp0s9: 192.168.99.X

    3. VM K8S
        Puente: enp0s3: 192.168.1.123/24 
        Red interna (K8S) enp0s8: 192.168.99.2/30

    4. Communication channel
        COmmunication channel in two modalities with both TCP/UDP:

        4.1 Relay: VM router
            Puente: enp0s3: 192.168.1.125/24 
            Red interna (ON) enp0s8: 172.16.1.1/30
            Red interna (DT) enp0s9: 172.16.2.1/30  

        4.2 Cloud private server: 
            ip: 79.143.93.210

## Running the PoC:

### Create the ON and NDT
In the VM ON and VM NDT:

1. Create (if not created yet) the script nodes.py
```bash
nano on_ndt.py
```

### VM ON:
Initiate the ON
```bash
sudo python3 on_ndt.py --role on
```

### VM NDT
```bash
sudo python3 on_ndt.py --role ndt
```

### ORder for execute the test
First start the relay local or cloud:
```bash
python3 relay.py --proto <udp or tcp> --port 7001
```

Second start the DTM
```bash
python3 dtm.py --proto <udp or tcp> --relay-ip <172.16.1.1 or 79.143.93.210> --relay-port 7001
python3 dtm_v12.py --proto udp --relay-ip 79.143.93.210 --relay-port 7001 --relay-kind vps --log-file /home/ubuntu/ndtm_events.log & NDT_PID=$!
```

Start the ONM
```bash
python3 onm.py --proto <udp or tcp> --relay-ip <172.16.1.1 or 79.143.93.210> --relay-port 7001
sudo python3 onm_v12.py --proto udp --relay-ip 79.143.93.210 --relay-port 7001 --relay-kind vps --log-file /home/ubuntu/onm_events.log
```

```bash
python3 monitor_resources.py --pid 4569 --iface enp0s8 --out ndt_1to1_resources.csv --interval 0.2
```


caso federado:
en ON:
```bash
python3 onm_fed.py \
  --proto udp \
  --relay-ip <IP_ROUTER> \
  --base-port 7001 \
  --fanout 5 \
  --relay-kind router \
  --log-file /home/ubuntu/onm_events.log &
ON_PID=$!
```

```bash
python3 monitor_resources.py \
  --pid $ON_PID \
  --iface enp0s8 \
  --out on_federated_fanout5.csv \
  --interval 0.5
```

```bash
set r1 r2 up 50 1 10
set r1 r2 down 0 0 1000
```

en el router
```bash
python3 listen_ports.py \
  --proto udp \
  --base-port 7001 \
  --fanout 5 \
  --csv dummy_rx_fanout5.csv
```

📊 CÓMO SACAS LAS MÉTRICAS Y PLOTS (DESPUÉS)

Cuando tengas:

on_federated_fanout1.csv
on_federated_fanout2.csv
on_federated_fanout5.csv
on_federated_fanout10.csv
on_federated_fanout20.csv


Copias esos archivos a tu máquina de análisis (o una VM con venv limpio).

7️⃣ Máquina de análisis — Plots finales
python3 analyze_monitor_results.py \
  --mode federated \
  --csv on_federated_fanout1.csv on_federated_fanout2.csv \
        on_federated_fanout5.csv on_federated_fanout10.csv \
        on_federated_fanout20.csv \
  --outdir out_federated \
  --link-capacity-mbps 100



latencias
en el ON:
arrancar el onm
python3 onm_fed.py --proto udp --relay-ip 79.143.93.210 --base-port 7001 --fanout 2 --relay-kind vps --log-file /home/ubuntu/onm_fanout_udp_f2.log --pid-file /home/ubuntu/onm.pid

arrancar el monitor
python3 monitor_resources.py --pid $(cat /home/ubuntu/onm.pid)  --iface enp0s3 --out /home/ubuntu/fanout/onm_resources_tcp_f50.csv --interval 1.0

analizar consumo de recursos:
python3 analyze_monitor_resources.py   --input-dir /home/ubuntu/fanout   --outdir out_federated   --link-capacity-mbps 100