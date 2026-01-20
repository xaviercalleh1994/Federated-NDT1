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
```

Start the ONM
```bash
python3 onm.py --proto <udp or tcp> --relay-ip <172.16.1.1 or 79.143.93.210> --relay-port 7001
```
