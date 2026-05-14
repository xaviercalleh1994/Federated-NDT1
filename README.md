# Federated NDT Framework
This framework is a Proof-of-Concept (PoC) for the federation of multiple Network Digital Twin (NDT) instances, as proposed by Xavier Calle-Heredia and Xavier Hesselbach (UPC). 

The proposed federation of multiple NDTs is a novel topic that is envisioned as 

## Overview
Currently, the framework is provided with two operational configurations: 
- The federated Mode (1:N) (***Under development and testing***): 
    The framework considers an existing network deployed with Mininet technology, defined as Original Network (ON), which emulates a simple existing network. With this framework it is possible to deploy multiple Mininet replicas of the ON. The framework relies in two manager entities: the Original Network Manager (ONM) and the Digital Twin Manager (DTM).

    - The ONM oversees the ON and receives events intended to modify the topology of the ON. Then, forwards the event towards the DTM.
    - The DTM is the main entity of the system and is in charge of the synchronization of the NDTs. The DTM is decompossed in two agents: the DTM Core is a centralized agent that receives and distribute ON events. While the DTM distributed agents are in charge of implementing each ON event into the NDT.
    ![Framework_architecture_fed](Images/PoC_fed_arch.png)
- A conventional mode (1:1): this is the currently analyzed configuration in the literature and considers a sinlge NDT instance derived from an ON. This mode will be further analyzed for the inclusion of Extended Reality (XR) services.
![Framework_architecture_fed_11](Images/PoC_11_arch.png)

## Framework Components
The framework relies in a Kubernetes architecture to support the deployment of each component. Therefore, main components deployed as K8s pods are:
- ONM 
- DTM (core)
- ON
- NDTs (includes two containers: the NDT and the DTM distributed agent)

The ON as well as the NDTs are created from [topology](federated/topology.json) json file. The current topology is shown next:
![net_arch](Images/mininet_net.png)


## Requirements

### Single machine (v1.6 — Kubernetes)
| Component | Version |
|-----------|---------|
| Linux | Ubuntu 24.04 / Linux Mint 22.3 |
| Kubernetes | v1.34+ |
| containerd | v2.1+ |
| Calico CNI | any |
| Docker | 24+ (for building images) |
| Python | 3.10+ |
| Open vSwitch | 3.x |
| Mininet | 2.3+ |

Python dependencies (see `requirements.txt`):
- `orjson` — fast JSON serialization in DTM
- `mininet` — network emulation (inside containers)

---

## Repository Structure
This repository includes the necessary files for supporting the experiments including: python scripts, bash scripts, Dockerfiles and yaml files

```
federated-ndt-framework/
└── federated/
    ├── DTM/
    │   ├── dtm.py                  DTM core + distributed agent
    │   └── Dockerfile
    ├── ONM/
    │   ├── onm.py                  ONM logic + CLI
    │   ├── on_events_gen.py        Automated event injector
    │   ├── onm_commands.txt        Default command set
    │   ├── send_events.sh
    │   └── Dockerfile
    ├── on_ndt_creation/
    │   ├── on_ndt.py               Mininet keeper (ON + NDT roles)
    │   ├── Dockerfile
    │   └── entrypoint_on.sh
    ├── ndt_deployment/
    │   ├── mininet_ndts/
    │   │   ├── Dockerfile
    │   │   ├── entrypoint.sh
    │   │   ├── launch_ndts.sh
    │   │   └── stop_ndts.sh
    │   └── simulated_ndts/
    │       ├── ndt_gen.py          NDT simulator (Exp. 2)
    │       └── Dockerfile
    ├── topology.json               Default 2-LAN topology
    ├── kubernetes/
    │   ├── namespace.yaml
    │   ├── on-pod.yaml             Pod ON + Service svc-on
    │   ├── onm-pod.yaml            Pod ONM (hostNetwork)
    │   ├── dtm-pod.yaml            Pod DTM core (hostNetwork, federated)
    │   ├── dtm-ndt-pod.yaml        Pod DTM+NDT (1:1 mode)
    │   ├── launch_testbed.sh       Full testbed launcher
    │   └── stop_testbed.sh         Full testbed stopper
    └── analysis/
        ├── experiment_1.py
        └── experiment_2.py
```

---

## Topology files
 
Topology files define the Mininet network structure loaded by the ON and all NDTs. They are JSON files with the following format:
 
```json
{
  "description": "2-LAN topology",
  "backbone": {"nat_ip": "10.0.0.254/24"},
  "lans": [
    {"id": 1, "subnet": "192.168.10", "hosts": 2},
    {"id": 2, "subnet": "192.168.20", "hosts": 2}
  ]
}
```
 
The default topology (`federated/topology.json`) is a 2-LAN topology. The ONM image includes two topology files available at runtime:
- `/app/2lan.json` — 2-LAN topology (default, 2 routers, 4 hosts)
- `/app/3lan.json` — 3-LAN topology (3 routers, 6 hosts)
To add more topology files, copy them to `ONM/` before building the image — the Dockerfile copies all `.json` files from that directory into `/app/`.
 
The topology is shared between the ON and all NDTs via a hostPath volume (`/tmp/ndt-run`) mounted at `/run` inside each pod. When the ONM writes a new topology, all pods read it from the same path.
 
---


## Prepare the environment
### 1. Build Docker images and import into containerd
 
```bash
cd federated-ndt-framework/federated
 
docker build -t dtm:1.0         DTM/
docker build -t onm:1.0         ONM/
docker build -t on-mininet:1.0  on_ndt_creation/
docker build -t ndt-mininet:1.0 ndt_deployment/mininet_ndts/
```
 
Then import each image into containerd so Kubernetes can use them:
 
```bash
docker save dtm:1.0         | sudo ctr -n k8s.io images import -
docker save onm:1.0         | sudo ctr -n k8s.io images import -
docker save on-mininet:1.0  | sudo ctr -n k8s.io images import -
docker save ndt-mininet:1.0 | sudo ctr -n k8s.io images import -
```

### 2. Verify images

```bash
sudo crictl images | grep -E "dtm|onm|mininet"
```

## Running the Testbed

### Federated mode (1:N)

```bash
cd federated/kubernetes

# Launch testbed with N=2 NDTs
./launch_testbed.sh --mode federated --fanout 2

# Check all pods are Running
kubectl get pods -n ndt
```

Expected pods:
```
ndt-1     2/2     Running   (ndt + dtm-agent)
ndt-2     2/2     Running   (ndt + dtm-agent)
pod-dtm   1/1     Running   
pod-on    1/1     Running   
pod-onm   1/1     Running   
```

To start the experiment, you need to run the ONM:
```bash
kubectl exec -it pod-onm -n ndt -- python3 onm.py \
    --proto udp --relay-ip 127.0.0.1 \
    --relay-port 7001 --relay-kind router \
    --on-ip 10.233.85.156 \
    --log-file /tmp/onm.log
```

You can use multiple windows to oversee the ON and each NDT and execute them:
```bash
# Original network:
kubectl exec -it pod-on -n ndt -- bash
```

```bash
# NDT 1:
kubectl exec -it ndt-1 -n ndt -- bash
```

```bash
# NDT 2:
kubectl exec -it ndt-2 -n ndt -- bash
```

In the ON and NDTs pods you can check connectivity between r1 and r2 routers
```bash
mnexec -a $(pgrep -f "mininet:r1") ping -c 3 10.0.0.22
PING 10.0.0.2 (10.0.0.2) 56(84) bytes of data.
64 bytes from 10.0.0.2: icmp_seq=1 ttl=64 time=2.41 ms
64 bytes from 10.0.0.2: icmp_seq=2 ttl=64 time=0.664 ms
64 bytes from 10.0.0.2: icmp_seq=3 ttl=64 time=0.064 ms
```

Then, in the ONM pod you can disable the link just by introducing the comand in the CLI:
```bash
> set r1 r2 down 0 0 1000
[ONM] Applied locally and sent to DTM
> 
```

Chcek again in the ON and NDTs pods the r1-r2 connectivity:
```bash
mnexec -a $(pgrep -f "mininet:r1") ping -c 3 10.0.0.22
ping: connect: Network is unreachable
```

You can restart the link in the ONM:
```bash
> set r1 r2 down 0 0 1000
[ONM] Applied locally and sent to DTM
> 
```

In the ON and NDTs pods you can check connectivity between r1 and r2 routers
```bash
mnexec -a $(pgrep -f "mininet:r1") ping -c 3 10.0.0.22
PING 10.0.0.2 (10.0.0.2) 56(84) bytes of data.
64 bytes from 10.0.0.2: icmp_seq=1 ttl=64 time=2.41 ms
64 bytes from 10.0.0.2: icmp_seq=2 ttl=64 time=0.664 ms
64 bytes from 10.0.0.2: icmp_seq=3 ttl=64 time=0.064 ms
```

## Dynamic Topology Change
 
The ONM supports changing the network topology at runtime without restarting the testbed. When a new topology is applied, the ON and all NDTs reload Mininet with the new configuration simultaneously.
 
### How it works
 
1. The ONM writes the new topology to `/run/topology_config.json` (shared hostPath volume).
2. The ONM sends a `RELOAD` command to the ON via the control API.
3. The ONM sends a `TOPOLOGY_UPDATE` event to the DTM core.
4. The DTM core forwards the `TOPOLOGY_UPDATE` to each NDT's distributed agent.
5. Each DTM agent writes the new topology to `/run/topology_config.json` and sends a `RELOAD` to its local NDT.
6. All Mininet instances restart with the new topology.
### Topology files in the ONM pod
 
The ONM image includes two topology files at `/app/`:
- `/app/2lan.json` — 2-LAN topology (default)
- `/app/3lan.json` — 3-LAN topology
To add a custom topology permanently, copy the JSON file to `ONM/` and rebuild the image:
 
```bash
cp my_topology.json federated/ONM/
docker build -t onm:1.0 federated/ONM/
docker save onm:1.0 | sudo ctr -n k8s.io images import -
```
 
### Applying a topology change
 
In the ONM CLI:
```
> topology /app/3lan.json
[ONM] New topology saved to /run/topology_config.json
[ONM] ON Mininet reloading...
[ONM] ON reload complete (3 LANs ready)
[ONM] TOPOLOGY_UPDATE sent to DTM (3 LANs)
[ONM] Graph reloaded: 14 nodes
```
 
To revert to 2 LANs:
```
> topology /app/2lan.json
```
 
### Verifying the topology change
 
After applying a new topology, verify that the ON and all NDTs reloaded correctly:
 
```bash
# ON — should show 3 LANs
kubectl logs pod-on -n ndt --since=2m | grep -E "TOPO|LAN"
 
# NDT-1 — should show 3 LANs
kubectl logs ndt-1 -n ndt -c ndt --since=2m | grep -E "TOPO|LAN"
 
# NDT-2 — should show 3 LANs
kubectl logs ndt-2 -n ndt -c ndt --since=2m | grep -E "TOPO|LAN"
```
 
Expected output for each:
```
*** [TOPO] Loaded /run/topology_config.json — 3 LANs, 6 hosts
*** LAN 1: 192.168.10.x, 2 hosts
*** LAN 2: 192.168.20.x, 2 hosts
*** LAN 3: 192.168.30.x, 2 hosts
*** [TOPO] Exported topology to /run/topology.json
```
 
To verify connectivity in the new LAN 3 (from inside the ON or any NDT pod):
```bash
mnexec -a $(pgrep -f "mininet:h3_1") ping -c 3 8.8.8.8
PING 8.8.8.8 (8.8.8.8) 56(84) bytes of data.
64 bytes from 8.8.8.8: icmp_seq=1 ttl=113 time=15.2 ms
64 bytes from 8.8.8.8: icmp_seq=2 ttl=113 time=10.9 ms
64 bytes from 8.8.8.8: icmp_seq=3 ttl=113 time=11.4 ms
```
















## Running the Testbed
### Federated mode (1:N)

```bash
cd federated

# Launch testbed with N=2 NDTs
./kubernetes/launch_testbed.sh --mode federated --fanout 2--mode federated --fanout 2

# Check all pods are Running
kubectl get pods -n ndt
```



After completing the set-up process, the testbed is ready to run. We have thought in two experiments. 
- Experiment 1 (Ready to test): Intended to test the synchronization from the ON to the N NDTs. Therefore, the ONM will receive a modification command, the ONM will apply this in the ON mininet and after that i will forward the modification to the DTM which will forward the command and apply it to the NDTs.
- Experiment 2 (***Under development***): Intended to demonstrate the DTM coordination whn receiving updates from the NDTs and requires to arrange them to forward them properly to the ONM.

Therefore, we will focus just in Experiment 1. The order to run the experiment is:
### 1. Run the N NDTs (PC2):
```bash
./launch_ndts.sh <number of NDTs> <DTMs IP Address> <listening port> udp <ACK port> 
# For instantce:
./launch_ndts.sh 8 10.4.47.53 8001 udp 9000
# Wait until all NDTs are ready.
# You can check with:
docker ps
```
![ndts_ready](Images/ndts_ready.png)

### 2. Run the router VM
```bash
python3 relay.py --proto udp --port 7001
```

### 3. Run the ON (PC1-VM ON)
```bash
sudo python3 on_ndt.py --role on
```
![ON-mininet](Images/on_mininet.png)

### 4. Run the DTM (PC1-VM DTM)
Before running, it is required to install some python dependencies to properly run the DTM and then, gather the latency metrics. 
Before running the DTM for the first time you can create a virtual environment and install dependecies:
```bash
chmod +x install_dependencies.sh 
./install_dependencies.sh 
```

Once the script has concluded the virtual environment is ready and you can start the DTM. If you want to use the DTM again, you just need to activate the virtual environment:
```bash
source venv/bin/activate
```

```bash
python3 dtm.py \
    --proto udp \
    --relay-ip 172.16.2.1 \
    --relay-port 7001 \
    --mode federated \
    --fanout <Number of NDTs> \
    --dst-ip <PC2 ip address> \
    --base-port <NDTs listenning port> \
    --feedback-port <ACKs receiving port> \
    --experiment-duration <Duration time in seconds> \
    --log-file /home/ubuntu/exp1/run1/1to1/dtm.log

# For instance:
python3 dtm.py \
    --proto udp \
    --relay-ip 172.16.2.1 \
    --relay-port 7001 \
    --mode federated \
    --fanout 1 \
    --dst-ip 10.4.47.50 \
    --base-port 8001 \
    --feedback-port 9000 \
    --experiment-duration 150 \
    --log-file /home/ubuntu/exp1/run1/1to1/dtm.log
```
![DTM_run](Images/dtm_run.png)

We recommend to save dtm log files in an appropriate folde structure similar as the one shown next where 1toX will store the dtm log of the experiment with X NDTs deployed. This will facilitate gathering the federation metrics.

- exp1/
    - run1/
        - 1to1/
            - dtm.log
        - 1to8/
            - dtm.log
        - 1to16/
        - 1to64/
    - run2/
        - 1to1/
        - 1to64/
    - run3/
    - run4/
    - run5/

### 5. Run the ONM (PC1-VM ON)
The ONM can be run as follows:
```bash
python3 onm.py \
    --proto udp \
    --relay-ip 172.16.1.1 \
    --relay-port 7001 \
    --relay-kind router
```

![ONM_cli](Images/onm_cli.png)

Then, you can manually introduce a command from [ON-commands](federated/ONM/onm_commands.txt) (Just copy and paste). 

In order to automate the experiment, we have prepared a script to automatically send the commands each 2 seconds (a safe time to allow each NDT to apply the modification). Therefore, we recommend to run the ONM like this:
```bash
./send_events.sh | python3 onm.py \
    --proto udp \
    --relay-ip 172.16.1.1 \
    --relay-port 7001 \
    --relay-kind router
```

You can check that the synchronization is actually working (Running the ON in manual mode, i.e. without send_events.sh). To do so, you can send an ON command and then access to any NDT container as follows:
```bash
# In VM ON in ONM console down the link between r1 and r2 nodes:
set r1 r2 down 0 0 1000

# in PC2:
mnexec -a $(pgrep -f "mininet:r1") ping 10.0.0.2
# you should notice that r2 is unreachable from r1. 
# You can bring the link up again from the ONM and re-test.
```
![ping_test](Images/ping_test_ndt.png)


### Gather metrics
Metrics that can be retrieved are related with the synchronization latencies due to federation. To gather the metrics use the script [exp-1-analysis](federated/analysis/experiment_1.py):
```bash
python3 experiment_1.py \
    --logs /home/ubuntu/exp1/ \
    --out  /home/ubuntu/exp1/results/
```

## License
This project is licensed under the GNU General Public License v3.0.
See the [LICENSE](LICENSE) file for details.

# What is comming in the future?
## Further Implementation of Experiment 2 (***Under development and testing***) and Experiment 3 (XR integration):

## Set the host laptop in high efficiency mode:
```bash
# ON
powercfg /setactive 8c5e7fda-e8bf-4a96-9a85-a6e23a8c635c
# off
powercfg /setactive 381b4222-f694-41f0-9685-ff5bb260df2e
```

## Start the Experiments
### ONM
To autoamte the ONM commands execution there are two supporting files: on_events_gen.py and onm_commands.txt.
Arguments: 
- file: contains the ON commands.
- rate: rate in ev/second.
- duration: experiment duration in seconds.
- monck-on: flag to avoid the execution of the events in the ON
- log-file: path where a supporting log is saved (optional)

```bash
python3 on_events_gen.py --file onm_commands.txt --rate <rate: events per second> --duration <experiment time> | python3 onm.py --proto tcp --relay-ip 172.16.1.1 --relay-port 7001 --relay-kind vps  --mock-on  --log-file /home/ubuntu/final_test/2core/1to64_tcp/onm.log
```

### DTM:
- 1:1 Mode:

```bash
python3 dtm.py --proto tcp --relay-ip 172.16.2.1 --relay-port 7001 --mode 1:1 --log-file /home/ubuntu/dtm_events.log -experiment-duration 75
```

- 1:N Mode:

To further gather the metrics, it is recommended next folder structure for saving experiments, where 1toN it is the suggested name of the folder which stores the DTM log and N is the number of NDTs deployed:

- losses/
    - run1/
        - 1to1/
            - dtm.log
        - 1to5/
            - dtm.log
        - 1to10/
        - 1to20/
    - run2/
        - 1to1/
        - 1to20/
    - run3/
    - run4/
    - run5/

```bash
python3 dtm.py --proto tcp  --relay-ip 172.16.2.1  --relay-port 7001  --mode federated  --fanout 15  --dst-ip 192.168.99.2  --base-port 8001  --feedback-port 9000  --group-size 1  --wave-delay 0  --max-queue 30  --experiment-duration 75  --disable-priorities  --log-file /home/ubuntu/final_test/experiment2/losses/1to15/dtm.log
```

Arguments:
  - mode: 1:1/federated
  - fanout (federated mode): Number of fan out NDTs
  - dst-ip (federated mode): IP address of the VM that deploys and generates NDT events.
  - base-port (federated mode): port used by the DTM to transmit the ON events to the NDTs.
  - feedback-port (federated mode): port used by the DTM to listen to the NDT events.
  - group size (federated mode-optional): used to forward events to the NDTs in groups.
  - wave-delay (federated mode-optional): delay between transmission groups.
  - max-queue (federated mode-optional): size of the queue handled by the DTM, if no size is proved the queue is unlimited.
  - experiment-duration: duration of the experiment in seconds.
  - disable-priorities (federated mode-optional): if it is turned on the events are discarded equally when the queue is full; if is turned off the critical events are forwarded while non-critical events might be discarded.

### NDTs and generator:
Generate mode
- Uniform

```bash
python3 ndt_gen.py --mode generate --fanout 15 --feedback-ip 192.168.99.1 --feedback-port 9000 --priority-mode paper_mix --rate 3000 -arrival-mode uniform --duration 60 --start-delay 5
```

- Poisson

```bash
python3 ndt_gen.py --mode generate --fanout 15 --feedback-ip 192.168.99.1 --feedback-port 9000 --priority-mode paper_mix --rate 3000 -arrival-mode poisson --duration 60 --start-delay 5
```

### Get metrics:
```bash
python3 experiment_2.py --logs /home/ubuntu/final_test/experiment2/losses/ --out  /home/ubuntu/final_test/results/losses/ --C 2000
```
