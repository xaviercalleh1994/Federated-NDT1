# VBox Virtual Machines configuration guide
This tutorial will guide you to properly configure each VM for running the PoC.

## Script to run in each VM to configure interfaces
### In the VM ON
```bash
sudo nano /root/arranque_red.sh
```

```bash
#!/bin/bash
# Activar forwarding (necesario para Mininet)
sysctl -w net.ipv4.ip_forward=1
# Configurar IP hacia el Router
ip addr flush dev enp0s8
ip addr add 172.16.1.2/30 dev enp0s8
ip link set enp0s8 up
# Ruta por defecto interna (Todo lo que no sea local, al Router VM)
ip route add 10.0.2.0/24 via 172.16.1.1
```

### In the VM NDT
```bash
sudo nano /root/arranque_red.sh
```

```bash
#!/bin/bash
sysctl -w net.ipv4.ip_forward=1
# Pata hacia Router Central
ip addr flush dev enp0s8
ip addr add 172.16.2.2/30 dev enp0s8
ip link set enp0s8 up
# Pata hacia K8s (Cable directo)
ip addr flush dev enp0s9
ip addr add 192.168.99.1/30 dev enp0s9
ip link set enp0s9 up
# Rutas
ip route add 10.0.1.0/24 via 172.16.2.1
```

### In the VM Router
```bash
sudo nano /root/arranque_red.sh
```

```bash
#!/bin/bash
# 1. Habilitar modo Router
sysctl -w net.ipv4.ip_forward=1
# 2. Configurar WAN (Internet) 
ip link set enp0s3 up
dhclient enp0s3
echo "nameserver 8.8.8.8" > /etc/resolv.conf
# Configurar Redes Internas
ip link set enp0s8 up
ip addr flush dev enp0s8
ip addr add 172.16.1.1/30 dev enp0s8
ip link set enp0s9 up
ip addr flush dev enp0s9
ip addr add 172.16.2.1/30 dev enp0s9
# Configurar NAT (Salida a Internet)
iptables -t nat -F
iptables -t nat -A POSTROUTING -o enp0s3 -j MASQUERADE
# Rutas hacia Mininet
ip route add 10.0.1.0/24 via 172.16.1.2
ip route add 10.0.2.0/24 via 172.16.2.2
sudo ip route add 192.168.0.0/16 via 192.168.99.1
```

### In each VM
```bash
sudo chmod +x /root/arranque_red.sh
```

```bash
sudo crontab -e
## add at the end:
@reboot /root/arranque_red.sh
```