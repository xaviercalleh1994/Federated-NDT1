import numpy as np
import matplotlib.pyplot as plt
import random


def simulate_protocol(name, payload, overhead, handshake_rtts, reliable, loss_rate, rtt_ms=100, trials=500):
    rtt = rtt_ms / 1000.0
    rto = 2 * rtt  # Retransmission Timeout estimate

    delays = []
    total_sent_bytes = []

    for _ in range(trials):
        # Initial Handshake delay (0 for UDP/BP, 1.5 for TCP, 1.0 for QUIC)
        current_delay = handshake_rtts * rtt
        attempts = 0
        success = False

        while not success:
            attempts += 1
            if random.random() > loss_rate:
                success = True
                current_delay += (rtt / 2.0)  # One-way data trip
            else:
                if not reliable:
                    current_delay += (rtt / 2.0)
                    break
                else:
                    # Reliable protocols wait for RTO before re-trying
                    current_delay += rto

        delays.append(current_delay * 1000)  # Convert to ms

        # Calculate bytes based on attempts
        if reliable:
            total_sent_bytes.append(attempts * (payload + overhead))
        else:
            total_sent_bytes.append(payload + overhead)

    avg_delay = np.mean(delays)

    # Goodput Calculation
    if not reliable:
        success_rate = (1 - loss_rate)
        # Goodput is successful payload / average bytes spent
        avg_goodput = (success_rate * payload / np.mean(total_sent_bytes)) * 100
    else:
        avg_goodput = (payload / np.mean(total_sent_bytes)) * 100

    return avg_delay, avg_goodput


# Configuration
payload_size = 216  # bytes
loss_range = np.linspace(0, 0.5, 20)  # 0% to 50% loss
base_rtt = 100  # ms

# Protocol Models (Payload=216)
# QUIC overhead is high here because it pads initial packets to 1200 bytes
protocols_config = {
    "UDP": {"overhead": 8, "handshake": 0, "reliable": False, "color": 'blue'},
    "TCP": {"overhead": 20, "handshake": 1.5, "reliable": True, "color": 'red'},
    "QUIC": {"overhead": 984, "handshake": 1.0, "reliable": True, "color": 'green'},
    "BPv7": {"overhead": 150, "handshake": 0, "reliable": True, "color": 'orange'}
}

results = {name: {"delay": [], "goodput": []} for name in protocols_config}

for lr in loss_range:
    for name, config in protocols_config.items():
        d, g = simulate_protocol(name, payload_size, config['overhead'],
                                 config['handshake'], config['reliable'], lr, base_rtt)
        results[name]["delay"].append(d)
        results[name]["goodput"].append(g)

# --- Plotting Results ---
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 8), sharex=True)

for name, config in protocols_config.items():
    ax1.plot(loss_range * 100, results[name]["delay"], label=name,
             color=config['color'], marker='o', markersize=4)
    ax2.plot(loss_range * 100, results[name]["goodput"], label=name,
             color=config['color'], marker='s', markersize=4)

# Formatting Latency Plot
ax1.set_ylabel("Avg. Latency (ms)")
ax1.set_title(f"Impact of Packet Loss on {payload_size}-byte Transmission")
ax1.grid(True, linestyle='--', alpha=0.6)
ax1.legend()

# Formatting Goodput Plot
ax2.set_ylabel("Effective Goodput (%)")
ax2.set_xlabel("Packet Loss Rate (%)")
ax2.grid(True, linestyle='--', alpha=0.6)
ax2.set_ylim(0, 100)

plt.tight_layout()
plt.show()