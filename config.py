"""
config.py - Shared Configuration & Protocol Constants
Decentralized Fault-Tolerant Group Chat System
"""

import socket

# Network Discovery Constants
BROADCAST_PORT = 50000
BROADCAST_IP = "<broadcast>"
BUFFER_SIZE = 4096

# Timing Constants (in seconds)
HEARTBEAT_INTERVAL = 2.0   # Send heartbeat every 2 seconds
HEARTBEAT_TIMEOUT = 6.0    # 3 missed heartbeats (6s) = Node considered crashed
ELECTION_TIMEOUT = 2.0     # Wait time for 'OK' responses in Bully Algorithm


def get_local_ip() -> str:
    """
    Determines the machine's active LAN IP address so peers across different
    physical machines can communicate via UDP unicast.
    """
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # Does not actually send packets; used to inspect routing interface
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    except Exception:
        ip = "127.0.0.1"
    finally:
        s.close()
    return ip
