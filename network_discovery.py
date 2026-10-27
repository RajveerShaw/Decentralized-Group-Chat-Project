"""
network_discovery.py - Module 1: Network & Dynamic Discovery Layer
Handles UDP Broadcast (Port 50000) for host discovery, UDP Unicast socket
communication, and maintains the shared Group View of active peers.
"""

import socket
import json
import time
import threading
from config import BROADCAST_PORT, BROADCAST_IP, BUFFER_SIZE, get_local_ip


class NetworkDiscoveryManager:
    def __init__(self, node):
        self.node = node
        self.local_ip = get_local_ip()

        # 1. Create UDP Unicast Socket (OS assigns an available dynamic port)
        self.unicast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.unicast_sock.bind(("0.0.0.0", 0))
        self.unicast_port = self.unicast_sock.getsockname()[1]

        # 2. Create UDP Broadcast Socket (Port 50000) for Dynamic Discovery
        self.bcast_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.bcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            try:
                self.bcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            except OSError:
                pass
        self.bcast_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        self.bcast_sock.bind(("", BROADCAST_PORT))

        # 3. Group View: maps node_id (int) -> dict of peer metadata
        self.view_lock = threading.Lock()
        self.peers = {
            self.node.node_id: {
                "ip": self.local_ip,
                "port": self.unicast_port,
                "username": self.node.username,
                "last_seen": time.time(),
            }
        }

    def send_unicast(self, target_ip: str, target_port: int, payload: dict):
        """Serializes a dictionary as JSON and sends it via UDP Unicast."""
        try:
            data = json.dumps(payload).encode("utf-8")
            self.unicast_sock.sendto(data, (target_ip, target_port))
        except Exception as e:
            print(f"[!] Unicast send error to {target_ip}:{target_port}: {e}")

    def send_broadcast(self, payload: dict):
        """Serializes a dictionary as JSON and broadcasts it on Port 50000."""
        try:
            data = json.dumps(payload).encode("utf-8")
            self.bcast_sock.sendto(data, (BROADCAST_IP, BROADCAST_PORT))
        except Exception as e:
            # Fallback to local subnet broadcast if <broadcast> fails
            try:
                self.bcast_sock.sendto(data, ("255.255.255.255", BROADCAST_PORT))
            except Exception as ex:
                print(f"[!] Broadcast send error: {ex}")

    def broadcast_discovery(self):
        """Broadcasts a DISCOVER packet when the node starts or joins the network."""
        packet = {
            "type": "DISCOVER",
            "node_id": self.node.node_id,
            "username": self.node.username,
            "ip": self.local_ip,
            "port": self.unicast_port,
        }
        self.send_broadcast(packet)

    def update_peer(self, node_id: int, ip: str, port: int, username: str = "Peer") -> bool:
        """Adds or updates a peer in the Group View. Returns True if newly discovered."""
        node_id = int(node_id)
        with self.view_lock:
            is_new = node_id not in self.peers
            existing_name = self.peers.get(node_id, {}).get("username", username)
            self.peers[node_id] = {
                "ip": ip,
                "port": int(port),
                "username": username if username != "Peer" else existing_name,
                "last_seen": time.time(),
            }
            return is_new

    def remove_peer(self, node_id: int):
        """Removes a crashed peer from the Group View."""
        node_id = int(node_id)
        with self.view_lock:
            if node_id in self.peers and node_id != self.node.node_id:
                del self.peers[node_id]

    def get_peers_snapshot(self) -> dict:
        """Returns a thread-safe copy of the active Group View."""
        with self.view_lock:
            return dict(self.peers)

    def listen_broadcast_loop(self):
        """Daemon thread listening for UDP Broadcast DISCOVER packets on Port 50000."""
        while self.node.running:
            try:
                data, _ = self.bcast_sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode("utf-8"))

                if msg.get("type") == "DISCOVER":
                    sender_id = int(msg["node_id"])
                    if sender_id == self.node.node_id:
                        continue

                    is_new = self.update_peer(
                        sender_id, msg["ip"], msg["port"], msg.get("username", "Peer")
                    )
                    if is_new:
                        print(
                            f"\n[Discovery] + Node {sender_id} ({msg.get('username')}) "
                            f"joined from {msg['ip']}:{msg['port']}"
                        )

                    # Reply via UDP Unicast with our identity, current Leader, and sequence state
                    reply = {
                        "type": "DISCOVER_REPLY",
                        "node_id": self.node.node_id,
                        "username": self.node.username,
                        "ip": self.local_ip,
                        "port": self.unicast_port,
                        "leader_id": self.node.election.leader_id,
                        "last_seq": self.node.multicast.last_delivered_seq,
                    }
                    self.send_unicast(msg["ip"], int(msg["port"]), reply)

                    # If the newly joined node has a higher ID than the current leader, trigger election
                    if (
                        self.node.election.leader_id is not None
                        and sender_id > self.node.election.leader_id
                    ):
                        self.node.election.start_election()

            except Exception as e:
                if self.node.running:
                    print(f"[!] Broadcast listener error: {e}")

    def handle_discover_reply(self, msg: dict):
        """Processes a unicast DISCOVER_REPLY from an existing peer."""
        sender_id = int(msg["node_id"])
        is_new = self.update_peer(
            sender_id, msg["ip"], msg["port"], msg.get("username", "Peer")
        )
        if is_new:
            print(
                f"[Discovery] + Discovered existing peer: Node {sender_id} "
                f"({msg.get('username')}) at {msg['ip']}:{msg['port']}"
            )

        # Synchronize current Leader if valid and higher than our own ID
        remote_leader = msg.get("leader_id")
        if remote_leader is not None:
            remote_leader = int(remote_leader)
            if remote_leader >= self.node.node_id:
                self.node.election.leader_id = remote_leader

        # Synchronize latest sequence number for newly joining node
        remote_seq = int(msg.get("last_seq", 0))
        if remote_seq > self.node.multicast.last_delivered_seq:
            self.node.multicast.last_delivered_seq = remote_seq
