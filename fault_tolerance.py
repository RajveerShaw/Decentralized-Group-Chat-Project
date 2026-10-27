"""
fault_tolerance.py - Module 3: Fault Tolerance & Heartbeat Module
Periodically sends UDP unicast HEARTBEAT packets to all peers in the Group View,
detects crashed regular peers or a crashed Leader, and triggers recovery.
"""

import time
from config import HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT


class FaultToleranceManager:
    def __init__(self, node):
        self.node = node

    def heartbeat_loop(self):
        """
        Daemon thread that sends periodic heartbeats every 2 seconds and checks
        if any peer has exceeded the 6-second timeout (3 missed heartbeats).
        """
        while self.node.running:
            time.sleep(HEARTBEAT_INTERVAL)
            now = time.time()

            # Refresh our own timestamp in the Group View
            self.node.discovery.update_peer(
                self.node.node_id,
                self.node.discovery.local_ip,
                self.node.discovery.unicast_port,
                self.node.username,
            )

            peers = self.node.discovery.get_peers_snapshot()

            # 1. Send UDP Unicast HEARTBEAT to all other active peers
            for nid, info in peers.items():
                if nid != self.node.node_id:
                    self.node.discovery.send_unicast(
                        info["ip"],
                        info["port"],
                        {
                            "type": "HEARTBEAT",
                            "node_id": self.node.node_id,
                            "username": self.node.username,
                            "ip": self.node.discovery.local_ip,
                            "port": self.node.discovery.unicast_port,
                        },
                    )

            # 2. Check for crashed nodes (missed 3 consecutive heartbeats = 6.0s)
            leader_crashed = False
            for nid, info in peers.items():
                if nid != self.node.node_id and (now - info["last_seen"]) > HEARTBEAT_TIMEOUT:
                    print(
                        f"\n[Fault Tolerance] - Node {nid} ({info.get('username')}) "
                        f"timed out ({HEARTBEAT_TIMEOUT}s). Removing from Group View."
                    )
                    self.node.discovery.remove_peer(nid)
                    if nid == self.node.election.leader_id:
                        leader_crashed = True

            # 3. If the Leader crashed, immediately trigger a Bully Election
            if leader_crashed:
                print(
                    "[Fault Tolerance] ! Elected Leader crashed! "
                    "Triggering Bully Leader Election..."
                )
                self.node.election.leader_id = None
                self.node.election.start_election()

    def handle_heartbeat(self, msg: dict):
        """Updates the last_seen timestamp when a HEARTBEAT arrives from a peer."""
        sender_id = int(msg["node_id"])
        is_new = self.node.discovery.update_peer(
            sender_id, msg["ip"], msg["port"], msg.get("username", "Peer")
        )
        if is_new:
            print(
                f"\n[Discovery] + Added Node {sender_id} ({msg.get('username')}) "
                f"via Heartbeat from {msg['ip']}:{msg['port']}"
            )
