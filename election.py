"""
election.py - Module 2: Voting / Leader Election Module
Implements the Bully Algorithm using comparable integer Node IDs.
The elected Leader serves as the central Message Sequencer for Total Ordering.
"""

import threading
from config import ELECTION_TIMEOUT


class BullyElectionManager:
    def __init__(self, node):
        self.node = node
        self.leader_id = None
        self.election_in_progress = False
        self.received_ok = False
        self.lock = threading.Lock()

    def start_election(self):
        """Initiates a Bully Algorithm election."""
        with self.lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
            self.received_ok = False

        print(f"\n[Election] Starting Bully Election (My Node ID: {self.node.node_id})...")
        peers = self.node.discovery.get_peers_snapshot()

        # Find all active nodes in the Group View with a strictly higher ID
        higher_nodes = [
            (nid, info) for nid, info in peers.items() if nid > self.node.node_id
        ]

        if not higher_nodes:
            # No higher-ID nodes exist; immediately become Coordinator (Leader)
            self.declare_victory()
            return

        # Challenge all higher-ID nodes with an ELECTION message
        for nid, info in higher_nodes:
            self.node.discovery.send_unicast(
                info["ip"],
                info["port"],
                {
                    "type": "ELECTION",
                    "node_id": self.node.node_id,
                    "username": self.node.username,
                    "ip": self.node.discovery.local_ip,
                    "port": self.node.discovery.unicast_port,
                },
            )

        # Wait ELECTION_TIMEOUT seconds for any 'OK' response
        timer = threading.Timer(ELECTION_TIMEOUT, self._evaluate_election_timeout)
        timer.daemon = True
        timer.start()

    def _evaluate_election_timeout(self):
        """Called after ELECTION_TIMEOUT to check if any higher node bullied us."""
        with self.lock:
            got_ok = self.received_ok
            if got_ok:
                # A higher node responded with OK; wait for its COORDINATOR announcement
                self.election_in_progress = False
                return

        # No higher node responded in time -> we win the election!
        self.declare_victory()

    def declare_victory(self):
        """Declares this node as the new Coordinator (Leader & Sequencer)."""
        with self.lock:
            self.leader_id = self.node.node_id
            self.election_in_progress = False

        # Synchronize the Leader's sequence counter with the highest delivered sequence number
        self.node.multicast.sync_sequencer_on_leadership()

        print(
            f"\n[Election] *** I AM NOW THE ELECTED LEADER / SEQUENCER "
            f"(Node {self.node.node_id}) ***"
        )

        # Broadcast COORDINATOR announcement to all peers in the Group View
        peers = self.node.discovery.get_peers_snapshot()
        for nid, info in peers.items():
            if nid != self.node.node_id:
                self.node.discovery.send_unicast(
                    info["ip"],
                    info["port"],
                    {
                        "type": "COORDINATOR",
                        "leader_id": self.node.node_id,
                        "username": self.node.username,
                        "ip": self.node.discovery.local_ip,
                        "port": self.node.discovery.unicast_port,
                    },
                )

    def handle_election_message(self, msg: dict):
        """Handles an incoming ELECTION message from a lower-ID node."""
        sender_id = int(msg["node_id"])
        self.node.discovery.update_peer(
            sender_id, msg["ip"], msg["port"], msg.get("username", "Peer")
        )

        if self.node.node_id > sender_id:
            # Reply with OK to bully the lower-ID sender and start our own election
            self.node.discovery.send_unicast(
                msg["ip"],
                int(msg["port"]),
                {"type": "OK", "node_id": self.node.node_id},
            )
            self.start_election()

    def handle_ok_message(self, msg: dict):
        """Handles an incoming OK message from a higher-ID node."""
        with self.lock:
            self.received_ok = True

    def handle_coordinator_message(self, msg: dict):
        """Handles an incoming COORDINATOR victory announcement."""
        new_leader = int(msg["leader_id"])
        self.node.discovery.update_peer(
            new_leader, msg["ip"], msg["port"], msg.get("username", "Peer")
        )

        if new_leader < self.node.node_id:
            # A lower-ID node tried to claim leadership; bully it!
            self.start_election()
            return

        with self.lock:
            self.leader_id = new_leader
            self.election_in_progress = False

        print(
            f"\n[Election] *** Node {new_leader} ({msg.get('username', 'Leader')}) "
            f"is now the Elected Leader (Sequencer) ***"
        )
