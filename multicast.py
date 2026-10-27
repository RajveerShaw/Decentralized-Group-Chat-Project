"""
multicast.py - Module 4: Reliable Ordered Multicast Module
Implements Sequencer-Based Total Order Multicast with a Hold-Back Queue
and Negative Acknowledgements (NACKs) for packet loss recovery.
"""

import threading


class ReliableMulticastManager:
    def __init__(self, node):
        self.node = node
        self.lock = threading.Lock()

        # Sequencer counter (used only when this node is the Elected Leader)
        self.seq_counter = 0

        # Total order delivery state (tracked by every node)
        self.last_delivered_seq = 0
        self.hold_back_queue = {}   # {seq_num: msg_dict} for out-of-order messages
        self.message_history = {}   # {seq_num: msg_dict} cache for NACK retransmissions

        # Demo feature: allows simulating a dropped UDP packet to prove Hold-Back Queue & NACKs
        self.simulate_drop_next = False

    def sync_sequencer_on_leadership(self):
        """
        Ensures a newly elected Leader continues assigning sequence numbers seamlessly
        from the highest known sequence number in the chat room.
        """
        with self.lock:
            highest_cached = max(self.message_history.keys(), default=0)
            self.seq_counter = max(
                self.seq_counter, self.last_delivered_seq, highest_cached
            )

    def send_chat_request(self, text: str):
        """
        Called when the local user types a message. Unicasts CHAT_REQ to the Leader
        (Sequencer) so it can assign a global sequence number.
        """
        leader_id = self.node.election.leader_id
        if leader_id is None:
            print("[Multicast] ! No Leader elected yet. Hold on while election completes...")
            self.node.election.start_election()
            return

        peers = self.node.discovery.get_peers_snapshot()
        if leader_id not in peers:
            print("[Multicast] ! Leader not found in Group View. Starting election...")
            self.node.election.start_election()
            return

        leader_info = peers[leader_id]
        self.node.discovery.send_unicast(
            leader_info["ip"],
            leader_info["port"],
            {
                "type": "CHAT_REQ",
                "sender_id": self.node.node_id,
                "sender": self.node.username,
                "text": text,
            },
        )

    def handle_chat_request_as_leader(self, msg: dict):
        """
        Executed ONLY by the Elected Leader (Sequencer).
        Increments the global sequence counter, caches the message, and multicasts
        CHAT_ORDERED to every peer in the Group View.
        """
        if self.node.election.leader_id != self.node.node_id:
            return

        with self.lock:
            self.seq_counter += 1
            seq = self.seq_counter
            ordered_packet = {
                "type": "CHAT_ORDERED",
                "seq": seq,
                "sender_id": msg.get("sender_id"),
                "sender": msg["sender"],
                "text": msg["text"],
            }
            self.message_history[seq] = ordered_packet

        peers = self.node.discovery.get_peers_snapshot()
        for nid, info in peers.items():
            self.node.discovery.send_unicast(info["ip"], info["port"], ordered_packet)

    def handle_ordered_message(self, msg: dict):
        """
        Processes an incoming CHAT_ORDERED packet.
        - Delivers immediately if seq == last_delivered_seq + 1
        - Buffers in Hold-Back Queue and sends NACK if seq > last_delivered_seq + 1
        """
        seq = int(msg["seq"])

        with self.lock:
            # Demo helper: optionally drop one incoming packet to demonstrate NACK recovery
            if self.simulate_drop_next and not msg.get("retransmitted", False):
                self.simulate_drop_next = False
                print(
                    f"\n[Demo Simulation] ! Artificially dropped incoming packet "
                    f"seq={seq} ('{msg['text']}') to demonstrate Hold-Back Queue!"
                )
                return

            # Cache message for peer/leader retransmission support
            self.message_history[seq] = msg

            # Case 1: Duplicate or already delivered message
            if seq <= self.last_delivered_seq:
                return

            # Case 2: Exact next expected sequence number -> Deliver immediately!
            if seq == self.last_delivered_seq + 1:
                self._deliver_to_user(msg)
                self.last_delivered_seq = seq

                # Drain any consecutive messages waiting in the Hold-Back Queue
                while (self.last_delivered_seq + 1) in self.hold_back_queue:
                    next_seq = self.last_delivered_seq + 1
                    buffered_msg = self.hold_back_queue.pop(next_seq)
                    print(
                        f"[Hold-Back Queue] -> Unblocking buffered message seq={next_seq}"
                    )
                    self._deliver_to_user(buffered_msg)
                    self.last_delivered_seq = next_seq

            # Case 3: Sequence gap detected (seq > last_delivered_seq + 1)
            elif seq > self.last_delivered_seq + 1:
                self.hold_back_queue[seq] = msg
                missing_list = list(range(self.last_delivered_seq + 1, seq))
                print(
                    f"\n[Multicast] ! Sequence gap detected! Received seq={seq}, "
                    f"expected {self.last_delivered_seq + 1}. "
                    f"Buffered seq={seq} in Hold-Back Queue. "
                    f"Sending NACK for missing seq: {missing_list}"
                )
                self._send_nacks(missing_list)

    def _deliver_to_user(self, msg: dict):
        """Prints the delivered message in strict global sequence order."""
        tag = " (recovered via NACK)" if msg.get("retransmitted") else ""
        print(f"\n[Chat #{msg['seq']}] {msg['sender']}: {msg['text']}{tag}")

    def _send_nacks(self, missing_seqs: list):
        """Sends unicast NACK requests to the Leader (and peers) for missing sequence numbers."""
        peers = self.node.discovery.get_peers_snapshot()
        leader_id = self.node.election.leader_id

        # Prefer asking the Leader; if unavailable, ask all peers
        targets = (
            [peers[leader_id]]
            if leader_id in peers
            else [info for nid, info in peers.items() if nid != self.node.node_id]
        )

        for missing_seq in missing_seqs:
            nack_packet = {
                "type": "NACK",
                "missing_seq": missing_seq,
                "requester_ip": self.node.discovery.local_ip,
                "requester_port": self.node.discovery.unicast_port,
            }
            for target in targets:
                self.node.discovery.send_unicast(
                    target["ip"], target["port"], nack_packet
                )

    def handle_nack(self, msg: dict):
        """
        Responds to a peer's NACK by retransmitting the requested message from
        our message_history cache.
        """
        missing_seq = int(msg["missing_seq"])
        with self.lock:
            cached_msg = self.message_history.get(missing_seq)

        if cached_msg:
            retransmit_copy = dict(cached_msg)
            retransmit_copy["retransmitted"] = True
            print(
                f"\n[Multicast] Retransmitting cached message seq={missing_seq} "
                f"to {msg['requester_ip']}:{msg['requester_port']} in response to NACK."
            )
            self.node.discovery.send_unicast(
                msg["requester_ip"], int(msg["requester_port"]), retransmit_copy
            )
