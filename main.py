"""
main.py - Main Entry Point & CLI User Interface Layer
Ties together Dynamic Discovery, Bully Leader Election, Heartbeat Fault Tolerance,
and Sequencer-Based Reliable Ordered Multicast into a symmetric Peer Node.
"""

import sys
import time
import json
import random
import threading
from config import BUFFER_SIZE
from network_discovery import NetworkDiscoveryManager
from election import BullyElectionManager
from fault_tolerance import FaultToleranceManager
from multicast import ReliableMulticastManager


class ChatNode:
    def __init__(self, username: str, custom_id: int = None):
        self.username = username
        self.running = True

        # Generate a unique comparable integer ID (millisecond timestamp suffix + random)
        if custom_id is not None:
            self.node_id = int(custom_id)
        else:
            self.node_id = (int(time.time() * 1000) % 1000000) + random.randint(10, 999)

        # Initialize the 4 core Distributed Systems modules
        self.discovery = NetworkDiscoveryManager(self)
        self.election = BullyElectionManager(self)
        self.fault_tolerance = FaultToleranceManager(self)
        self.multicast = ReliableMulticastManager(self)

        print("=" * 72)
        print(f" Decentralized Fault-Tolerant Group Chat Node Started")
        print(f" Username : {self.username}")
        print(f" Node ID  : {self.node_id} (Used for Bully Leader Election)")
        print(f" Address  : {self.discovery.local_ip}:{self.discovery.unicast_port}")
        print(" Commands : Type any message to chat, or use /peers, /leader,")
        print("            /sim_drop, /queue, /history, /elect, /help, /quit")
        print("=" * 72)

    def listen_unicast_loop(self):
        """
        Daemon thread that receives all UDP Unicast protocol packets and dispatches
        them to the appropriate module.
        """
        while self.running:
            try:
                data, _ = self.discovery.unicast_sock.recvfrom(BUFFER_SIZE)
                msg = json.loads(data.decode("utf-8"))
                mtype = msg.get("type")

                if mtype == "DISCOVER_REPLY":
                    self.discovery.handle_discover_reply(msg)
                elif mtype == "HEARTBEAT":
                    self.fault_tolerance.handle_heartbeat(msg)
                elif mtype == "ELECTION":
                    self.election.handle_election_message(msg)
                elif mtype == "OK":
                    self.election.handle_ok_message(msg)
                elif mtype == "COORDINATOR":
                    self.election.handle_coordinator_message(msg)
                elif mtype == "CHAT_REQ":
                    self.multicast.handle_chat_request_as_leader(msg)
                elif mtype == "CHAT_ORDERED":
                    self.multicast.handle_ordered_message(msg)
                elif mtype == "NACK":
                    self.multicast.handle_nack(msg)

            except Exception as e:
                if self.running:
                    print(f"[!] Unicast listener error: {e}")

    def start(self):
        """Starts background daemon threads, runs discovery, and enters the CLI loop."""
        threading.Thread(
            target=self.discovery.listen_broadcast_loop, daemon=True
        ).start()
        threading.Thread(target=self.listen_unicast_loop, daemon=True).start()
        threading.Thread(
            target=self.fault_tolerance.heartbeat_loop, daemon=True
        ).start()

        # Broadcast DISCOVER packet to find active peers on the local network
        self.discovery.broadcast_discovery()
        time.sleep(1.2)

        # If no Leader was discovered or our ID is higher, initiate Bully Election
        if (
            self.election.leader_id is None
            or self.node_id > self.election.leader_id
        ):
            self.election.start_election()

        self.run_cli()

    def run_cli(self):
        """Main thread interactive command-line interface."""
        while self.running:
            try:
                user_input = input().strip()
                if not user_input:
                    continue

                if user_input.startswith("/"):
                    self.handle_slash_command(user_input)
                else:
                    self.multicast.send_chat_request(user_input)

            except (KeyboardInterrupt, EOFError):
                print("\n[*] Shutting down node...")
                self.running = False
                break

    def handle_slash_command(self, cmd: str):
        """Built-in helper commands to inspect state during the live demo."""
        if cmd == "/peers":
            peers = self.discovery.get_peers_snapshot()
            print("\n--- Active Group View (Discovered Peers) ---")
            for nid, info in sorted(peers.items()):
                role = " [LEADER / SEQUENCER]" if nid == self.election.leader_id else ""
                self_tag = " (Me)" if nid == self.node_id else ""
                print(
                    f"  • Node {nid}: {info['username']} @ {info['ip']}:{info['port']}"
                    f"{role}{self_tag}"
                )
            print("--------------------------------------------")

        elif cmd == "/leader":
            lid = self.election.leader_id
            peers = self.discovery.get_peers_snapshot()
            lname = peers.get(lid, {}).get("username", "Unknown")
            print(f"\n[Status] Current Elected Leader: Node {lid} ({lname})")

        elif cmd == "/elect":
            self.election.start_election()

        elif cmd == "/sim_drop":
            self.multicast.simulate_drop_next = True
            print(
                "\n[Demo Simulation] Armed! The NEXT incoming ordered chat message "
                "will be artificially dropped so you can demonstrate the Hold-Back "
                "Queue and automatic NACK recovery when the subsequent message arrives."
            )

        elif cmd == "/queue":
            print(
                f"\n[Multicast State] Last Delivered Seq: {self.multicast.last_delivered_seq} "
                f"| Hold-Back Queue: {list(self.multicast.hold_back_queue.keys())}"
            )

        elif cmd == "/history":
            print("\n--- Delivered Message History Cache ---")
            for seq, m in sorted(self.multicast.message_history.items()):
                print(f"  [{seq}] {m['sender']}: {m['text']}")
            print("---------------------------------------")

        elif cmd == "/help":
            print(
                "\nAvailable Demo Commands:\n"
                "  /peers    - Show all active nodes in the Group View\n"
                "  /leader   - Show the current Elected Leader (Sequencer)\n"
                "  /sim_drop - Drop the next incoming chat packet to demo Hold-Back Queue & NACK\n"
                "  /queue    - Show last delivered seq and Hold-Back Queue contents\n"
                "  /history  - Show cached messages available for NACK retransmission\n"
                "  /elect    - Manually trigger a Bully Leader Election\n"
                "  /quit     - Exit the chat node"
            )

        elif cmd == "/quit":
            print("[*] Exiting chat node...")
            self.running = False
            sys.exit(0)
        else:
            print(f"[!] Unknown command '{cmd}'. Type /help for options.")


if __name__ == "__main__":
    username_arg = sys.argv[1] if len(sys.argv) > 1 else f"User-{random.randint(10, 99)}"
    custom_id_arg = int(sys.argv[2]) if len(sys.argv) > 2 else None
    node = ChatNode(username=username_arg, custom_id=custom_id_arg)
    node.start()
