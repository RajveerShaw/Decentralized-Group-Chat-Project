# Decentralized Fault-Tolerant Group Chat System

University of Stuttgart — Distributed Systems (WS 2026/2027) Student Project

## 1. Project Overview
This project implements a symmetric **Peer-to-Peer (P2P) Decentralized Group Chat System** in Python using only standard library sockets (`socket`, `threading`, `json`, `time`). Every node runs identical software and acts as both client and server.

### How Each Course Requirement is Met:
1. **Architectural Model (`main.py`, `config.py`)**: Symmetric P2P architecture with a dynamically elected Coordinator (Sequencer). Uses UDP Broadcast (Port `50000`) for discovery and UDP Unicast (dynamic port) for peer-to-peer communication, backed by daemon threads for concurrent listening and failure detection.
2. **Dynamic Discovery (`network_discovery.py`)**: Joining nodes broadcast a `DISCOVER` packet on UDP port `50000`. Active peers add the newcomer to their Group View and reply via unicast (`DISCOVER_REPLY`) with their identity, current Leader ID, and latest sequence number.
3. **Leader Election / Voting (`election.py`)**: Implements the **Bully Algorithm** using comparable integer Node IDs. The node with the highest active ID is elected as Coordinator (`COORDINATOR`) and acts as the central Message Sequencer.
4. **Fault Tolerance (`fault_tolerance.py`)**: Peers exchange unicast `HEARTBEAT` packets every 2 seconds. If a node misses 3 heartbeats (6-second timeout), it is removed from the Group View. If the Leader crashes, surviving peers immediately trigger a Bully election and the new Leader resumes sequencing from the highest known sequence number.
5. **Reliable Ordered Multicast (`multicast.py`)**: Implements **Sequencer-Based Total Order Multicast** with a **Hold-Back Queue** and **Negative Acknowledgements (NACKs)**. Senders unicast `CHAT_REQ` to the Leader, which increments `seq_counter` and multicasts `CHAT_ORDERED(seq)`. Peers deliver strictly in order (`seq == last_delivered_seq + 1`), buffer out-of-order packets in a Hold-Back Queue, and send `NACK` requests to recover missing messages.

---

## 2. How to Run the Project

### Option A: Local Testing on One Laptop (3 Terminal Windows)
Open 3 terminal windows in this folder and run:
```bash
# Terminal 1 (Node ID 100)
python3 main.py Alice 100

# Terminal 2 (Node ID 200)
python3 main.py Bob 200

# Terminal 3 (Node ID 300 - Highest ID, becomes Leader)
python3 main.py Charlie 300
```
*(Note: Passing an explicit integer ID like `100`, `200`, `300` as the second argument is optional, but very helpful during the live demo to clearly show the Bully Algorithm!)*

### Option B: Multi-Laptop Live Demo Setup (2+ Physical Machines)
Because `eduroam` blocks UDP broadcast traffic, connect all demo laptops to a **smartphone Wi-Fi hotspot** or personal router.
- On **Laptop 1**: `python3 main.py Rajveer 100`
- On **Laptop 2**: `python3 main.py Teammate 200`


