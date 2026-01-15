# DIMS - Distributed Inventory Management System

A Python-based distributed inventory management system demonstrating leader election, ring repair, and best-effort replication using a custom TCP ring, UDP discovery, and HTTP API.

## Project Structure

The project is organized into the following modules:

- `api/`: External HTTP REST interface for clients.
- `client/`: A simulated scanner client.
- `common/`: Shared code, like the communication protocol.
- `core/`: Core logic for state management, replication, and leader election.
- `network/`: TCP ring and UDP discovery implementation.
- `server/`: Main application entry point, configuration, and logger.
- `storage/`: Write-Ahead Log (WAL) and recovery logic.
## Installation

To set up the development environment, follow these steps:

### Using uv (Recommended)

1.  **Install uv**: If you don't have `uv` installed, you can get it via `pipx`:
    ```bash
    pipx install uv
    ```
    Or, if you prefer `pip`:
    ```bash
    pip install uv
    ```

2.  **Create and activate a virtual environment**:
    ```bash
    uv venv
    source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`
    ```

3.  **Install dependencies**:
    ```bash
    uv pip install -r requirements.txt
    ```

### Using pip

1.  **Create and activate a virtual environment**:
    ```bash
    python -m venv .venv
    source .venv/bin/activate  # On Windows, use `.venv\Scripts\activate`
    ```

2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## How to Run

### 1. Start a Single Node (as Leader)

To start a single node, which will elect itself as the leader, run the following command from the project root directory:

```bash
python -m server.main --node-id 1 --http-port 8001 --tcp-port 9001
```

This node will listen for client requests on `http://localhost:8001` and for internal ring communication on port `9001`.

### 2. Start Another Node (Join the Ring)

By default, nodes use UDP broadcast to discover peers and repair the ring, so you can start additional nodes without specifying a successor. Open a new terminal and run:

```bash
python -m server.main --node-id 2 --http-port 8002 --tcp-port 9002
```

- `--node-id 2`: Gives this node a unique ID.
- `--successor "localhost:9001"` (optional): Pins the successor if you want a fixed ring link.

You can add more nodes the same way (unique node IDs and ports).

### 3. Start the Scanner Client

The client will automatically discover the leader using UDP broadcast and start sending updates. Open a third terminal:

```bash
python -m client.scanner_client
```

The client logs updates and automatically handles leader changes if a leader node fails and a new one is elected.

### Ports and Discovery

- HTTP: per-node `--http-port` (default 8000).
- TCP ring: per-node `--tcp-port` (default 9000).
- UDP discovery: `DIMS_UDP_PORT` (default 10000). Nodes and clients must be on the same subnet for broadcast discovery.

### Logging and WAL

- Runtime logs are written to `debug_log/`:
  - `server_<node_id>_<timestamp>.log`
  - `client_<timestamp>.log`
- WAL files are written to `data/wal_<node_id>.log`.

## How It Works

- **Leader Discovery**: Clients use UDP broadcast to find the leader's HTTP endpoint.
- **Node Discovery for Ring Repair**: Nodes use UDP broadcast to find peers when repairing or joining the ring.
- **Ring-based Communication**: Nodes are organized in a logical ring and communicate over TCP. Each node only knows about its direct successor.
- **Replication**: The leader writes updates to a Write-Ahead Log (WAL), applies them to its state, and propagates them around the ring. Followers receive the updates, apply them, and forward the replication message.
- **Leader Election**: If a node detects a failure (e.g., successor unreachable), a ring-based election is triggered. The node with the highest ID wins and announces the leader via a coordinator message.
- **Crash Recovery**: When a node restarts, it replays its WAL to recover its state before rejoining the ring as a follower.

## Progress & Milestones

### Current Status
- Completed: UDP leader/node discovery, TCP ring repair, coordinator-based election propagation, WAL recovery, file-based logging.
- In progress: scenario verification for crash -> repair -> election -> client rediscovery, replication continuity after repair.
- Planned: multi-node stress checks and broader failure simulations.

### Immediate Next Step (1-2 Days)
- **Scenario Verification**: Validate leader crash -> ring repair -> election convergence -> client rediscovery, and confirm replication resumes after repair.

### Weekly Milestones (Roadmap to Completion)

1.  **Milestone 1: Robust Networking & Discovery (Done)**
    - Implementation of TCP message framing, dynamic IP resolution for discovery, and basic fault detection wiring.
2.  **Milestone 2: Reliable Replication & State Consistency (In Progress)**
    - Implementation: Hardening the `ReplicationManager` to handle edge cases in message forwarding and ensuring followers correctly persist replicated updates to their local WAL.
3.  **Milestone 3: Resilient Ring Management & Election Stability (In Progress)**
    - Implementation: Finalizing the ring-based election logic to handle complex scenarios like concurrent elections or multiple node failures, ensuring the ring always closes correctly.
4.  **Milestone 4: Comprehensive Scenario-based Testing (Planned)**
    - Testing: Validating the entire system against specific failure scenarios, including Leader crashes, rapid node churn, network latency, and full cluster recovery from WAL.
