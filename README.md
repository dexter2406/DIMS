# DIMS - Distributed Inventory Management System

A Python-based distributed inventory management system demonstrating concepts like leader election, replication, and fault tolerance using a custom TCP ring and HTTP API.

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

### 2. Start a Follower Node

To add a second node to the cluster as a follower, you need to tell it the address of its successor in the ring. Open a new terminal and run:

```bash
python -m server.main --node-id 2 --http-port 8002 --tcp-port 9002 --successor "localhost:9001"
```

- `--node-id 2`: Gives this node a unique ID.
- `--successor "localhost:9001"`: Tells this node to connect to the first node (which is listening on port `9001`).

You can add more followers by creating a chain (e.g., a third node could connect to the second node).

### 3. Start the Scanner Client

The client will automatically discover the leader using UDP broadcast and start sending updates. Open a third terminal:

```bash
python -m client.scanner_client
```

The client will print the updates it sends and will automatically handle leader changes if a leader node fails and a new one is elected.

## How It Works

- **Leader Discovery**: Clients use UDP broadcast to find the leader's HTTP endpoint.
- **Ring-based Communication**: Nodes are organized in a logical ring and communicate over TCP. Each node only knows about its direct successor.
- **Replication**: The leader writes updates to a Write-Ahead Log (WAL), applies them to its state, and propagates them around the ring. Followers receive the updates and apply them to their own state.
- **Leader Election**: If a node detects a failure (e.g., its successor is unreachable), a ring-based election is triggered. The node with the highest ID wins and becomes the new leader.
- **Crash Recovery**: When a node restarts, it replays its WAL to recover its state before rejoining the ring as a follower.

## Progress & Milestones

### Immediate Next Step (1-2 Days)
- **Fault Tolerance Verification**: Manually simulate a node crash (e.g., by killing a follower process) and verify that the `TCPRingClient` correctly detects the failure and triggers the `ElectionManager` to initiate a ring repair.

### Weekly Milestones (Roadmap to Completion)

1.  **Milestone 1: Robust Networking & Discovery (Current)**
    - Implementation of TCP message framing, dynamic IP resolution for discovery, and basic fault detection wiring.
2.  **Milestone 2: Reliable Replication & State Consistency**
    - Implementation: Hardening the `ReplicationManager` to handle edge cases in message forwarding and ensuring followers correctly persist replicated updates to their local WAL.
3.  **Milestone 3: Resilient Ring Management & Election Stability**
    - Implementation: Finalizing the ring-based election logic to handle complex scenarios like concurrent elections or multiple node failures, ensuring the ring always closes correctly.
4.  **Milestone 4: Comprehensive Scenario-based Testing**
    - Testing: Validating the entire system against specific failure scenarios, including Leader crashes, rapid node churn, network latency, and full cluster recovery from WAL.
