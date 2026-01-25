The following content is the **Frozen FINAL Implementation Brief**: subsequent implementation and discussion are only allowed within this scope; to change the scope, you must explicitly update this brief first.
Original proposal: Proposal_Gruppe3_v3.pdf

---

## 1) Final Architecture (modules and responsibility boundaries)

Overall system: **Scanner Clients -> (HTTP REST) -> Leader Server -> (internal TCP ring) -> Followers**; **UDP broadcast is used for dynamic discovery of the Leader HTTP endpoint and node discovery during ring repair**.

### A. Directories/Modules

**common/**

* `protocol.py`: **internal message types, JSON format, field conventions** (shared "language" across modules).
* `logging_utils.py`: logging configuration and log file path helpers.

**server/**

* `main.py`: **system entry point**; initialize Node; start TCP/UDP/HTTP threads/loops; unified lifecycle management.
* `config.py`: configuration loading (ports, Node ID, etc).

**api/**

* `http_server.py`: external HTTP JSON API (Scanner submits update POST; clients can query status/inventory).

  * If this node is Leader: receive update -> append WAL -> update in-memory state -> trigger replication
  * If this node is Follower: reject/redirect/return 503 + instruct client to re-run UDP discover (choose one, but be consistent)

**storage/**

* `wal.py`: **WAL (append-only JSON log)**: append on each update; basis for crash recovery.
* `recovery.py`: read WAL on startup and replay to restore in-memory state; recovered node joins ring as follower.

**core/**

* `state.py`: in-memory KV inventory + node role/runtime status (leader/follower, node_id, successor, etc).
  * Inventory updates apply IN/SHIP deltas via `apply_inventory_op`.
* `replication.py`: **Passive Replication**: Leader propagates updates to followers via internal TCP ring; followers apply updates to state (and may optionally write WAL).
* `election.py`: **Ring-based election (highest Node ID)**; triggered by TCP disconnect/heartbeat timeout.

**network/**

* `ring.py`: internal TCP neighbor long connection (maintains only successor); handles connection management, message send/receive, heartbeats, link failure detection, ring repair (skip failed nodes).
* `udp_discovery.py`: UDP broadcast: clients/servers use it to locate the current leader HTTP endpoint and perform node discovery for ring repair (no prior config).

**client/**

* `scanner_client.py`: client helper: UDP discover leader -> send HTTP POST updates; no built-in simulation loop.
* `scanner_simulator.py`: CLI simulation runner for deterministic IN/SHIP sequences.

---

## 2) Dependency and Collaboration Map

### 2.1 Hard dependencies (must define common "language/interfaces" first)

1. **common/protocol.py** (internal message fields and types)
   -> dependents: `network/ring.py`, `core/election.py`, `core/replication.py`, `core/state.py`

2. **storage/wal.py record schema** (JSON structure for each WAL entry)
   -> dependents: `api/http_server.py`, `core/replication.py` (whether followers also write logs depends on your implementation choice, but schema must be fixed), `storage/recovery.py`

3. **Node runtime state model (state.py)**
   -> dependents: `http_server.py` (leader/follower check), `election.py` (write leader result), `replication.py` (apply update), `ring.py` (neighbor/heartbeat state)

### 2.2 Can be parallelized (after "interface freeze")

* `udp_discovery.py` can be implemented independently (as long as request/response fields are clear: leader_ip, leader_http_port, leader_id for leader discovery; node_id, tcp_addr for node discovery).
* `scanner_client.py` can be implemented independently (as long as UDP discover response structure + HTTP endpoint path/JSON body are clear).
* `logger.py`, `config.py` can be implemented independently.

### 2.3 Runtime main path (end-to-end)

Scanner POST update -> `http_server.py` (leader) -> `wal.py` append -> `state.py` apply -> `replication.py` emit replication msg -> `ring.py` send to successor -> follower `ring.py` recv -> `replication/state.py` apply (+ optional `wal.py`)

---

## 3) Final Technical Constraints

From Proposal v3 (must be satisfied):

1. **Language: Python**
2. **External interface: HTTP REST** (Scanner Clients -> System)
3. **Internal coordination: custom socket middleware**

   * **TCP**: persistent connections, only between ring neighbors; used for election/replication/heartbeats
   * **UDP broadcast/multicast**: used for dynamic discovery of leader HTTP endpoint and peer discovery for ring repair
4. **Topology: dynamic logical ring**; each server **only maintains successor** (no global membership list).
5. **Fault tolerance: tolerant to crashes** (detect failures via timeout/connection failure; recovery node replays WAL and rejoins).

Additional frozen constraints (from the conversation):
6) **No Docker** (this project does not use containerization for delivery/scoring; run directly with python).
7) **Consistency requirement downgraded from eventual consistency to best-effort propagation** (see next section "Assumptions/Guarantees").

---

## 4) Final Assumptions and Guarantees

### 4.1 What we "guarantee" (Scope Guarantees)

* **Crash tolerance (basic level)**:

  * Detect suspected crash via TCP disconnect or heartbeat timeout, and trigger ring repair + election.
  * On restart, a node can read local WAL to recover to the state it previously persisted, then rejoin as a follower.
* **Leader discovery is available**: clients/servers can find the current leader HTTP endpoint via UDP broadcast (assumes broadcast within the same subnet).

### 4.2 What we explicitly do NOT guarantee

* **No eventual consistency guarantee**:

  * Update propagation is best-effort: the leader attempts to forward via the TCP ring, but does not ensure every follower receives or catches up on historical gaps.
* **No end-to-end zero data loss**:

  * Leader crash, network jitter, or TCP disconnects can cause incomplete propagation; the system continues running and this is not considered an error.
* **No exactly-once / dedup**:

  * Client retries or connection jitter can cause duplicate updates; unless you explicitly add `update_id` idempotency in the protocol, it is not guaranteed.
* **No partition healing / conflict resolution**:

  * The design has no strong consistency or conflict resolution protocol; no multi-leader writes or merges.

---

## 5) Explicit Non-goals (out of scope)

To avoid scope creep, the following are explicitly out of scope (requests during implementation should be rejected or deferred):

1. **No Docker / Kubernetes / Compose delivery**
2. **No strong consistency/consensus protocols** (Raft/Paxos/2PC, etc)
3. **No "replica catch-up/repair/reconciliation" mechanisms** (no anti-entropy / gossip repair / snapshot install)
4. **No read scaling and follower reads** (if external reads/writes are implemented, default to leader-only to avoid consistency debate)
5. **No security/auth/encryption** (TLS, tokens, ACLs, etc)
6. **No complex ops** (monitoring, metrics, auto deployment, failure injection frameworks, etc)
7. **No complex client ecosystem** (keep only the simulated scanner_client)
