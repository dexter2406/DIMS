## 1. Cases
> Cases are used to form complete demo scenarios
### Normal Case – Case 1 (4 Nodes Join the Ring)

- **Action:**
    
    Start Node 1 and Node 2 on Machine A; Node 3 and Node 4 on Machine B; then start the Clients on both machines.
    
- **Result:**
    - The first node starts in solo mode and becomes the Leader.
    - Other nodes discover the cluster via UDP and join the ring; each join checks node IDs and may trigger an election if the ID is higher.
    - Clients on both machines automatically discover the Leader’s HTTP address via UDP.
```python
$env:DIMS_UDP_BROADCAST_ADDR="172.20.10.15" # only setup for 2 machines
""" 
Run Server N
python -m server.main --node-id <N> --host <IP> --http-port 800<N>  --tcp-port 900<N>
- IP is checked by `ipconfig` in Windows Cmd
"""
python -m server.main --node-id 1 --http-port 8001 --tcp-port 9001 # --host 172.20.10.15 if not local
python -m server.main --node-id 2 --http-port 8002 --tcp-port 9002 # --host

""" 
Run Client (simulator)
python -m client.scanner_simulator --op IN --type <item_name> --start-no <item_no> --num 5
"""
python -m client.scanner_simulator --op IN --type sku --start-no 1000 --num 5

```

### Normal Case – Case 2 (Concurrent Clients + Replication)

- **Action:**
    
    Client on Machine A sends 5 IN requests; Client on Machine B sends 5 IN requests.
    
- **Result:**
    - The Leader processes all writes sequentially and persists them using WAL.
    - Updates are forwarded through the ring to followers; all nodes show the same `/inventory` state after propagation.


### Failure Case – Case 3 (Leader Crash)

- **Action:**
    
    Force-stop the Node process that is currently the Leader.
    
- **Result:**
    - Remaining nodes detect the failure, trigger an election, and elect a new Leader.
    - Clients keep sending requests; after failures, they rediscover the new Leader and continue updating.
```python
"""
Set Client to run a long time by `--num 1000`, testing re-discorvery of the new leader
"""
python -m client.scanner_simulator --op IN --type sku --start-no 1000 --num 1000
```

### Failure Case – Case 4 (Ring Break and Repair)

- **Action:**
    
    Force-stop a middle node on one machine (non-Leader).
    
- **Result:**
    - The upstream node detects the broken link and discovers a new successor via UDP.
    - During repair, the successor is rebuilt based on the currently reachable node set; the ring closes automatically and the system continues serving requests.


### Failure Case – Case 5 (Node Recovery + WAL Replay)

- **Action:**
    
    Restart the previously stopped node.
    
- **Result:**
    - The node restores its local state from WAL before the crash.
    - It rejoins the ring as a follower and continues receiving subsequent updates.


### Failure Case – Case 6 (Follower Restart)

- **Action:**
    
    Kill one follower; clients continue sending 2–3 updates; then restart the follower.
    
- **Expected Result:**
    - The system continues running (Leader remains active).
    - The restarted node replays WAL, rejoins the ring, and receives subsequent updates to stay in sync.

---

## 2. Scenarios

### Scenario Baseline: Normal Operation with Active Clients

**Covers:** Case 1 → Case 2

1. Nodes are started on two machines and form a cluster via UDP discovery
2. A Leader is elected according to the election algorithm
3. Clients on both machines automatically discover the Leader’s HTTP endpoint
4. Multiple clients send concurrent inventory updates
5. The Leader processes all writes sequentially, persists them using WAL, and forwards updates through the ring
6. All active nodes reflect the same inventory state after propagation


### Scenario 1: Leader Failure and Recovery

**Covers:** Baseline → Case 3

1. The system is running under normal client write load
2. The current Leader process is forcibly terminated
3. Remaining nodes detect the failure and trigger a new leader election
4. A new Leader is elected
5. Clients temporarily fail, rediscover the new Leader via UDP, and continue sending updates


### Scenario 2: Follower Failure, Repair, and Restart

**Covers:** Baseline → Case 4 → Case 5 → Case 6

1. The system is running under normal client write load
2. A non-leader (follower) node is forcibly terminated
3. Neighboring nodes detect the broken ring and repair it using the currently reachable node set
4. The system continues operating while the follower is down
5. The stopped follower is restarted
6. The node restores its local state from WAL and rejoins the ring as a follower
7. The restarted node receives and applies all subsequent updates