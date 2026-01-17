# server/main.py

"""
The main entry point for a DIMS node.
This script initializes all components of a node and starts the required services.
"""
import signal
import time
import sys
import os
import logging

# Ensure the project root is in the Python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from server.config import config
from core.state import NodeState, ROLE_LEADER, ROLE_FOLLOWER
from storage.wal import WriteAheadLog
from storage.recovery import RecoveryManager
from network.udp_discovery import DiscoveryListener
from network.ring import TCPRingServer, TCPRingClient
from core.replication import ReplicationManager
from core.election import ElectionManager
from api.http_server import APIServer
from common.protocol import parse_message, MSG_REPLICATION, MSG_ELECTION, MSG_HEARTBEAT, MSG_COORDINATOR
from common.logging_utils import configure_logging, build_log_path

log = logging.getLogger(__name__)


def _peek_node_id_from_args(default_id: int) -> int:
    for index, arg in enumerate(sys.argv):
        if arg.startswith("--node-id="):
            value = arg.split("=", 1)[1]
            try:
                return int(value)
            except ValueError:
                return default_id
        if arg == "--node-id" and index + 1 < len(sys.argv):
            try:
                return int(sys.argv[index + 1])
            except ValueError:
                return default_id
    return default_id

def _valid_item_id(item_id: str) -> bool:
    if not isinstance(item_id, str):
        return False
    return bool(item_id.strip())

class Node:
    """
    Represents a single node in the DIMS cluster, encapsulating all its components.
    """
    def __init__(self, config):
        self.config = config
        self.running = True
        
        # 1. Initialize core state and storage
        log.info(f"Initializing Node {config.node_id}...")
        self.state = NodeState(config.node_id)
        self.wal = WriteAheadLog(config.wal_path)

        # 2. Perform recovery from WAL
        self.recovery_manager = RecoveryManager(self.state, self.wal)
        self.recovery_manager.recover_state()
        # After recovery, every node starts as a follower
        self.state.set_role(ROLE_FOLLOWER)
        
        # 3. Initialize networking components
        # TCP client needs the initial successor address from config
        if config.successor_addr:
            host, port = config.successor_addr.split(':')
            self.state.set_successor((host, int(port)))
        
        self.ring_client = TCPRingClient(self.state, config)
        self.replication_manager = ReplicationManager(self.state, self.ring_client, self.wal)
        self.election_manager = ElectionManager(self.state, self.ring_client)
        
        # Wire the failure callback to trigger an election if the successor connection drops
        self.ring_client.on_failure = self.election_manager.start_election
        
        # The TCP server receives messages and dispatches them via handle_tcp_message
        self.ring_server = TCPRingServer(config, self.handle_tcp_message)
        
        # The UDP server responds to discovery requests
        self.discovery_listener = DiscoveryListener(self.state, config)
        
        # The HTTP server handles client API calls
        self.http_server = APIServer(config, self.state, self.handle_http_update)

        # List of all running components for easy lifecycle management
        self.components = [
            self.ring_server,
            self.ring_client,
            self.discovery_listener,
            self.http_server
        ]

    def start(self):
        """Starts all components of the node."""
        log.info(f"Starting Node {self.config.node_id}...")
        for component in self.components:
            component.start()
            
        # If no successor is defined, try to discover existing nodes before electing self
        if not self.state.successor_addr:
            log.info("No successor defined, attempting to discover ring...")
            self.ring_client._repair_ring()
            
            if not self.state.successor_addr:
                log.info("Still no successor found, starting an election to become leader.")
                self.election_manager.start_election()

    def stop(self):
        """Stops all components gracefully."""
        log.info(f"Stopping Node {self.config.node_id}...")
        self.running = False
        for component in reversed(self.components): # Stop in reverse order
            if hasattr(component, 'stop'):
                component.stop()
        
        # Wait for threads to finish
        for component in reversed(self.components):
            if component.is_alive():
                component.join(timeout=2)
                
        self.wal.close()
        log.info("Node stopped.")
    
    # --- Callback Wiring ---

    def handle_tcp_message(self, data: bytes):
        """
        Main dispatcher for all incoming TCP messages from the predecessor.
        This is the central point for wiring callbacks.
        """
        try:
            msg = parse_message(data)
            msg_type = msg.get("type")

            if msg_type == MSG_REPLICATION:
                self.replication_manager.handle_replication_message(msg)
            elif msg_type == MSG_ELECTION:
                self.election_manager.handle_election_message(msg)
            elif msg_type == MSG_COORDINATOR:
                self.election_manager.handle_coordinator_message(msg)
            elif msg_type == MSG_HEARTBEAT:
                # In a more robust system, you'd update a 'last_seen' timestamp
                # for the predecessor and use it to detect failures.
                log.debug(f"Received heartbeat from predecessor: {msg.get('payload')}")
            else:
                log.warning(f"Received unknown message type: {msg_type}")
        except Exception as e:
            log.error(f"Error handling TCP message: {e}")

    def handle_http_update(self, update_payload: dict):
        """
        Callback for the HTTP server when a valid update is received.
        This function orchestrates the leader's responsibilities:
        1. Log to WAL.
        2. Update in-memory state.
        3. Replicate to followers.
        """
        if not self.state.is_leader():
            log.warning("Non-leader received an update request. This should not happen.")
            return False, 503, {"error": "Service Unavailable"}
            
        try:
            # Reconstruct the log record to be stored
            item_id = update_payload.get("item_id")
            op = update_payload.get("op")
            quantity = update_payload.get("quantity")
            if not _valid_item_id(item_id):
                return False, 400, {"error": "Invalid item_id. Expected non-empty string."}

            if not isinstance(op, str):
                return False, 400, {"error": "Invalid op. Expected IN or SHIP."}

            op = op.upper()
            if op not in {"IN", "SHIP"}:
                return False, 400, {"error": "Invalid op. Expected IN or SHIP."}

            if not isinstance(quantity, int) or quantity <= 0:
                return False, 400, {"error": "Invalid quantity. Expected positive integer."}

            ok, new_qty, err = self.state.apply_inventory_op(
                item_id,
                op,
                quantity,
                apply=False,
            )
            if not ok:
                status_code = 409 if err == "insufficient_inventory" else 400
                return False, status_code, {"error": "Insufficient inventory."}

            wal_record = {"op": op, "item_id": item_id, "quantity": quantity}
            
            # 1. Log to WAL for durability
            self.wal.append(wal_record)
            
            # 2. Apply to in-memory state
            ok, new_qty, err = self.state.apply_inventory_op(item_id, op, quantity, apply=True)
            if not ok:
                log.error("Failed to apply inventory op after WAL append: %s", err)
                return False, 500, {"error": "Internal error applying inventory update."}
            
            # 3. Trigger replication to followers
            replication_payload = {
                "item_id": item_id,
                "op": op,
                "quantity": quantity,
            }
            self.replication_manager.replicate_update(replication_payload)
            
            return True, 202, {"status": "Accepted", "item_id": item_id, "quantity": new_qty}
        except Exception as e:
            log.error(f"Error processing HTTP update: {e}")
            return False, 500, {"error": "Internal Server Error"}

def main():
    """
    Main function to run a DIMS node.
    """
    node_id_for_log = _peek_node_id_from_args(config.node_id)
    log_file = build_log_path("server", node_id=node_id_for_log)
    configure_logging(log_file, level=logging.INFO, to_console=True)

    # Load configuration from args and environment
    config.load_from_args()
    log.info("Configuration loaded for Node %s", config.node_id)
    
    node = Node(config)
    
    def signal_handler(sig, frame):
        log.info("Caught signal, shutting down gracefully...")
        node.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    node.start()
    
    # Keep the main thread alive while services run in the background
    while node.running:
        try:
            time.sleep(1)
        except InterruptedException:
            node.stop()
            break

if __name__ == '__main__':
    main()
    
