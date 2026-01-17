# core/replication.py

"""
Handles the replication of state changes from the leader to followers.
- The leader multicasts updates to all followers.
- Followers receive updates and apply them to their local state.
"""

# Add project root to path to allow absolute imports
import sys
import os
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.state import NodeState
from network.ring import TCPRingClient
from common.protocol import create_message, parse_message, MSG_REPLICATION
from storage.wal import WriteAheadLog

logger = logging.getLogger(__name__)

class ReplicationManager:
    """
    Manages the replication process. On the leader, it sends updates.
    On followers, it processes incoming replication messages.
    """
    def __init__(self,
                 node_state: NodeState,
                 ring_client: TCPRingClient,
                 wal: WriteAheadLog = None):
        """
        Initializes the ReplicationManager.

        Args:
            node_state (NodeState): The state of the current node.
            ring_client (TCPRingClient): The client for sending messages to the successor.
            wal (WriteAheadLog, optional): The WAL for followers to log replicated changes. Defaults to None.
        """
        self.node_state = node_state
        self.ring_client = ring_client
        self.wal = wal

    def replicate_update(self, update_data: dict):
        """
        (Leader-side) Creates a replication message and sends it to the successor.
        This should be called after the leader has persisted the change to its own WAL
        and updated its in-memory state.
        """
        if not self.node_state.is_leader():
            logger.warning("A non-leader node attempted to replicate an update.")
            return

        logger.info(f"Leader replicating update: {update_data}")
        replication_msg = create_message(MSG_REPLICATION, payload=update_data)
        
        try:
            self.ring_client.send_message(replication_msg)
            logger.info("Replication message sent to successor.")
        except Exception as e:
            logger.error(f"Error sending replication message: {e}")

    def handle_replication_message(self, msg: dict):
        """
        (Follower-side) Processes a replication message received from the predecessor.
        It applies the change to the local state and may write to its own WAL.
        """
        if self.node_state.is_leader():
            # Leaders should not accept replication messages in this simple model,
            # as it could indicate a split-brain or misconfiguration.
            # We'll just ignore it.
            return

        payload = msg.get("payload")
        if not payload:
            logger.warning("Received replication message with no payload.")
            return
            
        logger.info(f"Follower received replication message: {payload}")

        # The core logic of applying the state change
        # This must match the operation performed on the leader
        item_id = payload.get("item_id")
        op = payload.get("op")
        quantity = payload.get("quantity")

        if item_id is None or op is None or quantity is None:
            logger.warning("Received replication message with missing fields: %s", payload)
            return

        ok, _new_qty, err = self.node_state.apply_inventory_op(item_id, op, quantity, apply=False)
        if not ok:
            logger.error("Replication message failed validation: %s", err)
            return

        # Optionally, follower also writes to its WAL for durability
        if self.wal:
            wal_record = {"op": str(op).upper(), "item_id": item_id, "quantity": quantity}
            self.wal.append(wal_record)

        # Apply to in-memory state
        ok, _new_qty, err = self.node_state.apply_inventory_op(item_id, op, quantity, apply=True)
        if not ok:
            logger.error("Failed to apply replicated update: %s", err)
            return

        # The message needs to be propagated around the ring
        # The follower re-sends the exact same message to its successor.
        try:
            # Re-serialize the original message to forward it
            # Note: A more optimized implementation would forward the raw bytes directly
            original_message_bytes = create_message(MSG_REPLICATION, payload)
            self.ring_client.send_message(original_message_bytes)
            logger.info("Follower forwarded replication message to its successor.")
        except Exception as e:
            logger.error(f"Follower failed to forward replication message: {e}")

# Example Usage
if __name__ == '__main__':
    # This is difficult to test in isolation.
    # The integration with the TCP ring components is essential.
    print("ReplicationManager logic is defined.")
    print("Integration testing with the main application loop is required to verify correctness.")
    
    # A conceptual test:
    # 1. Mock a Leader NodeState and a TCPRingClient.
    # 2. Create a ReplicationManager for the leader.
    # 3. Call `replicate_update` and verify the ring client's `send_message` is called.
    # 4. Mock a Follower NodeState, a TCPRingClient, and a WAL.
    # 5. Create a ReplicationManager for the follower.
    # 6. Call `handle_replication_message` and verify the state and WAL are updated,
    #    and that the follower's ring client also calls `send_message`.
    
    print("\nConceptual test plan created.")
