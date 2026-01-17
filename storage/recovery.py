# storage/recovery.py

"""
Handles the node recovery process on startup.
This involves reading the Write-Ahead Log (WAL) and replaying the logged
operations to restore the in-memory state (e.g., the inventory).
After recovery, the node can rejoin the cluster as a follower.
"""

# Add project root to path to allow absolute imports
import sys
import os
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from storage.wal import WriteAheadLog
from core.state import NodeState

logger = logging.getLogger(__name__)

class RecoveryManager:
    """
    Manages the state recovery from a Write-Ahead Log.
    """
    def __init__(self, node_state: NodeState, wal: WriteAheadLog):
        """
        Initializes the RecoveryManager.

        Args:
            node_state (NodeState): The node's state object to be restored.
            wal (WriteAheadLog): The WAL object to read from.
        """
        self.node_state = node_state
        self.wal = wal

    def recover_state(self):
        """
        Reads the WAL and replays all logged operations to restore the
        node's in-memory state.
        """
        logger.info("Starting state recovery for Node %s...", self.node_state.node_id)
        
        recovered_ops = 0
        for record in self.wal.read_all():
            try:
                # This logic must be kept in sync with how operations are logged
                op_type = record.get("op")
                if op_type in {"IN", "SHIP"}:
                    item_id = record.get("item_id")
                    quantity = record.get("quantity")
                    if item_id is not None and quantity is not None:
                        ok, _new_qty, err = self.node_state.apply_inventory_op(
                            item_id,
                            op_type,
                            quantity,
                            apply=True,
                        )
                        if ok:
                            recovered_ops += 1
                        else:
                            logger.warning(
                                "Skipping invalid record during recovery: %s (%s)",
                                record,
                                err,
                            )
                else:
                    logger.warning("Skipping unknown record type during recovery: %s", op_type)
            except Exception as e:
                logger.error("Error processing record %s: %s", record, e)

        logger.info("Recovery complete. Total operations replayed: %s", recovered_ops)
        logger.info("Final recovered inventory: %s", self.node_state.get_inventory())

# Example Usage
if __name__ == '__main__':
    # This example demonstrates the recovery process. It requires the
    # other modules (NodeState, WriteAheadLog) to be available.
    
    # 1. Setup a temporary environment
    demo_wal_path = './data/recovery_demo.log'
    if os.path.exists(demo_wal_path):
        os.remove(demo_wal_path)

    # 2. Create a WAL and log some operations
    wal = WriteAheadLog(wal_path=demo_wal_path)
    wal.append({"op": "IN", "item_id": "item:R", "quantity": 99})
    wal.append({"op": "IN", "item_id": "item:S", "quantity": 199})
    wal.append({"op": "UNKNOWN", "data": "some other event"}) # Should be skipped
    wal.append({"op": "IN", "item_id": "item:R", "quantity": 6}) # Update existing
    wal.close()

    # 3. Simulate a node restart by creating a fresh state object
    fresh_node_state = NodeState(node_id=101)
    logger.info("State before recovery: %s", fresh_node_state.get_inventory())
    
    # 4. Perform the recovery
    recovery_wal = WriteAheadLog(wal_path=demo_wal_path)
    recovery_manager = RecoveryManager(node_state=fresh_node_state, wal=recovery_wal)
    recovery_manager.recover_state()
    
    # 5. Verify the state was restored
    final_inventory = fresh_node_state.get_inventory()
    logger.info("State after recovery: %s", final_inventory)
    
    assert final_inventory.get("item:R") == 105
    assert final_inventory.get("item:S") == 199
    
    # Clean up
    recovery_wal.close()
    os.remove(demo_wal_path)
    logger.info("RecoveryManager functionality is working correctly.")
