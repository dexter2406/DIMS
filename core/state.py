# core/state.py

"""
Manages the node's runtime state, including its role (leader/follower),
network information, and the in-memory inventory data. This module acts as
the single source of truth for the node's current condition.
"""

import threading
import logging
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger(__name__)

# Node roles
ROLE_LEADER = "LEADER"
ROLE_FOLLOWER = "FOLLOWER"

class NodeState:
    """
    A thread-safe class to manage the state of a node.
    This includes both the application data (inventory) and the node's
    role and position in the distributed system.
    """
    def __init__(self, node_id: int):
        self._lock = threading.Lock()

        # --- Node's identity and role ---
        self.node_id: int = node_id
        self.role: str = ROLE_FOLLOWER  # Nodes start as followers by default
        self.leader_id: Optional[int] = None

        # --- In-memory inventory ---
        # A simple key-value store for inventory items
        self.inventory: Dict[str, Any] = {}

        # --- Network state for the ring ---
        self.successor_addr: Optional[Tuple[str, int]] = None

    def set_role(self, role: str):
        """Sets the role of the node (LEADER or FOLLOWER)."""
        with self._lock:
            if role not in [ROLE_LEADER, ROLE_FOLLOWER]:
                raise ValueError(f"Invalid role: {role}")
            self.role = role
            if role == ROLE_LEADER:
                self.leader_id = self.node_id
            logger.info("Node %s is now %s", self.node_id, self.role)

    def is_leader(self) -> bool:
        """Checks if the current node is the leader."""
        with self._lock:
            return self.role == ROLE_LEADER

    def update_inventory(self, item_id: str, quantity: int) -> bool:
        """
        Updates the quantity of an item in the inventory.
        This is the core "business logic" operation.
        """
        with self._lock:
            logger.info("Updating inventory: item '%s' to quantity %s", item_id, quantity)
            self.inventory[item_id] = quantity
            return True # In a real app, might have validation

    def get_inventory(self) -> Dict[str, Any]:
        """Returns a copy of the current inventory."""
        with self._lock:
            return self.inventory.copy()

    def set_successor(self, addr: Optional[Tuple[str, int]]):
        """Sets the network address of the successor node."""
        with self._lock:
            self.successor_addr = addr
            logger.info("Node %s's successor is set to %s", self.node_id, addr)
            
    def __repr__(self) -> str:
        """Provides a string representation of the node's state."""
        with self._lock:
            return (
                f"<NodeState id={self.node_id} role={self.role} "
                f"leader={self.leader_id} successor={self.successor_addr} "
                f"inventory_items={len(self.inventory)}>")

# Example Usage
if __name__ == "__main__":
    # Create a state object for a node with ID 5
    state = NodeState(node_id=5)
    logger.info(state)

    # Promote to leader
    state.set_role(ROLE_LEADER)
    logger.info("Is this node the leader? %s", state.is_leader())
    logger.info(state)

    # Perform some inventory updates
    state.update_inventory("item-A", 100)
    state.update_inventory("item-B", 250)
    
    # Set a successor
    state.set_successor(('localhost', 9001))
    logger.info(state)

    # Get the current inventory
    current_inventory = state.get_inventory()
    logger.info("Current Inventory: %s", current_inventory)

    assert state.is_leader()
    assert current_inventory["item-A"] == 100
    logger.info("NodeState basic operations are working correctly.")
