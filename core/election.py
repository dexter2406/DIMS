# core/election.py

"""
Implements a ring-based leader election using the LeLann-Chang-Roberts (LCR) algorithm.
The node swallow the smaller node id and pass on the higher id. The node with the highest ID wins. 

Notes on implementation details / adaptations:
- Elections are triggered on topology changes (e.g., disconnect/repair), not only startup.
- Messages carry a term (term + origin) to suppress stale/duplicate elections.
- `MSG_ELECTION` carries candidate_id and term; `MSG_COORDINATOR` announces the winner.
- `is_participating` prevents starting/forwarding multiple concurrent elections for this same node.
- Each node forwards higher candidates and discards lower candidates while participating.
"""

# Add project root to path to allow absolute imports
import sys
import os
import logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.state import NodeState, ROLE_LEADER, ROLE_FOLLOWER
from network.ring import TCPRingClient
from common.protocol import create_message, MSG_ELECTION, MSG_COORDINATOR

logger = logging.getLogger(__name__)

class ElectionManager:
    """
    Manages the leader election process for a node.
    """
    def __init__(self, node_state: NodeState, ring_client: TCPRingClient):
        """Initialize election state and dependencies."""
        self.node_state = node_state
        self.ring_client = ring_client
        self.is_participating = False # Flag to prevent multiple elections at once
        self.current_term = 0
        self.current_term_origin = 0

    def _term_tuple(self, term: int, origin: int) -> tuple[int, int]:
        """Build a comparable term tuple."""
        return (term, origin)

    def _is_stale_term(self, term: int, origin: int) -> bool:
        """Check whether a term is older than the current term."""
        return self._term_tuple(term, origin) < self._term_tuple(self.current_term, self.current_term_origin)

    def _maybe_update_term(self, term: int, origin: int):
        """Update the term if newer and reset participation."""
        if self._term_tuple(term, origin) > self._term_tuple(self.current_term, self.current_term_origin):
            self.current_term = term
            self.current_term_origin = origin
            self.is_participating = False

    def start_election(self):
        """
        Starts a new election. The node sends an election message with its
        own ID to its successor.
        """
        self.current_term += 1
        self.current_term_origin = self.node_state.node_id
        if not self.ring_client._is_connected():
            logger.warning("Cannot start election: Not connected to a successor.")
            # If a node is isolated, it can declare itself leader.
            logger.info("Declaring self as leader due to isolation.")
            self.node_state.set_role(ROLE_LEADER)
            self.node_state.leader_id = self.node_state.node_id
            self.is_participating = False
            return

        logger.info("Node %s is starting an election.", self.node_state.node_id)
        self.is_participating = True
        election_payload = {
            "candidate_id": self.node_state.node_id,
            "term": self.current_term,
            "term_origin": self.current_term_origin,
        }
        election_msg = create_message(MSG_ELECTION, payload=election_payload)
        
        try:
            self.ring_client.send_message(election_msg)
        except Exception as e:
            logger.error("Failed to send initial election message: %s", e)
            self.is_participating = False
            # Maybe trigger again after a delay
    
    def handle_election_message(self, msg: dict):
        """
        Processes an incoming election message.
        - If the message's candidate ID is higher, forward the message.
        - If the message's candidate ID is lower, substitute this node's ID and forward.
        - If the message's candidate ID is this node's own ID, this node has won.
        """
        payload = msg.get("payload", {})
        candidate_id = payload.get("candidate_id")
        term = payload.get("term", 0)
        term_origin = payload.get("term_origin", 0)

        if candidate_id is None:
            logger.warning("Received invalid election message.")
            return

        if self._is_stale_term(term, term_origin):
            logger.info("Ignoring stale election message for term %s:%s.", term, term_origin)
            return

        self._maybe_update_term(term, term_origin)

        logger.info(
            "Node %s received election message with candidate %s.",
            self.node_state.node_id,
            candidate_id,
        )

        # Case 1: The incoming candidate ID is this node's own ID.
        # This means the message has circulated the entire ring and this node is the winner.
        if candidate_id == self.node_state.node_id:
            logger.info("Node %s has won the election!", self.node_state.node_id)
            self.node_state.set_role(ROLE_LEADER)
            self.node_state.leader_id = self.node_state.node_id
            self.is_participating = False
            
            # Announce the winner to the rest of the ring
            logger.info("Node %s is announcing victory.", self.node_state.node_id)
            updated_msg = create_message(
                MSG_COORDINATOR,
                payload={
                    "leader_id": self.node_state.node_id,
                    "term": self.current_term,
                    "term_origin": self.current_term_origin,
                },
            )

        # Case 2: The incoming candidate ID is greater than this node's ID.
        # Forward the message unchanged.
        elif candidate_id > self.node_state.node_id:
            logger.info("Forwarding election message for higher candidate %s.", candidate_id)
            self.is_participating = True # This node is now part of the ongoing election
            updated_msg = create_message(
                MSG_ELECTION,
                payload={
                    "candidate_id": candidate_id,
                    "term": self.current_term,
                    "term_origin": self.current_term_origin,
                },
            )
        
        # Case 3: The incoming candidate ID is less than this node's ID.
        # And this node is not already participating in an election with its own ID.
        # Substitute this node's ID and forward.
        elif candidate_id < self.node_state.node_id and not self.is_participating:
            logger.info(
                "Replacing candidate %s with own ID %s.",
                candidate_id,
                self.node_state.node_id,
            )
            self.is_participating = True
            updated_msg = create_message(
                MSG_ELECTION,
                payload={
                    "candidate_id": self.node_state.node_id,
                    "term": self.current_term,
                    "term_origin": self.current_term_origin,
                },
            )
        
        else:
            # This can happen if candidate_id < self.node_state.node_id but this node
            # is already participating (i.e., has already sent its own ID).
            # In this case, the message from the lower-ID node is discarded.
            logger.info(
                "Discarding election message from lower candidate %s as I am already participating.",
                candidate_id,
            )
            return

        try:
            self.ring_client.send_message(updated_msg)
        except Exception as e:
            logger.error("Failed to forward election message: %s", e)
            # The successor link is probably broken. A new election will likely
            # be triggered by the connection failure logic.
            self.is_participating = False
            return

    def handle_coordinator_message(self, msg: dict):
        """
        Processes the announcement of a new leader.
        """
        payload = msg.get("payload", {})
        leader_id = payload.get("leader_id")
        term = payload.get("term", 0)
        term_origin = payload.get("term_origin", 0)

        if self._is_stale_term(term, term_origin):
            logger.info("Ignoring stale coordinator for term %s:%s.", term, term_origin)
            return

        self._maybe_update_term(term, term_origin)

        if leader_id == self.node_state.node_id:
            # Message has traveled full circle
            logger.info("Coordinator message returned to leader. Election cycle complete.")
            return

        if leader_id == self.node_state.leader_id and self.current_term == term:
            logger.info("Ignoring duplicate coordinator for term %s:%s.", term, term_origin)
            return

        logger.info("Node %s updating leader to %s.", self.node_state.node_id, leader_id)
        self.node_state.leader_id = leader_id
        self.node_state.set_role(ROLE_FOLLOWER)
        self.is_participating = False

        # Forward the announcement
        try:
            self.ring_client.send_message(create_message(MSG_COORDINATOR, payload))
        except Exception as e:
            logger.error("Failed to forward coordinator message: %s", e)

# Example Usage
if __name__ == '__main__':
    # Election logic is highly dependent on the ring state and is best tested
    # through integration.
    # A conceptual test flow:
    # - Node A, B, C with IDs 10, 20, 30.
    # - Ring is A -> B -> C -> A.
    # - Node A's successor connection fails. It triggers an election.
    # - A sends ELECTION(10) to B (assuming it reconnects).
    # - B receives ELECTION(10). Since 20 > 10, B sends ELECTION(20) to C.
    # - C receives ELECTION(20). Since 30 > 20, C sends ELECTION(30) to A.
    # - A receives ELECTION(30). Since 30 > 10, A forwards ELECTION(30) to B.
    # - B receives ELECTION(30). Since 30 > 20, B forwards ELECTION(30) to C.
    # - C receives ELECTION(30). It's its own ID. C declares itself leader.
    pass
