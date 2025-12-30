# core/election.py

"""
Implements the leader election algorithm. This is a ring-based algorithm
where the node with the highest ID is elected as the leader.
An election is triggered by events like startup or connection failure.
"""

# Add project root to path to allow absolute imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.state import NodeState, ROLE_LEADER, ROLE_FOLLOWER
from network.ring import TCPRingClient
from common.protocol import create_message, MSG_ELECTION

class ElectionManager:
    """
    Manages the leader election process for a node.
    """
    def __init__(self, node_state: NodeState, ring_client: TCPRingClient):
        self.node_state = node_state
        self.ring_client = ring_client
        self.is_participating = False # Flag to prevent multiple elections at once

    def start_election(self):
        """
        Starts a new election. The node sends an election message with its
        own ID to its successor.
        """
        if not self.ring_client._is_connected():
            print("Cannot start election: Not connected to a successor.")
            # If a node is isolated, it can declare itself leader.
            print("Declaring self as leader due to isolation.")
            self.node_state.set_role(ROLE_LEADER)
            self.node_state.leader_id = self.node_state.node_id
            self.is_participating = False
            return

        print(f"Node {self.node_state.node_id} is starting an election.")
        self.is_participating = True
        election_payload = {"candidate_id": self.node_state.node_id}
        election_msg = create_message(MSG_ELECTION, payload=election_payload)
        
        try:
            self.ring_client.send_message(election_msg)
        except Exception as e:
            print(f"Failed to send initial election message: {e}")
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

        if candidate_id is None:
            print("Received invalid election message.")
            return

        print(f"Node {self.node_state.node_id} received election message with candidate {candidate_id}.")

        # Case 1: The incoming candidate ID is this node's own ID.
        # This means the message has circulated the entire ring and this node is the winner.
        if candidate_id == self.node_state.node_id:
            print(f"Node {self.node_state.node_id} has won the election!")
            self.node_state.set_role(ROLE_LEADER)
            self.node_state.leader_id = self.node_state.node_id
            self.is_participating = False
            # Optionally, send a "COORDINATOR" message to announce the result.
            return

        # Case 2: The incoming candidate ID is greater than this node's ID.
        # Forward the message unchanged.
        if candidate_id > self.node_state.node_id:
            print(f"Forwarding election message for higher candidate {candidate_id}.")
            self.is_participating = True # This node is now part of the ongoing election
            updated_msg = create_message(MSG_ELECTION, payload={"candidate_id": candidate_id})
        
        # Case 3: The incoming candidate ID is less than this node's ID.
        # And this node is not already participating in an election with its own ID.
        # Substitute this node's ID and forward.
        elif candidate_id < self.node_state.node_id and not self.is_participating:
            print(f"Replacing candidate {candidate_id} with own ID {self.node_state.node_id}.")
            self.is_participating = True
            updated_msg = create_message(MSG_ELECTION, payload={"candidate_id": self.node_state.node_id})
        
        else:
            # This can happen if candidate_id < self.node_state.node_id but this node
            # is already participating (i.e., has already sent its own ID).
            # In this case, the message from the lower-ID node is discarded.
            print(f"Discarding election message from lower candidate {candidate_id} as I am already participating.")
            return

        try:
            self.ring_client.send_message(updated_msg)
        except Exception as e:
            print(f"Failed to forward election message: {e}")
            # The successor link is probably broken. A new election will likely
            # be triggered by the connection failure logic.
            self.is_participating = False

# Example Usage
if __name__ == '__main__':
    # Election logic is highly dependent on the ring state and is best tested
    # through integration.
    print("ElectionManager logic is defined.")
    print("Correctness depends on a functioning TCP ring and message passing.")
    
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
    
    print("\nConceptual test plan created.")
