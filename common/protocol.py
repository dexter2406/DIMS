# common/protocol.py

"""
Defines the internal message protocol for communication between nodes.
This includes message types, JSON structures, and field conventions.
All modules that send or receive messages should adhere to this protocol.
"""

import json
import logging
from typing import Dict, Any, Literal

logger = logging.getLogger(__name__)

# Message types
MSG_HEARTBEAT = "HEARTBEAT"
MSG_ELECTION = "ELECTION"  # Payload: {"candidate_id": int, "term": int, "term_origin": int}
MSG_COORDINATOR = "COORDINATOR"  # Payload: {"leader_id": int, "term": int, "term_origin": int}
MSG_REPLICATION = "REPLICATION"
MSG_UPDATE_ACK = "UPDATE_ACK" # Optional, for more reliable replication

# UDP Discovery messages
MSG_DISCOVERY_REQUEST = "DISCOVERY_REQUEST"
MSG_DISCOVERY_RESPONSE = "DISCOVERY_RESPONSE"
MSG_NODE_QUERY = "NODE_QUERY"
MSG_NODE_PRESENCE = "NODE_PRESENCE"


def create_message(msg_type: str, payload: Dict[str, Any] = None) -> bytes:
    """
    Creates a message and serializes it to JSON.

    Args:
        msg_type (str): The type of the message (e.g., 'HEARTBEAT').
        payload (Dict[str, Any], optional): The message payload. Defaults to None.

    Returns:
        bytes: The JSON-serialized message as bytes.
    """
    message = {
        "type": msg_type,
        "payload": payload or {},
    }
    return json.dumps(message).encode('utf-8')


def parse_message(data: bytes) -> Dict[str, Any]:
    """
    Parses a JSON-serialized message.

    Args:
        data (bytes): The message data.

    Returns:
        Dict[str, Any]: The parsed message as a dictionary.
    """
    return json.loads(data.decode('utf-8'))

# Example Usage (for demonstration)
if __name__ == "__main__":
    # Example: Creating a replication message
    update_payload = {"item_id": "item-123", "quantity": 10}
    replication_msg = create_message(MSG_REPLICATION, payload=update_payload)
    logger.info("Serialized replication message: %s", replication_msg)

    # Example: Parsing a message
    parsed_msg = parse_message(replication_msg)
    logger.info("Parsed message: %s", parsed_msg)
    assert parsed_msg["type"] == MSG_REPLICATION
    assert parsed_msg["payload"]["item_id"] == "item-123"

    # Example: Election message
    election_msg = create_message(MSG_ELECTION, payload={"candidate_id": 99})
    logger.info("Serialized election message: %s", election_msg)
    parsed_election_msg = parse_message(election_msg)
    logger.info("Parsed election message: %s", parsed_election_msg)

    logger.info("Protocol definitions and helpers are ready.")
