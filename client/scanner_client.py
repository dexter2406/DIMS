# client/scanner_client.py

"""
Scanner client utilities for discovering the leader and sending updates.
Behavioral simulations live in a separate script for easier customization.
"""

import requests
import json
import logging
from typing import Optional, Tuple

# Add project root to path to allow absolute imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from network.udp_discovery import discover_leader
from server.config import AppConfig
from common.logging_utils import configure_logging, build_log_path

logger = logging.getLogger(__name__)

class ScannerClient:
    """
    A client that simulates a barcode scanner updating the inventory.
    """
    def __init__(self, config: AppConfig):
        self.config = config
        self.leader_http_addr: Optional[str] = None

    def find_leader(self):
        """
        Discovers the leader's HTTP address using UDP broadcast.
        Returns True on success, False on failure.
        """
        logger.info("Attempting to discover the leader...")
        result = discover_leader(self.config)
        if result:
            _leader_id, http_addr = result
            self.leader_http_addr = f"http://{http_addr}"
            logger.info("Leader found at %s", self.leader_http_addr)
            return True
        else:
            logger.warning("Could not find a leader. Will retry later.")
            self.leader_http_addr = None
            return False

    def send_update(self, item_id: str, op: str, quantity: int) -> Tuple[bool, int]:
        """
        Sends a single inventory update to the discovered leader.

        Args:
            item_id (str): The ID of the item to update.
            op (str): Inventory operation (IN or SHIP).
            quantity (int): The quantity delta for the item.

        Returns:
            Tuple[bool, int]: (accepted, status_code). Status code 0 means no response.
        """
        if not self.leader_http_addr:
            logger.warning("Cannot send update: No leader is known.")
            return False, 0

        url = f"{self.leader_http_addr}/update"
        payload = {"item_id": item_id, "op": op, "quantity": quantity}
        headers = {'Content-Type': 'application/json'}

        try:
            logger.info("Sending update to %s: %s", url, payload)
            response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=5)

            if response.status_code == 202: # Accepted
                logger.info("Update accepted by leader.")
                return True, response.status_code
            # Handle cases where the target node is not the leader
            elif response.status_code == 503:
                logger.warning("Update rejected: Node is not the leader. Will rediscover.")
                self.leader_http_addr = None # Force rediscovery
                return False, response.status_code
            else:
                logger.warning(
                    "Failed to send update. Status: %s, Body: %s",
                    response.status_code,
                    response.text,
                )
                return False, response.status_code

        except requests.exceptions.RequestException as e:
            logger.error("Error sending update: %s", e)
            self.leader_http_addr = None # Assume leader is down, force rediscovery
            return False, 0


if __name__ == '__main__':
    log_file = build_log_path("client")
    configure_logging(log_file, level=logging.INFO, to_console=True)
    logger.info("ScannerClient provides leader discovery and update helpers.")
    logger.info("Use `python -m client.scanner_simulator -h` for simulation runs.")
