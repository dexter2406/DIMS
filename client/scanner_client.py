# client/scanner_client.py

"""
A simulated scanner client that represents an edge device in the inventory system.
Its workflow is:
1. Use UDP broadcast to discover the current leader node.
2. Periodically send inventory updates to the leader via HTTP POST requests.
3. If an update fails, it will re-discover the leader and retry.
"""

import requests
import json
import time
import random
from typing import Optional

# Add project root to path to allow absolute imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from network.udp_discovery import discover_leader
from server.config import AppConfig

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
        print("Attempting to discover the leader...")
        result = discover_leader(self.config)
        if result:
            _leader_id, http_addr = result
            self.leader_http_addr = f"http://{http_addr}"
            print(f"Leader found at {self.leader_http_addr}")
            return True
        else:
            print("Could not find a leader. Will retry later.")
            self.leader_http_addr = None
            return False

    def send_update(self, item_id: str, quantity: int) -> bool:
        """
        Sends a single inventory update to the discovered leader.

        Args:
            item_id (str): The ID of the item to update.
            quantity (int): The new quantity for the item.

        Returns:
            bool: True if the update was successfully accepted, False otherwise.
        """
        if not self.leader_http_addr:
            print("Cannot send update: No leader is known.")
            return False

        url = f"{self.leader_http_addr}/update"
        payload = {"item_id": item_id, "quantity": quantity}
        headers = {'Content-Type': 'application/json'}

        try:
            print(f"Sending update to {url}: {payload}")
            response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=5)

            if response.status_code == 202: # Accepted
                print("Update accepted by leader.")
                return True
            # Handle cases where the target node is not the leader
            elif response.status_code == 503:
                print("Update rejected: Node is not the leader. Will rediscover.")
                self.leader_http_addr = None # Force rediscovery
                return False
            else:
                print(f"Failed to send update. Status: {response.status_code}, Body: {response.text}")
                return False

        except requests.exceptions.RequestException as e:
            print(f"Error sending update: {e}")
            self.leader_http_addr = None # Assume leader is down, force rediscovery
            return False

    def run_simulation(self, interval_seconds: int = 3):
        """
        Runs a continuous simulation of scanning items for a few iterations.

        Args:
            interval_seconds (int): The time to wait between sending updates.
        """
        item_counter = 0
        for _ in range(3): # Run for 3 iterations then exit
            # Step 1: Ensure we have a leader
            if not self.leader_http_addr:
                if not self.find_leader():
                    time.sleep(interval_seconds)
                    continue # Try again after a delay
            
            # Step 2: Generate and send a simulated update
            item_id = f"item-SKU-{1000 + item_counter % 10}" # Cycle through 10 items
            quantity = random.randint(1, 200)
            
            if not self.send_update(item_id, quantity):
                print("Retrying after failed update...")
                # The send_update method clears the leader address on failure,
                # so the next loop iteration will trigger rediscovery.
                time.sleep(1) # Shorter delay for immediate retry
                continue

            item_counter += 1
            print(f"--- Waiting for {interval_seconds} seconds before next scan ---")
            time.sleep(interval_seconds)


if __name__ == '__main__':
    # To run this client, you need a server node running.
    # The server should be started first.
    
    print("--- Starting DIMS Scanner Client Simulation ---")
    print("This client will first use UDP broadcast to find the leader.")
    print("Once found, it will send POST requests to the /update endpoint.")
    print("If the connection fails or the node is not the leader, it will rediscover.")
    print("Press Ctrl+C to stop.")
    
    client_config = AppConfig()
    
    # You can override the broadcast address if needed, e.g., for local testing
    # client_config.udp_broadcast_addr = 'localhost'
    
    client = ScannerClient(client_config)
    
    try:
        client.run_simulation(interval_seconds=5)
    except KeyboardInterrupt:
        print("\nScanner client simulation stopped.")
