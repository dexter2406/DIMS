# network/udp_discovery.py

"""
Implements the leader discovery mechanism using UDP broadcast.
- Servers listen for discovery requests and respond if they are the leader.
- Clients broadcast discovery requests to find the leader's HTTP endpoint.
This avoids hardcoding the leader's address in the clients.
"""

import socket
import threading
import time
from typing import Optional, Tuple

# Add project root to path to allow absolute imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.protocol import create_message, parse_message, MSG_DISCOVERY_REQUEST, MSG_DISCOVERY_RESPONSE
from core.state import NodeState

class DiscoveryListener(threading.Thread):
    """
    A server-side thread that listens for UDP broadcast requests and responds
    if the current node is the leader.
    """
    def __init__(self, node_state: NodeState, config):
        super().__init__(daemon=True)
        self.node_state = node_state
        self.config = config
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.running = False

    def run(self):
        self.sock.bind(('', self.config.udp_discovery_port))
        print(f"UDP Discovery Listener started on port {self.config.udp_discovery_port}")
        self.running = True
        
        while self.running:
            try:
                data, addr = self.sock.recvfrom(1024)
                msg = parse_message(data)
                
                if msg.get("type") == MSG_DISCOVERY_REQUEST and self.node_state.is_leader():
                    print(f"Received discovery request from {addr}, I am the leader.")
                    # If listening on 0.0.0.0, respond with a loopback address for local clients.
                    # A more advanced implementation could resolve the actual interface IP.
                    response_host = self.config.http_host
                    if response_host == '0.0.0.0':
                        response_host = '127.0.0.1'
                    
                    response_payload = {
                        "leader_id": self.node_state.leader_id,
                        "http_addr": f"{response_host}:{self.config.http_port}"
                    }
                    response_msg = create_message(MSG_DISCOVERY_RESPONSE, response_payload)
                    self.sock.sendto(response_msg, addr)
            except Exception as e:
                if self.running:
                    print(f"Error in DiscoveryListener: {e}")

    def stop(self):
        self.running = False
        self.sock.close()
        print("UDP Discovery Listener stopped.")

def discover_leader(config) -> Optional[Tuple[int, str]]:
    """
    Client-side function to find the leader by sending a UDP broadcast.

    Returns:
        A tuple containing (leader_id, http_address) or None if no leader is found.
    """
    print("Broadcasting to discover the leader...")
    
    client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    client_sock.settimeout(3.0) # Wait up to 3 seconds for a response

    request_msg = create_message(MSG_DISCOVERY_REQUEST)
    
    try:
        # Broadcast the request
        client_sock.sendto(request_msg, (config.udp_broadcast_addr, config.udp_discovery_port))
        
        # Wait for a response
        data, _ = client_sock.recvfrom(1024)
        response = parse_message(data)

        if response.get("type") == MSG_DISCOVERY_RESPONSE:
            payload = response.get("payload", {})
            leader_id = payload.get("leader_id")
            http_addr = payload.get("http_addr")
            if leader_id is not None and http_addr:
                print(f"Discovered Leader {leader_id} at {http_addr}")
                return leader_id, http_addr
    except socket.timeout:
        print("Leader discovery timed out. No response received.")
    except Exception as e:
        print(f"An error occurred during leader discovery: {e}")
    finally:
        client_sock.close()
        
    return None

# Example Usage
if __name__ == '__main__':
    # This requires running two separate processes (or threads) to simulate.
    # Here, we'll simulate the workflow in a single script.
    
    from server.config import AppConfig
    
    # 1. Mock a Leader Node
    leader_config = AppConfig()
    leader_config.node_id = 10
    
    leader_state = NodeState(node_id=leader_config.node_id)
    leader_state.set_role("LEADER")
    
    listener = DiscoveryListener(leader_state, leader_config)
    listener.start()
    
    # Give the listener a moment to start up
    time.sleep(0.5)
    
    # 2. Mock a Client searching for the leader
    client_config = AppConfig() # Use default config for client
    discovered_info = discover_leader(client_config)
    
    # 3. Verify
    assert discovered_info is not None
    discovered_id, discovered_addr = discovered_info
    assert discovered_id == leader_config.node_id
    assert discovered_addr == f"{leader_config.http_host}:{leader_config.http_port}"
    
    print("\nSuccessfully discovered the leader.")
    
    # 4. Clean up
    listener.stop()
    listener.join()
    print("UDP Discovery example finished.")
