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
import logging
from typing import Optional, Tuple

# Add project root to path to allow absolute imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common.protocol import (
    create_message, parse_message, 
    MSG_DISCOVERY_REQUEST, MSG_DISCOVERY_RESPONSE,
    MSG_NODE_QUERY, MSG_NODE_PRESENCE
)
from core.state import NodeState

logger = logging.getLogger(__name__)

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
        logger.info("UDP Discovery Listener started on port %s", self.config.udp_discovery_port)
        self.running = True
        
        while self.running:
            try:
                data, addr = self.sock.recvfrom(1024)
                msg = parse_message(data)
                msg_type = msg.get("type")
                response_host = self.config.resolve_advertised_host(addr[0])

                if msg_type == MSG_DISCOVERY_REQUEST and self.node_state.is_leader():
                    logger.info("Received leader discovery request from %s", addr)
                    response_payload = {
                        "leader_id": self.node_state.leader_id,
                        "http_addr": f"{response_host}:{self.config.http_port}"
                    }
                    response_msg = create_message(MSG_DISCOVERY_RESPONSE, response_payload)
                    self.sock.sendto(response_msg, addr)
                
                elif msg_type == MSG_NODE_QUERY:
                    logger.debug("Received node query from %s", addr)
                    response_payload = {
                        "node_id": self.node_state.node_id,
                        "tcp_addr": f"{response_host}:{self.config.tcp_port}"
                    }
                    response_msg = create_message(MSG_NODE_PRESENCE, response_payload)
                    self.sock.sendto(response_msg, addr)
            except Exception as e:
                if self.running:
                    logger.error("Error in DiscoveryListener: %s", e)

    def stop(self):
        self.running = False
        self.sock.close()
        logger.info("UDP Discovery Listener stopped.")

def discover_leader(config) -> Optional[Tuple[int, str]]:
    """
    Client-side function to find the leader by sending a UDP broadcast.

    Returns:
        A tuple containing (leader_id, http_address) or None if no leader is found.
    """
    logger.info("Broadcasting to discover the leader...")
    
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
                logger.info("Discovered Leader %s at %s", leader_id, http_addr)
                return leader_id, http_addr
    except socket.timeout:
        logger.warning("Leader discovery timed out. No response received.")
    except Exception as e:
        logger.error("An error occurred during leader discovery: %s", e)
    finally:
        client_sock.close()
        
    return None

def discover_nodes(config, timeout: float = 2.0) -> list:
    """
    Broadcasts a query to find all active nodes in the network.
    Used for ring repair and dynamic joining.
    """
    logger.info("Broadcasting to discover all active nodes...")
    nodes = []
    
    client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    client_sock.settimeout(timeout)

    query_msg = create_message(MSG_NODE_QUERY)
    
    try:
        client_sock.sendto(query_msg, (config.udp_broadcast_addr, config.udp_discovery_port))
        
        start_time = time.time()
        while time.time() - start_time < timeout:
            try:
                data, _ = client_sock.recvfrom(1024)
                response = parse_message(data)
                if response.get("type") == MSG_NODE_PRESENCE:
                    payload = response.get("payload", {})
                    node_id = payload.get("node_id")
                    tcp_addr = payload.get("tcp_addr")
                    if node_id is not None and tcp_addr:
                        # Avoid duplicates
                        if not any(n['node_id'] == node_id for n in nodes):
                            nodes.append({"node_id": node_id, "tcp_addr": tcp_addr})
            except socket.timeout:
                break
    except Exception as e:
        logger.error("Error during node discovery: %s", e)
    finally:
        client_sock.close()
        
    logger.info("Discovered %d active nodes.", len(nodes))
    return nodes

# Example Usage
if __name__ == '__main__':
    # This requires running two separate processes (or threads) to simulate.
    # Here, we'll simulate the workflow in a single script.
    
    from server.config import AppConfig
    
    # 1. Mock a Leader Node
    leader_config = AppConfig()
    leader_config.node_id = 10
    # Fix: Bind to a specific IP for the test so the assertion below holds true.
    # If left as 0.0.0.0, the discovery response would contain a resolved IP (e.g. 192.168.x.x),
    # which would not match '0.0.0.0' in the assertion.
    leader_config.http_host = '127.0.0.1'
    
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
    # print(f"{leader_config.http_host}:{leader_config.http_port}")
    assert discovered_addr == f"{leader_config.http_host}:{leader_config.http_port}"
    
    logger.info("Successfully discovered the leader.")
    
    # 4. Clean up
    listener.stop()
    listener.join()
    logger.info("UDP Discovery example finished.")
