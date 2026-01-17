# network/ring.py

"""
Manages the node's participation in the TCP-based logical ring.
This includes:
- Connecting to a successor.
- Accepting a connection from a predecessor.
- Sending and receiving messages (heartbeats, replication, election).
- Detecting connection failures and triggering ring repair/election.
"""

import socket
import threading
import time
import struct
import logging
from typing import Optional, Callable

# Add project root to path to allow absolute imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.state import NodeState
from server.config import AppConfig
from common.protocol import create_message, MSG_HEARTBEAT
from network.udp_discovery import discover_nodes

logger = logging.getLogger(__name__)

# A callback type for handling received messages
MessageHandler = Callable[[bytes], None]

class TCPRingServer(threading.Thread):
    """
    Listens for an incoming TCP connection from the predecessor node.
    Once connected, it receives messages and passes them to a handler.
    """
    def __init__(self, config: AppConfig, message_handler: MessageHandler):
        super().__init__(daemon=True)
        self.config = config
        self.message_handler = message_handler
        self.running = False
        self._predecessor_conn: Optional[socket.socket] = None

    def run(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((self.config.tcp_host, self.config.tcp_port))
            s.listen()
            logger.info("TCP Ring Server listening on %s:%s", self.config.tcp_host, self.config.tcp_port)
            self.running = True
            
            while self.running:
                try:
                    conn, addr = s.accept()
                    logger.info("Accepted connection from predecessor at %s", addr)
                    self._predecessor_conn = conn
                    
                    with self._predecessor_conn:
                        while self.running:
                            # Read 4-byte length prefix
                            header = self._recv_exactly(4)
                            if not header:
                                logger.info("Predecessor connection closed.")
                                break
                            
                            msg_len = struct.unpack('>I', header)[0]
                            
                            # Read the payload based on the length
                            data = self._recv_exactly(msg_len)
                            if not data:
                                logger.info("Predecessor connection closed during message body.")
                                break
                                
                            self.message_handler(data)
                except Exception as e:
                    if self.running:
                        logger.error("Error in TCPRingServer: %s", e)
                finally:
                    self._predecessor_conn = None
                    logger.info("Waiting for new predecessor connection...")

    def stop(self):
        self.running = False
        # To unblock the accept() call
        try:
            # Connect to self to unblock listener
            with socket.create_connection((self.config.tcp_host, self.config.tcp_port)):
                pass
        except:
            pass
        logger.info("TCP Ring Server stopped.")

    def _recv_exactly(self, n: int) -> Optional[bytes]:
        """Helper to receive exactly n bytes from the predecessor connection."""
        data = b''
        while len(data) < n:
            packet = self._predecessor_conn.recv(n - len(data))
            if not packet:
                return None
            data += packet
        return data


class TCPRingClient(threading.Thread):
    """
    Manages the outgoing TCP connection to the successor node.
    Sends heartbeats and other messages. Handles connection failures.
    """
    def __init__(self, node_state: NodeState, config: AppConfig, on_failure: Optional[Callable[[], None]] = None):
        super().__init__(daemon=True)
        self.node_state = node_state
        self.config = config
        self.on_failure = on_failure
        self.running = False
        self._repair_triggered = False
        self._successor_conn: Optional[socket.socket] = None
        self._message_buffer = []
        self._buffer_lock = threading.Lock()
        self._last_topology_check = 0
        self._topology_check_interval = 10  # Seconds between topology re-evaluations

    def run(self):
        self.running = True
        self._last_topology_check = time.time()

        while self.running:
            # Periodic topology check to handle new node joins
            if time.time() - self._last_topology_check > self._topology_check_interval:
                self._check_topology()
                self._last_topology_check = time.time()

            if not self._is_connected():
                # Proactively try to find a successor if we don't have one (dynamic join/repair)
                if not self.node_state.successor_addr:
                    self._repair_ring()
                    if self.node_state.successor_addr:
                        self._repair_triggered = True

                successor_addr = self.node_state.successor_addr
                if successor_addr:
                    try:
                        logger.info("Attempting to connect to successor at %s...", successor_addr)
                        self._successor_conn = socket.create_connection(
                            successor_addr, timeout=self.config.connection_timeout
                        )
                        logger.info("Successfully connected to successor at %s", successor_addr)
                        
                        # Flush buffered messages
                        with self._buffer_lock:
                            if self._message_buffer:
                                logger.info("Flushing %d buffered messages.", len(self._message_buffer))
                                for msg in self._message_buffer:
                                    length_prefix = struct.pack('>I', len(msg))
                                    self._successor_conn.sendall(length_prefix + msg)
                                self._message_buffer.clear()

                        # Only trigger election if this connection is the result of a repair/failure
                        if self._repair_triggered and self.on_failure:
                            logger.info("Triggering election after ring topology change/establishment.")
                            self.on_failure()
                            self._repair_triggered = False
                    except Exception as e:
                        logger.warning("Failed to connect to successor %s: %s", successor_addr, e)
                        self._repair_ring()
                        
                        # If repair found a successor, mark it so next connect triggers election.
                        if self.node_state.successor_addr:
                            logger.info("Repair found successor. Election scheduled after connection.")
                            self._repair_triggered = True
                        else:
                            # If isolated, trigger election immediately (self-elect)
                            if self.on_failure:
                                self.on_failure()
                            self._repair_triggered = False

                        time.sleep(self.config.heartbeat_interval)
                        continue

            # If connected, send periodic heartbeats
            if self._is_connected():
                try:
                    self.send_message(create_message(MSG_HEARTBEAT, {"node_id": self.node_state.node_id}))
                except Exception as e:
                    logger.warning("Failed to send heartbeat to successor: %s", e)
                    self._close_connection()
                    self._repair_ring() # Fix: Ensure ring is repaired on heartbeat failure
                    self._repair_triggered = True
                    if self.node_state.successor_addr is None and self.on_failure:
                        self.on_failure()
                        self._repair_triggered = False
            
            time.sleep(self.config.heartbeat_interval)

    def _check_topology(self):
        """
        Periodically checks if a better successor has appeared (e.g., a new node joined).
        If so, updates the state and closes the current connection to trigger a reconnect.
        """
        target_node = self._discover_best_successor_node()
        if not target_node:
            return

        host, port_str = target_node['tcp_addr'].split(':')
        new_addr = (host, int(port_str))

        if new_addr != self.node_state.successor_addr:
            logger.info("Topology optimization: Switching successor to Node %s at %s", 
                        target_node['node_id'], new_addr)
            self.node_state.set_successor(new_addr)
            self._repair_triggered = True
            self._close_connection()

    def _discover_best_successor_node(self):
        """Helper to discover nodes and calculate the ideal successor."""
        active_nodes = discover_nodes(self.config)
        others = [n for n in active_nodes if n['node_id'] != self.node_state.node_id]
        
        if not others:
            return None

        others.sort(key=lambda x: x['node_id'])
        
        for node in others:
            if node['node_id'] > self.node_state.node_id:
                return node
        return others[0]

    def _repair_ring(self):
        """
        Attempts to find a new successor using UDP discovery to repair the ring.
        """
        logger.info("Initiating successor discovery (repair/join) for Node %s...", self.node_state.node_id)
        target_node = self._discover_best_successor_node()
        
        if not target_node:
            logger.info("No other nodes found. Node %s is isolated.", self.node_state.node_id)
            self.node_state.set_successor(None)
            return
            
        host, port_str = target_node['tcp_addr'].split(':')
        logger.info("Topology update: Node %s selected new successor Node %s at %s:%s", 
                    self.node_state.node_id, target_node['node_id'], host, port_str)
        self.node_state.set_successor((host, int(port_str)))

    def send_message(self, msg: bytes):
        with self._buffer_lock:
            if self._is_connected():
                try:
                    # Prefix the message with its 4-byte length (big-endian)
                    length_prefix = struct.pack('>I', len(msg))
                    self._successor_conn.sendall(length_prefix + msg)
                    return
                except Exception as e:
                    logger.warning("Failed to send message, buffering and reconnecting: %s", e)
                    self._close_connection()
                    # Fall through to buffer append
            
            logger.info("Buffering message (queue size: %d)", len(self._message_buffer) + 1)
            self._message_buffer.append(msg)

    def _is_connected(self) -> bool:
        return self._successor_conn is not None

    def _close_connection(self):
        if self._successor_conn:
            self._successor_conn.close()
            self._successor_conn = None
        logger.info("Successor connection closed.")

    def stop(self):
        self.running = False
        self._close_connection()
        logger.info("TCP Ring Client stopped.")

# Example Usage
if __name__ == '__main__':
    # This is complex to demo in one file. The flow would be:
    # 1. A 'successor' node starts its TCPRingServer.
    # 2. A 'predecessor' node starts its TCPRingClient and connects to the successor.
    # 3. The predecessor also starts its own TCPRingServer to accept another node.
    
    # --- Mock Successor Node ---
    succ_config = AppConfig()
    succ_config.tcp_port = 9001
    
    def handle_msg(data):
        logger.info("Successor received: %s", data.decode())

    server = TCPRingServer(succ_config, handle_msg)
    server.start()
    time.sleep(0.1)

    # --- Mock Predecessor Node ---
    pred_config = AppConfig()
    pred_config.node_id = 50
    pred_state = NodeState(pred_config.node_id)
    pred_state.set_successor(('localhost', 9001))

    client = TCPRingClient(pred_state, pred_config)
    client.start()

    # Let the client connect and send a heartbeat
    logger.info("Waiting for client to send a heartbeat...")
    time.sleep(pred_config.heartbeat_interval + 1)

    # --- Cleanup ---
    client.stop()
    server.stop()
    client.join()
    server.join()
    
    logger.info("Ring networking components basic test finished.")
