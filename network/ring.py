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
from typing import Optional, Callable

# Add project root to path to allow absolute imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.state import NodeState
from server.config import AppConfig
from common.protocol import create_message, MSG_HEARTBEAT

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
            print(f"TCP Ring Server listening on {self.config.tcp_host}:{self.config.tcp_port}")
            self.running = True
            
            while self.running:
                try:
                    conn, addr = s.accept()
                    print(f"Accepted connection from predecessor at {addr}")
                    self._predecessor_conn = conn
                    
                    with self._predecessor_conn:
                        while self.running:
                            data = self._predecessor_conn.recv(4096)
                            if not data:
                                print("Predecessor connection closed.")
                                break
                            self.message_handler(data)
                except Exception as e:
                    if self.running:
                        print(f"Error in TCPRingServer: {e}")
                finally:
                    self._predecessor_conn = None
                    print("Waiting for new predecessor connection...")

    def stop(self):
        self.running = False
        # To unblock the accept() call
        try:
            # Connect to self to unblock listener
            with socket.create_connection((self.config.tcp_host, self.config.tcp_port)):
                pass
        except:
            pass
        print("TCP Ring Server stopped.")


class TCPRingClient(threading.Thread):
    """
    Manages the outgoing TCP connection to the successor node.
    Sends heartbeats and other messages. Handles connection failures.
    """
    def __init__(self, node_state: NodeState, config: AppConfig):
        super().__init__(daemon=True)
        self.node_state = node_state
        self.config = config
        self.running = False
        self._successor_conn: Optional[socket.socket] = None

    def run(self):
        self.running = True
        while self.running:
            successor_addr = self.node_state.successor_addr
            if successor_addr and not self._is_connected():
                try:
                    print(f"Attempting to connect to successor at {successor_addr}...")
                    self._successor_conn = socket.create_connection(
                        successor_addr, timeout=self.config.connection_timeout
                    )
                    print(f"Successfully connected to successor at {successor_addr}")
                except Exception as e:
                    print(f"Failed to connect to successor {successor_addr}: {e}")
                    # TODO: Trigger ring repair/election logic here
                    self.node_state.set_successor(None) # Clear bad successor
                    time.sleep(self.config.heartbeat_interval) # Wait before retrying
                    continue

            # If connected, send periodic heartbeats
            if self._is_connected():
                try:
                    self.send_message(create_message(MSG_HEARTBEAT, {"node_id": self.node_state.node_id}))
                except Exception as e:
                    print(f"Failed to send heartbeat to successor: {e}")
                    self._close_connection()
                    # TODO: Trigger ring repair/election
            
            time.sleep(self.config.heartbeat_interval)

    def send_message(self, msg: bytes):
        if not self._is_connected():
            raise ConnectionError("Not connected to a successor.")
        self._successor_conn.sendall(msg)

    def _is_connected(self) -> bool:
        return self._successor_conn is not None

    def _close_connection(self):
        if self._successor_conn:
            self._successor_conn.close()
            self._successor_conn = None
        print("Successor connection closed.")

    def stop(self):
        self.running = False
        self._close_connection()
        print("TCP Ring Client stopped.")

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
        print(f"Successor received: {data.decode()}")

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
    print("Waiting for client to send a heartbeat...")
    time.sleep(pred_config.heartbeat_interval + 1)

    # --- Cleanup ---
    client.stop()
    server.stop()
    client.join()
    server.join()
    
    print("\nRing networking components basic test finished.")
