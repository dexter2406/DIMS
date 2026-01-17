# api/http_server.py

"""
Provides the external HTTP REST API for clients (scanners) to interact with
the distributed inventory system. It uses Python's built-in http.server.
"""

import json
import threading
import logging
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Callable, Tuple
from urllib.parse import urlparse, parse_qs

# Add project root to path to allow absolute imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from core.state import NodeState, ROLE_LEADER
from server.config import AppConfig

logger = logging.getLogger(__name__)

# A callback for handling a valid POST request
UpdateHandler = Callable[[dict], Tuple[bool, int, dict]]

class APRequestHandler(BaseHTTPRequestHandler):
    """
    HTTP request handler for the API. It's instantiated once for each request.
    The node_state and update_handler are passed via the server instance.
    """
    
    def _send_response(self, status_code: int, body: dict):
        self.send_response(status_code)
        self.send_header('Content-type', 'application/json')
        self.send_header('Connection', 'close')
        self.end_headers()
        self.close_connection = True
        self.wfile.write(json.dumps(body).encode('utf-8'))

    def do_GET(self):
        """Handles GET requests, e.g., for status checks."""
        parsed_url = urlparse(self.path)
        if parsed_url.path == '/status':
            node_state: NodeState = self.server.node_state
            inventory = node_state.get_inventory()
            status_body = {
                "node_id": node_state.node_id,
                "role": node_state.role,
                "leader_id": node_state.leader_id,
                "inventory_size": len(inventory),
                "total_units": sum(inventory.values())
            }
            self._send_response(200, status_body)
        elif parsed_url.path == '/inventory':
            node_state: NodeState = self.server.node_state
            inventory = node_state.get_inventory()
            params = parse_qs(parsed_url.query)
            by_type_raw = params.get("by_type", ["0"])[0]
            by_type = str(by_type_raw).lower() in {"1", "true", "yes", "y"}

            if by_type:
                totals = {}
                for item_id, quantity in inventory.items():
                    item_type, sep, _uid = item_id.partition(":")
                    if not sep or not item_type:
                        item_type = item_id
                    totals[item_type] = totals.get(item_type, 0) + quantity
                self._send_response(200, {"by_type": totals})
            else:
                self._send_response(200, {"items": inventory})
        else:
            self._send_response(404, {"error": "Not Found"})

    def do_POST(self):
        """Handles POST requests, specifically for inventory updates."""
        node_state: NodeState = self.server.node_state
        update_handler: UpdateHandler = self.server.update_handler

        # --- Follower Logic ---
        if not node_state.is_leader():
            # A follower should reject the request and inform the client.
            # Option 1: Simple "Service Unavailable"
            error_body = {
                "error": "Service Unavailable. This node is not the leader.",
                "leader_hint": f"Current known leader is Node {node_state.leader_id}"
            }
            self._send_response(503, error_body)
            # Option 2 (more advanced): Redirect to the known leader.
            # self.send_response(307) # Temporary Redirect
            # self.send_header('Location', f'http://{leader_http_addr}/update')
            # self.end_headers()
            return

        # --- Leader Logic ---
        if self.path == '/update':
            try:
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                update_payload = json.loads(post_data)
                
                logger.info("Leader received HTTP update: %s", update_payload)
                
                # Pass the update to the handler function (defined in main.py)
                # This handler will be responsible for WAL, state update, and replication
                result = update_handler(update_payload)
                if isinstance(result, tuple) and len(result) == 3:
                    success, status_code, body = result
                else:
                    success = bool(result)
                    status_code = 202 if success else 400
                    body = {"status": "Accepted"} if success else {"error": "Invalid update data"}

                self._send_response(status_code, body)

            except json.JSONDecodeError:
                self._send_response(400, {"error": "Bad Request: Invalid JSON"})
            except Exception as e:
                logger.error("Error handling POST request: %s", e)
                self._send_response(500, {"error": "Internal Server Error"})
        else:
            self._send_response(404, {"error": "Not Found"})

class APIServer(threading.Thread):
    """
    A thread that runs the HTTP server.
    """
    def __init__(self, config: AppConfig, node_state: NodeState, update_handler: UpdateHandler):
        super().__init__(daemon=True)
        self.server_address = (config.http_host, config.http_port)
        
        # Custom HTTPServer that holds references to our application state
        class CustomHTTPServer(HTTPServer):
            def __init__(self, *args, **kwargs):
                self.node_state = node_state
                self.update_handler = update_handler
                super().__init__(*args, **kwargs)

        self.httpd = CustomHTTPServer(self.server_address, APRequestHandler)
        logger.info("HTTP API server will run on %s:%s", self.server_address[0], self.server_address[1])

    def run(self):
        logger.info("Starting HTTP API server...")
        self.httpd.serve_forever()

    def stop(self):
        logger.info("Stopping HTTP API server...")
        self.httpd.shutdown()
        self.httpd.server_close()
        logger.info("HTTP API server stopped.")

# Example Usage
if __name__ == '__main__':
    # This simulation shows the server running and handling a request.
    
    sim_config = AppConfig()
    sim_config.http_port = 8888
    
    # --- Simulate a LEADER node ---
    leader_state = NodeState(node_id=1)
    leader_state.set_role(ROLE_LEADER)

    def simple_update_handler(payload):
        logger.info("[Handler] Processing payload: %s", payload)
        ok = "item_id" in payload and "op" in payload and "quantity" in payload
        if ok:
            return True, 202, {"status": "Accepted"}
        return False, 400, {"error": "Invalid update data"}

    http_server = APIServer(sim_config, leader_state, simple_update_handler)
    http_server.start()

    # --- Simulate a FOLLOWER node (for comparison) ---
    follower_state = NodeState(node_id=2)
    follower_state.leader_id = 1 # Knows who the leader is
    
    # In a real app, you wouldn't run two servers on the same port.
    # We just demonstrate the logic. The handler for a follower is irrelevant
    # as it's never called.
    
    logger.info("--- Testing API Server ---")
    logger.info("Use a tool like curl to test:")
    logger.info("  curl http://localhost:8888/status")
    logger.info(
        "  curl -X POST -H \"Content-Type: application/json\" -d '{\"item_id\":\"sku:ABC\",\"op\":\"IN\",\"quantity\":10}' http://localhost:8888/update"
    )
    logger.info("To test follower behavior, you would need to run a separate server instance with a follower state.")
    
    try:
        # Keep the main thread alive to let the server run
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        http_server.stop()
        http_server.join()
    
    logger.info("HTTP Server example finished.")
