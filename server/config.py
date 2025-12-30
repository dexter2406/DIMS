# server/config.py

"""
Handles configuration loading for the server.
Configurations can be loaded from environment variables, a config file, or command-line arguments.
"""

import os
import argparse

class AppConfig:
    """
    A unified configuration class for the application.
    It holds all configurable parameters like Node ID, ports, etc.
    """
    def __init__(self):
        # Unique identifier for this node, crucial for elections
        self.node_id: int = int(os.getenv('DIMS_NODE_ID', 1))

        # HTTP server settings (for client communication)
        self.http_host: str = os.getenv('DIMS_HTTP_HOST', '0.0.0.0')
        self.http_port: int = int(os.getenv('DIMS_HTTP_PORT', 8000))

        # Internal TCP ring settings (for node-to-node communication)
        self.tcp_host: str = os.getenv('DIMS_TCP_HOST', '0.0.0.0')
        self.tcp_port: int = int(os.getenv('DIMS_TCP_PORT', 9000))
        
        # Successor's address for the initial ring connection.
        # Format: "host:port". If empty, this node starts as a lone leader.
        self.successor_addr: str = os.getenv('DIMS_SUCCESSOR_ADDR', None)

        # UDP discovery settings
        self.udp_discovery_port: int = int(os.getenv('DIMS_UDP_PORT', 10000))
        self.udp_broadcast_addr: str = os.getenv('DIMS_UDP_BROADCAST_ADDR', '<broadcast>')

        # WAL (Write-Ahead Log) settings
        self.wal_path: str = os.getenv('DIMS_WAL_PATH', f'./data/wal_{self.node_id}.log')

        # Timing settings (in seconds)
        self.heartbeat_interval: float = float(os.getenv('DIMS_HEARTBEAT_INTERVAL', 5.0))
        self.connection_timeout: float = float(os.getenv('DIMS_CONNECTION_TIMEOUT', 10.0))
        self.election_timeout: float = float(os.getenv('DIMS_ELECTION_TIMEOUT', 5.0))

    def load_from_args(self):
        """
        Parses command-line arguments and overrides default/environment settings.
        This allows for dynamic configuration at runtime.
        """
        parser = argparse.ArgumentParser(description="DIMS Distributed Inventory Management System Node")
        parser.add_argument("--node-id", type=int, help=f"Unique ID for this node (default: {self.node_id})")
        parser.add_argument("--http-port", type=int, help=f"HTTP port for client API (default: {self.http_port})")
        parser.add_argument("--tcp-port", type=int, help=f"Internal TCP port for ring communication (default: {self.tcp_port})")
        parser.add_argument("--successor", type=str, help="Address (host:port) of the successor node to connect to.")
        parser.add_argument("--wal-path", type=str, help=f"Path to the Write-Ahead Log file (default: {self.wal_path})")

        args = parser.parse_args()

        if args.node_id is not None:
            self.node_id = args.node_id
            # Update default WAL path if node_id is set via argument
            if args.wal_path is None:
                self.wal_path = f'./data/wal_{self.node_id}.log'

        if args.http_port:
            self.http_port = args.http_port
        if args.tcp_port:
            self.tcp_port = args.tcp_port
        if args.successor:
            self.successor_addr = args.successor
        if args.wal_path:
            self.wal_path = args.wal_path
            
        print(f"Configuration loaded for Node {self.node_id}")


# Global config instance
config = AppConfig()

if __name__ == '__main__':
    # This block demonstrates how to load and access the configuration.
    # In a real run, you'd call 'load_from_args' from your main entry point.
    
    print("Default configuration:")
    print(f"  Node ID: {config.node_id}")
    print(f"  HTTP Endpoint: {config.http_host}:{config.http_port}")
    print(f"  TCP Endpoint: {config.tcp_host}:{config.tcp_port}")
    print(f"  Successor: {config.successor_addr}")
    print(f"  WAL Path: {config.wal_path}")
    
    # To test argument parsing, you would run from the command line:
    # python -m server.config --node-id 10 --http-port 8080 --successor "localhost:9001"
    
    # config.load_from_args() # Example of loading from args
    
    # print("\nConfiguration after parsing args (example):")
    # print(f"  Node ID: {config.node_id}")
    # print(f"  HTTP Port: {config.http_port}")
