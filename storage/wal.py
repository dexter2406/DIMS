# storage/wal.py

"""
Implements the Write-Ahead Log (WAL) for ensuring durability of state changes.
Every state-mutating operation (like an inventory update) must be recorded
in the WAL before being applied to the in-memory state. This allows for
state recovery after a crash.
"""

import json
import os
import threading
import logging
from typing import Dict, Any, Generator

logger = logging.getLogger(__name__)

class WriteAheadLog:
    """
    A thread-safe, append-only log for recording operations.
    The log is stored as a sequence of JSON objects, one per line.
    """
    def __init__(self, wal_path: str):
        self._lock = threading.Lock()
        self.wal_path = wal_path
        
        # Ensure the directory for the log file exists
        wal_dir = os.path.dirname(self.wal_path)
        if wal_dir:
            os.makedirs(wal_dir, exist_ok=True)
            
        # Open the file in append mode. The file handle is kept open to
        # reduce overhead on frequent writes.
        self.file = open(self.wal_path, 'a')
        logger.info("WAL initialized at %s", self.wal_path)

    def append(self, record: Dict[str, Any]):
        """
        Appends a record to the WAL in a thread-safe manner.

        Args:
            record (Dict[str, Any]): The operation to be logged, which must
                                     be JSON-serializable.
        """
        with self._lock:
            log_entry = json.dumps(record)
            self.file.write(log_entry + '\n')
            self.file.flush() # Ensure it's written to disk immediately

    def read_all(self) -> Generator[Dict[str, Any], None, None]:
        """
        Reads all records from the WAL from beginning to end.
        This is primarily used during the recovery process.

        Yields:
            A generator of dictionaries, each representing a logged record.
        """
        try:
            with open(self.wal_path, 'r') as f:
                for line in f:
                    if line.strip():
                        yield json.loads(line)
        except FileNotFoundError:
            # It's okay if the file doesn't exist on first start
            logger.info("WAL file not found at %s. Starting fresh.", self.wal_path)
            return

    def close(self):
        """Closes the WAL file handle."""
        with self._lock:
            self.file.close()

    def __del__(self):
        """Ensure the file is closed when the object is garbage collected."""
        if not self.file.closed:
            self.close()

# Example Usage
if __name__ == "__main__":
    # Use a temporary WAL file for the demo
    demo_wal_path = './data/wal_demo.log'
    if os.path.exists(demo_wal_path):
        os.remove(demo_wal_path)

    wal = WriteAheadLog(wal_path=demo_wal_path)

    # Simulate some operations being logged
    logger.info("Appending records to the WAL...")
    op1 = {"op": "IN", "item_id": "skuX", "quantity": 50}
    op2 = {"op": "IN", "item_id": "skuY", "quantity": 120}
    wal.append(op1)
    wal.append(op2)
    
    # The 'wal' object is closed automatically on exit, but we can be explicit
    wal.close()
    logger.info("WAL closed.")

    # --- Recovery Simulation ---
    logger.info("Simulating recovery by reading all records from the WAL:")
    
    # In a real scenario, a new WAL object would be created on startup
    recovery_wal = WriteAheadLog(wal_path=demo_wal_path)
    
    recovered_ops = 0
    for record in recovery_wal.read_all():
        logger.info("  - Recovered record: %s", record)
        recovered_ops += 1
    
    logger.info("Total records recovered: %s", recovered_ops)
    assert recovered_ops == 2

    recovery_wal.close()
    
    # Clean up the demo file
    os.remove(demo_wal_path)
    logger.info("WAL functionality is working correctly.")
