# client/scanner_simulator.py

"""
CLI simulator for ScannerClient. Sends deterministic IN/SHIP updates based on
simple parameters for repeatable testing.
"""

import argparse
import logging
import time

# Add project root to path to allow absolute imports
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from client.scanner_client import ScannerClient
from server.config import AppConfig
from common.logging_utils import configure_logging, build_log_path

logger = logging.getLogger(__name__)


def _parse_args():
    """Parse CLI arguments for the scanner simulator."""
    parser = argparse.ArgumentParser(description="DIMS Scanner Client Simulator")
    parser.add_argument(
        "--op",
        required=True,
        type=str.upper,
        choices=["IN", "SHIP"],
        help="Inventory operation",
    )
    parser.add_argument(
        "--type",
        required=True,
        dest="item_type",
        help="Item ID prefix (final ID is prefix + number)",
    )
    parser.add_argument("--start-no", type=int, required=True, help="Start ID number")
    parser.add_argument("--num", type=int, required=True, help="How many items to process")
    parser.add_argument(
        "--interval",
        type=float,
        default=3,
        help="Seconds to wait between requests",
    )
    parser.add_argument(
        "--quantity",
        type=int,
        default=1,
        help="Quantity per item (default: 1)",
    )
    return parser.parse_args()


def _iter_item_ids(item_type: str, start_no: int, num: int):
    """Yield item IDs built from type prefix and numeric range."""
    for offset in range(num):
        yield f"{item_type}{start_no + offset}"


def _send_sequence(client: ScannerClient, op: str, item_type: str, start_no: int, num: int, quantity: int, interval: float):
    """Send a sequence of updates, handling retries and stop conditions."""
    for item_id in _iter_item_ids(item_type, start_no, num):
        accepted, status_code = client.send_update(item_id, op, quantity)
        time.sleep(1)  # Small delay to avoid overwhelming logs
        if accepted:
            logger.info("Applied %s for %s (qty=%s)", op, item_id, quantity)
        else:
            if status_code == 409:
                logger.warning("Insufficient inventory for %s. Stopping.", item_id)
                break

            if status_code in {0, 503}:
                logger.warning("Update failed (status=%s). Rediscovering leader...", status_code)
                if client.find_leader():
                    accepted, status_code = client.send_update(item_id, op, quantity)
                    if accepted:
                        logger.info("Applied %s for %s (qty=%s)", op, item_id, quantity)
                        if interval > 0:
                            time.sleep(interval)
                        continue
                    if status_code == 409:
                        logger.warning("Insufficient inventory for %s. Stopping.", item_id)
                        break

            logger.warning("Update failed for %s (status=%s). Stopping.", item_id, status_code)
            break

        if interval > 0:
            time.sleep(interval)


def main():
    """Run the simulator with parsed arguments and exit code."""
    args = _parse_args()

    if args.num <= 0:
        logger.error("--num must be positive.")
        return 2

    if args.quantity <= 0:
        logger.error("--quantity must be positive.")
        return 2

    log_file = build_log_path("client")
    configure_logging(log_file, level=logging.INFO, to_console=True)

    config = AppConfig()
    client = ScannerClient(config)

    if not client.find_leader():
        logger.error("No leader discovered. Exiting.")
        return 1

    _send_sequence(
        client=client,
        op=args.op,
        item_type=args.item_type,
        start_no=args.start_no,
        num=args.num,
        quantity=args.quantity,
        interval=args.interval,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
