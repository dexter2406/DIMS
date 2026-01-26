# client/scanner_simulator.py

"""
CLI simulator for ScannerClient. Sends deterministic IN/OUT updates based on
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
        choices=["IN", "OUT"],
        help="Inventory operation",
    )
    parser.add_argument(
        "--item-id-start",
        default="item1000",
        help="Starting item_id (expects trailing digits for auto-increment)",
    )
    parser.add_argument(
        "--id-range",
        type=int,
        default=3,
        help="How many consecutive item_ids to generate",
    )
    parser.add_argument(
        "--count",
        type=int,
        default=1,
        help="Quantity applied to each generated item_id",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=3,
        help="Seconds to wait between requests",
    )
    return parser.parse_args()


def _split_item_id_start(item_id_start: str):
    """Split item_id_start into prefix and numeric suffix."""
    prefix = item_id_start.rstrip("0123456789")
    suffix = item_id_start[len(prefix):]
    if not suffix:
        raise ValueError("item-id-start must end with digits (e.g., item1000)")
    return prefix, int(suffix)


def _iter_item_ids(item_id_start: str, id_range: int):
    """Yield item IDs built from a starting ID and range size."""
    prefix, start_no = _split_item_id_start(item_id_start)
    for offset in range(id_range):
        yield f"{prefix}{start_no + offset}"


def _send_sequence(
    client: ScannerClient,
    op: str,
    item_id_start: str,
    id_range: int,
    count: int,
    interval: float,
):
    """Send a sequence of updates, handling retries and stop conditions."""
    for item_id in _iter_item_ids(item_id_start, id_range):
        for _ in range(count):
            accepted, status_code = client.send_update(item_id, op, 1)
            time.sleep(1)  # Small delay to avoid overwhelming logs
            if accepted:
                logger.info("Applied %s for %s (qty=1)", op, item_id)
            else:
                if status_code == 409:
                    logger.warning("Insufficient inventory for %s. Stopping.", item_id)
                    return
                
                # Leader rediscovery
                if status_code in {0, 503}:
                    logger.warning("Update failed (status=%s). Rediscovering leader...", status_code)
                    if client.find_leader():
                        accepted, status_code = client.send_update(item_id, op, 1)
                        if accepted:
                            logger.info("Applied %s for %s (qty=1)", op, item_id)
                            if interval > 0:
                                time.sleep(interval)
                            continue
                        if status_code == 409:
                            logger.warning("Insufficient inventory for %s. Stopping.", item_id)
                            return

                logger.warning("Update failed for %s (status=%s). Stopping.", item_id, status_code)
                return

            if interval > 0:
                time.sleep(interval)


def main():
    """Run the simulator with parsed arguments and exit code."""
    args = _parse_args()

    if args.id_range <= 0:
        logger.error("--id-range must be positive.")
        return 2

    if args.count <= 0:
        logger.error("--count must be positive.")
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
        item_id_start=args.item_id_start,
        id_range=args.id_range,
        count=args.count,
        interval=args.interval,
    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
