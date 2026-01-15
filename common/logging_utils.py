# common/logging_utils.py

"""
Logging setup helpers for DIMS.
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional

LOG_FORMAT = "%(asctime)s - %(name)s - [%(levelname)s] - (%(module)s:%(lineno)d) - %(message)s"


def current_timestamp() -> str:
    """Returns a timestamp in MMDDYYYYHHMMSS format."""
    return datetime.now().strftime("%m%d%Y%H%M%S")


def build_log_path(prefix: str, node_id: Optional[int] = None, log_dir: str = "debug_log") -> str:
    """
    Builds a log file path with the required naming convention.
    """
    timestamp = current_timestamp()
    if node_id is None:
        filename = f"{prefix}_{timestamp}.log"
    else:
        filename = f"{prefix}_{node_id}_{timestamp}.log"
    return os.path.join(log_dir, filename)


def configure_logging(log_file: str, level: int = logging.INFO, to_console: bool = True) -> logging.Logger:
    """
    Configures root logging to a file (and optionally console).
    """
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    formatter = logging.Formatter(LOG_FORMAT)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    if to_console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)

    return root_logger
