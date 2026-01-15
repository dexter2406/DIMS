# server/logger.py

"""
Configures and provides a standardized logger for the application.
This ensures consistent log formatting and output across all modules.
"""

import logging
from common.logging_utils import configure_logging, build_log_path

def setup_logger(name: str = 'DIMS', level: int = logging.INFO) -> logging.Logger:
    """
    Sets up a configured logger instance.

    Args:
        name (str): The name of the logger.
        level (int): The logging level (e.g., logging.INFO, logging.DEBUG).

    Returns:
        logging.Logger: A configured logger instance.
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    return logger

def configure_server_logging(node_id: int, log_dir: str = "debug_log", level: int = logging.INFO, to_console: bool = True) -> logging.Logger:
    """
    Configures logging for a server node using the required naming convention.
    """
    log_file = build_log_path("server", node_id=node_id, log_dir=log_dir)
    return configure_logging(log_file, level=level, to_console=to_console)

# Create a default logger instance for easy import in other modules
log = setup_logger()

if __name__ == '__main__':
    # This block demonstrates the logger's usage.

    configure_server_logging(node_id=1, to_console=True)

    log.debug("This is a debug message.")
    log.info("This is an info message.")
    log.warning("This is a warning message.")
    log.error("This is an error message.")
    log.critical("This is a critical message.")

    # You can also create specific loggers for different modules
    api_logger = setup_logger('DIMS.api')
    api_logger.info("Logging from the API module.")

    core_logger = setup_logger('DIMS.core', level=logging.DEBUG)
    core_logger.debug("A detailed debug message from the core.")

    log.info("Logger is configured and ready.")
