# server/logger.py

"""
Configures and provides a standardized logger for the application.
This ensures consistent log formatting and output across all modules.
"""

import logging
import sys

def setup_logger(name: str = 'DIMS', level: int = logging.INFO) -> logging.Logger:
    """
    Sets up a configured logger instance.

    Args:
        name (str): The name of the logger.
        level (int): The logging level (e.g., logging.INFO, logging.DEBUG).

    Returns:
        logging.Logger: A configured logger instance.
    """
    # Create a logger
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.propagate = False # Prevent duplicate logs in parent loggers

    # Avoid adding handlers if they already exist
    if not logger.handlers:
        # Create a console handler
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(level)

        # Create a formatter and set it for the handler
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - [%(levelname)s] - (%(module)s:%(lineno)d) - %(message)s'
        )
        handler.setFormatter(formatter)

        # Add the handler to the logger
        logger.addHandler(handler)

    return logger

# Create a default logger instance for easy import in other modules
log = setup_logger()

if __name__ == '__main__':
    # This block demonstrates the logger's usage.
    
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
    
    print("\nLogger is configured and ready.")
