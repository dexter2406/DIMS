# common/utils.py

"""
Provides common utility functions and helper classes used across the system.
"""

import time
import json
import logging
from functools import wraps

logger = logging.getLogger(__name__)

def get_timestamp_ms() -> int:
    """Returns the current time in milliseconds."""
    return int(time.time() * 1000)

def serialize_data(data: dict) -> bytes:
    """Serializes a dictionary to a UTF-8 encoded byte string."""
    return json.dumps(data, sort_keys=True).encode('utf-8')

def deserialize_data(byte_data: bytes) -> dict:
    """Deserializes a UTF-8 encoded byte string into a dictionary."""
    return json.loads(byte_data.decode('utf-8'))

def retry(times: int, delay: float, exceptions: tuple = (Exception,)):
    """
    A decorator for retrying a function call upon specified exceptions.

    Args:
        times (int): The number of times to retry.
        delay (float): The delay in seconds between retries.
        exceptions (tuple): A tuple of exception classes to catch and retry on.
    """
    def decorator(func):
        """Wrap a function with retry behavior."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            """Retry the wrapped function on configured exceptions."""
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    logger.warning("Attempt %s failed: %s. Retrying in %ss...", attempt, e, delay)
                    time.sleep(delay)
            # Final attempt
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Example usage of the retry decorator
if __name__ == "__main__":
    
    @retry(times=3, delay=1, exceptions=(ValueError,)) # High chance of failure
    def might_fail(fail_chance: float):
        """Randomly raise a ValueError to demo retry behavior."""
        import random
        if random.random() < fail_chance:
            raise ValueError("Simulated failure")
        return "Success!"

    logger.info("Running a function that might fail with a retry mechanism...")
    try:
        result = might_fail(fail_chance=0.8) 
        logger.info("Function completed with result: %s", result)
    except ValueError as e:
        logger.error("Function failed after all retries: %s", e)

    logger.info("Utility functions are ready.")
    logger.info("Current timestamp (ms): %s", get_timestamp_ms())
