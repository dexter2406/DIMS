# common/utils.py

"""
Provides common utility functions and helper classes used across the system.
"""

import time
import json
from functools import wraps

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
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            while attempt < times:
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    attempt += 1
                    print(f"Attempt {attempt} failed: {e}. Retrying in {delay}s...")
                    time.sleep(delay)
            # Final attempt
            return func(*args, **kwargs)
        return wrapper
    return decorator

# Example usage of the retry decorator
if __name__ == "__main__":
    
    @retry(times=3, delay=1, exceptions=(ValueError,)) # High chance of failure
    def might_fail(fail_chance: float):
        import random
        if random.random() < fail_chance:
            raise ValueError("Simulated failure")
        return "Success!"

    print("Running a function that might fail with a retry mechanism...")
    try:
        result = might_fail(fail_chance=0.8) 
        print(f"Function completed with result: {result}")
    except ValueError as e:
        print(f"Function failed after all retries: {e}")

    print("\nUtility functions are ready.")
    print(f"Current timestamp (ms): {get_timestamp_ms()}")
