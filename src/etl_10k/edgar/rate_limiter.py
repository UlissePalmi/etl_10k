"""
Thread-safe rate limiter for coordinating SEC download requests across multiple workers.

This module implements a token bucket algorithm to ensure all download workers
respect the SEC's 10 requests/second rate limit while maximizing throughput.
"""

import threading
import time


class TokenBucketRateLimiter:
    """
    Thread-safe token bucket rate limiter for coordinating requests across workers.

    The token bucket algorithm maintains a bucket of tokens that regenerate at a fixed
    rate. Each request consumes one token. If no tokens are available, the request
    blocks until a token becomes available.

    This ensures a global rate limit is respected across all threads, preventing
    simultaneous request bursts that could violate rate limits.

    Example:
        # Create a shared rate limiter for all download workers
        rate_limiter = TokenBucketRateLimiter(rate=9.5, capacity=10.0)

        # In each worker thread:
        rate_limiter.acquire()  # Blocks if rate limit reached
        make_request()  # Safe to proceed
    """

    def __init__(self, rate: float, capacity: float = None):
        """
        Initialize the token bucket rate limiter.

        Args:
            rate: Requests per second (e.g., 9.5 for SEC with safety margin)
            capacity: Maximum token bucket size. Defaults to rate if not specified.
                     Higher capacity allows brief bursts when bucket is full.
        """
        self.rate = rate
        self.capacity = capacity if capacity is not None else rate
        # Start with 1 token to allow first request immediately while preventing startup burst
        self.tokens = 1.0
        self.last_update = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self, tokens: float = 1.0):
        """
        Acquire token(s) before making a request.

        This method blocks if insufficient tokens are available until they regenerate.
        Tokens regenerate continuously at the configured rate.

        Args:
            tokens: Number of tokens to acquire (default: 1.0 for one request)

        Thread-safety:
            Uses a lock to coordinate token accounting across threads. The lock is
            released during sleep to avoid blocking other threads unnecessarily.
        """
        while True:
            with self.lock: # Ensures that only 1 thread enters the function per time, aka 1 token given to only 1 thread  
                now = time.monotonic()
                elapsed = now - self.last_update

                # Regenerate tokens based on elapsed time
                # Cap at capacity to prevent unbounded accumulation
                self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
                self.last_update = now

                # If enough tokens available, consume and return
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return

                # Calculate how long to wait for next token
                deficit = tokens - self.tokens
                sleep_time = deficit / self.rate

            # Sleep outside the lock to avoid blocking other threads
            time.sleep(sleep_time)
