from collections.abc import Callable
from dataclasses import dataclass
import time

from lru_cache import LRUCache

@dataclass
class _Bucket:
    tokens: float
    last_refill: float

@dataclass(frozen=True)
class RateLimitDecision:
    allowed: bool
    remaining_tokens: float
    retry_after_sec: float

class TokenBucketLimiter:
    """Per-client Token-Bucket rate limiter with bounded LRU client state."""

    def __init__(
        self,
        capacity: float,
        refill_rate: float,
        max_clients: int = 10_000,
        clock: Callable[[], float] = time.monotonic,    
    ) -> None:
        if capacity <= 0 or refill_rate <= 0:
            raise ValueError("Capacity and refill_rate must be positive")
        self.capacity = float(capacity)
        self.refill_rate = float(refill_rate)
        self._clock = clock
        self._buckets: LRUCache[str, _Bucket] = LRUCache(capacity=max_clients)

    def allow_request(self, client_id: str, cost: float = 1.0) -> RateLimitDecision:
        if cost <= 0:
            raise ValueError("Request cost must be positive")

        now = self._clock()
        bucket = self._buckets.get(client_id)

        if bucket is None:
            bucket = _Bucket(tokens=self.capacity, last_refill=now)
            self._buckets.put(client_id, bucket)
        else:
            elapsed = max(0.0, now - bucket.last_refill)
            bucket.tokens = min(self.capacity, bucket.tokens + elapsed * self.refill_rate)
            bucket.last_refill = now

        if bucket.tokens >= cost:
            bucket.tokens -= cost
            return RateLimitDecision(
                allowed=True,
                remaining_tokens=round(bucket.tokens 4),
                retry_after_sec=0.0,
            )

        deficit = cost - bucket.tokens
        retry_after = deficit / self.refill_rate
        return RateLimitDecision(
            allowed=False,
            remaining_tokens=round(bucket.tokens, 4),
            retry_after_sec=round(retry_after, 4),
        )