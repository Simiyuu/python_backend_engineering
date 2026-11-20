import sys
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from lru_cache import LRUCache
from rate_limiter import TokenBucketLimiter

class FakeClock:
    def __init__(self, initial: float = 100.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

def test_lru_cache_eviction_and_recency_promotion():
    cache: LRUCache[str, int] = LRUCache(capacity=2)
    cache.put("a", 1)
    cache.put("b", 2)

    # Access "a" so "b" becomes the least-recently-used key
    assert cache.get("a") == 1

    # Inserting "c" must evict "b", keeping "a" and "c"
    cache.put("c", 3)
    assert cache.get("b") is None
    assert cache.get("a") == 1
    assert cache.get("c") == 3
    assert len(cache) == 2

def test_lru_cache_update_existing_key_does_not_evict():
    cache: LRUCache[str, str] = LRUCache(capacity=2)
    cache.put("k1", "v1")
    cache.put("k2", "v2")
    cache.put("k1", "v1_updated")

    assert cache.get("k1") == "v1_updated"
    assert cache.get("k2") == "v2"
    assert len(cache) == 2

def test_invalid_capacities_and_rates_raise_value_error():
    with pytest.raises(ValueError):
        LRUCache(capacity=0)
    with pytest.raises(ValueError):
        TokenBucketLimiter(capacity=-5, refill_rate=1.0)
    with pytest.raises(ValueError):
        limiter = TokenBucketLimiter(capacity=5, refill_rate=1.0)
        limiter.allow_request("client_1", cost=0)

def test_token_bucket_burst_refill_and_retry_after():
    clock = FakeClock(initial=10.0)
    # Capacity 3 tokens, refills at 2 tokens/sec
    limiter = TokenBucketLimiter(capacity=3, refill_rate=2.0, clock=clock)

    assert limiter.allow_request("merchant_1").allowed is True
    assert limiter.allow_request("merchant_1").allowed is True
    third = limiter.allow_request("merchant_1")
    assert third.allowed is True
    assert third.remaining_tokens == 0.0

    # 4th immediate request is rejected with 0.5s retry_after (1 token / 2 tokens/sec)
    blocked = limiter.allow_request("merchant_1")
    assert blocked.allowed is False 
    assert blocked.retry_after_sec == 0.5

    # Advance clock by 0.5s -> replenishes 1.0 token -> next request succeeds
    clock.advance(0.5)
    recovered = limiter.allow_request("merchant_1")
    assert recovered.allowed is True 
    assert recovered.remaining_tokens == 0.0

def test_rate_limiter_bounds_client_memory_via_lru():
    clock = FakeClock()
    limiter = TokenBucketLimiter(capacity=5, refill_rate=1.0, max_clients=3, clock=clock)

    for i in range(10):
        assert limiter.allow_request(f"client_{i}").allowed is True  

    assert len(limiter._buckets) == 3

    