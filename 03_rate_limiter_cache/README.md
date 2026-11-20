# Project 03: 0(1) LRU Cache & Bounded Token-Bucket API Rate Limiter

A zero-dependency API gateway traffic-shaping library implementing a strict $0(1)$ Least-Recently-Used cache and a per-client Token-Bucket rate limiter.

## Key Engineering Concepts
- **Strict $0(1)$ `LRUCache` (`src/lru_cache.py`):** Combines a Python dictionary with sentinel-bounded doubly-linked list nodes (`_Node` with `__slots__`) for $0(1)$ lookup, promotion, insertion, and eviction.
- **Token-Bucket Traffic Shaping (`src/rate_limiter.py`):** Supports configurable burst capacity, continuous fractional token replenishment via `time.monotonic`, and exact `retry_after_sec` backoff hints.
- **Bounded Gateway Memory:** Stores per-client rate_limit buckets inside ``LRUCache(max_clients)` to prevent memory exhaustion during high-cardinality client floods.
- **Deterministic Clock Injection:** Uses an injectable `clock` callable to test sub-second token replenishment without `time.sleep()`.

## Usage
```bash
# Execute unit and gateway integration tests
pytest -v
