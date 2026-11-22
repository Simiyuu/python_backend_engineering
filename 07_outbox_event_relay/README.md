# Ptoject 07: Transactional Outbox Relay & Idempotent Event Consumer

## Overview
Reliable asynchronous evenyt publisher implementing the **Transactional Outbox Pattern** alongside an **Idempotent Consumer** to solve dual-write consistency between databse transactions and message brokers.

## Architecture & Guarantees
- **Idempotent Outbox Append:** Deduplicates event writes by `event_id` within the local outbox store.
- **Lease-Based Worker Coordination:** `claim_batch()` transistions due `PENDING` events to `IN_FLIGHT` with a bounded lease window. If a worker crashes mid-flight, expired leases (`lease_expires_at <=now`) are automatically reclaimed by healthy workers.
- **Exponential Retry & Dead-Letter Queue (DLQ):** Failed deliveries back off exponentially (`base_backoff_sec * 2**(attempts - 1)`). Events exceeding `max_attempts` transition to `DEAD_LETTERED` so poison-pill messages never block healthy queue throughput.
- **Effectively-Once Processing:** Combines at-least-once relay delivery with `IdempotentEventConsumer` deduplication (`processed_ids`) so crash-induced redeliveries handlers once.

## Usage
```bash
pytest -v
