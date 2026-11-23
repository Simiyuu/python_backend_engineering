from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from outbox_relay import (
    IdempotentEventConsumer,
    OutboxEvent,
    OutboxRelayWorker,
    OutboxStatus,
    OutboxStore,
)

class FakeClock:
    def __init__(self, initial: float = 100.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now

    def advance(self,seconds: float) -> None:
        self.now += seconds

def test_outbox_publishes_fifo_batch_and_deduplicates_append() -> None:
    clock = FakeClock()
    store = OutboxStore(clock=clock)
    published_ids: list[str] = []

    e1 = store.append_event("evt_1", "txn_101", "PAYMENT_SETTLED", {"amount": "500.00"})
    e1_dup = store.append_event("evt_1", "txn_101", "PAYMENT_SETTLED", {"amount": "999.00"})
    store.append_event("evt_2", "txn_102", "PAYMENT_SETTLED", {"amount": "250.00"})

    assert e1 is e1_dup
    assert e1.payload == {"amount": "500.00"}

    worker = OutboxRelayWorker(store=store, publisher=lambda ev: published_ids.append(ev.event_id))
    count = worker.poll_once()

    assert count == 2
    assert published_ids == ["evt_1", "evt_2"]
    assert store.get("evt_1").status == OutboxStatus.PUBLISHED 
    assert store.get("evt_2").status == OutboxStatus.PUBLISHED
    assert worker.metrics.published == 2

def test_transient_failures_use_exponential_backoff_before_succeeding() -> None:
    clock = FakeClock(initial=100.0)
    store = OutboxStore(clock=clock)
    store.append_event("evt_flaky", "txn_201", "DISBURSEMENT_SENT", {"mission": 254700000001})

    call_count = {"n": 0}

    def flaky_broker(event: OutboxEvent) -> None:
        call_count["n"] += 1
        if call_count["n"] <= 2:
            raise ConnectionError("Kafka broker timeout")

    worker = OutboxRelayWorker(
        store=store,
        publisher=flaky_broker,
        max_attempts=4,
        base_backoff_sec=5.0,
    )

    # Attempt 1 at t=100 fails -> next_attempt_at = 100 + 5*2^0 = 105
    assert worker.poll_once() == 0
    ev = store.get("evt_flaky")
    assert ev.status == OutboxStatus.PENDING  
    assert ev.attempts == 1
    assert ev.next_attempt_at == pytest.approx(105.0)

    # Polling at t=104 does not re-claim before backoff expires
    clock.advance(4.0)
    assert worker.poll_once() == 0
    assert call_count["n"] == 1

    # Attempt 2 at t=105 fails -> next_attempt_at = 105 + 5*2^1 = 115
    clock.advance(1.0)
    assert worker.poll_once() == 0
    assert ev.attempts == 2
    assert ev.next_attempt_at == pytest.approx(115.0)
    
    # Attempt 3 at t=115 succeeds
    clock.advance(10.0)
    assert worker.poll_once() == 1
    assert ev.status == OutboxStatus.PUBLISHED 
    assert ev.published_at == pytest.approx(115.0)
    assert worker.metrics.retried == 2
    assert worker.metrics.published == 1

def test_poison_pill_moves_to_dead_letter_without_blocking_healthy_events() -> None:
    clock = FakeClock(initial=10.0)
    store = OutboxStore(clock=clock)
    store.append_event("evt_poison", "txn_bad", "MALFORMED_EVENT", {})

    delivered: list[str] = []

    def selective_broker(event: OutboxEvent) -> None:
        if event.event_id == "evt_poison":
            raise ValueError("Schema validation rejected by broker")
        delivered.append(event.event_id)

    worker = OutboxRelayWorker(
        store=store,
        publisher=selective_broker,
        max_attempts=3,
        base_backoff_sec=2.0,
    )

    # Fail 3 times: t=10 (next=12), t=12 (next=16), t=16 (DEAD_LETTERED)
    worker.poll_once()
    clock.advance(2.0)
    worker.poll_once()
    clock.advance(4.0)
    worker.poll_once()

    poison = store.get("evt_poison")
    assert poison.status == OutboxStatus.DEAD_LETTERED
    assert poison.attempts == 3
    assert "Schema validation" in (poison.last_error or "")
    assert worker.metrics.dead_lettered == 1

    # Healthy event queued afterward publishes cleanly
    store.append_event("evt_healthy", "txn_ok", "LEDGER_POSTED", {"amount": "100.00"})
    assert worker.poll_once() == 1
    assert delivered == ["evt_healthy"]

def test_expired_in_flight_lease_is_reclaimed_after_worker_crash() -> None:
    clock = FakeClock(initial=50.0)
    store = OutboxStore(clock=clock)
    store.append_event("evt_crash", "txn_301", "REVERSAL_COMPLETED", {"amount": "75.00"})

    # Worker 1 claims batch with 10s lease and crashes before acknowledging
    claimed, reclaimed = store.claim_batch(batch_size=5, lease_duration_sec=10.0)
    assert len(claimed) == 1
    assert reclaimed == 0
    assert store.get("evt_crash").status == OutboxStatus.IN_FLIGHT 

    delivered: list[str] = []
    worker_2 = OutboxRelayWorker(
        store=store,
        publisher=lambda ev: delivered.append(ev.event_id),
        lease_duration_sec=10.0,
    )

    # Before lease expiry (t=59), Worker 2 must not steal the active lease
    clock.advance(9.0)
    assert worker_2.poll_once() == 0
    assert delivered == []

    # At lease expiry (t=60), Worker 2 reclaims and completes delivery
    clock.advance(1.0)
    assert worker_2.poll_once() == 1
    assert delivered == ["evt_crash"]
    assert worker_2.metrics.reclaimed_leases == 1
    assert store.get("evt_crash").status == OutboxStatus.PUBLISHED

def test_at_least_once_relay_with_idempotent_consumer_executes_once() -> None:
    clock = FakeClock(initial=10.0)
    store = OutboxStore(clock=clock)
    store.append_event("evt_dedup", "txn_401", "WALLET_CREDITED", {"amount": "1200.00"})

    credited_amounts: list[str] = []
    consumer = IdempotentEventConsumer(
        handler=lambda ev: credited_amounts.append(ev.payload["amount"])
    )

    # Worker 1 delivers to consumer, then crashes before marking PUBLISHED in OutboxStore
    def crashing_after_send_publisher(event: OutboxEvent) -> None:
        consumer.consume(event)
        raise RuntimeError("Worker lost DB connection before ack")

    worker_1 = OutboxRelayWorker(
        store=store,
        publisher=crashing_after_send_publisher,
        max_attempts=3,
        base_backoff_sec=5.0,
    )
    assert worker_1.poll_once() == 0
    assert credited_amounts == ["1200.00"]

    # Worker 2 retires after backoff; consumer deduplicates the redelivery
    clock.advance(5.0)
    worker_2 = OutboxRelayWorker(
        store=store,
        publisher=lambda ev: consumer.consume(ev),
        max_attempts=3,
        base_backoff_sec=5.0,
    )
    assert worker_2.poll_once() == 1
    assert credited_amounts == ["1200.00"]
    assert consumer.processed_count == 1
    assert consumer.duplicates_ignored == 1
    assert store.get("evt_dedup").status == OutboxStatus.PUBLISHED
