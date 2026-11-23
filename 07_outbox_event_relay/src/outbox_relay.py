from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
import time
from typing import Any

class OutboxStatus(Enum):
    PENDING = "PENDING"
    IN_FLIGHT = "IN_FLIGHT"
    PUBLISHED = "PUBLISHED"
    DEAD_LETTERED = "DEAD_LETTERED"

@dataclass
class OutboxEvent:
    event_id: str
    aggregate_id: str
    event_type: str
    payload: dict[str, Any]
    created_at: float
    status: OutboxStatus = OutboxStatus.PENDING
    attempts: int = 0
    next_attempt_at: float = 0.0
    lease_expires_at: float | None = None
    published_at: float | None = None
    last_error: str | None = None

@dataclass
class RelayMetrics:
    polled: int = 0
    published: int = 0
    retried: int = 0
    dead_lettered: int = 0
    reclaimed_leases: int = 0

class OutboxStore:
    """In-memory transactional outbox store with lease-based batch claiming."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._events: dict[str, OutboxEvent] = {}

    def append_event(
        self,
        event_id: str,
        aggregate_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> OutboxEvent:
        """Persist an outbox event idempotently by event_id."""
        if not event_id.strip():
            raise ValueError("event_id cannot be empty")
        if not aggregate_id.strip():
            raise ValueError("aggregate_id cannot be empty")
        if not event_type.strip():
            raise ValueError("event_type cannot be empty")

        if event_id in self._events:
            return self._events[event_id]

        now = self._clock()
        event = OutboxEvent(
            event_id=event_id,
            aggregate_id=aggregate_id,
            event_type=event_type,
            payload=dict(payload),
            created_at=now,
            next_attempt_at=now,
        )
        self._events[event_id] = event
        return event

    def get(self, event_id: str) -> OutboxEvent:
        if event_id not in self._events:
            raise KeyError(f"Unknown outbox event '{event_id}'")
        return self._events[event_id]

    def list_by_status(self, status: OutboxStatus) -> list[OutboxEvent]:
        return [ev for ev in self._events.values() if ev.status == status]

    def claim_batch(
        self,
        batch_size: int = 10,
        lease_duration_sec: float = 30.0,
    ) -> tuple[list[OutboxEvent], int]:
        """Claim eligible PENDING events and expired IN_FLIGHT leases in FIFO order."""
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        if lease_duration_sec <= 0:
            raise ValueError("lease_duration_sec must be > 0")

        now = self._clock()
        claimed: list[OutboxEvent] = []
        reclaimed_count = 0

        for event in self._events.values():
            if len(claimed) >= batch_size:
                break
            
            is_due_pending = (
                event.status == OutboxStatus.PENDING and event.next_attempt_at <= now
            )
            is_expired_lease = (
                event.status == OutboxStatus.IN_FLIGHT
                and event.lease_expires_at is not None
                and event.lease_expires_at <= now
            )

            if is_due_pending or is_expired_lease:
                if is_expired_lease:
                    reclaimed_count += 1
                event.status = OutboxStatus.IN_FLIGHT
                event.lease_expires_at = now + lease_duration_sec
                claimed.append(event)

        return claimed, reclaimed_count

    def mark_published(self, event_id: str) -> None:
        event = self.get(event_id)
        event.status = OutboxStatus.PUBLISHED
        event.published_at = self._clock()
        event.lease_expires_at = None
        event.last_error = None

    def mark_failed(
        self,
        event_id: str,
        error_message: str,
        max_attempts: int = 3,
        base_backoff_sec: float = 1.0,
    ) -> OutboxStatus:
        event = self.get(event_id)
        now = self._clock()
        event.attempts += 1
        event.last_error = error_message
        event.lease_expires_at = None

        if event.attempts >= max_attempts:
            event.status = OutboxStatus.DEAD_LETTERED
        else:
            event.status = OutboxStatus.PENDING
            delay = base_backoff_sec * (2 ** (event.attempts - 1))
            event.next_attempt_at = now + delay

        return event.status

class OutboxRelayWorker:
    """Polls due events from OutboxStore and publishes them to a downstream broker."""

    def __init__(
        self,
        store: OutboxStore,
        publisher: Callable[[OutboxEvent], None],
        batch_size: int = 10,
        lease_duration_sec: float = 30.0,
        max_attempts: int = 3,
        base_backoff_sec: float = 1.0,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if base_backoff_sec <= 0:
            raise ValueError("base_backoff_sec must be > 0")

        self.store = store
        self._publisher = publisher
        self.batch_size = batch_size
        self.lease_duration_sec = lease_duration_sec
        self.max_attempts = max_attempts
        self.base_backoff_sec = base_backoff_sec
        self.metrics = RelayMetrics()

    def poll_once(self) -> int:
        """Claim and process one batch of sue outbox events; returns published count."""
        batch, reclaimed = self.store.claim_batch(
                batch_size=self.batch_size,
                lease_duration_sec=self.lease_duration_sec,
            )
        self.metrics.polled += len(batch)
        self.metrics.reclaimed_leases += reclaimed

        published_in_batch = 0
        for event in batch:
            try:
                self._publisher(event)
            except Exception as exc:
                new_status = self.store.mark_failed(
                    event_id=event.event_id,
                    error_message=str(exc),
                    max_attempts=self.max_attempts,
                    base_backoff_sec=self.base_backoff_sec,
                )
                if new_status == OutboxStatus.DEAD_LETTERED:
                    self.metrics.dead_lettered += 1
                else:
                    self.metrics.retried += 1
            else:
                self.store.mark_published(event.event_id)
                self.metrics.published += 1
                published_in_batch += 1

        return published_in_batch

class IdempotentEventConsumer:
    """Deduplicates at-least-once broker deliveries by event_id."""

    def __init__(self, handler: Callable[[OutboxEvent], None]) -> None:
        self._handler = handler
        self._processed_ids: set[str] = set()
        self.duplicates_ignored = 0

    @property
    def processed_count(self) -> int:
        return len(self._processed_ids)

    def consume(self, event: OutboxEvent) -> bool:
        """Execute handler only once per event_id; returns True if newly processed."""
        if event.event_id in self._processed_ids:
            self.duplicates_ignored += 1
            return False

        self._handler(event)
        self._processed_ids.add(event.event_id)
        return True

        