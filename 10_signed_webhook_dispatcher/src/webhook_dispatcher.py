from collections.abc import Callable 
from dataclasses import dataclass 
from enum import Enum 
import hashlib 
import hmac 
import json 
import time 
from typing import Any

class DeliveryStatus(Enum):
    PENDING = "PENDING"
    DELIVERED = "DELIVERED"
    DEAD_LETTERED = "DEAD_LETTERED"

@dataclass(frozen=True)
class WebhookEndpoint:
    endpoint_id: str
    url: str
    secret: str
    active: bool = True

    def __post_init__(self) -> None:
        if not self.endpoint_id.strip():
            raise ValueError("endpoint_id cannot be empty")
        if not self.url.strip():
            raise VlaueError("url cannot be empty")
        if not self.secret.strip():
            raise ValueError("secret cannot be empty")

@dataclass
class WebhookDelivery:
    delivery_id: str
    endpoint_id: str
    event_id: str
    event_type: str
    payload_json: str
    created_at: float
    status: DeliveryStatus = DeliveryStatus.PENDING 
    attempts: int = 0
    next_attempt_at: float = 0.0
    delivered_at: float | None = None
    last_status_code: int | None = None
    last_error: str | None = None

@dataclass(frozen=True)
class AttemptLog:
    delivery_id: str 
    endpoint_id: str 
    attempt_number: int
    timestamp: float
    success: bool
    status_code: int | None
    error: str | None = None

def sign_payload(secret: str, timestamp: int, payload_json: str) -> str:
    """Compute hex HMAC-SHA256 signature over '<timestamp>.<payload_json>'."""
    message = f"{timestamp}.{payload_json}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()
    return f"sha256={digest}"

def verify_signature(
    secret: str,
    timestamp: int,
    payload_json: str,
    signature_header: str,
    now_sec: float,
    max_age_sec: float = 300.0,
) -> bool:
    """Verify HMAC-SHA256 signatures and enforce anti-replay timestamp bounds."""
    if abs(now_sec - float(timestamp)) > max_age_sec:
        return False
    expected = sign_payload(secret=secret, timestamp=timestamp, payload_json=payload_json)
    return hmac.compare_digest(expected, signature_header)

class WebhookDispatcher:
    """Signs and dispatches merchant IPNwebhooks with retries and DLQ isolation."""

    def __init__(
        self, 
        transport: Callable[[WebhookEndpoin, str, dict[str, str]], int],
        clock: Callable[[], float] = time.time,
        max_attempts: int = 3,
        base_backoff_sec: float = 2.0,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if base_backoff_sec <= 0:
            raise ValueError("base_backoff_sec must be > 0")

        self._transport = transport
        self._clock = clock
        self.max_attempts = max_attempts
        self.base_backoff_sec = base_backoff_sec

        self._endpoints: dict[str, WebhookEndpoint] = {}
        self._deliveries: dict[str, WebhookDelivery] = {}
        self._idempotency_index: dict[tuple[str, str], str] = {}
        self._attempt_logs: list[AttemptLog] = []

    def register_endpoint(self, endpoint: WebhookEndpoint) -> None:
        self._endpoints[endpoint.endpoint_id] = endpoint

    def enqueue_event(
        self,
        event_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> list[WebhookDelivery]:
        """Fan out an event to active endpoints idempotently by (endpoint_id, event_id)."""
        if not event_id.strip():
            raise ValueError("event_id cannot be empty")
        if not event_type.strip():
            raise ValueError("event_type cannot be empty")

        now =self._clock()
        canonical_json = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        created: list[WebhookDelivery] = []
        
        for endpoint in self._endpoints.values():
            if not endpoint.active:
                continue

            key = (endpoint.endpoint_id, event_id)
            if key in self._idempotency_index:
                existing_id = self._idempotency_index[key]
                created.append(self._deliveries[existing_id])
                continue

            delivery_id = f"{endpoint.endpoint_id}:{event_id}"
            delivery = WebhookDelivery(
                delivery_id=delivery_id,
                endpoint_id=endpoint.endpoint_id,
                event_id=event_id,
                event_type=event_type,
                payload_json=canonical_json,
                created_at=now,
                next_attempt_at=now,
            )
            self._deliveries[delivery_id] = delivery
            self._idempotency_index[key] = delivery_id
            created.append(delivery)

        return created

    def get_delivery(self, delivery_id: str) -> WebhookDelivery:
        if delivery_id not in self._deliveries:
            raise KeyError(f"Unknown delivery '{delvery_id}'")
        return self._deliveries[delivery_id]

    def attempt_logs(self, delivery_id: str | None = None) -> list[AttemptLog]:
        if delivery_id is None:
            return list(self._attempt_logs)
        return [log for log in self._attempt_logs if log.delivery_id == delivery_id]

    def dispatch_due(self, limit: int = 50) -> int:
        """"Dispatch due PENDING webhook deliveries; returns count deliveredin this run."""
        if limit < 1:
            raise ValueError("limit must be >= 1")

        now = self._clock()
        due = [
            d
            for d in self._deliveries.values()
            if d.status == DeliveryStatus.PENDING and d.next_attempt_at <= now
        ][:limit]

        delivered_count = 0
        for delivery in due:
            endpoint = self._endpoints[delivery.endpoint_id]
            ts = int(now)
            signature = sign_payload(
                secret=endpoint.secret,
                timestamp=ts,
                payload_json=delivery.payload_json,
            )
            headers = {
                "Content-Type": "application/json",
                "X-Webhook-Id": delivery.delivery_id,
                "X-Webhook-Timestamp": str(ts),
                "X-Webhook-Signature": signature,
            }

            delivery.attempts += 1
            try:
                status_code = self._transport(endpoint, delivery.payload_json, headers)
            except Exception as exc:
                self._record_failure(
                    delivery=delivery,
                    now=now,
                    status_code=None,
                    error=str(exc),
                )
                continue

            delivery.last_status_code = status_code
            if 200 <= status_code < 300:
                delivery.status = DeliveryStatus.DELIVERED
                delivery.delivered_at = now
                delivery.last_error = None
                delivered_count += 1
                self._attempt_logs.append(
                    AttemptLog(
                        delivery_id=delivery.delivery_id,
                        endpoint_id=delivery.endpoint_id,
                        attempt_number=delivery.attempts,
                        timestamp=now,
                        success=True,
                        status_code=status_code,
                    )
                )
            else:
                self._record_failure(
                    delivery=delivery,
                    now=now,
                    status_code=status_code,
                    error=f"HTTP {status_code}",
                )

        return delivered_count

    def _record_failure(
        self,
        delivery: WebhookDelivery,
        now: float,
        status_code: int | None,
        error: str,
    ) -> None:
        delivery.last_status_code = status_code
        delivery.last_error = error
        self._attempt_logs.append(
            AttemptLog(
                delivery_id=delivery.delivery_id,
                endpoint_id=delivery.endpoint_id,
                attempt_number=delivery.attempts,
                timestamp=now,
                success=False,
                status_code=status_code,
                error=error,
            )
        )

        if delivery.attempts >= self.max_attempts:
            delivery.status = DeliveryStatus.DEAD_LETTERED
        else:
            delay = self.base_backoff_sec * (2 ** (delivery.attempts - 1))
            delivery.next_attempt_at = now + delay
            