from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from webhook_dispatcher import (
    DeliveryStatus,
    WebhookDispatcher,
    WebhookEndpoint,
    sign_payload,
    verify_signature,
)

class FakeClock:
    def __init__(self, initial: float = 1000.0) -> None:
        self.now = initial

    def advance(self, seconds: float) -> None:
        self.now += seconds

    def __call__(self) -> float:
        return self.now

def test_hmac_signature_verification_and_replay_protection() -> None:
    secret = "whsec_live_9911"
    payload = '{"amount":1500,"txn_id":"MPESA_88"}'
    ts = 1700000000

    sig = sign_payload(secret, ts, payload)
    assert sig.startswith("sha256=")

    # Valid within 300s window
    assert verify_signature(secret, ts, payload, sig, now_sec=1700000120.0, max_age_sec=300.0) is True
    # Rejected outside replay window
    assert verify_signature(secret, ts, payload, sig, now_sec=1700000301.0, max_age_sec=300.0) is False
    # Rejected on tampered payload or wrong secret
    assert verify_signature(secret, ts, '{"amount":9999}', sig, now_sec=1700000010.0) is False
    assert verify_signature("wrong_secret", ts, payload, sig, now_sec=1700000010.0) is False

def test_fanout_signs_headers_and_skips_inactive_endpoint() -> None:
    clock = FakeClock(initial=1000.0)
    received: list[tuple[str, str, dict[str, str]]] = []

    def transport(ep: WebhookEndpoint, body: str, headers: dict[str, str]) -> int:
        received.append((ep.endpoint_id, body, headers))
        return 200

    dispatcher = WebhookDispatcher(transport=transport, clock=clock)
    dispatcher.register_endpoint(WebhookEndpoint("ep_active", "https://merchant-a.test/ipn", "sec_a", active=True))
    dispatcher.register_endpoint(WebhookEndpoint("ep_paused", "https://merchant-b.test/ipn", "sec_b", active=False))

    deliveries = dispatcher.enqueue_event("evt_100", "PAYMENT_RECEIVED", {"b": 2, "a": 1})
    assert len(deliveries) == 1
    assert deliveries[0].endpoint_id == "ep_active"

    assert dispatcher.dispatch_due() == 1
    assert len(deliveries) == 1

    ep_id, body, headers = received[0]
    assert ep_id == "ep_active"
    assert body == '{"a":1,"b":2}'
    assert verify_signature(
        secret="sec_a",
        timestamp=int(headers["X-Webhook-Timestamp"]),
        payload_json=body,
        signature_header=headers["X-Webhook-Signature"],
        now_sec=1000.0
    ) is True

def test_transient_503_uses_exponential_backoff_before_succeeding() -> None:
    clock = FakeClock(initial=500.0)
    calls = {"n": 0}

    def flaky_transport(ep: WebhookEndpoint, body: str, headers: dict[str, str]) -> int:
        calls["n"] += 1
        return 503 if calls["n"] <= 2 else 202

    dispatcher = WebhookDispatcher(
        transport=flaky_transport,
        clock=clock,
        max_attempts=4,
        base_backoff_sec=5.0,
    )
    dispatcher.register_endpoint(WebhookEndpoint("ep_1", "https://shop.test/webhook", "sec_1"))

    delivery = dispatcher.enqueue_event("evt_flaky", "C2B_CONFIRM", {"amount": 500})[0]

    # Attempt 1 at t=500 fails -> next_attempt_at = 500 + 5*2^0 = 505
    assert dispatcher.dispatch_due() == 0
    assert delivery.status == DeliveryStatus.PENDING
    assert delivery.attempts == 1
    assert delivery.next_attempt_at == pytest.approx(505.0)

    # Before backoff expires (t=504), nothing is dispatched
    clock.advance(4.0)
    assert dispatcher.dispatch_due() == 0
    assert calls["n"] == 1

    # Attempt 2 at t=505 fails -> next_attempt_at = 505 + 5*2^1 = 515
    clock.advance(1.0)
    assert dispatcher.dispatch_due() == 0
    assert delivery.attempts == 2
    assert delivery.next_attempt_at == pytest.approx(515.0)

    # Attempt 3 at t=515 succeeds
    clock.advance(10.0)
    assert dispatcher.dispatch_due() == 1
    assert delivery.status == DeliveryStatus.DELIVERED
    assert delivery.attempts == 3
    assert delivery.last_status_code == 202

def test_dead_letter_isolates_failing_endpoint_without_blocking_healthy_one() -> None:
    clock = FakeClock(initial=200.0)

    def selective_transport(ep: WebhookEndpoint, body: str, headers: dict[str, str]) -> int:
        if ep.endpoint_id == "ep_down":
            raise ConnectionError("DNS resolution failed")
        return 200

    dispatcher = WebhookDispatcher(
        transport=selective_transport,
        clock=clock,
        max_attempts=3,
        base_backoff_sec=2.0,
    )
    dispatcher.register_endpoint(WebhookEndpoint("ep_down", "https://down.test/ipn", "sec_down"))
    dispatcher.register_endpoint(WebhookEndpoint("ep_healthy", "https://ok.test/ipn", "sec_ok"))

    deliveries = dispatcher.enqueue_event("evt_fanout", "B2C_RESULT", {"ok": True})
    assert len(deliveries) == 2

    # t=200: healthy succeeds (1), down fails attempt 1 (next=202)
    assert dispatcher.dispatch_due() == 1

    # t=202: down fails attempt 2 (next=206)
    clock.advance(2.0)
    assert dispatcher.dispatch_due() == 0

    # t=206: down fails attempt 3 -> DEAD_LETTERED
    clock.advance(4.0)
    assert dispatcher.dispatch_due() == 0

    down_delivery = dispatcher.get_delivery("ep_down:evt_fanout")
    healthy_delivery = dispatcher.get_delivery("ep_healthy:evt_fanout")

    assert healthy_delivery.status == DeliveryStatus.DELIVERED
    assert down_delivery.status == DeliveryStatus.DEAD_LETTERED
    assert down_delivery.attempts == 3
    assert "DNS resolution failed" in (down_delivery.last_error or "")
    assert len(dispatcher.attempt_logs("ep_down:evt_fanout")) == 3

def test_enqueue_event_is_idempotent_and_validation_guards_reject_bad_inputs() -> None:
    dispatcher = WebhookDispatcher(transport=lambda *_: 200)
    dispatcher.register_endpoint(WebhookEndpoint("ep_1", "https://ok.test/ipn", "sec_1"))

    first = dispatcher.enqueue_event("evt_dup", "PAYMENT", {"amt": 100})
    second = dispatcher.enqueue_event("evt_dup", "PAYMENT", {"amt": 100})
    assert first[0] is second[0]

    with pytest.raises(ValueError, match="endpoint_id"):
        WebhookEndpoint("", "https://ok.test/ipn", "sec_1")

    with pytest.raises(ValueError, match="max_attempts"):
        WebhookDispatcher(transport=lambda *_:200, max_attempts=0)

    with pytest.raises(ValueError, match="base_backoff_sec"):
        WebhookDispatcher(transport=lambda *_:200, base_backoff_sec=0.0)
    
    with pytest.raises(ValueError, match="event_id"):
        dispatcher.enqueue_event("  ", "PAYMENT", {})
