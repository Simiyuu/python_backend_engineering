from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from circuit_breaker import (
    CircuitBreaker,
    CircuitOpenError,
    CircuitState,
    ResilientGatewayClient,
    UpstreamClientError,
    UpstreamTransientError,
)

class FakeClock:
    def __init__(self, initial: float = 100.0) -> None:
        self.now = initial

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

def test_circuit_breaker_opens_after_threshold_and_fast_fails() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(failure_threshold=3, recovery_timeout_sec=15.0, clock=clock)
    client = ResilientGatewayClient(circuit_breaker=breaker, max_retries=0)

    def failing_op() -> str:
        raise UpstreamTransientError("503 Service Unavailable")

    for _ in range(3):
        with pytest.raises(UpstreamTransientError):
            client.execute(failing_op)

    assert breaker.state == CircuitState.OPEN

    clock.advance(5.0)
    with pytest.raises(CircuitOpenError) as exc_info:
        client.execute(failing_op)

    assert exc_info.value.retry_after_sec == pytest.approx(10.0)
    assert client.metrics.total_attempts == 3
    assert client.metrics.short_circuited_calls == 1

def test_half_open_probe_reopens_on_failure_and_closes_on_success() -> None:
    clock = FakeClock()
    breaker = CircuitBreaker(
        failure_threshold=2,
        recovery_timeout_sec=10.00,
        half_open_max_calls=2,
        clock=clock,
    )
    client = ResilientGatewayClient(circuit_breaker=breaker, max_retries=0)

    for _ in range(2):
        with pytest.raises(UpstreamTransientError):
            client.execute(lambda: (_ for _ in ()).throw(UpstreamTransientError("502 Bad Gateway")))

    assert breaker.state == CircuitState.OPEN

    # Advance past recovery timeout -> transitions to HALF_OPEN
    clock.advance(10.0)
    assert breaker.state == CircuitState.HALF_OPEN

    # Failed probe trips immediately back to OPEN
    with pytest.raises(UpstreamTransientError):
        client.execute(lambda: (_ for _ in ()).throw(UpstreamTransientError("504 Gateway Timeout")))
    assert breaker.state == CircuitState.OPEN


    # Advance again and pass both required HALF_OPEN probes -> closes circuit
    clock.advance(10.0)
    assert breaker.state == CircuitState.HALF_OPEN
    assert client.execute(lambda: "probe_1_ok") == "probe_1_ok"
    assert breaker.state == CircuitState.HALF_OPEN
    assert client.execute(lambda: "probe_2_ok") == "probe_2_ok"
    assert breaker.state == CircuitState.CLOSED
    assert breaker.failure_count == 0

def test_client_errors_do_not_trip_breaker_or_trigger_retries() -> None:
    breaker = CircuitBreaker(failure_threshold=2)
    sleep_calls: list[float] = []
    client = ResilientGatewayClient(
        circuit_breaker=breaker,
        max_retries=3,
        sleeper=sleep_calls.append,
    )

    for _ in range(5):
        with pytest.raises(UpstreamClientError):
            client.execute(lambda: (_ for _ in ()).throw(UpstreamClientError("400 Invalid MSISDN")))

    assert breaker.state == CircuitState.CLOSED
    assert breaker.failure_count == 0
    assert client.metrics.total_attempts == 5
    assert client.metrics.total_retries == 0
    assert sleep_calls == []

def test_exponential_backoff_with_full_jitter_respects_caps() -> None:
    sleep_calls: list[float] = []
    recorded_bounds: list[tuple[float, float]] = []

    def midpoint_rng(low: float, high: float) -> float:
        recorded_bounds.append((low, high))
        return (low + high) / 2.0

    breaker = CircuitBreaker(failure_threshold=10)
    client = ResilientGatewayClient(
        circuit_breaker=breaker,
        max_retries=4,
        base_delay_sec=0.5,
        max_delay_sec=3.0,
        sleeper=sleep_calls.append,
        rng=midpoint_rng,
    )

    with pytest.raises(UpstreamTransientError):
        client.execute(lambda: (_ for _ in ()).throw(UpstreamTransientError("503 Unavailable")))

    # Caps per attempt: 0.5*2^0=0.5, 0.5*2^1=1.0, 0.5*2^2=2.0, 0.5*2^3=4.0 -> capped at 3.0
    assert recorded_bounds == [(0.0, 0.5), (0.0, 1.0), (0.0, 2.0), (0.0, 3.0)]
    assert sleep_calls ==[0.25, 0.5, 1.0, 1.5]
    assert client.metrics.total_attempts == 5
    assert client.metrics.total_retries == 4

def test_resilient_client_recovers_from_transient_blip_and_halts_when_circuit_trips() -> None:
    sleep_calls: list[float] = []
    breaker = CircuitBreaker(failure_threshold=2, recovery_timeout_sec=20.0)
    client = ResilientGatewayClient(
        circuit_breaker=breaker,
        max_retries=4,
        base_delay_sec=0.1,
        max_delay_sec=1.0,
        sleeper=sleep_calls.append,
        rng=lambda low, high: high,
    )

    # Case 1: 1 transient failure followed by success recovers cleanly
    attempts = {"count": 0}

    def flaky_then_ok() -> str:
        attempts["count"] += 1
        if attempts["count"] == 1:
            raise TimeoutError("Read timed out")
        return "200 OK"

    assert client.execute(flaky_then_ok) == "200 OK"
    assert breaker.state == CircuitState.CLOSED  
    assert breaker.failure_count == 0
    assert sleep_calls == [0.1]

    # CASE 2: Once failure_threshold (2) is hit during retries, stop retrying immediately
    sleep_calls.clear()
    with pytest.raises(UpstreamTransientError):
        client.execute(lambda: (_ for _ in ()).throw(UpstreamTransientError("503 Down")))

    assert breaker.state == CircuitState.OPEN
    # Attempt 0 failes (failure_count=1 -> sleeps 0.1s), Attempt 1 fails (failure_count=2 -> OPEN -> halts!)
    assert sleep_calls == [0.1]
    