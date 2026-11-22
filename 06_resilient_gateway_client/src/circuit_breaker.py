from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
import random
import time
from typing import TypeVar

T = TypeVar("T")

class CircuitState(Enum):
    CLOSED = "CLOSED"
    OPEN = "OPEN"
    HALF_OPEN = "HALF_OPEN"

class GatewayError(Exception):
    """Base exception for upstream gateway failures."""

class CircuitOpenError(GatewayError):
    """Raised when a call is rejected immediately because the circuit is OPEN."""
     
    def __init__(self, message: str, retry_after_sec: float = 0.0) -> None:
        super().__init__(message)
        self.retry_after_sec = max(0.0, retry_after_sec)


class UpstreamTransientError(GatewayError):
    """Retryable infrastructure failure (e.g., HTTP 502/503/504 or gateway timeout)."""

class UpstreamClientError(GatewayError):
    """Non-retryable caller error (e.g., HTTP 400/401/422) that must not trip the circuit."""

@dataclass
class GatewayMetrics:
    total_attempts: int = 0
    total_retries: int = 0
    successful_calls: int = 0
    short_circuited_calls: int = 0

class CircuitBreaker:
    """Three-state circuit breaker protecting downstream service dependencies."""

    def __init__(
        self,
        failure_threshold: int = 3,
        recovery_timeout_sec: float = 30.0,
        half_open_max_calls: int = 1,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_timeout_sec <= 0:
            raise ValueError("recovery_timeout_sec must be > 0")
        if half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be >= 1")

        self.failure_threshold = failure_threshold
        self.recovery_timeout_sec = recovery_timeout_sec
        self.half_open_max_calls = half_open_max_calls
        self._clock = clock
        
        self._state = CircuitState.CLOSED
        self._failure_count = 0
        self._opened_at: float | None = None
        self._half_open_in_flight = 0
        self._half_open_successes = 0

    @property
    def state(self) -> CircuitState:
        self._sync_state()
        return self._state

    @property
    def failure_count(self) -> inti:
        return self._failure_count

    def _sync_state(self) -> None:
        if self._state == CircuitState.OPEN and self._opened_at is not None:
            elapsed = self._clock() - self._opened_at
            if elapsed >= self.recovery_timeout_sec:
                self._state = CircuitState.HALF_OPEN  
                self._half_open_in_flight = 0
                self._half_open_successes = 0

    def _trip_open(self) -> None:
        self._state = CircuitState.OPEN
        self._opened_at = self._clock()
        self._half_open_in_flight = 0
        self._half_open_successes = 0

    def before_call(self) -> None:
        """Check whether a call is permitted; raises CircuitOpenError if blocked."""
        self._sync_state()

        if self._state == CircuitState.OPEN:
            elapsed = 0.0 if self._opened_at is None else self._clock() - self._opened_at
            remaining = max(0.0, self.recovery_timeout_sec - elapsed)
            raise CircuitOpenError(
                f"Circuit breaker is OPEN; retry after {remaining:.2f}s",
                retry_after_sec=remaining,
            )

            if self._state == CircuitState.HALF_OPEN:
                if self._half_open_in_flight >= self.half_open_max_calls:
                    raise CircuitOpenError(
                        "Circuit breaker is HALF_OPEN and probe capacity is saturated",
                        retry_after_sec=0.0,
                    )
                self._half_open_in_flight += 1

    def record_success(self) -> None:
        """Record a successful upstream execution."""
        if self._state == CircuitState.HALF_OPEN:
            self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
            self._half_open_successes += 1
            if self._half_open_successes >= self.half_open_max_calls:
                self._state = CircuitState.CLOSED
                self._failure_count = 0
                self._opened_at = None
        else:
            self._failure_count = 0

    def record_failure(self) -> None:
        """Record a transient infrastructure failure."""
        if self._state == CircuitState.HALF_OPEN:
            self._half_open_in_flight = max(0, self._half_open_in_flight - 1)
            self._trip_open()
        elif self._state == CircuitState.CLOSED:
            self._failure_count += 1
            if self._failure_count >= self.failure_threshold:
                self._trip_open()

    def record_ignored_error(self) -> None:
        """"Release HALF_OPEN probe capacity when a non-infrastructure 4xx error occurs."""
        if self._state == CircuitState.HALF_OPEN:
            self._half_open_in_flight = max(0, self._half_open_in_flight - 1)

class ResilientGatewayClient:
    """Executes upstream gateway calls with circuit breaking and full-jitter retries."""

    def __init__(
        self,
        circuit_breaker: CircuitBreaker | None = None,
        max_retries: int = 3,
        base_delay_sec: float = 0.2,
        max_delay_sec: float = 5.0,
        sleeper: Callable[[float], None] = time.sleep,
        rng: Callable[[float, float], float] = random.uniform,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        if base_delay_sec <= 0 or max_delay_sec <= 0:
            raise ValueError("base_delay_sec and max_delay_sec")
        if base_delay_sec > max_delay_sec:
            raise ValueError("base_delay_sec cannot exceed max_delay_sec")

        self.circuit_breaker = circuit_breaker or CircuitBreaker()
        self.max_retries = max_retries
        self.base_delay_sec = base_delay_sec
        self.max_delay_sec = max_delay_sec
        self._sleeper = sleeper
        self._rng = rng
        self.metrics = GatewayMetrics() 

    def compute_backoff_delay(self, attempt: int) -> float:
        """Compute capped exponential backoff with full jitter: uniform(0, cap)."""
        exponential_cap = min(self.max_delay_sec, self.base_delay_sec * (2**attempt))
        return self._rng(0.0, exponential_cap)

    def execute(self, operation: Callable[[], T]) -> T:
        """Execute operation with circuit breaker protection and jittered retries."""
        for attempt in range(self.max_retries + 1):
            try:
                self.circuit_breaker.before_call()
            except CircuitOpenError:
                self.metrics.short_circuited_calls += 1
                raise

            self.metrics.total_attempts += 1
            if attempt > 0:
                self.metrics.total_retries += 1

            try:
                result = operation()
            except UpstreamClientError:
                self.circuit_breaker.record_ignored_error()
                raise
            except (UpstreamTransientError, TimeoutError, ConnectionError):
                self.circuit_breaker.record_failure()
                if attempt >= self.max_retries or self.circuit_breaker.state == CircuitState.OPEN:
                    raise
                delay = self.compute_backoff_delay(attempt)
                self._sleeper(delay)
            else:
                self.circuit_breaker.record_success()
                self.metrics.successful_calls += 1
                return result

        raise RuntimeError("Unreachable retry loop termination")
