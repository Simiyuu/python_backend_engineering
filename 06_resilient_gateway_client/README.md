# Project 06: Resilient Upstream Gateway Client (Circuit Breaker & Full jitter Backoff)

## Overview
Fault-tolerant Python client wrapper for calling external telecom/payment gateways where transient upstream outages (`502`/`503`/`504`, socket timeouts) occure alongside non-retryable caller errors (`400`/`401`/`422`).

## Architecture & Guarantees
- **Three-State Circuit Breaker ($0(1)$ per call):**
  - `CLOSED`: Tracks consecutive transient infrastructure failures. Resets counter on any successful call.
  - `OPEN`: Short-circuits calls immediately with `CircuitOpenError` (including `retry_after_sec`) until `recovery_timeout_sec` elapses.
  - `HALF_OPEN`: Permits up to `half_open_max_calls` recovery probes. Any probe failure trips back to `OPEN`; full probe success closes the circuit.
- **Error Isolation:** `UpstreamClientError` (4xx validation/auth errors) bypasses retries and does not increment circuit failure counts.
- **Capped Exponential Backoff with Full Jitter:** Computes `delay = uniform(0, min(max_delay_sec, base_delay_sec * 2**attempt))` to prevent synchronized retry storms.
- **Deterministic Testing:** Injectable `clock`, `sleeper`, and `rng` dependencies verify timeout windows and backoff bounds without sleeping.

## Usage
```bash
pytest -v
