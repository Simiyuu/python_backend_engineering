# Project 10: Signed Webhook & IPN Delivery Engine

Implements an asynchronous webhook dispatcher for payment gateways that signs merchant callbacks with `HMAC-SHA256`, enforces anti-replay timestamp windows, and isolates failing endpoints via exponential backoff and dead-lettering.

## Core Capabilities
- **HMAC-SHA256 Payload Signing & Replay Protection:** Generates canonical JSON signatures (`sha256=<hex>`) bound to `X-Webhook-Timestamp` and verifies them in constant time (`hmac.compare_digest`).
- **Idempotent Multi-Endpoint Fanout:** Deduplicates (`endpoint_id, event_id`) deliveries while skipping inactive endpoints.
- **Exponential Backoff & Dead-Letter Isoltion:** Retries transient HTTP errors and network exceptions on a `base_backoff_sec * 2^(attempt - 1)` schedule and moves exhausted deliveries to `DEAD_LETTERED` with full `AttemptLog` history.
