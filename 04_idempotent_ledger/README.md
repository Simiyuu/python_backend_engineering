# Project 04: Atomic Double-Entry Mobile-Money Ledger

A thread-safe financial transaction engine enforcing double-entry bookkeeping invariants and idempotency-key deduplication for payment retries.

## Key Engineering Concepts
- **Idempotency-Key Deduplication (`src/ledger.py`):** Caches canonical transfer receipts keyed by `idempotency_key` and validates request fingerprints `(sender_id, receiver_id, amount)` to prevent double-debits on network retries while rejecting mutated payloads with `IdempotencyConflictError`.
- **Exact `Decimal` Bookkeeping:** Uses `decimal.Decimal` and paired immutable `JournalEntry` records (`DEBIT` / `CREDIT`) to eliminate floating-point drift.
- **Thread-Safe Concurrency & Accounting Invariant:** Serializes state transitions with `threading.Lock` and verifies `sum(debits) == sum(balances)` via `verify_invariant()`.

## Usage
```bash
# Execute unit and ThreadPoolExecutor concurrency tests
pytest -v
