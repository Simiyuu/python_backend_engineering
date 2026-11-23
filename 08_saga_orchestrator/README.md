# Project 08: Distrbuted Payment Saga Orchestrator

Implements an orchestration-based Saga coordinator for multi-step financial workflows where distributed services cannot share ACID database transaction.

## Core Capabilities
- **Sequential Forward Execution:** Runs registered steps in order, merging returned dictionaries into a shared per-saga execution context.
- **LIFO Compensating Rollbacks:** Unwinds completed steps in strict reverse order when any downstream step raises an exception.
- **Operator Escalation State (`FAILED`):** Halts compensation and marks the saga `FAILED` with full diagnostics if a compensating action fails.
- **Idempotency & Audit Trail:** Deduplicates repeated `execute(saga_id)` calls for terminal sagas and records timestamped `EXECUTE` / `COMPENSATE` audit entries.
