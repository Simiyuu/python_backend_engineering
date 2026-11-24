# Project 09: Two-Way Payment Settlement Reconciler

Impmenets an intraday and end-of-day financial reconciliation engine that compared internal core ledger records against external partner/bank settlement records using integer minor units (`cents`).

## Core Capabilities
- **Integer Minor-Unit Matching:** Eliminates IEEE-754 floating-point drift by evaluating amounts and tolerances in integer cents.
- **Configurable Tolerance & Drift Windows:** Support minor fee/rounding tolerances (`MATCHED_WITH_TOLERANCE`) and maximum settlement timestamp drift bounds (`TIMESTAMP_MISMATCH`)
- **Full Discrepancy Taxonomy:** Classifies `AMOUNT_MISMATCH`, `MISSING_IN_PARTNER`, `MISSING_IN_LEDGER`, and `DUPLICATE_PARTNER_RECORD` items with signed `delta_cents` for automated dispute or reversal workflows.

