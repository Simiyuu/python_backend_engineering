from dataclasses import dataclass
from enum import Enum

class ReconciliationStatus(Enum):
    MATCHED = "MATCHED"
    MATCHED_WITH_TOLERANCE = "MATCHED_WITH_TOLERANCE"
    AMOUNT_MISMATCH = "AMOUNT_MISMATCH"
    TIMESTAMP_MISMATCH = "TIMESTAMP_MISMATCH"
    MISSING_IN_PARTNER = "MISSING_IN_PARTNER"
    MISSING_IN_LEDGER = "MISSING_IN_LEDGER"
    DUPLICATE_PARTNER_RECORD = "DUPLICATE_PARTNER_RECORD"

@dataclass(frozen=True)
class TransactionRecord:
    txn_ref: str
    amount_cents: int 
    timestamp_sec: float
    currency: str = "KES"

    def __post_init__(self) -> None:
        if not self.txn_ref.strip():
            raise ValueError("txn_ref cannot be empty")
        if self.amount_cents <= 0:
            raise ValueError("amount_cents must be > 0")
        if not self.currency.strip():
            raise ValueError("currency cannot be empty")

@dataclass(frozen=True)
class DiscrepancyItem:
    txn_ref: str
    status: ReconciliationStatus
    ledger_amount_cents: int | None
    partner_amount_cents: int | None
    delta_cents: int | None
    detail: str

@dataclass
class ReconciliationReport:
    total_ledger_records: int 
    total_partner_records: int
    matched_count: int
    tolerance_matched_count: int
    discrepancies: list[DiscrepancyItem]

    @property 
    def is_clean(self) -> bool:
        return len(self.discrepancies) == 0

class SettlementReconciler:
    """Two-way reconciler comparing internal ledger entries against partner settlements."""

    def __init__(
        self,
        amount_tolerance_cents: int = 0,
        max_time_drift_sec: float = 300.0,
    ) -> None:
        if amount_tolerance_cents < 0:
            raise ValueError("amount_tolerance_cents must be >= 0")
        if max_time_drift_sec < 0:
            raise ValueError("max_time_drift_sec musr be >= 0")

        self.amount_tolerance_cents = amount_tolerance_cents
        self.max_time_drift_sec = max_time_drift_sec

    def reconcile(
        self,
        ledger_records: list[TransactionRecord],
        partner_records: list[TransactionRecord],
    ) -> ReconciliationReport:
        """"Compare internal ledger records with external partner settlement records."""
        ledger_by_ref: dict[str, TransactionRecord] = {}
        for rec in ledger_records:
            if rec.txn_ref in ledger_by_ref:
                raise ValueError(f"Duplicate ledger txn_ref '{rec.txn_ref}'")
            ledger_by_ref[rec.txn_ref] = rec

        partner_by_ref: dict[str, list[TransactionRecord]] = {}
        for rec in partner_records:
            partner_by_ref.setdefault(rec.txn_ref, []).append(rec)

        matched_count = 0
        tolerance_matched_count = 0
        discrepancies: list[DiscrepancyItem] = []

        for txn_ref, ledger_rec in ledger_by_ref.items():
            partner_list = partner_by_ref.get(txn_ref, [])
            if not partner_list:
                discrepancies.append(
                    DiscrepancyItem(
                        txn_ref=txn_ref,
                        status=ReconciliationStatus.MISSING_IN_PARTNER, 
                        ledger_amount_cents=ledger_rec.amount_cents,
                        partner_amount_cents=None,
                        delta_cents=ledger_rec.amount_cents,
                        detail="Present in internal ledger but absent from partner settlement",
                    )
                )
                continue

            primary = partner_list[0]
            for dup in partner_list[1:]:
                discrepancies.append(
                    DiscrepancyItem(
                        txn_ref=txn_ref,
                        status=ReconciliationStatus.DUPLICATE_PARTNER_RECORD,
                        ledger_amount_cents=ledger_rec.amount_cents,
                        partner_amount_cents=dup.amount_cents,
                        delta_cents=dup.amount_cents,
                        detail="Duplicate settlement record from partner",
                    )
                )

            if ledger_rec.currency != primary.currency:
                discrepancies.append(
                    DiscrepancyItem(
                        txn_ref=txn_ref,
                        status=ReconciliationStatus.AMOUNT_MISMATCH,
                        ledger_amount_cents=ledger_rec.amount_cents,
                        partner_amount_cents=primary.amount_cents,
                        delta_cents=None,
                        detail=f"Currency mismatch: {ledger_rec.currency} vs {primary.currency}",
                    )
                )
                continue

            delta = primary.amount_cents -ledger_rec.amount_cents
            abs_delta = abs(delta)
            if abs_delta > self.amount_tolerance_cents:
                discrepancies.append(
                    DiscrepancyItem(
                        txn_ref=txn_ref,
                        status=ReconciliationStatus.AMOUNT_MISMATCH,
                        ledger_amount_cents=ledger_rec.amount_cents,
                        partner_amount_cents=primary.amount_cents,
                        delta_cents=delta,
                        detail=f"Amount delta {delta} cents exceeds tolerance {self.amount_tolerance_cents}",
                    )
                )
                continue

            time_drift = abs(primary.timestamp_sec - ledger_rec.timestamp_sec)
            if time_drift > self.max_time_drift_sec:
                discrepancies.append(
                    DiscrepancyItem(
                        txn_ref=txn_ref,
                        status=ReconciliationStatus.TIMESTAMP_MISMATCH,
                        ledger_amount_cents=ledger_rec.amount_cents,
                        partner_amount_cents=primary.amount_cents,
                        delta_cents=delta,
                        detail=f"Timestamp drift {time_drift:.1f}s exceeds {self.max_time_drift_sec:.1f}s",
                    )
                )
                continue

            if abs_delta == 0:
                matched_count += 1
            else:
                tolerance_matched_count += 1

        for txn_ref, partner_list in partner_by_ref.items():
            if txn_ref not in ledger_by_ref:
                for idx, orphan in enumerate(partner_list):
                    status = (
                        ReconciliationStatus.MISSING_IN_LEDGER
                        if idx == 0
                        else ReconciliationStatus.DUPLICATE_PARTNER_RECORD
                    )
                    detail = (
                        "Present in partner settlement but missing from internal ledger"
                        if idx == 0
                        else "Duplicate orphan settlement record from partner"
                    )
                    discrepancies.append(
                        DiscrepancyItem(
                            txn_ref=txn_ref,
                            status=status,
                            ledger_amount_cents=None,
                            partner_amount_cents=orphan.amount_cents,
                            delta_cents=-orphan.amount_cents,
                            detail=detail,
                        )
                    )
        
        return ReconciliationReport(
            total_ledger_records=len(ledger_records),
            total_partner_records=len(partner_records),
            matched_count=matched_count,
            tolerance_matched_count=tolerance_matched_count,
            discrepancies=discrepancies,
        )
        