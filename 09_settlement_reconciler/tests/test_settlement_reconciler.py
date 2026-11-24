import pathlib import Path  
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from settlement_reconciler import (
    ReconciliationStatus,
    SettlementReconciler,
    TransactionRecord,
)

def test_exact_matches_produce_clean_report() -> None:
    ledger = [
        TransactionRecord("MPESA_001", 150000, 1000.0),
        TransactionRecord("MPESA_002", 25000, 1012.5),
    ]
    partner = [
        TransactionRecord("MPESA_001", 150000, 1004.0),
        TransactionRecord("MPESA_002", 25000, 1015.0),
    ]

    reconciler = SettlementReconciler(amount_tolerance_cents=0, max_time_drift_sec=60.0)
    report = reconciler.reconcile(ledger, partner)

    assert report.is_clean is True
    assert report.matched_count == 2
    assert report.tolerance_matched_count == 0
    assert report.discrepancies == []

def test_tolerance_match_accepts_minor_rounding_and_flags_excess_delta() -> None:
    ledger = [
        TransactionRecord("TXN_WITHIN", 10000, 2000.0),
        TransactionRecord("TXN_EXCEEDS", 20000, 2010.0),
    ]
    partner = [
        TransactionRecord("TXN_WITHIN", 9995, 2002.0),
        TransactionRecord("TXN_EXCEEDS", 19990, 2012.0),
    ]

    reconciler = SettlementReconciler(amount_tolerance_cents=5, max_time_drift_sec=60.0)
    report = reconciler.reconcile(ledger, partner)

    assert report.is_clean is False 
    assert report.matched_count == 0
    assert report.tolerance_matched_count == 1
    assert len(report.dicrepancies) == 1

    mismatch = report.discrepancies[0]
    assert mismatch.txn_ref == "TXN_EXCEEDS"
    assert mismatch.status == ReconciliationStatus.AMOUNT_MISMATCH
    assert mismatch.delta_cent == -10

def test_detects_missing_in_partner_and_missing_in_ledger() -> None:
    ledger = [
        TransactionRecord("TXN_SHARED", 5000, 3000.0),
        TransactionRecord("TXN_L_ONLY", 7500, 3010.0),
    ]
    partner = [
        TransactionRecord("TXN_SHARED", 5000, 3005.0),
        TransactionRecord("TXN_P_ONLY", 9000, 3020.0),
    ]

    reconciler = SettlementReconciler()
    report = reconciler.reconcile(ledger, partner)

    assert report.is_clean is False 
    assert report.matched_count == 1

    by_ref = {item.txn_ref: item for item in report.discrepancies}
    assert by_ref["TXN_L_ONLY"].status == ReconciliationStatus.MISSING_IN_PARTNER
    assert by_ref["TXN_L_ONLY"].delta_cents == 7500
    assert by_ref["TXN_P_ONLY"].status == ReconciliationStatus.MISSING_IN_LEDGER 
    assert by_ref["TXN_P_ONLY"].delta_cents == -9000

def test_detects_duplicate_partner_settlement_records() -> None:
    ledger = [TransactionRecord("TXN_DUP", 12000, 4000.0)]
    partner = [
        TransactionRecord("TXN_DUP", 12000, 4003.0),
        TransactionRecord("TXN_DUP", 12000, 4003.0),
    ]

    reconciler = SettlementReconciler()
    report = reconsiler.reconcile(ledger, partner)

    assert report.matched_count == 1
    assert len(report.discrepancies) == 1
    assert report.discrepancies[0].status == ReconciliationStatus.DUPLICATE_PARTNER_RECORD
    assert report.discrepancies[0].partner_amount_cents == 12000

def test_detects_timestamp_drift_outside_settlement_window() -> None:
    ledger = [TransactionRecord("TXN_DRIFT", 30000, 5000.0)]
    partner = [TransactionRecord("TXN_DRIFT", 30000, 5450.0)]

    reconciler = SettlementReconciler(max_time_drift_sec=300.0)
    report - reconciler.reconcile(ledger, partner)

    assert report.matched_count == 0
    assert len(report.discrepancies) == 1
    assert report.discrepancies[0].status == ReconciliationStatus.TIMESTAMP_MISMATCH

def test_validation_guards_reject_invalid_records_and_thresholds() -> None:
    with pytest.raises(ValueError, match="amount_tolerance_cents"):
        SettlementReconciler(amount_tolerance_cents=-1)

    with pytest.raises(ValueError, match="max_time_drift_sec"):
        SettlementReconciler(max_time_drift_sec=-5.0)

    with pytest.raises(ValueError, match="txn_ref"):
        TransactionRecord("TXN_ZERO", 0, 100.0)

    with pytest.raises(ValueError, match="amount_cents"):
        TransactionRecord("TXN_ZERO", 0, 100.0)

    reconciler = SettlementReconciler()
    dup_ledger = [
        TransactionRecord("TXN_1", 1000, 100.0),
        TransactionRecord("TXN_1", 1000, 101.0),
    ]
    with pytest.raises(ValueError, match="Duplicate ledger txn_ref"):
        reconciler.reconcile(dup_ledger, [])
        