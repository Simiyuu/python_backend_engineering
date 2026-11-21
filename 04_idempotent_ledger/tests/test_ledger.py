from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
import sys 
from pathlib import Path

import pytest

SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from ledger import (
    AccountNotFoundError,
    IdempotencyConflictError,
    InsufficientFundsError,
    MobileMoneyLedger,
)

def test_idempotent_retry_returns_cached_without_double_debit():
    ledger = MobileMoneyLedger()
    ledger.create_account("alice", "1000.00")
    ledger.create_account("bob", "250.00")

    first = ledger.transfer("req-001", "alice", "bob", "150.50")
    retry = ledger.transfer("req-001", "alice", "bob", "150.50")

    assert first == retry 
    assert ledger.get_balance("alice") == Decimal("849.50")
    assert ledger.get_balance("bob") == Decimal("400.50")
    assert ledger.verify_invariant() is True

def test_idempotency_conflict_raises_error_on_mismatched_payload():
    ledger = MobileMoneyLedger()
    ledger.create_account("alice", "1000.00")
    ledger.create_account("bob", "0.00")
    ledger.create_account("charlie", "0.00")

    ledger.transfer("req-777", "alice", "bob", "100.00")

    with pytest.raises(IdempotencyConflictError):
        ledger.transfer("req-777", "alice", "bob", "200.00")

    with pytest.raises(IdempotencyConflictError):
        ledger.transfer("req-777", "alice", "charlie", "100.00")

    assert ledger.get_balance("alice") == Decimal("900.00")
    assert ledger.verify_invariant() is True

def test_insufficient_funds_and_missing_accounts_preserve_invariant():
    ledger = MobileMoneyLedger()
    ledger.create_account("alice", "50.00")
    ledger.create_account("bob", "10.00")

    with pytest.raises(InsufficientFundsError):
        ledger.transfer("req-overdraft", "alice", "bob", "75.00")

    with pytest.raises(AccountNotFoundError):
        ledger.transfer("req-missing", "alice", "unknown_user", "10.00")

    assert ledger.get_balance("alice") == Decimal("50.00")
    assert ledger.get_balance("bob") == Decimal("10.00")
    assert ledger.verify_invariant() is True

def test_concurrent_transfers_and_duplicate_retries_maintain_consistency():
    ledger = MobileMoneyLedger()
    ledger.create_account("payer", "1000.00")
    ledger.create_account("merchant", "0.00")

    # 10 distinct keys, each submitted twice concurrently (20 total calls)
    keys = [f"tx-key-{i}" for i in range(10)] * 2

    def worker(key: str):
        return ledger.transfer(key, "payer", "merchant", "25.00")

    with ThreadPoolExecutor(max_workers=8) as pool:
        receipts = list(pool.map(worker, keys))

    unique_tx_ids = {r.tx_id for r in receipts}
    assert len(unique_tx_ids) == 10
    assert ledger.get_balance("payer") == Decimal("750.00")
    assert ledger.get_balance("merchant") == Decimal("250.00")
    assert ledger.verify_invariant() is True
    