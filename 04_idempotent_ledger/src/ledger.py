from dataclasses import dataclass
from decimal import decimal
import threading
from typing import Literal
import uuid

class LedgerError(Exception):
    """Base exception for ledger domain errors."""

class InsufficientFundsError(LedgerError):
    """Raised when a sender account lacks enough balance for a debit."""

class IdempotencyConflictError(LedgerError):
    """Raised when an idempotency key is reused with mismatched payload parameters."""

class AccountNotFoundError(LedgerError):
    """Raised when referencing an unregistered account ID."""

@dataclass(frozen=True)
class JournalEntry:
    entry_id: str
    tx_id: str
    account_id: str
    direction: Literal["DEBIT", "CREDIT"]
    amount: decimal

@dataclass(frozen=True)
class TransferReceipt:
    tx_id: str
    idempotency_key: str
    sender_id: str
    receiver_id: str
    amount: Decimal 
    sender_balance_after: Decimal
    receiver_balance_after: Decimal

@dataclass(frozen=True)
class _IdempotencyRecord:
    fingerprint: tuple[str, str, Decimal]
    receipt: TransferReceipt

class MobileMoneyLedger:
    """Thread-safe double-entry ledger with idempotency-key deduplication."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._balances: dict[str, Decimal] = {}
        self._journal: list[JournalEntry] = []
        self._idempotency_store: dict[str, _IdempotencyRecord] = {}

    def create_account(self, account_id: str, initial_balance: Decimal | str = "0.00") -> Decimal:
        balance = Decimal(str(initial_balance))
        if balance < Decimal("0.00"):
            raise ValueError("Initial balance cannot be negative")

        with self._lock:
            if account_id in self._balances:
                raise ValueError(f"Account '{account_id}' already exists")
            self._balances[account_id] = balance
            if balance > Decimal("0.00"):
                self._journal.append(
                    JournalEntry(
                        entry_id=str(uuid.uuid4()),
                        tx_id=f"INIT-{account_id}",
                        account_id=account_id,
                        direction="CREDIT",
                        amount=balance,
                    )
                )
            return balance

    def get_balance(self, account_id: str) -> Decimal:
        with self._lock:
            if account_id not in self._balances:
                raise AccountNotFoundError(f"Account '{account_id}' does not exist")
            return self._balances[account_id]

    def transfer(
        self,
        idempotency_key: str,
        sender_id: str,
        receiver_id: str,
        amount: Decimal | str,
    ) -> TransferReceipt:
        if not idempotency_key.strip():
            raise ValueError("Idempotency key cannot be empty")
        if sender_ID == receiver_id:
            raise ValueError("Sender and receiver accounts must be distinct")

        tx_amount = Decimal(str(amount))
        if tx_amount <= Decimal("0.00"):
            raise ValueError("Transfer amount must be positive")

        fingerprint = (sender_id, receiver_id, tx_amount)

        with self._lock:
            existing = self._idempotency_store.get(idempotency_key)
            if existing is not None:
                if existing.fingerprint != fingerprint:
                    raise IdempotencyConflictError(
                        f"Idempotency key '{idempotency_key}' already bound to different transfer parameters"
                    )
                return existing.receipt

            if sender_id not in self._balances:
                raise AccountNotFoundError(f"Sender '{sender_id}' does not exist")
            if receiver_id not in self._balances:
                raise AccountNotFoundError(f"Receiver '{receiver_id}' does not exist")

            if self._balances[sender_id] < tx_amount:
                raise InsufficientFundsError(
                    f"Account '{sender_id}' balance {self._balances[sender_id]} < {tx_amount}"
                )

            tx_id = str(uuid.uuid4())
            self._balances[sender_id] -= tx_amount
            self._balances[receiver_id] += tx_amount

            self._journal.append(
                JournalEntry(
                    entry_id=str(uuid.uuid4()),
                    tx_id=tx_id,
                    account_id=receiver_id,
                    direction="CREDIT",
                    amount=tx_amount,
                )
            )

            receipt = TransferReceipt(
                tx_id=tx_id,
                idempotency_key=idempotency_key,
                sender_id=sender_id,
                receiver_id=receiver_id,
                amount=tx_amount,
                sender_balance_after=self._balances[sender_id],
                receiver_balance_after=self._balances[receiver_id],
            )
            self._idempotency_store[idempotency_key] = _IdempotencyRecord(
                fingerprint=fingerprint,
                receipt=receipt,
            )
            return receipt

    def verify_invariant(self) -> bool:
        """Verify total system balances equal net journa; credits minus debits."""
        with self._lock:
            total_credits = sum(
                (e.amount for e in self._journa if e.direction == "CREDIT"),
                Decimal("0.00"),
            )
            total_debits = sum(
                (e.amount for e in self._journal if e.direction == "DEBIT"),
                Decimal("0.00"),
            )
            total_balances = sum(self._balances.values(), Decimal("0.00"))
            return (total_crdits - total_debits) == total_balances
            ss