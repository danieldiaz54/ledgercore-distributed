"""Double-entry accounting domain models and zero-sum invariant enforcement.

All monetary values are represented strictly as exact integer minor units
(cents). Floats and booleans are explicitly rejected at construction time.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import re
from typing import Any

from src.domain.exceptions import (
    InsufficientFundsError,
    InvalidTransferError,
    ZeroSumInvariantViolationError,
)

_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


def _require_non_empty_str(value: object, field_name: str) -> None:
    if not isinstance(value, str) or not value.strip():
        raise InvalidTransferError(
            f"Field '{field_name}' must be a non-empty string.",
            details={"field": field_name},
        )


def _require_currency(value: object, field_name: str = "currency") -> None:
    if not isinstance(value, str) or not _CURRENCY_RE.fullmatch(value):
        raise InvalidTransferError(
            f"Field '{field_name}' must be a 3-letter uppercase ISO currency code (e.g. 'USD').",
            details={"field": field_name, "value": value},
        )


def _require_exact_int(value: object, field_name: str, *, min_value: int) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise InvalidTransferError(
            f"Field '{field_name}' must be an exact integer (booleans and floats are forbidden).",
            details={"field": field_name, "type": type(value).__name__},
        )
    if value < min_value:
        raise InvalidTransferError(
            f"Field '{field_name}' must be >= {min_value}, got {value}.",
            details={"field": field_name, "value": value, "min_value": min_value},
        )


class EntryDirection(str, Enum):
    """Accounting direction for an append-only ledger entry."""

    DEBIT = "DEBIT"
    CREDIT = "CREDIT"


@dataclass(frozen=True)
class Account:
    """Immutable self-validating ledger account snapshot denominated in integer cents."""

    account_id: str
    owner_name: str
    currency: str
    balance_cents: int
    status: str = "ACTIVE"
    version: int = 1

    def __post_init__(self) -> None:
        _require_non_empty_str(self.account_id, "account_id")
        _require_non_empty_str(self.owner_name, "owner_name")
        _require_currency(self.currency, "currency")
        _require_exact_int(self.balance_cents, "balance_cents", min_value=0)
        _require_non_empty_str(self.status, "status")
        _require_exact_int(self.version, "version", min_value=1)

    def ensure_sufficient_funds(self, requested_cents: int) -> None:
        """Verify that this account can cover ``requested_cents`` without overdraft."""
        _require_exact_int(requested_cents, "requested_cents", min_value=1)
        if self.balance_cents < requested_cents:
            raise InsufficientFundsError(
                account_id=self.account_id,
                available_cents=self.balance_cents,
                requested_cents=requested_cents,
            )

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary representation of the account."""
        return {
            "account_id": self.account_id,
            "owner_name": self.owner_name,
            "currency": self.currency,
            "balance_cents": self.balance_cents,
            "status": self.status,
            "version": self.version,
        }


@dataclass(frozen=True)
class LedgerEntry:
    """Immutable append-only double-entry journal line."""

    entry_id: str
    transaction_id: str
    account_id: str
    direction: EntryDirection
    amount_cents: int
    currency: str
    entry_type: str
    created_at: str

    def __post_init__(self) -> None:
        _require_non_empty_str(self.entry_id, "entry_id")
        _require_non_empty_str(self.transaction_id, "transaction_id")
        _require_non_empty_str(self.account_id, "account_id")
        if not isinstance(self.direction, EntryDirection):
            raise InvalidTransferError(
                "Field 'direction' must be an EntryDirection enum member (DEBIT or CREDIT).",
                details={"field": "direction", "value": self.direction},
            )
        _require_exact_int(self.amount_cents, "amount_cents", min_value=1)
        _require_currency(self.currency, "currency")
        _require_non_empty_str(self.entry_type, "entry_type")
        _require_non_empty_str(self.created_at, "created_at")

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary representation of the ledger entry."""
        return {
            "entry_id": self.entry_id,
            "transaction_id": self.transaction_id,
            "account_id": self.account_id,
            "direction": self.direction.value,
            "amount_cents": self.amount_cents,
            "currency": self.currency,
            "entry_type": self.entry_type,
            "created_at": self.created_at,
        }


class DoubleEntryValidator:
    """Pure domain validator enforcing the Double-Entry Zero-Sum invariant."""

    @staticmethod
    def validate_balanced(
        transaction_id: str,
        entries: tuple[LedgerEntry, ...] | list[LedgerEntry],
    ) -> None:
        """Verify that ``entries`` contains >= 2 entries and balances per currency.

        Raises:
            InvalidTransferError: If ``transaction_id`` is empty, ``entries`` is not a
                valid collection of ``LedgerEntry`` objects, or an entry references a
                different ``transaction_id``.
            ZeroSumInvariantViolationError: If fewer than 2 entries exist or if
                ``sum(DEBIT) != sum(CREDIT)`` for any currency in the transaction.
        """
        _require_non_empty_str(transaction_id, "transaction_id")

        if not isinstance(entries, (tuple, list)):
            raise InvalidTransferError(
                "Field 'entries' must be a tuple or list of LedgerEntry instances.",
                details={"type": type(entries).__name__},
            )

        for index, entry in enumerate(entries):
            if not isinstance(entry, LedgerEntry):
                raise InvalidTransferError(
                    f"Entry at index {index} must be a LedgerEntry instance.",
                    details={"index": index, "type": type(entry).__name__},
                )
            if entry.transaction_id != transaction_id:
                raise InvalidTransferError(
                    f"LedgerEntry '{entry.entry_id}' belongs to transaction "
                    f"'{entry.transaction_id}', expected '{transaction_id}'.",
                    details={
                        "entry_id": entry.entry_id,
                        "entry_transaction_id": entry.transaction_id,
                        "expected_transaction_id": transaction_id,
                    },
                )

        if len(entries) < 2:
            debit_total = sum(
                entry.amount_cents
                for entry in entries
                if entry.direction is EntryDirection.DEBIT
            )
            credit_total = sum(
                entry.amount_cents
                for entry in entries
                if entry.direction is EntryDirection.CREDIT
            )
            raise ZeroSumInvariantViolationError(
                transaction_id=transaction_id,
                debit_total=debit_total,
                credit_total=credit_total,
                message=(
                    f"Double-entry transaction '{transaction_id}' requires at least 2 ledger "
                    f"entries, received {len(entries)}."
                ),
            )

        totals_by_currency: dict[str, list[int]] = {}
        for entry in entries:
            bucket = totals_by_currency.setdefault(entry.currency, [0, 0])
            if entry.direction is EntryDirection.DEBIT:
                bucket[0] += entry.amount_cents
            elif entry.direction is EntryDirection.CREDIT:
                bucket[1] += entry.amount_cents

        for currency, (debit_total, credit_total) in totals_by_currency.items():
            if debit_total != credit_total:
                raise ZeroSumInvariantViolationError(
                    transaction_id=transaction_id,
                    debit_total=debit_total,
                    credit_total=credit_total,
                    currency=currency,
                )


__all__ = [
    "EntryDirection",
    "Account",
    "LedgerEntry",
    "DoubleEntryValidator",
]
