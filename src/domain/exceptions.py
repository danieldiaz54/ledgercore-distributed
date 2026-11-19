"""Typed domain exceptions for ledgercore-distributed.

All business invariant violations, idempotency conflicts, lock contentions,
and accounting integrity errors raise explicit subclasses of ``DomainError``.
Zero infrastructure or transport dependencies are permitted in this module.
"""

from __future__ import annotations

from typing import Any


class DomainError(Exception):
    """Base class for all explicit domain exceptions."""

    code: str = "ERR-400-DOMAIN-ERROR"

    def __init__(
        self,
        message: str,
        *,
        code: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.message: str = message
        self.code: str = code if code is not None else self.__class__.code
        self.details: dict[str, Any] = dict(details) if details is not None else {}
        super().__init__(message)


class InvalidTransferError(DomainError):
    """Raised when a transfer command, account, or domain entity violates validation rules."""

    code: str = "ERR-400-INVALID-TRANSFER"


class AccountNotFoundError(DomainError):
    """Raised when an operation references a non-existent ledger account."""

    code: str = "ERR-404-ACCOUNT-NOT-FOUND"

    def __init__(self, account_id: str, message: str | None = None) -> None:
        self.account_id: str = account_id
        resolved_message = message or f"Account '{account_id}' was not found."
        super().__init__(
            resolved_message,
            code=self.code,
            details={"account_id": account_id},
        )


class InsufficientFundsError(DomainError):
    """Raised when an account lacks sufficient balance (in integer cents) for a debit."""

    code: str = "ERR-422-INSUFFICIENT-FUNDS"

    def __init__(
        self,
        account_id: str,
        available_cents: int,
        requested_cents: int,
        message: str | None = None,
    ) -> None:
        self.account_id: str = account_id
        self.available_cents: int = available_cents
        self.requested_cents: int = requested_cents
        resolved_message = (
            message
            or f"Account '{account_id}' has insufficient funds: "
            f"available={available_cents} cents, requested={requested_cents} cents."
        )
        super().__init__(
            resolved_message,
            code=self.code,
            details={
                "account_id": account_id,
                "available_cents": available_cents,
                "requested_cents": requested_cents,
            },
        )


class IdempotencyConflictError(DomainError):
    """Raised when an idempotency key is reused with a mismatched SHA-256 request fingerprint."""

    code: str = "ERR-409-IDEMPOTENCY-CONFLICT"

    def __init__(
        self,
        idempotency_key: str,
        message: str | None = None,
        *,
        expected_fingerprint: str | None = None,
        received_fingerprint: str | None = None,
    ) -> None:
        self.idempotency_key: str = idempotency_key
        self.expected_fingerprint: str | None = expected_fingerprint
        self.received_fingerprint: str | None = received_fingerprint
        details: dict[str, Any] = {"idempotency_key": idempotency_key}
        if expected_fingerprint is not None:
            details["expected_fingerprint"] = expected_fingerprint
        if received_fingerprint is not None:
            details["received_fingerprint"] = received_fingerprint
        resolved_message = (
            message
            or f"Idempotency key '{idempotency_key}' was already used with a different request payload."
        )
        super().__init__(resolved_message, code=self.code, details=details)


class DistributedLockContentionError(DomainError):
    """Raised when a distributed resource mutex is currently held by another owner."""

    code: str = "ERR-409-LOCK-CONTENTION"

    def __init__(
        self,
        resource_key: str,
        message: str | None = None,
        *,
        current_owner: str | None = None,
        requested_owner: str | None = None,
    ) -> None:
        self.resource_key: str = resource_key
        self.current_owner: str | None = current_owner
        self.requested_owner: str | None = requested_owner
        details: dict[str, Any] = {"resource_key": resource_key}
        if current_owner is not None:
            details["current_owner"] = current_owner
        if requested_owner is not None:
            details["requested_owner"] = requested_owner
        if message is not None:
            resolved_message = message
        elif current_owner is not None:
            resolved_message = (
                f"Distributed lock contention on resource '{resource_key}': "
                f"currently held by '{current_owner}'."
            )
        else:
            resolved_message = (
                f"Distributed lock contention on resource '{resource_key}': "
                "resource is already locked by another owner."
            )
        super().__init__(resolved_message, code=self.code, details=details)


class ZeroSumInvariantViolationError(DomainError):
    """Raised when a double-entry transaction fails the zero-sum DEBIT == CREDIT invariant."""

    code: str = "ERR-500-ZERO-SUM-VIOLATION"

    def __init__(
        self,
        transaction_id: str,
        debit_total: int,
        credit_total: int,
        message: str | None = None,
        *,
        currency: str | None = None,
    ) -> None:
        self.transaction_id: str = transaction_id
        self.debit_total: int = debit_total
        self.credit_total: int = credit_total
        self.currency: str | None = currency
        details: dict[str, Any] = {
            "transaction_id": transaction_id,
            "debit_total": debit_total,
            "credit_total": credit_total,
        }
        if currency is not None:
            details["currency"] = currency
        if message is not None:
            resolved_message = message
        else:
            currency_suffix = f" for currency '{currency}'" if currency else ""
            resolved_message = (
                f"Zero-sum double-entry invariant violated for transaction '{transaction_id}'"
                f"{currency_suffix}: debit_total={debit_total}, credit_total={credit_total}."
            )
        super().__init__(resolved_message, code=self.code, details=details)


__all__ = [
    "DomainError",
    "InvalidTransferError",
    "AccountNotFoundError",
    "InsufficientFundsError",
    "IdempotencyConflictError",
    "DistributedLockContentionError",
    "ZeroSumInvariantViolationError",
]
