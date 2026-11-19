"""Pure domain layer exports for ledgercore-distributed."""

from __future__ import annotations

from src.domain.exceptions import (
    AccountNotFoundError,
    DistributedLockContentionError,
    DomainError,
    IdempotencyConflictError,
    InsufficientFundsError,
    InvalidTransferError,
    ZeroSumInvariantViolationError,
)
from src.domain.ledger import (
    Account,
    DoubleEntryValidator,
    EntryDirection,
    LedgerEntry,
)
from src.domain.repositories import LedgerRepositoryProtocol
from src.domain.saga_outbox import (
    DeadLetterEvent,
    DistributedLockManager,
    InMemoryKafkaBroker,
    OutboxEvent,
    OutboxStatus,
    SagaInstance,
    SagaStatus,
    TransferCommand,
)

__all__ = [
    "DomainError",
    "InvalidTransferError",
    "AccountNotFoundError",
    "InsufficientFundsError",
    "IdempotencyConflictError",
    "DistributedLockContentionError",
    "ZeroSumInvariantViolationError",
    "EntryDirection",
    "Account",
    "LedgerEntry",
    "DoubleEntryValidator",
    "SagaStatus",
    "OutboxStatus",
    "TransferCommand",
    "SagaInstance",
    "OutboxEvent",
    "DeadLetterEvent",
    "DistributedLockManager",
    "InMemoryKafkaBroker",
    "LedgerRepositoryProtocol",
]
