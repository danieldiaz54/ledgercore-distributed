"""Domain repository ports (dependency inversion interfaces).

Infrastructure adapters in ``src/infrastructure/`` MUST implement these
protocols without leaking storage, SQL, or transport details into ``src/domain/``.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from src.domain.ledger import Account, LedgerEntry
from src.domain.saga_outbox import (
    DeadLetterEvent,
    OutboxEvent,
    SagaInstance,
    TransferCommand,
)


@runtime_checkable
class LedgerRepositoryProtocol(Protocol):
    """Port defining atomic persistence operations for the double-entry Saga ledger."""

    def list_accounts(self) -> list[Account]:
        """Return all persisted ledger accounts."""
        ...

    def get_account(self, account_id: str) -> Account | None:
        """Return an account snapshot by ID, or ``None`` if not found."""
        ...

    def get_idempotency_record(self, idempotency_key: str) -> SagaInstance | None:
        """Look up an existing Saga instance by its client ``idempotency_key``."""
        ...

    def reserve_funds_with_outbox(
        self,
        command: TransferCommand,
        saga_id: str,
        fencing_token: int,
    ) -> SagaInstance:
        """Atomically debit source into clearing, append balanced entries, create RESERVED Saga, and insert PENDING OutboxEvent."""
        ...

    def settle_saga_with_outbox(
        self,
        saga_id: str,
        fencing_token: int,
    ) -> SagaInstance:
        """Atomically release clearing into destination, append balanced entries, mark Saga SETTLED, and insert PENDING OutboxEvent."""
        ...

    def compensate_saga_with_outbox(
        self,
        saga_id: str,
        fencing_token: int,
        failure_reason: str,
    ) -> SagaInstance:
        """Atomically post compensating reversal entries restoring source, mark Saga COMPENSATED, insert OutboxEvent, and record DeadLetterEvent."""
        ...

    def list_pending_outbox_events(self, limit: int = 50) -> list[OutboxEvent]:
        """Return oldest ``PENDING`` outbox events awaiting broker publication."""
        ...

    def mark_outbox_published(self, event_id: str) -> None:
        """Transition an outbox event from ``PENDING`` to ``PUBLISHED``."""
        ...

    def list_outbox_events(self, limit: int = 100) -> list[OutboxEvent]:
        """Return persisted outbox events across all statuses."""
        ...

    def list_sagas(self, limit: int = 100) -> list[SagaInstance]:
        """Return persisted Saga instances."""
        ...

    def list_ledger_entries(
        self,
        transaction_id: str | None = None,
        limit: int = 200,
    ) -> list[LedgerEntry]:
        """Return append-only double-entry ledger lines, optionally filtered by ``transaction_id``."""
        ...

    def list_dead_letter_events(self, limit: int = 100) -> list[DeadLetterEvent]:
        """Return persisted Dead-Letter Queue (DLQ) events."""
        ...


__all__ = ["LedgerRepositoryProtocol"]
