"""Application service layer for LedgerCore Distributed Saga & Outbox orchestration.

Coordinates domain primitives (``TransferCommand``, ``DistributedLockManager``,
``InMemoryKafkaBroker``) with ACID persistence (``SQLiteLedgerRepository``) while
maintaining strict idempotency, monotonic fencing tokens, append-only double-entry
accounting, and FIFO Transactional Outbox + DLQ relay.
"""

from __future__ import annotations

from datetime import datetime, timezone
import json
import os
from typing import Any
import uuid

from src.domain.exceptions import (
    AccountNotFoundError,
    IdempotencyConflictError,
    InvalidTransferError,
)
from src.domain.saga_outbox import (
    DeadLetterEvent,
    DistributedLockManager,
    InMemoryKafkaBroker,
    TransferCommand,
)
from src.infrastructure.persistence.seed_accounts import seed_default_accounts
from src.infrastructure.persistence.sqlite_ledger_repo import SQLiteLedgerRepository

_SETTLEMENT_FAILURE_REASON = "Downstream settlement partner timeout — auto-compensated"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class LedgerCoreApplicationService:
    """Application facade orchestrating distributed transfers, outbox relay, and audit queries."""

    def __init__(
        self,
        db_path: str = ":memory:",
        auto_seed: bool = True,
        *,
        repo: SQLiteLedgerRepository | None = None,
        lock_manager: DistributedLockManager | None = None,
        kafka_broker: InMemoryKafkaBroker | None = None,
    ) -> None:
        self.db_path: str = db_path
        self.repo: SQLiteLedgerRepository = (
            repo if repo is not None else SQLiteLedgerRepository(db_path=db_path)
        )
        self.lock_manager: DistributedLockManager = (
            lock_manager if lock_manager is not None else DistributedLockManager()
        )
        self.kafka_broker: InMemoryKafkaBroker = (
            kafka_broker if kafka_broker is not None else InMemoryKafkaBroker()
        )
        if auto_seed:
            seed_default_accounts(self.repo)

    def close(self) -> None:
        """Close underlying repository resources cleanly."""
        self.repo.close()

    def get_health(self) -> dict[str, Any]:
        """Return service health, telemetry counters, lock state, and global zero-sum audit."""
        audit = self.repo.compute_global_zero_sum_audit()
        accounts = self.repo.list_accounts()
        sagas = self.repo.list_sagas(limit=1000)
        pending_outbox = self.repo.list_pending_outbox_events(limit=1000)
        dlq_events = self.repo.list_dead_letter_events(limit=1000)
        active_locks = self.lock_manager.active_locks()

        return {
            "status": "ok",
            "service": "ledgercore-distributed",
            "environment": os.environ.get("LEDGERCORE_ENV", "development"),
            "accounts_count": len(accounts),
            "sagas_count": len(sagas),
            "pending_outbox_count": len(pending_outbox),
            "dlq_count": len(dlq_events),
            "active_locks_count": len(active_locks),
            "current_fencing_token": self.lock_manager.current_fencing_token(),
            "zero_sum_balanced": bool(audit["is_balanced"]),
            "zero_sum_audit": audit,
            "timestamp": _utc_now_iso(),
        }

    def list_accounts(self) -> list[dict[str, Any]]:
        """Return all ledger accounts serialized as plain dictionaries."""
        return [account.to_dict() for account in self.repo.list_accounts()]

    def _build_transfer_command(
        self,
        payload: dict[str, Any],
        idempotency_key_header: str | None = None,
    ) -> TransferCommand:
        if not isinstance(payload, dict):
            raise InvalidTransferError(
                "Transfer request payload must be a JSON object.",
                details={"type": type(payload).__name__},
            )

        header_key: str | None = None
        if idempotency_key_header is not None:
            if not isinstance(idempotency_key_header, str):
                raise InvalidTransferError(
                    "Header 'Idempotency-Key' must be a non-empty string.",
                    details={"field": "idempotency_key"},
                )
            stripped = idempotency_key_header.strip()
            if stripped:
                header_key = stripped

        body_key_raw = payload.get("idempotency_key")
        body_key: str | None = None
        if body_key_raw is not None:
            if not isinstance(body_key_raw, str):
                raise InvalidTransferError(
                    "Field 'idempotency_key' must be a non-empty string.",
                    details={"field": "idempotency_key"},
                )
            stripped_body = body_key_raw.strip()
            if stripped_body:
                body_key = stripped_body

        idempotency_key = header_key or body_key
        if not idempotency_key:
            raise InvalidTransferError(
                "Missing required 'Idempotency-Key' header or 'idempotency_key' field.",
                details={"field": "idempotency_key"},
            )

        if "amount_cents" not in payload:
            raise InvalidTransferError(
                "Field 'amount_cents' is required and must be a positive integer.",
                details={"field": "amount_cents"},
            )
        amount_cents = payload["amount_cents"]
        if (
            not isinstance(amount_cents, int)
            or isinstance(amount_cents, bool)
            or amount_cents <= 0
        ):
            raise InvalidTransferError(
                "Field 'amount_cents' must be a positive exact integer (booleans and floats are forbidden).",
                details={"field": "amount_cents", "value": amount_cents},
            )

        source_account_id = payload.get("source_account_id", "")
        destination_account_id = payload.get("destination_account_id", "")

        currency = payload.get("currency", "USD")
        if currency is None:
            currency = "USD"

        simulate_settlement_failure = payload.get("simulate_settlement_failure", False)
        if simulate_settlement_failure is None:
            simulate_settlement_failure = False

        return TransferCommand(
            idempotency_key=idempotency_key,
            source_account_id=source_account_id,
            destination_account_id=destination_account_id,
            amount_cents=amount_cents,
            currency=currency,
            simulate_settlement_failure=simulate_settlement_failure,
        )

    def _resolve_existing_idempotency(
        self,
        cmd: TransferCommand,
    ) -> tuple[int, dict[str, Any]] | None:
        record = self.repo.get_idempotency_record(cmd.idempotency_key)
        if record is None:
            return None

        if record["request_fingerprint"] != cmd.request_fingerprint:
            raise IdempotencyConflictError(
                cmd.idempotency_key,
                expected_fingerprint=record["request_fingerprint"],
                received_fingerprint=cmd.request_fingerprint,
            )

        existing_saga = self.repo.get_saga(record["saga_id"])
        if existing_saga is None:
            raise AccountNotFoundError(
                record["saga_id"],
                message=f"Saga '{record['saga_id']}' referenced by idempotency key was not found.",
            )

        entries = [
            entry.to_dict()
            for entry in self.repo.list_ledger_entries(transaction_id=existing_saga.saga_id)
        ]
        audit = self.repo.compute_global_zero_sum_audit()
        return (
            200,
            {
                "idempotent_replay": True,
                "saga": existing_saga.to_dict(),
                "ledger_entries": entries,
                "outbox_relayed": [],
                "zero_sum_audit": audit,
            },
        )

    def execute_transfer(
        self,
        payload: dict[str, Any],
        idempotency_key_header: str | None = None,
    ) -> tuple[int, dict[str, Any]]:
        """Execute a distributed double-entry transfer Saga with idempotency and lock fencing.

        Returns:
            ``(200, response_dict)`` on safe idempotent replay with matching fingerprint.
            ``(201, response_dict)`` when a new Saga completes (``SETTLED`` or ``COMPENSATED``).
        """
        cmd = self._build_transfer_command(
            payload,
            idempotency_key_header=idempotency_key_header,
        )

        # Fast-path idempotency check before acquiring a distributed resource mutex.
        replay = self._resolve_existing_idempotency(cmd)
        if replay is not None:
            return replay

        saga_id = f"SAGA-{uuid.uuid4().hex[:12].upper()}"
        fencing_token = self.lock_manager.acquire(cmd.source_account_id, owner=saga_id)
        try:
            # Double-check under mutex in case a concurrent caller completed the same key.
            replay_after_lock = self._resolve_existing_idempotency(cmd)
            if replay_after_lock is not None:
                return replay_after_lock

            # Step 1: Atomic reserve (DEBIT source, CREDIT escrow) + PENDING outbox event.
            self.repo.reserve_funds_with_outbox(
                cmd,
                saga_id=saga_id,
                fencing_token=fencing_token,
            )
            relayed_events = self.relay_pending_outbox_events()

            # Step 2: Settle OR compensate on simulated downstream settlement failure.
            dlq_event: DeadLetterEvent | None = None
            if cmd.simulate_settlement_failure:
                final_saga, _comp_outbox, dlq_event, _comp_entries = (
                    self.repo.compensate_saga_with_outbox(
                        saga_id,
                        failure_reason=_SETTLEMENT_FAILURE_REASON,
                    )
                )
                relayed_events.extend(self.relay_pending_outbox_events())
                dlq_kafka_payload: dict[str, Any] = {
                    "dlq_id": dlq_event.dlq_id,
                    "event_id": dlq_event.event_id,
                    "aggregate_id": final_saga.saga_id,
                    "saga_id": final_saga.saga_id,
                    "reason": dlq_event.reason,
                    "failed_at": dlq_event.failed_at,
                    "original_payload": json.loads(dlq_event.payload_json),
                }
                dlq_kafka_record = self.kafka_broker.publish(
                    topic=dlq_event.topic,
                    event_id=dlq_event.dlq_id,
                    event_type="TransferDeadLettered",
                    payload=dlq_kafka_payload,
                )
                relayed_events.append(dlq_kafka_record)
            else:
                final_saga, _settle_outbox, _settle_entries = (
                    self.repo.settle_saga_with_outbox(saga_id)
                )
                relayed_events.extend(self.relay_pending_outbox_events())

            ledger_entries = [
                entry.to_dict()
                for entry in self.repo.list_ledger_entries(transaction_id=final_saga.saga_id)
            ]
            zero_sum_audit = self.repo.compute_global_zero_sum_audit()

            response: dict[str, Any] = {
                "idempotent_replay": False,
                "saga": final_saga.to_dict(),
                "ledger_entries": ledger_entries,
                "outbox_relayed": relayed_events,
                "zero_sum_audit": zero_sum_audit,
            }
            if dlq_event is not None:
                response["dlq_event"] = dlq_event.to_dict()

            return 201, response
        finally:
            self.lock_manager.release(cmd.source_account_id, owner=saga_id)

    def relay_pending_outbox_events(self, limit: int = 100) -> list[dict[str, Any]]:
        """Poll pending outbox events FIFO, publish to Kafka, and mark each PUBLISHED."""
        pending = self.repo.list_pending_outbox_events(limit=limit)
        relayed: list[dict[str, Any]] = []
        for event in pending:
            payload = json.loads(event.payload_json)
            kafka_record = self.kafka_broker.publish(
                topic=event.topic,
                event_id=event.event_id,
                event_type=event.event_type,
                payload=payload,
            )
            self.repo.mark_outbox_published(event.event_id)
            relayed.append(kafka_record)
        return relayed

    def list_sagas(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return persisted Saga instances ordered from newest to oldest."""
        return [saga.to_dict() for saga in self.repo.list_sagas(limit=limit)]

    def get_saga(self, saga_id: str) -> dict[str, Any]:
        """Return a single Saga instance and its associated ledger entries, or raise 404."""
        if not isinstance(saga_id, str) or not saga_id.strip():
            raise InvalidTransferError(
                "Parameter 'saga_id' must be a non-empty string.",
                details={"field": "saga_id"},
            )
        saga = self.repo.get_saga(saga_id.strip())
        if saga is None:
            raise AccountNotFoundError(
                saga_id.strip(),
                message=f"Saga '{saga_id.strip()}' was not found.",
            )
        saga_dict = saga.to_dict()
        entries = [
            entry.to_dict()
            for entry in self.repo.list_ledger_entries(transaction_id=saga.saga_id)
        ]
        return {
            **saga_dict,
            "saga": saga_dict,
            "ledger_entries": entries,
        }

    def list_ledger_entries(
        self,
        transaction_id: str | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        """Return append-only ledger entries, optionally filtered by ``transaction_id``."""
        normalized_tx = (
            transaction_id.strip()
            if isinstance(transaction_id, str) and transaction_id.strip()
            else None
        )
        return [
            entry.to_dict()
            for entry in self.repo.list_ledger_entries(
                transaction_id=normalized_tx,
                limit=limit,
            )
        ]

    def get_zero_sum_audit(self) -> dict[str, Any]:
        """Return real-time global and per-currency DEBIT == CREDIT audit report."""
        return self.repo.compute_global_zero_sum_audit()

    def list_outbox_events(self, limit: int = 100) -> list[dict[str, Any]]:
        """Return persisted Transactional Outbox events ordered newest to oldest."""
        return [event.to_dict() for event in self.repo.list_outbox_events(limit=limit)]

    def list_kafka_events(
        self,
        topic: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return published Kafka topic events, optionally filtered by ``topic``."""
        normalized_topic = (
            topic.strip() if isinstance(topic, str) and topic.strip() else None
        )
        return self.kafka_broker.list_topic_events(
            topic=normalized_topic,
            limit=limit,
        )

    def list_dlq_events(self, limit: int = 50) -> list[dict[str, Any]]:
        """Return persisted Dead-Letter Queue (DLQ) events ordered newest to oldest."""
        return [event.to_dict() for event in self.repo.list_dead_letter_events(limit=limit)]


__all__ = ["LedgerCoreApplicationService"]
