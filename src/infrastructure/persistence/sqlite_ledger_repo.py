"""SQLite Double-Entry Ledger & Transactional Outbox Repository.

Implements atomic ACID state transitions combining account balance updates,
balanced double-entry ``ledger_entries`` inserts, Saga lifecycle tracking,
idempotency records, Transactional Outbox events, Dead-Letter Queue (DLQ)
archiving, and database-enforced append-only immutability triggers.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import threading
from typing import Any
import uuid

from src.domain.exceptions import (
    AccountNotFoundError,
    IdempotencyConflictError,
    InsufficientFundsError,
    InvalidTransferError,
)
from src.domain.ledger import (
    Account,
    DoubleEntryValidator,
    EntryDirection,
    LedgerEntry,
)
from src.domain.saga_outbox import (
    DeadLetterEvent,
    OutboxEvent,
    OutboxStatus,
    SagaInstance,
    SagaStatus,
    TransferCommand,
)

_APPEND_ONLY_VIOLATION_TOKEN = "APPEND_ONLY_LEDGER_VIOLATION"
_DEFAULT_ESCROW_ACCOUNT_ID = "ACC-ESCROW-CLEARING"


class AppendOnlyLedgerError(RuntimeError):
    """Raised when an UPDATE or DELETE is attempted on append-only ``ledger_entries``."""


class SQLiteLedgerRepository:
    """Thread-safe SQLite repository enforcing Transactional Outbox and Append-Only Ledger invariants."""

    def __init__(self, db_path: str = ":memory:") -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        if db_path != ":memory:":
            parent = Path(db_path).expanduser().resolve().parent
            parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(
            db_path,
            check_same_thread=False,
            isolation_level=None,
        )
        self._conn.row_factory = sqlite3.Row
        self._initialize_schema()

    def _initialize_schema(self) -> None:
        """Create tables, indexes, and strict append-only immutability triggers."""
        ddl_script = """
        PRAGMA foreign_keys = ON;

        CREATE TABLE IF NOT EXISTS accounts (
            account_id    TEXT PRIMARY KEY,
            owner_name    TEXT NOT NULL,
            currency      TEXT NOT NULL CHECK (length(currency) = 3),
            balance_cents INTEGER NOT NULL CHECK (balance_cents >= 0),
            status        TEXT NOT NULL DEFAULT 'ACTIVE',
            version       INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
            created_at    TEXT NOT NULL,
            updated_at    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS transfer_sagas (
            saga_id                TEXT PRIMARY KEY,
            idempotency_key        TEXT NOT NULL UNIQUE,
            request_fingerprint    TEXT NOT NULL,
            source_account_id      TEXT NOT NULL REFERENCES accounts(account_id),
            destination_account_id TEXT NOT NULL REFERENCES accounts(account_id),
            amount_cents           INTEGER NOT NULL CHECK (amount_cents > 0),
            currency               TEXT NOT NULL CHECK (length(currency) = 3),
            status                 TEXT NOT NULL CHECK (
                status IN ('INITIATED', 'RESERVED', 'SETTLED', 'COMPENSATING', 'COMPENSATED', 'FAILED_DLQ')
            ),
            fencing_token          INTEGER NOT NULL CHECK (fencing_token >= 1),
            failure_reason         TEXT,
            created_at             TEXT NOT NULL,
            updated_at             TEXT NOT NULL,
            CHECK (source_account_id <> destination_account_id)
        );

        CREATE TABLE IF NOT EXISTS ledger_entries (
            entry_id       TEXT PRIMARY KEY,
            transaction_id TEXT NOT NULL,
            account_id     TEXT NOT NULL REFERENCES accounts(account_id),
            direction      TEXT NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
            amount_cents   INTEGER NOT NULL CHECK (amount_cents > 0),
            currency       TEXT NOT NULL CHECK (length(currency) = 3),
            entry_type     TEXT NOT NULL,
            created_at     TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS outbox_events (
            event_id     TEXT PRIMARY KEY,
            aggregate_id TEXT NOT NULL REFERENCES transfer_sagas(saga_id),
            topic        TEXT NOT NULL,
            event_type   TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            status       TEXT NOT NULL CHECK (status IN ('PENDING', 'PUBLISHED', 'DEAD_LETTERED')),
            retry_count  INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
            created_at   TEXT NOT NULL,
            published_at TEXT
        );

        CREATE TABLE IF NOT EXISTS idempotency_records (
            idempotency_key     TEXT PRIMARY KEY,
            request_fingerprint TEXT NOT NULL,
            saga_id             TEXT NOT NULL UNIQUE REFERENCES transfer_sagas(saga_id),
            created_at          TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS dead_letter_events (
            dlq_id       TEXT PRIMARY KEY,
            event_id     TEXT NOT NULL,
            topic        TEXT NOT NULL,
            reason       TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            failed_at    TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS processed_consumer_events (
            consumer_group TEXT NOT NULL,
            event_id       TEXT NOT NULL,
            topic          TEXT NOT NULL,
            processed_at   TEXT NOT NULL,
            PRIMARY KEY (consumer_group, event_id)
        );

        CREATE INDEX IF NOT EXISTS idx_ledger_entries_transaction_id
            ON ledger_entries (transaction_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_ledger_entries_account_id
            ON ledger_entries (account_id, created_at);

        CREATE INDEX IF NOT EXISTS idx_outbox_events_status_created
            ON outbox_events (status, created_at);

        CREATE INDEX IF NOT EXISTS idx_transfer_sagas_status
            ON transfer_sagas (status, updated_at);

        CREATE TRIGGER IF NOT EXISTS trg_ledger_entries_no_update
        BEFORE UPDATE ON ledger_entries
        BEGIN
            SELECT RAISE(ABORT, 'APPEND_ONLY_LEDGER_VIOLATION: Double-entry ledger entries are strictly append-only');
        END;

        CREATE TRIGGER IF NOT EXISTS trg_ledger_entries_no_delete
        BEFORE DELETE ON ledger_entries
        BEGIN
            SELECT RAISE(ABORT, 'APPEND_ONLY_LEDGER_VIOLATION: Double-entry ledger entries are strictly append-only');
        END;
        """
        with self._lock:
            self._conn.executescript(ddl_script)
            self._conn.execute("PRAGMA foreign_keys = ON")

    @staticmethod
    def _utc_now_iso() -> str:
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def _new_id(prefix: str) -> str:
        return f"{prefix}-{uuid.uuid4().hex[:16]}"

    @contextmanager
    def _atomic_transaction(self) -> Iterator[sqlite3.Cursor]:
        """Acquire the repository lock and run a single ACID SQLite transaction."""
        with self._lock:
            cursor = self._conn.cursor()
            cursor.execute("BEGIN IMMEDIATE")
            try:
                yield cursor
                cursor.execute("COMMIT")
            except BaseException:
                if self._conn.in_transaction:
                    cursor.execute("ROLLBACK")
                raise

    @staticmethod
    def _row_to_account(row: sqlite3.Row) -> Account:
        return Account(
            account_id=str(row["account_id"]),
            owner_name=str(row["owner_name"]),
            currency=str(row["currency"]),
            balance_cents=int(row["balance_cents"]),
            status=str(row["status"]),
            version=int(row["version"]),
        )

    @staticmethod
    def _row_to_saga(row: sqlite3.Row) -> SagaInstance:
        return SagaInstance(
            saga_id=str(row["saga_id"]),
            idempotency_key=str(row["idempotency_key"]),
            request_fingerprint=str(row["request_fingerprint"]),
            source_account_id=str(row["source_account_id"]),
            destination_account_id=str(row["destination_account_id"]),
            amount_cents=int(row["amount_cents"]),
            currency=str(row["currency"]),
            status=SagaStatus(str(row["status"])),
            fencing_token=int(row["fencing_token"]),
            failure_reason=str(row["failure_reason"]) if row["failure_reason"] is not None else None,
            created_at=str(row["created_at"]),
            updated_at=str(row["updated_at"]),
        )

    @staticmethod
    def _row_to_ledger_entry(row: sqlite3.Row) -> LedgerEntry:
        return LedgerEntry(
            entry_id=str(row["entry_id"]),
            transaction_id=str(row["transaction_id"]),
            account_id=str(row["account_id"]),
            direction=EntryDirection(str(row["direction"])),
            amount_cents=int(row["amount_cents"]),
            currency=str(row["currency"]),
            entry_type=str(row["entry_type"]),
            created_at=str(row["created_at"]),
        )

    @staticmethod
    def _row_to_outbox_event(row: sqlite3.Row) -> OutboxEvent:
        return OutboxEvent(
            event_id=str(row["event_id"]),
            aggregate_id=str(row["aggregate_id"]),
            topic=str(row["topic"]),
            event_type=str(row["event_type"]),
            payload_json=str(row["payload_json"]),
            status=OutboxStatus(str(row["status"])),
            retry_count=int(row["retry_count"]),
            created_at=str(row["created_at"]),
            published_at=str(row["published_at"]) if row["published_at"] is not None else None,
        )

    @staticmethod
    def _row_to_dead_letter_event(row: sqlite3.Row) -> DeadLetterEvent:
        return DeadLetterEvent(
            dlq_id=str(row["dlq_id"]),
            event_id=str(row["event_id"]),
            topic=str(row["topic"]),
            reason=str(row["reason"]),
            payload_json=str(row["payload_json"]),
            failed_at=str(row["failed_at"]),
        )

    def _fetch_account_in_tx(self, cursor: sqlite3.Cursor, account_id: str) -> Account:
        row = cursor.execute(
            """
            SELECT account_id, owner_name, currency, balance_cents, status, version
            FROM accounts
            WHERE account_id = ?
            """,
            (account_id,),
        ).fetchone()
        if row is None:
            raise AccountNotFoundError(account_id)
        return self._row_to_account(row)

    @staticmethod
    def _insert_ledger_entries_in_tx(
        cursor: sqlite3.Cursor,
        entries: tuple[LedgerEntry, LedgerEntry],
    ) -> None:
        cursor.executemany(
            """
            INSERT INTO ledger_entries (
                entry_id,
                transaction_id,
                account_id,
                direction,
                amount_cents,
                currency,
                entry_type,
                created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    entry.entry_id,
                    entry.transaction_id,
                    entry.account_id,
                    entry.direction.value,
                    entry.amount_cents,
                    entry.currency,
                    entry.entry_type,
                    entry.created_at,
                )
                for entry in entries
            ],
        )

    @staticmethod
    def _insert_outbox_event_in_tx(cursor: sqlite3.Cursor, event: OutboxEvent) -> None:
        cursor.execute(
            """
            INSERT INTO outbox_events (
                event_id,
                aggregate_id,
                topic,
                event_type,
                payload_json,
                status,
                retry_count,
                created_at,
                published_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.aggregate_id,
                event.topic,
                event.event_type,
                event.payload_json,
                event.status.value,
                event.retry_count,
                event.created_at,
                event.published_at,
            ),
        )

    def upsert_account(self, account: Account) -> Account:
        """Insert or update an account record atomically."""
        now = self._utc_now_iso()
        with self._atomic_transaction() as cursor:
            cursor.execute(
                """
                INSERT INTO accounts (
                    account_id,
                    owner_name,
                    currency,
                    balance_cents,
                    status,
                    version,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    owner_name = excluded.owner_name,
                    currency = excluded.currency,
                    balance_cents = excluded.balance_cents,
                    status = excluded.status,
                    version = excluded.version,
                    updated_at = excluded.updated_at
                """,
                (
                    account.account_id,
                    account.owner_name,
                    account.currency,
                    account.balance_cents,
                    account.status,
                    account.version,
                    now,
                    now,
                ),
            )
            return self._fetch_account_in_tx(cursor, account.account_id)

    def get_account(self, account_id: str) -> Account:
        """Retrieve an account snapshot by ID, raising ``AccountNotFoundError`` if absent."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT account_id, owner_name, currency, balance_cents, status, version
                FROM accounts
                WHERE account_id = ?
                """,
                (account_id,),
            ).fetchone()
        if row is None:
            raise AccountNotFoundError(account_id)
        return self._row_to_account(row)

    def list_accounts(self) -> list[Account]:
        """Return all persisted accounts ordered deterministically by ``account_id``."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT account_id, owner_name, currency, balance_cents, status, version
                FROM accounts
                ORDER BY account_id ASC
                """
            ).fetchall()
        return [self._row_to_account(row) for row in rows]

    def get_idempotency_record(self, idempotency_key: str) -> dict[str, str] | None:
        """Return the persisted idempotency mapping or ``None`` if unseen."""
        with self._lock:
            row = self._conn.execute(
                """
                SELECT idempotency_key, request_fingerprint, saga_id
                FROM idempotency_records
                WHERE idempotency_key = ?
                """,
                (idempotency_key,),
            ).fetchone()
        if row is None:
            return None
        return {
            "idempotency_key": str(row["idempotency_key"]),
            "request_fingerprint": str(row["request_fingerprint"]),
            "saga_id": str(row["saga_id"]),
        }

    def reserve_funds_with_outbox(
        self,
        command: TransferCommand,
        saga_id: str,
        fencing_token: int,
        escrow_account_id: str = _DEFAULT_ESCROW_ACCOUNT_ID,
    ) -> tuple[SagaInstance, OutboxEvent, tuple[LedgerEntry, LedgerEntry]]:
        """Atomically reserve funds into escrow, record balanced ledger entries, saga, idempotency, and outbox event."""
        if not isinstance(fencing_token, int) or isinstance(fencing_token, bool) or fencing_token < 1:
            raise InvalidTransferError(
                "Field 'fencing_token' must be a positive integer.",
                details={"fencing_token": fencing_token},
            )
        if not isinstance(saga_id, str) or not saga_id.strip():
            raise InvalidTransferError(
                "Field 'saga_id' must be a non-empty string.",
                details={"saga_id": saga_id},
            )

        now = self._utc_now_iso()
        with self._atomic_transaction() as cursor:
            existing_idem = cursor.execute(
                """
                SELECT request_fingerprint, saga_id
                FROM idempotency_records
                WHERE idempotency_key = ?
                """,
                (command.idempotency_key,),
            ).fetchone()
            if existing_idem is not None:
                raise IdempotencyConflictError(
                    idempotency_key=command.idempotency_key,
                    expected_fingerprint=str(existing_idem["request_fingerprint"]),
                    received_fingerprint=command.request_fingerprint,
                )

            source = self._fetch_account_in_tx(cursor, command.source_account_id)
            destination = self._fetch_account_in_tx(cursor, command.destination_account_id)
            escrow = self._fetch_account_in_tx(cursor, escrow_account_id)

            for account in (source, destination, escrow):
                if account.currency != command.currency:
                    raise InvalidTransferError(
                        f"Currency mismatch for account '{account.account_id}': "
                        f"expected '{command.currency}', found '{account.currency}'.",
                        details={
                            "account_id": account.account_id,
                            "expected_currency": command.currency,
                            "account_currency": account.currency,
                        },
                    )

            if source.balance_cents < command.amount_cents:
                raise InsufficientFundsError(
                    account_id=source.account_id,
                    available_cents=source.balance_cents,
                    requested_cents=command.amount_cents,
                )

            debit_entry = LedgerEntry(
                entry_id=self._new_id("ENT"),
                transaction_id=saga_id,
                account_id=source.account_id,
                direction=EntryDirection.DEBIT,
                amount_cents=command.amount_cents,
                currency=command.currency,
                entry_type="TRANSFER_RESERVE",
                created_at=now,
            )
            credit_entry = LedgerEntry(
                entry_id=self._new_id("ENT"),
                transaction_id=saga_id,
                account_id=escrow.account_id,
                direction=EntryDirection.CREDIT,
                amount_cents=command.amount_cents,
                currency=command.currency,
                entry_type="TRANSFER_RESERVE",
                created_at=now,
            )
            entries = (debit_entry, credit_entry)
            DoubleEntryValidator.validate_balanced(saga_id, entries)

            new_source_balance = source.balance_cents - command.amount_cents
            new_escrow_balance = escrow.balance_cents + command.amount_cents

            cursor.execute(
                """
                UPDATE accounts
                SET balance_cents = ?, version = version + 1, updated_at = ?
                WHERE account_id = ? AND version = ?
                """,
                (new_source_balance, now, source.account_id, source.version),
            )
            if cursor.rowcount != 1:
                raise InvalidTransferError(
                    f"Optimistic concurrency conflict updating source account '{source.account_id}'."
                )

            cursor.execute(
                """
                UPDATE accounts
                SET balance_cents = ?, version = version + 1, updated_at = ?
                WHERE account_id = ? AND version = ?
                """,
                (new_escrow_balance, now, escrow.account_id, escrow.version),
            )
            if cursor.rowcount != 1:
                raise InvalidTransferError(
                    f"Optimistic concurrency conflict updating escrow account '{escrow.account_id}'."
                )

            saga = SagaInstance(
                saga_id=saga_id,
                idempotency_key=command.idempotency_key,
                request_fingerprint=command.request_fingerprint,
                source_account_id=source.account_id,
                destination_account_id=destination.account_id,
                amount_cents=command.amount_cents,
                currency=command.currency,
                status=SagaStatus.RESERVED,
                fencing_token=fencing_token,
                failure_reason=None,
                created_at=now,
                updated_at=now,
            )

            cursor.execute(
                """
                INSERT INTO transfer_sagas (
                    saga_id,
                    idempotency_key,
                    request_fingerprint,
                    source_account_id,
                    destination_account_id,
                    amount_cents,
                    currency,
                    status,
                    fencing_token,
                    failure_reason,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    saga.saga_id,
                    saga.idempotency_key,
                    saga.request_fingerprint,
                    saga.source_account_id,
                    saga.destination_account_id,
                    saga.amount_cents,
                    saga.currency,
                    saga.status.value,
                    saga.fencing_token,
                    saga.failure_reason,
                    saga.created_at,
                    saga.updated_at,
                ),
            )

            cursor.execute(
                """
                INSERT INTO idempotency_records (
                    idempotency_key,
                    request_fingerprint,
                    saga_id,
                    created_at
                ) VALUES (?, ?, ?, ?)
                """,
                (
                    command.idempotency_key,
                    command.request_fingerprint,
                    saga.saga_id,
                    now,
                ),
            )

            self._insert_ledger_entries_in_tx(cursor, entries)

            outbox_payload = {
                "aggregate_id": saga.saga_id,
                "saga_id": saga.saga_id,
                "idempotency_key": saga.idempotency_key,
                "source_account_id": saga.source_account_id,
                "destination_account_id": saga.destination_account_id,
                "escrow_account_id": escrow.account_id,
                "amount_cents": saga.amount_cents,
                "currency": saga.currency,
                "fencing_token": saga.fencing_token,
                "status": saga.status.value,
                "entry_ids": [debit_entry.entry_id, credit_entry.entry_id],
            }
            outbox_event = OutboxEvent(
                event_id=self._new_id("EVT"),
                aggregate_id=saga.saga_id,
                topic="ledger.transfers.reserved",
                event_type="TransferFundsReserved",
                payload_json=json.dumps(outbox_payload, sort_keys=True),
                status=OutboxStatus.PENDING,
                retry_count=0,
                created_at=now,
                published_at=None,
            )
            self._insert_outbox_event_in_tx(cursor, outbox_event)

            return saga, outbox_event, entries

    def settle_saga_with_outbox(
        self,
        saga_id: str,
        escrow_account_id: str = _DEFAULT_ESCROW_ACCOUNT_ID,
    ) -> tuple[SagaInstance, OutboxEvent, tuple[LedgerEntry, LedgerEntry]]:
        """Atomically settle a RESERVED saga from escrow into destination and record outbox event."""
        now = self._utc_now_iso()
        with self._atomic_transaction() as cursor:
            saga_row = cursor.execute(
                "SELECT * FROM transfer_sagas WHERE saga_id = ?",
                (saga_id,),
            ).fetchone()
            if saga_row is None:
                raise InvalidTransferError(
                    f"Saga '{saga_id}' was not found.",
                    details={"saga_id": saga_id},
                )
            current_saga = self._row_to_saga(saga_row)
            if not current_saga.status.can_transition_to(SagaStatus.SETTLED):
                raise InvalidTransferError(
                    f"Cannot settle saga '{saga_id}' from status '{current_saga.status.value}'.",
                    details={"saga_id": saga_id, "status": current_saga.status.value},
                )

            escrow = self._fetch_account_in_tx(cursor, escrow_account_id)
            destination = self._fetch_account_in_tx(cursor, current_saga.destination_account_id)
            escrow.ensure_sufficient_funds(current_saga.amount_cents)

            debit_entry = LedgerEntry(
                entry_id=self._new_id("ENT"),
                transaction_id=current_saga.saga_id,
                account_id=escrow.account_id,
                direction=EntryDirection.DEBIT,
                amount_cents=current_saga.amount_cents,
                currency=current_saga.currency,
                entry_type="TRANSFER_SETTLE",
                created_at=now,
            )
            credit_entry = LedgerEntry(
                entry_id=self._new_id("ENT"),
                transaction_id=current_saga.saga_id,
                account_id=destination.account_id,
                direction=EntryDirection.CREDIT,
                amount_cents=current_saga.amount_cents,
                currency=current_saga.currency,
                entry_type="TRANSFER_SETTLE",
                created_at=now,
            )
            entries = (debit_entry, credit_entry)
            DoubleEntryValidator.validate_balanced(current_saga.saga_id, entries)

            cursor.execute(
                """
                UPDATE accounts
                SET balance_cents = ?, version = version + 1, updated_at = ?
                WHERE account_id = ? AND version = ?
                """,
                (
                    escrow.balance_cents - current_saga.amount_cents,
                    now,
                    escrow.account_id,
                    escrow.version,
                ),
            )
            if cursor.rowcount != 1:
                raise InvalidTransferError(
                    f"Optimistic concurrency conflict updating escrow account '{escrow.account_id}'."
                )

            cursor.execute(
                """
                UPDATE accounts
                SET balance_cents = ?, version = version + 1, updated_at = ?
                WHERE account_id = ? AND version = ?
                """,
                (
                    destination.balance_cents + current_saga.amount_cents,
                    now,
                    destination.account_id,
                    destination.version,
                ),
            )
            if cursor.rowcount != 1:
                raise InvalidTransferError(
                    f"Optimistic concurrency conflict updating destination account '{destination.account_id}'."
                )

            self._insert_ledger_entries_in_tx(cursor, entries)

            cursor.execute(
                """
                UPDATE transfer_sagas
                SET status = ?, updated_at = ?
                WHERE saga_id = ?
                """,
                (SagaStatus.SETTLED.value, now, current_saga.saga_id),
            )

            settled_saga = SagaInstance(
                saga_id=current_saga.saga_id,
                idempotency_key=current_saga.idempotency_key,
                request_fingerprint=current_saga.request_fingerprint,
                source_account_id=current_saga.source_account_id,
                destination_account_id=current_saga.destination_account_id,
                amount_cents=current_saga.amount_cents,
                currency=current_saga.currency,
                status=SagaStatus.SETTLED,
                fencing_token=current_saga.fencing_token,
                failure_reason=None,
                created_at=current_saga.created_at,
                updated_at=now,
            )

            outbox_payload = {
                "aggregate_id": settled_saga.saga_id,
                "saga_id": settled_saga.saga_id,
                "idempotency_key": settled_saga.idempotency_key,
                "source_account_id": settled_saga.source_account_id,
                "destination_account_id": settled_saga.destination_account_id,
                "escrow_account_id": escrow.account_id,
                "amount_cents": settled_saga.amount_cents,
                "currency": settled_saga.currency,
                "fencing_token": settled_saga.fencing_token,
                "status": settled_saga.status.value,
                "entry_ids": [debit_entry.entry_id, credit_entry.entry_id],
            }
            outbox_event = OutboxEvent(
                event_id=self._new_id("EVT"),
                aggregate_id=settled_saga.saga_id,
                topic="ledger.transfers.settled",
                event_type="TransferSettled",
                payload_json=json.dumps(outbox_payload, sort_keys=True),
                status=OutboxStatus.PENDING,
                retry_count=0,
                created_at=now,
                published_at=None,
            )
            self._insert_outbox_event_in_tx(cursor, outbox_event)

            return settled_saga, outbox_event, entries

    def compensate_saga_with_outbox(
        self,
        saga_id: str,
        failure_reason: str,
        escrow_account_id: str = _DEFAULT_ESCROW_ACCOUNT_ID,
    ) -> tuple[SagaInstance, OutboxEvent, DeadLetterEvent, tuple[LedgerEntry, LedgerEntry]]:
        """Atomically reverse reserved funds via append-only compensating entries, emit outbox event, and log DLQ event."""
        if not isinstance(failure_reason, str) or not failure_reason.strip():
            raise InvalidTransferError(
                "Field 'failure_reason' must be a non-empty string.",
                details={"field": "failure_reason"},
            )

        now = self._utc_now_iso()
        with self._atomic_transaction() as cursor:
            saga_row = cursor.execute(
                "SELECT * FROM transfer_sagas WHERE saga_id = ?",
                (saga_id,),
            ).fetchone()
            if saga_row is None:
                raise InvalidTransferError(
                    f"Saga '{saga_id}' was not found.",
                    details={"saga_id": saga_id},
                )
            current_saga = self._row_to_saga(saga_row)
            if current_saga.status not in {SagaStatus.RESERVED, SagaStatus.COMPENSATING}:
                raise InvalidTransferError(
                    f"Cannot compensate saga '{saga_id}' from status '{current_saga.status.value}'.",
                    details={"saga_id": saga_id, "status": current_saga.status.value},
                )

            escrow = self._fetch_account_in_tx(cursor, escrow_account_id)
            source = self._fetch_account_in_tx(cursor, current_saga.source_account_id)
            escrow.ensure_sufficient_funds(current_saga.amount_cents)

            debit_entry = LedgerEntry(
                entry_id=self._new_id("ENT"),
                transaction_id=current_saga.saga_id,
                account_id=escrow.account_id,
                direction=EntryDirection.DEBIT,
                amount_cents=current_saga.amount_cents,
                currency=current_saga.currency,
                entry_type="SAGA_COMPENSATION",
                created_at=now,
            )
            credit_entry = LedgerEntry(
                entry_id=self._new_id("ENT"),
                transaction_id=current_saga.saga_id,
                account_id=source.account_id,
                direction=EntryDirection.CREDIT,
                amount_cents=current_saga.amount_cents,
                currency=current_saga.currency,
                entry_type="SAGA_COMPENSATION",
                created_at=now,
            )
            entries = (debit_entry, credit_entry)
            DoubleEntryValidator.validate_balanced(current_saga.saga_id, entries)

            cursor.execute(
                """
                UPDATE accounts
                SET balance_cents = ?, version = version + 1, updated_at = ?
                WHERE account_id = ? AND version = ?
                """,
                (
                    escrow.balance_cents - current_saga.amount_cents,
                    now,
                    escrow.account_id,
                    escrow.version,
                ),
            )
            if cursor.rowcount != 1:
                raise InvalidTransferError(
                    f"Optimistic concurrency conflict updating escrow account '{escrow.account_id}'."
                )

            cursor.execute(
                """
                UPDATE accounts
                SET balance_cents = ?, version = version + 1, updated_at = ?
                WHERE account_id = ? AND version = ?
                """,
                (
                    source.balance_cents + current_saga.amount_cents,
                    now,
                    source.account_id,
                    source.version,
                ),
            )
            if cursor.rowcount != 1:
                raise InvalidTransferError(
                    f"Optimistic concurrency conflict updating source account '{source.account_id}'."
                )

            self._insert_ledger_entries_in_tx(cursor, entries)

            cursor.execute(
                """
                UPDATE transfer_sagas
                SET status = ?, failure_reason = ?, updated_at = ?
                WHERE saga_id = ?
                """,
                (SagaStatus.COMPENSATED.value, failure_reason, now, current_saga.saga_id),
            )

            compensated_saga = SagaInstance(
                saga_id=current_saga.saga_id,
                idempotency_key=current_saga.idempotency_key,
                request_fingerprint=current_saga.request_fingerprint,
                source_account_id=current_saga.source_account_id,
                destination_account_id=current_saga.destination_account_id,
                amount_cents=current_saga.amount_cents,
                currency=current_saga.currency,
                status=SagaStatus.COMPENSATED,
                fencing_token=current_saga.fencing_token,
                failure_reason=failure_reason,
                created_at=current_saga.created_at,
                updated_at=now,
            )

            outbox_payload = {
                "aggregate_id": compensated_saga.saga_id,
                "saga_id": compensated_saga.saga_id,
                "idempotency_key": compensated_saga.idempotency_key,
                "source_account_id": compensated_saga.source_account_id,
                "destination_account_id": compensated_saga.destination_account_id,
                "escrow_account_id": escrow.account_id,
                "amount_cents": compensated_saga.amount_cents,
                "currency": compensated_saga.currency,
                "fencing_token": compensated_saga.fencing_token,
                "status": compensated_saga.status.value,
                "failure_reason": failure_reason,
                "entry_ids": [debit_entry.entry_id, credit_entry.entry_id],
            }
            outbox_event = OutboxEvent(
                event_id=self._new_id("EVT"),
                aggregate_id=compensated_saga.saga_id,
                topic="ledger.transfers.compensated",
                event_type="TransferCompensated",
                payload_json=json.dumps(outbox_payload, sort_keys=True),
                status=OutboxStatus.PENDING,
                retry_count=0,
                created_at=now,
                published_at=None,
            )
            self._insert_outbox_event_in_tx(cursor, outbox_event)

            dlq_event = DeadLetterEvent(
                dlq_id=self._new_id("DLQ"),
                event_id=outbox_event.event_id,
                topic="ledger.transfers.dlq",
                reason=failure_reason,
                payload_json=outbox_event.payload_json,
                failed_at=now,
            )
            cursor.execute(
                """
                INSERT INTO dead_letter_events (
                    dlq_id,
                    event_id,
                    topic,
                    reason,
                    payload_json,
                    failed_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    dlq_event.dlq_id,
                    dlq_event.event_id,
                    dlq_event.topic,
                    dlq_event.reason,
                    dlq_event.payload_json,
                    dlq_event.failed_at,
                ),
            )

            return compensated_saga, outbox_event, dlq_event, entries

    def list_pending_outbox_events(self, limit: int = 100) -> list[OutboxEvent]:
        """Return oldest ``PENDING`` outbox events in FIFO order."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM outbox_events
                WHERE status = ?
                ORDER BY rowid ASC
                LIMIT ?
                """,
                (OutboxStatus.PENDING.value, limit),
            ).fetchall()
        return [self._row_to_outbox_event(row) for row in rows]

    def mark_outbox_published(self, event_id: str) -> OutboxEvent:
        """Transition an outbox event to ``PUBLISHED`` and return its updated snapshot."""
        now = self._utc_now_iso()
        with self._atomic_transaction() as cursor:
            row = cursor.execute(
                "SELECT * FROM outbox_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if row is None:
                raise InvalidTransferError(
                    f"Outbox event '{event_id}' was not found.",
                    details={"event_id": event_id},
                )
            if str(row["status"]) != OutboxStatus.PUBLISHED.value:
                cursor.execute(
                    """
                    UPDATE outbox_events
                    SET status = ?, published_at = ?
                    WHERE event_id = ?
                    """,
                    (OutboxStatus.PUBLISHED.value, now, event_id),
                )
                row = cursor.execute(
                    "SELECT * FROM outbox_events WHERE event_id = ?",
                    (event_id,),
                ).fetchone()
            assert row is not None
            return self._row_to_outbox_event(row)

    def list_outbox_events(self, limit: int = 100) -> list[OutboxEvent]:
        """Return persisted outbox events ordered from newest to oldest."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM outbox_events
                ORDER BY rowid DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_outbox_event(row) for row in rows]

    def list_sagas(self, limit: int = 100) -> list[SagaInstance]:
        """Return persisted transfer sagas ordered from newest to oldest."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM transfer_sagas
                ORDER BY rowid DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_saga(row) for row in rows]

    def get_saga(self, saga_id: str) -> SagaInstance | None:
        """Return a Saga snapshot by ``saga_id``, or ``None`` if absent."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM transfer_sagas WHERE saga_id = ?",
                (saga_id,),
            ).fetchone()
        return self._row_to_saga(row) if row is not None else None

    def list_ledger_entries(
        self,
        transaction_id: str | None = None,
        limit: int = 200,
    ) -> list[LedgerEntry]:
        """Return append-only double-entry ledger lines, optionally filtered by ``transaction_id``."""
        with self._lock:
            if transaction_id is not None:
                rows = self._conn.execute(
                    """
                    SELECT * FROM ledger_entries
                    WHERE transaction_id = ?
                    ORDER BY rowid ASC
                    LIMIT ?
                    """,
                    (transaction_id, limit),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    """
                    SELECT * FROM ledger_entries
                    ORDER BY rowid DESC
                    LIMIT ?
                    """,
                    (limit,),
                ).fetchall()
        return [self._row_to_ledger_entry(row) for row in rows]

    def list_dead_letter_events(self, limit: int = 50) -> list[DeadLetterEvent]:
        """Return persisted Dead-Letter Queue (DLQ) events ordered from newest to oldest."""
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT * FROM dead_letter_events
                ORDER BY rowid DESC
                LIMIT ?
                """,
                (limit,),
            ).fetchall()
        return [self._row_to_dead_letter_event(row) for row in rows]

    def record_consumer_event(self, consumer_group: str, event_id: str, topic: str) -> bool:
        """Persist consumer deduplication key; returns True if newly processed, False if duplicate."""
        now = self._utc_now_iso()
        with self._atomic_transaction() as cursor:
            cursor.execute(
                """
                INSERT OR IGNORE INTO processed_consumer_events (
                    consumer_group,
                    event_id,
                    topic,
                    processed_at
                ) VALUES (?, ?, ?, ?)
                """,
                (consumer_group, event_id, topic, now),
            )
            return cursor.rowcount == 1

    def compute_global_zero_sum_audit(self) -> dict[str, Any]:
        """Audit the entire ``ledger_entries`` table verifying global and per-currency DEBIT == CREDIT."""
        with self._lock:
            summary_row = self._conn.execute(
                """
                SELECT
                    COUNT(*) AS total_entries,
                    COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount_cents ELSE 0 END), 0) AS total_debits_cents,
                    COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_cents ELSE 0 END), 0) AS total_credits_cents
                FROM ledger_entries
                """
            ).fetchone()
            currency_rows = self._conn.execute(
                """
                SELECT
                    currency,
                    COALESCE(SUM(CASE WHEN direction = 'DEBIT' THEN amount_cents ELSE 0 END), 0) AS debits_cents,
                    COALESCE(SUM(CASE WHEN direction = 'CREDIT' THEN amount_cents ELSE 0 END), 0) AS credits_cents
                FROM ledger_entries
                GROUP BY currency
                ORDER BY currency ASC
                """
            ).fetchall()

        assert summary_row is not None
        total_entries = int(summary_row["total_entries"])
        total_debits_cents = int(summary_row["total_debits_cents"])
        total_credits_cents = int(summary_row["total_credits_cents"])

        by_currency: dict[str, dict[str, Any]] = {}
        all_currencies_balanced = True
        for row in currency_rows:
            currency = str(row["currency"])
            debits = int(row["debits_cents"])
            credits = int(row["credits_cents"])
            balanced = debits == credits
            if not balanced:
                all_currencies_balanced = False
            by_currency[currency] = {
                "debits_cents": debits,
                "credits_cents": credits,
                "imbalance_cents": debits - credits,
                "is_balanced": balanced,
            }

        is_balanced = (total_debits_cents == total_credits_cents) and all_currencies_balanced
        return {
            "total_entries": total_entries,
            "total_debits_cents": total_debits_cents,
            "total_credits_cents": total_credits_cents,
            "imbalance_cents": total_debits_cents - total_credits_cents,
            "is_balanced": is_balanced,
            "by_currency": by_currency,
            "audited_at": self._utc_now_iso(),
        }

    def execute_raw_mutation_for_test(
        self,
        sql: str,
        params: tuple[Any, ...] = (),
    ) -> None:
        """Execute a raw SQL statement in tests to verify database-level append-only triggers.

        Maps any SQLite trigger error containing ``APPEND_ONLY_LEDGER_VIOLATION`` to
        ``AppendOnlyLedgerError``.
        """
        try:
            with self._atomic_transaction() as cursor:
                cursor.execute(sql, params)
        except sqlite3.DatabaseError as exc:
            if _APPEND_ONLY_VIOLATION_TOKEN in str(exc):
                raise AppendOnlyLedgerError(str(exc)) from exc
            raise

    def close(self) -> None:
        """Close the underlying SQLite connection cleanly."""
        with self._lock:
            self._conn.close()
