"""Distributed Saga, Transactional Outbox, Lock Fencing, and Broker domain primitives.

All types in this module are pure Python 3.12 standard library implementations
with zero external infrastructure dependencies.
"""

from __future__ import annotations

import copy
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import hashlib
import json
import re
import threading
import time
from typing import Any

from src.domain.exceptions import (
    DistributedLockContentionError,
    InvalidTransferError,
)

_CURRENCY_RE = re.compile(r"^[A-Z]{3}$")


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


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


def _require_valid_json(value: object, field_name: str) -> None:
    _require_non_empty_str(value, field_name)
    assert isinstance(value, str)
    try:
        json.loads(value)
    except (ValueError, TypeError) as exc:
        raise InvalidTransferError(
            f"Field '{field_name}' must contain valid JSON.",
            details={"field": field_name},
        ) from exc


class SagaStatus(str, Enum):
    """Lifecycle states of a distributed transfer Saga."""

    INITIATED = "INITIATED"
    RESERVED = "RESERVED"
    SETTLED = "SETTLED"
    COMPENSATING = "COMPENSATING"
    COMPENSATED = "COMPENSATED"
    FAILED_DLQ = "FAILED_DLQ"

    def can_transition_to(self, target: SagaStatus) -> bool:
        """Return whether transitioning from this status to ``target`` is legal."""
        allowed: dict[SagaStatus, frozenset[SagaStatus]] = {
            SagaStatus.INITIATED: frozenset({SagaStatus.RESERVED, SagaStatus.FAILED_DLQ}),
            SagaStatus.RESERVED: frozenset(
                {SagaStatus.SETTLED, SagaStatus.COMPENSATING, SagaStatus.FAILED_DLQ}
            ),
            SagaStatus.COMPENSATING: frozenset({SagaStatus.COMPENSATED, SagaStatus.FAILED_DLQ}),
            SagaStatus.SETTLED: frozenset(),
            SagaStatus.COMPENSATED: frozenset(),
            SagaStatus.FAILED_DLQ: frozenset(),
        }
        return target in allowed[self]


class OutboxStatus(str, Enum):
    """Delivery states of a Transactional Outbox event."""

    PENDING = "PENDING"
    PUBLISHED = "PUBLISHED"
    DEAD_LETTERED = "DEAD_LETTERED"


@dataclass(frozen=True)
class TransferCommand:
    """Self-validating immutable transfer command with deterministic SHA-256 fingerprint."""

    idempotency_key: str
    source_account_id: str
    destination_account_id: str
    amount_cents: int
    currency: str = "USD"
    simulate_settlement_failure: bool = False
    request_fingerprint: str = ""

    def __post_init__(self) -> None:
        _require_non_empty_str(self.idempotency_key, "idempotency_key")
        _require_non_empty_str(self.source_account_id, "source_account_id")
        _require_non_empty_str(self.destination_account_id, "destination_account_id")
        if self.source_account_id.strip() == self.destination_account_id.strip():
            raise InvalidTransferError(
                "Source and destination accounts must be distinct.",
                details={
                    "source_account_id": self.source_account_id,
                    "destination_account_id": self.destination_account_id,
                },
            )
        _require_exact_int(self.amount_cents, "amount_cents", min_value=1)
        _require_currency(self.currency, "currency")
        if not isinstance(self.simulate_settlement_failure, bool):
            raise InvalidTransferError(
                "Field 'simulate_settlement_failure' must be a boolean.",
                details={"field": "simulate_settlement_failure"},
            )
        if not isinstance(self.request_fingerprint, str):
            raise InvalidTransferError(
                "Field 'request_fingerprint' must be a string.",
                details={"field": "request_fingerprint"},
            )

        if not self.request_fingerprint:
            digest = self.compute_fingerprint(
                source_account_id=self.source_account_id,
                destination_account_id=self.destination_account_id,
                amount_cents=self.amount_cents,
                currency=self.currency,
                simulate_settlement_failure=self.simulate_settlement_failure,
            )
            object.__setattr__(self, "request_fingerprint", digest)
        elif not self.request_fingerprint.strip():
            raise InvalidTransferError(
                "Field 'request_fingerprint' cannot be whitespace-only.",
                details={"field": "request_fingerprint"},
            )

    @staticmethod
    def compute_fingerprint(
        *,
        source_account_id: str,
        destination_account_id: str,
        amount_cents: int,
        currency: str,
        simulate_settlement_failure: bool,
    ) -> str:
        """Compute canonical SHA-256 hex digest for idempotency payload verification."""
        canonical_payload = json.dumps(
            {
                "source_account_id": source_account_id,
                "destination_account_id": destination_account_id,
                "amount_cents": amount_cents,
                "currency": currency,
                "simulate_settlement_failure": simulate_settlement_failure,
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(canonical_payload.encode("utf-8")).hexdigest()

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary representation of the transfer command."""
        return {
            "idempotency_key": self.idempotency_key,
            "source_account_id": self.source_account_id,
            "destination_account_id": self.destination_account_id,
            "amount_cents": self.amount_cents,
            "currency": self.currency,
            "simulate_settlement_failure": self.simulate_settlement_failure,
            "request_fingerprint": self.request_fingerprint,
        }


@dataclass(frozen=True)
class SagaInstance:
    """Immutable snapshot of a distributed transfer Saga instance."""

    saga_id: str
    idempotency_key: str
    request_fingerprint: str
    source_account_id: str
    destination_account_id: str
    amount_cents: int
    currency: str
    status: SagaStatus
    fencing_token: int
    failure_reason: str | None
    created_at: str
    updated_at: str

    def __post_init__(self) -> None:
        _require_non_empty_str(self.saga_id, "saga_id")
        _require_non_empty_str(self.idempotency_key, "idempotency_key")
        _require_non_empty_str(self.request_fingerprint, "request_fingerprint")
        _require_non_empty_str(self.source_account_id, "source_account_id")
        _require_non_empty_str(self.destination_account_id, "destination_account_id")
        if self.source_account_id.strip() == self.destination_account_id.strip():
            raise InvalidTransferError(
                "Saga source and destination accounts must be distinct.",
                details={
                    "source_account_id": self.source_account_id,
                    "destination_account_id": self.destination_account_id,
                },
            )
        _require_exact_int(self.amount_cents, "amount_cents", min_value=1)
        _require_currency(self.currency, "currency")
        if not isinstance(self.status, SagaStatus):
            raise InvalidTransferError(
                "Field 'status' must be a SagaStatus enum member.",
                details={"field": "status", "value": self.status},
            )
        _require_exact_int(self.fencing_token, "fencing_token", min_value=1)
        if self.failure_reason is not None and not isinstance(self.failure_reason, str):
            raise InvalidTransferError(
                "Field 'failure_reason' must be a string or None.",
                details={"field": "failure_reason"},
            )
        _require_non_empty_str(self.created_at, "created_at")
        _require_non_empty_str(self.updated_at, "updated_at")

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary representation of the Saga instance."""
        return {
            "saga_id": self.saga_id,
            "idempotency_key": self.idempotency_key,
            "request_fingerprint": self.request_fingerprint,
            "source_account_id": self.source_account_id,
            "destination_account_id": self.destination_account_id,
            "amount_cents": self.amount_cents,
            "currency": self.currency,
            "status": self.status.value,
            "fencing_token": self.fencing_token,
            "failure_reason": self.failure_reason,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class OutboxEvent:
    """Immutable Transactional Outbox event persisted atomically with ledger mutations."""

    event_id: str
    aggregate_id: str
    topic: str
    event_type: str
    payload_json: str
    status: OutboxStatus
    retry_count: int
    created_at: str
    published_at: str | None

    def __post_init__(self) -> None:
        _require_non_empty_str(self.event_id, "event_id")
        _require_non_empty_str(self.aggregate_id, "aggregate_id")
        _require_non_empty_str(self.topic, "topic")
        _require_non_empty_str(self.event_type, "event_type")
        _require_valid_json(self.payload_json, "payload_json")
        if not isinstance(self.status, OutboxStatus):
            raise InvalidTransferError(
                "Field 'status' must be an OutboxStatus enum member.",
                details={"field": "status", "value": self.status},
            )
        _require_exact_int(self.retry_count, "retry_count", min_value=0)
        _require_non_empty_str(self.created_at, "created_at")
        if self.published_at is not None:
            _require_non_empty_str(self.published_at, "published_at")
        if self.status is OutboxStatus.PUBLISHED and self.published_at is None:
            raise InvalidTransferError(
                "Published outbox events must include a non-empty 'published_at' timestamp.",
                details={"event_id": self.event_id},
            )

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary representation of the Outbox event."""
        return {
            "event_id": self.event_id,
            "aggregate_id": self.aggregate_id,
            "topic": self.topic,
            "event_type": self.event_type,
            "payload_json": self.payload_json,
            "status": self.status.value,
            "retry_count": self.retry_count,
            "created_at": self.created_at,
            "published_at": self.published_at,
        }


@dataclass(frozen=True)
class DeadLetterEvent:
    """Immutable Dead-Letter Queue (DLQ) record for failed settlements or poison events."""

    dlq_id: str
    event_id: str
    topic: str
    reason: str
    payload_json: str
    failed_at: str

    def __post_init__(self) -> None:
        _require_non_empty_str(self.dlq_id, "dlq_id")
        _require_non_empty_str(self.event_id, "event_id")
        _require_non_empty_str(self.topic, "topic")
        _require_non_empty_str(self.reason, "reason")
        _require_valid_json(self.payload_json, "payload_json")
        _require_non_empty_str(self.failed_at, "failed_at")

    def to_dict(self) -> dict[str, Any]:
        """Return a plain dictionary representation of the DLQ event."""
        return {
            "dlq_id": self.dlq_id,
            "event_id": self.event_id,
            "topic": self.topic,
            "reason": self.reason,
            "payload_json": self.payload_json,
            "failed_at": self.failed_at,
        }


class DistributedLockManager:
    """Thread-safe Redlock-style distributed mutex issuing monotonic fencing tokens."""

    def __init__(self) -> None:
        self._mutex = threading.Lock()
        self._fencing_counter: int = 0
        self._locks: dict[str, dict[str, Any]] = {}

    def _evict_expired_locked(self, now_monotonic: float) -> None:
        expired_keys = [
            key
            for key, entry in self._locks.items()
            if entry["expires_at_monotonic"] is not None
            and now_monotonic >= entry["expires_at_monotonic"]
        ]
        for key in expired_keys:
            del self._locks[key]

    def acquire(
        self,
        resource_key: str,
        owner: str,
        ttl_seconds: float | None = None,
    ) -> int:
        """Acquire an exclusive lock on ``resource_key`` and return a monotonic fencing token.

        Raises:
            InvalidTransferError: If ``resource_key`` or ``owner`` is empty, or ``ttl_seconds``
                is non-positive.
            DistributedLockContentionError: If ``resource_key`` is already held by another owner.
        """
        _require_non_empty_str(resource_key, "resource_key")
        _require_non_empty_str(owner, "owner")
        if ttl_seconds is not None:
            if (
                not isinstance(ttl_seconds, (int, float))
                or isinstance(ttl_seconds, bool)
                or ttl_seconds <= 0
            ):
                raise InvalidTransferError(
                    "Field 'ttl_seconds' must be a positive number when provided.",
                    details={"field": "ttl_seconds", "value": ttl_seconds},
                )

        now_monotonic = time.monotonic()
        with self._mutex:
            self._evict_expired_locked(now_monotonic)
            existing = self._locks.get(resource_key)
            if existing is not None and existing["owner"] != owner:
                raise DistributedLockContentionError(
                    resource_key=resource_key,
                    current_owner=existing["owner"],
                    requested_owner=owner,
                )

            self._fencing_counter += 1
            fencing_token = self._fencing_counter
            expires_at = (
                now_monotonic + float(ttl_seconds) if ttl_seconds is not None else None
            )
            self._locks[resource_key] = {
                "owner": owner,
                "fencing_token": fencing_token,
                "acquired_at": _utc_now_iso(),
                "expires_at_monotonic": expires_at,
            }
            return fencing_token

    def release(self, resource_key: str, owner: str) -> None:
        """Release a previously acquired lock owned by ``owner``.

        Releasing a non-existent or already-expired lock is an idempotent no-op.

        Raises:
            InvalidTransferError: If ``resource_key`` or ``owner`` is empty.
            DistributedLockContentionError: If the lock is currently held by a different owner.
        """
        _require_non_empty_str(resource_key, "resource_key")
        _require_non_empty_str(owner, "owner")

        now_monotonic = time.monotonic()
        with self._mutex:
            self._evict_expired_locked(now_monotonic)
            existing = self._locks.get(resource_key)
            if existing is None:
                return
            if existing["owner"] != owner:
                raise DistributedLockContentionError(
                    resource_key=resource_key,
                    current_owner=existing["owner"],
                    requested_owner=owner,
                    message=(
                        f"Cannot release lock on resource '{resource_key}': "
                        f"owned by '{existing['owner']}', requested by '{owner}'."
                    ),
                )
            del self._locks[resource_key]

    def active_locks(self) -> dict[str, dict[str, Any]]:
        """Return a defensive snapshot of all currently held non-expired locks."""
        now_monotonic = time.monotonic()
        with self._mutex:
            self._evict_expired_locked(now_monotonic)
            return {
                key: {
                    "owner": entry["owner"],
                    "fencing_token": entry["fencing_token"],
                    "acquired_at": entry["acquired_at"],
                }
                for key, entry in self._locks.items()
            }

    def current_fencing_token(self) -> int:
        """Return the latest issued fencing token value (0 before first acquisition)."""
        with self._mutex:
            return self._fencing_counter


class InMemoryKafkaBroker:
    """Thread-safe partitioned topic broker with effectively-once consumer deduplication."""

    def __init__(self, num_partitions: int = 4) -> None:
        _require_exact_int(num_partitions, "num_partitions", min_value=1)
        self._mutex = threading.Lock()
        self._num_partitions: int = num_partitions
        self._partition_offsets: dict[tuple[str, int], int] = {}
        self._topic_events: dict[str, list[dict[str, Any]]] = {}
        self._all_events: list[dict[str, Any]] = []
        self._consumed_events: dict[str, set[str]] = {}

    def _select_partition(self, event_id: str, payload: dict[str, Any]) -> int:
        partition_key = str(
            payload.get("aggregate_id") or payload.get("saga_id") or event_id
        )
        digest = hashlib.sha256(partition_key.encode("utf-8")).digest()
        return int.from_bytes(digest[:4], "big") % self._num_partitions

    def publish(
        self,
        topic: str,
        event_id: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        """Append an event to a deterministic topic partition and return its metadata record."""
        _require_non_empty_str(topic, "topic")
        _require_non_empty_str(event_id, "event_id")
        _require_non_empty_str(event_type, "event_type")
        if not isinstance(payload, dict):
            raise InvalidTransferError(
                "Field 'payload' must be a dictionary.",
                details={"field": "payload", "type": type(payload).__name__},
            )

        partition = self._select_partition(event_id, payload)
        with self._mutex:
            offset = self._partition_offsets.get((topic, partition), 0)
            self._partition_offsets[(topic, partition)] = offset + 1
            record: dict[str, Any] = {
                "topic": topic,
                "partition": partition,
                "offset": offset,
                "event_id": event_id,
                "event_type": event_type,
                "payload": copy.deepcopy(payload),
                "published_at": _utc_now_iso(),
            }
            self._topic_events.setdefault(topic, []).append(record)
            self._all_events.append(record)
            return copy.deepcopy(record)

    def consume_idempotent(self, consumer_group: str, event_id: str) -> bool:
        """Deduplicate event delivery per consumer group.

        Returns ``True`` on the first delivery so the consumer applies side effects,
        and ``False`` on duplicate redeliveries so they are safely ignored.
        """
        _require_non_empty_str(consumer_group, "consumer_group")
        _require_non_empty_str(event_id, "event_id")

        with self._mutex:
            seen = self._consumed_events.setdefault(consumer_group, set())
            if event_id in seen:
                return False
            seen.add(event_id)
            return True

    def list_topic_events(
        self,
        topic: str | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return defensive copies of up to the most recent ``limit`` published events."""
        _require_exact_int(limit, "limit", min_value=1)
        if topic is not None:
            _require_non_empty_str(topic, "topic")

        with self._mutex:
            source = (
                self._all_events
                if topic is None
                else self._topic_events.get(topic, [])
            )
            return copy.deepcopy(source[-limit:])


__all__ = [
    "SagaStatus",
    "OutboxStatus",
    "TransferCommand",
    "SagaInstance",
    "OutboxEvent",
    "DeadLetterEvent",
    "DistributedLockManager",
    "InMemoryKafkaBroker",
]
