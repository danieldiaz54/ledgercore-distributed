"""Domain invariants, Saga/Outbox primitives, lock fencing, Kafka deduplication, and AST purity tests."""

from __future__ import annotations

import ast
from dataclasses import FrozenInstanceError
from pathlib import Path
import threading
import time
import unittest

from src.domain.exceptions import (
    DistributedLockContentionError,
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

REPO_ROOT = Path(__file__).resolve().parents[1]
DOMAIN_DIR = REPO_ROOT / "src" / "domain"

FORBIDDEN_DOMAIN_IMPORT_PREFIXES: tuple[str, ...] = (
    "sqlite3",
    "http",
    "aiohttp",
    "fastapi",
    "starlette",
    "flask",
    "django",
    "urllib",
    "requests",
    "sqlalchemy",
    "psycopg",
    "redis",
    "kafka",
    "confluent_kafka",
    "src.infrastructure",
    "src.interfaces",
    "src.application",
)


class TestAccountAndLedgerEntryInvariants(unittest.TestCase):
    """Verify exact integer-cent validation, currency rules, and immutability on core models."""

    def test_account_accepts_valid_exact_integer_cents_and_is_frozen(self) -> None:
        zero_account = Account(
            account_id="ACC-ESCROW-CLEARING",
            owner_name="Escrow Pool",
            currency="USD",
            balance_cents=0,
        )
        self.assertEqual(zero_account.balance_cents, 0)
        self.assertEqual(zero_account.status, "ACTIVE")
        self.assertEqual(zero_account.version, 1)
        self.assertEqual(zero_account.to_dict()["balance_cents"], 0)

        with self.assertRaises(FrozenInstanceError):
            zero_account.balance_cents = 500  # type: ignore[misc]

    def test_account_rejects_float_bool_negative_and_non_int_balances(self) -> None:
        invalid_balances: list[object] = [
            100.0,
            0.01,
            -1,
            -50000,
            True,
            False,
            "1000",
            None,
        ]
        for bad_balance in invalid_balances:
            with self.subTest(balance_cents=bad_balance):
                with self.assertRaises(InvalidTransferError) as ctx:
                    Account(
                        account_id="ACC-001",
                        owner_name="Alice",
                        currency="USD",
                        balance_cents=bad_balance,  # type: ignore[arg-type]
                    )
                self.assertEqual(ctx.exception.code, "ERR-400-INVALID-TRANSFER")

    def test_account_rejects_invalid_currencies_and_metadata(self) -> None:
        for bad_currency in ("usd", "US", "USDD", "123", "", "   ", None, 840):
            with self.subTest(currency=bad_currency):
                with self.assertRaises(InvalidTransferError):
                    Account(
                        account_id="ACC-001",
                        owner_name="Alice",
                        currency=bad_currency,  # type: ignore[arg-type]
                        balance_cents=1000,
                    )

        with self.assertRaises(InvalidTransferError):
            Account(account_id="  ", owner_name="Alice", currency="USD", balance_cents=100)
        with self.assertRaises(InvalidTransferError):
            Account(account_id="ACC-1", owner_name="", currency="USD", balance_cents=100)
        with self.assertRaises(InvalidTransferError):
            Account(
                account_id="ACC-1",
                owner_name="Alice",
                currency="USD",
                balance_cents=100,
                version=0,
            )
        with self.assertRaises(InvalidTransferError):
            Account(
                account_id="ACC-1",
                owner_name="Alice",
                currency="USD",
                balance_cents=100,
                version=True,  # type: ignore[arg-type]
            )

    def test_account_ensure_sufficient_funds_boundary_and_errors(self) -> None:
        account = Account(
            account_id="ACC-CUSTOMER-003",
            owner_name="Client",
            currency="USD",
            balance_cents=10_000,
        )
        # Exact balance boundary must succeed without raising.
        account.ensure_sufficient_funds(10_000)
        account.ensure_sufficient_funds(1)

        with self.assertRaises(InsufficientFundsError) as ctx:
            account.ensure_sufficient_funds(10_001)
        self.assertEqual(ctx.exception.code, "ERR-422-INSUFFICIENT-FUNDS")
        self.assertEqual(ctx.exception.account_id, "ACC-CUSTOMER-003")
        self.assertEqual(ctx.exception.available_cents, 10_000)
        self.assertEqual(ctx.exception.requested_cents, 10_001)

        for invalid_requested in (0, -50, 10.5, True, False):
            with self.subTest(requested_cents=invalid_requested):
                with self.assertRaises(InvalidTransferError):
                    account.ensure_sufficient_funds(invalid_requested)  # type: ignore[arg-type]

    def test_ledger_entry_rejects_float_bool_zero_negative_and_bad_currency(self) -> None:
        valid_entry = LedgerEntry(
            entry_id="ENT-001",
            transaction_id="SAGA-001",
            account_id="ACC-TREASURY-001",
            direction=EntryDirection.DEBIT,
            amount_cents=25_000,
            currency="USD",
            entry_type="TRANSFER_RESERVE",
            created_at="2026-11-18T22:00:00+00:00",
        )
        self.assertEqual(valid_entry.to_dict()["direction"], "DEBIT")

        with self.assertRaises(FrozenInstanceError):
            valid_entry.amount_cents = 1  # type: ignore[misc]

        for bad_amount in (0, -1, -100, 25.5, 25.0, True, False, "250"):
            with self.subTest(amount_cents=bad_amount):
                with self.assertRaises(InvalidTransferError):
                    LedgerEntry(
                        entry_id="ENT-BAD",
                        transaction_id="SAGA-001",
                        account_id="ACC-TREASURY-001",
                        direction=EntryDirection.DEBIT,
                        amount_cents=bad_amount,  # type: ignore[arg-type]
                        currency="USD",
                        entry_type="TRANSFER_RESERVE",
                        created_at="2026-11-18T22:00:00+00:00",
                    )

        for bad_currency in ("eur", "US", "EURO", "12A", ""):
            with self.subTest(currency=bad_currency):
                with self.assertRaises(InvalidTransferError):
                    LedgerEntry(
                        entry_id="ENT-BAD",
                        transaction_id="SAGA-001",
                        account_id="ACC-TREASURY-001",
                        direction=EntryDirection.CREDIT,
                        amount_cents=500,
                        currency=bad_currency,
                        entry_type="TRANSFER_RESERVE",
                        created_at="2026-11-18T22:00:00+00:00",
                    )

        # Plain string direction (not EntryDirection enum member) must be rejected.
        with self.assertRaises(InvalidTransferError):
            LedgerEntry(
                entry_id="ENT-BAD",
                transaction_id="SAGA-001",
                account_id="ACC-TREASURY-001",
                direction="DEBIT",  # type: ignore[arg-type]
                amount_cents=500,
                currency="USD",
                entry_type="TRANSFER_RESERVE",
                created_at="2026-11-18T22:00:00+00:00",
            )


class TestDoubleEntryValidator(unittest.TestCase):
    """Verify DoubleEntryValidator zero-sum accounting invariant."""

    @staticmethod
    def _make_entry(
        entry_id: str,
        direction: EntryDirection,
        amount_cents: int,
        *,
        transaction_id: str = "SAGA-TX-001",
        account_id: str = "ACC-TREASURY-001",
        currency: str = "USD",
    ) -> LedgerEntry:
        return LedgerEntry(
            entry_id=entry_id,
            transaction_id=transaction_id,
            account_id=account_id,
            direction=direction,
            amount_cents=amount_cents,
            currency=currency,
            entry_type="TRANSFER_RESERVE",
            created_at="2026-11-18T22:00:00+00:00",
        )

    def test_balanced_pair_and_multi_leg_transactions_pass(self) -> None:
        debit = self._make_entry("E1", EntryDirection.DEBIT, 50_000, account_id="ACC-A")
        credit = self._make_entry("E2", EntryDirection.CREDIT, 50_000, account_id="ACC-B")
        DoubleEntryValidator.validate_balanced("SAGA-TX-001", (debit, credit))

        # Multi-leg split and multi-currency balanced postings also pass.
        split_entries = [
            self._make_entry("E3", EntryDirection.DEBIT, 10_000, currency="USD"),
            self._make_entry("E4", EntryDirection.CREDIT, 7_000, currency="USD"),
            self._make_entry("E5", EntryDirection.CREDIT, 3_000, currency="USD"),
            self._make_entry("E6", EntryDirection.DEBIT, 4_500, currency="EUR"),
            self._make_entry("E7", EntryDirection.CREDIT, 4_500, currency="EUR"),
        ]
        DoubleEntryValidator.validate_balanced("SAGA-TX-001", split_entries)

    def test_single_entry_and_empty_entries_raise_zero_sum_violation(self) -> None:
        with self.assertRaises(ZeroSumInvariantViolationError) as empty_ctx:
            DoubleEntryValidator.validate_balanced("SAGA-TX-001", [])
        self.assertEqual(empty_ctx.exception.code, "ERR-500-ZERO-SUM-VIOLATION")
        self.assertEqual(empty_ctx.exception.debit_total, 0)
        self.assertEqual(empty_ctx.exception.credit_total, 0)

        single = self._make_entry("E1", EntryDirection.DEBIT, 5_000)
        with self.assertRaises(ZeroSumInvariantViolationError) as single_ctx:
            DoubleEntryValidator.validate_balanced("SAGA-TX-001", [single])
        self.assertEqual(single_ctx.exception.transaction_id, "SAGA-TX-001")
        self.assertEqual(single_ctx.exception.debit_total, 5_000)
        self.assertEqual(single_ctx.exception.credit_total, 0)

    def test_unequal_debit_and_credit_totals_raise_zero_sum_violation(self) -> None:
        debit = self._make_entry("E1", EntryDirection.DEBIT, 50_000)
        credit = self._make_entry("E2", EntryDirection.CREDIT, 49_999)
        with self.assertRaises(ZeroSumInvariantViolationError) as ctx:
            DoubleEntryValidator.validate_balanced("SAGA-TX-001", (debit, credit))
        self.assertEqual(ctx.exception.code, "ERR-500-ZERO-SUM-VIOLATION")
        self.assertEqual(ctx.exception.debit_total, 50_000)
        self.assertEqual(ctx.exception.credit_total, 49_999)
        self.assertEqual(ctx.exception.currency, "USD")

    def test_cross_currency_mismatch_raises_zero_sum_violation(self) -> None:
        debit_usd = self._make_entry("E1", EntryDirection.DEBIT, 50_000, currency="USD")
        credit_eur = self._make_entry("E2", EntryDirection.CREDIT, 50_000, currency="EUR")
        with self.assertRaises(ZeroSumInvariantViolationError) as ctx:
            DoubleEntryValidator.validate_balanced("SAGA-TX-001", (debit_usd, credit_eur))
        self.assertEqual(ctx.exception.code, "ERR-500-ZERO-SUM-VIOLATION")
        self.assertIn(ctx.exception.currency, {"USD", "EUR"})

    def test_mismatched_transaction_id_raises_invalid_transfer_error(self) -> None:
        e1 = self._make_entry("E1", EntryDirection.DEBIT, 1_000, transaction_id="SAGA-TX-001")
        e2 = self._make_entry("E2", EntryDirection.CREDIT, 1_000, transaction_id="SAGA-OTHER")
        with self.assertRaises(InvalidTransferError):
            DoubleEntryValidator.validate_balanced("SAGA-TX-001", (e1, e2))


class TestTransferCommandAndSagaPrimitives(unittest.TestCase):
    """Verify deterministic SHA-256 fingerprinting and Saga/Outbox/DLQ model guards."""

    def test_transfer_command_deterministic_sha256_fingerprint(self) -> None:
        cmd1 = TransferCommand(
            idempotency_key="idem-001",
            source_account_id="ACC-TREASURY-001",
            destination_account_id="ACC-MERCHANT-002",
            amount_cents=150_000,
            currency="USD",
            simulate_settlement_failure=False,
        )
        cmd2 = TransferCommand(
            idempotency_key="idem-001",
            source_account_id="ACC-TREASURY-001",
            destination_account_id="ACC-MERCHANT-002",
            amount_cents=150_000,
            currency="USD",
            simulate_settlement_failure=False,
        )
        cmd_different_key_same_payload = TransferCommand(
            idempotency_key="idem-002",
            source_account_id="ACC-TREASURY-001",
            destination_account_id="ACC-MERCHANT-002",
            amount_cents=150_000,
            currency="USD",
            simulate_settlement_failure=False,
        )
        cmd_mutated_amount = TransferCommand(
            idempotency_key="idem-001",
            source_account_id="ACC-TREASURY-001",
            destination_account_id="ACC-MERCHANT-002",
            amount_cents=150_001,
            currency="USD",
            simulate_settlement_failure=False,
        )
        cmd_mutated_flag = TransferCommand(
            idempotency_key="idem-001",
            source_account_id="ACC-TREASURY-001",
            destination_account_id="ACC-MERCHANT-002",
            amount_cents=150_000,
            currency="USD",
            simulate_settlement_failure=True,
        )

        self.assertEqual(len(cmd1.request_fingerprint), 64)
        self.assertRegex(cmd1.request_fingerprint, r"^[0-9a-f]{64}$")
        self.assertEqual(cmd1.request_fingerprint, cmd2.request_fingerprint)
        self.assertEqual(
            cmd1.request_fingerprint,
            cmd_different_key_same_payload.request_fingerprint,
        )
        self.assertNotEqual(cmd1.request_fingerprint, cmd_mutated_amount.request_fingerprint)
        self.assertNotEqual(cmd1.request_fingerprint, cmd_mutated_flag.request_fingerprint)

    def test_transfer_command_rejects_self_transfer_empty_key_and_non_positive_amounts(self) -> None:
        # Self-transfer rejected (exact and whitespace-padded).
        with self.assertRaises(InvalidTransferError):
            TransferCommand(
                idempotency_key="idem-self",
                source_account_id="ACC-TREASURY-001",
                destination_account_id="ACC-TREASURY-001",
                amount_cents=1_000,
            )
        with self.assertRaises(InvalidTransferError):
            TransferCommand(
                idempotency_key="idem-self-ws",
                source_account_id="ACC-TREASURY-001",
                destination_account_id=" ACC-TREASURY-001 ",
                amount_cents=1_000,
            )

        # Empty or whitespace-only idempotency key rejected.
        for bad_key in ("", "   ", None):
            with self.subTest(idempotency_key=bad_key):
                with self.assertRaises(InvalidTransferError):
                    TransferCommand(
                        idempotency_key=bad_key,  # type: ignore[arg-type]
                        source_account_id="ACC-TREASURY-001",
                        destination_account_id="ACC-MERCHANT-002",
                        amount_cents=1_000,
                    )

        # Non-positive, float, or bool amounts rejected.
        for bad_amount in (0, -1, -500, 10.25, True, False):
            with self.subTest(amount_cents=bad_amount):
                with self.assertRaises(InvalidTransferError):
                    TransferCommand(
                        idempotency_key="idem-bad-amt",
                        source_account_id="ACC-TREASURY-001",
                        destination_account_id="ACC-MERCHANT-002",
                        amount_cents=bad_amount,  # type: ignore[arg-type]
                    )

        # Non-boolean simulate_settlement_failure rejected.
        with self.assertRaises(InvalidTransferError):
            TransferCommand(
                idempotency_key="idem-bad-flag",
                source_account_id="ACC-TREASURY-001",
                destination_account_id="ACC-MERCHANT-002",
                amount_cents=500,
                simulate_settlement_failure="true",  # type: ignore[arg-type]
            )

    def test_saga_status_transitions_and_entity_invariants(self) -> None:
        self.assertTrue(SagaStatus.INITIATED.can_transition_to(SagaStatus.RESERVED))
        self.assertFalse(SagaStatus.INITIATED.can_transition_to(SagaStatus.SETTLED))
        self.assertTrue(SagaStatus.RESERVED.can_transition_to(SagaStatus.SETTLED))
        self.assertTrue(SagaStatus.RESERVED.can_transition_to(SagaStatus.COMPENSATING))
        self.assertTrue(SagaStatus.COMPENSATING.can_transition_to(SagaStatus.COMPENSATED))
        self.assertFalse(SagaStatus.SETTLED.can_transition_to(SagaStatus.COMPENSATED))
        self.assertFalse(SagaStatus.COMPENSATED.can_transition_to(SagaStatus.SETTLED))

        # SagaInstance rejects invalid fencing_token (< 1 or bool).
        with self.assertRaises(InvalidTransferError):
            SagaInstance(
                saga_id="SAGA-1",
                idempotency_key="idem-1",
                request_fingerprint="a" * 64,
                source_account_id="ACC-1",
                destination_account_id="ACC-2",
                amount_cents=500,
                currency="USD",
                status=SagaStatus.RESERVED,
                fencing_token=0,
                failure_reason=None,
                created_at="2026-11-18T22:00:00+00:00",
                updated_at="2026-11-18T22:00:00+00:00",
            )

        # OutboxEvent rejects invalid JSON and PUBLISHED without published_at.
        with self.assertRaises(InvalidTransferError):
            OutboxEvent(
                event_id="EVT-1",
                aggregate_id="SAGA-1",
                topic="ledger.transfers.reserved",
                event_type="TransferFundsReserved",
                payload_json="{invalid-json",
                status=OutboxStatus.PENDING,
                retry_count=0,
                created_at="2026-11-18T22:00:00+00:00",
                published_at=None,
            )
        with self.assertRaises(InvalidTransferError):
            OutboxEvent(
                event_id="EVT-2",
                aggregate_id="SAGA-1",
                topic="ledger.transfers.reserved",
                event_type="TransferFundsReserved",
                payload_json="{}",
                status=OutboxStatus.PUBLISHED,
                retry_count=0,
                created_at="2026-11-18T22:00:00+00:00",
                published_at=None,
            )

        # DeadLetterEvent rejects malformed payload_json.
        with self.assertRaises(InvalidTransferError):
            DeadLetterEvent(
                dlq_id="DLQ-1",
                event_id="EVT-1",
                topic="ledger.transfers.dlq",
                reason="timeout",
                payload_json="not-json",
                failed_at="2026-11-18T22:00:00+00:00",
            )


class TestDistributedLockManager(unittest.TestCase):
    """Verify monotonic fencing tokens, contention errors, unauthorized release, and thread safety."""

    def test_monotonic_fencing_tokens_contention_and_unauthorized_release(self) -> None:
        locks = DistributedLockManager()
        self.assertEqual(locks.current_fencing_token(), 0)

        t1 = locks.acquire("ACC-TREASURY-001", owner="SAGA-A")
        self.assertEqual(t1, 1)
        self.assertEqual(locks.current_fencing_token(), 1)

        # Competing owner must be rejected with DistributedLockContentionError.
        with self.assertRaises(DistributedLockContentionError) as cont_ctx:
            locks.acquire("ACC-TREASURY-001", owner="SAGA-B")
        self.assertEqual(cont_ctx.exception.code, "ERR-409-LOCK-CONTENTION")
        self.assertEqual(cont_ctx.exception.resource_key, "ACC-TREASURY-001")
        self.assertEqual(cont_ctx.exception.current_owner, "SAGA-A")
        self.assertEqual(cont_ctx.exception.requested_owner, "SAGA-B")

        # Unauthorized release by another owner must fail and preserve lock ownership.
        with self.assertRaises(DistributedLockContentionError):
            locks.release("ACC-TREASURY-001", owner="SAGA-B")
        snapshot = locks.active_locks()
        self.assertEqual(snapshot["ACC-TREASURY-001"]["owner"], "SAGA-A")
        self.assertEqual(snapshot["ACC-TREASURY-001"]["fencing_token"], 1)

        # Defensive copy check on active_locks().
        snapshot["ACC-TREASURY-001"]["owner"] = "TAMPERED"
        self.assertEqual(locks.active_locks()["ACC-TREASURY-001"]["owner"], "SAGA-A")

        # Authorized release frees lock; subsequent acquisition receives strictly greater token.
        locks.release("ACC-TREASURY-001", owner="SAGA-A")
        self.assertEqual(locks.active_locks(), {})

        # Idempotent no-op release on already unlocked resource.
        locks.release("ACC-TREASURY-001", owner="SAGA-A")

        t2 = locks.acquire("ACC-TREASURY-001", owner="SAGA-B")
        t3 = locks.acquire("ACC-MERCHANT-002", owner="SAGA-C")
        self.assertEqual(t2, 2)
        self.assertEqual(t3, 3)
        self.assertGreater(t3, t2)
        self.assertGreater(t2, t1)

    def test_ttl_expiration_and_concurrent_lock_race(self) -> None:
        locks = DistributedLockManager()

        with self.assertRaises(InvalidTransferError):
            locks.acquire("ACC-1", owner="SAGA-1", ttl_seconds=0)
        with self.assertRaises(InvalidTransferError):
            locks.acquire("ACC-1", owner="SAGA-1", ttl_seconds=True)  # type: ignore[arg-type]

        t1 = locks.acquire("ACC-TTL", owner="SAGA-1", ttl_seconds=0.03)
        self.assertEqual(t1, 1)
        time.sleep(0.06)
        t2 = locks.acquire("ACC-TTL", owner="SAGA-2")
        self.assertEqual(t2, 2)
        locks.release("ACC-TTL", owner="SAGA-2")

        # Concurrent acquisition race: exactly 1 winner out of 16 threads.
        barrier = threading.Barrier(16)
        winners: list[tuple[str, int]] = []
        contentions: list[str] = []
        result_lock = threading.Lock()

        def worker(idx: int) -> None:
            owner = f"SAGA-THREAD-{idx}"
            barrier.wait()
            try:
                token = locks.acquire("ACC-HOT-RESOURCE", owner=owner)
                with result_lock:
                    winners.append((owner, token))
            except DistributedLockContentionError:
                with result_lock:
                    contentions.append(owner)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        self.assertEqual(len(winners), 1)
        self.assertEqual(len(contentions), 15)


class TestInMemoryKafkaBroker(unittest.TestCase):
    """Verify deterministic partitioning, monotonic offsets, and consumer-group deduplication."""

    def test_deterministic_partitioning_and_monotonic_offsets(self) -> None:
        broker = InMemoryKafkaBroker(num_partitions=4)
        payload = {"aggregate_id": "SAGA-PARTITION-TEST", "amount_cents": 12_500}

        rec1 = broker.publish(
            topic="ledger.transfers.reserved",
            event_id="EVT-1",
            event_type="TransferFundsReserved",
            payload=payload,
        )
        rec2 = broker.publish(
            topic="ledger.transfers.reserved",
            event_id="EVT-2",
            event_type="TransferFundsReserved",
            payload=payload,
        )
        rec3 = broker.publish(
            topic="ledger.transfers.settled",
            event_id="EVT-3",
            event_type="TransferSettled",
            payload=payload,
        )

        # Same aggregate_id deterministically routes to the exact same partition across topics.
        self.assertEqual(rec1["partition"], rec2["partition"])
        self.assertEqual(rec1["partition"], rec3["partition"])
        self.assertGreaterEqual(rec1["partition"], 0)
        self.assertLess(rec1["partition"], 4)

        # Per-(topic, partition) offsets increment monotonically from 0.
        self.assertEqual(rec1["offset"], 0)
        self.assertEqual(rec2["offset"], 1)
        self.assertEqual(rec3["offset"], 0)

        # Defensive deep copy ensures caller mutations do not alter stored events.
        payload["amount_cents"] = 999_999
        rec1["payload"]["amount_cents"] = 0
        stored = broker.list_topic_events(topic="ledger.transfers.reserved")
        self.assertEqual(len(stored), 2)
        self.assertEqual(stored[0]["payload"]["amount_cents"], 12_500)

    def test_consume_idempotent_deduplication_and_concurrency(self) -> None:
        broker = InMemoryKafkaBroker(num_partitions=4)

        # First delivery returns True; duplicate redeliveries return False.
        self.assertTrue(broker.consume_idempotent("settlement-group", "EVT-100"))
        self.assertFalse(broker.consume_idempotent("settlement-group", "EVT-100"))
        self.assertFalse(broker.consume_idempotent("settlement-group", "EVT-100"))

        # Independent consumer group receives the event once, then deduplicates.
        self.assertTrue(broker.consume_idempotent("audit-projection-group", "EVT-100"))
        self.assertFalse(broker.consume_idempotent("audit-projection-group", "EVT-100"))

        # Multi-threaded duplicate delivery race: exactly 1 thread returns True.
        barrier = threading.Barrier(16)
        outcomes: list[bool] = []
        outcomes_lock = threading.Lock()

        def consume_worker() -> None:
            barrier.wait()
            accepted = broker.consume_idempotent("concurrent-group", "EVT-RACE-999")
            with outcomes_lock:
                outcomes.append(accepted)

        threads = [threading.Thread(target=consume_worker) for _ in range(16)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=5)

        self.assertEqual(outcomes.count(True), 1)
        self.assertEqual(outcomes.count(False), 15)


class TestDomainPurityAST(unittest.TestCase):
    """Enforce .agents/rules/domain-purity.md via AST inspection of src/domain/*.py."""

    def test_domain_modules_have_zero_forbidden_infrastructure_imports(self) -> None:
        domain_files = sorted(DOMAIN_DIR.rglob("*.py"))
        self.assertGreaterEqual(
            len(domain_files),
            4,
            "Expected domain source files under src/domain/ for AST purity audit.",
        )

        violations: list[str] = []
        for py_file in domain_files:
            source = py_file.read_text(encoding="utf-8")
            tree = ast.parse(source, filename=str(py_file))

            imported_modules: list[str] = []
            for node in ast.walk(tree):
                if isinstance(node, ast.Import):
                    for alias in node.names:
                        imported_modules.append(alias.name)
                elif isinstance(node, ast.ImportFrom):
                    if node.module:
                        imported_modules.append(node.module)

            for mod_name in imported_modules:
                for forbidden in FORBIDDEN_DOMAIN_IMPORT_PREFIXES:
                    if mod_name == forbidden or mod_name.startswith(f"{forbidden}."):
                        violations.append(f"{py_file.name}: forbidden import '{mod_name}'")

        self.assertEqual(
            violations,
            [],
            f"Domain purity violations detected in src/domain/: {violations}",
        )


if __name__ == "__main__":
    unittest.main()
