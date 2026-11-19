"""Persistence layer tests: LedgerRepositoryProtocol, Transactional Outbox, ACID rollback, append-only triggers, and concurrency."""

from __future__ import annotations

import json
import threading
import unittest

from src.domain.exceptions import (
    AccountNotFoundError,
    IdempotencyConflictError,
    InsufficientFundsError,
    InvalidTransferError,
)
from src.domain.ledger import Account, EntryDirection
from src.domain.repositories import LedgerRepositoryProtocol
from src.domain.saga_outbox import OutboxStatus, SagaStatus, TransferCommand
from src.infrastructure.persistence.seed_accounts import (
    DEFAULT_ACCOUNTS,
    seed_default_accounts,
)
from src.infrastructure.persistence.sqlite_ledger_repo import (
    AppendOnlyLedgerError,
    SQLiteLedgerRepository,
)


class TestSQLiteLedgerRepositoryPersistence(unittest.TestCase):
    """Verify ACID outbox persistence, append-only database triggers, and concurrency."""

    def setUp(self) -> None:
        self.repo = SQLiteLedgerRepository(db_path=":memory:")
        seed_default_accounts(self.repo)

    def tearDown(self) -> None:
        self.repo.close()

    def test_protocol_conformance_and_idempotent_seed_default_accounts(self) -> None:
        self.assertIsInstance(self.repo, LedgerRepositoryProtocol)

        accounts = self.repo.list_accounts()
        self.assertEqual(len(accounts), 5)
        expected_ids = {acct.account_id for acct in DEFAULT_ACCOUNTS}
        self.assertEqual({acct.account_id for acct in accounts}, expected_ids)

        treasury = self.repo.get_account("ACC-TREASURY-001")
        self.assertEqual(treasury.balance_cents, 50_000_000)
        self.assertEqual(treasury.version, 1)

        # Mutate balance via a real transfer, then re-seed: existing live balances must persist.
        cmd = TransferCommand(
            idempotency_key="idem-seed-check",
            source_account_id="ACC-TREASURY-001",
            destination_account_id="ACC-MERCHANT-002",
            amount_cents=100_000,
        )
        self.repo.reserve_funds_with_outbox(cmd, saga_id="SAGA-SEED-001", fencing_token=1)
        self.repo.settle_saga_with_outbox("SAGA-SEED-001")

        reseeded = seed_default_accounts(self.repo)
        self.assertEqual(len(reseeded), 5)
        self.assertEqual(len(self.repo.list_accounts()), 5)

        treasury_after = self.repo.get_account("ACC-TREASURY-001")
        merchant_after = self.repo.get_account("ACC-MERCHANT-002")
        self.assertEqual(treasury_after.balance_cents, 49_900_000)
        self.assertEqual(treasury_after.version, 2)
        self.assertEqual(merchant_after.balance_cents, 12_600_000)
        self.assertEqual(merchant_after.version, 2)

        with self.assertRaises(AccountNotFoundError):
            self.repo.get_account("ACC-UNKNOWN-999")

    def test_atomic_reserve_and_settle_with_outbox(self) -> None:
        cmd = TransferCommand(
            idempotency_key="idem-settle-001",
            source_account_id="ACC-TREASURY-001",
            destination_account_id="ACC-MERCHANT-002",
            amount_cents=250_000,
            currency="USD",
        )

        # Step 1: Reserve into escrow + PENDING outbox event.
        reserved_saga, reserve_outbox, reserve_entries = self.repo.reserve_funds_with_outbox(
            cmd,
            saga_id="SAGA-SETTLE-001",
            fencing_token=1,
        )
        self.assertEqual(reserved_saga.status, SagaStatus.RESERVED)
        self.assertEqual(reserved_saga.fencing_token, 1)
        self.assertEqual(reserve_outbox.topic, "ledger.transfers.reserved")
        self.assertEqual(reserve_outbox.event_type, "TransferFundsReserved")
        self.assertEqual(reserve_outbox.status, OutboxStatus.PENDING)
        self.assertEqual(len(reserve_entries), 2)
        self.assertEqual(
            {e.entry_type for e in reserve_entries},
            {"TRANSFER_RESERVE"},
        )
        self.assertEqual(self.repo.get_account("ACC-TREASURY-001").balance_cents, 49_750_000)
        self.assertEqual(self.repo.get_account("ACC-ESCROW-CLEARING").balance_cents, 250_000)
        self.assertEqual(self.repo.get_account("ACC-MERCHANT-002").balance_cents, 12_500_000)

        # Idempotency record persisted atomically.
        idem = self.repo.get_idempotency_record("idem-settle-001")
        self.assertIsNotNone(idem)
        assert idem is not None
        self.assertEqual(idem["saga_id"], "SAGA-SETTLE-001")
        self.assertEqual(idem["request_fingerprint"], cmd.request_fingerprint)

        # Step 2: Settle from escrow into destination + PENDING outbox event.
        settled_saga, settle_outbox, settle_entries = self.repo.settle_saga_with_outbox(
            "SAGA-SETTLE-001"
        )
        self.assertEqual(settled_saga.status, SagaStatus.SETTLED)
        self.assertEqual(settle_outbox.topic, "ledger.transfers.settled")
        self.assertEqual(settle_outbox.event_type, "TransferSettled")
        self.assertEqual(settle_outbox.status, OutboxStatus.PENDING)
        self.assertEqual(len(settle_entries), 2)
        self.assertEqual({e.entry_type for e in settle_entries}, {"TRANSFER_SETTLE"})

        # Verify exact final balances and zero escrow residue.
        self.assertEqual(self.repo.get_account("ACC-TREASURY-001").balance_cents, 49_750_000)
        self.assertEqual(self.repo.get_account("ACC-MERCHANT-002").balance_cents, 12_750_000)
        self.assertEqual(self.repo.get_account("ACC-ESCROW-CLEARING").balance_cents, 0)

        # Outbox FIFO relay and mark_outbox_published lifecycle.
        pending = self.repo.list_pending_outbox_events()
        self.assertEqual([evt.event_id for evt in pending], [reserve_outbox.event_id, settle_outbox.event_id])
        for evt in pending:
            published = self.repo.mark_outbox_published(evt.event_id)
            self.assertEqual(published.status, OutboxStatus.PUBLISHED)
            self.assertIsNotNone(published.published_at)
        self.assertEqual(self.repo.list_pending_outbox_events(), [])

        # Global zero-sum accounting audit.
        audit = self.repo.compute_global_zero_sum_audit()
        self.assertTrue(audit["is_balanced"])
        self.assertEqual(audit["imbalance_cents"], 0)
        self.assertEqual(audit["total_entries"], 4)
        self.assertEqual(audit["total_debits_cents"], 500_000)
        self.assertEqual(audit["total_credits_cents"], 500_000)

        # Terminal SETTLED saga cannot be settled or compensated again.
        with self.assertRaises(InvalidTransferError):
            self.repo.settle_saga_with_outbox("SAGA-SETTLE-001")
        with self.assertRaises(InvalidTransferError):
            self.repo.compensate_saga_with_outbox("SAGA-SETTLE-001", "late timeout")

    def test_atomic_reserve_and_compensate_restores_balance_with_dlq_and_zero_sum(self) -> None:
        initial_source_balance = self.repo.get_account("ACC-CUSTOMER-003").balance_cents
        initial_dest_balance = self.repo.get_account("ACC-SETTLEMENT-004").balance_cents
        amount = 180_000

        cmd = TransferCommand(
            idempotency_key="idem-comp-001",
            source_account_id="ACC-CUSTOMER-003",
            destination_account_id="ACC-SETTLEMENT-004",
            amount_cents=amount,
            simulate_settlement_failure=True,
        )
        self.repo.reserve_funds_with_outbox(cmd, saga_id="SAGA-COMP-001", fencing_token=7)
        self.assertEqual(
            self.repo.get_account("ACC-CUSTOMER-003").balance_cents,
            initial_source_balance - amount,
        )
        self.assertEqual(self.repo.get_account("ACC-ESCROW-CLEARING").balance_cents, amount)

        failure_reason = "Partner bank clearing gateway timed out"
        comp_saga, comp_outbox, dlq_event, comp_entries = self.repo.compensate_saga_with_outbox(
            "SAGA-COMP-001",
            failure_reason=failure_reason,
        )

        self.assertEqual(comp_saga.status, SagaStatus.COMPENSATED)
        self.assertEqual(comp_saga.failure_reason, failure_reason)
        self.assertEqual(comp_outbox.topic, "ledger.transfers.compensated")
        self.assertEqual(comp_outbox.event_type, "TransferCompensated")
        self.assertEqual(comp_outbox.status, OutboxStatus.PENDING)

        # Verify SAGA_COMPENSATION append-only entries.
        self.assertEqual(len(comp_entries), 2)
        self.assertEqual({e.entry_type for e in comp_entries}, {"SAGA_COMPENSATION"})
        debit_entry, credit_entry = comp_entries
        self.assertEqual(debit_entry.direction, EntryDirection.DEBIT)
        self.assertEqual(debit_entry.account_id, "ACC-ESCROW-CLEARING")
        self.assertEqual(credit_entry.direction, EntryDirection.CREDIT)
        self.assertEqual(credit_entry.account_id, "ACC-CUSTOMER-003")

        # Verify source balance restored, destination untouched, escrow cleared.
        self.assertEqual(
            self.repo.get_account("ACC-CUSTOMER-003").balance_cents,
            initial_source_balance,
        )
        self.assertEqual(
            self.repo.get_account("ACC-SETTLEMENT-004").balance_cents,
            initial_dest_balance,
        )
        self.assertEqual(self.repo.get_account("ACC-ESCROW-CLEARING").balance_cents, 0)

        # Verify DLQ event persisted atomically.
        self.assertEqual(dlq_event.topic, "ledger.transfers.dlq")
        self.assertEqual(dlq_event.reason, failure_reason)
        self.assertEqual(json.loads(dlq_event.payload_json)["saga_id"], "SAGA-COMP-001")
        dlq_list = self.repo.list_dead_letter_events()
        self.assertEqual(len(dlq_list), 1)
        self.assertEqual(dlq_list[0].dlq_id, dlq_event.dlq_id)

        # All 4 append-only ledger rows (2 reserve + 2 compensation) exist and balance to zero.
        all_entries = self.repo.list_ledger_entries(transaction_id="SAGA-COMP-001")
        self.assertEqual(len(all_entries), 4)
        self.assertEqual(
            [e.entry_type for e in all_entries],
            ["TRANSFER_RESERVE", "TRANSFER_RESERVE", "SAGA_COMPENSATION", "SAGA_COMPENSATION"],
        )

        audit = self.repo.compute_global_zero_sum_audit()
        self.assertTrue(audit["is_balanced"])
        self.assertEqual(audit["imbalance_cents"], 0)
        self.assertEqual(audit["total_debits_cents"], amount * 2)
        self.assertEqual(audit["total_credits_cents"], amount * 2)

    def test_insufficient_funds_rollback_leaves_zero_partial_rows(self) -> None:
        source_before = self.repo.get_account("ACC-CUSTOMER-003")
        escrow_before = self.repo.get_account("ACC-ESCROW-CLEARING")

        overdraft_cmd = TransferCommand(
            idempotency_key="idem-overdraft-001",
            source_account_id="ACC-CUSTOMER-003",
            destination_account_id="ACC-MERCHANT-002",
            amount_cents=source_before.balance_cents + 1,
        )

        with self.assertRaises(InsufficientFundsError) as ctx:
            self.repo.reserve_funds_with_outbox(
                overdraft_cmd,
                saga_id="SAGA-OVERDRAFT-001",
                fencing_token=1,
            )
        self.assertEqual(ctx.exception.code, "ERR-422-INSUFFICIENT-FUNDS")
        self.assertEqual(ctx.exception.available_cents, source_before.balance_cents)
        self.assertEqual(ctx.exception.requested_cents, source_before.balance_cents + 1)

        # Complete ACID rollback: zero partial ledger, outbox, saga, or idempotency rows.
        self.assertEqual(self.repo.list_ledger_entries(), [])
        self.assertEqual(self.repo.list_outbox_events(), [])
        self.assertEqual(self.repo.list_sagas(), [])
        self.assertIsNone(self.repo.get_idempotency_record("idem-overdraft-001"))
        self.assertEqual(self.repo.get_account("ACC-CUSTOMER-003"), source_before)
        self.assertEqual(self.repo.get_account("ACC-ESCROW-CLEARING"), escrow_before)

        audit = self.repo.compute_global_zero_sum_audit()
        self.assertTrue(audit["is_balanced"])
        self.assertEqual(audit["total_entries"], 0)

    def test_currency_mismatch_and_duplicate_idempotency_rollback_cleanly(self) -> None:
        self.repo.upsert_account(
            Account(
                account_id="ACC-EUR-009",
                owner_name="Euro Clearing",
                currency="EUR",
                balance_cents=500_000,
            )
        )
        bad_currency_cmd = TransferCommand(
            idempotency_key="idem-eur-mismatch",
            source_account_id="ACC-EUR-009",
            destination_account_id="ACC-MERCHANT-002",
            amount_cents=10_000,
            currency="USD",
        )
        with self.assertRaises(InvalidTransferError):
            self.repo.reserve_funds_with_outbox(
                bad_currency_cmd,
                saga_id="SAGA-BAD-CCY",
                fencing_token=1,
            )
        self.assertEqual(self.repo.list_ledger_entries(), [])
        self.assertEqual(self.repo.list_outbox_events(), [])

        # Duplicate idempotency key raises IdempotencyConflictError without duplicating entries.
        valid_cmd = TransferCommand(
            idempotency_key="idem-dup-key",
            source_account_id="ACC-TREASURY-001",
            destination_account_id="ACC-MERCHANT-002",
            amount_cents=10_000,
        )
        self.repo.reserve_funds_with_outbox(valid_cmd, saga_id="SAGA-DUP-1", fencing_token=1)
        with self.assertRaises(IdempotencyConflictError):
            self.repo.reserve_funds_with_outbox(valid_cmd, saga_id="SAGA-DUP-2", fencing_token=2)
        self.assertEqual(len(self.repo.list_sagas()), 1)
        self.assertEqual(len(self.repo.list_ledger_entries()), 2)

    def test_database_level_append_only_triggers_block_update_and_delete(self) -> None:
        cmd = TransferCommand(
            idempotency_key="idem-immutable-001",
            source_account_id="ACC-TREASURY-001",
            destination_account_id="ACC-MERCHANT-002",
            amount_cents=75_000,
        )
        _, _, reserve_entries = self.repo.reserve_funds_with_outbox(
            cmd,
            saga_id="SAGA-IMMUTABLE-001",
            fencing_token=1,
        )
        target_entry = reserve_entries[0]

        # BEFORE UPDATE trigger must abort and map to AppendOnlyLedgerError.
        with self.assertRaises(AppendOnlyLedgerError) as update_ctx:
            self.repo.execute_raw_mutation_for_test(
                "UPDATE ledger_entries SET amount_cents = ? WHERE entry_id = ?",
                (1, target_entry.entry_id),
            )
        self.assertIn("APPEND_ONLY_LEDGER_VIOLATION", str(update_ctx.exception))

        # BEFORE DELETE trigger must abort on single-row and bulk delete.
        with self.assertRaises(AppendOnlyLedgerError) as delete_ctx:
            self.repo.execute_raw_mutation_for_test(
                "DELETE FROM ledger_entries WHERE entry_id = ?",
                (target_entry.entry_id,),
            )
        self.assertIn("APPEND_ONLY_LEDGER_VIOLATION", str(delete_ctx.exception))

        with self.assertRaises(AppendOnlyLedgerError):
            self.repo.execute_raw_mutation_for_test("DELETE FROM ledger_entries")

        # Verify ledger entries are completely intact and untouched.
        persisted = self.repo.list_ledger_entries(transaction_id="SAGA-IMMUTABLE-001")
        self.assertEqual(len(persisted), 2)
        matching = [e for e in persisted if e.entry_id == target_entry.entry_id]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0].amount_cents, 75_000)

    def test_consumer_deduplication_persistence(self) -> None:
        self.assertTrue(
            self.repo.record_consumer_event("ledger-projector", "EVT-123", "ledger.transfers.settled")
        )
        self.assertFalse(
            self.repo.record_consumer_event("ledger-projector", "EVT-123", "ledger.transfers.settled")
        )
        self.assertTrue(
            self.repo.record_consumer_event("risk-engine", "EVT-123", "ledger.transfers.settled")
        )

    def test_multithreaded_concurrent_transfers_preserve_exact_balances_and_zero_sum(self) -> None:
        num_settle_threads = 16
        num_compensate_threads = 8
        total_threads = num_settle_threads + num_compensate_threads
        transfer_amount = 25_000

        initial_treasury = self.repo.get_account("ACC-TREASURY-001").balance_cents
        initial_merchant = self.repo.get_account("ACC-MERCHANT-002").balance_cents

        barrier = threading.Barrier(total_threads)
        errors: list[BaseException] = []
        errors_lock = threading.Lock()

        def worker(index: int, should_compensate: bool) -> None:
            try:
                cmd = TransferCommand(
                    idempotency_key=f"idem-concurrent-{index}",
                    source_account_id="ACC-TREASURY-001",
                    destination_account_id="ACC-MERCHANT-002",
                    amount_cents=transfer_amount,
                    simulate_settlement_failure=should_compensate,
                )
                saga_id = f"SAGA-CONC-{index:03d}"
                barrier.wait()
                self.repo.reserve_funds_with_outbox(
                    cmd,
                    saga_id=saga_id,
                    fencing_token=index + 1,
                )
                if should_compensate:
                    self.repo.compensate_saga_with_outbox(
                        saga_id,
                        failure_reason="Simulated concurrent partner failure",
                    )
                else:
                    self.repo.settle_saga_with_outbox(saga_id)
            except BaseException as exc:  # noqa: BLE001
                with errors_lock:
                    errors.append(exc)

        threads: list[threading.Thread] = []
        for idx in range(num_settle_threads):
            threads.append(threading.Thread(target=worker, args=(idx, False)))
        for idx in range(num_settle_threads, total_threads):
            threads.append(threading.Thread(target=worker, args=(idx, True)))

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=15)

        self.assertEqual(errors, [], f"Unexpected thread errors: {errors}")

        expected_settled_total = num_settle_threads * transfer_amount
        self.assertEqual(
            self.repo.get_account("ACC-TREASURY-001").balance_cents,
            initial_treasury - expected_settled_total,
        )
        self.assertEqual(
            self.repo.get_account("ACC-MERCHANT-002").balance_cents,
            initial_merchant + expected_settled_total,
        )
        self.assertEqual(self.repo.get_account("ACC-ESCROW-CLEARING").balance_cents, 0)

        self.assertEqual(len(self.repo.list_sagas(limit=100)), total_threads)
        self.assertEqual(len(self.repo.list_dead_letter_events(limit=100)), num_compensate_threads)

        audit = self.repo.compute_global_zero_sum_audit()
        self.assertTrue(audit["is_balanced"])
        self.assertEqual(audit["imbalance_cents"], 0)
        self.assertEqual(audit["total_entries"], total_threads * 4)

    def test_concurrent_overdraft_race_never_allows_negative_balance(self) -> None:
        self.repo.upsert_account(
            Account(
                account_id="ACC-LIMITED-005",
                owner_name="Bounded Wallet",
                currency="USD",
                balance_cents=50_000,
            )
        )
        barrier = threading.Barrier(10)
        succeeded: list[str] = []
        rejected: list[str] = []
        other_errors: list[BaseException] = []
        lock = threading.Lock()

        def spend_worker(idx: int) -> None:
            cmd = TransferCommand(
                idempotency_key=f"idem-race-overdraft-{idx}",
                source_account_id="ACC-LIMITED-005",
                destination_account_id="ACC-MERCHANT-002",
                amount_cents=10_000,
            )
            saga_id = f"SAGA-RACE-{idx:02d}"
            barrier.wait()
            try:
                self.repo.reserve_funds_with_outbox(cmd, saga_id=saga_id, fencing_token=idx + 1)
                self.repo.settle_saga_with_outbox(saga_id)
                with lock:
                    succeeded.append(saga_id)
            except InsufficientFundsError:
                with lock:
                    rejected.append(saga_id)
            except BaseException as exc:  # noqa: BLE001
                with lock:
                    other_errors.append(exc)

        threads = [threading.Thread(target=spend_worker, args=(i,)) for i in range(10)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(other_errors, [])
        self.assertEqual(len(succeeded), 5)
        self.assertEqual(len(rejected), 5)
        self.assertEqual(self.repo.get_account("ACC-LIMITED-005").balance_cents, 0)
        self.assertEqual(self.repo.get_account("ACC-ESCROW-CLEARING").balance_cents, 0)
        audit = self.repo.compute_global_zero_sum_audit()
        self.assertTrue(audit["is_balanced"])
        self.assertEqual(audit["imbalance_cents"], 0)


if __name__ == "__main__":
    unittest.main()
