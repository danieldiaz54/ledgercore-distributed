# ADR-002: SHA-256 Idempotency Keys & Append-Only Double-Entry Compensation

> **Status**: Accepted  
> **Date**: 2026-11-18  
> **Decision Makers**: Integration Architect, Domain Architect, Data Architect  
> **Related Specs**: `.specs/01-domain/saga-outbox-double-entry-ledger.spec.md`, `.specs/02-data/transactional-outbox-and-immutable-ledger.spec.md`, `.specs/03-integration/saga-api-outbox-relay-and-java-service.spec.md`

---

## 1. Context

Payment and money-movement APIs operate over unreliable networks where clients, load balancers, and mobile SDKs routinely retry timed-out `POST /api/v1/transfers` requests. Without deterministic idempotency controls, a single network retry can double-charge a customer account.

Simultaneously, financial audits and regulatory compliance (SOX, PCI-DSS, IFRS/GAAP double-entry standards) prohibit mutating or deleting historical accounting rows when a downstream settlement partner fails. A failed transfer cannot simply run `DELETE FROM ledger_entries` or overwrite balances in place without destroying the cryptographic audit trail.

---

## 2. Decision

We enforce two complementary invariants across both the Python 3.12 runtime and the Java 21 + Spring Boot 3 microservice blueprint:

### 2.1 Deterministic `Idempotency-Key` + SHA-256 Request Fingerprinting
1. Every `POST /api/v1/transfers` request MUST supply a non-empty `Idempotency-Key` header (or `idempotency_key` JSON field).
2. The domain layer (`TransferCommand`) computes a deterministic SHA-256 hex digest over the canonical JSON representation (`sort_keys=True`, compact separators) of `{source_account_id, destination_account_id, amount_cents, currency, simulate_settlement_failure}`.
3. The `(idempotency_key, request_fingerprint, saga_id)` tuple is stored in `idempotency_records` inside the **same ACID transaction** as the initial fund reservation.
4. On subsequent requests carrying an existing `idempotency_key`:
   - **Exact Fingerprint Match**: Return `200 OK` with `idempotent_replay: true` and the original Saga + ledger entries without debiting accounts or emitting duplicate outbox events.
   - **Fingerprint Mismatch**: Reject immediately with `409 Conflict` (`ERR-409-IDEMPOTENCY-CONFLICT`) so a client bug cannot silently reuse an old key for a different amount or recipient.

### 2.2 Exact Integer-Cent Append-Only Double-Entry Accounting & Compensation
1. **Zero Floating-Point Money**: All amounts and balances use exact integer minor units (`balance_cents`, `amount_cents`). Floats and Python booleans are explicitly rejected with `400 Bad Request` (`ERR-400-INVALID-TRANSFER`).
2. **Database-Enforced Immutability**: `BEFORE UPDATE` and `BEFORE DELETE` triggers on `ledger_entries` (in both PostgreSQL `migrations/001_init_ledgercore_outbox.sql` and `SQLiteLedgerRepository`) abort any attempt to alter historical entries with `APPEND_ONLY_LEDGER_VIOLATION`.
3. **Zero-Sum Invariant (`SUM(DEBIT) == SUM(CREDIT)`)**: Every accounting phase posts a balanced pair validated by `DoubleEntryValidator.validate_balanced` before SQL insertion:
   - **Reserve (`TRANSFER_RESERVE`)**: `DEBIT source_account` + `CREDIT ACC-ESCROW-CLEARING`.
   - **Settle (`TRANSFER_SETTLE`)**: `DEBIT ACC-ESCROW-CLEARING` + `CREDIT destination_account`.
   - **Compensate (`SAGA_COMPENSATION`)**: `DEBIT ACC-ESCROW-CLEARING` + `CREDIT source_account`, restoring the source account balance to its exact pre-transfer amount while preserving full forensic history and archiving the root cause in `dead_letter_events` + Kafka topic `ledger.transfers.dlq`.

---

## 3. Alternatives Considered

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **Best-Effort In-Memory Idempotency Cache (TTL-only)** | Fast reads | Loses keys on restart or failover; vulnerable to duplicate charges after reboot | **Rejected** |
| **In-Place Row Deletion / UPDATE on Saga Failure** | Smaller table size | Destroys audit trail, breaks compliance, hides settlement failures | **Rejected** |
| **Floating-Point / IEEE-754 Dollars** | Simpler display formatting | Accumulates binary rounding errors (`0.1 + 0.2 != 0.3`), violates zero-sum invariant | **Rejected** |
| **ACID `idempotency_records` + SHA-256 Fingerprint + Append-Only Reversal Entries** | Crash-safe deduplication, payload tamper detection (`409`), mathematically provable zero-sum audit at all times | Slightly higher storage growth (4 ledger rows per transfer) | **Accepted** |

---

## 4. Consequences

### Positive
- Clients can safely retry any timed-out `POST /api/v1/transfers` call indefinitely using the same `Idempotency-Key` and observe `200 OK` (`idempotent_replay: true`) with zero balance drift.
- `GET /api/v1/ledger/audit` mathematically proves `total_debits_cents == total_credits_cents` (`imbalance_cents == 0`) across every currency after both `SETTLED` and `COMPENSATED` transfers.
- Every downstream settlement failure leaves an immutable double-entry reversal trail and a Dead-Letter Queue record (`GET /api/v1/dlq` and Kafka `ledger.transfers.dlq`) for operational triage.

### Trade-offs & Mitigations
- Ledger storage grows strictly monotonically (append-only). Mitigated in production PostgreSQL via time-range table partitioning on `created_at` and indexed queries on `(transaction_id, created_at)` and `(account_id, created_at DESC)`.
