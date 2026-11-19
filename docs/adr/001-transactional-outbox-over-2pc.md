# ADR-001: Transactional Outbox Pattern & Orchestrated Sagas over Distributed Two-Phase Commit (2PC / XA)

> **Status**: Accepted  
> **Date**: 2026-11-18  
> **Decision Makers**: Integration Architect, Domain Architect, Data Architect  
> **Related Specs**: `.specs/01-domain/saga-outbox-double-entry-ledger.spec.md`, `.specs/02-data/transactional-outbox-and-immutable-ledger.spec.md`, `.specs/03-integration/saga-api-outbox-relay-and-java-service.spec.md`

---

## 1. Context

In a distributed financial ledger spanning a relational ledger database (PostgreSQL / SQLite), a message broker (Apache Kafka), and downstream clearing partners, mutating account balances and publishing domain events (`TransferFundsReserved`, `TransferSettled`, `TransferCompensated`) are two distinct network I/O boundaries.

Naively performing a **dual-write** (committing the SQL transaction and subsequently calling `kafka.publish(...)`) causes two catastrophic failure modes:
1. **Lost Events (Ghost Balances)**: The database transaction commits, then the process crashes or the broker is unreachable before publication completes; downstream settlement and audit consumers never learn of the transfer.
2. **Phantom Events**: Publishing to Kafka before SQL commit succeeds, then the SQL commit rolls back due to optimistic concurrency or constraint failure; downstream services act on money that never moved.

The traditional enterprise alternative — **Two-Phase Commit (2PC / XA Transactions)** across PostgreSQL and Kafka — introduces blocking coordinator locks across network partitions, severely degrades throughput under contention, cannot span external clearing APIs, and leaves in-doubt prepared transactions if the coordinator crashes mid-phase.

---

## 2. Decision

We adopt **Orchestrated Sagas backed by the Transactional Outbox Pattern** combined with **Distributed Lock Fencing** and **Idempotent Consumer Deduplication**:

1. **Single ACID Boundary**: Every state transition (`reserve_funds_with_outbox`, `settle_saga_with_outbox`, `compensate_saga_with_outbox`) executes inside **one local database transaction** (`BEGIN IMMEDIATE` / `@Transactional`) that atomically updates `accounts`, appends balanced rows to `ledger_entries`, transitions `transfer_sagas`, records `idempotency_records`, and inserts a `PENDING` row into `outbox_events` (plus `dead_letter_events` on compensation).
2. **Asynchronous FIFO Outbox Relay**: A relay worker (`relay_pending_outbox_events` / `@Scheduled`) polls `outbox_events WHERE status = 'PENDING' ORDER BY created_at ASC`, publishes each event to Kafka keyed by `aggregate_id` (`saga_id`) for per-saga partition ordering, and marks the outbox row `PUBLISHED` with `published_at` only after broker acknowledgement.
3. **At-Least-Once Transport -> Effectively-Once Consumption**: Because a crash between Kafka publish and `mark_outbox_published` can redeliver an event, every downstream consumer deduplicates by `(consumer_group, event_id)` (`processed_consumer_events` / `InMemoryKafkaBroker.consume_idempotent`).
4. **Monotonic Fencing Tokens**: Before executing a transfer Saga on `source_account_id`, the orchestrator acquires a distributed mutex (`DistributedLockManager` / Redis `SET NX PX` + `INCR`) and embeds the monotonic `fencing_token` in the Saga record, preventing stale leaders from mutating state after GC pauses or network splits.

---

## 3. Alternatives Considered

| Option | Pros | Cons | Verdict |
|---|---|---|---|
| **Dual-Write (DB then Kafka)** | Minimal code | Unavoidable data/event divergence on process crashes or broker blips | **Rejected** |
| **XA / 2PC Distributed Transactions** | Synchronous cross-resource atomicity | Blocking locks, coordinator SPOF, poor latency, incompatible with external settlement APIs | **Rejected** |
| **CDC Log Tailing (Debezium)** | Near-zero polling overhead at ultra-high scale | Operational overhead of Kafka Connect + WAL slot monitoring for initial runtime; compatible with our exact `outbox_events` table | **Deferred (Drop-In Upgrade Path)** |
| **Transactional Outbox + Orchestrated Saga** | Zero dual-write gap, non-blocking, explicit compensation & DLQ visibility, identical SQL schema for polling or Debezium CDC | Requires idempotent consumers and outbox relay monitoring | **Accepted** |

---

## 4. Consequences

### Positive
- **Zero Dual-Write Loss**: If the database transaction commits, the event is guaranteed to exist in `outbox_events` and will be relayed to Kafka even across restarts.
- **High Availability**: Ledger writes succeed independently of temporary Kafka broker latency; the outbox buffers events cleanly and drains FIFO on recovery.
- **Seamless CDC Upgrade Path**: Because `outbox_events` is already an append-oriented table in `migrations/001_init_ledgercore_outbox.sql`, production deployments can replace polling with Debezium Outbox Event Router with zero domain or schema changes.

### Trade-offs & Mitigations
- **Duplicate Delivery Possible**: Mitigated by mandatory consumer-side deduplication (`processed_consumer_events` primary key `(consumer_group, event_id)`).
- **Eventual Consistency Across Services**: Mitigated by explicit Saga states (`RESERVED -> SETTLED | COMPENSATED`) and escrow clearing isolation (`ACC-ESCROW-CLEARING`).
