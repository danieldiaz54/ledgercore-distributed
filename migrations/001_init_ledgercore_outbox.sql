-- =============================================================================
-- Migration 001: LedgerCore Distributed — Double-Entry Ledger & Transactional Outbox
-- Target Engine: PostgreSQL 16+
-- Spec: .specs/02-data/transactional-outbox-and-immutable-ledger.spec.md
-- =============================================================================

CREATE TABLE IF NOT EXISTS accounts (
    account_id    TEXT PRIMARY KEY,
    owner_name    TEXT NOT NULL CHECK (length(trim(owner_name)) > 0),
    currency      CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    balance_cents BIGINT NOT NULL CHECK (balance_cents >= 0),
    status        TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (length(trim(status)) > 0),
    version       BIGINT NOT NULL DEFAULT 1 CHECK (version >= 1),
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS transfer_sagas (
    saga_id                TEXT PRIMARY KEY,
    idempotency_key        TEXT NOT NULL UNIQUE,
    request_fingerprint    TEXT NOT NULL CHECK (length(trim(request_fingerprint)) > 0),
    source_account_id      TEXT NOT NULL REFERENCES accounts(account_id),
    destination_account_id TEXT NOT NULL REFERENCES accounts(account_id),
    amount_cents           BIGINT NOT NULL CHECK (amount_cents > 0),
    currency               CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    status                 TEXT NOT NULL CHECK (
        status IN ('INITIATED', 'RESERVED', 'SETTLED', 'COMPENSATING', 'COMPENSATED', 'FAILED_DLQ')
    ),
    fencing_token          BIGINT NOT NULL CHECK (fencing_token >= 1),
    failure_reason         TEXT,
    created_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at             TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_transfer_sagas_distinct_accounts CHECK (source_account_id <> destination_account_id)
);

CREATE TABLE IF NOT EXISTS ledger_entries (
    entry_id       TEXT PRIMARY KEY,
    transaction_id TEXT NOT NULL CHECK (length(trim(transaction_id)) > 0),
    account_id     TEXT NOT NULL REFERENCES accounts(account_id),
    direction      TEXT NOT NULL CHECK (direction IN ('DEBIT', 'CREDIT')),
    amount_cents   BIGINT NOT NULL CHECK (amount_cents > 0),
    currency       CHAR(3) NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    entry_type     TEXT NOT NULL CHECK (length(trim(entry_type)) > 0),
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS outbox_events (
    event_id     TEXT PRIMARY KEY,
    aggregate_id TEXT NOT NULL REFERENCES transfer_sagas(saga_id),
    topic        TEXT NOT NULL CHECK (length(trim(topic)) > 0),
    event_type   TEXT NOT NULL CHECK (length(trim(event_type)) > 0),
    payload_json JSONB NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('PENDING', 'PUBLISHED', 'DEAD_LETTERED')),
    retry_count  INTEGER NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ,
    CONSTRAINT chk_outbox_published_timestamp CHECK (
        status <> 'PUBLISHED' OR published_at IS NOT NULL
    )
);

CREATE TABLE IF NOT EXISTS idempotency_records (
    idempotency_key     TEXT PRIMARY KEY,
    request_fingerprint TEXT NOT NULL CHECK (length(trim(request_fingerprint)) > 0),
    saga_id             TEXT NOT NULL UNIQUE REFERENCES transfer_sagas(saga_id),
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dead_letter_events (
    dlq_id       TEXT PRIMARY KEY,
    event_id     TEXT NOT NULL CHECK (length(trim(event_id)) > 0),
    topic        TEXT NOT NULL CHECK (length(trim(topic)) > 0),
    reason       TEXT NOT NULL CHECK (length(trim(reason)) > 0),
    payload_json JSONB NOT NULL,
    failed_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS processed_consumer_events (
    consumer_group TEXT NOT NULL CHECK (length(trim(consumer_group)) > 0),
    event_id       TEXT NOT NULL CHECK (length(trim(event_id)) > 0),
    topic          TEXT NOT NULL CHECK (length(trim(topic)) > 0),
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (consumer_group, event_id)
);

-- =============================================================================
-- Operational Indexes (Outbox Polling, Ledger Audits, Saga Lookups)
-- =============================================================================

CREATE INDEX IF NOT EXISTS idx_ledger_entries_transaction_id
    ON ledger_entries (transaction_id, created_at);

CREATE INDEX IF NOT EXISTS idx_ledger_entries_account_id
    ON ledger_entries (account_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_outbox_events_pending_fifo
    ON outbox_events (status, created_at ASC)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_transfer_sagas_status_updated
    ON transfer_sagas (status, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_dead_letter_events_failed_at
    ON dead_letter_events (failed_at DESC);

-- =============================================================================
-- Database-Enforced Append-Only Immutability for Double-Entry Ledger
-- =============================================================================

CREATE OR REPLACE FUNCTION prevent_ledger_entry_mutation()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'APPEND_ONLY_LEDGER_VIOLATION: Double-entry ledger entries are strictly append-only (attempted % on entry_id=%)',
        TG_OP, OLD.entry_id
        USING ERRCODE = 'check_violation';
END;
$$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'trg_ledger_entries_no_update'
          AND tgrelid = 'ledger_entries'::regclass
    ) THEN
        CREATE TRIGGER trg_ledger_entries_no_update
            BEFORE UPDATE ON ledger_entries
            FOR EACH ROW
            EXECUTE FUNCTION prevent_ledger_entry_mutation();
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'trg_ledger_entries_no_delete'
          AND tgrelid = 'ledger_entries'::regclass
    ) THEN
        CREATE TRIGGER trg_ledger_entries_no_delete
            BEFORE DELETE ON ledger_entries
            FOR EACH ROW
            EXECUTE FUNCTION prevent_ledger_entry_mutation();
    END IF;
END;
$$;
