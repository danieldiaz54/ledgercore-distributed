package com.ledgercore.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.regex.Pattern;
import org.hibernate.annotations.Immutable;

/**
 * Append-only double-entry accounting journal line mapped to {@code ledger_entries}.
 *
 * <p>All monetary amounts are persisted strictly in exact integer minor units ({@code long} cents).
 * Updates and deletes are prohibited both at the ORM layer ({@link Immutable}, zero mutator methods)
 * and at the PostgreSQL layer via {@code prevent_ledger_entry_mutation()} BEFORE UPDATE/DELETE triggers.
 */
@Entity
@Immutable
@Table(name = "ledger_entries")
public class LedgerEntry {

    private static final Pattern ISO_CURRENCY_PATTERN = Pattern.compile("^[A-Z]{3}$");

    public enum EntryDirection {
        DEBIT,
        CREDIT
    }

    @Id
    @Column(name = "entry_id", nullable = false, updatable = false)
    private String entryId;

    @Column(name = "transaction_id", nullable = false, updatable = false)
    private String transactionId;

    @Column(name = "account_id", nullable = false, updatable = false)
    private String accountId;

    @Enumerated(EnumType.STRING)
    @Column(name = "direction", nullable = false, updatable = false)
    private EntryDirection direction;

    @Column(name = "amount_cents", nullable = false, updatable = false)
    private long amountCents;

    @Column(name = "currency", nullable = false, updatable = false, length = 3)
    private String currency;

    @Column(name = "entry_type", nullable = false, updatable = false)
    private String entryType;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected LedgerEntry() {
        // Required by JPA specification
    }

    public LedgerEntry(
            String entryId,
            String transactionId,
            String accountId,
            EntryDirection direction,
            long amountCents,
            String currency,
            String entryType,
            Instant createdAt
    ) {
        this.entryId = requireNonBlank(entryId, "entryId");
        this.transactionId = requireNonBlank(transactionId, "transactionId");
        this.accountId = requireNonBlank(accountId, "accountId");
        this.direction = Objects.requireNonNull(direction, "Field 'direction' must not be null.");
        if (amountCents <= 0) {
            throw new IllegalArgumentException("Field 'amountCents' must be strictly positive, got " + amountCents);
        }
        this.amountCents = amountCents;
        if (currency == null || !ISO_CURRENCY_PATTERN.matcher(currency).matches()) {
            throw new IllegalArgumentException("Field 'currency' must be a 3-letter uppercase ISO code.");
        }
        this.currency = currency;
        this.entryType = requireNonBlank(entryType, "entryType");
        this.createdAt = Objects.requireNonNull(createdAt, "Field 'createdAt' must not be null.");
    }

    public static LedgerEntry debit(
            String transactionId,
            String accountId,
            long amountCents,
            String currency,
            String entryType,
            Instant createdAt
    ) {
        return new LedgerEntry(
                "ENT-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16),
                transactionId,
                accountId,
                EntryDirection.DEBIT,
                amountCents,
                currency,
                entryType,
                createdAt
        );
    }

    public static LedgerEntry credit(
            String transactionId,
            String accountId,
            long amountCents,
            String currency,
            String entryType,
            Instant createdAt
    ) {
        return new LedgerEntry(
                "ENT-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16),
                transactionId,
                accountId,
                EntryDirection.CREDIT,
                amountCents,
                currency,
                entryType,
                createdAt
        );
    }

    /**
     * Enforces the double-entry zero-sum accounting invariant per currency:
     * {@code SUM(DEBIT amountCents) == SUM(CREDIT amountCents)} with at least 2 entries.
     */
    public static void validateBalanced(String transactionId, List<LedgerEntry> entries) {
        requireNonBlank(transactionId, "transactionId");
        if (entries == null || entries.size() < 2) {
            throw new IllegalStateException(
                    "Double-entry transaction '" + transactionId + "' requires at least 2 ledger entries."
            );
        }

        Map<String, long[]> totalsByCurrency = new HashMap<>();
        for (LedgerEntry entry : entries) {
            Objects.requireNonNull(entry, "LedgerEntry must not be null.");
            if (!transactionId.equals(entry.getTransactionId())) {
                throw new IllegalArgumentException(
                        "Entry '" + entry.getEntryId() + "' belongs to transaction '"
                                + entry.getTransactionId() + "', expected '" + transactionId + "'."
                );
            }
            long[] bucket = totalsByCurrency.computeIfAbsent(entry.getCurrency(), k -> new long[2]);
            if (entry.getDirection() == EntryDirection.DEBIT) {
                bucket[0] = Math.addExact(bucket[0], entry.getAmountCents());
            } else {
                bucket[1] = Math.addExact(bucket[1], entry.getAmountCents());
            }
        }

        for (Map.Entry<String, long[]> currencyTotals : totalsByCurrency.entrySet()) {
            long debits = currencyTotals.getValue()[0];
            long credits = currencyTotals.getValue()[1];
            if (debits != credits) {
                throw new IllegalStateException(
                        "Zero-sum invariant violated for transaction '" + transactionId
                                + "' in currency '" + currencyTotals.getKey()
                                + "': debits=" + debits + ", credits=" + credits
                );
            }
        }
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Field '" + fieldName + "' must be non-blank.");
        }
        return value;
    }

    public String getEntryId() {
        return entryId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public String getAccountId() {
        return accountId;
    }

    public EntryDirection getDirection() {
        return direction;
    }

    public long getAmountCents() {
        return amountCents;
    }

    public String getCurrency() {
        return currency;
    }

    public String getEntryType() {
        return entryType;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
