package com.ledgercore.application;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ledgercore.domain.LedgerEntry;
import com.ledgercore.domain.OutboxEvent;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.time.Instant;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Distributed Transfer Saga Orchestrator implementing:
 * <ul>
 *   <li>SHA-256 {@code Idempotency-Key} replay &amp; payload conflict detection (409 Conflict)</li>
 *   <li>Redis distributed mutex with monotonic fencing tokens &amp; compare-and-delete Lua release</li>
 *   <li>Two-phase atomic double-entry accounting ({@code RESERVE -> SETTLE | COMPENSATE})</li>
 *   <li>Transactional Outbox relay to Apache Kafka with Dead-Letter Queue routing</li>
 * </ul>
 */
@Service
public class TransferSagaOrchestrator {

    public static final String ESCROW_ACCOUNT_ID = "ACC-ESCROW-CLEARING";
    public static final String TOPIC_RESERVED = "ledger.transfers.reserved";
    public static final String TOPIC_SETTLED = "ledger.transfers.settled";
    public static final String TOPIC_COMPENSATED = "ledger.transfers.compensated";
    public static final String TOPIC_DLQ = "ledger.transfers.dlq";

    private static final String FENCING_COUNTER_KEY = "ledgercore:fencing_token";
    private static final Duration LOCK_TTL = Duration.ofSeconds(30);
    private static final String SETTLEMENT_FAILURE_REASON =
            "Downstream settlement partner timeout — auto-compensated";

    private static final DefaultRedisScript<Long> COMPARE_AND_DELETE_LOCK_SCRIPT =
            new DefaultRedisScript<>(
                    "if redis.call('get', KEYS[1]) == ARGV[1] then "
                            + "return redis.call('del', KEYS[1]) "
                            + "else return 0 end",
                    Long.class
            );

    private final LedgerPersistencePort repository;
    private final StringRedisTemplate redisTemplate;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public TransferSagaOrchestrator(
            LedgerPersistencePort repository,
            StringRedisTemplate redisTemplate,
            KafkaTemplate<String, String> kafkaTemplate,
            ObjectMapper objectMapper
    ) {
        this.repository = Objects.requireNonNull(repository, "repository must not be null");
        this.redisTemplate = Objects.requireNonNull(redisTemplate, "redisTemplate must not be null");
        this.kafkaTemplate = Objects.requireNonNull(kafkaTemplate, "kafkaTemplate must not be null");
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper must not be null");
    }

    public TransferExecutionResult executeTransfer(TransferCommand command) {
        validateCommand(command);
        String fingerprint = command.computeSha256Fingerprint();

        Optional<SagaSnapshot> existing = repository.findSagaByIdempotencyKey(command.idempotencyKey());
        if (existing.isPresent()) {
            SagaSnapshot saga = existing.get();
            if (!saga.requestFingerprint().equals(fingerprint)) {
                throw new IdempotencyConflictException(command.idempotencyKey());
            }
            List<LedgerEntry> entries = repository.findLedgerEntriesByTransaction(saga.sagaId());
            return new TransferExecutionResult(true, saga, entries, 0);
        }

        String sagaId = "SAGA-" + UUID.randomUUID().toString().replace("-", "").substring(0, 12).toUpperCase();
        String lockKey = "ledgercore:lock:account:" + command.sourceAccountId();
        long fencingToken = acquireDistributedLock(lockKey, sagaId);

        try {
            // Step 1: Atomic reserve (DEBIT source, CREDIT escrow) + PENDING outbox event.
            reserveFundsWithOutbox(command, sagaId, fingerprint, fencingToken);
            int relayedCount = relayPendingOutboxEvents();

            // Step 2: Settle OR run append-only compensating reversal + DLQ archival.
            SagaSnapshot finalSaga;
            if (command.simulateSettlementFailure()) {
                finalSaga = compensateSagaWithOutbox(sagaId, SETTLEMENT_FAILURE_REASON);
                relayedCount += relayPendingOutboxEvents();
                publishDeadLetterEvent(finalSaga, SETTLEMENT_FAILURE_REASON);
            } else {
                finalSaga = settleSagaWithOutbox(sagaId);
                relayedCount += relayPendingOutboxEvents();
            }

            List<LedgerEntry> allEntries = repository.findLedgerEntriesByTransaction(sagaId);
            return new TransferExecutionResult(false, finalSaga, allEntries, relayedCount);
        } finally {
            releaseDistributedLock(lockKey, sagaId);
        }
    }

    @Transactional
    public SagaSnapshot reserveFundsWithOutbox(
            TransferCommand command,
            String sagaId,
            String fingerprint,
            long fencingToken
    ) {
        long sourceBalance = repository.findAccountBalanceCents(command.sourceAccountId())
                .orElseThrow(() -> new AccountNotFoundException(command.sourceAccountId()));
        repository.findAccountBalanceCents(command.destinationAccountId())
                .orElseThrow(() -> new AccountNotFoundException(command.destinationAccountId()));

        if (sourceBalance < command.amountCents()) {
            throw new InsufficientFundsException(
                    command.sourceAccountId(),
                    sourceBalance,
                    command.amountCents()
            );
        }

        Instant now = Instant.now();
        List<LedgerEntry> reserveEntries = List.of(
                LedgerEntry.debit(
                        sagaId,
                        command.sourceAccountId(),
                        command.amountCents(),
                        command.currency(),
                        "TRANSFER_RESERVE",
                        now
                ),
                LedgerEntry.credit(
                        sagaId,
                        ESCROW_ACCOUNT_ID,
                        command.amountCents(),
                        command.currency(),
                        "TRANSFER_RESERVE",
                        now
                )
        );
        LedgerEntry.validateBalanced(sagaId, reserveEntries);

        OutboxEvent outbox = OutboxEvent.pending(
                sagaId,
                TOPIC_RESERVED,
                "TransferFundsReserved",
                serializeEventPayload(sagaId, command, SagaStatus.RESERVED, fencingToken, null),
                now
        );

        return repository.persistReserveAtomic(command, sagaId, fingerprint, fencingToken, reserveEntries, outbox);
    }

    @Transactional
    public SagaSnapshot settleSagaWithOutbox(String sagaId) {
        SagaSnapshot reserved = repository.findSagaById(sagaId)
                .orElseThrow(() -> new AccountNotFoundException(sagaId));

        Instant now = Instant.now();
        List<LedgerEntry> settleEntries = List.of(
                LedgerEntry.debit(
                        sagaId,
                        ESCROW_ACCOUNT_ID,
                        reserved.amountCents(),
                        reserved.currency(),
                        "TRANSFER_SETTLE",
                        now
                ),
                LedgerEntry.credit(
                        sagaId,
                        reserved.destinationAccountId(),
                        reserved.amountCents(),
                        reserved.currency(),
                        "TRANSFER_SETTLE",
                        now
                )
        );
        LedgerEntry.validateBalanced(sagaId, settleEntries);

        OutboxEvent outbox = OutboxEvent.pending(
                sagaId,
                TOPIC_SETTLED,
                "TransferSettled",
                serializeSagaTransitionPayload(reserved, SagaStatus.SETTLED, null),
                now
        );
        return repository.persistSettleAtomic(sagaId, settleEntries, outbox);
    }

    @Transactional
    public SagaSnapshot compensateSagaWithOutbox(String sagaId, String failureReason) {
        SagaSnapshot reserved = repository.findSagaById(sagaId)
                .orElseThrow(() -> new AccountNotFoundException(sagaId));

        Instant now = Instant.now();
        List<LedgerEntry> reversalEntries = List.of(
                LedgerEntry.debit(
                        sagaId,
                        ESCROW_ACCOUNT_ID,
                        reserved.amountCents(),
                        reserved.currency(),
                        "SAGA_COMPENSATION",
                        now
                ),
                LedgerEntry.credit(
                        sagaId,
                        reserved.sourceAccountId(),
                        reserved.amountCents(),
                        reserved.currency(),
                        "SAGA_COMPENSATION",
                        now
                )
        );
        LedgerEntry.validateBalanced(sagaId, reversalEntries);

        OutboxEvent outbox = OutboxEvent.pending(
                sagaId,
                TOPIC_COMPENSATED,
                "TransferCompensated",
                serializeSagaTransitionPayload(reserved, SagaStatus.COMPENSATED, failureReason),
                now
        );
        return repository.persistCompensateAtomic(sagaId, failureReason, reversalEntries, outbox);
    }

    @Scheduled(fixedDelayString = "${ledgercore.outbox.relay-delay-ms:1000}")
    @Transactional
    public int relayPendingOutboxEvents() {
        List<OutboxEvent> pending = repository.findPendingOutboxEventsFifo(100);
        int publishedCount = 0;
        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate
                        .send(event.getTopic(), event.getAggregateId(), event.getPayloadJson())
                        .get(5, TimeUnit.SECONDS);
                event.markPublished(Instant.now());
                repository.saveOutboxEvent(event);
                publishedCount++;
            } catch (Exception ex) {
                int retries = event.recordDeliveryRetry();
                if (retries >= 5) {
                    event.markDeadLettered();
                }
                repository.saveOutboxEvent(event);
            }
        }
        return publishedCount;
    }

    private long acquireDistributedLock(String lockKey, String ownerId) {
        Boolean acquired = redisTemplate.opsForValue().setIfAbsent(lockKey, ownerId, LOCK_TTL);
        if (!Boolean.TRUE.equals(acquired)) {
            throw new DistributedLockContentionException(lockKey);
        }
        Long token = redisTemplate.opsForValue().increment(FENCING_COUNTER_KEY);
        return token != null ? token : 1L;
    }

    private void releaseDistributedLock(String lockKey, String ownerId) {
        redisTemplate.execute(COMPARE_AND_DELETE_LOCK_SCRIPT, List.of(lockKey), ownerId);
    }

    private void publishDeadLetterEvent(SagaSnapshot saga, String failureReason) {
        String dlqPayload = serializeSagaTransitionPayload(saga, SagaStatus.COMPENSATED, failureReason);
        kafkaTemplate.send(TOPIC_DLQ, saga.sagaId(), dlqPayload);
    }

    private void validateCommand(TransferCommand command) {
        if (command == null) {
            throw new InvalidTransferException("Transfer command must not be null.");
        }
        if (command.idempotencyKey() == null || command.idempotencyKey().isBlank()) {
            throw new InvalidTransferException("Header 'Idempotency-Key' or body 'idempotency_key' is required.");
        }
        if (command.sourceAccountId() == null || command.sourceAccountId().isBlank()
                || command.destinationAccountId() == null || command.destinationAccountId().isBlank()) {
            throw new InvalidTransferException("Source and destination account IDs must be non-blank.");
        }
        if (command.sourceAccountId().trim().equals(command.destinationAccountId().trim())) {
            throw new InvalidTransferException("Source and destination accounts must be distinct.");
        }
        if (command.amountCents() <= 0) {
            throw new InvalidTransferException("Field 'amount_cents' must be strictly positive.");
        }
        if (command.currency() == null || !command.currency().matches("^[A-Z]{3}$")) {
            throw new InvalidTransferException("Field 'currency' must be a 3-letter uppercase ISO code.");
        }
    }

    private String serializeEventPayload(
            String sagaId,
            TransferCommand cmd,
            SagaStatus status,
            long fencingToken,
            String failureReason
    ) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("aggregate_id", sagaId);
        body.put("saga_id", sagaId);
        body.put("idempotency_key", cmd.idempotencyKey());
        body.put("source_account_id", cmd.sourceAccountId());
        body.put("destination_account_id", cmd.destinationAccountId());
        body.put("amount_cents", cmd.amountCents());
        body.put("currency", cmd.currency());
        body.put("status", status.name());
        body.put("fencing_token", fencingToken);
        if (failureReason != null) {
            body.put("failure_reason", failureReason);
        }
        return toJson(body);
    }

    private String serializeSagaTransitionPayload(
            SagaSnapshot saga,
            SagaStatus targetStatus,
            String failureReason
    ) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("aggregate_id", saga.sagaId());
        body.put("saga_id", saga.sagaId());
        body.put("idempotency_key", saga.idempotencyKey());
        body.put("source_account_id", saga.sourceAccountId());
        body.put("destination_account_id", saga.destinationAccountId());
        body.put("amount_cents", saga.amountCents());
        body.put("currency", saga.currency());
        body.put("status", targetStatus.name());
        body.put("fencing_token", saga.fencingToken());
        if (failureReason != null) {
            body.put("failure_reason", failureReason);
        }
        return toJson(body);
    }

    private String toJson(Map<String, Object> payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException ex) {
            throw new IllegalStateException("Failed to serialize outbox payload to JSON.", ex);
        }
    }

    // -------------------------------------------------------------------------
    // Cohesive Domain Commands, Snapshots, Persistence Port & Typed Exceptions
    // -------------------------------------------------------------------------

    public enum SagaStatus {
        INITIATED,
        RESERVED,
        SETTLED,
        COMPENSATING,
        COMPENSATED,
        FAILED_DLQ
    }

    public record TransferCommand(
            String idempotencyKey,
            String sourceAccountId,
            String destinationAccountId,
            long amountCents,
            String currency,
            boolean simulateSettlementFailure
    ) {
        public String computeSha256Fingerprint() {
            String canonical = String.format(
                    "{\"amount_cents\":%d,\"currency\":\"%s\",\"destination_account_id\":\"%s\","
                            + "\"simulate_settlement_failure\":%s,\"source_account_id\":\"%s\"}",
                    amountCents,
                    currency,
                    destinationAccountId,
                    simulateSettlementFailure,
                    sourceAccountId
            );
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                return HexFormat.of().formatHex(digest.digest(canonical.getBytes(StandardCharsets.UTF_8)));
            } catch (NoSuchAlgorithmException ex) {
                throw new IllegalStateException("SHA-256 algorithm unavailable.", ex);
            }
        }
    }

    public record SagaSnapshot(
            String sagaId,
            String idempotencyKey,
            String requestFingerprint,
            String sourceAccountId,
            String destinationAccountId,
            long amountCents,
            String currency,
            SagaStatus status,
            long fencingToken,
            String failureReason,
            Instant createdAt,
            Instant updatedAt
    ) {}

    public record TransferExecutionResult(
            boolean idempotentReplay,
            SagaSnapshot saga,
            List<LedgerEntry> ledgerEntries,
            int outboxEventsRelayed
    ) {}

    public interface LedgerPersistencePort {
        Optional<SagaSnapshot> findSagaByIdempotencyKey(String idempotencyKey);

        Optional<SagaSnapshot> findSagaById(String sagaId);

        Optional<Long> findAccountBalanceCents(String accountId);

        List<LedgerEntry> findLedgerEntriesByTransaction(String transactionId);

        SagaSnapshot persistReserveAtomic(
                TransferCommand command,
                String sagaId,
                String requestFingerprint,
                long fencingToken,
                List<LedgerEntry> entries,
                OutboxEvent outboxEvent
        );

        SagaSnapshot persistSettleAtomic(
                String sagaId,
                List<LedgerEntry> entries,
                OutboxEvent outboxEvent
        );

        SagaSnapshot persistCompensateAtomic(
                String sagaId,
                String failureReason,
                List<LedgerEntry> reversalEntries,
                OutboxEvent outboxEvent
        );

        List<OutboxEvent> findPendingOutboxEventsFifo(int limit);

        OutboxEvent saveOutboxEvent(OutboxEvent event);
    }

    public static final class InvalidTransferException extends RuntimeException {
        public static final String CODE = "ERR-400-INVALID-TRANSFER";

        public InvalidTransferException(String message) {
            super(message);
        }
    }

    public static final class AccountNotFoundException extends RuntimeException {
        public static final String CODE = "ERR-404-ACCOUNT-NOT-FOUND";
        private final String resourceId;

        public AccountNotFoundException(String resourceId) {
            super("Account or Saga '" + resourceId + "' was not found.");
            this.resourceId = resourceId;
        }

        public String getResourceId() {
            return resourceId;
        }
    }

    public static final class IdempotencyConflictException extends RuntimeException {
        public static final String CODE = "ERR-409-IDEMPOTENCY-CONFLICT";
        private final String idempotencyKey;

        public IdempotencyConflictException(String idempotencyKey) {
            super("Idempotency key '" + idempotencyKey + "' was already used with a different request payload.");
            this.idempotencyKey = idempotencyKey;
        }

        public String getIdempotencyKey() {
            return idempotencyKey;
        }
    }

    public static final class DistributedLockContentionException extends RuntimeException {
        public static final String CODE = "ERR-409-LOCK-CONTENTION";
        private final String resourceKey;

        public DistributedLockContentionException(String resourceKey) {
            super("Distributed lock contention on resource '" + resourceKey + "'.");
            this.resourceKey = resourceKey;
        }

        public String getResourceKey() {
            return resourceKey;
        }
    }

    public static final class InsufficientFundsException extends RuntimeException {
        public static final String CODE = "ERR-422-INSUFFICIENT-FUNDS";
        private final String accountId;
        private final long availableCents;
        private final long requestedCents;

        public InsufficientFundsException(String accountId, long availableCents, long requestedCents) {
            super("Account '" + accountId + "' has insufficient funds: available="
                    + availableCents + " cents, requested=" + requestedCents + " cents.");
            this.accountId = accountId;
            this.availableCents = availableCents;
            this.requestedCents = requestedCents;
        }

        public String getAccountId() {
            return accountId;
        }

        public long getAvailableCents() {
            return availableCents;
        }

        public long getRequestedCents() {
            return requestedCents;
        }
    }
}
