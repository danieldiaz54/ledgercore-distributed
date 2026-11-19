package com.ledgercore.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.Objects;
import java.util.UUID;

/**
 * Transactional Outbox event persisted atomically in the same ACID database transaction
 * as ledger balance updates and Saga state transitions, eliminating database/Kafka dual-write drift.
 */
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    public enum OutboxStatus {
        PENDING,
        PUBLISHED,
        DEAD_LETTERED
    }

    @Id
    @Column(name = "event_id", nullable = false, updatable = false)
    private String eventId;

    @Column(name = "aggregate_id", nullable = false, updatable = false)
    private String aggregateId;

    @Column(name = "topic", nullable = false, updatable = false)
    private String topic;

    @Column(name = "event_type", nullable = false, updatable = false)
    private String eventType;

    @Column(name = "payload_json", nullable = false, updatable = false, columnDefinition = "jsonb")
    private String payloadJson;

    @Enumerated(EnumType.STRING)
    @Column(name = "status", nullable = false)
    private OutboxStatus status;

    @Column(name = "retry_count", nullable = false)
    private int retryCount;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    protected OutboxEvent() {
        // Required by JPA specification
    }

    public OutboxEvent(
            String eventId,
            String aggregateId,
            String topic,
            String eventType,
            String payloadJson,
            OutboxStatus status,
            int retryCount,
            Instant createdAt,
            Instant publishedAt
    ) {
        this.eventId = requireNonBlank(eventId, "eventId");
        this.aggregateId = requireNonBlank(aggregateId, "aggregateId");
        this.topic = requireNonBlank(topic, "topic");
        this.eventType = requireNonBlank(eventType, "eventType");
        this.payloadJson = requireNonBlank(payloadJson, "payloadJson");
        this.status = Objects.requireNonNull(status, "Field 'status' must not be null.");
        if (retryCount < 0) {
            throw new IllegalArgumentException("Field 'retryCount' must be >= 0.");
        }
        this.retryCount = retryCount;
        this.createdAt = Objects.requireNonNull(createdAt, "Field 'createdAt' must not be null.");
        if (status == OutboxStatus.PUBLISHED && publishedAt == null) {
            throw new IllegalArgumentException("Published outbox events must include 'publishedAt'.");
        }
        this.publishedAt = publishedAt;
    }

    public static OutboxEvent pending(
            String aggregateId,
            String topic,
            String eventType,
            String payloadJson,
            Instant createdAt
    ) {
        return new OutboxEvent(
                "EVT-" + UUID.randomUUID().toString().replace("-", "").substring(0, 16),
                aggregateId,
                topic,
                eventType,
                payloadJson,
                OutboxStatus.PENDING,
                0,
                createdAt,
                null
        );
    }

    public void markPublished(Instant publishedTimestamp) {
        Objects.requireNonNull(publishedTimestamp, "publishedTimestamp must not be null.");
        if (this.status == OutboxStatus.DEAD_LETTERED) {
            throw new IllegalStateException("Cannot publish a dead-lettered outbox event: " + eventId);
        }
        this.status = OutboxStatus.PUBLISHED;
        this.publishedAt = publishedTimestamp;
    }

    public int recordDeliveryRetry() {
        this.retryCount = Math.incrementExact(this.retryCount);
        return this.retryCount;
    }

    public void markDeadLettered() {
        this.status = OutboxStatus.DEAD_LETTERED;
    }

    private static String requireNonBlank(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Field '" + fieldName + "' must be non-blank.");
        }
        return value;
    }

    public String getEventId() {
        return eventId;
    }

    public String getAggregateId() {
        return aggregateId;
    }

    public String getTopic() {
        return topic;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPayloadJson() {
        return payloadJson;
    }

    public OutboxStatus getStatus() {
        return status;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }
}
