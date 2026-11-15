package com.example.eventprocessing.domain;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.data.mongodb.core.mapping.FieldType;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "events")
public class Event {

    @Id
    private String id;

    private String eventId;

    private String payloadHash;
    private String customerId;
    private String type;

    @Field(targetType = FieldType.DECIMAL128)
    private BigDecimal amount;

    private String currency;
    private Instant timestamp;
    private Map<String, Object> metadata;

    private EventStatus status;
    private int retryCount;
    private Instant nextProcessingAttemptAt;

    private String workerId;
    private long leaseVersion;
    private Instant processingStartedAt;
    private Instant processingLeaseUntil;

    private OutboxStatus outboxStatus;
    private Instant outboxNextAttemptAt;
    private String publisherId;
    private Instant publishingLeaseUntil;
    private long publishLeaseVersion;
    private int outboxPublishAttempts;
    private Instant publishedAt;

    private DlqStatus dlqStatus;
    private Instant dlqPublishedAt;

    private String errorMessage;
    private String correlationId;
    private Instant createdAt;
    private Instant updatedAt;
    private Instant processedAt;
}
