package com.example.eventprocessing.api.dto;

import com.example.eventprocessing.domain.EventStatus;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

@Data
@Builder
public class EventResponse {
    private String eventId;
    private String customerId;
    private String type;
    private BigDecimal amount;
    private String currency;
    private Instant timestamp;
    private Map<String, Object> metadata;
    private EventStatus status;
    private int retryCount;
    private Instant createdAt;
    private Instant processedAt;

    /** Present only when an error exists (spec 3.2). */
    @JsonInclude(JsonInclude.Include.NON_NULL)
    private String errorMessage;
}
