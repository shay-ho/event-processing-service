package com.example.eventprocessing.infrastructure.sqs;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Small SQS message body. Processing messages carry only eventId + correlationId (MongoDB is the source of truth).
 * DLQ messages additionally carry retryCount, errorMessage and a timestamp (spec section 10).
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
public class SqsMessagePayload {
    private String eventId;
    private String correlationId;
    private String errorMessage;
    private Integer retryCount;
    private String timestamp;

    public SqsMessagePayload(String eventId, String correlationId) {
        this.eventId = eventId;
        this.correlationId = correlationId;
    }
}
