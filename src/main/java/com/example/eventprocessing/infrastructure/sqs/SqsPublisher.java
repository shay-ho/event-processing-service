package com.example.eventprocessing.infrastructure.sqs;

import com.example.eventprocessing.config.EventsProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

@Component
public class SqsPublisher {

    private static final Logger log = LoggerFactory.getLogger(SqsPublisher.class);
    public static final String CORRELATION_ATTRIBUTE = "X-Correlation-ID";

    private final SqsTemplate sqsTemplate;
    private final ObjectMapper objectMapper;
    private final String queueName;
    private final String dlqName;

    public SqsPublisher(Optional<SqsTemplate> sqsTemplate,
                        ObjectMapper objectMapper,
                        EventsProperties props) {
        this.sqsTemplate = sqsTemplate.orElse(null);
        this.objectMapper = objectMapper;
        this.queueName = props.getSqs().getQueueName();
        this.dlqName = props.getSqs().getDlqName();
    }

    public void publishEventId(String eventId, String correlationId) {
        send(queueName, new SqsMessagePayload(eventId, correlationId), correlationId, eventId);
        log.info("Published eventId={} to SQS queue={}", eventId, queueName);
    }

    public void publishDlqMessage(String eventId, String correlationId, String errorMessage, int retryCount) {
        SqsMessagePayload payload = new SqsMessagePayload(
                eventId, correlationId, errorMessage, retryCount, Instant.now().toString());
        send(dlqName, payload, correlationId, eventId);
        log.info("Published DLQ message for eventId={} to queue={}", eventId, dlqName);
    }

    private void send(String queue, SqsMessagePayload payload, String correlationId, String eventId) {
        if (sqsTemplate == null) {
            throw new IllegalStateException("SqsTemplate is not available; cannot publish eventId=" + eventId);
        }
        try {
            String json = objectMapper.writeValueAsString(payload);
            sqsTemplate.send(to -> {
                to.queue(queue).payload(json);
                if (correlationId != null && !correlationId.isBlank()) {
                    // Correlation ID travels as an SQS Message Attribute (spec section 12).
                    to.header(CORRELATION_ATTRIBUTE, correlationId);
                }
            });
        } catch (Exception e) {
            throw new IllegalStateException("Failed to send message to SQS queue " + queue + " for eventId=" + eventId, e);
        }
    }
}
