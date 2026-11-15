package com.example.eventprocessing.infrastructure.sqs;

import com.example.eventprocessing.application.EventProcessingService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.awspring.cloud.sqs.annotation.SqsListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.messaging.MessageHeaders;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Thin SQS adapter: parse the message and delegate to {@link EventProcessingService}.
 * Throwing from this method means the message is NOT acknowledged and SQS will redeliver it.
 */
@Component
public class SqsConsumer {

    private static final Logger log = LoggerFactory.getLogger(SqsConsumer.class);

    /** Header prefix Spring Cloud AWS 3.x applies to SQS Message Attributes. */
    static final String SQS_MESSAGE_ATTRIBUTE_PREFIX = "Sqs_MA_";

    private final EventProcessingService eventProcessingService;
    private final ObjectMapper objectMapper;

    public SqsConsumer(EventProcessingService eventProcessingService, ObjectMapper objectMapper) {
        this.eventProcessingService = eventProcessingService;
        this.objectMapper = objectMapper;
    }

    @SqsListener("${events.sqs.queue-name:events-queue}")
    public void listen(String messageBody, MessageHeaders headers) {

        SqsMessagePayload payload = parse(messageBody);
        String eventId = payload.getEventId();
        if (eventId == null || eventId.isBlank()) {
            // Poison message: failing lets SQS redrive it to the DLQ (RedrivePolicy in init-sqs.sh).
            throw new IllegalArgumentException("SQS message payload is missing eventId");
        }
        String correlationId = resolveCorrelationId(headers, payload);

        log.info("Received SQS message eventId={} correlationId={}", eventId, correlationId);
        eventProcessingService.handleMessage(eventId, correlationId);
    }

    /**
     * Spring Cloud AWS maps SQS Message Attributes to Spring headers prefixed with {@code Sqs_MA_}
     * (e.g. attribute {@code X-Correlation-ID} arrives as {@code Sqs_MA_X-Correlation-ID}).
     * Resolution order: prefixed message attribute, unprefixed header (defensive), message body.
     */
    static String resolveCorrelationId(Map<String, Object> headers, SqsMessagePayload payload) {
        if (headers != null) {
            for (String key : new String[]{
                    SQS_MESSAGE_ATTRIBUTE_PREFIX + SqsPublisher.CORRELATION_ATTRIBUTE,
                    SqsPublisher.CORRELATION_ATTRIBUTE}) {
                Object value = headers.get(key);
                if (value != null && !value.toString().isBlank()) {
                    return value.toString();
                }
            }
        }
        return payload != null ? payload.getCorrelationId() : null;
    }

    private SqsMessagePayload parse(String body) {
        try {
            JsonNode node = objectMapper.readTree(body);
            // Tolerate a double-encoded JSON string body.
            if (node != null && node.isTextual()) {
                node = objectMapper.readTree(node.asText());
            }
            return objectMapper.treeToValue(node, SqsMessagePayload.class);
        } catch (Exception e) {
            throw new IllegalArgumentException("Unparseable SQS message", e);
        }
    }
}
