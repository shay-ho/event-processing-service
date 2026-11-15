package com.example.eventprocessing.unit;

import com.example.eventprocessing.infrastructure.sqs.SqsConsumerTestAccess;
import com.example.eventprocessing.infrastructure.sqs.SqsMessagePayload;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/** Spec section 12: the Correlation ID arrives as an SQS Message Attribute (header prefix {@code Sqs_MA_}). */
class SqsConsumerCorrelationTest {

    private static SqsMessagePayload body(String correlationId) {
        return new SqsMessagePayload("evt-1", correlationId);
    }

    @Test
    void prefixedMessageAttributeHeaderIsUsed() {
        Map<String, Object> headers = Map.of("Sqs_MA_X-Correlation-ID", "from-attribute");
        assertEquals("from-attribute", SqsConsumerTestAccess.resolve(headers, body("from-body")));
    }

    @Test
    void prefixedAttributeWinsOverUnprefixedHeaderAndBody() {
        Map<String, Object> headers = new HashMap<>();
        headers.put("Sqs_MA_X-Correlation-ID", "prefixed");
        headers.put("X-Correlation-ID", "unprefixed");
        assertEquals("prefixed", SqsConsumerTestAccess.resolve(headers, body("from-body")));
    }

    @Test
    void unprefixedHeaderIsAcceptedAsDefensiveFallback() {
        assertEquals("unprefixed", SqsConsumerTestAccess.resolve(Map.of("X-Correlation-ID", "unprefixed"), body("from-body")));
    }

    @Test
    void fallsBackToBodyWhenNoAttributePresent() {
        assertEquals("from-body", SqsConsumerTestAccess.resolve(Map.of(), body("from-body")));
        assertEquals("from-body", SqsConsumerTestAccess.resolve(null, body("from-body")));
    }

    @Test
    void blankAttributeIsIgnored() {
        assertEquals("from-body", SqsConsumerTestAccess.resolve(Map.of("Sqs_MA_X-Correlation-ID", "  "), body("from-body")));
    }

    @Test
    void returnsNullWhenNothingAvailable() {
        assertNull(SqsConsumerTestAccess.resolve(Map.of(), body(null)));
    }
}
