package com.example.eventprocessing.infrastructure.sqs;

import java.util.Map;

/** Test-only bridge to the package-private {@code SqsConsumer.resolveCorrelationId}. */
public final class SqsConsumerTestAccess {
    private SqsConsumerTestAccess() {
    }

    public static String resolve(Map<String, Object> headers, SqsMessagePayload payload) {
        return SqsConsumer.resolveCorrelationId(headers, payload);
    }
}
