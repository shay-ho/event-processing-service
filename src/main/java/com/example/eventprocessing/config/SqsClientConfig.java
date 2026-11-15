package com.example.eventprocessing.config;

import io.awspring.cloud.autoconfigure.sqs.SqsAsyncClientCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;

import java.time.Duration;

/**
 * Spec section 12: explicit timeouts for every external call. MongoDB timeouts are in the connection URI;
 * this applies API-call and per-attempt timeouts to the SQS client (used by the publisher AND the listener).
 */
@Configuration
public class SqsClientConfig {

    @Bean
    public SqsAsyncClientCustomizer sqsTimeoutCustomizer(EventsProperties props) {
        return builder -> applyTimeouts(builder, props.getSqs());
    }

    static void applyTimeouts(SqsAsyncClientBuilder builder, EventsProperties.Sqs sqs) {
        long attempt = sqs.getApiCallAttemptTimeoutSeconds();
        long total = sqs.getApiCallTimeoutSeconds();
        if (attempt <= 0 || total < attempt) {
            throw new IllegalArgumentException(
                    "events.sqs.api-call-timeout-seconds must be >= events.sqs.api-call-attempt-timeout-seconds > 0");
        }
        builder.overrideConfiguration(o -> o
                .apiCallTimeout(Duration.ofSeconds(total))
                .apiCallAttemptTimeout(Duration.ofSeconds(attempt)));
    }
}
