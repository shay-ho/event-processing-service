package com.example.eventprocessing.config;

import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.SqsAsyncClientBuilder;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

/** Spec section 12: timeouts on every external call - SQS client API-call and per-attempt timeouts. */
class SqsClientTimeoutTest {

    private static SqsAsyncClientBuilder builder() {
        return SqsAsyncClient.builder()
                .region(Region.US_EAST_1)
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")));
    }

    @Test
    void configuredTimeoutsAreAppliedToTheClient() {
        EventsProperties.Sqs sqs = new EventsProperties.Sqs();
        sqs.setApiCallTimeoutSeconds(45);
        sqs.setApiCallAttemptTimeoutSeconds(20);
        SqsAsyncClientBuilder b = builder();
        SqsClientConfig.applyTimeouts(b, sqs);
        try (SqsAsyncClient client = b.build()) {
            var cfg = client.serviceClientConfiguration().overrideConfiguration();
            assertEquals(Duration.ofSeconds(45), cfg.apiCallTimeout().orElseThrow());
            assertEquals(Duration.ofSeconds(20), cfg.apiCallAttemptTimeout().orElseThrow());
        }
    }

    @Test
    void defaultsExceedTheLongPollWait() {
        EventsProperties.Sqs sqs = new EventsProperties.Sqs();
        assertTrue(sqs.getApiCallAttemptTimeoutSeconds() > 10, "must exceed the 10s long-poll wait");
        assertTrue(sqs.getApiCallTimeoutSeconds() >= sqs.getApiCallAttemptTimeoutSeconds());
    }

    @Test
    void inconsistentTimeoutsAreRejected() {
        EventsProperties.Sqs sqs = new EventsProperties.Sqs();
        sqs.setApiCallTimeoutSeconds(5);
        sqs.setApiCallAttemptTimeoutSeconds(30);
        assertThrows(IllegalArgumentException.class, () -> SqsClientConfig.applyTimeouts(builder(), sqs));
    }
}
