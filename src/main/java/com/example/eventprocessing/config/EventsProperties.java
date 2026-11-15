package com.example.eventprocessing.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * Central, externalised configuration (prefix "events"). All values have safe defaults
 * that match the example numbers in the spec (3 retries, 5s/15s/45s backoff, 30s lease).
 */
@Data
@ConfigurationProperties(prefix = "events")
public class EventsProperties {

    private Sqs sqs = new Sqs();
    private Processing processing = new Processing();
    private Outbox outbox = new Outbox();
    private Api api = new Api();

    @Data
    public static class Api {
        /** Maximum accepted HTTP body size of POST /api/events (spec section 12: payload size validation). */
        private long maxPayloadBytes = 32768;
    }

    @Data
    public static class Sqs {
        private String queueName = "events-queue";
        private String dlqName = "events-queue-dlq";
        /**
         * Hard timeout for one SQS API call including SDK retries. MUST be larger than the long-poll wait
         * (spring.cloud.aws.sqs.listener.poll-timeout, 10s) or receive calls would be aborted.
         */
        private long apiCallTimeoutSeconds = 60;
        /** Timeout of a single SQS HTTP attempt. Must also exceed the long-poll wait. */
        private long apiCallAttemptTimeoutSeconds = 30;
        /** How often queue depth gauges are refreshed. */
        private long metricsRefreshMs = 30000;
    }

    @Data
    public static class Processing {
        /** Max business retries (retryCount max). 3 retries => 4 processing attempts in total. */
        private int maxRetries = 3;
        /**
         * Worker lease. There is no renewal: normal processing must be much shorter, and the SQS Visibility Timeout
         * of the main queue MUST be longer than this value (60s vs 30s), so that a message redelivered after a
         * worker crash finds the lease already expired and can take the event over.
         */
        private long leaseSeconds = 30;
        private long retryBaseSeconds = 5;
        private long retryMultiplier = 3;
        private long backoffCapSeconds = 300;
        private double jitterRatio = 0.2;
        /** Demo / fault-injection hook (metadata.simulateFailure = transient|permanent). Off by default. */
        private boolean failureSimulationEnabled = false;
        /** Max messages processed concurrently (= SQS max-concurrent-messages). */
        private int workerConcurrency = 10;
    }

    @Data
    public static class Outbox {
        private long leaseSeconds = 30;
        private int batchSize = 100;
        private long baseSeconds = 5;
        private long backoffCapSeconds = 300;
        private long intervalMs = 2000;
    }
}
