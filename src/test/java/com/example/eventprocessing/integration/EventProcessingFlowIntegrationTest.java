package com.example.eventprocessing.integration;

import com.example.eventprocessing.api.dto.CreateEventRequest;
import com.example.eventprocessing.application.EventIngestionService;
import com.example.eventprocessing.domain.DlqStatus;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.domain.OutboxStatus;
import com.example.eventprocessing.infrastructure.mongo.EventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * Full end-to-end flow against real MongoDB + LocalStack SQS (requires Docker):
 * API ingestion -> Outbox publisher -> SQS -> Worker -> PROCESSED, plus Retry and DLQ paths.
 * Schedulers and the SQS listener are ENABLED here.
 */
@SpringBootTest(properties = {
        "events.processing.failure-simulation-enabled=true",
        "events.outbox.interval-ms=500",
        "events.processing.retry-base-seconds=1",
        "events.processing.jitter-ratio=0"
})
class EventProcessingFlowIntegrationTest {

    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.4")).withServices(LocalStackContainer.Service.SQS);

    static {
        MONGO.start();
        LOCALSTACK.start();
        try {
            LOCALSTACK.execInContainer("awslocal", "sqs", "create-queue", "--queue-name", "events-queue-dlq");
            LOCALSTACK.execInContainer("awslocal", "sqs", "create-queue", "--queue-name", "events-queue");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create LocalStack queues", e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("flow_db"));
        registry.add("spring.cloud.aws.endpoint", () -> LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.SQS).toString());
        registry.add("spring.cloud.aws.region.static", LOCALSTACK::getRegion);
        registry.add("spring.cloud.aws.credentials.access-key", LOCALSTACK::getAccessKey);
        registry.add("spring.cloud.aws.credentials.secret-key", LOCALSTACK::getSecretKey);
    }

    @Autowired
    private EventIngestionService ingestionService;
    @Autowired
    private EventRepository eventRepository;

    @BeforeEach
    void clean() {
        eventRepository.deleteAll();
    }

    private void ingest(String eventId, Map<String, Object> metadata) {
        CreateEventRequest req = new CreateEventRequest();
        req.setEventId(eventId);
        req.setCustomerId("cust-flow");
        req.setType("PAYMENT");
        req.setAmount(new BigDecimal("42.00"));
        req.setCurrency("ILS");
        req.setTimestamp(Instant.now());
        req.setMetadata(metadata);
        ingestionService.ingestEvent(req, "corr-" + eventId);
    }

    private Event awaitStatus(String eventId, EventStatus status, Duration timeout) {
        await().atMost(timeout).pollInterval(Duration.ofMillis(300))
                .until(() -> eventRepository.findByEventId(eventId).map(e -> e.getStatus() == status).orElse(false));
        return eventRepository.findByEventId(eventId).orElseThrow();
    }

    @Test
    void happyPathOutboxPublishesToSqsAndWorkerProcesses() {
        ingest("flow-1", Map.of("source", "it"));

        Event done = awaitStatus("flow-1", EventStatus.PROCESSED, Duration.ofSeconds(40));

        assertNotNull(done.getProcessedAt());
        assertEquals(0, done.getRetryCount());
        await().atMost(Duration.ofSeconds(10)).until(() ->
                eventRepository.findByEventId("flow-1").orElseThrow().getOutboxStatus() == OutboxStatus.PUBLISHED);
    }

    @Test
    void transientFailureIsRetriedViaOutboxAndEventuallyProcessed() {
        ingest("flow-2", Map.of("simulateFailure", "transient:1"));

        Event done = awaitStatus("flow-2", EventStatus.PROCESSED, Duration.ofSeconds(60));
        assertEquals(1, done.getRetryCount());
    }

    @Test
    void permanentFailureEndsInFailedAndDlqMessageIsPublished() {
        ingest("flow-3", Map.of("simulateFailure", "permanent"));

        awaitStatus("flow-3", EventStatus.FAILED, Duration.ofSeconds(40));
        await().atMost(Duration.ofSeconds(30)).until(() ->
                eventRepository.findByEventId("flow-3").orElseThrow().getDlqStatus() == DlqStatus.PUBLISHED);
    }
}
