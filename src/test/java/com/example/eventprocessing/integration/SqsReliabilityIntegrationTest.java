package com.example.eventprocessing.integration;

import com.example.eventprocessing.api.dto.CreateEventRequest;
import com.example.eventprocessing.application.BusinessExceptions;
import com.example.eventprocessing.application.EventBusinessHandler;
import com.example.eventprocessing.application.EventIngestionService;
import com.example.eventprocessing.application.EventProcessingService;
import com.example.eventprocessing.domain.DlqStatus;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.domain.OutboxStatus;
import com.example.eventprocessing.infrastructure.mongo.EventRepository;
import com.example.eventprocessing.infrastructure.scheduler.OutboxPublisherScheduler;
import com.example.eventprocessing.infrastructure.sqs.SqsPublisher;
import io.awspring.cloud.sqs.operations.SqsTemplate;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.Message;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Real MongoDB + LocalStack SQS tests (requires Docker) for the scenarios spec section 14 asks to PROVE:
 * <ul>
 *   <li>Correlation ID travels as an SQS Message Attribute and the consumer reads it ({@code Sqs_MA_} prefix).</li>
 *   <li>Worker crash: no ACK -> SQS redelivers after the Visibility Timeout -> the claim takes over the expired lease.</li>
 *   <li>Outbox crash window: SQS send succeeded, Mongo PUBLISHED update did not -> duplicate message -> one business execution.</li>
 *   <li>A finally failed event is sent to the DLQ directly by the worker.</li>
 * </ul>
 * The Outbox interval is set very high and the tests invoke the scheduler explicitly, so every step is deterministic.
 * The main queue has a 4s visibility timeout and the worker lease is 2s (lease &lt; visibility timeout, as in production:
 * 30s lease / 60s visibility) to make the crash recovery observable.
 */
@SpringBootTest(properties = {
        "events.sqs.queue-name=rel-queue",
        "events.sqs.dlq-name=rel-queue-dlq",
        "events.schedulers.enabled=true",
        "events.outbox.interval-ms=3600000",
        "events.processing.lease-seconds=2",
        "events.processing.jitter-ratio=0",
        "events.processing.failure-simulation-enabled=true"
})
class SqsReliabilityIntegrationTest {

    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");
    static final LocalStackContainer LOCALSTACK =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.4")).withServices(LocalStackContainer.Service.SQS);

    static {
        MONGO.start();
        LOCALSTACK.start();
        try {
            LOCALSTACK.execInContainer("awslocal", "sqs", "create-queue", "--queue-name", "rel-queue-dlq");
            LOCALSTACK.execInContainer("awslocal", "sqs", "create-queue", "--queue-name", "rel-queue",
                    "--attributes", "VisibilityTimeout=4");
        } catch (Exception e) {
            throw new IllegalStateException("Failed to create LocalStack queues", e);
        }
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("sqs_rel_db"));
        registry.add("spring.cloud.aws.endpoint", () -> LOCALSTACK.getEndpointOverride(LocalStackContainer.Service.SQS).toString());
        registry.add("spring.cloud.aws.region.static", LOCALSTACK::getRegion);
        registry.add("spring.cloud.aws.credentials.access-key", LOCALSTACK::getAccessKey);
        registry.add("spring.cloud.aws.credentials.secret-key", LOCALSTACK::getSecretKey);
    }

    /** Counts business executions per eventId; can be slowed down via metadata.slowMillis; supports simulateFailure=permanent. */
    static class CountingHandler implements EventBusinessHandler {
        final Map<String, AtomicInteger> executions = new ConcurrentHashMap<>();

        @Override
        public void handle(Event event) {
            executions.computeIfAbsent(event.getEventId(), k -> new AtomicInteger()).incrementAndGet();
            Map<String, Object> md = event.getMetadata();
            if (md == null) {
                return;
            }
            if ("permanent".equals(md.get("simulateFailure"))) {
                throw new BusinessExceptions.NonRetryableBusinessException("Simulated invalid business data");
            }
            if (md.get("slowMillis") instanceof Number n) {
                try {
                    Thread.sleep(n.longValue());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        int count(String eventId) {
            AtomicInteger c = executions.get(eventId);
            return c == null ? 0 : c.get();
        }
    }

    @TestConfiguration
    static class HandlerConfig {
        @Bean
        @Primary
        CountingHandler countingHandler() {
            return new CountingHandler();
        }
    }

    @SpyBean
    private EventProcessingService processingService;
    @Autowired
    private CountingHandler handler;
    @Autowired
    private EventIngestionService ingestionService;
    @Autowired
    private EventRepository eventRepository;
    @Autowired
    private MongoTemplate mongoTemplate;
    @Autowired
    private OutboxPublisherScheduler outboxScheduler;
    @Autowired
    private SqsPublisher sqsPublisher;
    @Autowired
    private SqsTemplate sqsTemplate;
    @Autowired
    private SqsAsyncClient sqs;

    @BeforeEach
    void clean() {
        eventRepository.deleteAll();
    }

    private void ingest(String eventId, Map<String, Object> metadata) {
        CreateEventRequest req = new CreateEventRequest();
        req.setEventId(eventId);
        req.setCustomerId("cust-sqs");
        req.setType("PAYMENT");
        req.setAmount(new BigDecimal("10.00"));
        req.setCurrency("ILS");
        req.setTimestamp(Instant.now());
        req.setMetadata(metadata);
        ingestionService.ingestEvent(req, "corr-" + eventId);
    }

    private Event load(String eventId) {
        return eventRepository.findByEventId(eventId).orElseThrow();
    }

    private void awaitStatus(String eventId, EventStatus status) {
        await().atMost(Duration.ofSeconds(45)).pollInterval(Duration.ofMillis(300))
                .until(() -> eventRepository.findByEventId(eventId).map(e -> e.getStatus() == status).orElse(false));
    }

    private void setField(String eventId, String field, Object value) {
        mongoTemplate.updateFirst(new Query(Criteria.where("eventId").is(eventId)), new Update().set(field, value), Event.class);
    }

    private List<Message> receiveFromDlq(String eventId, int expected, Duration timeout) throws Exception {
        String url = sqs.getQueueUrl(r -> r.queueName("rel-queue-dlq")).get().queueUrl();
        List<Message> found = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        while (found.size() < expected && System.nanoTime() < deadline) {
            for (Message m : sqs.receiveMessage(r -> r.queueUrl(url).maxNumberOfMessages(10).waitTimeSeconds(2)
                    .messageAttributeNames("All")).get().messages()) {
                if (m.body().contains("\"" + eventId + "\"")) {
                    found.add(m);
                }
            }
        }
        return found;
    }

    // ---------------------------------------------------------------- Correlation ID (spec section 12)

    @Test
    void publisherSendsCorrelationIdAsSqsMessageAttribute() throws Exception {
        sqsPublisher.publishDlqMessage("evt-attr-1", "corr-attr-1", "boom", 3);

        List<Message> messages = receiveFromDlq("evt-attr-1", 1, Duration.ofSeconds(20));
        assertEquals(1, messages.size());
        assertNotNull(messages.get(0).messageAttributes().get(SqsPublisher.CORRELATION_ATTRIBUTE),
                "X-Correlation-ID must be present as an SQS Message Attribute");
        assertEquals("corr-attr-1", messages.get(0).messageAttributes().get(SqsPublisher.CORRELATION_ATTRIBUTE).stringValue());
    }

    @Test
    void consumerReadsCorrelationIdFromMessageAttributeAndNotOnlyFromBody() {
        ingest("evt-corr-1", Map.of("source", "it"));
        // Body carries a DIFFERENT correlation id: the attribute must win, proving it is read via the Sqs_MA_ header.
        sqsTemplate.send(to -> to.queue("rel-queue")
                .payload("{\"eventId\":\"evt-corr-1\",\"correlationId\":\"body-corr\"}")
                .header(SqsPublisher.CORRELATION_ATTRIBUTE, "attribute-corr"));

        verify(processingService, timeout(30_000)).handleMessage(eq("evt-corr-1"), eq("attribute-corr"));
        awaitStatus("evt-corr-1", EventStatus.PROCESSED);
    }

    // ---------------------------------------------------------------- Worker crash: redelivery + takeover

    @Test
    void crashedWorkerMessageIsRedeliveredAfterVisibilityTimeoutAndTakenOverByExpiredLeaseClaim() {
        ingest("evt-crash-1", Map.of("source", "it"));
        // First delivery: the worker claims the event and "crashes" (no result written, no ACK). Later calls run for real.
        doAnswer(inv -> {
            eventRepository.claimWorkerEvent(inv.getArgument(0), "crashed-worker", Duration.ofSeconds(2)).orElseThrow();
            throw new IllegalStateException("simulated worker crash");
        }).doCallRealMethod().when(processingService).handleMessage(eq("evt-crash-1"), any());

        outboxScheduler.publishOutboxEvents();

        awaitStatus("evt-crash-1", EventStatus.PROCESSED);
        verify(processingService, times(2)).handleMessage(eq("evt-crash-1"), any());
        assertEquals(1, handler.count("evt-crash-1"), "the business logic runs once: only the second delivery got to it");
        assertEquals(2, load("evt-crash-1").getLeaseVersion(), "the takeover claim incremented the fencing token");
    }

    // ---------------------------------------------------------------- Outbox crash window

    @Test
    void outboxCrashWindowDuplicateSqsMessageResultsInSingleBusinessExecution() {
        ingest("evt-ocw-1", Map.of("source", "it"));

        // Publisher A: claims, SENDS to SQS successfully, then "crashes" before marking PUBLISHED in Mongo.
        Event claimed = eventRepository.claimOutboxEvent("crashed-publisher", Duration.ofSeconds(30)).orElseThrow();
        sqsPublisher.publishEventId(claimed.getEventId(), claimed.getCorrelationId());
        assertEquals(OutboxStatus.PUBLISHING, load("evt-ocw-1").getOutboxStatus());

        // Lease expires; a live publisher recovers the event and sends the message AGAIN.
        setField("evt-ocw-1", "publishingLeaseUntil", Instant.now().minusSeconds(5));
        outboxScheduler.publishOutboxEvents();

        // Both messages reach the worker; the atomic claim guarantees a single business execution.
        verify(processingService, timeout(30_000).times(2)).handleMessage(eq("evt-ocw-1"), any());
        awaitStatus("evt-ocw-1", EventStatus.PROCESSED);
        await().atMost(Duration.ofSeconds(10)).until(() -> load("evt-ocw-1").getOutboxStatus() == OutboxStatus.PUBLISHED);
        assertEquals(1, handler.count("evt-ocw-1"), "duplicate SQS message must not cause a second business execution");
        assertEquals(0, load("evt-ocw-1").getRetryCount());
    }

    // ---------------------------------------------------------------- DLQ published directly by the worker

    @Test
    void permanentFailureIsSentToDlqDirectlyByTheWorkerWithCorrelationId() throws Exception {
        ingest("evt-dlq-1", Map.of("simulateFailure", "permanent"));
        outboxScheduler.publishOutboxEvents();

        awaitStatus("evt-dlq-1", EventStatus.FAILED);
        await().atMost(Duration.ofSeconds(10)).until(() -> load("evt-dlq-1").getDlqStatus() == DlqStatus.PUBLISHED);

        List<Message> dlqMessages = receiveFromDlq("evt-dlq-1", 1, Duration.ofSeconds(30));
        assertEquals(1, dlqMessages.size());
        assertEquals("corr-evt-dlq-1",
                dlqMessages.get(0).messageAttributes().get(SqsPublisher.CORRELATION_ATTRIBUTE).stringValue());
    }
}
