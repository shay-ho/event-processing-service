package com.example.eventprocessing.integration;

import com.example.eventprocessing.api.dto.CreateEventRequest;
import com.example.eventprocessing.application.EventIngestionService;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.infrastructure.mongo.EventRepository;
import com.example.eventprocessing.infrastructure.sqs.SqsPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.MongoDBContainer;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Map;

/**
 * Base class for MongoDB-backed integration tests (requires Docker).
 * SQS and all schedulers are disabled so that tests drive every state transition explicitly and deterministically;
 * {@link SqsPublisher} is a mock (the worker sends the DLQ message itself when an event finally fails).
 * The container is started once per JVM (singleton pattern) so Spring's context cache stays valid.
 */
@SpringBootTest(properties = {
        "spring.cloud.aws.sqs.enabled=false",
        "events.schedulers.enabled=false",
        "events.processing.failure-simulation-enabled=true"
})
public abstract class AbstractMongoIntegrationTest {

    static final MongoDBContainer MONGO = new MongoDBContainer("mongo:6.0");

    static {
        MONGO.start();
    }

    @DynamicPropertySource
    static void mongoProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.data.mongodb.uri", () -> MONGO.getReplicaSetUrl("event_db"));
    }

    @MockBean
    protected SqsPublisher sqsPublisher;
    @Autowired
    protected EventRepository eventRepository;
    @Autowired
    protected EventIngestionService ingestionService;
    @Autowired
    protected MongoTemplate mongoTemplate;

    @BeforeEach
    protected void cleanDatabase() {
        eventRepository.deleteAll();
    }

    protected CreateEventRequest request(String eventId, String customerId, String amount, Map<String, Object> metadata) {
        CreateEventRequest req = new CreateEventRequest();
        req.setEventId(eventId);
        req.setCustomerId(customerId);
        req.setType("PAYMENT");
        req.setAmount(new BigDecimal(amount));
        req.setCurrency("ILS");
        req.setTimestamp(Instant.parse("2026-11-12T12:00:00Z"));
        req.setMetadata(metadata);
        return req;
    }

    protected Event ingest(String eventId) {
        return ingestionService.ingestEvent(request(eventId, "cust-1", "100.00", Map.of("source", "test")), "corr-" + eventId).getEvent();
    }

    protected Event ingest(String eventId, Map<String, Object> metadata) {
        return ingestionService.ingestEvent(request(eventId, "cust-1", "100.00", metadata), "corr-" + eventId).getEvent();
    }

    protected Event load(String eventId) {
        return eventRepository.findByEventId(eventId).orElseThrow();
    }

    /** Simulates "time passes": sets arbitrary fields directly on the document. */
    protected void setFields(String eventId, Map<String, Object> fields) {
        Update update = new Update();
        fields.forEach(update::set);
        mongoTemplate.updateFirst(new Query(Criteria.where("eventId").is(eventId)), update, Event.class);
    }

    protected void expireWorkerLease(String eventId) {
        setFields(eventId, Map.of("processingLeaseUntil", Instant.now().minusSeconds(5)));
    }

    protected void expirePublisherLease(String eventId) {
        setFields(eventId, Map.of("publishingLeaseUntil", Instant.now().minusSeconds(5)));
    }
}
