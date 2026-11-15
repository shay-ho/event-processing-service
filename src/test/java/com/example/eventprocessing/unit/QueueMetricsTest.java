package com.example.eventprocessing.unit;

import com.example.eventprocessing.application.QueueMetrics;
import com.example.eventprocessing.config.EventsProperties;
import com.example.eventprocessing.domain.Event;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueAttributesResponse;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlRequest;
import software.amazon.awssdk.services.sqs.model.GetQueueUrlResponse;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/** Spec section 12: queue depth and message/event age metrics. */
class QueueMetricsTest {

    private SqsAsyncClient sqs;
    private MongoTemplate mongo;
    private SimpleMeterRegistry registry;
    private QueueMetrics metrics;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        sqs = mock(SqsAsyncClient.class);
        mongo = mock(MongoTemplate.class);
        registry = new SimpleMeterRegistry();

        when(sqs.getQueueUrl(any(Consumer.class))).thenReturn(
                CompletableFuture.completedFuture(GetQueueUrlResponse.builder().queueUrl("http://q/url").build()));
        when(sqs.getQueueAttributes(any(Consumer.class))).thenReturn(CompletableFuture.completedFuture(
                GetQueueAttributesResponse.builder().attributes(Map.of(
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES, "7",
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE, "3",
                        QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED, "1")).build()));

        metrics = new QueueMetrics(Optional.of(sqs), mongo, registry, new EventsProperties());
    }

    private double queueGauge(String queue, String state) {
        return registry.get("events.sqs.queue.messages").tag("queue", queue).tag("state", state).gauge().value();
    }

    @Test
    void queueDepthGaugesAreRegisteredForMainQueueAndDlqAndPopulatedOnRefresh() {
        metrics.refresh();

        for (String queue : new String[]{"events-queue", "events-queue-dlq"}) {
            assertEquals(7, queueGauge(queue, "visible"));
            assertEquals(3, queueGauge(queue, "in_flight"));
            assertEquals(1, queueGauge(queue, "delayed"));
        }
    }

    @Test
    void oldestAgeGaugesReflectTheOldestUnfinishedEventAndPendingOutbox() {
        Event old = new Event();
        old.setCreatedAt(Instant.now().minusSeconds(120));
        when(mongo.findOne(any(Query.class), eq(Event.class))).thenReturn(old);

        metrics.refresh();

        assertTrue(registry.get("events.oldest.unprocessed.age.seconds").gauge().value() >= 119);
        assertTrue(registry.get("events.outbox.oldest.pending.age.seconds").gauge().value() >= 119);
    }

    @Test
    void ageIsZeroWhenNothingIsWaiting() {
        when(mongo.findOne(any(Query.class), eq(Event.class))).thenReturn(null);
        metrics.refresh();
        assertEquals(0, registry.get("events.oldest.unprocessed.age.seconds").gauge().value());
    }

    @Test
    void sqsFailureDoesNotBreakRefreshAndStillUpdatesEventAges() {
        when(sqs.getQueueAttributes(any(Consumer.class))).thenReturn(CompletableFuture.failedFuture(new RuntimeException("sqs down")));
        Event old = new Event();
        old.setCreatedAt(Instant.now().minusSeconds(60));
        when(mongo.findOne(any(Query.class), eq(Event.class))).thenReturn(old);

        assertDoesNotThrow(() -> metrics.refresh());
        assertTrue(registry.get("events.oldest.unprocessed.age.seconds").gauge().value() >= 59);
    }

    @Test
    void worksWithoutSqsClient() {
        QueueMetrics noSqs = new QueueMetrics(Optional.empty(), mongo, new SimpleMeterRegistry(), new EventsProperties());
        assertDoesNotThrow(noSqs::refresh);
    }
}
