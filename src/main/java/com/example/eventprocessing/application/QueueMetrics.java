package com.example.eventprocessing.application;

import com.example.eventprocessing.config.EventsProperties;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.domain.OutboxStatus;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Spec section 12 monitoring: queue depth and message/event age.
 * <ul>
 *   <li>{@code events.sqs.queue.messages{queue,state=visible|in_flight|delayed}} - from SQS GetQueueAttributes
 *       (main queue and DLQ).</li>
 *   <li>{@code events.oldest.unprocessed.age.seconds} - age of the oldest event that is still RECEIVED / RETRYING /
 *       PROCESSING (MongoDB is the source of truth).</li>
 *   <li>{@code events.outbox.oldest.pending.age.seconds} - age of the oldest event whose Outbox is still PENDING.</li>
 * </ul>
 * SQS's own {@code ApproximateAgeOfOldestMessage} is only available in CloudWatch (it is not a queue attribute),
 * so the alarm on it is a deployment concern; the Mongo-based ages above are the in-application equivalent.
 * All values are refreshed periodically (never at scrape time) and a failed refresh never breaks the application.
 */
@Component
public class QueueMetrics {

    private static final Logger log = LoggerFactory.getLogger(QueueMetrics.class);
    private static final Duration SQS_CALL_TIMEOUT = Duration.ofSeconds(10);

    private final SqsAsyncClient sqs;
    private final MongoTemplate mongoTemplate;
    private final List<String> queueNames;
    private final Map<String, AtomicLong[]> depthByQueue = new java.util.concurrent.ConcurrentHashMap<>();
    private final Map<String, String> queueUrls = new java.util.concurrent.ConcurrentHashMap<>();
    private final AtomicLong oldestUnprocessedAgeSeconds = new AtomicLong();
    private final AtomicLong oldestPendingOutboxAgeSeconds = new AtomicLong();

    public QueueMetrics(Optional<SqsAsyncClient> sqs, MongoTemplate mongoTemplate,
                        MeterRegistry registry, EventsProperties props) {
        this.sqs = sqs.orElse(null);
        this.mongoTemplate = mongoTemplate;
        this.queueNames = List.of(props.getSqs().getQueueName(), props.getSqs().getDlqName());

        for (String queue : queueNames) {
            AtomicLong visible = new AtomicLong();
            AtomicLong inFlight = new AtomicLong();
            AtomicLong delayed = new AtomicLong();
            depthByQueue.put(queue, new AtomicLong[]{visible, inFlight, delayed});
            register(registry, queue, "visible", visible);
            register(registry, queue, "in_flight", inFlight);
            register(registry, queue, "delayed", delayed);
        }
        Gauge.builder("events.oldest.unprocessed.age.seconds", oldestUnprocessedAgeSeconds, AtomicLong::get)
                .baseUnit("seconds").register(registry);
        Gauge.builder("events.outbox.oldest.pending.age.seconds", oldestPendingOutboxAgeSeconds, AtomicLong::get)
                .baseUnit("seconds").register(registry);
    }

    private static void register(MeterRegistry registry, String queue, String state, AtomicLong holder) {
        Gauge.builder("events.sqs.queue.messages", holder, AtomicLong::get)
                .tag("queue", queue).tag("state", state).register(registry);
    }

    @Scheduled(fixedDelayString = "${events.sqs.metrics-refresh-ms:30000}")
    public void refresh() {
        refreshQueueDepth();
        refreshEventAges();
    }

    void refreshQueueDepth() {
        if (sqs == null) {
            return;
        }
        for (String queue : queueNames) {
            try {
                String url = queueUrls.computeIfAbsent(queue, this::resolveQueueUrl);
                Map<QueueAttributeName, String> attrs = sqs.getQueueAttributes(r -> r.queueUrl(url).attributeNames(
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES,
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE,
                                QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED))
                        .get(SQS_CALL_TIMEOUT.toSeconds(), TimeUnit.SECONDS).attributes();
                AtomicLong[] holders = depthByQueue.get(queue);
                holders[0].set(parse(attrs.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES)));
                holders[1].set(parse(attrs.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_NOT_VISIBLE)));
                holders[2].set(parse(attrs.get(QueueAttributeName.APPROXIMATE_NUMBER_OF_MESSAGES_DELAYED)));
            } catch (Exception e) {
                queueUrls.remove(queue);
                log.warn("Failed to refresh SQS depth metrics for queue {}: {}", queue, e.getMessage());
            }
        }
    }

    private String resolveQueueUrl(String queue) {
        try {
            return sqs.getQueueUrl(r -> r.queueName(queue)).get(SQS_CALL_TIMEOUT.toSeconds(), TimeUnit.SECONDS).queueUrl();
        } catch (Exception e) {
            throw new IllegalStateException("Cannot resolve queue url for " + queue, e);
        }
    }

    void refreshEventAges() {
        try {
            oldestUnprocessedAgeSeconds.set(ageOfOldest(Criteria.where("status")
                    .in(EventStatus.RECEIVED, EventStatus.RETRYING, EventStatus.PROCESSING)));
            oldestPendingOutboxAgeSeconds.set(ageOfOldest(Criteria.where("outboxStatus").is(OutboxStatus.PENDING)));
        } catch (Exception e) {
            log.warn("Failed to refresh event age metrics: {}", e.getMessage());
        }
    }

    private long ageOfOldest(Criteria criteria) {
        Query query = new Query(criteria).with(Sort.by(Sort.Direction.ASC, "createdAt")).limit(1);
        query.fields().include("createdAt");
        Event oldest = mongoTemplate.findOne(query, Event.class);
        if (oldest == null || oldest.getCreatedAt() == null) {
            return 0;
        }
        return Math.max(0, Duration.between(oldest.getCreatedAt(), Instant.now()).toSeconds());
    }

    private static long parse(String value) {
        return value == null ? 0 : Long.parseLong(value);
    }
}
