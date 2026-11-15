package com.example.eventprocessing.infrastructure.scheduler;

import com.example.eventprocessing.application.BackoffCalculator;
import com.example.eventprocessing.application.EventMetrics;
import com.example.eventprocessing.config.EventsProperties;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.infrastructure.mongo.EventRepository;
import com.example.eventprocessing.infrastructure.sqs.SqsPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

@Component
@ConditionalOnProperty(name = "events.schedulers.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisherScheduler {

    private static final Logger log = LoggerFactory.getLogger(OutboxPublisherScheduler.class);

    private final EventRepository eventRepository;
    private final SqsPublisher sqsPublisher;
    private final BackoffCalculator backoff;
    private final EventMetrics metrics;
    private final EventsProperties props;
    /** Unique per application instance, so that two instances never share a publisher identity. */
    private final String publisherId = "outbox-publisher-" + UUID.randomUUID();

    public OutboxPublisherScheduler(EventRepository eventRepository, SqsPublisher sqsPublisher,
                                    BackoffCalculator backoff, EventMetrics metrics, EventsProperties props) {
        this.eventRepository = eventRepository;
        this.sqsPublisher = sqsPublisher;
        this.backoff = backoff;
        this.metrics = metrics;
        this.props = props;
    }

    @Scheduled(fixedDelayString = "${events.outbox.interval-ms:2000}")
    public void publishOutboxEvents() {
        try {
            Duration leaseDuration = Duration.ofSeconds(props.getOutbox().getLeaseSeconds());
            int batchSize = props.getOutbox().getBatchSize();
            // Claim only when there is capacity left: never claim an event we will not publish in this run.
            for (int i = 0; i < batchSize; i++) {
                Optional<Event> claimed = eventRepository.claimOutboxEvent(publisherId, leaseDuration);
                if (claimed.isEmpty()) {
                    break;
                }
                publishOne(claimed.get());
            }
        } catch (Exception e) {
            log.error("Outbox publisher run failed", e);
        }
    }

    private void publishOne(Event event) {
        String eventId = event.getEventId();
        long publishLeaseVersion = event.getPublishLeaseVersion();
        MDC.put("eventId", eventId);
        if (event.getCorrelationId() != null) {
            MDC.put("correlationId", event.getCorrelationId());
        }
        try {
            sqsPublisher.publishEventId(eventId, event.getCorrelationId());
            boolean published = eventRepository.markOutboxPublished(eventId, publisherId, publishLeaseVersion);
            if (!published) {
                // Lease lost / replaced (fencing). A duplicate SQS message may exist; the worker claim makes it harmless.
                log.warn("Could not mark outbox PUBLISHED for eventId={}; lease lost or expired", eventId);
            }
        } catch (Exception e) {
            log.error("Failed to publish outbox eventId={} to SQS", eventId, e);
            metrics.outboxFailure();
            // outboxPublishAttempts was already incremented by the claim.
            Instant nextAttemptAt = Instant.now().plus(backoff.outboxBackoff(event.getOutboxPublishAttempts()));
            eventRepository.markOutboxFailedAndScheduleRetry(eventId, publisherId, publishLeaseVersion, nextAttemptAt);
        } finally {
            MDC.remove("eventId");
            MDC.remove("correlationId");
        }
    }
}
