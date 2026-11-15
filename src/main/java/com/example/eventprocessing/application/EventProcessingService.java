package com.example.eventprocessing.application;

import com.example.eventprocessing.application.BusinessExceptions.NonRetryableBusinessException;
import com.example.eventprocessing.application.BusinessExceptions.RetryableBusinessException;
import com.example.eventprocessing.config.EventsProperties;
import com.example.eventprocessing.domain.DlqStatus;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.infrastructure.mongo.EventRepository;
import com.example.eventprocessing.infrastructure.sqs.SqsPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Worker-side orchestration: atomic claim (which also takes over an expired lease), business processing,
 * fenced result updates, retry/fail decisions and the hand-off of finally failed events to the DLQ.
 * <p>
 * Any RuntimeException thrown from {@link #handleMessage} means "do NOT ACK the SQS message": SQS redelivers it after
 * the Visibility Timeout (and redrives it to the DLQ after {@code maxReceiveCount} failed receives).
 * <p>
 * There is no lease renewal: the SQS Visibility Timeout MUST be longer than {@code events.processing.lease-seconds},
 * and normal processing must be much shorter than the lease. A crashed worker simply never ACKs; once the lease
 * expires the redelivered message takes the event over in {@code claimWorkerEvent}.
 */
@Service
public class EventProcessingService {

    private static final Logger log = LoggerFactory.getLogger(EventProcessingService.class);

    private final EventRepository eventRepository;
    private final EventBusinessHandler businessHandler;
    private final BackoffCalculator backoff;
    private final EventMetrics metrics;
    private final EventsProperties props;
    private final SqsPublisher sqsPublisher;

    public EventProcessingService(EventRepository eventRepository,
                                  EventBusinessHandler businessHandler,
                                  BackoffCalculator backoff,
                                  EventMetrics metrics,
                                  EventsProperties props,
                                  SqsPublisher sqsPublisher) {
        this.eventRepository = eventRepository;
        this.businessHandler = businessHandler;
        this.backoff = backoff;
        this.metrics = metrics;
        this.props = props;
        this.sqsPublisher = sqsPublisher;
    }

    public void handleMessage(String eventId, String correlationId) {
        String workerId = "worker-" + UUID.randomUUID();
        Duration leaseDuration = Duration.ofSeconds(props.getProcessing().getLeaseSeconds());

        MDC.put("eventId", eventId);
        MDC.put("workerId", workerId);
        if (correlationId != null) {
            MDC.put("correlationId", correlationId);
        }
        try {
            Optional<Event> claimedOpt = eventRepository.claimWorkerEvent(eventId, workerId, leaseDuration);
            if (claimedOpt.isEmpty()) {
                handleUnclaimedMessage(eventId);
                return;
            }
            Event event = claimedOpt.get();
            if (correlationId == null && event.getCorrelationId() != null) {
                MDC.put("correlationId", event.getCorrelationId());
            }
            MDC.put("leaseVersion", String.valueOf(event.getLeaseVersion()));
            processClaimed(event, workerId);
        } finally {
            MDC.remove("eventId");
            MDC.remove("workerId");
            MDC.remove("correlationId");
            MDC.remove("leaseVersion");
        }
    }

    private void processClaimed(Event event, String workerId) {
        String eventId = event.getEventId();
        long leaseVersion = event.getLeaseVersion();

        long startNanos = System.nanoTime();
        try {
            businessHandler.handle(event);
        } catch (NonRetryableBusinessException e) {
            failPermanently(event, workerId, leaseVersion, e.getMessage());
            return;
        } catch (RetryableBusinessException e) {
            retryOrFail(event, workerId, leaseVersion, e.getMessage());
            return;
        } catch (RuntimeException e) {
            // Unexpected error in business logic is treated as a temporary failure.
            log.error("Unexpected business error for eventId={}", eventId, e);
            retryOrFail(event, workerId, leaseVersion, "Unexpected error: " + e.getClass().getSimpleName());
            return;
        } finally {
            metrics.processingTimer().record(System.nanoTime() - startNanos, TimeUnit.NANOSECONDS);
        }

        if (!eventRepository.markProcessed(eventId, workerId, leaseVersion)) {
            throw leaseLost("PROCESSED", eventId);
        }
        log.info("Processed eventId={}", eventId);
    }

    private void retryOrFail(Event event, String workerId, long leaseVersion, String reason) {
        int nextRetryCount = event.getRetryCount() + 1;
        if (nextRetryCount > props.getProcessing().getMaxRetries()) {
            failPermanently(event, workerId, leaseVersion, "Max retries exceeded: " + reason);
            return;
        }
        Instant nextAttemptAt = Instant.now().plus(backoff.businessRetryBackoff(nextRetryCount));
        boolean scheduled = eventRepository.scheduleRetry(event.getEventId(), workerId, leaseVersion,
                nextRetryCount, nextAttemptAt, reason);
        if (!scheduled) {
            throw leaseLost("RETRYING", event.getEventId());
        }
        metrics.retryScheduled();
        log.warn("eventId={} scheduled retry #{} at {}", event.getEventId(), nextRetryCount, nextAttemptAt);
    }

    /** FAILED is written first (fenced); the DLQ message is then sent. If the send fails the message is not ACKed. */
    private void failPermanently(Event event, String workerId, long leaseVersion, String reason) {
        if (!eventRepository.markFailed(event.getEventId(), workerId, leaseVersion, reason)) {
            throw leaseLost("FAILED", event.getEventId());
        }
        log.warn("eventId={} FAILED: {}", event.getEventId(), reason);
        publishToDlq(event.getEventId(), event.getCorrelationId(), reason, event.getRetryCount());
    }

    /**
     * Sends the DLQ message and marks {@code dlqStatus=PUBLISHED}. Failure => exception => no ACK => SQS redelivers,
     * and {@link #handleUnclaimedMessage} retries the publication (and SQS Redrive is the last safety net).
     * At-least-once: if the Mongo update fails after a successful send, the DLQ message may be sent twice;
     * DLQ consumers de-duplicate by eventId.
     */
    private void publishToDlq(String eventId, String correlationId, String errorMessage, int retryCount) {
        try {
            sqsPublisher.publishDlqMessage(eventId, correlationId, errorMessage, retryCount);
        } catch (RuntimeException e) {
            metrics.dlqFailure();
            log.error("Failed to publish DLQ message for eventId={}; message is not ACKed", eventId, e);
            throw e;
        }
        if (eventRepository.markDlqPublished(eventId)) {
            metrics.dlqPublished();
        } else {
            log.warn("DLQ message sent for eventId={} but dlqStatus was not updated to PUBLISHED", eventId);
        }
    }

    private IllegalStateException leaseLost(String target, String eventId) {
        // Fencing: the lease expired or was taken over; this worker must not write a result and must not ACK.
        metrics.leaseLost();
        log.error("Worker lost ownership of eventId={}; could not update MongoDB to {}", eventId, target);
        return new IllegalStateException("Failed to update MongoDB to " + target + " (lease lost) for event: " + eventId);
    }

    /**
     * Claim returned empty: ACK only when it is safe - final state, a worker with a valid lease already owns it,
     * or a RETRYING event whose time has not come (the Outbox will send a new message then).
     * A FAILED event whose DLQ message was not published yet is published now. Otherwise throw so SQS redelivers.
     */
    private void handleUnclaimedMessage(String eventId) {
        Optional<Event> currentOpt = eventRepository.findByEventId(eventId);
        if (currentOpt.isEmpty()) {
            log.warn("SQS message refers to non-existent eventId={}; ACK", eventId);
            return;
        }
        Event existing = currentOpt.get();
        Instant now = Instant.now();
        EventStatus status = existing.getStatus();
        if (status == EventStatus.FAILED && existing.getDlqStatus() == DlqStatus.PENDING) {
            log.warn("eventId={} is FAILED but its DLQ message is not published yet; publishing now", eventId);
            publishToDlq(eventId, existing.getCorrelationId(), existing.getErrorMessage(), existing.getRetryCount());
        } else if (status == EventStatus.PROCESSED || status == EventStatus.FAILED) {
            log.info("Duplicate message for eventId={} in final status {}; ACK", eventId, status);
        } else if (status == EventStatus.PROCESSING && existing.getProcessingLeaseUntil() != null
                && existing.getProcessingLeaseUntil().isAfter(now)) {
            log.info("eventId={} is being processed by {}; ACK duplicate", eventId, existing.getWorkerId());
        } else if (status == EventStatus.RETRYING && existing.getNextProcessingAttemptAt() != null
                && existing.getNextProcessingAttemptAt().isAfter(now)) {
            log.info("eventId={} retry is scheduled for the future; ACK early message", eventId);
        } else {
            log.warn("Claim returned empty for eventId={} in status {}; not ACK-ing", eventId, status);
            throw new IllegalStateException("Unable to claim event: " + eventId);
        }
    }
}
