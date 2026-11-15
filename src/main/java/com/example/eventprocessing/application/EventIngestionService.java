package com.example.eventprocessing.application;

import com.example.eventprocessing.api.dto.CreateEventRequest;
import com.example.eventprocessing.domain.DlqStatus;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.domain.OutboxStatus;
import com.example.eventprocessing.exception.ServiceUnavailableException;
import com.example.eventprocessing.infrastructure.mongo.EventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.Optional;

@Service
public class EventIngestionService {

    private static final Logger log = LoggerFactory.getLogger(EventIngestionService.class);

    /** Bounded retries on transient MongoDB failures (spec section 9): 3 attempts, 100ms / 200ms backoff. */
    static final int MAX_DB_ATTEMPTS = 3;
    static final long DB_BACKOFF_BASE_MS = 100;

    private final EventRepository eventRepository;
    private final EventMetrics metrics;

    public EventIngestionService(EventRepository eventRepository, EventMetrics metrics) {
        this.eventRepository = eventRepository;
        this.metrics = metrics;
    }

    public IngestionResult ingestEvent(CreateEventRequest request, String correlationId) {
        String eventId = request.getEventId();
        String timestampStr = request.getTimestamp() != null ? request.getTimestamp().toString() : "";

        String calculatedHash = CanonicalHashUtils.calculateCanonicalHash(
                request.getCustomerId(),
                request.getType(),
                request.getAmount(),
                request.getCurrency(),
                timestampStr,
                request.getMetadata()
        );

        Instant now = Instant.now();
        // One document: business data + status + Outbox fields => atomic "save event + need to publish".
        Event event = Event.builder()
                .eventId(eventId)
                .payloadHash(calculatedHash)
                .customerId(request.getCustomerId())
                .type(request.getType())
                .amount(request.getAmount())
                .currency(request.getCurrency())
                .timestamp(request.getTimestamp())
                .metadata(request.getMetadata())
                .status(EventStatus.RECEIVED)
                .retryCount(0)
                .nextProcessingAttemptAt(now)
                .outboxStatus(OutboxStatus.PENDING)
                .outboxNextAttemptAt(now)
                .dlqStatus(DlqStatus.NONE)
                .correlationId(correlationId)
                .createdAt(now)
                .updatedAt(now)
                .build();

        try {
            Event saved = withDbRetry(() -> eventRepository.insert(event));
            metrics.ingested();
            log.info("Ingested new eventId={} correlationId={}", eventId, correlationId);
            return new IngestionResult(IngestionStatus.ACCEPTED, saved);
        } catch (DuplicateKeyException e) {
            // Unique index on eventId is the final guard against concurrent duplicates.
            Optional<Event> existingOpt = withDbRetry(() -> eventRepository.findByEventId(eventId));
            if (existingOpt.isEmpty()) {
                throw e;
            }
            Event existing = existingOpt.get();
            if (calculatedHash.equals(existing.getPayloadHash())) {
                metrics.duplicate();
                log.info("Idempotent repeat for eventId={} (status={})", eventId, existing.getStatus());
                return new IngestionResult(IngestionStatus.OK, existing);
            }
            metrics.conflict();
            log.warn("Conflict for eventId={}: same eventId, different payload", eventId);
            return new IngestionResult(IngestionStatus.CONFLICT, existing);
        }
    }

    private <T> T withDbRetry(java.util.function.Supplier<T> action) {
        RuntimeException last = null;
        for (int attempt = 1; attempt <= MAX_DB_ATTEMPTS; attempt++) {
            try {
                return action.get();
            } catch (DuplicateKeyException e) {
                throw e; // not a transient failure
            } catch (DataAccessResourceFailureException | TransientDataAccessException e) {
                last = e;
                log.warn("MongoDB unavailable (attempt {}/{}): {}", attempt, MAX_DB_ATTEMPTS, e.getMessage());
                if (attempt < MAX_DB_ATTEMPTS) {
                    try {
                        Thread.sleep(DB_BACKOFF_BASE_MS * (1L << (attempt - 1)));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }
        throw new ServiceUnavailableException("MongoDB unavailable after " + MAX_DB_ATTEMPTS + " attempts", last);
    }

    public enum IngestionStatus {
        ACCEPTED,
        OK,
        CONFLICT
    }

    public static class IngestionResult {
        private final IngestionStatus status;
        private final Event event;

        public IngestionResult(IngestionStatus status, Event event) {
            this.status = status;
            this.event = event;
        }

        public IngestionStatus getStatus() {
            return status;
        }

        public Event getEvent() {
            return event;
        }
    }
}
