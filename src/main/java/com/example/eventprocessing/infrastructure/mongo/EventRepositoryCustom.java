package com.example.eventprocessing.infrastructure.mongo;

import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

public interface EventRepositoryCustom {

    /** History query: createdAt in [from, to] (both inclusive, both optional), optional status. Sorted by pageable. */
    Page<Event> findCustomerEvents(String customerId, Instant from, Instant to, EventStatus status, Pageable pageable);

    Optional<Event> claimOutboxEvent(String publisherId, Duration leaseDuration);

    boolean markOutboxPublished(String eventId, String publisherId, long publishLeaseVersion);

    boolean markOutboxFailedAndScheduleRetry(String eventId, String publisherId, long publishLeaseVersion, Instant nextAttemptAt);

    /**
     * Atomic claim of one event for processing. Claimable: {@code RECEIVED}; {@code RETRYING} whose
     * {@code nextProcessingAttemptAt} has arrived; or {@code PROCESSING} whose lease has expired (a crashed worker
     * is recovered right here, there is no separate recovery scheduler). Each claim increments {@code leaseVersion}
     * (fencing token).
     */
    Optional<Event> claimWorkerEvent(String eventId, String workerId, Duration leaseDuration);

    boolean markProcessed(String eventId, String workerId, long leaseVersion);

    boolean scheduleRetry(String eventId, String workerId, long leaseVersion, int nextRetryCount, Instant nextAttemptAt, String errorMessage);

    /** Sets FAILED and {@code dlqStatus=PENDING}; the worker then publishes the DLQ message. */
    boolean markFailed(String eventId, String workerId, long leaseVersion, String errorMessage);

    /** FAILED + dlqStatus PENDING -> PUBLISHED. */
    boolean markDlqPublished(String eventId);
}
