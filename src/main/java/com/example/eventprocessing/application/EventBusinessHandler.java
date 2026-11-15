package com.example.eventprocessing.application;

import com.example.eventprocessing.domain.Event;

/**
 * The pluggable business logic executed by the worker.
 * <p>
 * If it calls an external service (e.g. a payment gateway) it MUST pass {@code event.getEventId()} as a constant
 * Idempotency-Key on every attempt (spec section 11). That protection only holds if the external service supports
 * idempotency keys; otherwise duplicate external side effects cannot be prevented with MongoDB + SQS alone.
 * <p>
 * Normal processing time must stay well below {@code events.processing.lease-seconds} (there is no lease renewal).
 * A result written after the lease expired is rejected by the fenced update and the event is processed again.
 */
public interface EventBusinessHandler {

    /**
     * @throws BusinessExceptions.RetryableBusinessException    temporary failure
     * @throws BusinessExceptions.NonRetryableBusinessException invalid business data
     */
    void handle(Event event);
}
