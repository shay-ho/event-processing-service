package com.example.eventprocessing.application;

import com.example.eventprocessing.domain.Event;

/**
 * Port to the external system the business logic calls (e.g. a payment gateway).
 * Spec section 11: every attempt for the same event MUST send the same, constant Idempotency-Key
 * (the {@code eventId}). The duplicate-action guarantee holds only if the external service honours that key.
 */
public interface ExternalOperationClient {

    /**
     * @param idempotencyKey constant per event ({@code event.getEventId()}), identical on every attempt/retry
     * @throws BusinessExceptions.RetryableBusinessException on a temporary failure of the external call
     */
    void execute(String idempotencyKey, Event event);
}
