package com.example.eventprocessing.application;

import com.example.eventprocessing.domain.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/** Default placeholder: the exercise has no real external service. Replace with a real client (same contract). */
@Component
public class NoOpExternalOperationClient implements ExternalOperationClient {

    private static final Logger log = LoggerFactory.getLogger(NoOpExternalOperationClient.class);

    @Override
    public void execute(String idempotencyKey, Event event) {
        log.debug("No-op external operation eventId={} idempotencyKey={}", event.getEventId(), idempotencyKey);
    }
}
