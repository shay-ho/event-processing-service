package com.example.eventprocessing.application;

import com.example.eventprocessing.config.EventsProperties;
import com.example.eventprocessing.domain.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

/**
 * Default placeholder business logic (the spec does not define a concrete business operation).
 * Optional fault-injection, only when events.processing.failure-simulation-enabled=true:
 * metadata.simulateFailure = "transient" | "permanent" | "transient:N" (fail while retryCount &lt; N).
 */
@Component
public class SimulatedBusinessHandler implements EventBusinessHandler {

    private static final Logger log = LoggerFactory.getLogger(SimulatedBusinessHandler.class);

    private final EventsProperties props;
    private final ExternalOperationClient externalClient;

    public SimulatedBusinessHandler(EventsProperties props, ExternalOperationClient externalClient) {
        this.props = props;
        this.externalClient = externalClient;
    }

    @Override
    public void handle(Event event) {
        log.info("Executing business logic eventId={} type={}", event.getEventId(), event.getType());

        Object flag = props.getProcessing().isFailureSimulationEnabled() && event.getMetadata() != null
                ? event.getMetadata().get("simulateFailure") : null;
        String mode = flag instanceof String m ? m : null;

        // Invalid business data is detected BEFORE any external side effect.
        if ("permanent".equals(mode)) {
            throw new BusinessExceptions.NonRetryableBusinessException("Simulated invalid business data");
        }

        // The external call always uses the constant eventId as Idempotency-Key (spec section 11).
        externalClient.execute(event.getEventId(), event);

        // Transient failures are simulated AFTER the external call succeeded ("response lost"), which is exactly
        // the situation the Idempotency-Key protects against: the retry repeats the call with the same key.
        if ("transient".equals(mode)) {
            throw new BusinessExceptions.RetryableBusinessException("Simulated transient failure");
        }
        if (mode != null && mode.startsWith("transient:")) {
            int failUntil = Integer.parseInt(mode.substring("transient:".length()));
            if (event.getRetryCount() < failUntil) {
                throw new BusinessExceptions.RetryableBusinessException(
                        "Simulated transient failure (retryCount=" + event.getRetryCount() + ")");
            }
        }
    }
}
