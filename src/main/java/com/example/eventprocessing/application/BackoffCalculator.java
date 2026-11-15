package com.example.eventprocessing.application;

import com.example.eventprocessing.config.EventsProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Exponential backoff with jitter and a hard ceiling (spec section 9).
 * Business retry: 5s, 15s, 45s (base * multiplier^(n-1)) + jitter, capped.
 * Publish retry (Outbox): base * 2^(attempt-1) + jitter, capped.
 */
@Component
public class BackoffCalculator {

    private final EventsProperties props;

    public BackoffCalculator(EventsProperties props) {
        this.props = props;
    }

    /** @param retryCount the retryCount that is about to be stored (1 = first retry). */
    public Duration businessRetryBackoff(int retryCount) {
        EventsProperties.Processing p = props.getProcessing();
        double raw = p.getRetryBaseSeconds() * Math.pow(p.getRetryMultiplier(), Math.max(0, retryCount - 1));
        return withJitterAndCap(raw, p.getJitterRatio(), p.getBackoffCapSeconds());
    }

    public Duration outboxBackoff(int attempts) {
        EventsProperties.Outbox o = props.getOutbox();
        double raw = o.getBaseSeconds() * Math.pow(2, Math.min(Math.max(0, attempts - 1), 20));
        return withJitterAndCap(raw, props.getProcessing().getJitterRatio(), o.getBackoffCapSeconds());
    }

    private static Duration withJitterAndCap(double rawSeconds, double jitterRatio, long capSeconds) {
        double jitter = jitterRatio > 0 ? ThreadLocalRandom.current().nextDouble(0, rawSeconds * jitterRatio) : 0;
        long millis = (long) ((rawSeconds + jitter) * 1000);
        return Duration.ofMillis(Math.min(millis, capSeconds * 1000));
    }
}
