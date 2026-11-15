package com.example.eventprocessing.unit;

import com.example.eventprocessing.application.BackoffCalculator;
import com.example.eventprocessing.config.EventsProperties;
import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.*;

class BackoffCalculatorTest {

    @Test
    void businessRetryBackoffFollowsSpecWithJitterAndCap() {
        EventsProperties props = new EventsProperties();
        BackoffCalculator calc = new BackoffCalculator(props);

        // base 5s, x3 per retry (5s, 15s, 45s) + up to 20% jitter
        assertBetween(calc.businessRetryBackoff(1), 5, 6);
        assertBetween(calc.businessRetryBackoff(2), 15, 18);
        assertBetween(calc.businessRetryBackoff(3), 45, 54);
        // hard ceiling
        assertTrue(calc.businessRetryBackoff(30).toSeconds() <= props.getProcessing().getBackoffCapSeconds());
    }

    @Test
    void outboxBackoffGrowsAndIsCapped() {
        EventsProperties props = new EventsProperties();
        BackoffCalculator calc = new BackoffCalculator(props);
        assertBetween(calc.outboxBackoff(1), 5, 6);
        assertBetween(calc.outboxBackoff(2), 10, 12);
        assertTrue(calc.outboxBackoff(50).toSeconds() <= props.getOutbox().getBackoffCapSeconds());
    }

    private static void assertBetween(Duration actual, double minSeconds, double maxSeconds) {
        double s = actual.toMillis() / 1000.0;
        assertTrue(s >= minSeconds && s <= maxSeconds, "expected " + minSeconds + ".." + maxSeconds + " but was " + s);
    }
}
