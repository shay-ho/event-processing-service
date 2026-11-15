package com.example.eventprocessing.reliability;

import com.example.eventprocessing.application.EventProcessingService;
import com.example.eventprocessing.application.ExternalOperationClient;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.integration.AbstractMongoIntegrationTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Spec section 14 - External Idempotency. A fake external service that behaves like a real idempotent API
 * (same Idempotency-Key => the side effect happens once) is wired in place of the default no-op client.
 * Scenario: the external call SUCCEEDS but the worker then fails (response lost) -> the event is retried and the
 * external service is called AGAIN. The test proves the retry reuses the SAME key (the eventId), so the external
 * side effect is performed exactly once.
 * <p>
 * Limit (spec): this protection exists only because the external service honours the key; MongoDB + SQS alone
 * cannot prevent a duplicate external action.
 */
@Import(ExternalIdempotencyTest.FakeExternalConfig.class)
class ExternalIdempotencyTest extends AbstractMongoIntegrationTest {

    static class IdempotentExternalService implements ExternalOperationClient {
        final List<String> receivedKeys = Collections.synchronizedList(new ArrayList<>());
        final Set<String> performedSideEffects = Collections.synchronizedSet(new HashSet<>());

        @Override
        public void execute(String idempotencyKey, Event event) {
            receivedKeys.add(idempotencyKey);
            performedSideEffects.add(idempotencyKey); // second call with the same key is a no-op (set semantics)
        }

        void reset() {
            receivedKeys.clear();
            performedSideEffects.clear();
        }
    }

    @TestConfiguration
    static class FakeExternalConfig {
        @Bean
        @Primary
        IdempotentExternalService idempotentExternalService() {
            return new IdempotentExternalService();
        }
    }

    @Autowired
    private IdempotentExternalService external;
    @Autowired
    private EventProcessingService processingService;

    @BeforeEach
    void resetExternal() {
        external.reset();
    }

    @Test
    void retryAfterLostResponseCallsExternalServiceAgainWithTheSameKeySoSideEffectHappensOnce() {
        ingest("evt-ext-1", Map.of("simulateFailure", "transient:1"));

        processingService.handleMessage("evt-ext-1", "corr");
        assertEquals(EventStatus.RETRYING, load("evt-ext-1").getStatus());
        assertEquals(1, external.receivedKeys.size(), "external call happened, then the response was 'lost'");

        setFields("evt-ext-1", Map.of("nextProcessingAttemptAt", Instant.now().minusSeconds(1)));
        processingService.handleMessage("evt-ext-1", "corr");

        assertEquals(EventStatus.PROCESSED, load("evt-ext-1").getStatus());
        assertEquals(2, external.receivedKeys.size(), "the retry calls the external service again");
        assertEquals(List.of("evt-ext-1", "evt-ext-1"), external.receivedKeys, "key is the constant eventId on every attempt");
        assertEquals(1, external.performedSideEffects.size(), "idempotent external service performs the action once");
    }

    @Test
    void duplicateSqsMessageAfterSuccessDoesNotCallExternalServiceAgain() {
        ingest("evt-ext-2", Map.of("source", "it"));

        processingService.handleMessage("evt-ext-2", "corr");
        processingService.handleMessage("evt-ext-2", "corr"); // duplicate delivery

        assertEquals(EventStatus.PROCESSED, load("evt-ext-2").getStatus());
        assertEquals(List.of("evt-ext-2"), external.receivedKeys);
    }

    @Test
    void invalidBusinessDataFailsBeforeAnyExternalCall() {
        ingest("evt-ext-3", Map.of("simulateFailure", "permanent"));

        processingService.handleMessage("evt-ext-3", "corr");

        assertEquals(EventStatus.FAILED, load("evt-ext-3").getStatus());
        assertTrue(external.receivedKeys.isEmpty());
    }

    @Test
    void differentEventsUseDifferentKeys() {
        ingest("evt-ext-4a", Map.of("source", "it"));
        ingest("evt-ext-4b", Map.of("source", "it"));

        processingService.handleMessage("evt-ext-4a", "corr");
        processingService.handleMessage("evt-ext-4b", "corr");

        assertEquals(Set.of("evt-ext-4a", "evt-ext-4b"), external.performedSideEffects);
    }
}
