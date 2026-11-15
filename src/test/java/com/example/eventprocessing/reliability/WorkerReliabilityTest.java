package com.example.eventprocessing.reliability;

import com.example.eventprocessing.application.EventProcessingService;
import com.example.eventprocessing.domain.DlqStatus;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.domain.OutboxStatus;
import com.example.eventprocessing.integration.AbstractMongoIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Spec section 14: Worker Concurrency, Crash Recovery (expired-lease takeover in the claim), Fencing, Retry Backoff, DLQ. */
class WorkerReliabilityTest extends AbstractMongoIntegrationTest {

    private static final Duration LEASE = Duration.ofSeconds(30);

    @Autowired
    private EventProcessingService processingService;

    @Test
    void twoWorkersReceiveSameEventOnlyOneGetsValidLease() throws Exception {
        ingest("evt-w-1");
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Optional<Event>>> futures = new ArrayList<>();
        for (String id : List.of("worker-A", "worker-B")) {
            futures.add(pool.submit(() -> {
                start.await();
                return eventRepository.claimWorkerEvent("evt-w-1", id, LEASE);
            }));
        }
        start.countDown();
        long claims = 0;
        for (Future<Optional<Event>> f : futures) {
            if (f.get(10, TimeUnit.SECONDS).isPresent()) claims++;
        }
        pool.shutdown();
        assertEquals(1, claims);
        assertEquals(EventStatus.PROCESSING, load("evt-w-1").getStatus());
    }

    @Test
    void activeLeaseCannotBeStolenByAnotherWorker() {
        ingest("evt-w-2");
        eventRepository.claimWorkerEvent("evt-w-2", "worker-A", LEASE).orElseThrow();

        assertTrue(eventRepository.claimWorkerEvent("evt-w-2", "worker-B", LEASE).isEmpty());
        assertEquals(1, load("evt-w-2").getLeaseVersion());
    }

    @Test
    void expiredLeaseIsTakenOverByTheClaimItselfWithHigherFencingToken() {
        ingest("evt-w-3");
        Event first = eventRepository.claimWorkerEvent("evt-w-3", "worker-A", LEASE).orElseThrow(); // ...then worker-A crashes
        expireWorkerLease("evt-w-3");

        Event second = eventRepository.claimWorkerEvent("evt-w-3", "worker-B", LEASE).orElseThrow();

        assertEquals(EventStatus.PROCESSING, second.getStatus());
        assertEquals("worker-B", second.getWorkerId());
        assertEquals(first.getLeaseVersion() + 1, second.getLeaseVersion());
        assertEquals(0, second.getRetryCount(), "a takeover is not a business retry");
        assertTrue(second.getProcessingLeaseUntil().isAfter(Instant.now()));
    }

    @Test
    void redeliveredMessageAfterWorkerCrashIsProcessedOnceTheLeaseExpired() {
        ingest("evt-w-4");
        eventRepository.claimWorkerEvent("evt-w-4", "crashed-worker", LEASE).orElseThrow(); // crash: never ACKed
        expireWorkerLease("evt-w-4");

        processingService.handleMessage("evt-w-4", "corr"); // SQS redelivery after the visibility timeout

        Event done = load("evt-w-4");
        assertEquals(EventStatus.PROCESSED, done.getStatus());
        assertEquals(2, done.getLeaseVersion());
    }

    @Test
    void redeliveredMessageWhileLeaseIsStillValidIsAckedAsDuplicate() {
        ingest("evt-w-5");
        eventRepository.claimWorkerEvent("evt-w-5", "worker-A", LEASE).orElseThrow();

        assertDoesNotThrow(() -> processingService.handleMessage("evt-w-5", "corr"));

        Event unchanged = load("evt-w-5");
        assertEquals(EventStatus.PROCESSING, unchanged.getStatus());
        assertEquals("worker-A", unchanged.getWorkerId());
    }

    @Test
    void staleWorkerIsFencedOutAfterLeaseExpiredOrReplaced() {
        ingest("evt-w-6");
        Event first = eventRepository.claimWorkerEvent("evt-w-6", "worker-A", LEASE).orElseThrow();
        expireWorkerLease("evt-w-6");

        // lease expired and nobody took over yet: the old worker still must not write
        assertFalse(eventRepository.markProcessed("evt-w-6", "worker-A", first.getLeaseVersion()));

        // a new worker takes over with a higher fencing token
        Event second = eventRepository.claimWorkerEvent("evt-w-6", "worker-B", LEASE).orElseThrow();
        assertTrue(second.getLeaseVersion() > first.getLeaseVersion());

        assertFalse(eventRepository.markProcessed("evt-w-6", "worker-A", first.getLeaseVersion()));
        assertFalse(eventRepository.scheduleRetry("evt-w-6", "worker-A", first.getLeaseVersion(), 1, Instant.now(), "x"));
        assertFalse(eventRepository.markFailed("evt-w-6", "worker-A", first.getLeaseVersion(), "x"));
        assertTrue(eventRepository.markProcessed("evt-w-6", "worker-B", second.getLeaseVersion()));
    }

    @Test
    void retryingEventIsNotClaimedBeforeNextAttemptAndRetryCountOnlyGrowsOnScheduling() {
        ingest("evt-w-7");
        assertEquals(0, load("evt-w-7").getRetryCount());
        Event claimed = eventRepository.claimWorkerEvent("evt-w-7", "worker-A", LEASE).orElseThrow();
        assertEquals(0, claimed.getRetryCount(), "claiming an attempt must not increase retryCount");

        Instant next = Instant.now().plusSeconds(120);
        assertTrue(eventRepository.scheduleRetry("evt-w-7", "worker-A", claimed.getLeaseVersion(), 1, next, "boom"));
        Event retrying = load("evt-w-7");
        assertEquals(EventStatus.RETRYING, retrying.getStatus());
        assertEquals(1, retrying.getRetryCount());
        assertEquals(OutboxStatus.PENDING, retrying.getOutboxStatus());

        assertTrue(eventRepository.claimWorkerEvent("evt-w-7", "worker-B", LEASE).isEmpty());

        setFields("evt-w-7", Map.of("nextProcessingAttemptAt", Instant.now().minusSeconds(1)));
        assertTrue(eventRepository.claimWorkerEvent("evt-w-7", "worker-B", LEASE).isPresent());
    }

    @Test
    void duplicateMessagesAreHarmlessAfterProcessing() {
        ingest("evt-w-8");
        processingService.handleMessage("evt-w-8", "corr");
        Event processed = load("evt-w-8");
        assertEquals(EventStatus.PROCESSED, processed.getStatus());
        assertNotNull(processed.getProcessedAt());

        // duplicate SQS delivery (outbox crash window): no exception (=> ACK) and no second processing
        assertDoesNotThrow(() -> processingService.handleMessage("evt-w-8", "corr"));
        assertEquals(processed.getProcessedAt(), load("evt-w-8").getProcessedAt());
        assertEquals(1, load("evt-w-8").getLeaseVersion());
    }

    @Test
    void transientFailureSchedulesRetryThenSucceeds() {
        ingest("evt-w-9", Map.of("simulateFailure", "transient:1"));

        processingService.handleMessage("evt-w-9", "corr");
        Event retrying = load("evt-w-9");
        assertEquals(EventStatus.RETRYING, retrying.getStatus());
        assertEquals(1, retrying.getRetryCount());
        assertEquals(OutboxStatus.PENDING, retrying.getOutboxStatus());
        assertTrue(retrying.getNextProcessingAttemptAt().isAfter(Instant.now()));

        // early (duplicate) message: safe to ACK, state unchanged
        assertDoesNotThrow(() -> processingService.handleMessage("evt-w-9", "corr"));
        assertEquals(EventStatus.RETRYING, load("evt-w-9").getStatus());

        setFields("evt-w-9", Map.of("nextProcessingAttemptAt", Instant.now().minusSeconds(1)));
        processingService.handleMessage("evt-w-9", "corr");
        Event done = load("evt-w-9");
        assertEquals(EventStatus.PROCESSED, done.getStatus());
        assertEquals(1, done.getRetryCount());
        assertNull(done.getErrorMessage());
    }

    @Test
    void exhaustedRetriesEndInFailedAndDlqPublishedAfterFourAttempts() {
        ingest("evt-w-10", Map.of("simulateFailure", "transient"));
        for (int attempt = 1; attempt <= 4; attempt++) {
            setFields("evt-w-10", Map.of("nextProcessingAttemptAt", Instant.now().minusSeconds(1)));
            processingService.handleMessage("evt-w-10", "corr");
        }
        Event failed = load("evt-w-10");
        assertEquals(EventStatus.FAILED, failed.getStatus());
        assertEquals(3, failed.getRetryCount());
        assertEquals(DlqStatus.PUBLISHED, failed.getDlqStatus());
        assertNotNull(failed.getErrorMessage());
        verify(sqsPublisher, times(1)).publishDlqMessage(eq("evt-w-10"), any(), anyString(), eq(3));
    }

    @Test
    void invalidBusinessDataFailsImmediatelyWithoutRetryAndIsSentToDlq() {
        ingest("evt-w-11", Map.of("simulateFailure", "permanent"));
        processingService.handleMessage("evt-w-11", "corr");
        Event failed = load("evt-w-11");
        assertEquals(EventStatus.FAILED, failed.getStatus());
        assertEquals(0, failed.getRetryCount());
        assertEquals(DlqStatus.PUBLISHED, failed.getDlqStatus());
        assertNotNull(failed.getDlqPublishedAt());
        verify(sqsPublisher, times(1)).publishDlqMessage(eq("evt-w-11"), any(), anyString(), eq(0));
    }

    @Test
    void dlqPublishFailureIsNotAckedAndTheRedeliveredMessagePublishesIt() {
        ingest("evt-w-12", Map.of("simulateFailure", "permanent"));
        doThrow(new IllegalStateException("SQS down")).when(sqsPublisher)
                .publishDlqMessage(anyString(), any(), any(), anyInt());

        assertThrows(IllegalStateException.class, () -> processingService.handleMessage("evt-w-12", "corr"),
                "exception => the SQS message is NOT acknowledged");
        Event failed = load("evt-w-12");
        assertEquals(EventStatus.FAILED, failed.getStatus());
        assertEquals(DlqStatus.PENDING, failed.getDlqStatus());

        // SQS redelivers the message; SQS is healthy again.
        reset(sqsPublisher);
        assertDoesNotThrow(() -> processingService.handleMessage("evt-w-12", "corr"));

        assertEquals(DlqStatus.PUBLISHED, load("evt-w-12").getDlqStatus());
        verify(sqsPublisher, times(1)).publishDlqMessage(eq("evt-w-12"), any(), any(), anyInt());
    }

    @Test
    void duplicateMessageForAlreadyPublishedFailedEventDoesNotSendAnotherDlqMessage() {
        ingest("evt-w-13", Map.of("simulateFailure", "permanent"));
        processingService.handleMessage("evt-w-13", "corr");

        assertDoesNotThrow(() -> processingService.handleMessage("evt-w-13", "corr"));

        verify(sqsPublisher, times(1)).publishDlqMessage(eq("evt-w-13"), any(), any(), anyInt());
        verify(sqsPublisher, never()).publishEventId(anyString(), any());
    }
}
