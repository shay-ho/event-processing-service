package com.example.eventprocessing.application;

import com.example.eventprocessing.config.EventsProperties;
import com.example.eventprocessing.domain.DlqStatus;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.infrastructure.mongo.EventRepository;
import com.example.eventprocessing.infrastructure.sqs.SqsPublisher;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/** Docker-free unit tests of the worker orchestration: fenced writes, lease loss, retry, DLQ hand-off. */
class EventProcessingServiceTest {

    private EventRepository repo;
    private SqsPublisher sqsPublisher;
    private BackoffCalculator backoff;
    private SimpleMeterRegistry registry;
    private EventsProperties props;

    @BeforeEach
    void setUp() {
        repo = mock(EventRepository.class);
        sqsPublisher = mock(SqsPublisher.class);
        backoff = mock(BackoffCalculator.class);
        registry = new SimpleMeterRegistry();
        props = new EventsProperties();
        when(backoff.businessRetryBackoff(anyInt())).thenReturn(Duration.ofSeconds(5));
    }

    private EventProcessingService service(EventBusinessHandler handler) {
        return new EventProcessingService(repo, handler, backoff, new EventMetrics(registry, repo), props, sqsPublisher);
    }

    private Event claimable(String id, int retryCount) {
        Event e = new Event();
        e.setEventId(id);
        e.setLeaseVersion(1);
        e.setRetryCount(retryCount);
        e.setCorrelationId("corr-" + id);
        when(repo.claimWorkerEvent(eq(id), anyString(), any(Duration.class))).thenReturn(Optional.of(e));
        return e;
    }

    @Test
    void successfulProcessingMarksProcessedWithTheClaimedFencingToken() {
        claimable("e1", 0);
        when(repo.markProcessed(eq("e1"), anyString(), eq(1L))).thenReturn(true);

        assertDoesNotThrow(() -> service(event -> { }).handleMessage("e1", "corr"));

        verify(repo).markProcessed(eq("e1"), anyString(), eq(1L));
        verifyNoInteractions(sqsPublisher);
    }

    @Test
    void lostLeaseOnResultWriteThrowsSoTheMessageIsNotAckedAndIsCounted() {
        claimable("e2", 0);
        when(repo.markProcessed(anyString(), anyString(), anyLong())).thenReturn(false);

        assertThrows(IllegalStateException.class, () -> service(event -> { }).handleMessage("e2", "corr"));

        assertEquals(1.0, registry.get("events.worker.lease.lost").counter().count());
    }

    @Test
    void retryableFailureSchedulesRetryWithIncrementedCounter() {
        claimable("e3", 1);
        when(repo.scheduleRetry(eq("e3"), anyString(), eq(1L), eq(2), any(Instant.class), anyString())).thenReturn(true);

        service(event -> { throw new BusinessExceptions.RetryableBusinessException("temporary"); })
                .handleMessage("e3", "corr");

        verify(repo).scheduleRetry(eq("e3"), anyString(), eq(1L), eq(2), any(Instant.class), eq("temporary"));
        verify(repo, never()).markFailed(anyString(), anyString(), anyLong(), anyString());
        verifyNoInteractions(sqsPublisher);
        assertEquals(1.0, registry.get("events.retries").counter().count());
    }

    @Test
    void retriesExhaustedMarksFailedAndPublishesDlqMessage() {
        claimable("e4", 3); // maxRetries = 3 => the 4th failure is final
        when(repo.markFailed(eq("e4"), anyString(), eq(1L), anyString())).thenReturn(true);
        when(repo.markDlqPublished("e4")).thenReturn(true);

        service(event -> { throw new BusinessExceptions.RetryableBusinessException("still down"); })
                .handleMessage("e4", "corr");

        verify(repo, never()).scheduleRetry(anyString(), anyString(), anyLong(), anyInt(), any(), anyString());
        verify(sqsPublisher).publishDlqMessage(eq("e4"), eq("corr-e4"), contains("Max retries exceeded"), eq(3));
        verify(repo).markDlqPublished("e4");
        assertEquals(1.0, registry.get("events.dlq.published").counter().count());
    }

    @Test
    void nonRetryableFailureGoesStraightToFailedAndDlqWithoutRetry() {
        claimable("e5", 0);
        when(repo.markFailed(eq("e5"), anyString(), eq(1L), eq("bad data"))).thenReturn(true);
        when(repo.markDlqPublished("e5")).thenReturn(true);

        service(event -> { throw new BusinessExceptions.NonRetryableBusinessException("bad data"); })
                .handleMessage("e5", "corr");

        verify(repo, never()).scheduleRetry(anyString(), anyString(), anyLong(), anyInt(), any(), anyString());
        verify(sqsPublisher).publishDlqMessage("e5", "corr-e5", "bad data", 0);
    }

    @Test
    void dlqPublishFailureThrowsAndLeavesDlqStatusPending() {
        claimable("e6", 0);
        when(repo.markFailed(anyString(), anyString(), anyLong(), anyString())).thenReturn(true);
        doThrow(new IllegalStateException("SQS down")).when(sqsPublisher)
                .publishDlqMessage(anyString(), any(), any(), anyInt());

        assertThrows(IllegalStateException.class, () -> service(event -> {
            throw new BusinessExceptions.NonRetryableBusinessException("bad data");
        }).handleMessage("e6", "corr"));

        verify(repo, never()).markDlqPublished(anyString());
        assertEquals(1.0, registry.get("events.dlq.failures").counter().count());
    }

    @Test
    void redeliveredMessageOfFailedEventWithPendingDlqPublishesIt() {
        when(repo.claimWorkerEvent(eq("e7"), anyString(), any(Duration.class))).thenReturn(Optional.empty());
        Event failed = new Event();
        failed.setEventId("e7");
        failed.setStatus(EventStatus.FAILED);
        failed.setDlqStatus(DlqStatus.PENDING);
        failed.setErrorMessage("bad data");
        failed.setRetryCount(2);
        failed.setCorrelationId("corr-e7");
        when(repo.findByEventId("e7")).thenReturn(Optional.of(failed));
        when(repo.markDlqPublished("e7")).thenReturn(true);

        assertDoesNotThrow(() -> service(event -> { }).handleMessage("e7", "corr"));

        verify(sqsPublisher).publishDlqMessage("e7", "corr-e7", "bad data", 2);
        verify(repo).markDlqPublished("e7");
    }

    @Test
    void redeliveredMessageOfFailedEventWithPublishedDlqIsJustAcked() {
        when(repo.claimWorkerEvent(eq("e8"), anyString(), any(Duration.class))).thenReturn(Optional.empty());
        Event failed = new Event();
        failed.setEventId("e8");
        failed.setStatus(EventStatus.FAILED);
        failed.setDlqStatus(DlqStatus.PUBLISHED);
        when(repo.findByEventId("e8")).thenReturn(Optional.of(failed));

        assertDoesNotThrow(() -> service(event -> { }).handleMessage("e8", "corr"));

        verifyNoInteractions(sqsPublisher);
    }

    @Test
    void messageForEventWithActiveLeaseIsAckedAsDuplicate() {
        when(repo.claimWorkerEvent(eq("e9"), anyString(), any(Duration.class))).thenReturn(Optional.empty());
        Event processing = new Event();
        processing.setEventId("e9");
        processing.setStatus(EventStatus.PROCESSING);
        processing.setProcessingLeaseUntil(Instant.now().plusSeconds(20));
        when(repo.findByEventId("e9")).thenReturn(Optional.of(processing));

        assertDoesNotThrow(() -> service(event -> { }).handleMessage("e9", "corr"));
    }

    @Test
    void unclaimableEventInAnyOtherStateIsNotAcked() {
        when(repo.claimWorkerEvent(eq("e10"), anyString(), any(Duration.class))).thenReturn(Optional.empty());
        Event odd = new Event();
        odd.setEventId("e10");
        odd.setStatus(EventStatus.RETRYING);
        odd.setNextProcessingAttemptAt(Instant.now().minusSeconds(1)); // due, yet the claim failed (e.g. a race)
        when(repo.findByEventId("e10")).thenReturn(Optional.of(odd));

        assertThrows(IllegalStateException.class, () -> service(event -> { }).handleMessage("e10", "corr"));
    }
}
