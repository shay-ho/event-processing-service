package com.example.eventprocessing.unit;

import com.example.eventprocessing.application.BackoffCalculator;
import com.example.eventprocessing.application.EventMetrics;
import com.example.eventprocessing.config.EventsProperties;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.infrastructure.mongo.EventRepository;
import com.example.eventprocessing.infrastructure.scheduler.OutboxPublisherScheduler;
import com.example.eventprocessing.infrastructure.sqs.SqsPublisher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/** An Outbox run must never claim more events than it will publish (otherwise the extra event is stuck in PUBLISHING). */
class SchedulerBatchTest {

    private EventRepository repo;
    private SqsPublisher sqs;
    private BackoffCalculator backoff;
    private EventMetrics metrics;
    private EventsProperties props;

    @BeforeEach
    void setUp() {
        repo = mock(EventRepository.class);
        sqs = mock(SqsPublisher.class);
        backoff = mock(BackoffCalculator.class);
        metrics = mock(EventMetrics.class);
        props = new EventsProperties();
        props.getOutbox().setBatchSize(3);
    }

    private static Event event(String id) {
        Event e = new Event();
        e.setEventId(id);
        e.setCorrelationId("corr-" + id);
        return e;
    }

    @Test
    void outboxClaimsExactlyBatchSizeWhenMoreEventsAreAvailable() {
        when(repo.claimOutboxEvent(anyString(), any(Duration.class)))
                .thenAnswer(inv -> Optional.of(event("e")));
        when(repo.markOutboxPublished(anyString(), anyString(), anyLong())).thenReturn(true);

        new OutboxPublisherScheduler(repo, sqs, backoff, metrics, props).publishOutboxEvents();

        verify(repo, times(3)).claimOutboxEvent(anyString(), any(Duration.class));
        verify(sqs, times(3)).publishEventId(anyString(), any());
    }

    @Test
    void outboxStopsAtFirstEmptyClaim() {
        when(repo.claimOutboxEvent(anyString(), any(Duration.class)))
                .thenReturn(Optional.of(event("a")), Optional.empty());
        when(repo.markOutboxPublished(anyString(), anyString(), anyLong())).thenReturn(true);

        new OutboxPublisherScheduler(repo, sqs, backoff, metrics, props).publishOutboxEvents();

        verify(repo, times(2)).claimOutboxEvent(anyString(), any(Duration.class));
        verify(sqs, times(1)).publishEventId(anyString(), any());
    }
}
