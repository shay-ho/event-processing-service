package com.example.eventprocessing.application;

import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.infrastructure.mongo.EventRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Micrometer metrics required by spec section 12. Status gauges are refreshed periodically (not at scrape time). */
@Component
public class EventMetrics {

    private static final Logger log = LoggerFactory.getLogger(EventMetrics.class);

    private final EventRepository eventRepository;
    private final Counter ingested;
    private final Counter duplicates;
    private final Counter conflicts;
    private final Counter retries;
    private final Counter outboxFailures;
    private final Counter dlqFailures;
    private final Counter dlqPublished;
    private final Counter leaseLost;
    private final Timer processingTimer;
    private final Map<EventStatus, AtomicLong> statusCounts = new EnumMap<>(EventStatus.class);

    public EventMetrics(MeterRegistry registry, EventRepository eventRepository) {
        this.eventRepository = eventRepository;
        this.ingested = registry.counter("events.ingested");
        this.duplicates = registry.counter("events.ingest.duplicates");
        this.conflicts = registry.counter("events.ingest.conflicts");
        this.retries = registry.counter("events.retries");
        this.outboxFailures = registry.counter("events.outbox.failures");
        this.dlqFailures = registry.counter("events.dlq.failures");
        this.dlqPublished = registry.counter("events.dlq.published");
        this.leaseLost = registry.counter("events.worker.lease.lost");
        this.processingTimer = registry.timer("events.processing.time");
        for (EventStatus status : EventStatus.values()) {
            AtomicLong holder = new AtomicLong();
            statusCounts.put(status, holder);
            Gauge.builder("events.by.status", holder, AtomicLong::get).tag("status", status.name()).register(registry);
        }
    }

    public void ingested() { ingested.increment(); }
    public void duplicate() { duplicates.increment(); }
    public void conflict() { conflicts.increment(); }
    public void retryScheduled() { retries.increment(); }
    public void outboxFailure() { outboxFailures.increment(); }
    public void dlqFailure() { dlqFailures.increment(); }
    public void dlqPublished() { dlqPublished.increment(); }
    public void leaseLost() { leaseLost.increment(); }
    public Timer processingTimer() { return processingTimer; }

    @Scheduled(fixedDelayString = "${events.metrics.refresh-ms:30000}")
    public void refreshStatusGauges() {
        try {
            for (EventStatus status : EventStatus.values()) {
                statusCounts.get(status).set(eventRepository.countByStatus(status));
            }
        } catch (Exception e) {
            log.warn("Failed to refresh status gauges: {}", e.getMessage());
        }
    }
}
