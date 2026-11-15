package com.example.eventprocessing.integration;

import com.example.eventprocessing.application.EventIngestionService.IngestionStatus;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.domain.OutboxStatus;
import org.junit.jupiter.api.Test;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

class IngestionIntegrationTest extends AbstractMongoIntegrationTest {

    @Test
    void validIngestionStoresReceivedAndOutboxPendingInOneDocument() {
        var result = ingestionService.ingestEvent(request("evt-1", "cust-1", "1250.50", Map.of("source", "mobile")), "corr-1");

        assertEquals(IngestionStatus.ACCEPTED, result.getStatus());
        Event stored = load("evt-1");
        assertEquals(EventStatus.RECEIVED, stored.getStatus());
        assertEquals(OutboxStatus.PENDING, stored.getOutboxStatus());
        assertEquals(0, stored.getRetryCount());
        assertEquals(new java.math.BigDecimal("1250.50"), stored.getAmount());
        assertTrue(stored.getPayloadHash().startsWith("sha256:"));
        assertEquals("corr-1", stored.getCorrelationId());
    }

    @Test
    void sameEventIdSamePayloadReturnsOkAndDoesNotCreateSecondDocument() {
        assertEquals(IngestionStatus.ACCEPTED,
                ingestionService.ingestEvent(request("evt-2", "c", "10", Map.of("a", "b")), "c1").getStatus());
        assertEquals(IngestionStatus.OK,
                ingestionService.ingestEvent(request("evt-2", "c", "10.00", Map.of("a", "b")), "c2").getStatus());
        assertEquals(1, eventRepository.count());
    }

    @Test
    void sameEventIdDifferentPayloadReturnsConflict() {
        ingestionService.ingestEvent(request("evt-3", "c", "10", null), "c1");
        assertEquals(IngestionStatus.CONFLICT,
                ingestionService.ingestEvent(request("evt-3", "c", "11", null), "c2").getStatus());
        assertEquals(1, eventRepository.count());
    }

    @Test
    void idempotencyIsUniformRegardlessOfEventStatus() {
        ingest("evt-4");
        setFields("evt-4", Map.of("status", EventStatus.PROCESSED));
        assertEquals(IngestionStatus.OK,
                ingestionService.ingestEvent(request("evt-4", "cust-1", "100.00", Map.of("source", "test")), "x").getStatus());
        assertEquals(EventStatus.PROCESSED, load("evt-4").getStatus());
    }

    @Test
    void concurrentIdenticalRequestsCreateExactlyOneDocument() throws Exception {
        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<IngestionStatus>> futures = new ArrayList<>();
        for (int i = 0; i < threads; i++) {
            futures.add(pool.submit(() -> {
                start.await();
                return ingestionService.ingestEvent(request("evt-race", "c", "5", Map.of("k", "v")), "corr").getStatus();
            }));
        }
        start.countDown();
        int accepted = 0;
        int ok = 0;
        for (Future<IngestionStatus> f : futures) {
            IngestionStatus s = f.get(30, TimeUnit.SECONDS);
            if (s == IngestionStatus.ACCEPTED) accepted++;
            if (s == IngestionStatus.OK) ok++;
        }
        pool.shutdown();
        assertEquals(1, accepted);
        assertEquals(threads - 1, ok);
        assertEquals(1, eventRepository.count());
    }

    @Test
    void historyFiltersSortsAndPaginatesDeterministically() {
        Instant base = Instant.now().minus(Duration.ofHours(1));
        for (int i = 0; i < 5; i++) {
            ingest("evt-h" + i);
            // identical createdAt for two events to prove the _id tie-breaker gives a stable order
            setFields("evt-h" + i, Map.of("createdAt", i < 2 ? base : base.plusSeconds(i * 10L)));
        }
        setFields("evt-h4", Map.of("status", EventStatus.PROCESSED));

        var sort = Sort.by(Sort.Order.desc("createdAt"), Sort.Order.desc("_id"));
        var page0 = eventRepository.findCustomerEvents("cust-1", null, null, null, PageRequest.of(0, 2, sort));
        var page1 = eventRepository.findCustomerEvents("cust-1", null, null, null, PageRequest.of(1, 2, sort));
        assertEquals(5, page0.getTotalElements());
        assertEquals("evt-h4", page0.getContent().get(0).getEventId());
        assertEquals(2, page1.getContent().size());

        var processed = eventRepository.findCustomerEvents("cust-1", null, null, EventStatus.PROCESSED, PageRequest.of(0, 10, sort));
        assertEquals(1, processed.getTotalElements());

        // inclusive range boundaries
        var exact = eventRepository.findCustomerEvents("cust-1", base, base, null, PageRequest.of(0, 10, sort));
        assertEquals(2, exact.getTotalElements());

        var tiePage = eventRepository.findCustomerEvents("cust-1", base, base, null, PageRequest.of(0, 10, sort));
        assertTrue(tiePage.getContent().get(0).getId().compareTo(tiePage.getContent().get(1).getId()) > 0);

        assertEquals(0, eventRepository.findCustomerEvents("other", null, null, null, PageRequest.of(0, 10, sort)).getTotalElements());
    }
}
