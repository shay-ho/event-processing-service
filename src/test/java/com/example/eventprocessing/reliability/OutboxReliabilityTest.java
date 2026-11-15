package com.example.eventprocessing.reliability;

import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.OutboxStatus;
import com.example.eventprocessing.integration.AbstractMongoIntegrationTest;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/** Spec section 14: Outbox Publisher Concurrency and Crash Window. */
class OutboxReliabilityTest extends AbstractMongoIntegrationTest {

    @Test
    void twoPublishersCompeteOnlyOneClaimIsValid() throws Exception {
        ingest("evt-ob-1");
        ExecutorService pool = Executors.newFixedThreadPool(2);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Optional<Event>>> futures = new ArrayList<>();
        for (String id : List.of("pub-A", "pub-B")) {
            futures.add(pool.submit(() -> {
                start.await();
                return eventRepository.claimOutboxEvent(id, Duration.ofSeconds(30));
            }));
        }
        start.countDown();
        long claims = 0;
        for (Future<Optional<Event>> f : futures) {
            if (f.get(10, TimeUnit.SECONDS).isPresent()) claims++;
        }
        pool.shutdown();
        assertEquals(1, claims);
        assertEquals(OutboxStatus.PUBLISHING, load("evt-ob-1").getOutboxStatus());
    }

    @Test
    void stalePublisherCannotUpdateAfterLeaseWasTakenOver() {
        ingest("evt-ob-2");
        Event claimA = eventRepository.claimOutboxEvent("pub-A", Duration.ofSeconds(30)).orElseThrow();

        expirePublisherLease("evt-ob-2");
        Event claimB = eventRepository.claimOutboxEvent("pub-B", Duration.ofSeconds(30)).orElseThrow();
        assertEquals(claimA.getPublishLeaseVersion() + 1, claimB.getPublishLeaseVersion());

        // old publisher is fenced out, new publisher succeeds
        assertFalse(eventRepository.markOutboxPublished("evt-ob-2", "pub-A", claimA.getPublishLeaseVersion()));
        assertFalse(eventRepository.markOutboxFailedAndScheduleRetry("evt-ob-2", "pub-A", claimA.getPublishLeaseVersion(), Instant.now()));
        assertTrue(eventRepository.markOutboxPublished("evt-ob-2", "pub-B", claimB.getPublishLeaseVersion()));
        assertEquals(OutboxStatus.PUBLISHED, load("evt-ob-2").getOutboxStatus());
    }

    @Test
    void publisherCannotMarkPublishedAfterItsOwnLeaseExpired() {
        ingest("evt-ob-3");
        Event claim = eventRepository.claimOutboxEvent("pub-A", Duration.ofSeconds(30)).orElseThrow();
        expirePublisherLease("evt-ob-3");
        assertFalse(eventRepository.markOutboxPublished("evt-ob-3", "pub-A", claim.getPublishLeaseVersion()));
    }

    @Test
    void failedPublishReturnsToPendingWithFutureAttemptAndIsNotClaimedEarly() {
        ingest("evt-ob-4");
        Event claim = eventRepository.claimOutboxEvent("pub-A", Duration.ofSeconds(30)).orElseThrow();
        Instant next = Instant.now().plusSeconds(60);
        assertTrue(eventRepository.markOutboxFailedAndScheduleRetry("evt-ob-4", "pub-A", claim.getPublishLeaseVersion(), next));

        Event reloaded = load("evt-ob-4");
        assertEquals(OutboxStatus.PENDING, reloaded.getOutboxStatus());
        assertTrue(eventRepository.claimOutboxEvent("pub-B", Duration.ofSeconds(30)).isEmpty());
    }

    @Test
    void crashWindowExpiredPublishingLeaseIsReclaimedAndMessageMayBeSentTwice() {
        ingest("evt-ob-5");
        eventRepository.claimOutboxEvent("pub-A", Duration.ofSeconds(30)).orElseThrow(); // "sent to SQS", then crash
        expirePublisherLease("evt-ob-5");
        Optional<Event> reclaimed = eventRepository.claimOutboxEvent("pub-B", Duration.ofSeconds(30));
        assertTrue(reclaimed.isPresent(), "an expired PUBLISHING lease must be recoverable");
        // The duplicate message is harmless because of the worker's atomic claim - see WorkerReliabilityTest.
    }
}
