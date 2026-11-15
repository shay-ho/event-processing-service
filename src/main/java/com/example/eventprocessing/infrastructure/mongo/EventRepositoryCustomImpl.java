package com.example.eventprocessing.infrastructure.mongo;

import com.example.eventprocessing.domain.DlqStatus;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.OutboxStatus;
import com.example.eventprocessing.domain.EventStatus;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.FindAndModifyOptions;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Repository;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

@Repository
public class EventRepositoryCustomImpl implements EventRepositoryCustom {

    private final MongoTemplate mongoTemplate;

    public EventRepositoryCustomImpl(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public Page<Event> findCustomerEvents(String customerId, Instant from, Instant to, EventStatus status, Pageable pageable) {
        Criteria criteria = Criteria.where("customerId").is(customerId);
        if (status != null) {
            criteria = criteria.and("status").is(status);
        }
        if (from != null && to != null) {
            criteria = criteria.and("createdAt").gte(from).lte(to);
        } else if (from != null) {
            criteria = criteria.and("createdAt").gte(from);
        } else if (to != null) {
            criteria = criteria.and("createdAt").lte(to);
        }
        Query countQuery = new Query(criteria);
        long total = mongoTemplate.count(countQuery, Event.class);
        Query pageQuery = new Query(criteria).with(pageable);
        List<Event> content = mongoTemplate.find(pageQuery, Event.class);
        return new PageImpl<>(content, pageable, total);
    }

    @Override
    public Optional<Event> claimOutboxEvent(String publisherId, Duration leaseDuration) {
        Instant now = Instant.now();
        Instant leaseUntil = now.plus(leaseDuration);

        Criteria pendingCriteria = Criteria.where("outboxStatus").is(OutboxStatus.PENDING)
                .and("outboxNextAttemptAt").lte(now);

        Criteria expiredPublishingCriteria = Criteria.where("outboxStatus").is(OutboxStatus.PUBLISHING)
                .and("publishingLeaseUntil").lte(now);

        Query query = new Query(new Criteria().orOperator(pendingCriteria, expiredPublishingCriteria))
                .with(Sort.by(Sort.Direction.ASC, "outboxNextAttemptAt"));
        Update update = new Update()
                .set("outboxStatus", OutboxStatus.PUBLISHING)
                .set("publisherId", publisherId)
                .set("publishingLeaseUntil", leaseUntil)
                .inc("publishLeaseVersion", 1)
                .inc("outboxPublishAttempts", 1)
                .set("updatedAt", now);

        FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true);
        Event claimed = mongoTemplate.findAndModify(query, update, options, Event.class);
        return Optional.ofNullable(claimed);
    }

    @Override
    public boolean markOutboxPublished(String eventId, String publisherId, long publishLeaseVersion) {
        Instant now = Instant.now();
        Query query = new Query(Criteria.where("eventId").is(eventId)
                .and("outboxStatus").is(OutboxStatus.PUBLISHING)
                .and("publisherId").is(publisherId)
                .and("publishLeaseVersion").is(publishLeaseVersion)
                .and("publishingLeaseUntil").gt(now));

        Update update = new Update()
                .set("outboxStatus", OutboxStatus.PUBLISHED)
                .set("publishedAt", now)
                .set("updatedAt", now)
                .unset("publisherId")
                .unset("publishingLeaseUntil");

        var result = mongoTemplate.updateFirst(query, update, Event.class);
        return result.getModifiedCount() > 0;
    }

    @Override
    public boolean markOutboxFailedAndScheduleRetry(String eventId, String publisherId, long publishLeaseVersion, Instant nextAttemptAt) {
        Instant now = Instant.now();
        Query query = new Query(Criteria.where("eventId").is(eventId)
                .and("outboxStatus").is(OutboxStatus.PUBLISHING)
                .and("publisherId").is(publisherId)
                .and("publishLeaseVersion").is(publishLeaseVersion)
                .and("publishingLeaseUntil").gt(now));

        Update update = new Update()
                .set("outboxStatus", OutboxStatus.PENDING)
                .set("outboxNextAttemptAt", nextAttemptAt)
                .set("updatedAt", now)
                .unset("publisherId")
                .unset("publishingLeaseUntil");

        var result = mongoTemplate.updateFirst(query, update, Event.class);
        return result.getModifiedCount() > 0;
    }

    @Override
    public Optional<Event> claimWorkerEvent(String eventId, String workerId, Duration leaseDuration) {
        Instant now = Instant.now();
        Instant leaseUntil = now.plus(leaseDuration);

        Criteria receivedCriteria = Criteria.where("status").is(EventStatus.RECEIVED);
        Criteria retryingCriteria = Criteria.where("status").is(EventStatus.RETRYING)
                .and("nextProcessingAttemptAt").lte(now);
        // A crashed worker never ACKs; once its lease expired the redelivered message takes the event over here.
        Criteria expiredLeaseCriteria = Criteria.where("status").is(EventStatus.PROCESSING)
                .and("processingLeaseUntil").lte(now);

        Criteria statusCriteria = new Criteria().orOperator(receivedCriteria, retryingCriteria, expiredLeaseCriteria);
        Query query = new Query(Criteria.where("eventId").is(eventId).andOperator(statusCriteria));

        Update update = new Update()
                .set("status", EventStatus.PROCESSING)
                .set("workerId", workerId)
                .set("processingStartedAt", now)
                .set("processingLeaseUntil", leaseUntil)
                .inc("leaseVersion", 1)
                .set("updatedAt", now);

        FindAndModifyOptions options = FindAndModifyOptions.options().returnNew(true);
        Event claimed = mongoTemplate.findAndModify(query, update, options, Event.class);
        return Optional.ofNullable(claimed);
    }

    @Override
    public boolean markProcessed(String eventId, String workerId, long leaseVersion) {
        Instant now = Instant.now();
        Query query = new Query(Criteria.where("eventId").is(eventId)
                .and("status").is(EventStatus.PROCESSING)
                .and("workerId").is(workerId)
                .and("leaseVersion").is(leaseVersion)
                .and("processingLeaseUntil").gt(now));

        Update update = new Update()
                .set("status", EventStatus.PROCESSED)
                .set("processedAt", now)
                .set("updatedAt", now)
                .unset("errorMessage")
                .unset("workerId")
                .unset("processingLeaseUntil")
                .unset("processingStartedAt");

        var result = mongoTemplate.updateFirst(query, update, Event.class);
        return result.getModifiedCount() > 0;
    }

    @Override
    public boolean scheduleRetry(String eventId, String workerId, long leaseVersion, int nextRetryCount, Instant nextAttemptAt, String errorMessage) {
        Instant now = Instant.now();

        Query query = new Query(Criteria.where("eventId").is(eventId)
                .and("status").is(EventStatus.PROCESSING)
                .and("workerId").is(workerId)
                .and("leaseVersion").is(leaseVersion)
                .and("processingLeaseUntil").gt(now));

        // Single atomic update: counter + schedule + outbox re-arm (spec section 9).
        Update update = new Update()
                .set("status", EventStatus.RETRYING)
                .set("retryCount", nextRetryCount)
                .set("nextProcessingAttemptAt", nextAttemptAt)
                .set("outboxStatus", OutboxStatus.PENDING)
                .set("outboxNextAttemptAt", nextAttemptAt)
                .set("outboxPublishAttempts", 0)
                .set("errorMessage", errorMessage)
                .set("updatedAt", now)
                .unset("workerId")
                .unset("processingLeaseUntil")
                .unset("processingStartedAt")
                .unset("publisherId")
                .unset("publishingLeaseUntil");

        var result = mongoTemplate.updateFirst(query, update, Event.class);
        return result.getModifiedCount() > 0;
    }

    @Override
    public boolean markFailed(String eventId, String workerId, long leaseVersion, String errorMessage) {
        Instant now = Instant.now();
        Query query = new Query(Criteria.where("eventId").is(eventId)
                .and("status").is(EventStatus.PROCESSING)
                .and("workerId").is(workerId)
                .and("leaseVersion").is(leaseVersion)
                .and("processingLeaseUntil").gt(now));

        Update update = new Update()
                .set("status", EventStatus.FAILED)
                .set("dlqStatus", DlqStatus.PENDING)
                .set("errorMessage", errorMessage)
                .set("updatedAt", now)
                .unset("workerId")
                .unset("processingLeaseUntil")
                .unset("processingStartedAt");

        var result = mongoTemplate.updateFirst(query, update, Event.class);
        return result.getModifiedCount() > 0;
    }

    @Override
    public boolean markDlqPublished(String eventId) {
        Instant now = Instant.now();
        Query query = new Query(Criteria.where("eventId").is(eventId)
                .and("status").is(EventStatus.FAILED)
                .and("dlqStatus").is(DlqStatus.PENDING));

        Update update = new Update()
                .set("dlqStatus", DlqStatus.PUBLISHED)
                .set("dlqPublishedAt", now)
                .set("updatedAt", now);

        var result = mongoTemplate.updateFirst(query, update, Event.class);
        return result.getModifiedCount() > 0;
    }
}
