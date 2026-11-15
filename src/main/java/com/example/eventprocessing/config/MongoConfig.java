package com.example.eventprocessing.config;

import com.example.eventprocessing.domain.Event;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.index.IndexOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.index.PartialIndexFilter;
import org.springframework.data.mongodb.core.query.Criteria;

/**
 * Creates the indexes defined in spec section 5 that are still used (no DLQ publisher scan anymore), with the spec names.
 * Runs during bean initialisation (before the web server accepts traffic and before any scheduler fires),
 * and a failure aborts startup: the unique eventId index is a correctness requirement.
 * (spring.data.mongodb.auto-index-creation is disabled so that no duplicate/conflicting index is auto-created.)
 */
@Configuration
public class MongoConfig {

    private static final Logger log = LoggerFactory.getLogger(MongoConfig.class);

    private final MongoTemplate mongoTemplate;

    public MongoConfig(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
    }

    @PostConstruct
    public void initIndices() {
        try {
            log.info("Initializing MongoDB indexes...");
            IndexOperations ops = mongoTemplate.indexOps(Event.class);

            ops.ensureIndex(new Index().named("idx_eventId_unique")
                    .on("eventId", Sort.Direction.ASC).unique());

            ops.ensureIndex(new Index().named("idx_customer_created")
                    .on("customerId", Sort.Direction.ASC)
                    .on("createdAt", Sort.Direction.DESC)
                    .on("_id", Sort.Direction.DESC));

            ops.ensureIndex(new Index().named("idx_customer_status_created")
                    .on("customerId", Sort.Direction.ASC)
                    .on("status", Sort.Direction.ASC)
                    .on("createdAt", Sort.Direction.DESC)
                    .on("_id", Sort.Direction.DESC));

            ops.ensureIndex(new Index().named("idx_outbox_pending")
                    .on("outboxStatus", Sort.Direction.ASC)
                    .on("outboxNextAttemptAt", Sort.Direction.ASC)
                    .partial(PartialIndexFilter.of(Criteria.where("outboxStatus").is("PENDING"))));

            ops.ensureIndex(new Index().named("idx_outbox_recovery")
                    .on("outboxStatus", Sort.Direction.ASC)
                    .on("publishingLeaseUntil", Sort.Direction.ASC)
                    .partial(PartialIndexFilter.of(Criteria.where("outboxStatus").is("PUBLISHING"))));

            ops.ensureIndex(new Index().named("idx_worker_recovery")
                    .on("status", Sort.Direction.ASC)
                    .on("processingLeaseUntil", Sort.Direction.ASC)
                    .partial(PartialIndexFilter.of(Criteria.where("status").is("PROCESSING"))));

            log.info("MongoDB indexes ready.");
        } catch (Exception e) {
            log.error("FATAL: failed to initialize MongoDB indexes: {}", e.getMessage(), e);
            throw new IllegalStateException("Failed to initialize critical MongoDB indexes", e);
        }
    }
}
