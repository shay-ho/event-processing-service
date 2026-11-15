# Event Processing Service

A service for asynchronous ingestion and processing of business events.
**Java 21 · Spring Boot 3.2 · MongoDB 6 · AWS SQS (LocalStack locally) · Maven**

> The system provides **At-Least-Once** (not Exactly-Once). MongoDB is the source of truth; SQS is used for message delivery and worker execution.

## Local Run

### Full Run with Docker Compose

```bash
docker compose up --build -d     # MongoDB + LocalStack (queue + DLQ + Redrive) + application
mvn test                         # Unit + integration tests (Docker is required because of Testcontainers)
```

### Run the Application Outside Docker (Alternative)

If MongoDB and LocalStack are already running locally (for example, from `docker compose up -d mongodb localstack`):

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

`docker compose` waits for LocalStack to become *healthy*, meaning that `init-sqs.sh` has created both queues.

## Environment Variables

| Variable | Default | Description |
| --- | --- | --- |
| `MONGODB_URI` | `mongodb://localhost:27017/event_db?...timeouts` | MongoDB connection (including timeouts) |
| `AWS_ENDPOINT` | – (only in the `local` profile: `http://localhost:4566`) | LocalStack endpoint. Not configured in production |
| `AWS_REGION` | `us-east-1` | AWS region |
| `EVENTS_QUEUE_NAME` / `EVENTS_DLQ_NAME` | `events-queue` / `events-queue-dlq` | Queue names |
| `EVENTS_WORKER_CONCURRENCY` | `10` | Maximum number of messages processed concurrently |
| `SPRING_PROFILES_ACTIVE` | – | `local` activates `application-local.yml` (dummy LocalStack credentials only) |

No secrets are stored in the code: in production, credentials come from the ECS Task IAM Role / Secrets Manager. All Lease, Retry, and Backoff parameters are under `events.*` (see `EventsProperties`).

## API

| Endpoint | Description |
| --- | --- |
| `POST /api/events` | `202` new event · `200` same eventId and same content (regardless of status) · `409` same eventId with different content · `400` validation · `503` MongoDB unavailable |
| `GET /api/events/{eventId}` | `200` / `404` |
| `GET /api/customers/{customerId}/events` | Parameters: `from`, `to` (inclusive bounds on `createdAt`), `status`, `page=0`, `size=20` (maximum 100). Sort: `createdAt DESC, _id DESC` |

The `X-Correlation-ID` header is accepted or generated, stored in the document, propagated to SQS as a Message Attribute, and returned in the response.

```bash
curl -i -X POST localhost:8080/api/events -H 'Content-Type: application/json' -d '{
  "eventId":"evt-123","customerId":"cust-1","type":"PAYMENT","amount":1250.50,
  "currency":"ILS","timestamp":"2026-11-12T12:00:00Z","metadata":{"source":"mobile","channel":"app"}}'
curl localhost:8080/api/events/evt-123
curl 'localhost:8080/api/customers/cust-1/events?status=PROCESSED&size=10'
curl localhost:8080/actuator/prometheus | grep events_
```

### Idempotency

* Same `eventId` with the same payload ⇒ `HTTP 200 OK` (without creating another event).
* Same `eventId` with a different payload ⇒ `HTTP 409 Conflict`.

The unique `eventId` in MongoDB (unique index) and canonical `payloadHash` prevent duplicate event creation even when requests arrive concurrently.

## Architectural Decisions

* **Single document per event** (data + Outbox + Leases + DLQ) – atomicity without Multi-Document Transactions.
* **Outbox** with atomic Claim (`findAndModify`), Lease, and `publishLeaseVersion` as a Fencing Token.
* **Worker**: Atomic Claim (`findAndModify`) also handles expired Leases; every result update requires `workerId + leaseVersion + active lease` (Fencing). **Without Lease renewal**: `lease-seconds=30`, and a fixed SQS Visibility Timeout of 60 seconds (must be greater than the Lease).
* **Business Retry** is stored in MongoDB (`retryCount`, `nextProcessingAttemptAt`) in the same atomic update that returns the Outbox to PENDING. Up to 3 retries (4 attempts), Backoff 5/15/45 seconds + Jitter + cap.
* **Crash recovery** without a Scheduler: a crashed Worker does not ACK the message ⇒ SQS redelivers it after 60 seconds, and the Claim takes the event once the Lease has expired (`leaseVersion+1`).
* **DLQ**: Permanent failure (invalid data / retry limit exceeded) ⇒ the Worker writes `FAILED` + `dlqStatus=PENDING`, sends a message directly to the DLQ, and marks it `PUBLISHED`. A failed send ⇒ no ACK ⇒ redelivery completes the send. **SQS Redrive** (`maxReceiveCount=5`) is a safety net for poison messages.
* **Graceful shutdown**: `server.shutdown=graceful` and the Spring Cloud AWS Listener Container; interrupted processing causes the Lease to expire and the message to be redelivered.
* Indexes required by the specification (with exact names) are created at application startup; failure prevents the application from starting.

Full details and diagrams: `ARCHITECTURE.md`. Specification gap report: `SPEC_GAP_REPORT.md`.

## Known Limitations

* **Idempotency with an external service**: The default handler (`SimulatedBusinessHandler`) is a placeholder. If an external service is enabled in the future, `eventId` must be sent as the Idempotency-Key; without idempotency support on the external service side, duplicate external actions cannot be prevented.
* **Redrive Policy** (`maxReceiveCount=5`) is intended for "poison" messages. If MongoDB is unavailable for an extended period, a message may move to the SQS DLQ without the event being processed; the event remains in MongoDB with `status=RECEIVED` (or `RETRYING`), and `outboxStatus=PUBLISHED` – this is an Outbox field, not the event status. The same applies to an event on which the Worker repeatedly crashes (5 deliveries): the message moves to the DLQ and the event remains `PROCESSING` in MongoDB. A Redrive/investigation procedure and a DLQ alarm are required (not defined here).
* The DLQ is used both for business-failure messages (sent by the Worker) and for messages moved by SQS Redrive. DLQ consumers identify duplicates by `eventId`.
* **Without Lease renewal**: Processing that lasts longer than 30 seconds loses the Lease; the result write is rejected (Fencing), no ACK is sent, and the event is processed again (At-Least-Once; therefore an Idempotency-Key is required for external calls). Normal processing is expected to be significantly shorter.
* **Crash recovery does not consume a business Retry**: `retryCount` increases only for business failures. Repeated crashes are limited by the SQS `maxReceiveCount`.
* Pagination is based on Page/Offset (`count` + `skip`); Cursor-based pagination can be introduced later.
* Gauges `events.by.status` are refreshed every 30 seconds using `count` (without a dedicated index on `status` alone).
* **Not implemented here**: IAM, Alarms, Dashboards, ECS Auto Scaling, Secrets Manager, load testing/Fault-injection – these are deployment/operational configurations rather than application code.

## Configuration and Metrics
| Setting / Metric | Default | Description |
|---|---|---|
| `events.api.max-payload-bytes` (`EVENTS_MAX_PAYLOAD_BYTES`) | `32768` | Maximum body size for `POST /api/events`; exceeding it ⇒ `413` |
| `events.sqs.api-call-timeout-seconds` | `60` | Total timeout for an SQS call (including SDK retries) |
| `events.sqs.api-call-attempt-timeout-seconds` | `30` | Timeout for a single attempt; must be greater than long-poll (10s) |
| `events.sqs.queue.messages{queue,state}` | – | Queue and DLQ depth: `visible` / `in_flight` / `delayed` |
| `events.oldest.unprocessed.age.seconds` | – | Age of the oldest event in state RECEIVED/RETRYING/PROCESSING |
| `events.outbox.oldest.pending.age.seconds` | – | Age of the oldest event whose Outbox is still PENDING |

`ApproximateAgeOfOldestMessage` for SQS is available only in CloudWatch (an alarm on it is a deployment configuration), so the age is calculated from MongoDB.

## Workers (Simplified Version)
* `events.processing.worker-concurrency` (`EVENTS_WORKER_CONCURRENCY`, default 10) sets `max-concurrent-messages`.
* **Configuration constraint:** `SQS VisibilityTimeout (60s)` > `events.processing.lease-seconds (30s)` > normal processing time. The value 60 is configured in `init-sqs.sh`. This allows a message redelivered after a crash to find an expired Lease and claim the event.
* Removed in the simplified version (without changing the business guarantees): `InFlightTracker`, `WorkerShutdownCoordinator`, `EventLeaseRenewService`, `VisibilityExtender`, `LeaseRecoveryScheduler`, `DlqPublisherScheduler`. Retained: Idempotency, Outbox, Atomic Claim, `processingLeaseUntil`, `leaseVersion` (Fencing), Retry + Backoff, DLQ, metrics, Correlation ID, and indexes.
* The `idx_dlq_pending` and `idx_dlq_recovery` indexes were removed (there is no longer a Mongo DLQ scan); the remaining indexes were retained.
