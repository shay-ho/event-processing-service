# AI Usage

## AI Tools Used

- **Claude (Anthropic)** — used throughout the development process to review the implementation against the assignment and detailed specification, identify gaps, propose code and test changes, and help prepare and refine the project documentation.
- The project was developed through several AI-assisted iterations, with the main focus on closing specification gaps, improving reliability, refining tests, and simplifying the final implementation.
- This document describes the current consolidated version.

## Areas Where AI Assisted

AI assistance was used for:

- Reviewing the implementation against the assignment and the detailed specification.
- Identifying missing or incomplete reliability requirements.
- Proposing implementation changes for idempotency, duplicate prevention, Outbox processing, worker claiming, leases, fencing, retries, failure handling, and DLQ behavior.
- Proposing and reviewing unit, integration, reliability, and LocalStack/SQS tests.
- Reviewing API validation, error handling, configuration, logging, and observability.
- Reviewing and improving `README.md`, `ARCHITECTURE.md`, `SPEC_GAP_REPORT.md`, and this document.
- Reviewing project structure and naming for clarity and maintainability.

## Accepted AI-Assisted Decisions

The following are examples of AI-assisted technical decisions that were reviewed and incorporated into the implementation:

### 1. Event idempotency using `eventId` and canonical `payloadHash`

The implementation stores a unique `eventId` together with a canonical SHA-256 payload hash.

The resulting behavior is:

- A new `eventId` is accepted with HTTP `202`.
- A duplicate request with the same `eventId` and identical payload is treated as an idempotent duplicate and returns HTTP `200`.
- A duplicate request with the same `eventId` but a different payload returns HTTP `409 Conflict`.

This was accepted because it directly addresses the assignment's idempotency requirement while also preventing conflicting reuse of an event ID.

### 2. Atomic worker claim with lease and fencing

The worker uses an atomic MongoDB claim operation and stores lease information and a `leaseVersion` fencing token.

This was accepted because the processing flow must prevent two workers from simultaneously owning the same event and must protect MongoDB writes from stale workers after lease takeover.

### 3. Persist retry decisions before acknowledging the old message

When a retryable business failure occurs, the retry decision and next processing time are persisted in MongoDB before the current SQS message is acknowledged.

This was accepted because acknowledging the old message before persisting the retry decision could lose the event.

### 4. Outbox information stored with the event

The event document contains the Outbox state, allowing event persistence and the need to publish the event to be recorded together.

This was accepted because the assignment requires the event and its publishing intent to be persisted atomically, while avoiding a distributed transaction between MongoDB and SQS.

### 5. Stable external idempotency key

The `eventId` is used as the stable idempotency key for every attempt of an external business operation.

This was accepted because retries must not create a new business identity for the same event.

## Rejected or Modified AI-Assisted Suggestions

AI-generated suggestions were not treated as authoritative. Suggestions were reviewed against the assignment, the detailed specification, the existing implementation, and the desired scope of the solution.

Examples of decisions that were rejected or deliberately not implemented include:

### 1. Kafka, Kubernetes, and unnecessary distributed infrastructure

Kafka, Kubernetes, sharding, and multi-document transactions were not added.

These technologies were considered unnecessary for the assignment and would have increased complexity without improving compliance with the required core behavior.

### 2. Additional background recovery components

Several versions experimented with additional background mechanisms such as dedicated lease-recovery, visibility-extension, shutdown-coordination, and DLQ-publisher schedulers.

In v11 these mechanisms were removed where the simpler SQS redelivery + expired-lease claim/recovery flow was sufficient.

This was a deliberate simplification rather than an attempt to remove the required reliability guarantees.

### 3. Exactly-once processing

The implementation does not claim exactly-once processing.

The design is explicitly based on **at-least-once delivery** with idempotent business processing and duplicate-safe state transitions.

This distinction was retained because SQS and distributed processing cannot be treated as an exactly-once execution environment.

### 4. Production AWS infrastructure in the application code

ECS/Fargate, IAM, Secrets Manager, CloudWatch dashboards/alarms, autoscaling, full load testing, and fault-injection infrastructure were not implemented as application features.

Where required by the assignment/specification, these are described as production deployment and operational considerations rather than being artificially added to the core assignment implementation.

## Verification of AI-Generated Suggestions

AI-generated code and design suggestions were reviewed before being incorporated.

Verification included:

- Comparing proposed behavior with the assignment and detailed specification.
- Checking consistency with the existing domain model and state transitions.
- Reviewing MongoDB update conditions for atomicity and race prevention.
- Reviewing SQS acknowledgement behavior and crash windows.
- Reviewing retry ordering to ensure retry state is persisted before message acknowledgement.
- Reviewing duplicate delivery behavior.
- Reviewing lease expiration and fencing behavior.
- Reviewing the test cases intended to demonstrate the reliability guarantees.
- Reviewing documentation against the actual implementation rather than assuming that a proposed design was implemented.

The AI output was therefore treated as engineering input, not as a source of truth.

## Technical Ownership

AI assistance was used as a productivity and review tool, but the final implementation decisions remain the developer's responsibility.

The developer retained ownership of:

- Deciding which suggestions matched the assignment.
- Rejecting unnecessary technologies and complexity.
- Choosing the final architecture and project structure.
- Deciding which reliability mechanisms were required for the current scope.
- Reviewing the generated code and proposed changes.
- Defining the final behavior of idempotency, retries, leases, fencing, Outbox processing, DLQ handling, and acknowledgement semantics.
- Ensuring that documentation describes the implementation accurately.
- Identifying limitations that must not be hidden or overstated.

AI-generated suggestions were therefore accepted, modified, or rejected based on engineering judgment rather than copied blindly.

## Development History

The project was developed through several iterative AI-assisted reviews and implementation passes.

The main focus of these iterations was:

- Closing gaps against the assignment and detailed specification.
- Improving idempotency, duplicate prevention, Outbox handling, worker claiming, leases, fencing, retries, and DLQ behavior.
- Adding and refining reliability and integration tests.
- Improving SQS/LocalStack integration, correlation IDs, metrics, graceful shutdown, and documentation.
- Simplifying the final implementation by removing background mechanisms that were not necessary for the required reliability guarantees.

The current version represents the final consolidated implementation after these iterations.

## Summary

AI was used extensively as an engineering productivity and review tool, but not as an authority.

The final solution was selected by comparing AI-assisted proposals with the assignment requirements, the detailed specification, the existing implementation, and the desired scope. Where an AI suggestion increased unnecessary complexity, conflicted with the specification, or was not justified by the assignment, it was rejected or modified.

The most important remaining verification step is to compile and execute the complete project and test suite in an environment with Java/Maven, Docker, MongoDB, and LocalStack available. Until that verification is completed, runtime correctness should not be claimed solely on the basis of AI review.
