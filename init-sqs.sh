#!/bin/bash
set -e
echo "Initializing LocalStack SQS queues..."

# DLQ first (needed for the redrive policy ARN).
awslocal sqs create-queue --queue-name events-queue-dlq \
  --attributes MessageRetentionPeriod=1209600

DLQ_ARN=$(awslocal sqs get-queue-attributes \
  --queue-url http://localhost:4566/000000000000/events-queue-dlq \
  --attribute-names QueueArn --query 'Attributes.QueueArn' --output text)

# Main queue: 60s visibility timeout (MUST be longer than the 30s worker lease: there is no lease/visibility renewal,
# a crashed worker's message reappears after 60s, when its lease has already expired) + redrive to DLQ after 5 failed receives.
awslocal sqs create-queue --queue-name events-queue \
  --attributes "{\"VisibilityTimeout\":\"60\",\"RedrivePolicy\":\"{\\\"deadLetterTargetArn\\\":\\\"${DLQ_ARN}\\\",\\\"maxReceiveCount\\\":\\\"5\\\"}\"}"

echo "Queues created successfully."
