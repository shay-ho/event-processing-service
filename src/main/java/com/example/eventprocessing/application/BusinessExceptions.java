package com.example.eventprocessing.application;

/** Failure classification for business processing (spec section 9). */
public final class BusinessExceptions {

    private BusinessExceptions() {
    }

    /** Temporary failure (e.g. downstream timeout): the event is scheduled for a business retry. */
    public static class RetryableBusinessException extends RuntimeException {
        public RetryableBusinessException(String message) {
            super(message);
        }

        public RetryableBusinessException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /** Invalid business data: no retry, the event goes straight to FAILED (+ DLQ). */
    public static class NonRetryableBusinessException extends RuntimeException {
        public NonRetryableBusinessException(String message) {
            super(message);
        }
    }
}
