package com.example.eventprocessing.exception;

/** Thrown when MongoDB stayed unavailable after bounded retries on the ingestion path (mapped to HTTP 503). */
public class ServiceUnavailableException extends RuntimeException {
    public ServiceUnavailableException(String message, Throwable cause) {
        super(message, cause);
    }
}
