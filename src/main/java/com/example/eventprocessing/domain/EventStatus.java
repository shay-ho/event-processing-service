package com.example.eventprocessing.domain;

public enum EventStatus {
    RECEIVED,
    PROCESSING,
    RETRYING,
    PROCESSED,
    FAILED
}
