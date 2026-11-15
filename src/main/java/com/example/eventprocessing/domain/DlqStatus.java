package com.example.eventprocessing.domain;

/** NONE: not failed. PENDING: FAILED, DLQ message not sent yet. PUBLISHED: DLQ message sent. */
public enum DlqStatus {
    NONE,
    PENDING,
    PUBLISHED
}
