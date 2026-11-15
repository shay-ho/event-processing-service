package com.example.eventprocessing.api.dto;

import lombok.Builder;
import lombok.Data;

import java.util.List;

@Data
@Builder
public class CustomerEventHistoryResponse {
    private String customerId;
    private int page;
    private int size;
    private long totalElements;
    private int totalPages;
    private List<EventResponse> events;
}
