package com.example.eventprocessing.api;

import com.example.eventprocessing.api.dto.CreateEventRequest;
import com.example.eventprocessing.api.dto.CustomerEventHistoryResponse;
import com.example.eventprocessing.api.dto.EventResponse;
import com.example.eventprocessing.application.EventIngestionService;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.infrastructure.mongo.EventRepository;
import jakarta.validation.Valid;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api")
public class EventController {

    static final String CORRELATION_HEADER = "X-Correlation-ID";
    static final int MAX_PAGE_SIZE = 100;

    private final EventIngestionService eventIngestionService;
    private final EventRepository eventRepository;

    public EventController(EventIngestionService eventIngestionService, EventRepository eventRepository) {
        this.eventIngestionService = eventIngestionService;
        this.eventRepository = eventRepository;
    }

    @PostMapping("/events")
    public ResponseEntity<?> createEvent(
            @Valid @RequestBody CreateEventRequest request,
            @RequestHeader(value = CORRELATION_HEADER, required = false) String correlationId) {

        if (correlationId == null || correlationId.isBlank()) {
            correlationId = "corr-" + UUID.randomUUID();
        }

        var result = eventIngestionService.ingestEvent(request, correlationId);
        Event event = result.getEvent();

        HttpHeaders headers = new HttpHeaders();
        headers.add(CORRELATION_HEADER, correlationId);

        switch (result.getStatus()) {
            case ACCEPTED:
                return ResponseEntity.status(HttpStatus.ACCEPTED).headers(headers).body(mapToResponse(event));
            case OK:
                return ResponseEntity.status(HttpStatus.OK).headers(headers).body(mapToResponse(event));
            default:
                Map<String, Object> body = new LinkedHashMap<>();
                body.put("timestamp", Instant.now().toString());
                body.put("status", 409);
                body.put("error", "Conflict");
                body.put("message", "eventId '" + request.getEventId()
                        + "' already exists with a different payload");
                return ResponseEntity.status(HttpStatus.CONFLICT).headers(headers).body(body);
        }
    }

    @GetMapping("/events/{eventId}")
    public ResponseEntity<EventResponse> getEvent(@PathVariable String eventId) {
        return eventRepository.findByEventId(eventId)
                .map(event -> ResponseEntity.ok(mapToResponse(event)))
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @GetMapping("/customers/{customerId}/events")
    public ResponseEntity<CustomerEventHistoryResponse> getCustomerEvents(
            @PathVariable String customerId,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant from,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) Instant to,
            @RequestParam(required = false) EventStatus status,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {

        if (page < 0) {
            throw new IllegalArgumentException("Page index must not be less than zero");
        }
        if (size <= 0) {
            throw new IllegalArgumentException("Page size must be greater than zero");
        }
        if (from != null && to != null && from.isAfter(to)) {
            throw new IllegalArgumentException("'from' must not be after 'to'");
        }

        int pageSize = Math.min(size, MAX_PAGE_SIZE);
        Sort sort = Sort.by(Sort.Order.desc("createdAt"), Sort.Order.desc("_id"));
        Pageable pageable = PageRequest.of(page, pageSize, sort);

        Page<Event> eventPage = eventRepository.findCustomerEvents(customerId, from, to, status, pageable);

        List<EventResponse> eventResponses = eventPage.getContent().stream()
                .map(this::mapToResponse)
                .toList();

        return ResponseEntity.ok(CustomerEventHistoryResponse.builder()
                .customerId(customerId)
                .page(page)
                .size(pageSize)
                .totalElements(eventPage.getTotalElements())
                .totalPages(eventPage.getTotalPages())
                .events(eventResponses)
                .build());
    }

    private EventResponse mapToResponse(Event event) {
        return EventResponse.builder()
                .eventId(event.getEventId())
                .customerId(event.getCustomerId())
                .type(event.getType())
                .amount(event.getAmount())
                .currency(event.getCurrency())
                .timestamp(event.getTimestamp())
                .metadata(event.getMetadata())
                .status(event.getStatus())
                .retryCount(event.getRetryCount())
                .createdAt(event.getCreatedAt())
                .processedAt(event.getProcessedAt())
                .errorMessage(event.getErrorMessage())
                .build();
    }
}
