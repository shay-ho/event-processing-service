package com.example.eventprocessing.api;

import com.example.eventprocessing.application.EventIngestionService;
import com.example.eventprocessing.application.EventIngestionService.IngestionResult;
import com.example.eventprocessing.application.EventIngestionService.IngestionStatus;
import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import com.example.eventprocessing.exception.ServiceUnavailableException;
import com.example.eventprocessing.infrastructure.mongo.EventRepository;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.PageImpl;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(EventController.class)
class EventControllerTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private EventIngestionService eventIngestionService;

    @MockBean
    private EventRepository eventRepository;

    private static String validBody(String eventId) {
        return """
                {"eventId":"%s","customerId":"cust-1","type":"PAYMENT","amount":1250.50,
                 "currency":"ILS","timestamp":"%s","metadata":{"source":"mobile","channel":"app"}}
                """.formatted(eventId, Instant.now().minusSeconds(5));
    }

    private static Event event(String id) {
        return Event.builder().eventId(id).customerId("cust-1").status(EventStatus.RECEIVED).build();
    }

    @Test
    void validationFailureReturns400() throws Exception {
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .content("{\"eventId\":\"\",\"amount\":-50.00}"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.validationErrors").exists());
    }

    @Test
    void malformedJsonReturns400() throws Exception {
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content("{not json"))
                .andExpect(status().isBadRequest());
    }

    @Test
    void invalidCurrencyAndNestedMetadataAreRejected() throws Exception {
        String body = """
                {"eventId":"e1","customerId":"c","type":"PAYMENT","amount":1,"currency":"XXQ",
                 "timestamp":"2026-01-01T00:00:00Z","metadata":{"nested":{"a":1}}}
                """;
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().isBadRequest());
    }

    @Test
    void newEventReturns202AndCorrelationHeader() throws Exception {
        when(eventIngestionService.ingestEvent(any(), any()))
                .thenReturn(new IngestionResult(IngestionStatus.ACCEPTED, event("evt-1")));

        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON)
                        .header("X-Correlation-ID", "corr-abc").content(validBody("evt-1")))
                .andExpect(status().isAccepted())
                .andExpect(header().string("X-Correlation-ID", "corr-abc"))
                .andExpect(jsonPath("$.eventId").value("evt-1"))
                .andExpect(jsonPath("$.status").value("RECEIVED"));
    }

    @Test
    void duplicateWithSamePayloadReturns200() throws Exception {
        when(eventIngestionService.ingestEvent(any(), any()))
                .thenReturn(new IngestionResult(IngestionStatus.OK, event("evt-1")));
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(validBody("evt-1")))
                .andExpect(status().isOk());
    }

    @Test
    void duplicateWithDifferentPayloadReturns409() throws Exception {
        when(eventIngestionService.ingestEvent(any(), any()))
                .thenReturn(new IngestionResult(IngestionStatus.CONFLICT, event("evt-1")));
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(validBody("evt-1")))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.status").value(409));
    }

    @Test
    void mongoUnavailableReturns503() throws Exception {
        when(eventIngestionService.ingestEvent(any(), any()))
                .thenThrow(new ServiceUnavailableException("down", new RuntimeException()));
        mockMvc.perform(post("/api/events").contentType(MediaType.APPLICATION_JSON).content(validBody("evt-1")))
                .andExpect(status().isServiceUnavailable());
    }

    @Test
    void getUnknownEventReturns404() throws Exception {
        when(eventRepository.findByEventId("nope")).thenReturn(Optional.empty());
        mockMvc.perform(get("/api/events/nope")).andExpect(status().isNotFound());
    }

    @Test
    void historyCapsSizeAt100AndUsesDefaults() throws Exception {
        when(eventRepository.findCustomerEvents(any(), any(), any(), any(), any(Pageable.class)))
                .thenReturn(new PageImpl<>(List.of()));

        mockMvc.perform(get("/api/customers/cust-1/events").param("size", "1000"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.size").value(100))
                .andExpect(jsonPath("$.page").value(0));

        ArgumentCaptor<Pageable> captor = ArgumentCaptor.forClass(Pageable.class);
        verify(eventRepository).findCustomerEvents(eq("cust-1"), any(), any(), any(), captor.capture());
        assertEquals(100, captor.getValue().getPageSize());
        assertEquals("createdAt: DESC,_id: DESC", captor.getValue().getSort().toString());
    }

    @Test
    void historyRejectsBadStatusAndBadRange() throws Exception {
        mockMvc.perform(get("/api/customers/cust-1/events").param("status", "BOGUS"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/customers/cust-1/events")
                        .param("from", "2026-12-01T00:00:00Z").param("to", "2026-01-01T00:00:00Z"))
                .andExpect(status().isBadRequest());
        mockMvc.perform(get("/api/customers/cust-1/events").param("page", "-1"))
                .andExpect(status().isBadRequest());
    }
}
