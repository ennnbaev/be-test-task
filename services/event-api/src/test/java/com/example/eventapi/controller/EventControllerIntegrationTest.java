package com.example.eventapi.controller;

import com.example.eventapi.BaseIntegrationTest;
import com.example.eventapi.entity.Event;
import com.example.eventapi.repository.EventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class EventControllerIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EventRepository eventRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        eventRepository.deleteAll();
    }

    // ── POST /events ──────────────────────────────────────────────────────────

    @Test
    void createEvent_returns201WithIdAndStatus() throws Exception {
        mockMvc.perform(post("/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type":"user.signup","userId":"u-1"}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.id").isString())
                .andExpect(jsonPath("$.status", is("RECEIVED")));

        verify(kafkaTemplate).send(eq("events"), anyString(), anyString());
    }

    @Test
    void createEvent_thenFindById_returnsConsistentData() throws Exception {
        String body = mockMvc.perform(post("/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"type":"payment.processed","amount":99.99}
                                """))
                .andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();

        String id = objectMapper.readTree(body).get("id").asText();

        mockMvc.perform(get("/events/{id}", id))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is(id)))
                .andExpect(jsonPath("$.type", is("payment.processed")))
                .andExpect(jsonPath("$.status", is("RECEIVED")))
                .andExpect(jsonPath("$.payload.amount", is(99.99)))
                .andExpect(jsonPath("$.createdAt").isString());
    }

    @Test
    void createEvent_malformedJson_returns400() throws Exception {
        mockMvc.perform(post("/events")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("not-json"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code", is("BAD_REQUEST")));
    }

    // ── GET /events/{id} ──────────────────────────────────────────────────────

    @Test
    void findById_existingEvent_returns200WithParsedPayload() throws Exception {
        Event event = saved("order.created", """
                {"type":"order.created","orderId":"o-99"}
                """, Instant.now());

        mockMvc.perform(get("/events/{id}", event.getId()))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id", is(event.getId().toString())))
                .andExpect(jsonPath("$.type", is("order.created")))
                .andExpect(jsonPath("$.status", is("RECEIVED")))
                .andExpect(jsonPath("$.payload.orderId", is("o-99")))
                .andExpect(jsonPath("$.createdAt").isString());
    }

    @Test
    void findById_unknownId_returns404() throws Exception {
        mockMvc.perform(get("/events/{id}", UUID.randomUUID()))
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.code", is("NOT_FOUND")));
    }

    @Test
    void findById_invalidUuid_returns400() throws Exception {
        mockMvc.perform(get("/events/not-a-uuid"))
                .andExpect(status().isBadRequest());
    }

    // ── GET /events ───────────────────────────────────────────────────────────

    @Test
    void findAll_noFilters_returnsPaginatedList() throws Exception {
        saved("a", """
                {"type":"a"}
                """, Instant.now());
        saved("b", """
                {"type":"b"}
                """, Instant.now());

        mockMvc.perform(get("/events"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content", hasSize(2)))
                .andExpect(jsonPath("$.totalElements", is(2)))
                .andExpect(jsonPath("$.page", is(0)))
                .andExpect(jsonPath("$.size", is(20)));
    }

    @Test
    void findAll_filterByType_returnsOnlyMatchingEvents() throws Exception {
        saved("user.signup", """
                {"type":"user.signup"}
                """, Instant.now());
        saved("order.created", """
                {"type":"order.created"}
                """, Instant.now());

        mockMvc.perform(get("/events").param("type", "user.signup"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content", hasSize(1)))
                .andExpect(jsonPath("$.content[0].type", is("user.signup")));
    }

    @Test
    void findAll_filterByTimeRange_returnsOnlyEventsInRange() throws Exception {
        Instant now = Instant.now();
        saved("old", """
                {"type":"old"}
                """, now.minus(2, ChronoUnit.DAYS));
        saved("recent", """
                {"type":"recent"}
                """, now.minus(1, ChronoUnit.HOURS));

        String from = now.minus(6, ChronoUnit.HOURS).toString();

        mockMvc.perform(get("/events").param("from", from))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content", hasSize(1)))
                .andExpect(jsonPath("$.content[0].type", is("recent")));
    }

    @Test
    void findAll_pagination_respectsPageAndSize() throws Exception {
        for (int i = 0; i < 5; i++) {
            saved("t", String.format("""
                    {"type":"t","i":%d}
                    """, i), Instant.now());
        }

        mockMvc.perform(get("/events").param("page", "0").param("size", "2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content", hasSize(2)))
                .andExpect(jsonPath("$.totalElements", is(5)))
                .andExpect(jsonPath("$.totalPages", is(3)));
    }

    @Test
    void findAll_sizeTooLarge_returns400() throws Exception {
        mockMvc.perform(get("/events").param("size", "200"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code", is("VALIDATION_ERROR")));
    }

    @Test
    void findAll_negativePageNumber_returns400() throws Exception {
        mockMvc.perform(get("/events").param("page", "-1"))
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.code", is("VALIDATION_ERROR")));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private Event saved(String type, String payload, Instant createdAt) {
        Event e = new Event();
        e.setId(UUID.randomUUID());
        e.setPayload(payload.strip());
        e.setType(type);
        e.setStatus("RECEIVED");
        e.setCreatedAt(createdAt);
        return eventRepository.save(e);
    }
}
