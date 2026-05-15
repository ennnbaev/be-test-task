package com.example.eventapi.controller;

import com.example.eventapi.BaseIntegrationTest;
import com.example.eventapi.entity.Event;
import com.example.eventapi.repository.EventRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

class StatsControllerIntegrationTest extends BaseIntegrationTest {

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private EventRepository eventRepository;

    @BeforeEach
    void setUp() {
        eventRepository.deleteAll();
    }

    @Test
    void summary_emptyDatabase_returnsZeros() throws Exception {
        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalCount", is(0)))
                .andExpect(jsonPath("$.last24hCount", is(0)))
                .andExpect(jsonPath("$.top5TypesLast7Days", hasSize(0)));
    }

    @Test
    void summary_totalCount_reflectsAllEvents() throws Exception {
        saved("a", Instant.now());
        saved("b", Instant.now());
        saved("b", Instant.now());

        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalCount", is(3)));
    }

    @Test
    void summary_countByType_groupsCorrectly() throws Exception {
        saved("user.signup",   Instant.now());
        saved("user.signup",   Instant.now());
        saved("order.created", Instant.now());

        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.countByType['user.signup']",   is(2)))
                .andExpect(jsonPath("$.countByType['order.created']", is(1)));
    }

    @Test
    void summary_last24hCount_excludesOlderEvents() throws Exception {
        saved("x", Instant.now().minus(2, ChronoUnit.DAYS));  // old — excluded
        saved("x", Instant.now().minus(1, ChronoUnit.HOURS)); // recent — counted
        saved("x", Instant.now().minus(1, ChronoUnit.HOURS)); // recent — counted

        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.totalCount",   is(3)))
                .andExpect(jsonPath("$.last24hCount", is(2)));
    }

    @Test
    void summary_top5_returnsOnlyTop5InDescendingOrder() throws Exception {
        // 6 types with different counts, all within the last 7 days
        saveN("type-a", 10, Instant.now());
        saveN("type-b",  8, Instant.now());
        saveN("type-c",  6, Instant.now());
        saveN("type-d",  4, Instant.now());
        saveN("type-e",  2, Instant.now());
        saveN("type-f",  1, Instant.now()); // 6th — must be excluded

        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.top5TypesLast7Days", hasSize(5)))
                .andExpect(jsonPath("$.top5TypesLast7Days[0].type",  is("type-a")))
                .andExpect(jsonPath("$.top5TypesLast7Days[0].count", is(10)))
                .andExpect(jsonPath("$.top5TypesLast7Days[4].type",  is("type-e")));
    }

    @Test
    void summary_top5_excludesEventsOlderThan7Days() throws Exception {
        saved("old-type", Instant.now().minus(8, ChronoUnit.DAYS)); // outside window
        saveN("new-type", 3, Instant.now());

        mockMvc.perform(get("/stats/summary"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.top5TypesLast7Days", hasSize(1)))
                .andExpect(jsonPath("$.top5TypesLast7Days[0].type", is("new-type")));
    }

    // ── helpers ───────────────────────────────────────────────────────────────

    private void saved(String type, Instant createdAt) {
        Event e = new Event();
        e.setId(UUID.randomUUID());
        e.setPayload("{\"type\":\"" + type + "\"}");
        e.setType(type);
        e.setStatus("RECEIVED");
        e.setCreatedAt(createdAt);
        eventRepository.save(e);
    }

    private void saveN(String type, int count, Instant createdAt) {
        for (int i = 0; i < count; i++) {
            saved(type, createdAt);
        }
    }
}
