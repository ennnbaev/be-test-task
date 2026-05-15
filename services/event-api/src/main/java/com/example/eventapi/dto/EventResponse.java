package com.example.eventapi.dto;

import java.time.Instant;
import java.util.Map;

public record EventResponse(
        String id,
        Map<String, Object> payload,
        String type,
        String status,
        Instant createdAt
) {
}
