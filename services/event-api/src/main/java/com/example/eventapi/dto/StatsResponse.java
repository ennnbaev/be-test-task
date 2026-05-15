package com.example.eventapi.dto;

import java.util.List;
import java.util.Map;

public record StatsResponse(
        long totalCount,
        Map<String, Long> countByType,
        long last24hCount,
        List<TypeCount> top5TypesLast7Days
) {
}
