package com.example.eventapi.service;

import com.example.eventapi.dto.StatsResponse;
import com.example.eventapi.dto.TypeCount;
import com.example.eventapi.repository.EventRepository;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
@Transactional(readOnly = true)
public class StatsService {

    private final EventRepository eventRepository;

    public StatsService(EventRepository eventRepository) {
        this.eventRepository = eventRepository;
    }

    @Cacheable("stats-summary")
    public StatsResponse getSummary() {
        long totalCount = eventRepository.count();

        Map<String, Long> countByType = new LinkedHashMap<>();
        eventRepository.countGroupedByType()
                .forEach(p -> countByType.put(p.getType(), p.getCount()));

        Instant since24h = Instant.now().minus(24, ChronoUnit.HOURS);
        long last24hCount = eventRepository.countCreatedAfter(since24h);

        Instant since7d = Instant.now().minus(7, ChronoUnit.DAYS);
        List<TypeCount> top5 = eventRepository.top5TypesSince(since7d).stream()
                .map(p -> new TypeCount(p.getType(), p.getCount()))
                .toList();

        return new StatsResponse(totalCount, countByType, last24hCount, top5);
    }
}
