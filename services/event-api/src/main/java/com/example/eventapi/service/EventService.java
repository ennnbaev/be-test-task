package com.example.eventapi.service;

import com.example.eventapi.dto.EventCreatedResponse;
import com.example.eventapi.dto.EventResponse;
import com.example.eventapi.dto.PagedResponse;
import com.example.eventapi.entity.Event;
import com.example.eventapi.exception.EventNotFoundException;
import com.example.eventapi.repository.EventRepository;
import com.example.eventapi.repository.EventSpecifications;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.data.jpa.domain.Specification;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@Service
@Transactional(readOnly = true)
public class EventService {

    private static final Logger log = LoggerFactory.getLogger(EventService.class);
    private static final String TOPIC = "events";
    private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

    private final EventRepository eventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public EventService(EventRepository eventRepository,
                        KafkaTemplate<String, String> kafkaTemplate,
                        ObjectMapper objectMapper) {
        this.eventRepository = eventRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Transactional
    public EventCreatedResponse createEvent(Map<String, Object> body) {
        UUID id = UUID.randomUUID();
        String payload = toJson(body);
        String type = body.get("type") instanceof String t ? t : null;

        Event event = new Event();
        event.setId(id);
        event.setPayload(payload);
        event.setType(type);
        event.setStatus("RECEIVED");
        event.setCreatedAt(Instant.now());
        eventRepository.save(event);

        Map<String, Object> kafkaMessage = new LinkedHashMap<>();
        kafkaMessage.put("id", id.toString());
        kafkaMessage.put("payload", body);
        kafkaTemplate.send(TOPIC, id.toString(), toJson(kafkaMessage));

        log.info("Event created: id={}, type={}", id, type);
        return new EventCreatedResponse(id.toString(), "RECEIVED");
    }

    public EventResponse findById(UUID id) {
        Event event = eventRepository.findById(id)
                .orElseThrow(() -> new EventNotFoundException(id));
        return toResponse(event);
    }

    public PagedResponse<EventResponse> findAll(String type, Instant from, Instant to, int page, int size) {
        Specification<Event> spec = Specification.where(null);
        if (type != null) spec = spec.and(EventSpecifications.hasType(type));
        if (from != null) spec = spec.and(EventSpecifications.createdAfter(from));
        if (to != null)   spec = spec.and(EventSpecifications.createdBefore(to));

        PageRequest pageable = PageRequest.of(page, size, Sort.by("createdAt").descending());
        Page<Event> result = eventRepository.findAll(spec, pageable);

        List<EventResponse> content = result.getContent().stream()
                .map(this::toResponse)
                .toList();

        return new PagedResponse<>(content, page, size, result.getTotalElements(), result.getTotalPages());
    }

    private EventResponse toResponse(Event event) {
        return new EventResponse(
                event.getId().toString(),
                fromJson(event.getPayload()),
                event.getType(),
                event.getStatus(),
                event.getCreatedAt()
        );
    }

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Serialization failed", e);
        }
    }

    private Map<String, Object> fromJson(String json) {
        try {
            return objectMapper.readValue(json, MAP_TYPE);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to deserialize event payload", e);
        }
    }
}
