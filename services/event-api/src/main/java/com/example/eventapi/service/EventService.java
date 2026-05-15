package com.example.eventapi.service;

import com.example.eventapi.dto.EventCreatedResponse;
import com.example.eventapi.entity.Event;
import com.example.eventapi.repository.EventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class EventService {

    private static final Logger log = LoggerFactory.getLogger(EventService.class);
    private static final String TOPIC = "events";

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

    private String toJson(Object obj) {
        try {
            return objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Serialization failed", e);
        }
    }
}
