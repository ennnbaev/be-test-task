package com.example.eventapi;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/events")
public class EventController {

    private static final Logger log = LoggerFactory.getLogger(EventController.class);
    private static final String TOPIC = "events";

    private final EventRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public EventController(EventRepository repository,
                           KafkaTemplate<String, String> kafkaTemplate,
                           ObjectMapper objectMapper) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> create(@RequestBody Map<String, Object> body) {
        UUID id = UUID.randomUUID();
        String payload;
        try {
            payload = objectMapper.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            // Body was already parsed from JSON, re-serialization should never fail
            throw new IllegalStateException("Failed to serialize event payload", e);
        }

        String type = body.get("type") instanceof String t ? t : null;

        Event event = new Event();
        event.setId(id);
        event.setPayload(payload);
        event.setType(type);
        event.setStatus("RECEIVED");
        event.setCreatedAt(Instant.now());
        repository.save(event);

        Map<String, Object> message = new LinkedHashMap<>();
        message.put("id", id.toString());
        message.put("payload", body);
        try {
            kafkaTemplate.send(TOPIC, id.toString(), objectMapper.writeValueAsString(message));
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize Kafka message", e);
        }

        log.info("Event created: id={}, type={}", id, type);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("id", id.toString());
        response.put("status", "RECEIVED");
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}
