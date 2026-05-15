package com.example.eventapi.controller;

import com.example.eventapi.dto.EventCreatedResponse;
import com.example.eventapi.service.EventService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/events")
public class EventController {

    private final EventService eventService;

    public EventController(EventService eventService) {
        this.eventService = eventService;
    }

    @PostMapping
    public ResponseEntity<EventCreatedResponse> create(@RequestBody Map<String, Object> body) {
        EventCreatedResponse response = eventService.createEvent(body);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }
}
