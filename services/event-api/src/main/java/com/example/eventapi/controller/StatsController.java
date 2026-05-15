package com.example.eventapi.controller;

import com.example.eventapi.dto.StatsResponse;
import com.example.eventapi.service.StatsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/stats")
public class StatsController {

    private final StatsService statsService;

    public StatsController(StatsService statsService) {
        this.statsService = statsService;
    }

    @GetMapping("/summary")
    public ResponseEntity<StatsResponse> getSummary() {
        return ResponseEntity.ok(statsService.getSummary());
    }
}
