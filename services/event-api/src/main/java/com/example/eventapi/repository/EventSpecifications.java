package com.example.eventapi.repository;

import com.example.eventapi.entity.Event;
import org.springframework.data.jpa.domain.Specification;

import java.time.Instant;

public class EventSpecifications {

    private EventSpecifications() {}

    public static Specification<Event> hasType(String type) {
        return (root, query, cb) -> cb.equal(root.get("type"), type);
    }

    public static Specification<Event> createdAfter(Instant from) {
        return (root, query, cb) -> cb.greaterThanOrEqualTo(root.get("createdAt"), from);
    }

    public static Specification<Event> createdBefore(Instant to) {
        return (root, query, cb) -> cb.lessThanOrEqualTo(root.get("createdAt"), to);
    }
}
