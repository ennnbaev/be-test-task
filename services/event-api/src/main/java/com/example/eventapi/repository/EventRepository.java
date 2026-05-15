package com.example.eventapi.repository;

import com.example.eventapi.entity.Event;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.JpaSpecificationExecutor;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

@Repository
public interface EventRepository extends JpaRepository<Event, UUID>, JpaSpecificationExecutor<Event> {

    @Query("SELECT e.type AS type, COUNT(e) AS count FROM Event e " +
           "WHERE e.type IS NOT NULL GROUP BY e.type ORDER BY COUNT(e) DESC")
    List<TypeCountProjection> countGroupedByType();

    @Query("SELECT COUNT(e) FROM Event e WHERE e.createdAt >= :since")
    long countCreatedAfter(@Param("since") Instant since);

    // LIMIT is not standard JPQL — native SQL gives us the exact plan
    // the composite index idx_events_type_created_at covers this query.
    @Query(value = """
            SELECT type, COUNT(*) AS count
            FROM events
            WHERE created_at >= :since AND type IS NOT NULL
            GROUP BY type
            ORDER BY count DESC
            LIMIT 5
            """, nativeQuery = true)
    List<TypeCountProjection> top5TypesSince(@Param("since") Instant since);
}
