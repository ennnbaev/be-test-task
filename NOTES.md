# Implementation Notes

## What was implemented

- **Java**: Baseline fixes — Flyway, package structure, error handling, env vars (phase 0)
- **Java**: Read API — `GET /events/{id}`, `GET /events` with filtering and pagination (phase 1)
- **Java**: Statistics — `GET /stats/summary` with Caffeine cache (phase 2)
- **Python**: Control Plane — `GET /health`, `GET /events/{id}/status` (phase 3)

## What was skipped

- **Java — Authentication**: JWT auth would be the natural next step; the Read and Stats endpoints are currently open. `POST /events` is also open by design per the task spec, but in production it should require authentication too — at minimum an API key or service-level token, otherwise any actor can flood the pipeline.
- **Python — Replay** (`POST /events/{id}/replay`): not implemented.
- **Python — Concurrent processing with ordering & graceful shutdown**: not implemented; the processor remains serial.

---

## Changes to the supplied baseline code

### Java (`event-api`)

| What changed | Why |
|---|---|
| `POST /events` now returns `201 Created` | `200 OK` is semantically wrong for resource creation; `201` signals that a new resource was produced and is now addressable. |
| Removed `throws JsonProcessingException` from controller | Propagating a checked serialization exception through the HTTP layer leaks internals; it is caught locally and wrapped in an `IllegalStateException` (which the global handler turns into a 500). |
| `spring.jpa.hibernate.ddl-auto=update` → `none` + Flyway | `ddl-auto=update` silently drops or skips columns it cannot reconcile, has no rollback story, and is not safe in multi-instance deployments. Flyway gives versioned, auditable, repeatable migrations. |
| Credentials moved to `${ENV_VAR:default}` in `application.properties` | Hard-coded secrets in source files are a security risk. Environment-variable injection keeps secrets out of the repo and allows different values per environment without code changes. |
| Added `spring-boot-starter-test`, Testcontainers, `spring-kafka-test` | The original `pom.xml` had zero test dependencies — the project could not be tested at all. |
| Added `spring-boot-starter-actuator` | Provides `/actuator/health` and `/actuator/metrics` with zero extra code — standard operational baseline for any Spring Boot service. |
| Added `spring-boot-starter-validation` | Enables Bean Validation (`@Valid`, `@NotNull`, etc.) for request parameters and bodies. |
| Added `GlobalExceptionHandler` | Without a centralized handler, Spring returns HTML error pages or inconsistent JSON shapes on 4xx/5xx. All errors now return `{"error":"...","code":"...","timestamp":"..."}`. |
| Added `type` column to `events` table | The Read API needs to filter by type and Statistics needs to group by type. Extracting it at write time into a dedicated indexed column avoids expensive `payload::jsonb->>'type'` expressions on every read. |
| `db/init.sql` updated to include `type` column and new indexes | Keeps the docker-compose seed schema consistent with what Flyway produces on a clean start. |

### Python (`event-processor`)

| What changed | Why |
|---|---|
| All config constants replaced with `os.getenv(...)` | Hard-coded `localhost` addresses break in any containerised or multi-host environment. |
| Minio object key changed from `uuid4().xml` to `{event_id}.xml` | The original code generated a random filename, permanently losing the link between a Kafka event and its Minio object. Using the event id as the key makes the object deterministically addressable, enables the `/status` endpoint, and makes re-processing idempotent (a second `put_object` with the same key overwrites the same object rather than creating a duplicate). |
| `print(...)` replaced with `logging` | `logging` supports log levels, timestamps, and structured output; `print` does not. |
| Added `try/except` around `process_message` | A single bad message no longer kills the consumer loop; the error is logged and the loop continues. |
| Added `try/finally: consumer.close()` | Ensures the consumer is properly closed and offsets are flushed when the process exits. |

---

## Design decisions and trade-offs

### `type` column vs. JSON path expression

Storing `type` as a dedicated column (extracted at write time) adds a small overhead on `POST /events` but makes all downstream queries O(log n) with a B-tree index instead of a full-table JSON scan. The trade-off is that `type` is now denormalised — if the payload's `type` field changes after ingestion (it shouldn't for an immutable event log), the column would be stale. For an append-only event log this is acceptable.

### Max page size: 100

A page of 100 events with typical payloads (~1–2 KB each) produces a response of roughly 100–200 KB — well within a comfortable HTTP response budget. Beyond 100 the latency/payload size grows linearly with no clear benefit; clients that need bulk export should use a dedicated batch endpoint or direct DB access.

### Statistics — query design and performance

`GET /stats/summary` runs four queries per cache miss, each hitting a dedicated index:

| Metric | Query type | Index used |
|---|---|---|
| `totalCount` | `COUNT(*)` | PK / index-only scan |
| `countByType` | JPQL `GROUP BY e.type` | `idx_events_type` |
| `last24hCount` | JPQL `COUNT WHERE createdAt >= :since` | `idx_events_created_at` |
| `top5TypesLast7Days` | native SQL `GROUP BY … ORDER BY count DESC LIMIT 5` | `idx_events_type_created_at` |

`LIMIT 5` is not part of standard JPQL, so the top-5 query uses `nativeQuery = true`. The composite index `(type, created_at)` covers both the `WHERE created_at >= :since` filter and the `GROUP BY type`, so PostgreSQL can resolve the query with an index scan and avoid a sequential read.

`totalCount` is exact, not approximate (`pg_class.reltuples`). The trade-off: for tables above ~100 M rows a plain `COUNT(*)` can take seconds even with an index-only scan. At that scale, the right fix is a pre-aggregated materialized view or an event counter table maintained by triggers — not an approximation.

**Caffeine cache (TTL 60 s):** `@Cacheable("stats-summary")` wraps the entire `getSummary()` call. The cache holds at most one entry (`maximumSize=1`) and expires after 60 seconds. This means stats may lag by up to one minute, which is acceptable for an aggregate dashboard. The cache is disabled in tests (`spring.cache.type=none` injected via `@DynamicPropertySource`) so each test sees live data from the DB.

### Read API — Specification vs JPQL

For `GET /events` filtering by `type`, `from`, `to` I chose Spring Data **JPA Specification** over JPQL with optional parameters. JPQL patterns like `WHERE (:type IS NULL OR e.type = :type)` confuse the query planner — it must choose an index plan before knowing whether the parameter is null, so it often defaults to a seq scan. `Specification` composes only the predicates that are actually needed, so PostgreSQL sees a clean query and can use the `idx_events_type` or `idx_events_type_created_at` index correctly.

Default sort order is `createdAt DESC` — newest events first, which matches the most common consumption pattern (tail the log).

### Max page size: 100

100 events × ~2 KB average payload = ~200 KB response — comfortable for HTTP. Clients that need bulk export should use a batch/export endpoint or direct DB access. Exceeding 100 returns `400 VALIDATION_ERROR` via `@Max(100)` on the controller parameter and `ConstraintViolationException` handling in `GlobalExceptionHandler`.

### Statistics performance

All aggregates in `GET /stats/summary` are computed via a single round-trip query that leverages the `idx_events_created_at` and `idx_events_type_created_at` indexes. On a table with millions of rows the 24 h and 7 day filters benefit from the `created_at` index; the `GROUP BY type` benefits from the type index. If the table grows to hundreds of millions of rows, the next step would be a pre-aggregated materialized view refreshed on a schedule.

### Minio key design (Python)

Using `{event_id}.xml` as the Minio object key provides:
1. **Idempotency** — replaying an event produces the same key, so `put_object` overwrites rather than duplicates.
2. **Addressability** — the status endpoint can check `stat_object(bucket, f"{id}.xml")` without maintaining a separate lookup table.
3. **Debuggability** — operators can find the XML for any event id without a mapping table.

The downside: if two Kafka messages carry the same event id (e.g., a replay), the second overwrites the first. This is intentional — the latest processed version wins.

---

### Python Control Plane — status endpoint design

The status endpoint resolves state in this order:

1. **Check Minio** (`stat_object(bucket, "{event_id}.xml")`) — if the object exists, the event is `"processed"`. Because the object key is deterministic (`{event_id}.xml`, fixed in phase 0), this check survives processor restarts. `processedAt` is taken from the object's `last_modified` timestamp in Minio.
2. **Check in-memory pending set** — if the event id is in the set, return `"pending"`. The set is populated by `state.mark_pending()` before writing to Minio and cleared by `state.discard_pending()` in a `finally` block.
3. **Neither** — return `404`. The processor has never seen this event id.

Trade-off: in-memory pending state is lost on restart. After a restart, a message that was mid-flight at crash time will briefly appear as `404` until it is reprocessed (at-least-once delivery). This is acceptable because:
- The Kafka consumer uses `auto.offset.reset=earliest` so in-flight messages are retried after restart.
- Once reprocessing completes, Minio becomes the source of truth again.

An alternative would be a Redis-backed or Postgres-backed pending state, but that adds infrastructure. For this task the in-memory approach is the right trade-off.

### Python Control Plane — health check

`GET /health` creates a temporary `AdminClient` (confluent-kafka) per request and calls `list_topics(timeout=3)`. This verifies broker reachability, not just that the client is configured. The Minio check calls `bucket_exists()`, which requires a real round-trip. A "the process is alive" signal (just returning 200) is explicitly not enough per the task spec.

### Python — threading model

The FastAPI/uvicorn server runs in a daemon thread alongside the Kafka consumer loop in the main thread. They share only the `state` module, which is protected by a `threading.Lock`. This is safe because:
- `state` operations are short critical sections (set add/discard/contains).
- The Minio client is thread-safe for independent requests.
- The Kafka consumer is used only from the main thread.

## How to run and test

### Start infrastructure

```bash
cd dev
docker compose up -d
```

### Run event-api

```bash
cd services/event-api
./mvnw spring-boot:run
```

Run Java integration tests (requires Docker for Testcontainers):

```bash
cd services/event-api
./mvnw test
```

### Run event-processor

```bash
cd services/event-processor-python
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
python main.py
# Control-plane API starts on http://localhost:8081
```

Run Python unit tests (no Docker needed — clients are mocked):

```bash
cd services/event-processor-python
pytest tests/ -v
```

### Smoke test

```bash
# Create an event
curl -X POST http://localhost:8080/events \
  -H 'Content-Type: application/json' \
  -d '{"type":"user.signup","userId":"u-1","email":"alice@example.com"}'
# → 201 {"id":"<uuid>","status":"RECEIVED"}

# Read it back
curl http://localhost:8080/events/<uuid>
# → 200 {"id":"...","payload":{"type":"user.signup",...},"status":"RECEIVED","createdAt":"..."}

# List with filter
curl "http://localhost:8080/events?type=user.signup&size=5"

# Stats
curl http://localhost:8080/stats/summary

# Processor health
curl http://localhost:8081/health
# → 200 {"status":"ok"}  or  503 {"status":"unhealthy","errors":[...]}

# Processing status
curl http://localhost:8081/events/<uuid>/status
# → {"status":"processed","objectKey":"<uuid>.xml","processedAt":"..."}
```

---

## What I would change with more time

- **Authentication**: add JWT bearer auth protecting `GET /events*` and `GET /stats/*`, with seeded USER/ADMIN accounts.
- **Transactional outbox**: the current `POST /events` saves to Postgres and then publishes to Kafka in two separate operations. If the process crashes between the two, the event is in the DB but never reaches Kafka. The outbox pattern (write a pending outbox row in the same transaction, relay it to Kafka asynchronously) eliminates this race.
- **Metrics**: expose event ingestion rate, processing lag, and Minio write latency via Micrometer/Prometheus.
- **Python concurrent processing**: implement the per-key ordered concurrent worker pool described in the task.
- **Distributed tracing**: propagate a correlation id (e.g., W3C `traceparent`) from the HTTP request through Kafka and into Minio object metadata.
