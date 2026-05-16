"""
Control-plane HTTP API for event-processor.

Endpoints:
  GET /health               — liveness + real connectivity check for Kafka and Minio
  GET /events/{id}/status   — processing status for a given event id
"""

import os

from confluent_kafka.admin import AdminClient
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from minio import Minio
from minio.error import S3Error

import state

app = FastAPI(title="event-processor control plane")

_minio: Minio | None = None
_kafka_bootstrap: str | None = None
_bucket: str = os.getenv("MINIO_BUCKET", "events")


def init(minio_client: Minio, kafka_bootstrap: str) -> None:
    """Called once at startup to wire in shared clients."""
    global _minio, _kafka_bootstrap
    _minio = minio_client
    _kafka_bootstrap = kafka_bootstrap


@app.get("/health")
def health():
    """
    Returns 200 only when both Kafka and Minio are actually reachable.
    Returns 503 with a list of failures otherwise.
    """
    if _minio is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    errors: list[str] = []

    try:
        _minio.bucket_exists(_bucket)
    except Exception as exc:
        errors.append(f"minio: {exc}")

    try:
        admin = AdminClient({
            "bootstrap.servers": _kafka_bootstrap,
            "socket.timeout.ms": 3000,
        })
        try:
            admin.list_topics(timeout=3)
        finally:
            del admin
    except Exception as exc:
        errors.append(f"kafka: {exc}")

    if errors:
        return JSONResponse(
            status_code=503,
            content={"status": "unhealthy", "errors": errors},
        )
    return {"status": "ok"}


@app.get("/events/{event_id}/status")
def get_event_status(event_id: str):
    """
    Returns the processing status of an event.

    Resolution order:
      1. Check Minio for {event_id}.xml  → "processed" (survives restarts)
      2. Check in-memory pending set     → "pending"
      3. Neither                         → 404
    """
    if _minio is None:
        raise HTTPException(status_code=503, detail="Service not initialized")

    try:
        obj = _minio.stat_object(_bucket, f"{event_id}.xml")
        return {
            "status": "processed",
            "objectKey": f"{event_id}.xml",
            "processedAt": obj.last_modified.isoformat(),
        }
    except S3Error as exc:
        if exc.code != "NoSuchKey":
            raise HTTPException(status_code=503, detail="Storage check failed")

    if state.is_pending(event_id):
        return {"status": "pending"}

    raise HTTPException(
        status_code=404,
        detail=f"Event {event_id!r} has not been seen by the processor",
    )
