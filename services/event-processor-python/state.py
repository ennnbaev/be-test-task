"""
In-memory tracking of events currently being processed.

Only "pending" state lives here. "Processed" state is derived from Minio
directly (via stat_object) so it survives processor restarts — the object
key {event_id}.xml is deterministic and always addressable.
"""

import threading

_lock = threading.Lock()
_pending: set[str] = set()


def mark_pending(event_id: str) -> None:
    with _lock:
        _pending.add(event_id)


def discard_pending(event_id: str) -> None:
    with _lock:
        _pending.discard(event_id)


def is_pending(event_id: str) -> bool:
    with _lock:
        return event_id in _pending
