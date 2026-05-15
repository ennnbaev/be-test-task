"""
Unit tests for the control-plane API.

Minio and Kafka clients are mocked so no real infrastructure is required.
"""

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from minio.error import S3Error

import api
import state


@pytest.fixture(autouse=True)
def reset_state():
    """Ensure pending set is clean before every test."""
    with state._lock:
        state._pending.clear()
    yield
    with state._lock:
        state._pending.clear()


@pytest.fixture
def mock_minio():
    return MagicMock()


@pytest.fixture
def client(mock_minio):
    api.init(mock_minio, "localhost:9092")
    return TestClient(api.app)


# ── GET /health ───────────────────────────────────────────────────────────────

class TestHealth:

    def test_healthy_returns_200(self, client, mock_minio):
        mock_minio.bucket_exists.return_value = True

        with patch("api.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value.list_topics.return_value = {}
            response = client.get("/health")

        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_minio_down_returns_503(self, client, mock_minio):
        mock_minio.bucket_exists.side_effect = Exception("connection refused")

        with patch("api.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value.list_topics.return_value = {}
            response = client.get("/health")

        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "unhealthy"
        assert any("minio" in e for e in body["errors"])

    def test_kafka_down_returns_503(self, client, mock_minio):
        mock_minio.bucket_exists.return_value = True

        with patch("api.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value.list_topics.side_effect = Exception("broker unreachable")
            response = client.get("/health")

        assert response.status_code == 503
        body = response.json()
        assert body["status"] == "unhealthy"
        assert any("kafka" in e for e in body["errors"])

    def test_both_down_reports_two_errors(self, client, mock_minio):
        mock_minio.bucket_exists.side_effect = Exception("minio down")

        with patch("api.AdminClient") as mock_admin_cls:
            mock_admin_cls.return_value.list_topics.side_effect = Exception("kafka down")
            response = client.get("/health")

        assert response.status_code == 503
        assert len(response.json()["errors"]) == 2


# ── GET /events/{id}/status ───────────────────────────────────────────────────

class TestEventStatus:

    def test_unknown_event_returns_404(self, client, mock_minio):
        mock_minio.stat_object.side_effect = S3Error(
            "NoSuchKey", "Not found", "resource", "req-1", "host", MagicMock()
        )
        response = client.get("/events/unknown-id/status")
        assert response.status_code == 404

    def test_pending_event_returns_pending(self, client, mock_minio):
        mock_minio.stat_object.side_effect = S3Error(
            "NoSuchKey", "Not found", "resource", "req-1", "host", MagicMock()
        )
        state.mark_pending("evt-pending")

        response = client.get("/events/evt-pending/status")
        assert response.status_code == 200
        assert response.json()["status"] == "pending"

    def test_processed_event_returns_processed(self, client, mock_minio):
        processed_at = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_stat = MagicMock()
        mock_stat.last_modified = processed_at
        mock_minio.stat_object.return_value = mock_stat

        response = client.get("/events/evt-done/status")
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "processed"
        assert body["objectKey"] == "evt-done.xml"
        assert "processedAt" in body

    def test_processed_takes_priority_over_pending(self, client, mock_minio):
        """Minio check wins over in-memory pending — Minio is the source of truth."""
        processed_at = datetime(2024, 6, 1, 12, 0, 0, tzinfo=timezone.utc)
        mock_stat = MagicMock()
        mock_stat.last_modified = processed_at
        mock_minio.stat_object.return_value = mock_stat
        state.mark_pending("evt-race")

        response = client.get("/events/evt-race/status")
        assert response.status_code == 200
        assert response.json()["status"] == "processed"

    def test_minio_error_returns_503(self, client, mock_minio):
        mock_minio.stat_object.side_effect = S3Error(
            "InternalError", "Storage failure", "resource", "req-1", "host", MagicMock()
        )
        response = client.get("/events/evt-err/status")
        assert response.status_code == 503
