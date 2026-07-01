"""Tests for v0.6.0 features: recurring schedules, bulk operations, dry-run simulation."""

from __future__ import annotations

import asyncio
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from agent_webhook.models import (
    ScheduleInterval,
    WebhookSchedule,
    DeliveryStatus,
    WebhookStatus,
)
from agent_webhook.service import WebhookService
from agent_webhook.store import WebhookStore
from agent_webhook.store_sqlite import SQLiteStore


# ── ScheduleInterval & WebhookSchedule model tests ────────────────────


class TestWebhookScheduleModel:
    """Tests for the WebhookSchedule and ScheduleInterval models."""

    def test_schedule_defaults(self):
        """Schedule has sensible defaults."""
        s = WebhookSchedule(
            name="test",
            endpoint_id="ep1",
            payload={"msg": "hello"},
            interval_value=5,
        )
        assert s.interval_unit == ScheduleInterval.MINUTES
        assert s.active is True
        assert s.max_runs == 0
        assert s.run_count == 0
        assert s.last_run_at is None
        assert s.id  # auto-generated

    def test_interval_seconds(self):
        """interval_seconds property converts correctly."""
        assert WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=30, interval_unit=ScheduleInterval.SECONDS,
        ).interval_seconds == 30.0

        assert WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=5, interval_unit=ScheduleInterval.MINUTES,
        ).interval_seconds == 300.0

        assert WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=2, interval_unit=ScheduleInterval.HOURS,
        ).interval_seconds == 7200.0

        assert WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=1, interval_unit=ScheduleInterval.DAYS,
        ).interval_seconds == 86400.0

    def test_is_due_active_and_time(self):
        """is_due returns True when active and time has passed."""
        past = datetime.now(timezone.utc) - timedelta(minutes=5)
        s = WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=5, next_run_at=past,
        )
        assert s.is_due() is True

    def test_is_due_not_active(self):
        """is_due returns False when inactive."""
        now = datetime.now(timezone.utc)
        s = WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=5, active=False, next_run_at=now,
        )
        assert s.is_due() is False

    def test_is_due_future(self):
        """is_due returns False when next_run_at is in the future."""
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        s = WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=5, next_run_at=future,
        )
        assert s.is_due() is False

    def test_is_due_exhausted(self):
        """is_due returns False when max_runs is reached."""
        s = WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=5, max_runs=3, run_count=3,
        )
        assert s.is_due() is False
        assert s.is_exhausted() is True

    def test_is_due_max_runs_not_reached(self):
        """is_due returns True when max_runs not yet reached."""
        s = WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=5, max_runs=3, run_count=2,
        )
        assert s.is_due() is True
        assert s.is_exhausted() is False

    def test_is_due_naive_datetime(self):
        """is_due handles naive datetimes by assuming UTC."""
        past_naive = datetime.now(timezone.utc).replace(tzinfo=None) - timedelta(minutes=5)
        s = WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=5, next_run_at=past_naive,
        )
        assert s.is_due() is True

    def test_compute_next_run(self):
        """compute_next_run adds the interval."""
        now = datetime.now(timezone.utc)
        s = WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=10, interval_unit=ScheduleInterval.MINUTES,
        )
        nxt = s.compute_next_run(now)
        assert nxt == now + timedelta(minutes=10)

    def test_compute_next_run_naive(self):
        """compute_next_run handles naive datetimes."""
        naive = datetime(2026, 1, 1, 12, 0, 0)
        s = WebhookSchedule(
            name="t", endpoint_id="e", payload={},
            interval_value=1, interval_unit=ScheduleInterval.HOURS,
        )
        nxt = s.compute_next_run(naive)
        assert nxt.tzinfo is not None  # Should have UTC tzinfo
        assert nxt.hour == 13

    def test_schedule_serialization(self):
        """Schedule round-trips through JSON."""
        s = WebhookSchedule(
            name="heartbeat",
            endpoint_id="ep123",
            payload={"ping": True},
            interval_value=30,
            interval_unit=ScheduleInterval.SECONDS,
        )
        data = s.model_dump(mode="json")
        restored = WebhookSchedule.model_validate(data)
        assert restored.name == "heartbeat"
        assert restored.interval_unit == ScheduleInterval.SECONDS
        assert restored.interval_seconds == 30.0

    def test_invalid_interval_value(self):
        """interval_value must be >= 1."""
        with pytest.raises(Exception):
            WebhookSchedule(
                name="t", endpoint_id="e", payload={},
                interval_value=0,
            )


# ── JSON Store schedule tests ─────────────────────────────────────────


class TestScheduleStoreJSON:
    """Tests for schedule CRUD in the JSON store."""

    @pytest.fixture
    def store(self, tmp_path):
        return WebhookStore(tmp_path / "test.json")

    def test_add_and_get_schedule(self, store):
        s = WebhookSchedule(
            name="test", endpoint_id="ep1", payload={"x": 1},
            interval_value=5,
        )
        store.add_schedule(s)
        got = store.get_schedule(s.id)
        assert got is not None
        assert got.name == "test"
        assert got.endpoint_id == "ep1"

    def test_get_nonexistent_schedule(self, store):
        assert store.get_schedule("nonexistent") is None

    def test_list_schedules(self, store):
        for i in range(3):
            store.add_schedule(WebhookSchedule(
                name=f"s{i}", endpoint_id="ep1", payload={},
                interval_value=5,
            ))
        assert len(store.list_schedules()) == 3

    def test_list_schedules_by_endpoint(self, store):
        store.add_schedule(WebhookSchedule(
            name="a", endpoint_id="ep1", payload={}, interval_value=5,
        ))
        store.add_schedule(WebhookSchedule(
            name="b", endpoint_id="ep2", payload={}, interval_value=5,
        ))
        assert len(store.list_schedules(endpoint_id="ep1")) == 1
        assert len(store.list_schedules(endpoint_id="ep2")) == 1

    def test_list_schedules_active_only(self, store):
        store.add_schedule(WebhookSchedule(
            name="active", endpoint_id="ep1", payload={}, interval_value=5,
            active=True,
        ))
        store.add_schedule(WebhookSchedule(
            name="inactive", endpoint_id="ep1", payload={}, interval_value=5,
            active=False,
        ))
        active = store.list_schedules(active_only=True)
        assert len(active) == 1
        assert active[0].name == "active"

    def test_update_schedule(self, store):
        s = WebhookSchedule(
            name="test", endpoint_id="ep1", payload={},
            interval_value=5,
        )
        store.add_schedule(s)
        updated = store.update_schedule(s.id, active=False, run_count=3)
        assert updated is not None
        assert updated.active is False
        assert updated.run_count == 3

    def test_update_nonexistent_schedule(self, store):
        assert store.update_schedule("nope", active=False) is None

    def test_delete_schedule(self, store):
        s = WebhookSchedule(
            name="test", endpoint_id="ep1", payload={}, interval_value=5,
        )
        store.add_schedule(s)
        assert store.delete_schedule(s.id) is True
        assert store.get_schedule(s.id) is None

    def test_delete_nonexistent_schedule(self, store):
        assert store.delete_schedule("nope") is False

    def test_due_schedules(self, store):
        past = datetime.now(timezone.utc) - timedelta(minutes=5)
        future = datetime.now(timezone.utc) + timedelta(hours=1)

        store.add_schedule(WebhookSchedule(
            name="due", endpoint_id="ep1", payload={}, interval_value=5,
            next_run_at=past,
        ))
        store.add_schedule(WebhookSchedule(
            name="not_due", endpoint_id="ep1", payload={}, interval_value=5,
            next_run_at=future,
        ))
        due = store.due_schedules()
        assert len(due) == 1
        assert due[0].name == "due"

    def test_due_schedules_excludes_inactive(self, store):
        past = datetime.now(timezone.utc) - timedelta(minutes=5)
        store.add_schedule(WebhookSchedule(
            name="inactive", endpoint_id="ep1", payload={}, interval_value=5,
            next_run_at=past, active=False,
        ))
        assert len(store.due_schedules()) == 0


# ── SQLite Store schedule tests ───────────────────────────────────────


class TestScheduleStoreSQLite:
    """Tests for schedule CRUD in the SQLite store."""

    @pytest.fixture
    def store(self, tmp_path):
        return SQLiteStore(tmp_path / "test.db")

    def test_add_and_get_schedule(self, store):
        s = WebhookSchedule(
            name="test", endpoint_id="ep1", payload={"x": 1},
            interval_value=5,
        )
        store.add_schedule(s)
        got = store.get_schedule(s.id)
        assert got is not None
        assert got.name == "test"
        assert got.payload == {"x": 1}

    def test_list_schedules_sqlite(self, store):
        for i in range(3):
            store.add_schedule(WebhookSchedule(
                name=f"s{i}", endpoint_id="ep1", payload={},
                interval_value=5,
            ))
        assert len(store.list_schedules()) == 3

    def test_list_schedules_by_endpoint_sqlite(self, store):
        store.add_schedule(WebhookSchedule(
            name="a", endpoint_id="ep1", payload={}, interval_value=5,
        ))
        store.add_schedule(WebhookSchedule(
            name="b", endpoint_id="ep2", payload={}, interval_value=5,
        ))
        assert len(store.list_schedules(endpoint_id="ep1")) == 1

    def test_list_schedules_active_only_sqlite(self, store):
        store.add_schedule(WebhookSchedule(
            name="active", endpoint_id="ep1", payload={}, interval_value=5,
        ))
        store.add_schedule(WebhookSchedule(
            name="inactive", endpoint_id="ep1", payload={}, interval_value=5,
            active=False,
        ))
        active = store.list_schedules(active_only=True)
        assert len(active) == 1
        assert active[0].name == "active"

    def test_update_schedule_sqlite(self, store):
        s = WebhookSchedule(
            name="test", endpoint_id="ep1", payload={}, interval_value=5,
        )
        store.add_schedule(s)
        updated = store.update_schedule(s.id, run_count=5, active=False)
        assert updated is not None
        assert updated.run_count == 5
        assert updated.active is False

    def test_delete_schedule_sqlite(self, store):
        s = WebhookSchedule(
            name="test", endpoint_id="ep1", payload={}, interval_value=5,
        )
        store.add_schedule(s)
        assert store.delete_schedule(s.id) is True
        assert store.get_schedule(s.id) is None

    def test_due_schedules_sqlite(self, store):
        past = datetime.now(timezone.utc) - timedelta(minutes=5)
        store.add_schedule(WebhookSchedule(
            name="due", endpoint_id="ep1", payload={}, interval_value=5,
            next_run_at=past,
        ))
        due = store.due_schedules()
        assert len(due) == 1


# ── Service schedule tests ────────────────────────────────────────────


class TestScheduleService:
    """Tests for schedule operations via WebhookService."""

    @pytest.fixture
    def service(self, tmp_path):
        return WebhookService(store_path=str(tmp_path / "test.json"))

    def test_create_schedule(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        schedule = service.create_schedule(
            name="heartbeat",
            endpoint_id=ep.id,
            payload={"ping": True},
            interval_value=30,
            interval_unit="seconds",
        )
        assert schedule is not None
        assert schedule.name == "heartbeat"
        assert schedule.endpoint_id == ep.id
        assert schedule.interval_seconds == 30.0

    def test_create_schedule_nonexistent_endpoint(self, service):
        result = service.create_schedule(
            name="test", endpoint_id="nonexistent",
            payload={}, interval_value=5,
        )
        assert result is None

    def test_pause_and_resume_schedule(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        schedule = service.create_schedule(
            name="test", endpoint_id=ep.id,
            payload={}, interval_value=5,
        )
        paused = service.pause_schedule(schedule.id)
        assert paused is not None
        assert paused.active is False

        resumed = service.resume_schedule(schedule.id)
        assert resumed is not None
        assert resumed.active is True

    def test_pause_nonexistent_schedule(self, service):
        assert service.pause_schedule("nonexistent") is None

    def test_delete_schedule(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        schedule = service.create_schedule(
            name="test", endpoint_id=ep.id,
            payload={}, interval_value=5,
        )
        assert service.delete_schedule(schedule.id) is True
        assert service.get_schedule(schedule.id) is None

    def test_list_schedules(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        for i in range(3):
            service.create_schedule(
                name=f"s{i}", endpoint_id=ep.id,
                payload={}, interval_value=5,
            )
        assert len(service.list_schedules()) == 3

    def test_create_schedule_with_start_at(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        future = datetime.now(timezone.utc) + timedelta(hours=2)
        schedule = service.create_schedule(
            name="delayed", endpoint_id=ep.id,
            payload={}, interval_value=10,
            start_at=future,
        )
        assert schedule is not None
        assert schedule.next_run_at >= future - timedelta(seconds=1)

    def test_create_schedule_naive_start_at(self, service):
        """Naive datetime for start_at should be treated as UTC."""
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        naive = datetime(2026, 12, 25, 10, 0, 0)
        schedule = service.create_schedule(
            name="xmas", endpoint_id=ep.id,
            payload={}, interval_value=10,
            start_at=naive,
        )
        assert schedule is not None
        assert schedule.next_run_at.tzinfo is not None


# ── Schedule processing tests ─────────────────────────────────────────


class TestScheduleProcessing:
    """Tests for the process_due_schedules service method."""

    @pytest.fixture
    def service(self, tmp_path):
        return WebhookService(store_path=str(tmp_path / "test.json"))

    def test_process_due_creates_delivery(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        service.create_schedule(
            name="heartbeat",
            endpoint_id=ep.id,
            payload={"ping": True},
            interval_value=5,
        )
        deliveries = asyncio.new_event_loop().run_until_complete(
            service.process_due_schedules()
        )
        assert len(deliveries) == 1
        assert deliveries[0].payload == {"ping": True}
        assert deliveries[0].status == DeliveryStatus.PENDING
        assert "schedule_id" in deliveries[0].metadata

    def test_process_due_advances_run_count(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        schedule = service.create_schedule(
            name="heartbeat",
            endpoint_id=ep.id,
            payload={},
            interval_value=5,
        )
        asyncio.new_event_loop().run_until_complete(
            service.process_due_schedules()
        )
        updated = service.get_schedule(schedule.id)
        assert updated.run_count == 1
        assert updated.last_run_at is not None
        assert updated.last_delivery_id is not None
        assert updated.next_run_at > datetime.now(timezone.utc)

    def test_process_due_max_runs_deactivates(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        schedule = service.create_schedule(
            name="once",
            endpoint_id=ep.id,
            payload={},
            interval_value=5,
            max_runs=1,
        )
        asyncio.new_event_loop().run_until_complete(
            service.process_due_schedules()
        )
        updated = service.get_schedule(schedule.id)
        assert updated.run_count == 1
        assert updated.active is False  # Deactivated after max_runs

    def test_process_due_skips_inactive(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        service.create_schedule(
            name="inactive",
            endpoint_id=ep.id,
            payload={},
            interval_value=5,
            # active defaults True, but we'll pause it
        )
        schedules = service.list_schedules()
        service.pause_schedule(schedules[0].id)

        deliveries = asyncio.new_event_loop().run_until_complete(
            service.process_due_schedules()
        )
        assert len(deliveries) == 0

    def test_process_due_no_schedules(self, service):
        deliveries = asyncio.new_event_loop().run_until_complete(
            service.process_due_schedules()
        )
        assert len(deliveries) == 0


# ── Bulk endpoint operations tests ────────────────────────────────────


class TestBulkOperations:
    """Tests for bulk pause/resume/disable/delete."""

    @pytest.fixture
    def service(self, tmp_path):
        return WebhookService(store_path=str(tmp_path / "test.json"))

    @pytest.fixture
    def tagged_endpoints(self, service):
        eps = {}
        eps["prod1"] = service.create_endpoint(name="prod1", url="https://p1.com/h", tags=["prod"])
        eps["prod2"] = service.create_endpoint(name="prod2", url="https://p2.com/h", tags=["prod"])
        eps["dev1"] = service.create_endpoint(name="dev1", url="https://d1.com/h", tags=["dev"])
        return eps

    def test_bulk_pause_by_ids(self, service, tagged_endpoints):
        result = service.bulk_pause(endpoint_ids=[tagged_endpoints["prod1"].id, tagged_endpoints["dev1"].id])
        assert len(result) == 2
        ep1 = service.get_endpoint(tagged_endpoints["prod1"].id)
        ep2 = service.get_endpoint(tagged_endpoints["dev1"].id)
        assert ep1.status == WebhookStatus.PAUSED
        assert ep2.status == WebhookStatus.PAUSED

    def test_bulk_pause_by_tag(self, service, tagged_endpoints):
        result = service.bulk_pause(tag="prod")
        assert len(result) == 2
        # Prod endpoints should be paused
        for key in ("prod1", "prod2"):
            ep = service.get_endpoint(tagged_endpoints[key].id)
            assert ep.status == WebhookStatus.PAUSED
        # Dev endpoint should NOT be paused
        dev = service.get_endpoint(tagged_endpoints["dev1"].id)
        assert dev.status == WebhookStatus.ACTIVE

    def test_bulk_resume(self, service, tagged_endpoints):
        service.bulk_pause(tag="prod")
        result = service.bulk_resume(tag="prod")
        assert len(result) == 2
        ep = service.get_endpoint(tagged_endpoints["prod1"].id)
        assert ep.status == WebhookStatus.ACTIVE

    def test_bulk_disable(self, service, tagged_endpoints):
        result = service.bulk_disable(tag="prod")
        assert len(result) == 2
        ep = service.get_endpoint(tagged_endpoints["prod1"].id)
        assert ep.status == WebhookStatus.DISABLED

    def test_bulk_delete(self, service, tagged_endpoints):
        result = service.bulk_delete(tag="dev")
        assert len(result) == 1
        assert service.get_endpoint(tagged_endpoints["dev1"].id) is None
        # Prod endpoints should still exist
        assert service.get_endpoint(tagged_endpoints["prod1"].id) is not None

    def test_bulk_no_filter_returns_empty(self, service, tagged_endpoints):
        """Bulk operation with no filter should return empty (safety)."""
        result = service.bulk_pause()
        assert result == []

    def test_bulk_resume_specific_ids(self, service, tagged_endpoints):
        service.bulk_pause(tag="prod")
        result = service.bulk_resume(endpoint_ids=[tagged_endpoints["prod1"].id])
        assert len(result) == 1
        assert service.get_endpoint(tagged_endpoints["prod1"].id).status == WebhookStatus.ACTIVE
        assert service.get_endpoint(tagged_endpoints["prod2"].id).status == WebhookStatus.PAUSED


# ── Dry-run simulation tests ──────────────────────────────────────────


class TestDryRunSimulation:
    """Tests for the simulate_delivery dry-run feature."""

    @pytest.fixture
    def service(self, tmp_path):
        return WebhookService(store_path=str(tmp_path / "test.json"))

    def test_simulate_basic(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        result = service.simulate_delivery(
            endpoint_id=ep.id,
            payload={"message": "hello"},
        )
        assert result["dry_run"] is True
        assert result["endpoint_id"] == ep.id
        assert result["url"] == "https://example.com/hook"
        assert result["method"] == "POST"
        assert result["original_payload"] == {"message": "hello"}
        assert result["payload_size_bytes"] > 0
        assert "Content-Type" in result["headers"]
        assert result["signature_present"] is False

    def test_simulate_nonexistent_endpoint(self, service):
        result = service.simulate_delivery(
            endpoint_id="nonexistent",
            payload={},
        )
        assert "error" in result

    def test_simulate_with_secret(self, service):
        ep = service.create_endpoint(
            name="secure",
            url="https://example.com/hook",
            secret="mysecret",
        )
        result = service.simulate_delivery(
            endpoint_id=ep.id,
            payload={"data": "sensitive"},
        )
        assert result["signature_present"] is True
        assert result["signature_preview"] is not None
        assert "X-Webhook-Signature" in result["headers"]

    def test_simulate_with_event_type(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        result = service.simulate_delivery(
            endpoint_id=ep.id,
            payload={"x": 1},
            event_type="order.created",
        )
        assert result["event_type"] == "order.created"
        assert result["headers"]["X-Webhook-Event"] == "order.created"

    def test_simulate_includes_retry_policy(self, service):
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        result = service.simulate_delivery(
            endpoint_id=ep.id,
            payload={},
        )
        assert "retry_policy" in result
        assert result["retry_policy"]["max_retries"] == 3
        assert "retry_on_status_codes" in result["retry_policy"]

    def test_simulate_includes_headers(self, service):
        ep = service.create_endpoint(
            name="test",
            url="https://example.com/hook",
            headers={"X-Custom": "value123"},
        )
        result = service.simulate_delivery(
            endpoint_id=ep.id,
            payload={},
        )
        assert result["headers"]["X-Custom"] == "value123"

    def test_simulate_no_http_request(self, service, tmp_path):
        """Verify no actual HTTP request is made — check no delivery was created."""
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        service.simulate_delivery(
            endpoint_id=ep.id,
            payload={"test": True},
        )
        # No deliveries should exist
        deliveries = service.list_deliveries()
        assert len(deliveries) == 0

    def test_simulate_with_rate_limit(self, service):
        ep = service.create_endpoint(
            name="test",
            url="https://example.com/hook",
            rate_limit={"max_requests": 10, "period": "minute"},
        )
        result = service.simulate_delivery(
            endpoint_id=ep.id,
            payload={},
        )
        assert result["rate_limit"] is not None
        assert result["rate_limit"]["max_requests"] == 10

    def test_simulate_paused_endpoint(self, service):
        """Simulation should work even on paused endpoints (for testing)."""
        ep = service.create_endpoint(name="test", url="https://example.com/hook")
        service.pause_endpoint(ep.id)
        result = service.simulate_delivery(
            endpoint_id=ep.id,
            payload={},
        )
        assert result["endpoint_status"] == "paused"
        assert result["dry_run"] is True


# ── Worker schedule integration tests ─────────────────────────────────


class TestWorkerScheduleIntegration:
    """Tests that the DeliveryWorker fires due schedules."""

    def test_worker_triggers_schedules(self, tmp_path):
        """Worker's _poll_once should fire due schedules and create deliveries."""
        from agent_webhook.worker import DeliveryWorker, WorkerConfig

        service = WebhookService(store_path=str(tmp_path / "test.json"))
        ep = service.create_endpoint(name="test", url="https://example.com/hook")

        # Create a schedule that's due now
        service.create_schedule(
            name="heartbeat",
            endpoint_id=ep.id,
            payload={"ping": True},
            interval_value=5,
        )

        worker = DeliveryWorker(service, WorkerConfig(poll_interval=0.1))
        loop = asyncio.new_event_loop()

        try:
            loop.run_until_complete(worker.start())
            # Trigger one poll cycle
            count = loop.run_until_complete(worker.trigger())
            # Should have processed 1 schedule (and tried delivery — may fail but delivery is created)
            assert count >= 1

            # Check the schedule was advanced
            schedules = service.list_schedules()
            assert schedules[0].run_count == 1
        finally:
            loop.run_until_complete(worker.stop())
            loop.close()
