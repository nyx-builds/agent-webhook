"""Tests for v1.0.0 features: Outbox, Event Replay Engine, and Schema Validation."""

from __future__ import annotations

import asyncio
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from agent_webhook.models import (
    OutboxEntry,
    OutboxStatus,
    ReplayBatch,
    ReplayBatchStatus,
    SchemaVersion,
    WebhookEndpoint,
    WebhookMethod,
)
from agent_webhook.outbox import OutboxEngine, _validate_json_schema
from agent_webhook.service import WebhookService
from agent_webhook.store_sqlite import SQLiteStore


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def tmp_db():
    """Temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield path
    Path(path).unlink(missing_ok=True)


@pytest.fixture
def service(tmp_db):
    """WebhookService backed by SQLite."""
    return WebhookService(store_path=tmp_db)


@pytest.fixture
def store(tmp_db):
    """Direct SQLiteStore for low-level testing."""
    return SQLiteStore(tmp_db)


@pytest.fixture
def outbox_engine(store):
    """OutboxEngine with SQLite store."""
    return OutboxEngine(store)


@pytest.fixture
def endpoint(service):
    """Create a test endpoint."""
    ep = service.create_endpoint(
        name="Test Endpoint",
        url="https://example.com/webhook",
        tags=["test"],
    )
    return ep


# ── Model Tests ─────────────────────────────────────────────────────


class TestOutboxEntryModel:
    """Tests for the OutboxEntry model."""

    def test_outbox_entry_creation(self):
        entry = OutboxEntry(
            event_type="order.created",
            payload={"order_id": 123, "total": 99.99},
            endpoint_ids=["ep-1", "ep-2"],
            source="delivery",
        )
        assert entry.event_type == "order.created"
        assert entry.status == OutboxStatus.PENDING
        assert entry.source == "delivery"
        assert len(entry.endpoint_ids) == 2
        assert entry.success_count == 0
        assert entry.failure_count == 0
        assert entry.delivered_at is None

    def test_outbox_entry_defaults(self):
        entry = OutboxEntry(
            event_type="test.event",
            payload={"x": 1},
            endpoint_ids=["ep-1"],
        )
        assert entry.source == "delivery"
        assert entry.headers == {}
        assert entry.metadata == {}
        assert entry.validation_status == "not_checked"
        assert entry.validation_errors == []

    def test_is_replayable(self):
        """Entries that were delivered, failed, or partially delivered can be replayed."""
        for status in [OutboxStatus.DELIVERED, OutboxStatus.FAILED, OutboxStatus.PARTIAL]:
            entry = OutboxEntry(
                event_type="test.event",
                payload={},
                endpoint_ids=["ep-1"],
                status=status,
            )
            assert entry.is_replayable(), f"Status {status} should be replayable"

        for status in [OutboxStatus.PENDING, OutboxStatus.SKIPPED]:
            entry = OutboxEntry(
                event_type="test.event",
                payload={},
                endpoint_ids=["ep-1"],
                status=status,
            )
            assert not entry.is_replayable(), f"Status {status} should NOT be replayable"


class TestReplayBatchModel:
    """Tests for the ReplayBatch model."""

    def test_replay_batch_creation(self):
        now = datetime.now(timezone.utc)
        batch = ReplayBatch(
            start_time=now - timedelta(hours=1),
            end_time=now,
            event_type="order.created",
        )
        assert batch.status == ReplayBatchStatus.PENDING
        assert batch.total_entries == 0
        assert batch.processed == 0
        assert batch.progress_pct == 0.0

    def test_progress_pct(self):
        batch = ReplayBatch(total_entries=10, processed=5)
        assert batch.progress_pct == 50.0

        batch2 = ReplayBatch(total_entries=3, processed=1)
        assert batch2.progress_pct == 33.3

    def test_progress_pct_zero_entries(self):
        batch = ReplayBatch(total_entries=0, processed=0)
        assert batch.progress_pct == 0.0

    def test_replay_batch_statuses(self):
        for status in ReplayBatchStatus:
            batch = ReplayBatch(status=status)
            assert batch.status == status


class TestSchemaVersionModel:
    """Tests for the SchemaVersion model."""

    def test_schema_version_creation(self):
        sv = SchemaVersion(
            event_type="order.created",
            version="1.0",
            schema_def={
                "type": "object",
                "required": ["order_id"],
                "properties": {
                    "order_id": {"type": "integer"},
                    "total": {"type": "number"},
                },
            },
        )
        assert sv.event_type == "order.created"
        assert sv.version == "1.0"
        assert sv.active is True
        assert sv.strict is False
        assert sv.description is None

    def test_schema_version_with_alias(self):
        """SchemaVersion accepts 'schema' as alias for 'schema_def'."""
        sv = SchemaVersion(
            event_type="test",
            version="1.0",
            **{"schema": {"type": "object"}},
        )
        assert sv.schema_def == {"type": "object"}

    def test_schema_version_strict_mode(self):
        sv = SchemaVersion(
            event_type="test",
            version="2.0",
            schema_def={"type": "object"},
            strict=True,
            description="Strict schema for critical events",
        )
        assert sv.strict is True
        assert sv.description == "Strict schema for critical events"


# ── Store Layer Tests ───────────────────────────────────────────────


class TestOutboxStore:
    """Tests for the outbox store operations."""

    def test_add_and_get_outbox_entry(self, store):
        entry = OutboxEntry(
            event_type="order.created",
            payload={"order_id": 123},
            endpoint_ids=["ep-1", "ep-2"],
            source="delivery",
        )
        store.add_outbox_entry(entry)
        retrieved = store.get_outbox_entry(entry.id)
        assert retrieved is not None
        assert retrieved.event_type == "order.created"
        assert retrieved.payload == {"order_id": 123}
        assert retrieved.endpoint_ids == ["ep-1", "ep-2"]

    def test_list_outbox_no_filter(self, store):
        for i in range(5):
            entry = OutboxEntry(
                event_type=f"event.{i}",
                payload={"i": i},
                endpoint_ids=["ep-1"],
            )
            store.add_outbox_entry(entry)
        entries = store.list_outbox()
        assert len(entries) == 5

    def test_list_outbox_by_event_type(self, store):
        store.add_outbox_entry(OutboxEntry(
            event_type="order.created",
            payload={},
            endpoint_ids=["ep-1"],
        ))
        store.add_outbox_entry(OutboxEntry(
            event_type="order.updated",
            payload={},
            endpoint_ids=["ep-1"],
        ))
        entries = store.list_outbox(event_type="order.created")
        assert len(entries) == 1
        assert entries[0].event_type == "order.created"

    def test_list_outbox_by_status(self, store):
        store.add_outbox_entry(OutboxEntry(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1"],
            status=OutboxStatus.DELIVERED,
        ))
        store.add_outbox_entry(OutboxEntry(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1"],
            status=OutboxStatus.FAILED,
        ))
        delivered = store.list_outbox(status=OutboxStatus.DELIVERED)
        assert len(delivered) == 1
        assert delivered[0].status == OutboxStatus.DELIVERED

    def test_list_outbox_by_source(self, store):
        store.add_outbox_entry(OutboxEntry(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1"],
            source="schedule",
        ))
        store.add_outbox_entry(OutboxEntry(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1"],
            source="delivery",
        ))
        scheduled = store.list_outbox(source="schedule")
        assert len(scheduled) == 1
        assert scheduled[0].source == "schedule"

    def test_list_outbox_by_time_range(self, store):
        now = datetime.now(timezone.utc)
        old = now - timedelta(days=2)
        recent = now - timedelta(minutes=5)

        entry_old = OutboxEntry(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1"],
            created_at=old,
        )
        entry_new = OutboxEntry(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1"],
            created_at=recent,
        )
        store.add_outbox_entry(entry_old)
        store.add_outbox_entry(entry_new)

        # Filter for last day only
        entries = store.list_outbox(start_time=now - timedelta(days=1))
        assert len(entries) == 1
        assert entries[0].id == entry_new.id

    def test_list_outbox_by_endpoint(self, store):
        store.add_outbox_entry(OutboxEntry(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1", "ep-2"],
        ))
        store.add_outbox_entry(OutboxEntry(
            event_type="test",
            payload={},
            endpoint_ids=["ep-3"],
        ))
        entries = store.list_outbox(endpoint_id="ep-1")
        assert len(entries) == 1
        assert "ep-1" in entries[0].endpoint_ids

    def test_update_outbox_entry(self, store):
        entry = OutboxEntry(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1"],
        )
        store.add_outbox_entry(entry)

        updated = store.update_outbox_entry(
            entry.id,
            status=OutboxStatus.DELIVERED,
            success_count=1,
            failure_count=0,
            delivered_at=datetime.now(timezone.utc),
            delivery_ids=["del-1"],
        )
        assert updated is not None
        assert updated.status == OutboxStatus.DELIVERED
        assert updated.success_count == 1
        assert updated.delivery_ids == ["del-1"]
        assert updated.delivered_at is not None

    def test_outbox_count(self, store):
        store.add_outbox_entry(OutboxEntry(
            event_type="order.created",
            payload={},
            endpoint_ids=["ep-1"],
            status=OutboxStatus.DELIVERED,
        ))
        store.add_outbox_entry(OutboxEntry(
            event_type="order.created",
            payload={},
            endpoint_ids=["ep-1"],
            status=OutboxStatus.FAILED,
        ))
        assert store.outbox_count() == 2
        assert store.outbox_count(event_type="order.created") == 2
        assert store.outbox_count(status=OutboxStatus.DELIVERED) == 1
        assert store.outbox_count(status=OutboxStatus.FAILED) == 1

    def test_outbox_get_nonexistent(self, store):
        assert store.get_outbox_entry("nonexistent-id") is None


class TestReplayBatchStore:
    """Tests for replay batch store operations."""

    def test_add_and_get_replay_batch(self, store):
        batch = ReplayBatch(
            start_time=datetime.now(timezone.utc) - timedelta(hours=1),
            end_time=datetime.now(timezone.utc),
            event_type="order.created",
            total_entries=5,
        )
        store.add_replay_batch(batch)
        retrieved = store.get_replay_batch(batch.id)
        assert retrieved is not None
        assert retrieved.event_type == "order.created"
        assert retrieved.total_entries == 5
        assert retrieved.status == ReplayBatchStatus.PENDING

    def test_list_replay_batches(self, store):
        for i in range(3):
            store.add_replay_batch(ReplayBatch(total_entries=i))
        batches = store.list_replay_batches()
        assert len(batches) == 3

    def test_list_replay_batches_by_status(self, store):
        store.add_replay_batch(ReplayBatch(status=ReplayBatchStatus.COMPLETED))
        store.add_replay_batch(ReplayBatch(status=ReplayBatchStatus.PENDING))
        completed = store.list_replay_batches(status="completed")
        assert len(completed) == 1

    def test_update_replay_batch(self, store):
        batch = ReplayBatch(total_entries=10)
        store.add_replay_batch(batch)

        updated = store.update_replay_batch(
            batch.id,
            status=ReplayBatchStatus.COMPLETED,
            processed=10,
            succeeded=8,
            failed=2,
            completed_at=datetime.now(timezone.utc),
        )
        assert updated is not None
        assert updated.status == ReplayBatchStatus.COMPLETED
        assert updated.processed == 10
        assert updated.succeeded == 8
        assert updated.failed == 2


class TestSchemaVersionStore:
    """Tests for schema version store operations."""

    def test_add_and_get_schema(self, store):
        sv = SchemaVersion(
            event_type="order.created",
            version="1.0",
            schema_def={"type": "object", "required": ["order_id"]},
        )
        store.add_schema_version(sv)

        retrieved = store.get_schema_version("order.created")
        assert retrieved is not None
        assert retrieved.event_type == "order.created"
        assert retrieved.version == "1.0"

    def test_get_schema_by_version(self, store):
        sv1 = SchemaVersion(
            event_type="order.created",
            version="1.0",
            schema_def={"type": "object"},
        )
        sv2 = SchemaVersion(
            event_type="order.created",
            version="2.0",
            schema_def={"type": "object", "required": ["order_id"]},
        )
        store.add_schema_version(sv1)
        store.add_schema_version(sv2)

        retrieved = store.get_schema_version("order.created", "1.0")
        assert retrieved is not None
        assert retrieved.version == "1.0"

    def test_get_latest_active_schema(self, store):
        sv1 = SchemaVersion(
            event_type="order.created",
            version="1.0",
            schema_def={"type": "object"},
        )
        sv2 = SchemaVersion(
            event_type="order.created",
            version="2.0",
            schema_def={"type": "object", "required": ["id"]},
        )
        store.add_schema_version(sv1)
        store.add_schema_version(sv2)

        latest = store.get_schema_version("order.created")
        assert latest is not None
        # Latest active is sv2 (created after sv1)
        assert latest.version == "2.0"

    def test_list_schemas(self, store):
        store.add_schema_version(SchemaVersion(
            event_type="order.created", version="1.0", schema_def={"type": "object"}
        ))
        store.add_schema_version(SchemaVersion(
            event_type="order.updated", version="1.0", schema_def={"type": "object"}
        ))
        schemas = store.list_schema_versions()
        assert len(schemas) == 2

    def test_list_schemas_by_event_type(self, store):
        store.add_schema_version(SchemaVersion(
            event_type="order.created", version="1.0", schema_def={"type": "object"}
        ))
        store.add_schema_version(SchemaVersion(
            event_type="order.updated", version="1.0", schema_def={"type": "object"}
        ))
        schemas = store.list_schema_versions(event_type="order.created")
        assert len(schemas) == 1

    def test_update_schema(self, store):
        sv = SchemaVersion(
            event_type="test",
            version="1.0",
            schema_def={"type": "object"},
        )
        store.add_schema_version(sv)
        updated = store.update_schema_version(sv.id, active=False, strict=True)
        assert updated is not None
        assert updated.active is False
        assert updated.strict is True

    def test_delete_schema(self, store):
        sv = SchemaVersion(
            event_type="test",
            version="1.0",
            schema_def={"type": "object"},
        )
        store.add_schema_version(sv)
        assert store.delete_schema_version(sv.id) is True
        assert store.get_schema_version("test") is None

    def test_delete_nonexistent_schema(self, store):
        assert store.delete_schema_version("nonexistent") is False


# ── OutboxEngine Tests ──────────────────────────────────────────────


class TestOutboxEngine:
    """Tests for the OutboxEngine business logic."""

    def test_record_event(self, outbox_engine):
        entry = outbox_engine.record_event(
            event_type="order.created",
            payload={"order_id": 123},
            endpoint_ids=["ep-1"],
            source="delivery",
        )
        assert entry is not None
        assert entry.event_type == "order.created"
        assert entry.status == OutboxStatus.PENDING

    def test_finalize_entry_all_success(self, outbox_engine):
        entry = outbox_engine.record_event(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1", "ep-2"],
        )
        finalized = outbox_engine.finalize_entry(
            entry.id,
            delivery_ids=["d-1", "d-2"],
            success_count=2,
            failure_count=0,
        )
        assert finalized.status == OutboxStatus.DELIVERED
        assert finalized.success_count == 2

    def test_finalize_entry_all_failure(self, outbox_engine):
        entry = outbox_engine.record_event(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1", "ep-2"],
        )
        finalized = outbox_engine.finalize_entry(
            entry.id,
            delivery_ids=["d-1", "d-2"],
            success_count=0,
            failure_count=2,
        )
        assert finalized.status == OutboxStatus.FAILED

    def test_finalize_entry_partial(self, outbox_engine):
        entry = outbox_engine.record_event(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1", "ep-2"],
        )
        finalized = outbox_engine.finalize_entry(
            entry.id,
            delivery_ids=["d-1", "d-2"],
            success_count=1,
            failure_count=1,
        )
        assert finalized.status == OutboxStatus.PARTIAL

    def test_get_entry(self, outbox_engine):
        entry = outbox_engine.record_event(
            event_type="test",
            payload={"x": 1},
            endpoint_ids=["ep-1"],
        )
        retrieved = outbox_engine.get_entry(entry.id)
        assert retrieved is not None
        assert retrieved.id == entry.id

    def test_list_entries(self, outbox_engine):
        for i in range(3):
            outbox_engine.record_event(
                event_type=f"event.{i}",
                payload={"i": i},
                endpoint_ids=["ep-1"],
            )
        entries = outbox_engine.list_entries()
        assert len(entries) == 3

    def test_list_entries_filtered(self, outbox_engine):
        outbox_engine.record_event(
            event_type="order.created",
            payload={},
            endpoint_ids=["ep-1"],
        )
        outbox_engine.record_event(
            event_type="order.updated",
            payload={},
            endpoint_ids=["ep-1"],
        )
        entries = outbox_engine.list_entries(event_type="order.created")
        assert len(entries) == 1

    def test_outbox_stats(self, outbox_engine):
        # Record 5 entries
        for i in range(3):
            entry = outbox_engine.record_event(
                event_type="test",
                payload={},
                endpoint_ids=["ep-1"],
            )
            outbox_engine.finalize_entry(entry.id, ["d-1"], 1, 0)

        for i in range(2):
            entry = outbox_engine.record_event(
                event_type="test",
                payload={},
                endpoint_ids=["ep-1"],
            )
            outbox_engine.finalize_entry(entry.id, ["d-1"], 0, 1)

        stats = outbox_engine.outbox_stats()
        assert stats["enabled"] is True
        assert stats["total"] == 5
        assert stats["delivered"] == 3
        assert stats["failed"] == 2
        assert stats["delivery_rate_pct"] == 60.0


class TestReplayBatchEngine:
    """Tests for the replay batch engine operations."""

    def test_create_replay_batch(self, outbox_engine):
        # Record some events
        for i in range(5):
            outbox_engine.record_event(
                event_type="order.created",
                payload={"i": i},
                endpoint_ids=["ep-1"],
            )

        batch = outbox_engine.create_replay_batch(
            event_type="order.created",
        )
        assert batch is not None
        assert batch.total_entries == 5
        assert batch.status == ReplayBatchStatus.PENDING
        assert len(batch.replayed_entry_ids) == 5

    def test_create_replay_batch_empty(self, outbox_engine):
        batch = outbox_engine.create_replay_batch()
        assert batch is not None
        assert batch.total_entries == 0

    def test_create_replay_batch_with_time_range(self, outbox_engine):
        now = datetime.now(timezone.utc)

        # Record an old entry
        old_entry = outbox_engine.record_event(
            event_type="test",
            payload={"old": True},
            endpoint_ids=["ep-1"],
        )
        # Manually set its created_at to 2 days ago
        outbox_engine._store.update_outbox_entry(
            old_entry.id, created_at=now - timedelta(days=2)
        )

        # Record a recent entry
        new_entry = outbox_engine.record_event(
            event_type="test",
            payload={"new": True},
            endpoint_ids=["ep-1"],
        )

        # Replay only the last day
        batch = outbox_engine.create_replay_batch(
            start_time=now - timedelta(days=1),
        )
        assert batch.total_entries == 1

    @pytest.mark.asyncio
    async def test_execute_replay_batch_dry_run(self, outbox_engine):
        """Execute without delivery_callback is a dry-run."""
        for i in range(3):
            outbox_engine.record_event(
                event_type="test",
                payload={"i": i},
                endpoint_ids=["ep-1"],
            )

        batch = outbox_engine.create_replay_batch(event_type="test")
        result = await outbox_engine.execute_replay_batch(batch.id)

        assert result is not None
        assert result.status == ReplayBatchStatus.COMPLETED
        assert result.processed == 3
        assert result.succeeded == 3

    @pytest.mark.asyncio
    async def test_execute_replay_batch_with_callback(self, outbox_engine):
        delivered_payloads = []

        async def callback(entry):
            delivered_payloads.append(entry.payload)
            return []

        for i in range(3):
            outbox_engine.record_event(
                event_type="test",
                payload={"i": i},
                endpoint_ids=["ep-1"],
            )

        batch = outbox_engine.create_replay_batch(event_type="test")
        result = await outbox_engine.execute_replay_batch(batch.id, delivery_callback=callback)

        assert result.status == ReplayBatchStatus.COMPLETED
        assert len(delivered_payloads) == 3

    def test_cancel_replay_batch(self, outbox_engine):
        outbox_engine.record_event(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1"],
        )
        batch = outbox_engine.create_replay_batch(event_type="test")
        cancelled = outbox_engine.cancel_replay_batch(batch.id)
        assert cancelled is not None
        assert cancelled.status == ReplayBatchStatus.CANCELLED

    def test_list_replay_batches(self, outbox_engine):
        for i in range(3):
            outbox_engine.create_replay_batch()
        batches = outbox_engine.list_replay_batches()
        assert len(batches) == 3


# ── Schema Validation Engine Tests ─────────────────────────────────


class TestSchemaValidationEngine:
    """Tests for schema validation in the OutboxEngine."""

    def test_create_and_get_schema(self, outbox_engine):
        sv = outbox_engine.create_schema(
            event_type="order.created",
            version="1.0",
            schema={
                "type": "object",
                "required": ["order_id"],
                "properties": {
                    "order_id": {"type": "integer"},
                },
            },
        )
        assert sv is not None

        retrieved = outbox_engine.get_schema("order.created")
        assert retrieved is not None
        assert retrieved.version == "1.0"

    def test_validate_payload_valid(self, outbox_engine):
        outbox_engine.create_schema(
            event_type="order.created",
            version="1.0",
            schema={
                "type": "object",
                "required": ["order_id"],
                "properties": {
                    "order_id": {"type": "integer"},
                    "total": {"type": "number"},
                },
            },
        )

        valid, errors, version = outbox_engine.validate_payload(
            "order.created",
            {"order_id": 123, "total": 99.99},
        )
        assert valid is True
        assert errors == []
        assert version == "1.0"

    def test_validate_payload_missing_required(self, outbox_engine):
        outbox_engine.create_schema(
            event_type="order.created",
            version="1.0",
            schema={
                "type": "object",
                "required": ["order_id"],
                "properties": {
                    "order_id": {"type": "integer"},
                },
            },
        )

        valid, errors, version = outbox_engine.validate_payload(
            "order.created",
            {"total": 99.99},  # Missing order_id
        )
        assert valid is False
        assert len(errors) > 0
        assert any("order_id" in e for e in errors)

    def test_validate_payload_wrong_type(self, outbox_engine):
        outbox_engine.create_schema(
            event_type="order.created",
            version="1.0",
            schema={
                "type": "object",
                "properties": {
                    "order_id": {"type": "integer"},
                },
            },
        )

        valid, errors, version = outbox_engine.validate_payload(
            "order.created",
            {"order_id": "not-an-integer"},
        )
        assert valid is False
        assert len(errors) > 0

    def test_validate_payload_no_schema(self, outbox_engine):
        """When no schema exists, everything is valid."""
        valid, errors, version = outbox_engine.validate_payload(
            "nonexistent.event",
            {"anything": True},
        )
        assert valid is True
        assert errors == []
        assert version is None

    def test_list_schemas(self, outbox_engine):
        outbox_engine.create_schema(
            event_type="order.created",
            version="1.0",
            schema={"type": "object"},
        )
        outbox_engine.create_schema(
            event_type="order.updated",
            version="1.0",
            schema={"type": "object"},
        )
        schemas = outbox_engine.list_schemas()
        assert len(schemas) == 2

    def test_update_schema(self, outbox_engine):
        sv = outbox_engine.create_schema(
            event_type="test",
            version="1.0",
            schema={"type": "object"},
        )
        updated = outbox_engine.update_schema(sv.id, strict=True)
        assert updated is not None
        assert updated.strict is True

    def test_delete_schema(self, outbox_engine):
        sv = outbox_engine.create_schema(
            event_type="test",
            version="1.0",
            schema={"type": "object"},
        )
        assert outbox_engine.delete_schema(sv.id) is True
        assert outbox_engine.get_schema("test") is None

    def test_multiple_versions_coexist(self, outbox_engine):
        """Multiple schema versions can coexist for the same event type."""
        outbox_engine.create_schema(
            event_type="order.created",
            version="1.0",
            schema={
                "type": "object",
                "required": ["order_id"],
                "properties": {"order_id": {"type": "integer"}},
            },
        )
        outbox_engine.create_schema(
            event_type="order.created",
            version="2.0",
            schema={
                "type": "object",
                "required": ["order_id", "currency"],
                "properties": {
                    "order_id": {"type": "integer"},
                    "currency": {"type": "string"},
                },
            },
        )
        schemas = outbox_engine.list_schemas(event_type="order.created")
        assert len(schemas) == 2

        # Latest active is 2.0
        latest = outbox_engine.get_schema("order.created")
        assert latest.version == "2.0"


# ── Validation Function Tests ───────────────────────────────────────


class TestValidateJsonSchema:
    """Tests for the _validate_json_schema utility function."""

    def test_valid_payload(self):
        errors = _validate_json_schema(
            {"name": "test", "age": 25},
            {
                "type": "object",
                "required": ["name"],
                "properties": {
                    "name": {"type": "string"},
                    "age": {"type": "integer"},
                },
            },
        )
        assert errors == []

    def test_missing_required_field(self):
        errors = _validate_json_schema(
            {"age": 25},
            {
                "type": "object",
                "required": ["name"],
                "properties": {"name": {"type": "string"}},
            },
        )
        assert len(errors) > 0

    def test_wrong_type(self):
        errors = _validate_json_schema(
            {"name": 123},
            {
                "type": "object",
                "properties": {"name": {"type": "string"}},
            },
        )
        assert len(errors) > 0

    def test_empty_schema(self):
        """Empty schema validates everything."""
        errors = _validate_json_schema({"anything": True}, {})
        assert errors == []


# ── Service Layer Tests ─────────────────────────────────────────────


class TestOutboxService:
    """Tests for outbox features exposed through the WebhookService."""

    def test_service_has_outbox_property(self, service):
        assert hasattr(service, "outbox")
        assert service.outbox is not None

    def test_record_outbox_event(self, service, endpoint):
        entry = service.record_outbox_event(
            event_type="order.created",
            payload={"order_id": 123},
            endpoint_ids=[endpoint.id],
        )
        assert entry is not None
        assert entry.event_type == "order.created"

    def test_get_outbox_entry(self, service):
        entry = service.record_outbox_event(
            event_type="test",
            payload={"x": 1},
            endpoint_ids=["ep-1"],
        )
        retrieved = service.get_outbox_entry(entry.id)
        assert retrieved is not None
        assert retrieved.id == entry.id

    def test_list_outbox(self, service):
        for i in range(3):
            service.record_outbox_event(
                event_type=f"event.{i}",
                payload={},
                endpoint_ids=["ep-1"],
            )
        entries = service.list_outbox()
        assert len(entries) == 3

    def test_outbox_stats(self, service):
        stats = service.outbox_stats()
        assert stats["enabled"] is True
        assert stats["total"] == 0

    def test_finalize_outbox_entry(self, service):
        entry = service.record_outbox_event(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1", "ep-2"],
        )
        finalized = service.finalize_outbox_entry(
            entry.id,
            delivery_ids=["d-1", "d-2"],
            success_count=2,
            failure_count=0,
        )
        assert finalized.status == OutboxStatus.DELIVERED

    def test_create_replay_batch(self, service):
        service.record_outbox_event(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1"],
        )
        batch = service.create_replay_batch(event_type="test")
        assert batch is not None
        assert batch.total_entries == 1

    @pytest.mark.asyncio
    async def test_execute_replay_batch(self, service, endpoint):
        # Record events with actual endpoint
        for i in range(3):
            service.record_outbox_event(
                event_type="test",
                payload={"i": i},
                endpoint_ids=[endpoint.id],
            )

        batch = service.create_replay_batch(event_type="test")
        result = await service.execute_replay_batch(batch.id)
        assert result is not None
        assert result.status == ReplayBatchStatus.COMPLETED
        assert result.processed == 3

    def test_cancel_replay_batch(self, service):
        service.record_outbox_event(
            event_type="test",
            payload={},
            endpoint_ids=["ep-1"],
        )
        batch = service.create_replay_batch(event_type="test")
        cancelled = service.cancel_replay_batch(batch.id)
        assert cancelled is not None
        assert cancelled.status == ReplayBatchStatus.CANCELLED

    def test_get_replay_batch(self, service):
        batch = service.create_replay_batch()
        retrieved = service.get_replay_batch(batch.id)
        assert retrieved is not None

    def test_list_replay_batches(self, service):
        for _ in range(3):
            service.create_replay_batch()
        batches = service.list_replay_batches()
        assert len(batches) == 3


class TestSchemaService:
    """Tests for schema validation through the WebhookService."""

    def test_create_schema(self, service):
        sv = service.create_schema(
            event_type="order.created",
            version="1.0",
            schema={
                "type": "object",
                "required": ["order_id"],
                "properties": {"order_id": {"type": "integer"}},
            },
        )
        assert sv is not None
        assert sv.event_type == "order.created"

    def test_get_schema(self, service):
        service.create_schema(
            event_type="order.created",
            version="1.0",
            schema={"type": "object"},
        )
        sv = service.get_schema("order.created")
        assert sv is not None

    def test_list_schemas(self, service):
        service.create_schema("event.a", "1.0", {"type": "object"})
        service.create_schema("event.b", "1.0", {"type": "object"})
        schemas = service.list_schemas()
        assert len(schemas) == 2

    def test_validate_payload_valid(self, service):
        service.create_schema(
            "order.created",
            "1.0",
            {
                "type": "object",
                "required": ["order_id"],
                "properties": {"order_id": {"type": "integer"}},
            },
        )
        result = service.validate_payload("order.created", {"order_id": 123})
        assert result["valid"] is True
        assert result["errors"] == []
        assert result["schema_version"] == "1.0"

    def test_validate_payload_invalid(self, service):
        service.create_schema(
            "order.created",
            "1.0",
            {
                "type": "object",
                "required": ["order_id"],
                "properties": {"order_id": {"type": "integer"}},
            },
        )
        result = service.validate_payload("order.created", {"total": 99.99})
        assert result["valid"] is False
        assert len(result["errors"]) > 0

    def test_validate_payload_no_schema(self, service):
        result = service.validate_payload("no.schema", {"anything": True})
        assert result["valid"] is True
        assert result["schema_version"] is None

    def test_delete_schema(self, service):
        sv = service.create_schema("test", "1.0", {"type": "object"})
        assert service.delete_schema(sv.id) is True
        assert service.get_schema("test") is None


# ── Integration Tests ───────────────────────────────────────────────


class TestOutboxIntegration:
    """End-to-end integration tests for the outbox flow."""

    @pytest.mark.asyncio
    async def test_full_outbox_lifecycle(self, service, endpoint):
        """Test the full outbox lifecycle: record → finalize → replay."""
        # 1. Record events
        entries = []
        for i in range(5):
            entry = service.record_outbox_event(
                event_type="order.created",
                payload={"order_id": i},
                endpoint_ids=[endpoint.id],
            )
            entries.append(entry)

        # 2. Finalize some as delivered, some as failed
        for i, entry in enumerate(entries):
            service.finalize_outbox_entry(
                entry.id,
                delivery_ids=[f"d-{i}"],
                success_count=1 if i < 3 else 0,
                failure_count=0 if i < 3 else 1,
            )

        # 3. Verify stats
        stats = service.outbox_stats()
        assert stats["total"] == 5
        assert stats["delivered"] == 3
        assert stats["failed"] == 2

        # 4. Create and execute replay for failed entries
        batch = service.create_replay_batch(
            event_type="order.created",
        )
        assert batch.total_entries == 5

        result = await service.execute_replay_batch(batch.id)
        assert result.status == ReplayBatchStatus.COMPLETED
        assert result.processed == 5

    @pytest.mark.asyncio
    async def test_replay_with_target_override(self, service):
        """Replay events to a different endpoint than the original."""
        # Create two endpoints
        ep1 = service.create_endpoint("Original", "https://example.com/orig")
        ep2 = service.create_endpoint("New Target", "https://example.com/new")

        # Record events for ep1
        for i in range(3):
            service.record_outbox_event(
                event_type="test",
                payload={"i": i},
                endpoint_ids=[ep1.id],
            )

        # Replay to ep2 instead
        batch = service.create_replay_batch(
            event_type="test",
            target_endpoint_ids=[ep2.id],
        )
        result = await service.execute_replay_batch(batch.id)
        assert result.status == ReplayBatchStatus.COMPLETED
        # All 3 entries should have been replayed to ep2
        assert result.succeeded == 3

    def test_outbox_with_json_store_fallback(self):
        """Outbox gracefully degrades on JSON store (no SQLite)."""
        from agent_webhook.store import WebhookStore

        path = tempfile.mktemp(suffix=".json")
        try:
            json_store = WebhookStore(path)
            engine = OutboxEngine(json_store)

            # All operations should return None/empty gracefully
            assert engine.record_event("test", {}, ["ep-1"]) is None
            assert engine.get_entry("x") is None
            assert engine.list_entries() == []

            stats = engine.outbox_stats()
            assert stats["enabled"] is False
        finally:
            Path(path).unlink(missing_ok=True)

    @pytest.mark.asyncio
    async def test_replay_idempotency(self, outbox_engine):
        """Replaying a completed batch returns the same result."""
        for i in range(3):
            outbox_engine.record_event(
                event_type="test",
                payload={"i": i},
                endpoint_ids=["ep-1"],
            )

        batch = outbox_engine.create_replay_batch(event_type="test")

        # Execute once
        result1 = await outbox_engine.execute_replay_batch(batch.id)
        assert result1.status == ReplayBatchStatus.COMPLETED

        # Execute again — should return same batch without re-processing
        result2 = await outbox_engine.execute_replay_batch(batch.id)
        assert result2.status == ReplayBatchStatus.COMPLETED
        assert result2.processed == result1.processed


# ── REST API Tests ──────────────────────────────────────────────────


class TestOutboxRestAPI:
    """Tests for the outbox REST API endpoints."""

    @pytest.fixture
    def client(self, tmp_db):
        from fastapi.testclient import TestClient
        from agent_webhook.rest_api import create_app

        app = create_app(store_path=tmp_db)
        return TestClient(app)

    def test_outbox_stats_empty(self, client):
        resp = client.get("/outbox/stats")
        assert resp.status_code == 200
        data = resp.json()
        assert data["enabled"] is True
        assert data["total"] == 0

    def test_outbox_list_empty(self, client):
        resp = client.get("/outbox")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 0

    def test_outbox_get_not_found(self, client):
        resp = client.get("/outbox/nonexistent-id")
        assert resp.status_code == 404

    def test_schema_crud_via_api(self, client):
        """Create, get, list, validate, and delete a schema via REST."""
        # Create
        resp = client.post("/schemas", json={
            "event_type": "order.created",
            "version": "1.0",
            "schema": {
                "type": "object",
                "required": ["order_id"],
                "properties": {"order_id": {"type": "integer"}},
            },
        })
        assert resp.status_code == 200
        sv_id = resp.json()["id"]

        # Get
        resp = client.get("/schemas/order.created")
        assert resp.status_code == 200

        # List
        resp = client.get("/schemas")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

        # Validate
        resp = client.post("/schemas/validate", json={
            "event_type": "order.created",
            "payload": {"order_id": 123},
        })
        assert resp.status_code == 200
        assert resp.json()["valid"] is True

        # Delete
        resp = client.delete(f"/schemas/{sv_id}")
        assert resp.status_code == 200

    def test_replay_endpoints(self, client):
        """Test replay create, get, list, and cancel via REST."""
        # Create a replay batch (empty)
        resp = client.post("/replay/create")
        assert resp.status_code == 200
        batch_id = resp.json()["id"]

        # Get status
        resp = client.get(f"/replay/{batch_id}")
        assert resp.status_code == 200

        # List batches
        resp = client.get("/replay")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

        # Cancel
        resp = client.post(f"/replay/{batch_id}/cancel")
        assert resp.status_code == 200
        assert resp.json()["status"] == "cancelled"
