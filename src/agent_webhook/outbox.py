"""Outbox & Event Replay Engine — transactional outbox pattern for webhook delivery.

The outbox ensures every outbound webhook event is durably persisted BEFORE
delivery is attempted. This provides:

- **At-least-once delivery**: If the process crashes after recording the outbox
  entry but before delivery, the event can be recovered and replayed.
- **Event replay**: Re-deliver events from any time range to recover from
  downstream data loss, migrate to new endpoints, or debug issues.
- **Schema validation**: Optionally validate payloads against JSON Schema
  before delivery, with strict and non-strict modes.
- **Audit trail**: Every outbound event is durably recorded with its payload,
  targets, and outcome.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

from .models import (
    OutboxEntry,
    OutboxStatus,
    ReplayBatch,
    ReplayBatchStatus,
    SchemaVersion,
    WebhookDelivery,
)
from .store import WebhookStore


class OutboxEngine:
    """Business logic for the outbox pattern: recording, validation, and replay.

    The engine sits between the service layer and the store. When ``send_webhook``
    is called, the service creates an outbox entry via ``record_event()`` before
    delivery. After delivery completes, ``finalize_entry()`` updates the entry
    with the outcome.

    Replay operations are tracked as ``ReplayBatch`` objects for auditability.
    """

    def __init__(self, store: WebhookStore):
        self._store = store

    # ── Event Recording ─────────────────────────────────────────────

    def record_event(
        self,
        event_type: str,
        payload: dict[str, Any],
        endpoint_ids: list[str],
        source: str = "delivery",
        headers: dict[str, str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> OutboxEntry | None:
        """Record an event in the outbox before delivery.

        This is called by the service layer just before creating deliveries.
        Returns the outbox entry, or ``None`` if the store doesn't support outbox.
        """
        if not hasattr(self._store, "add_outbox_entry"):
            return None

        entry = OutboxEntry(
            event_type=event_type,
            payload=payload,
            endpoint_ids=endpoint_ids,
            source=source,
            headers=headers or {},
            metadata=metadata or {},
        )
        return self._store.add_outbox_entry(entry)

    def finalize_entry(
        self,
        entry_id: str,
        delivery_ids: list[str],
        success_count: int,
        failure_count: int,
    ) -> OutboxEntry | None:
        """Update an outbox entry after delivery completes.

        Sets the aggregate status based on success/failure counts:
        - All succeeded → DELIVERED
        - All failed → FAILED
        - Mixed → PARTIAL
        """
        if not hasattr(self._store, "update_outbox_entry"):
            return None

        total = success_count + failure_count
        if failure_count == 0:
            status = OutboxStatus.DELIVERED
        elif success_count == 0:
            status = OutboxStatus.FAILED
        else:
            status = OutboxStatus.PARTIAL

        return self._store.update_outbox_entry(
            entry_id,
            delivery_ids=delivery_ids,
            success_count=success_count,
            failure_count=failure_count,
            status=status,
            delivered_at=datetime.now(timezone.utc),
        )

    # ── Querying ────────────────────────────────────────────────────

    def get_entry(self, entry_id: str) -> OutboxEntry | None:
        if not hasattr(self._store, "get_outbox_entry"):
            return None
        return self._store.get_outbox_entry(entry_id)

    def list_entries(
        self,
        event_type: str | None = None,
        source: str | None = None,
        status: OutboxStatus | None = None,
        endpoint_id: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        limit: int = 100,
    ) -> list[OutboxEntry]:
        if not hasattr(self._store, "list_outbox"):
            return []
        return self._store.list_outbox(
            event_type=event_type,
            source=source,
            status=status,
            endpoint_id=endpoint_id,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
        )

    def outbox_stats(self) -> dict[str, Any]:
        """Get aggregate outbox statistics."""
        if not hasattr(self._store, "outbox_count"):
            return {"enabled": False}

        total = self._store.outbox_count()
        delivered = self._store.outbox_count(status=OutboxStatus.DELIVERED)
        failed = self._store.outbox_count(status=OutboxStatus.FAILED)
        pending = self._store.outbox_count(status=OutboxStatus.PENDING)
        partial = self._store.outbox_count(status=OutboxStatus.PARTIAL)
        skipped = self._store.outbox_count(status=OutboxStatus.SKIPPED)

        delivery_rate = (delivered / total * 100) if total > 0 else 0.0

        return {
            "enabled": True,
            "total": total,
            "delivered": delivered,
            "failed": failed,
            "pending": pending,
            "partial": partial,
            "skipped": skipped,
            "delivery_rate_pct": round(delivery_rate, 2),
        }

    # ── Event Replay ────────────────────────────────────────────────

    def create_replay_batch(
        self,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        event_type: str | None = None,
        endpoint_id: str | None = None,
        source: str | None = None,
        target_endpoint_ids: list[str] | None = None,
    ) -> ReplayBatch | None:
        """Create a replay batch (preview mode — doesn't execute yet).

        Selects matching outbox entries and records the batch metadata.
        Call ``execute_replay_batch()`` to actually deliver.
        """
        if not hasattr(self._store, "add_replay_batch"):
            return None

        # Normalize timestamps to UTC
        if start_time is not None and start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=timezone.utc)
        if end_time is not None and end_time.tzinfo is None:
            end_time = end_time.replace(tzinfo=timezone.utc)

        entries = self.list_entries(
            event_type=event_type,
            source=source,
            endpoint_id=endpoint_id,
            start_time=start_time,
            end_time=end_time,
            limit=10000,
        )

        batch = ReplayBatch(
            start_time=start_time,
            end_time=end_time,
            event_type=event_type,
            endpoint_id=endpoint_id,
            source=source,
            target_endpoint_ids=target_endpoint_ids,
            total_entries=len(entries),
            replayed_entry_ids=[e.id for e in entries],
        )
        return self._store.add_replay_batch(batch)

    async def execute_replay_batch(
        self,
        batch_id: str,
        delivery_callback: Any | None = None,
    ) -> ReplayBatch | None:
        """Execute a replay batch — re-deliver all selected outbox entries.

        Args:
            batch_id: The replay batch ID.
            delivery_callback: Optional async callable ``(entry: OutboxEntry) -> list[WebhookDelivery]``.
                If provided, it's called for each entry to perform the actual delivery.
                If None, entries are only marked as replayed (dry-run mode).

        Returns:
            The updated replay batch with results.
        """
        if not hasattr(self._store, "update_replay_batch"):
            return None

        batch = self._store.get_replay_batch(batch_id)
        if batch is None:
            return None
        if batch.status in (ReplayBatchStatus.COMPLETED, ReplayBatchStatus.CANCELLED):
            return batch

        self._store.update_replay_batch(batch_id, status=ReplayBatchStatus.IN_PROGRESS)

        succeeded = 0
        failed = 0
        created_delivery_ids: list[str] = []

        for entry_id in batch.replayed_entry_ids:
            entry = self._store.get_outbox_entry(entry_id)
            if entry is None:
                failed += 1
                continue

            try:
                if delivery_callback is not None:
                    deliveries = await delivery_callback(entry)
                    created_delivery_ids.extend(d.id for d in deliveries)
                    succeeded += 1
                else:
                    # Dry-run mode: just mark as replayed
                    succeeded += 1
            except Exception:
                failed += 1

        self._store.update_replay_batch(
            batch_id,
            status=ReplayBatchStatus.COMPLETED,
            processed=len(batch.replayed_entry_ids),
            succeeded=succeeded,
            failed=failed,
            created_delivery_ids=created_delivery_ids,
            completed_at=datetime.now(timezone.utc),
        )
        return self._store.get_replay_batch(batch_id)

    def cancel_replay_batch(self, batch_id: str) -> ReplayBatch | None:
        """Cancel a pending or in-progress replay batch."""
        if not hasattr(self._store, "update_replay_batch"):
            return None
        batch = self._store.get_replay_batch(batch_id)
        if batch is None or batch.status in (
            ReplayBatchStatus.COMPLETED,
            ReplayBatchStatus.FAILED,
        ):
            return batch
        return self._store.update_replay_batch(
            batch_id,
            status=ReplayBatchStatus.CANCELLED,
            completed_at=datetime.now(timezone.utc),
        )

    def get_replay_batch(self, batch_id: str) -> ReplayBatch | None:
        if not hasattr(self._store, "get_replay_batch"):
            return None
        return self._store.get_replay_batch(batch_id)

    def list_replay_batches(self, status: str | None = None) -> list[ReplayBatch]:
        if not hasattr(self._store, "list_replay_batches"):
            return []
        return self._store.list_replay_batches(status=status)

    # ── Schema Validation ───────────────────────────────────────────

    def create_schema(
        self,
        event_type: str,
        version: str,
        schema: dict[str, Any],
        strict: bool = False,
        description: str | None = None,
    ) -> SchemaVersion | None:
        """Register a JSON Schema for an event type."""
        if not hasattr(self._store, "add_schema_version"):
            return None

        sv = SchemaVersion(
            event_type=event_type,
            version=version,
            schema_def=schema,
            strict=strict,
            description=description,
        )
        return self._store.add_schema_version(sv)

    def get_schema(
        self,
        event_type: str,
        version: str | None = None,
    ) -> SchemaVersion | None:
        if not hasattr(self._store, "get_schema_version"):
            return None
        return self._store.get_schema_version(event_type, version)

    def list_schemas(
        self,
        event_type: str | None = None,
        active_only: bool = False,
    ) -> list[SchemaVersion]:
        if not hasattr(self._store, "list_schema_versions"):
            return []
        return self._store.list_schema_versions(event_type, active_only)

    def update_schema(self, sv_id: str, **updates: Any) -> SchemaVersion | None:
        if not hasattr(self._store, "update_schema_version"):
            return None
        return self._store.update_schema_version(sv_id, **updates)

    def delete_schema(self, sv_id: str) -> bool:
        if not hasattr(self._store, "delete_schema_version"):
            return False
        return self._store.delete_schema_version(sv_id)

    def validate_payload(
        self,
        event_type: str,
        payload: dict[str, Any],
    ) -> tuple[bool, list[str], str | None]:
        """Validate a payload against the active schema for an event type.

        Returns:
            Tuple of ``(valid, errors, schema_version)``.
            If no schema is registered, returns ``(True, [], None)``.
        """
        sv = self.get_schema(event_type)
        if sv is None:
            return True, [], None

        errors = _validate_json_schema(payload, sv.schema_def)
        valid = len(errors) == 0
        return valid, errors, sv.version


def _validate_json_schema(
    payload: dict[str, Any],
    schema: dict[str, Any],
) -> list[str]:
    """Validate payload against a JSON Schema.

    Uses ``jsonschema`` library if available. Otherwise, does basic type
    checking on common fields.
    """
    errors: list[str] = []

    try:
        import jsonschema  # type: ignore[import-untyped]

        validator_cls = jsonschema.Draft7Validator
        validator = validator_cls(schema)
        for error in validator.iter_errors(payload):
            path = ".".join(str(p) for p in error.absolute_path) or "(root)"
            errors.append(f"{path}: {error.message}")
    except ImportError:
        # Fallback: basic type checking for required fields
        if "required" in schema:
            for field in schema["required"]:
                if field not in payload:
                    errors.append(f"Missing required field: {field}")
        if "properties" in schema:
            for field, field_schema in schema["properties"].items():
                if field in payload:
                    expected_type = field_schema.get("type")
                    if expected_type:
                        type_map = {
                            "string": str,
                            "integer": int,
                            "number": (int, float),
                            "boolean": bool,
                            "array": list,
                            "object": dict,
                            "null": type(None),
                        }
                        expected = type_map.get(expected_type)
                        if expected and not isinstance(payload[field], expected):
                            errors.append(
                                f"Field '{field}': expected {expected_type}, "
                                f"got {type(payload[field]).__name__}"
                            )

    return errors
