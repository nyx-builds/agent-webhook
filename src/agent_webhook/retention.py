"""Data retention and cleanup for agent-webhook.

Automatically prunes old deliveries, delivery attempts, event log entries,
and resolved dead-letter entries based on configurable retention policies.
Essential for long-running production deployments — without cleanup, the
SQLite database grows unbounded.

Usage::

    from agent_webhook.retention import RetentionPolicy, RetentionManager
    from agent_webhook.service import WebhookService

    service = WebhookService(store_path="webhooks.db")
    policy = RetentionPolicy(
        delivery_retention_days=30,
        event_log_retention_days=90,
        dead_letter_retention_days=180,
    )
    manager = RetentionManager(service, policy)
    result = manager.run_cleanup()
    print(result)  # {'deleted_deliveries': 142, 'deleted_event_logs': 3, ...}

The cleanup can be run periodically (e.g. once a day by the worker) or
triggered manually via CLI, MCP tool, or REST API.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any

from .service import WebhookService

logger = logging.getLogger(__name__)


@dataclass
class RetentionPolicy:
    """Configuration for data retention.

    Set any retention field to 0 to disable cleanup for that data type
    (keep forever).

    Attributes:
        delivery_retention_days: Delete deliveries older than N days (0 = keep forever).
        delivery_keep_failed: If True, never delete failed/dead-letter deliveries
            (only successful/abandoned ones are pruned).
        event_log_retention_days: Delete event log entries older than N days.
        dead_letter_retention_days: Delete dead-letter entries older than N days.
            Set to 0 to keep forever.
        dead_letter_delete_replayed: If True, delete dead-letter entries that
            have been replayed (regardless of age).
        incoming_retention_days: Delete incoming webhook records older than N days.
        cleanup_batch_size: Maximum records to delete per cleanup run (per type).
            Prevents long-running transactions. 0 = unlimited.
    """

    delivery_retention_days: int = 30
    delivery_keep_failed: bool = False
    event_log_retention_days: int = 90
    dead_letter_retention_days: int = 180
    dead_letter_delete_replayed: bool = True
    incoming_retention_days: int = 7
    cleanup_batch_size: int = 5000

    def to_dict(self) -> dict[str, Any]:
        return {
            "delivery_retention_days": self.delivery_retention_days,
            "delivery_keep_failed": self.delivery_keep_failed,
            "event_log_retention_days": self.event_log_retention_days,
            "dead_letter_retention_days": self.dead_letter_retention_days,
            "dead_letter_delete_replayed": self.dead_letter_delete_replayed,
            "incoming_retention_days": self.incoming_retention_days,
            "cleanup_batch_size": self.cleanup_batch_size,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> RetentionPolicy:
        return cls(
            delivery_retention_days=int(data.get("delivery_retention_days", 30)),
            delivery_keep_failed=bool(data.get("delivery_keep_failed", False)),
            event_log_retention_days=int(data.get("event_log_retention_days", 90)),
            dead_letter_retention_days=int(data.get("dead_letter_retention_days", 180)),
            dead_letter_delete_replayed=bool(data.get("dead_letter_delete_replayed", True)),
            incoming_retention_days=int(data.get("incoming_retention_days", 7)),
            cleanup_batch_size=int(data.get("cleanup_batch_size", 5000)),
        )


@dataclass
class CleanupResult:
    """Result of a retention cleanup run."""

    deleted_deliveries: int = 0
    deleted_event_logs: int = 0
    deleted_dead_letter: int = 0
    deleted_incoming: int = 0
    ran_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    errors: list[str] = field(default_factory=list)

    @property
    def total_deleted(self) -> int:
        return (
            self.deleted_deliveries
            + self.deleted_event_logs
            + self.deleted_dead_letter
            + self.deleted_incoming
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "deleted_deliveries": self.deleted_deliveries,
            "deleted_event_logs": self.deleted_event_logs,
            "deleted_dead_letter": self.deleted_dead_letter,
            "deleted_incoming": self.deleted_incoming,
            "total_deleted": self.total_deleted,
            "ran_at": self.ran_at.isoformat(),
            "errors": self.errors,
        }


class RetentionManager:
    """Runs data retention cleanup against the webhook store.

    Works with both JSON and SQLite stores. SQLite stores get efficient
    SQL-based deletion; JSON stores do in-memory filtering.
    """

    def __init__(
        self,
        service: WebhookService,
        policy: RetentionPolicy | None = None,
    ) -> None:
        self._service = service
        self._policy = policy or RetentionPolicy()
        self._last_result: CleanupResult | None = None

    @property
    def policy(self) -> RetentionPolicy:
        return self._policy

    @property
    def last_result(self) -> CleanupResult | None:
        return self._last_result

    def update_policy(self, **kwargs: Any) -> RetentionPolicy:
        """Update retention policy fields."""
        current = self._policy.to_dict()
        current.update(kwargs)
        self._policy = RetentionPolicy.from_dict(current)
        return self._policy

    def run_cleanup(self) -> CleanupResult:
        """Execute a full cleanup pass. Returns the result summary.

        Safe to call repeatedly — each call only deletes data older than
        the configured thresholds.
        """
        result = CleanupResult()
        store = self._service.store

        # ── Deliveries ──
        if self._policy.delivery_retention_days > 0:
            try:
                result.deleted_deliveries = self._cleanup_deliveries(store)
            except Exception as e:
                result.errors.append(f"delivery cleanup: {e}")
                logger.exception("Delivery cleanup failed")

        # ── Event Log ──
        if self._policy.event_log_retention_days > 0:
            try:
                result.deleted_event_logs = self._cleanup_event_log(store)
            except Exception as e:
                result.errors.append(f"event log cleanup: {e}")
                logger.exception("Event log cleanup failed")

        # ── Dead Letter Queue ──
        try:
            result.deleted_dead_letter = self._cleanup_dead_letter(store)
        except Exception as e:
            result.errors.append(f"dead letter cleanup: {e}")
            logger.exception("Dead letter cleanup failed")

        # ── Incoming Webhooks ──
        if self._policy.incoming_retention_days > 0:
            try:
                result.deleted_incoming = self._cleanup_incoming(store)
            except Exception as e:
                result.errors.append(f"incoming cleanup: {e}")
                logger.exception("Incoming cleanup failed")

        self._last_result = result
        logger.info(
            "Retention cleanup complete: %d deliveries, %d event logs, %d DLQ, %d incoming deleted",
            result.deleted_deliveries,
            result.deleted_event_logs,
            result.deleted_dead_letter,
            result.deleted_incoming,
        )
        return result

    def _cleanup_deliveries(self, store: Any) -> int:
        """Delete old deliveries."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._policy.delivery_retention_days)
        deleted = 0

        # SQLite store — use SQL for efficiency
        if hasattr(store, "_get_conn"):
            conn = store._get_conn()
            query = "DELETE FROM deliveries WHERE created_at < ?"
            params: list[Any] = [cutoff.isoformat()]

            if self._policy.delivery_keep_failed:
                # Keep failed and dead_letter statuses
                query += " AND status NOT IN ('failed', 'dead_letter', 'retrying')"

            if self._policy.cleanup_batch_size > 0:
                query += f" LIMIT {int(self._policy.cleanup_batch_size)}"

            cursor = conn.execute(query, params)
            conn.commit()
            deleted = cursor.rowcount

            # Also clean up orphaned delivery attempts
            conn.execute(
                "DELETE FROM delivery_attempts WHERE delivery_id NOT IN (SELECT id FROM deliveries)"
            )
            conn.commit()

        else:
            # JSON store — in-memory filtering
            all_deliveries = store.list_deliveries(limit=100000)
            to_keep = []
            for d in all_deliveries:
                if d.created_at < cutoff:
                    if self._policy.delivery_keep_failed and d.status.value in ("failed", "dead_letter", "retrying"):
                        to_keep.append(d)
                    else:
                        deleted += 1
                        # Clean attempts too
                        if hasattr(store, "_attempts"):
                            store._attempts.pop(d.id, None)
                else:
                    to_keep.append(d)
            store._deliveries = {d.id: d for d in to_keep}
            if hasattr(store, "_save"):
                store._save()

        return deleted

    def _cleanup_event_log(self, store: Any) -> int:
        """Delete old event log entries."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._policy.event_log_retention_days)
        deleted = 0

        if hasattr(store, "_get_conn"):
            conn = store._get_conn()
            query = "DELETE FROM event_log WHERE timestamp < ?"
            params: list[Any] = [cutoff.isoformat()]
            if self._policy.cleanup_batch_size > 0:
                query += f" LIMIT {int(self._policy.cleanup_batch_size)}"
            cursor = conn.execute(query, params)
            conn.commit()
            deleted = cursor.rowcount
        else:
            all_logs = store.list_event_log(limit=100000)
            to_keep = [e for e in all_logs if e.timestamp >= cutoff]
            deleted = len(all_logs) - len(to_keep)
            store._event_log = to_keep
            if hasattr(store, "_save"):
                store._save()

        return deleted

    def _cleanup_dead_letter(self, store: Any) -> int:
        """Delete old or replayed dead-letter entries."""
        deleted = 0

        # With 0 retention days, delete everything immediately
        if self._policy.dead_letter_retention_days <= 0 and not self._policy.dead_letter_delete_replayed:
            return 0

        if self._policy.dead_letter_retention_days <= 0:
            # 0 days = delete all entries
            cutoff = datetime.now(timezone.utc)
        else:
            cutoff = datetime.now(timezone.utc) - timedelta(days=self._policy.dead_letter_retention_days)

        if hasattr(store, "_get_conn"):
            conn = store._get_conn()
            # Delete replayed entries first
            if self._policy.dead_letter_delete_replayed:
                cursor = conn.execute(
                    "DELETE FROM dead_letter_queue WHERE replayed = 1"
                )
                deleted += cursor.rowcount

            # Delete old entries (0 days retention = delete all)
            cursor = conn.execute(
                "DELETE FROM dead_letter_queue WHERE created_at <= ?",
                (cutoff.isoformat(),),
            )
            deleted += cursor.rowcount
            conn.commit()
        else:
            all_dlq = store.list_dead_letter(limit=100000) if hasattr(store, "list_dead_letter") else []
            to_keep = []
            for entry in all_dlq:
                if self._policy.dead_letter_delete_replayed and entry.replayed:
                    deleted += 1
                    continue
                if entry.created_at <= cutoff:
                    deleted += 1
                    continue
                to_keep.append(entry)
            if hasattr(store, "_dead_letter"):
                store._dead_letter = {e.id: e for e in to_keep}
                if hasattr(store, "_save"):
                    store._save()

        return deleted

    def _cleanup_incoming(self, store: Any) -> int:
        """Delete old incoming webhook records."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=self._policy.incoming_retention_days)
        deleted = 0

        if hasattr(store, "_get_conn"):
            conn = store._get_conn()
            query = "DELETE FROM incoming WHERE received_at < ?"
            params: list[Any] = [cutoff.isoformat()]
            if self._policy.cleanup_batch_size > 0:
                query += f" LIMIT {int(self._policy.cleanup_batch_size)}"
            cursor = conn.execute(query, params)
            conn.commit()
            deleted = cursor.rowcount
        else:
            all_incoming = store.list_incoming(limit=100000)
            to_keep = [i for i in all_incoming if i.received_at >= cutoff]
            deleted = len(all_incoming) - len(to_keep)
            if hasattr(store, "_incoming"):
                store._incoming = to_keep
                if hasattr(store, "_save"):
                    store._save()

        return deleted

    def get_estimates(self) -> dict[str, int]:
        """Estimate how many records would be deleted by a cleanup run.

        Does not actually delete anything — useful for previewing impact.
        """
        store = self._service.store
        estimates: dict[str, int] = {}

        now = datetime.now(timezone.utc)

        # Deliveries
        if self._policy.delivery_retention_days > 0:
            cutoff = now - timedelta(days=self._policy.delivery_retention_days)
            all_deliveries = store.list_deliveries(limit=100000)
            count = 0
            for d in all_deliveries:
                if d.created_at < cutoff:
                    if not (self._policy.delivery_keep_failed and d.status.value in ("failed", "dead_letter", "retrying")):
                        count += 1
            estimates["deleted_deliveries"] = count

        # Event log
        if self._policy.event_log_retention_days > 0:
            cutoff = now - timedelta(days=self._policy.event_log_retention_days)
            all_logs = store.list_event_log(limit=100000)
            estimates["deleted_event_logs"] = sum(1 for e in all_logs if e.timestamp < cutoff)

        # Dead letter
        dlq_count = 0
        if hasattr(store, "list_dead_letter"):
            all_dlq = store.list_dead_letter(limit=100000)
            dlq_cutoff = now - timedelta(days=self._policy.dead_letter_retention_days)
            for entry in all_dlq:
                if self._policy.dead_letter_delete_replayed and entry.replayed:
                    dlq_count += 1
                elif self._policy.dead_letter_retention_days > 0 and entry.created_at < dlq_cutoff:
                    dlq_count += 1
        estimates["deleted_dead_letter"] = dlq_count

        # Incoming
        if self._policy.incoming_retention_days > 0:
            cutoff = now - timedelta(days=self._policy.incoming_retention_days)
            all_incoming = store.list_incoming(limit=100000)
            estimates["deleted_incoming"] = sum(1 for i in all_incoming if i.received_at < cutoff)

        estimates["total_deleted"] = sum(v for k, v in estimates.items() if k != "total_deleted")
        return estimates
