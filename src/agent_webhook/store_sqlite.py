"""SQLite-backed persistence for agent-webhook."""

from __future__ import annotations

import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import (
    DeadLetterEntry,
    DeliveryAttempt,
    DeliveryStatus,
    EventLogEntry,
    EventSubscription,
    IncomingWebhook,
    PayloadTransform,
    RelayRule,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookSchedule,
    WebhookStatus,
)


_SCHEMA = """
CREATE TABLE IF NOT EXISTS endpoints (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS deliveries (
    id TEXT PRIMARY KEY,
    endpoint_id TEXT NOT NULL,
    status TEXT NOT NULL,
    event_type TEXT,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    data TEXT NOT NULL,
    FOREIGN KEY (delivery_id) REFERENCES deliveries(id)
);

CREATE TABLE IF NOT EXISTS incoming (
    id TEXT PRIMARY KEY,
    path TEXT NOT NULL,
    processed INTEGER NOT NULL DEFAULT 0,
    received_at TEXT NOT NULL,
    data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS relay_rules (
    id TEXT PRIMARY KEY,
    data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS subscriptions (
    id TEXT PRIMARY KEY,
    endpoint_id TEXT NOT NULL,
    data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS event_log (
    id TEXT PRIMARY KEY,
    event_type TEXT NOT NULL,
    endpoint_id TEXT,
    timestamp TEXT NOT NULL,
    data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS transforms (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    type TEXT NOT NULL,
    data TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dead_letter_queue (
    id TEXT PRIMARY KEY,
    delivery_id TEXT NOT NULL,
    endpoint_id TEXT NOT NULL,
    replayed INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_deliveries_endpoint ON deliveries(endpoint_id);
CREATE INDEX IF NOT EXISTS idx_deliveries_status ON deliveries(status);
CREATE INDEX IF NOT EXISTS idx_deliveries_event_type ON deliveries(event_type);
CREATE INDEX IF NOT EXISTS idx_delivery_attempts_delivery ON delivery_attempts(delivery_id);
CREATE INDEX IF NOT EXISTS idx_incoming_path ON incoming(path);
CREATE INDEX IF NOT EXISTS idx_subscriptions_endpoint ON subscriptions(endpoint_id);
CREATE INDEX IF NOT EXISTS idx_event_log_type ON event_log(event_type);
CREATE INDEX IF NOT EXISTS idx_event_log_endpoint ON event_log(endpoint_id);
CREATE INDEX IF NOT EXISTS idx_event_log_timestamp ON event_log(timestamp);
CREATE INDEX IF NOT EXISTS idx_dead_letter_endpoint ON dead_letter_queue(endpoint_id);
CREATE INDEX IF NOT EXISTS idx_dead_letter_replayed ON dead_letter_queue(replayed);
CREATE INDEX IF NOT EXISTS idx_transforms_name ON transforms(name);

CREATE TABLE IF NOT EXISTS schedules (
    id TEXT PRIMARY KEY,
    endpoint_id TEXT NOT NULL,
    active INTEGER NOT NULL DEFAULT 1,
    next_run_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    data TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_schedules_endpoint ON schedules(endpoint_id);
CREATE INDEX IF NOT EXISTS idx_schedules_active ON schedules(active);
CREATE INDEX IF NOT EXISTS idx_schedules_next_run ON schedules(next_run_at);
"""


class SQLiteStore:
    """Thread-safe SQLite store for webhooks, deliveries, and relay rules."""

    def __init__(self, path: str | Path = "webhook_store.db"):
        self._path = str(path)
        self._lock = threading.Lock()
        self._local = threading.local()
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        """Get a thread-local connection."""
        if not hasattr(self._local, "conn") or self._local.conn is None:
            self._local.conn = sqlite3.connect(self._path, timeout=30.0)
            self._local.conn.row_factory = sqlite3.Row
            self._local.conn.execute("PRAGMA journal_mode=WAL")
            self._local.conn.execute("PRAGMA synchronous=NORMAL")
        return self._local.conn

    def _init_db(self) -> None:
        """Initialize the database schema."""
        conn = self._get_conn()
        conn.executescript(_SCHEMA)
        conn.commit()

    # --- Endpoints ---

    def add_endpoint(self, endpoint: WebhookEndpoint) -> WebhookEndpoint:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO endpoints (id, data) VALUES (?, ?)",
                (endpoint.id, endpoint.model_dump_json()),
            )
            conn.commit()
        return endpoint

    def get_endpoint(self, endpoint_id: str) -> WebhookEndpoint | None:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM endpoints WHERE id = ?", (endpoint_id,)).fetchone()
        if row is None:
            return None
        return WebhookEndpoint.model_validate_json(row["data"])

    def list_endpoints(
        self,
        status: WebhookStatus | None = None,
        tag: str | None = None,
    ) -> list[WebhookEndpoint]:
        conn = self._get_conn()
        rows = conn.execute("SELECT data FROM endpoints ORDER BY rowid").fetchall()
        endpoints = [WebhookEndpoint.model_validate_json(r["data"]) for r in rows]
        if status is not None:
            endpoints = [e for e in endpoints if e.status == status]
        if tag is not None:
            endpoints = [e for e in endpoints if tag in e.tags]
        return sorted(endpoints, key=lambda e: e.created_at)

    def update_endpoint(self, endpoint_id: str, **updates: Any) -> WebhookEndpoint | None:
        with self._lock:
            endpoint = self.get_endpoint(endpoint_id)
            if endpoint is None:
                return None
            for key, value in updates.items():
                if hasattr(endpoint, key):
                    setattr(endpoint, key, value)
            endpoint.updated_at = datetime.now(timezone.utc)
            conn = self._get_conn()
            conn.execute(
                "UPDATE endpoints SET data = ? WHERE id = ?",
                (endpoint.model_dump_json(), endpoint_id),
            )
            conn.commit()
        return endpoint

    def delete_endpoint(self, endpoint_id: str) -> bool:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.execute("DELETE FROM endpoints WHERE id = ?", (endpoint_id,))
            conn.commit()
            return cursor.rowcount > 0

    # --- Deliveries ---

    def add_delivery(self, delivery: WebhookDelivery) -> WebhookDelivery:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO deliveries (id, endpoint_id, status, event_type, created_at, data) VALUES (?, ?, ?, ?, ?, ?)",
                (delivery.id, delivery.endpoint_id, delivery.status.value, delivery.event_type, delivery.created_at.isoformat(), delivery.model_dump_json()),
            )
            conn.commit()
        return delivery

    def get_delivery(self, delivery_id: str) -> WebhookDelivery | None:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM deliveries WHERE id = ?", (delivery_id,)).fetchone()
        if row is None:
            return None
        delivery = WebhookDelivery.model_validate_json(row["data"])
        # Load attempts
        attempts = self._get_attempts_for_delivery(delivery_id)
        delivery.attempts = attempts
        return delivery

    def list_deliveries(
        self,
        endpoint_id: str | None = None,
        status: DeliveryStatus | None = None,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[WebhookDelivery]:
        conn = self._get_conn()
        query = "SELECT data FROM deliveries WHERE 1=1"
        params: list[Any] = []
        if endpoint_id is not None:
            query += " AND endpoint_id = ?"
            params.append(endpoint_id)
        if status is not None:
            query += " AND status = ?"
            params.append(status.value)
        if event_type is not None:
            query += " AND event_type = ?"
            params.append(event_type)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        deliveries = [WebhookDelivery.model_validate_json(r["data"]) for r in rows]
        # Load attempts for each delivery
        for d in deliveries:
            d.attempts = self._get_attempts_for_delivery(d.id)
        return deliveries

    def update_delivery(self, delivery_id: str, **updates: Any) -> WebhookDelivery | None:
        with self._lock:
            delivery = self.get_delivery(delivery_id)
            if delivery is None:
                return None
            for key, value in updates.items():
                if hasattr(delivery, key):
                    setattr(delivery, key, value)
            conn = self._get_conn()
            conn.execute(
                "UPDATE deliveries SET data = ?, status = ? WHERE id = ?",
                (delivery.model_dump_json(), delivery.status.value, delivery_id),
            )
            conn.commit()
        return delivery

    def add_delivery_attempt(self, delivery_id: str, attempt: DeliveryAttempt) -> WebhookDelivery | None:
        with self._lock:
            delivery = self.get_delivery(delivery_id)
            if delivery is None:
                return None
            delivery.attempts.append(attempt)
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO delivery_attempts (id, delivery_id, data) VALUES (?, ?, ?)",
                (attempt.id, delivery_id, attempt.model_dump_json()),
            )
            conn.execute(
                "UPDATE deliveries SET data = ? WHERE id = ?",
                (delivery.model_dump_json(), delivery_id),
            )
            conn.commit()
        return delivery

    def _get_attempts_for_delivery(self, delivery_id: str) -> list[DeliveryAttempt]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT data FROM delivery_attempts WHERE delivery_id = ? ORDER BY rowid",
            (delivery_id,),
        ).fetchall()
        return [DeliveryAttempt.model_validate_json(r["data"]) for r in rows]

    # --- Incoming Webhooks ---

    def add_incoming(self, incoming: IncomingWebhook) -> IncomingWebhook:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO incoming (id, path, processed, received_at, data) VALUES (?, ?, ?, ?, ?)",
                (incoming.id, incoming.path, 1 if incoming.processed else 0, incoming.received_at.isoformat(), incoming.model_dump_json()),
            )
            conn.commit()
        return incoming

    def get_incoming(self, incoming_id: str) -> IncomingWebhook | None:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM incoming WHERE id = ?", (incoming_id,)).fetchone()
        if row is None:
            return None
        return IncomingWebhook.model_validate_json(row["data"])

    def list_incoming(
        self,
        path: str | None = None,
        processed: bool | None = None,
        limit: int = 100,
    ) -> list[IncomingWebhook]:
        conn = self._get_conn()
        query = "SELECT data FROM incoming WHERE 1=1"
        params: list[Any] = []
        if path is not None:
            query += " AND path = ?"
            params.append(path)
        if processed is not None:
            query += " AND processed = ?"
            params.append(1 if processed else 0)
        query += " ORDER BY received_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        return [IncomingWebhook.model_validate_json(r["data"]) for r in rows]

    # --- Relay Rules ---

    def add_relay_rule(self, rule: RelayRule) -> RelayRule:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO relay_rules (id, data) VALUES (?, ?)",
                (rule.id, rule.model_dump_json()),
            )
            conn.commit()
        return rule

    def get_relay_rule(self, rule_id: str) -> RelayRule | None:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM relay_rules WHERE id = ?", (rule_id,)).fetchone()
        if row is None:
            return None
        return RelayRule.model_validate_json(row["data"])

    def list_relay_rules(self, active_only: bool = False) -> list[RelayRule]:
        conn = self._get_conn()
        rows = conn.execute("SELECT data FROM relay_rules ORDER BY rowid").fetchall()
        rules = [RelayRule.model_validate_json(r["data"]) for r in rows]
        if active_only:
            rules = [r for r in rules if r.active]
        return rules

    def delete_relay_rule(self, rule_id: str) -> bool:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.execute("DELETE FROM relay_rules WHERE id = ?", (rule_id,))
            conn.commit()
            return cursor.rowcount > 0

    def update_relay_rule(self, rule_id: str, **updates: Any) -> RelayRule | None:
        with self._lock:
            rule = self.get_relay_rule(rule_id)
            if rule is None:
                return None
            for key, value in updates.items():
                if hasattr(rule, key):
                    setattr(rule, key, value)
            conn = self._get_conn()
            conn.execute(
                "UPDATE relay_rules SET data = ? WHERE id = ?",
                (rule.model_dump_json(), rule_id),
            )
            conn.commit()
        return rule

    # --- Stats ---

    def get_stats(self, endpoint_id: str) -> dict[str, Any] | None:
        endpoint = self.get_endpoint(endpoint_id)
        if endpoint is None:
            return None
        deliveries = self.list_deliveries(endpoint_id=endpoint_id, limit=10000)
        if not deliveries:
            return {
                "endpoint_id": endpoint_id,
                "endpoint_name": endpoint.name,
                "total_deliveries": 0,
                "successful": 0,
                "failed": 0,
                "pending": 0,
                "retrying": 0,
                "abandoned": 0,
                "dead_letter": 0,
                "avg_duration_ms": None,
                "last_delivery_at": None,
                "last_success_at": None,
                "last_failure_at": None,
            }
        status_counts: dict[str, int] = {}
        for d in deliveries:
            status_counts[d.status.value] = status_counts.get(d.status.value, 0) + 1

        durations: list[float] = []
        last_delivery = None
        last_success = None
        last_failure = None
        for d in deliveries:
            if last_delivery is None or d.created_at > last_delivery:
                last_delivery = d.created_at
            for a in d.attempts:
                if a.duration_ms is not None:
                    durations.append(a.duration_ms)
                if a.status == DeliveryStatus.SUCCESS:
                    if last_success is None or (a.completed_at and a.completed_at > last_success):
                        last_success = a.completed_at
                if a.status == DeliveryStatus.FAILED:
                    if last_failure is None or (a.completed_at and a.completed_at > last_failure):
                        last_failure = a.completed_at

        return {
            "endpoint_id": endpoint_id,
            "endpoint_name": endpoint.name,
            "total_deliveries": len(deliveries),
            "successful": status_counts.get("success", 0),
            "failed": status_counts.get("failed", 0),
            "pending": status_counts.get("pending", 0),
            "retrying": status_counts.get("retrying", 0),
            "abandoned": status_counts.get("abandoned", 0),
            "dead_letter": status_counts.get("dead_letter", 0),
            "avg_duration_ms": sum(durations) / len(durations) if durations else None,
            "last_delivery_at": last_delivery,
            "last_success_at": last_success,
            "last_failure_at": last_failure,
        }

    def get_all_stats(self) -> list[dict[str, Any]]:
        conn = self._get_conn()
        rows = conn.execute("SELECT id FROM endpoints").fetchall()
        return [s for s in (self.get_stats(r["id"]) for r in rows) if s is not None]

    # --- Event Subscriptions ---

    def add_subscription(self, sub: EventSubscription) -> EventSubscription:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO subscriptions (id, endpoint_id, data) VALUES (?, ?, ?)",
                (sub.id, sub.endpoint_id, sub.model_dump_json()),
            )
            conn.commit()
        return sub

    def get_subscription(self, subscription_id: str) -> EventSubscription | None:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM subscriptions WHERE id = ?", (subscription_id,)).fetchone()
        if row is None:
            return None
        return EventSubscription.model_validate_json(row["data"])

    def list_subscriptions(
        self,
        endpoint_id: str | None = None,
    ) -> list[EventSubscription]:
        conn = self._get_conn()
        if endpoint_id is not None:
            rows = conn.execute("SELECT data FROM subscriptions WHERE endpoint_id = ? ORDER BY rowid", (endpoint_id,)).fetchall()
        else:
            rows = conn.execute("SELECT data FROM subscriptions ORDER BY rowid").fetchall()
        return [EventSubscription.model_validate_json(r["data"]) for r in rows]

    def delete_subscription(self, subscription_id: str) -> bool:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.execute("DELETE FROM subscriptions WHERE id = ?", (subscription_id,))
            conn.commit()
            return cursor.rowcount > 0

    # --- Event Log ---

    def add_event_log(self, entry: EventLogEntry) -> EventLogEntry:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO event_log (id, event_type, endpoint_id, timestamp, data) VALUES (?, ?, ?, ?, ?)",
                (entry.id, entry.event_type, entry.endpoint_id, entry.timestamp.isoformat(), entry.model_dump_json()),
            )
            # Keep only the last 1000 entries
            conn.execute(
                "DELETE FROM event_log WHERE id NOT IN (SELECT id FROM event_log ORDER BY timestamp DESC LIMIT 1000)"
            )
            conn.commit()
        return entry

    def list_event_log(
        self,
        event_type: str | None = None,
        endpoint_id: str | None = None,
        limit: int = 100,
    ) -> list[EventLogEntry]:
        conn = self._get_conn()
        query = "SELECT data FROM event_log WHERE 1=1"
        params: list[Any] = []
        if event_type is not None:
            query += " AND event_type = ?"
            params.append(event_type)
        if endpoint_id is not None:
            query += " AND endpoint_id = ?"
            params.append(endpoint_id)
        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        return [EventLogEntry.model_validate_json(r["data"]) for r in rows]

    def pending_deliveries(self) -> list[WebhookDelivery]:
        """Get all deliveries that are pending or retrying and due for delivery."""
        now = datetime.now(timezone.utc)
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT data FROM deliveries WHERE status IN (?, ?)",
            (DeliveryStatus.PENDING.value, DeliveryStatus.RETRYING.value),
        ).fetchall()
        deliveries = [WebhookDelivery.model_validate_json(r["data"]) for r in rows]
        result = []
        for d in deliveries:
            d.attempts = self._get_attempts_for_delivery(d.id)
            if d.status == DeliveryStatus.PENDING:
                # Skip scheduled deliveries that aren't due yet
                if d.scheduled_at is not None and d.scheduled_at > now:
                    continue
                result.append(d)
            elif d.status == DeliveryStatus.RETRYING and (d.next_retry_at is None or d.next_retry_at <= now):
                result.append(d)
        return sorted(result, key=lambda d: d.created_at)

    # --- Transforms ---

    def add_transform(self, transform: PayloadTransform) -> PayloadTransform:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO transforms (id, name, type, data) VALUES (?, ?, ?, ?)",
                (transform.id, transform.name, transform.type.value, transform.model_dump_json()),
            )
            conn.commit()
        return transform

    def get_transform(self, transform_id: str) -> PayloadTransform | None:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM transforms WHERE id = ?", (transform_id,)).fetchone()
        if row is None:
            return None
        return PayloadTransform.model_validate_json(row["data"])

    def list_transforms(self, type: str | None = None) -> list[PayloadTransform]:
        conn = self._get_conn()
        if type is not None:
            rows = conn.execute("SELECT data FROM transforms WHERE type = ? ORDER BY rowid", (type,)).fetchall()
        else:
            rows = conn.execute("SELECT data FROM transforms ORDER BY rowid").fetchall()
        return [PayloadTransform.model_validate_json(r["data"]) for r in rows]

    def update_transform(self, transform_id: str, **updates: Any) -> PayloadTransform | None:
        with self._lock:
            transform = self.get_transform(transform_id)
            if transform is None:
                return None
            for key, value in updates.items():
                if hasattr(transform, key):
                    setattr(transform, key, value)
            conn = self._get_conn()
            conn.execute(
                "UPDATE transforms SET name = ?, type = ?, data = ? WHERE id = ?",
                (transform.name, transform.type.value, transform.model_dump_json(), transform_id),
            )
            conn.commit()
        return transform

    def delete_transform(self, transform_id: str) -> bool:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.execute("DELETE FROM transforms WHERE id = ?", (transform_id,))
            conn.commit()
            return cursor.rowcount > 0

    # --- Dead Letter Queue ---

    def add_dead_letter(self, entry: DeadLetterEntry) -> DeadLetterEntry:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO dead_letter_queue (id, delivery_id, endpoint_id, replayed, created_at, data) VALUES (?, ?, ?, ?, ?, ?)",
                (entry.id, entry.delivery_id, entry.endpoint_id, 1 if entry.replayed else 0, entry.created_at.isoformat(), entry.model_dump_json()),
            )
            conn.commit()
        return entry

    def get_dead_letter(self, entry_id: str) -> DeadLetterEntry | None:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM dead_letter_queue WHERE id = ?", (entry_id,)).fetchone()
        if row is None:
            return None
        return DeadLetterEntry.model_validate_json(row["data"])

    def list_dead_letter(
        self,
        endpoint_id: str | None = None,
        replayed: bool | None = None,
        limit: int = 100,
    ) -> list[DeadLetterEntry]:
        conn = self._get_conn()
        query = "SELECT data FROM dead_letter_queue WHERE 1=1"
        params: list[Any] = []
        if endpoint_id is not None:
            query += " AND endpoint_id = ?"
            params.append(endpoint_id)
        if replayed is not None:
            query += " AND replayed = ?"
            params.append(1 if replayed else 0)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)

        rows = conn.execute(query, params).fetchall()
        return [DeadLetterEntry.model_validate_json(r["data"]) for r in rows]

    def update_dead_letter(self, entry_id: str, **updates: Any) -> DeadLetterEntry | None:
        with self._lock:
            entry = self.get_dead_letter(entry_id)
            if entry is None:
                return None
            for key, value in updates.items():
                if hasattr(entry, key):
                    setattr(entry, key, value)
            conn = self._get_conn()
            conn.execute(
                "UPDATE dead_letter_queue SET data = ?, replayed = ? WHERE id = ?",
                (entry.model_dump_json(), 1 if entry.replayed else 0, entry_id),
            )
            conn.commit()
        return entry

    def delete_dead_letter(self, entry_id: str) -> bool:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.execute("DELETE FROM dead_letter_queue WHERE id = ?", (entry_id,))
            conn.commit()
            return cursor.rowcount > 0

    def dead_letter_count(self, endpoint_id: str | None = None) -> int:
        conn = self._get_conn()
        if endpoint_id is not None:
            row = conn.execute("SELECT COUNT(*) as cnt FROM dead_letter_queue WHERE endpoint_id = ?", (endpoint_id,)).fetchone()
        else:
            row = conn.execute("SELECT COUNT(*) as cnt FROM dead_letter_queue").fetchone()
        return row["cnt"] if row else 0

    # --- Migrate from JSON ---

    def migrate_from_json(self, json_path: str | Path) -> dict[str, int]:
        """Migrate data from a JSON store file to this SQLite store.

        Returns a dict with counts of migrated items.
        """
        from .models import WebhookEndpoint as WE, WebhookDelivery as WD, IncomingWebhook as IW, RelayRule as RR, EventSubscription as ES, EventLogEntry as ELE

        with open(json_path, "r") as f:
            raw = json.load(f)

        counts = {"endpoints": 0, "deliveries": 0, "incoming": 0, "relay_rules": 0, "subscriptions": 0, "event_log": 0}

        with self._lock:
            conn = self._get_conn()
            for eid, edata in raw.get("endpoints", {}).items():
                ep = WE.model_validate(edata)
                conn.execute("INSERT OR IGNORE INTO endpoints (id, data) VALUES (?, ?)", (ep.id, ep.model_dump_json()))
                counts["endpoints"] += 1

            for did, ddata in raw.get("deliveries", {}).items():
                d = WD.model_validate(ddata)
                conn.execute(
                    "INSERT OR IGNORE INTO deliveries (id, endpoint_id, status, event_type, created_at, data) VALUES (?, ?, ?, ?, ?, ?)",
                    (d.id, d.endpoint_id, d.status.value, d.event_type, d.created_at.isoformat(), d.model_dump_json()),
                )
                for a in d.attempts:
                    conn.execute("INSERT OR IGNORE INTO delivery_attempts (id, delivery_id, data) VALUES (?, ?, ?)", (a.id, d.id, a.model_dump_json()))
                counts["deliveries"] += 1

            for iid, idata in raw.get("incoming", {}).items():
                i = IW.model_validate(idata)
                conn.execute("INSERT OR IGNORE INTO incoming (id, path, processed, received_at, data) VALUES (?, ?, ?, ?, ?)", (i.id, i.path, 1 if i.processed else 0, i.received_at.isoformat(), i.model_dump_json()))
                counts["incoming"] += 1

            for rid, rdata in raw.get("relay_rules", {}).items():
                r = RR.model_validate(rdata)
                conn.execute("INSERT OR IGNORE INTO relay_rules (id, data) VALUES (?, ?)", (r.id, r.model_dump_json()))
                counts["relay_rules"] += 1

            for sid, sdata in raw.get("subscriptions", {}).items():
                s = ES.model_validate(sdata)
                conn.execute("INSERT OR IGNORE INTO subscriptions (id, endpoint_id, data) VALUES (?, ?, ?)", (s.id, s.endpoint_id, s.model_dump_json()))
                counts["subscriptions"] += 1

            for edata in raw.get("event_log", []):
                e = ELE.model_validate(edata)
                conn.execute("INSERT OR IGNORE INTO event_log (id, event_type, endpoint_id, timestamp, data) VALUES (?, ?, ?, ?, ?)", (e.id, e.event_type, e.endpoint_id, e.timestamp.isoformat(), e.model_dump_json()))
                counts["event_log"] += 1

            conn.commit()

        return counts

    # --- Schedules ---

    def add_schedule(self, schedule: WebhookSchedule) -> WebhookSchedule:
        with self._lock:
            conn = self._get_conn()
            conn.execute(
                "INSERT INTO schedules (id, endpoint_id, active, next_run_at, created_at, data) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    schedule.id,
                    schedule.endpoint_id,
                    1 if schedule.active else 0,
                    schedule.next_run_at.isoformat(),
                    schedule.created_at.isoformat(),
                    schedule.model_dump_json(),
                ),
            )
            conn.commit()
        return schedule

    def get_schedule(self, schedule_id: str) -> WebhookSchedule | None:
        conn = self._get_conn()
        row = conn.execute("SELECT data FROM schedules WHERE id = ?", (schedule_id,)).fetchone()
        if row is None:
            return None
        return WebhookSchedule.model_validate_json(row["data"])

    def list_schedules(
        self,
        endpoint_id: str | None = None,
        active_only: bool = False,
    ) -> list[WebhookSchedule]:
        conn = self._get_conn()
        if endpoint_id is not None and active_only:
            rows = conn.execute(
                "SELECT data FROM schedules WHERE endpoint_id = ? AND active = 1 ORDER BY created_at",
                (endpoint_id,),
            ).fetchall()
        elif endpoint_id is not None:
            rows = conn.execute(
                "SELECT data FROM schedules WHERE endpoint_id = ? ORDER BY created_at",
                (endpoint_id,),
            ).fetchall()
        elif active_only:
            rows = conn.execute(
                "SELECT data FROM schedules WHERE active = 1 ORDER BY created_at"
            ).fetchall()
        else:
            rows = conn.execute("SELECT data FROM schedules ORDER BY created_at").fetchall()
        return [WebhookSchedule.model_validate_json(r["data"]) for r in rows]

    def update_schedule(self, schedule_id: str, **updates: Any) -> WebhookSchedule | None:
        with self._lock:
            conn = self._get_conn()
            row = conn.execute("SELECT data FROM schedules WHERE id = ?", (schedule_id,)).fetchone()
            if row is None:
                return None
            schedule = WebhookSchedule.model_validate_json(row["data"])
            from datetime import datetime, timezone
            for key, value in updates.items():
                if hasattr(schedule, key):
                    setattr(schedule, key, value)
            schedule.updated_at = datetime.now(timezone.utc)
            conn.execute(
                "UPDATE schedules SET data = ?, active = ?, next_run_at = ? WHERE id = ?",
                (
                    schedule.model_dump_json(),
                    1 if schedule.active else 0,
                    schedule.next_run_at.isoformat() if schedule.next_run_at else datetime.now(timezone.utc).isoformat(),
                    schedule_id,
                ),
            )
            conn.commit()
        return schedule

    def delete_schedule(self, schedule_id: str) -> bool:
        with self._lock:
            conn = self._get_conn()
            cursor = conn.execute("DELETE FROM schedules WHERE id = ?", (schedule_id,))
            conn.commit()
            return cursor.rowcount > 0

    def due_schedules(self) -> list[WebhookSchedule]:
        """Get all active schedules that are due to run."""
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT data FROM schedules WHERE active = 1 ORDER BY next_run_at"
        ).fetchall()
        schedules = [WebhookSchedule.model_validate_json(r["data"]) for r in rows]
        return [s for s in schedules if s.is_due()]
