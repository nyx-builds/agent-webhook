"""JSON file-based persistence for agent-webhook."""

from __future__ import annotations

import json
import threading
from pathlib import Path
from typing import Any

from .models import (
    DeliveryAttempt,
    DeliveryStatus,
    EventLogEntry,
    EventSubscription,
    IncomingWebhook,
    RelayRule,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookSchedule,
    WebhookStatus,
)


class WebhookStore:
    """Thread-safe JSON file store for webhooks, deliveries, and relay rules."""

    def __init__(self, path: str | Path = "webhook_store.json"):
        self._path = Path(path)
        self._lock = threading.Lock()
        self._data: dict[str, Any] = {
            "endpoints": {},
            "deliveries": {},
            "incoming": {},
            "relay_rules": {},
            "subscriptions": {},
            "event_log": [],
            "schedules": {},
        }
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            with open(self._path, "r") as f:
                raw = json.load(f)
            # Parse endpoints
            for eid, edata in raw.get("endpoints", {}).items():
                self._data["endpoints"][eid] = WebhookEndpoint.model_validate(edata)
            # Parse deliveries
            for did, ddata in raw.get("deliveries", {}).items():
                self._data["deliveries"][did] = WebhookDelivery.model_validate(ddata)
            # Parse incoming
            for iid, idata in raw.get("incoming", {}).items():
                self._data["incoming"][iid] = IncomingWebhook.model_validate(idata)
            # Parse relay rules
            for rid, rdata in raw.get("relay_rules", {}).items():
                self._data["relay_rules"][rid] = RelayRule.model_validate(rdata)
            # Parse subscriptions
            for sid, sdata in raw.get("subscriptions", {}).items():
                self._data["subscriptions"][sid] = EventSubscription.model_validate(sdata)
            # Parse event log
            for edata in raw.get("event_log", []):
                self._data["event_log"].append(EventLogEntry.model_validate(edata))
            # Parse schedules
            for sid, sdata in raw.get("schedules", {}).items():
                self._data["schedules"][sid] = WebhookSchedule.model_validate(sdata)

    def _save(self) -> None:
        serializable = {
            "endpoints": {k: v.model_dump(mode="json") for k, v in self._data["endpoints"].items()},
            "deliveries": {k: v.model_dump(mode="json") for k, v in self._data["deliveries"].items()},
            "incoming": {k: v.model_dump(mode="json") for k, v in self._data["incoming"].items()},
            "relay_rules": {k: v.model_dump(mode="json") for k, v in self._data["relay_rules"].items()},
            "subscriptions": {k: v.model_dump(mode="json") for k, v in self._data["subscriptions"].items()},
            "event_log": [e.model_dump(mode="json") for e in self._data["event_log"]],
            "schedules": {k: v.model_dump(mode="json") for k, v in self._data["schedules"].items()},
        }
        tmp = self._path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(serializable, f, indent=2, default=str)
        tmp.replace(self._path)

    # --- Endpoints ---

    def add_endpoint(self, endpoint: WebhookEndpoint) -> WebhookEndpoint:
        with self._lock:
            self._data["endpoints"][endpoint.id] = endpoint
            self._save()
        return endpoint

    def get_endpoint(self, endpoint_id: str) -> WebhookEndpoint | None:
        return self._data["endpoints"].get(endpoint_id)

    def list_endpoints(
        self,
        status: WebhookStatus | None = None,
        tag: str | None = None,
    ) -> list[WebhookEndpoint]:
        endpoints = list(self._data["endpoints"].values())
        if status is not None:
            endpoints = [e for e in endpoints if e.status == status]
        if tag is not None:
            endpoints = [e for e in endpoints if tag in e.tags]
        return sorted(endpoints, key=lambda e: e.created_at)

    def update_endpoint(self, endpoint_id: str, **updates: Any) -> WebhookEndpoint | None:
        with self._lock:
            endpoint = self._data["endpoints"].get(endpoint_id)
            if endpoint is None:
                return None
            for key, value in updates.items():
                if hasattr(endpoint, key):
                    setattr(endpoint, key, value)
            from datetime import datetime, timezone
            endpoint.updated_at = datetime.now(timezone.utc)
            self._save()
        return endpoint

    def delete_endpoint(self, endpoint_id: str) -> bool:
        with self._lock:
            if endpoint_id not in self._data["endpoints"]:
                return False
            del self._data["endpoints"][endpoint_id]
            self._save()
        return True

    # --- Deliveries ---

    def add_delivery(self, delivery: WebhookDelivery) -> WebhookDelivery:
        with self._lock:
            self._data["deliveries"][delivery.id] = delivery
            self._save()
        return delivery

    def get_delivery(self, delivery_id: str) -> WebhookDelivery | None:
        return self._data["deliveries"].get(delivery_id)

    def list_deliveries(
        self,
        endpoint_id: str | None = None,
        status: DeliveryStatus | None = None,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[WebhookDelivery]:
        deliveries = list(self._data["deliveries"].values())
        if endpoint_id is not None:
            deliveries = [d for d in deliveries if d.endpoint_id == endpoint_id]
        if status is not None:
            deliveries = [d for d in deliveries if d.status == status]
        if event_type is not None:
            deliveries = [d for d in deliveries if d.event_type == event_type]
        return sorted(deliveries, key=lambda d: d.created_at, reverse=True)[:limit]

    def update_delivery(self, delivery_id: str, **updates: Any) -> WebhookDelivery | None:
        with self._lock:
            delivery = self._data["deliveries"].get(delivery_id)
            if delivery is None:
                return None
            for key, value in updates.items():
                if hasattr(delivery, key):
                    setattr(delivery, key, value)
            self._save()
        return delivery

    def add_delivery_attempt(self, delivery_id: str, attempt: DeliveryAttempt) -> WebhookDelivery | None:
        with self._lock:
            delivery = self._data["deliveries"].get(delivery_id)
            if delivery is None:
                return None
            delivery.attempts.append(attempt)
            self._save()
        return delivery

    # --- Incoming Webhooks ---

    def add_incoming(self, incoming: IncomingWebhook) -> IncomingWebhook:
        with self._lock:
            self._data["incoming"][incoming.id] = incoming
            self._save()
        return incoming

    def get_incoming(self, incoming_id: str) -> IncomingWebhook | None:
        return self._data["incoming"].get(incoming_id)

    def list_incoming(
        self,
        path: str | None = None,
        processed: bool | None = None,
        limit: int = 100,
    ) -> list[IncomingWebhook]:
        incoming = list(self._data["incoming"].values())
        if path is not None:
            incoming = [i for i in incoming if i.path == path]
        if processed is not None:
            incoming = [i for i in incoming if i.processed == processed]
        return sorted(incoming, key=lambda i: i.received_at, reverse=True)[:limit]

    # --- Relay Rules ---

    def add_relay_rule(self, rule: RelayRule) -> RelayRule:
        with self._lock:
            self._data["relay_rules"][rule.id] = rule
            self._save()
        return rule

    def get_relay_rule(self, rule_id: str) -> RelayRule | None:
        return self._data["relay_rules"].get(rule_id)

    def list_relay_rules(self, active_only: bool = False) -> list[RelayRule]:
        rules = list(self._data["relay_rules"].values())
        if active_only:
            rules = [r for r in rules if r.active]
        return sorted(rules, key=lambda r: r.created_at)

    def delete_relay_rule(self, rule_id: str) -> bool:
        with self._lock:
            if rule_id not in self._data["relay_rules"]:
                return False
            del self._data["relay_rules"][rule_id]
            self._save()
        return True

    def update_relay_rule(self, rule_id: str, **updates: Any) -> RelayRule | None:
        with self._lock:
            rule = self._data["relay_rules"].get(rule_id)
            if rule is None:
                return None
            for key, value in updates.items():
                if hasattr(rule, key):
                    setattr(rule, key, value)
            self._save()
        return rule

    # --- Stats ---

    def get_stats(self, endpoint_id: str) -> dict[str, Any] | None:
        endpoint = self.get_endpoint(endpoint_id)
        if endpoint is None:
            return None
        deliveries = [d for d in self._data["deliveries"].values() if d.endpoint_id == endpoint_id]
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
        from .models import WebhookStats
        status_counts = {}
        for d in deliveries:
            status_counts[d.status] = status_counts.get(d.status, 0) + 1

        durations = []
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
            "successful": status_counts.get(DeliveryStatus.SUCCESS, 0),
            "failed": status_counts.get(DeliveryStatus.FAILED, 0),
            "pending": status_counts.get(DeliveryStatus.PENDING, 0),
            "retrying": status_counts.get(DeliveryStatus.RETRYING, 0),
            "abandoned": status_counts.get(DeliveryStatus.ABANDONED, 0),
            "dead_letter": status_counts.get(DeliveryStatus.DEAD_LETTER, 0),
            "avg_duration_ms": sum(durations) / len(durations) if durations else None,
            "last_delivery_at": last_delivery,
            "last_success_at": last_success,
            "last_failure_at": last_failure,
        }

    def get_all_stats(self) -> list[dict[str, Any]]:
        return [self.get_stats(eid) for eid in self._data["endpoints"] if self.get_stats(eid) is not None]

    # --- Event Subscriptions ---

    def add_subscription(self, sub: EventSubscription) -> EventSubscription:
        with self._lock:
            self._data["subscriptions"][sub.id] = sub
            self._save()
        return sub

    def get_subscription(self, subscription_id: str) -> EventSubscription | None:
        return self._data["subscriptions"].get(subscription_id)

    def list_subscriptions(
        self,
        endpoint_id: str | None = None,
    ) -> list[EventSubscription]:
        subs = list(self._data["subscriptions"].values())
        if endpoint_id is not None:
            subs = [s for s in subs if s.endpoint_id == endpoint_id]
        return sorted(subs, key=lambda s: s.created_at)

    def delete_subscription(self, subscription_id: str) -> bool:
        with self._lock:
            if subscription_id not in self._data["subscriptions"]:
                return False
            del self._data["subscriptions"][subscription_id]
            self._save()
        return True

    # --- Event Log ---

    def add_event_log(self, entry: EventLogEntry) -> EventLogEntry:
        with self._lock:
            self._data["event_log"].append(entry)
            # Keep only the last 1000 entries to prevent unbounded growth
            if len(self._data["event_log"]) > 1000:
                self._data["event_log"] = self._data["event_log"][-1000:]
            self._save()
        return entry

    def list_event_log(
        self,
        event_type: str | None = None,
        endpoint_id: str | None = None,
        limit: int = 100,
    ) -> list[EventLogEntry]:
        entries = list(self._data["event_log"])
        if event_type is not None:
            entries = [e for e in entries if e.event_type == event_type]
        if endpoint_id is not None:
            entries = [e for e in entries if e.endpoint_id == endpoint_id]
        return sorted(entries, key=lambda e: e.timestamp, reverse=True)[:limit]

    def pending_deliveries(self) -> list[WebhookDelivery]:
        """Get all deliveries that are pending or retrying and due for delivery."""
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc)
        result = []
        for d in self._data["deliveries"].values():
            if d.status == DeliveryStatus.PENDING:
                # Skip scheduled deliveries that aren't due yet
                if d.scheduled_at is not None and d.scheduled_at > now:
                    continue
                result.append(d)
            elif d.status == DeliveryStatus.RETRYING and (d.next_retry_at is None or d.next_retry_at <= now):
                result.append(d)
        return sorted(result, key=lambda d: d.created_at)

    # --- Schedules ---

    def add_schedule(self, schedule: WebhookSchedule) -> WebhookSchedule:
        with self._lock:
            self._data["schedules"][schedule.id] = schedule
            self._save()
        return schedule

    def get_schedule(self, schedule_id: str) -> WebhookSchedule | None:
        return self._data["schedules"].get(schedule_id)

    def list_schedules(
        self,
        endpoint_id: str | None = None,
        active_only: bool = False,
    ) -> list[WebhookSchedule]:
        schedules = list(self._data["schedules"].values())
        if endpoint_id is not None:
            schedules = [s for s in schedules if s.endpoint_id == endpoint_id]
        if active_only:
            schedules = [s for s in schedules if s.active]
        return sorted(schedules, key=lambda s: s.created_at)

    def update_schedule(self, schedule_id: str, **updates: Any) -> WebhookSchedule | None:
        with self._lock:
            schedule = self._data["schedules"].get(schedule_id)
            if schedule is None:
                return None
            for key, value in updates.items():
                if hasattr(schedule, key):
                    setattr(schedule, key, value)
            from datetime import datetime, timezone
            schedule.updated_at = datetime.now(timezone.utc)
            self._save()
        return schedule

    def delete_schedule(self, schedule_id: str) -> bool:
        with self._lock:
            if schedule_id not in self._data["schedules"]:
                return False
            del self._data["schedules"][schedule_id]
            self._save()
        return True

    def due_schedules(self) -> list[WebhookSchedule]:
        """Get all active schedules that are due to run."""
        return [s for s in self._data["schedules"].values() if s.is_due()]
