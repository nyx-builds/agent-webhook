"""Alert rules and notification system for agent-webhook.

Defines conditions that trigger alerts and notification channels that fire
when those conditions are met. This lets agents be *proactively* notified
about delivery problems instead of polling.

Alert conditions (evaluated against store state):

* ``circuit_open``       — A circuit breaker is in the OPEN state.
* ``dlq_threshold``      — Dead-letter queue count exceeds a threshold.
* ``endpoint_failure_rate`` — An endpoint's failure rate exceeds a percentage.
* ``endpoint_down``      — An endpoint has been disabled.
* ``delivery_stalled``   — Deliveries stuck in PENDING/RETRYING longer than a threshold.

Notification channels:

* ``webhook``  — Send a notification to a registered webhook endpoint.
* ``log``      — Write to the event audit log.
* ``callback`` — Invoke a Python callable.

Usage::

    from agent_webhook.alerts import AlertManager, AlertRule, AlertCondition
    from agent_webhook.service import WebhookService

    service = WebhookService(store_path="webhooks.db")
    manager = AlertManager(service)

    rule = AlertRule(
        name="High DLQ",
        condition=AlertCondition.DLQ_THRESHOLD,
        threshold=10,
        channels=[LogChannel(), WebhookChannel(endpoint_id="notify-ep")],
    )
    manager.add_rule(rule)

    # Evaluate all rules (call periodically or after deliveries)
    fired = manager.evaluate_all()
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any, Callable, Protocol

from .models import DeliveryStatus, WebhookStatus
from .service import WebhookService

logger = logging.getLogger(__name__)


# ─── Enums ───────────────────────────────────────────────────────────


class AlertCondition(str, Enum):
    """Conditions that can trigger an alert."""

    CIRCUIT_OPEN = "circuit_open"
    DLQ_THRESHOLD = "dlq_threshold"
    ENDPOINT_FAILURE_RATE = "endpoint_failure_rate"
    ENDPOINT_DOWN = "endpoint_down"
    DELIVERY_STALLED = "delivery_stalled"


class AlertSeverity(str, Enum):
    """Severity levels for alerts."""

    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"


class AlertStatus(str, Enum):
    """Status of an alert evaluation."""

    FIRING = "firing"
    RESOLVED = "resolved"


# ─── Notification Channels ───────────────────────────────────────────


class NotificationChannel(Protocol):
    """Protocol for notification delivery channels."""

    async def notify(self, alert: AlertEvent) -> None:
        """Deliver the alert notification."""
        ...

    def to_dict(self) -> dict[str, Any]:
        """Serialize channel config."""
        ...


@dataclass
class LogChannel:
    """Writes alert events to the webhook event audit log."""

    def async_notify(self, alert: AlertEvent) -> None:  # sync variant
        pass

    async def notify(self, alert: AlertEvent) -> None:
        # Deferred import to avoid circular reference at module load
        pass

    def to_dict(self) -> dict[str, Any]:
        return {"type": "log"}


@dataclass
class WebhookChannel:
    """Sends alerts to a registered webhook endpoint via the service engine."""

    endpoint_id: str
    event_type: str = "alert"

    async def notify(self, alert: AlertEvent) -> None:
        # Import here to avoid circular dependency; the notify is called
        # at runtime with a fully-constructed service available via the alert.
        # The actual send is handled by AlertManager._dispatch which has the
        # service reference.
        pass

    def to_dict(self) -> dict[str, Any]:
        return {"type": "webhook", "endpoint_id": self.endpoint_id, "event_type": self.event_type}


@dataclass
class CallbackChannel:
    """Invokes a Python callable with the alert event (sync or async)."""

    callback: Callable[[AlertEvent], Any]
    name: str = "callback"

    async def notify(self, alert: AlertEvent) -> None:
        result = self.callback(alert)
        if asyncio.iscoroutine(result):
            await result

    def to_dict(self) -> dict[str, Any]:
        return {"type": "callback", "name": self.name}


# ─── Alert Event ─────────────────────────────────────────────────────


@dataclass
class AlertEvent:
    """A concrete alert that has been triggered or resolved."""

    rule_id: str
    rule_name: str
    condition: AlertCondition
    severity: AlertSeverity
    status: AlertStatus
    message: str
    details: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def to_dict(self) -> dict[str, Any]:
        return {
            "rule_id": self.rule_id,
            "rule_name": self.rule_name,
            "condition": self.condition.value,
            "severity": self.severity.value,
            "status": self.status.value,
            "message": self.message,
            "details": self.details,
            "timestamp": self.timestamp.isoformat(),
        }


# ─── Alert Rule ──────────────────────────────────────────────────────


@dataclass
class AlertRule:
    """A rule that defines a condition and associated notification channels."""

    name: str
    condition: AlertCondition
    severity: AlertSeverity = AlertSeverity.WARNING
    threshold: float = 0.0
    cooldown_seconds: float = 300.0
    # scope: endpoint_id, tag, or None for all
    endpoint_id: str | None = None
    tag: str | None = None
    channels: list[NotificationChannel] = field(default_factory=list)
    enabled: bool = True
    id: str = field(default="")
    # Internal state (not persisted across restarts in this version)
    _last_fired: dict[str, datetime] = field(default_factory=dict, repr=False)
    _currently_firing: dict[str, bool] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not self.name:
            raise ValueError("Alert rule name is required")
        if not self.id:
            import uuid as _uuid
            self.id = str(_uuid.uuid4())
        if not self.channels:
            self.channels = [LogChannel()]

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "condition": self.condition.value,
            "severity": self.severity.value,
            "threshold": self.threshold,
            "cooldown_seconds": self.cooldown_seconds,
            "endpoint_id": self.endpoint_id,
            "tag": self.tag,
            "channels": [c.to_dict() if hasattr(c, "to_dict") else {"type": str(c)} for c in self.channels],
            "enabled": self.enabled,
        }

    def _in_cooldown(self, scope_key: str) -> bool:
        last = self._last_fired.get(scope_key)
        if last is None:
            return False
        return (datetime.now(timezone.utc) - last).total_seconds() < self.cooldown_seconds


# ─── Alert Manager ───────────────────────────────────────────────────


class AlertManager:
    """Evaluates alert rules against the current store state and dispatches notifications.

    Usage::

        manager = AlertManager(service)
        manager.add_rule(AlertRule(...))
        fired = await manager.evaluate_all()  # call periodically
    """

    def __init__(self, service: WebhookService) -> None:
        self._service = service
        self._rules: list[AlertRule] = []
        self._fired_history: list[AlertEvent] = []

    @property
    def rules(self) -> list[AlertRule]:
        return list(self._rules)

    @property
    def fired_history(self) -> list[AlertEvent]:
        return list(self._fired_history)

    def add_rule(self, rule: AlertRule) -> None:
        """Register an alert rule."""
        self._rules.append(rule)

    def remove_rule(self, rule_id: str) -> bool:
        before = len(self._rules)
        self._rules = [r for r in self._rules if r.id != rule_id]
        return len(self._rules) < before

    def get_rule(self, rule_id: str) -> AlertRule | None:
        for r in self._rules:
            if r.id == rule_id:
                return r
        return None

    def clear_rules(self) -> None:
        self._rules.clear()
        self._fired_history.clear()

    def _get_scoped_endpoints(self, rule: AlertRule) -> list[str]:
        """Resolve which endpoint IDs a rule applies to."""
        endpoints = self._service.list_endpoints()
        if rule.endpoint_id:
            return [rule.endpoint_id] if any(e.id == rule.endpoint_id for e in endpoints) else []
        if rule.tag:
            return [e.id for e in endpoints if rule.tag in e.tags]
        return [e.id for e in endpoints]

    # ── Condition Evaluators ──────────────────────────────────────────

    def _eval_circuit_open(
        self, rule: AlertRule, endpoint_ids: list[str]
    ) -> list[AlertEvent]:
        """Evaluate circuit_open condition."""
        events: list[AlertEvent] = []
        for eid in endpoint_ids:
            state = self._service.get_circuit_breaker_state(eid)
            if state and state.get("state") == "open":
                scope_key = f"circuit:{eid}"
                if rule._in_cooldown(scope_key):
                    continue
                events.append(AlertEvent(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    condition=rule.condition,
                    severity=rule.severity,
                    status=AlertStatus.FIRING,
                    message=f"Circuit breaker OPEN for endpoint {state.get('endpoint_id', eid)}",
                    details={
                        "endpoint_id": eid,
                        "state": state,
                    },
                ))
                rule._last_fired[scope_key] = datetime.now(timezone.utc)
                rule._currently_firing[scope_key] = True
            else:
                scope_key = f"circuit:{eid}"
                if rule._currently_firing.get(scope_key):
                    events.append(AlertEvent(
                        rule_id=rule.id,
                        rule_name=rule.name,
                        condition=rule.condition,
                        severity=rule.severity,
                        status=AlertStatus.RESOLVED,
                        message=f"Circuit breaker recovered for endpoint {eid}",
                        details={"endpoint_id": eid},
                    ))
                    rule._currently_firing[scope_key] = False
        return events

    def _eval_dlq_threshold(self, rule: AlertRule) -> list[AlertEvent]:
        """Evaluate dlq_threshold condition."""
        events: list[AlertEvent] = []
        threshold = int(rule.threshold) if rule.threshold else 10

        # Get total DLQ count (optionally per-endpoint)
        if rule.endpoint_id:
            count = self._service.dead_letter_count(endpoint_id=rule.endpoint_id)
            scope_key = f"dlq:{rule.endpoint_id}"
            if count >= threshold and not rule._in_cooldown(scope_key):
                events.append(AlertEvent(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    condition=rule.condition,
                    severity=rule.severity,
                    status=AlertStatus.FIRING,
                    message=f"Dead letter queue for endpoint {rule.endpoint_id} at {count} (threshold: {threshold})",
                    details={"endpoint_id": rule.endpoint_id, "count": count, "threshold": threshold},
                ))
                rule._last_fired[scope_key] = datetime.now(timezone.utc)
                rule._currently_firing[scope_key] = True
            elif count < threshold and rule._currently_firing.get(scope_key):
                events.append(AlertEvent(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    condition=rule.condition,
                    severity=rule.severity,
                    status=AlertStatus.RESOLVED,
                    message=f"Dead letter queue below threshold for {rule.endpoint_id} ({count})",
                    details={"endpoint_id": rule.endpoint_id, "count": count},
                ))
                rule._currently_firing[scope_key] = False
        else:
            count = self._service.dead_letter_count()
            scope_key = "dlq:all"
            if count >= threshold and not rule._in_cooldown(scope_key):
                events.append(AlertEvent(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    condition=rule.condition,
                    severity=rule.severity,
                    status=AlertStatus.FIRING,
                    message=f"Global dead letter queue at {count} (threshold: {threshold})",
                    details={"count": count, "threshold": threshold},
                ))
                rule._last_fired[scope_key] = datetime.now(timezone.utc)
                rule._currently_firing[scope_key] = True
            elif count < threshold and rule._currently_firing.get(scope_key):
                events.append(AlertEvent(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    condition=rule.condition,
                    severity=rule.severity,
                    status=AlertStatus.RESOLVED,
                    message=f"Global dead letter queue below threshold ({count})",
                    details={"count": count},
                ))
                rule._currently_firing[scope_key] = False
        return events

    def _eval_endpoint_failure_rate(
        self, rule: AlertRule, endpoint_ids: list[str]
    ) -> list[AlertEvent]:
        """Evaluate endpoint_failure_rate condition."""
        events: list[AlertEvent] = []
        threshold_pct = rule.threshold if rule.threshold > 0 else 50.0

        for eid in endpoint_ids:
            stats = self._service.get_stats(eid)
            if stats is None:
                continue
            total = stats.get("total_deliveries", 0)
            if total < 5:
                continue  # Need a minimum sample
            failed = stats.get("failed", 0) + stats.get("abandoned", 0) + stats.get("dead_letter", 0)
            completed = stats.get("successful", 0) + failed
            if completed == 0:
                continue
            failure_rate = (failed / completed) * 100
            scope_key = f"frate:{eid}"

            if failure_rate >= threshold_pct and not rule._in_cooldown(scope_key):
                events.append(AlertEvent(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    condition=rule.condition,
                    severity=rule.severity,
                    status=AlertStatus.FIRING,
                    message=f"Endpoint {stats.get('endpoint_name', eid)} failure rate at {failure_rate:.1f}% (threshold: {threshold_pct}%)",
                    details={
                        "endpoint_id": eid,
                        "failure_rate_pct": round(failure_rate, 2),
                        "threshold_pct": threshold_pct,
                        "total": total,
                        "failed": failed,
                    },
                ))
                rule._last_fired[scope_key] = datetime.now(timezone.utc)
                rule._currently_firing[scope_key] = True
            elif failure_rate < threshold_pct and rule._currently_firing.get(scope_key):
                events.append(AlertEvent(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    condition=rule.condition,
                    severity=rule.severity,
                    status=AlertStatus.RESOLVED,
                    message=f"Endpoint {eid} failure rate back to normal ({failure_rate:.1f}%)",
                    details={"endpoint_id": eid, "failure_rate_pct": round(failure_rate, 2)},
                ))
                rule._currently_firing[scope_key] = False
        return events

    def _eval_endpoint_down(
        self, rule: AlertRule, endpoint_ids: list[str]
    ) -> list[AlertEvent]:
        """Evaluate endpoint_down condition."""
        events: list[AlertEvent] = []
        for eid in endpoint_ids:
            ep = self._service.get_endpoint(eid)
            if ep is None:
                continue
            scope_key = f"down:{eid}"
            if ep.status == WebhookStatus.DISABLED and not rule._in_cooldown(scope_key):
                events.append(AlertEvent(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    condition=rule.condition,
                    severity=rule.severity,
                    status=AlertStatus.FIRING,
                    message=f"Endpoint '{ep.name}' is DISABLED",
                    details={"endpoint_id": eid, "endpoint_name": ep.name},
                ))
                rule._last_fired[scope_key] = datetime.now(timezone.utc)
                rule._currently_firing[scope_key] = True
            elif ep.status != WebhookStatus.DISABLED and rule._currently_firing.get(scope_key):
                events.append(AlertEvent(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    condition=rule.condition,
                    severity=rule.severity,
                    status=AlertStatus.RESOLVED,
                    message=f"Endpoint '{ep.name}' is back online ({ep.status.value})",
                    details={"endpoint_id": eid, "endpoint_name": ep.name},
                ))
                rule._currently_firing[scope_key] = False
        return events

    def _eval_delivery_stalled(
        self, rule: AlertRule, endpoint_ids: list[str]
    ) -> list[AlertEvent]:
        """Evaluate delivery_stalled condition."""
        events: list[AlertEvent] = []
        stall_minutes = rule.threshold if rule.threshold > 0 else 30
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=stall_minutes)

        # Check all pending/retrying deliveries
        pending = self._service.list_deliveries(
            status=DeliveryStatus.PENDING, limit=500
        )
        retrying = self._service.list_deliveries(
            status=DeliveryStatus.RETRYING, limit=500
        )
        stalled = [d for d in pending + retrying if d.created_at < cutoff]

        # Filter by scope
        if rule.endpoint_id:
            stalled = [d for d in stalled if d.endpoint_id == rule.endpoint_id]
        elif rule.tag:
            scoped = set(self._get_scoped_endpoints(rule))
            stalled = [d for d in stalled if d.endpoint_id in scoped]

        scope_key = f"stalled:{rule.endpoint_id or rule.tag or 'all'}"
        if len(stalled) > 0 and not rule._in_cooldown(scope_key):
            events.append(AlertEvent(
                rule_id=rule.id,
                rule_name=rule.name,
                condition=rule.condition,
                severity=rule.severity,
                status=AlertStatus.FIRING,
                message=f"{len(stalled)} deliveries stalled for >{stall_minutes} min",
                details={
                    "stalled_count": len(stalled),
                    "stall_threshold_minutes": stall_minutes,
                    "stalled_delivery_ids": [d.id for d in stalled[:10]],
                },
            ))
            rule._last_fired[scope_key] = datetime.now(timezone.utc)
            rule._currently_firing[scope_key] = True
        elif len(stalled) == 0 and rule._currently_firing.get(scope_key):
            events.append(AlertEvent(
                rule_id=rule.id,
                rule_name=rule.name,
                condition=rule.condition,
                severity=rule.severity,
                status=AlertStatus.RESOLVED,
                message="No stalled deliveries",
                details={"stall_threshold_minutes": stall_minutes},
            ))
            rule._currently_firing[scope_key] = False
        return events

    # ── Evaluation ────────────────────────────────────────────────────

    async def evaluate_all(self) -> list[AlertEvent]:
        """Evaluate all enabled rules and dispatch notifications for fired alerts.

        Returns the list of all alert events (firing and resolved) generated.
        """
        all_events: list[AlertEvent] = []

        for rule in self._rules:
            if not rule.enabled:
                continue
            try:
                events = await self._evaluate_rule(rule)
                all_events.extend(events)
            except Exception:
                logger.exception("Error evaluating alert rule '%s'", rule.name)

        # Dispatch notifications for all events
        for event in all_events:
            await self._dispatch(event)
            self._fired_history.append(event)

        # Cap history
        if len(self._fired_history) > 1000:
            self._fired_history = self._fired_history[-500:]

        return all_events

    async def _evaluate_rule(self, rule: AlertRule) -> list[AlertEvent]:
        """Evaluate a single rule and return generated events."""
        events: list[AlertEvent] = []

        if rule.condition == AlertCondition.CIRCUIT_OPEN:
            endpoint_ids = self._get_scoped_endpoints(rule)
            events = self._eval_circuit_open(rule, endpoint_ids)

        elif rule.condition == AlertCondition.DLQ_THRESHOLD:
            events = self._eval_dlq_threshold(rule)

        elif rule.condition == AlertCondition.ENDPOINT_FAILURE_RATE:
            endpoint_ids = self._get_scoped_endpoints(rule)
            events = self._eval_endpoint_failure_rate(rule, endpoint_ids)

        elif rule.condition == AlertCondition.ENDPOINT_DOWN:
            endpoint_ids = self._get_scoped_endpoints(rule)
            events = self._eval_endpoint_down(rule, endpoint_ids)

        elif rule.condition == AlertCondition.DELIVERY_STALLED:
            events = self._eval_delivery_stalled(rule, [])

        return events

    async def _dispatch(self, event: AlertEvent) -> None:
        """Dispatch an alert event to all notification channels of its rule."""
        rule = self.get_rule(event.rule_id)
        if rule is None:
            return

        for channel in rule.channels:
            try:
                if isinstance(channel, WebhookChannel):
                    await self._dispatch_webhook(channel, event)
                elif isinstance(channel, CallbackChannel):
                    await channel.notify(event)
                elif isinstance(channel, LogChannel):
                    self._dispatch_log(event)
                else:
                    # Try protocol-style notify
                    if hasattr(channel, "notify"):
                        result = channel.notify(event)
                        if asyncio.iscoroutine(result):
                            await result
            except Exception:
                logger.exception(
                    "Failed to dispatch alert '%s' via channel %s",
                    event.rule_name,
                    type(channel).__name__,
                )

    async def _dispatch_webhook(self, channel: WebhookChannel, event: AlertEvent) -> None:
        """Send alert notification to a webhook endpoint."""
        payload = {
            "alert": event.to_dict(),
            "source": "agent-webhook",
        }
        try:
            await self._service.send_webhook(
                endpoint_id=channel.endpoint_id,
                payload=payload,
                event_type=channel.event_type or "alert",
            )
        except Exception:
            logger.debug("Webhook alert dispatch failed for endpoint %s", channel.endpoint_id)

    def _dispatch_log(self, event: AlertEvent) -> None:
        """Write alert to the event audit log."""
        try:
            self._service.log_event(
                event_type=f"alert.{event.condition.value}.{event.status.value}",
                details=event.details,
                endpoint_id=event.details.get("endpoint_id"),
            )
        except Exception:
            logger.debug("Failed to log alert event")

    def get_active_alerts(self) -> list[AlertEvent]:
        """Get all currently-firing alerts from history."""
        return [e for e in self._fired_history if e.status == AlertStatus.FIRING]

    def get_alert_summary(self) -> dict[str, Any]:
        """Get a summary of alert state."""
        from collections import Counter
        status_counts = Counter(e.status.value for e in self._fired_history)
        severity_counts = Counter(e.severity.value for e in self._fired_history if e.status == AlertStatus.FIRING)
        return {
            "total_rules": len(self._rules),
            "enabled_rules": sum(1 for r in self._rules if r.enabled),
            "total_events": len(self._fired_history),
            "firing": status_counts.get(AlertStatus.FIRING.value, 0),
            "resolved": status_counts.get(AlertStatus.RESOLVED.value, 0),
            "active_by_severity": dict(severity_counts),
            "rules": [r.to_dict() for r in self._rules],
        }


# ─── Preset Rules ────────────────────────────────────────────────────


def default_alert_rules(
    notify_endpoint_id: str | None = None,
) -> list[AlertRule]:
    """Return a set of sensible default alert rules.

    Args:
        notify_endpoint_id: If provided, adds a webhook channel to each rule
            pointing at this endpoint. Otherwise, rules use log-only channels.
    """
    channels: list[NotificationChannel] = [LogChannel()]
    if notify_endpoint_id:
        channels.append(WebhookChannel(endpoint_id=notify_endpoint_id))

    return [
        AlertRule(
            name="Circuit Breaker Open",
            condition=AlertCondition.CIRCUIT_OPEN,
            severity=AlertSeverity.CRITICAL,
            cooldown_seconds=600,
            channels=list(channels),
        ),
        AlertRule(
            name="High Dead Letter Queue",
            condition=AlertCondition.DLQ_THRESHOLD,
            severity=AlertSeverity.CRITICAL,
            threshold=10,
            cooldown_seconds=300,
            channels=list(channels),
        ),
        AlertRule(
            name="High Endpoint Failure Rate",
            condition=AlertCondition.ENDPOINT_FAILURE_RATE,
            severity=AlertSeverity.WARNING,
            threshold=50.0,
            cooldown_seconds=300,
            channels=list(channels),
        ),
        AlertRule(
            name="Endpoint Disabled",
            condition=AlertCondition.ENDPOINT_DOWN,
            severity=AlertSeverity.WARNING,
            cooldown_seconds=600,
            channels=list(channels),
        ),
        AlertRule(
            name="Stalled Deliveries",
            condition=AlertCondition.DELIVERY_STALLED,
            severity=AlertSeverity.WARNING,
            threshold=30,
            cooldown_seconds=300,
            channels=list(channels),
        ),
    ]
