"""Service layer for agent-webhook — business logic on top of store and engine."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from .engine import DeliveryEngine
from .models import (
    DeliveryAttempt,
    DeliveryStatus,
    EventLogEntry,
    EventSubscription,
    Header,
    IncomingWebhook,
    RelayRule,
    RetryPolicy,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookMethod,
    WebhookStatus,
)
from .store import WebhookStore


class WebhookService:
    """High-level service for webhook management."""

    def __init__(self, store: WebhookStore | None = None, store_path: str = "webhook_store.json"):
        self._store = store or WebhookStore(store_path)
        self._engine = DeliveryEngine(self._store)

    @property
    def store(self) -> WebhookStore:
        return self._store

    @property
    def engine(self) -> DeliveryEngine:
        return self._engine

    # ── Endpoint Management ──────────────────────────────────────────

    def create_endpoint(
        self,
        name: str,
        url: str,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        tags: list[str] | None = None,
        secret: str | None = None,
        timeout_seconds: float = 30.0,
        description: str | None = None,
        max_retries: int = 3,
    ) -> WebhookEndpoint:
        """Create and register a new webhook endpoint."""
        header_objs = [Header(name=k, value=v) for k, v in (headers or {}).items()]
        endpoint = WebhookEndpoint(
            name=name,
            url=url,
            method=WebhookMethod(method),
            headers=header_objs,
            tags=tags or [],
            secret=secret,
            timeout_seconds=timeout_seconds,
            description=description,
            retry_policy=RetryPolicy(max_retries=max_retries),
        )
        return self._store.add_endpoint(endpoint)

    def get_endpoint(self, endpoint_id: str) -> WebhookEndpoint | None:
        return self._store.get_endpoint(endpoint_id)

    def list_endpoints(
        self,
        status: WebhookStatus | None = None,
        tag: str | None = None,
    ) -> list[WebhookEndpoint]:
        return self._store.list_endpoints(status=status, tag=tag)

    def update_endpoint(self, endpoint_id: str, **updates: Any) -> WebhookEndpoint | None:
        return self._store.update_endpoint(endpoint_id, **updates)

    def pause_endpoint(self, endpoint_id: str) -> WebhookEndpoint | None:
        return self._store.update_endpoint(endpoint_id, status=WebhookStatus.PAUSED)

    def resume_endpoint(self, endpoint_id: str) -> WebhookEndpoint | None:
        return self._store.update_endpoint(endpoint_id, status=WebhookStatus.ACTIVE)

    def disable_endpoint(self, endpoint_id: str) -> WebhookEndpoint | None:
        return self._store.update_endpoint(endpoint_id, status=WebhookStatus.DISABLED)

    def delete_endpoint(self, endpoint_id: str) -> bool:
        return self._store.delete_endpoint(endpoint_id)

    # ── Delivery Management ──────────────────────────────────────────

    async def send_webhook(
        self,
        endpoint_id: str,
        payload: dict[str, Any],
        event_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> WebhookDelivery:
        """Send a webhook delivery to an endpoint."""
        return await self._engine.send(
            endpoint_id=endpoint_id,
            payload=payload,
            event_type=event_type,
            metadata=metadata or {},
            headers=headers or {},
        )

    async def batch_send(
        self,
        endpoint_ids: list[str],
        payload: dict[str, Any],
        event_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> list[WebhookDelivery]:
        """Send the same payload to multiple endpoints."""
        results = []
        for eid in endpoint_ids:
            result = await self._engine.send(
                endpoint_id=eid,
                payload=payload,
                event_type=event_type,
                metadata=metadata or {},
                headers=headers or {},
            )
            results.append(result)
        return results

    async def retry_delivery(self, delivery_id: str) -> WebhookDelivery | None:
        """Retry a failed delivery."""
        d = self._store.get_delivery(delivery_id)
        if d is None:
            return None
        if d.status in (DeliveryStatus.SUCCESS,):
            return d
        # Reset status to pending
        self._store.update_delivery(d.id, status=DeliveryStatus.PENDING, next_retry_at=None)
        return await self._engine.process_delivery(d.id)

    def cancel_delivery(self, delivery_id: str) -> WebhookDelivery | None:
        """Cancel a pending or retrying delivery."""
        d = self._store.get_delivery(delivery_id)
        if d is None:
            return None
        if d.status in (DeliveryStatus.PENDING, DeliveryStatus.RETRYING):
            self._store.update_delivery(d.id, status=DeliveryStatus.ABANDONED)
            return self._store.get_delivery(delivery_id)
        return d

    def get_delivery(self, delivery_id: str) -> WebhookDelivery | None:
        return self._store.get_delivery(delivery_id)

    def list_deliveries(
        self,
        endpoint_id: str | None = None,
        status: DeliveryStatus | None = None,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[WebhookDelivery]:
        return self._store.list_deliveries(
            endpoint_id=endpoint_id,
            status=status,
            event_type=event_type,
            limit=limit,
        )

    async def process_pending(self) -> list[WebhookDelivery]:
        """Process all pending/ready deliveries."""
        return await self._engine.process_pending()

    # ── Event Subscriptions ──────────────────────────────────────────

    def add_subscription(
        self,
        endpoint_id: str,
        event_types: list[str],
    ) -> EventSubscription | None:
        """Subscribe an endpoint to specific event types."""
        ep = self._store.get_endpoint(endpoint_id)
        if ep is None:
            return None
        sub = EventSubscription(
            endpoint_id=endpoint_id,
            event_types=event_types,
        )
        self._store.add_subscription(sub)
        return sub

    def remove_subscription(self, subscription_id: str) -> bool:
        return self._store.delete_subscription(subscription_id)

    def list_subscriptions(
        self,
        endpoint_id: str | None = None,
    ) -> list[EventSubscription]:
        return self._store.list_subscriptions(endpoint_id=endpoint_id)

    async def send_to_subscribers(
        self,
        event_type: str,
        payload: dict[str, Any],
        metadata: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> list[WebhookDelivery]:
        """Send a payload to all endpoints subscribed to an event type."""
        subs = self._store.list_subscriptions()
        matching_endpoint_ids = []
        for sub in subs:
            if event_type in sub.event_types:
                ep = self._store.get_endpoint(sub.endpoint_id)
                if ep and ep.is_active():
                    matching_endpoint_ids.append(sub.endpoint_id)

        if not matching_endpoint_ids:
            return []

        return await self.batch_send(
            endpoint_ids=matching_endpoint_ids,
            payload=payload,
            event_type=event_type,
            metadata=metadata or {},
            headers=headers or {},
        )

    # ── Health Check ─────────────────────────────────────────────────

    async def health_check(self, endpoint_id: str) -> dict[str, Any]:
        """Test endpoint connectivity and verify HMAC signature if configured."""
        ep = self._store.get_endpoint(endpoint_id)
        if ep is None:
            return {"endpoint_id": endpoint_id, "status": "not_found"}

        # Create a test delivery
        test_payload = {
            "ping": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "agent_webhook_health_check": True,
        }

        delivery = WebhookDelivery(
            endpoint_id=endpoint_id,
            payload=test_payload,
            event_type="health_check",
            metadata={"health_check": True},
        )
        self._store.add_delivery(delivery)

        attempt = await self._engine.deliver(delivery)
        self._store.add_delivery_attempt(delivery.id, attempt)

        result: dict[str, Any] = {
            "endpoint_id": endpoint_id,
            "endpoint_name": ep.name,
            "url": ep.url,
            "healthy": attempt.status == DeliveryStatus.SUCCESS,
            "status_code": attempt.response_status_code,
            "duration_ms": attempt.duration_ms,
            "error": attempt.error_message,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if attempt.status == DeliveryStatus.SUCCESS:
            self._store.update_delivery(delivery.id, status=DeliveryStatus.SUCCESS)
        else:
            self._store.update_delivery(delivery.id, status=DeliveryStatus.ABANDONED)

        return result

    # ── Relay Rules ──────────────────────────────────────────────────

    def add_relay_rule(
        self,
        name: str,
        path_pattern: str,
        target_endpoint_ids: list[str],
        tags: list[str] | None = None,
    ) -> RelayRule:
        rule = RelayRule(
            name=name,
            path_pattern=path_pattern,
            target_endpoint_ids=target_endpoint_ids,
            tags=tags or [],
        )
        self._store.add_relay_rule(rule)
        return rule

    def list_relay_rules(self, active_only: bool = False) -> list[RelayRule]:
        return self._store.list_relay_rules(active_only=active_only)

    def delete_relay_rule(self, rule_id: str) -> bool:
        return self._store.delete_relay_rule(rule_id)

    def receive_incoming(
        self,
        path: str,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        body: dict[str, Any] | str | None = None,
        query_params: dict[str, str] | None = None,
        source_ip: str | None = None,
    ) -> list[str]:
        """Receive an incoming webhook and apply relay rules. Returns delivery IDs."""
        return self._engine.apply_relay_rules(
            path=path,
            method=method,
            headers=headers or {},
            body=body,
            query_params=query_params,
            source_ip=source_ip,
        )

    def list_incoming(
        self,
        path: str | None = None,
        processed: bool | None = None,
        limit: int = 100,
    ) -> list[IncomingWebhook]:
        return self._store.list_incoming(path=path, processed=processed, limit=limit)

    # ── Statistics ───────────────────────────────────────────────────

    def get_stats(self, endpoint_id: str) -> dict[str, Any] | None:
        return self._store.get_stats(endpoint_id)

    def get_all_stats(self) -> list[dict[str, Any]]:
        return self._store.get_all_stats()

    # ── Event Log ────────────────────────────────────────────────────

    def log_event(
        self,
        event_type: str,
        details: dict[str, Any] | None = None,
        endpoint_id: str | None = None,
        delivery_id: str | None = None,
    ) -> EventLogEntry:
        """Record an event in the audit log."""
        entry = EventLogEntry(
            event_type=event_type,
            details=details or {},
            endpoint_id=endpoint_id,
            delivery_id=delivery_id,
        )
        self._store.add_event_log(entry)
        return entry

    def list_event_log(
        self,
        event_type: str | None = None,
        endpoint_id: str | None = None,
        limit: int = 100,
    ) -> list[EventLogEntry]:
        return self._store.list_event_log(
            event_type=event_type,
            endpoint_id=endpoint_id,
            limit=limit,
        )

    async def close(self) -> None:
        await self._engine.close()
