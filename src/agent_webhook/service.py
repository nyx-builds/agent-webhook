"""Service layer for agent-webhook — business logic on top of store and engine."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any

from .engine import DeliveryEngine
from .models import (
    DeadLetterEntry,
    DeliveryAttempt,
    DeliveryStatus,
    EventLogEntry,
    EventSubscription,
    Header,
    IncomingWebhook,
    PayloadTransform,
    RateLimit,
    RateLimitPeriod,
    RelayRule,
    RetryPolicy,
    TransformType,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookMethod,
    WebhookStatus,
)
from .store import WebhookStore


class WebhookService:
    """High-level service for webhook management."""

    def __init__(self, store: WebhookStore | None = None, store_path: str = "webhook_store.json"):
        if store is not None:
            self._store = store
        elif store_path.endswith(".db"):
            from .store_sqlite import SQLiteStore
            self._store = SQLiteStore(store_path)
        else:
            self._store = WebhookStore(store_path)
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
        initial_delay_seconds: float = 1.0,
        max_delay_seconds: float = 300.0,
        backoff_multiplier: float = 2.0,
        retry_on_status_codes: list[int] | None = None,
        transform_ids: list[str] | None = None,
        rate_limit: dict[str, Any] | None = None,
    ) -> WebhookEndpoint:
        """Create and register a new webhook endpoint."""
        header_objs = [Header(name=k, value=v) for k, v in (headers or {}).items()]

        retry_policy_kwargs: dict[str, Any] = {
            "max_retries": max_retries,
            "initial_delay_seconds": initial_delay_seconds,
            "max_delay_seconds": max_delay_seconds,
            "backoff_multiplier": backoff_multiplier,
        }
        if retry_on_status_codes is not None:
            retry_policy_kwargs["retry_on_status_codes"] = retry_on_status_codes
        retry_policy = RetryPolicy(**retry_policy_kwargs)

        rate_limit_obj = None
        if rate_limit is not None:
            rate_limit_obj = RateLimit(
                max_requests=rate_limit["max_requests"],
                period=RateLimitPeriod(rate_limit.get("period", "minute")),
                burst=rate_limit.get("burst", 0),
            )

        endpoint = WebhookEndpoint(
            name=name,
            url=url,
            method=WebhookMethod(method),
            headers=header_objs,
            tags=tags or [],
            secret=secret,
            timeout_seconds=timeout_seconds,
            description=description,
            retry_policy=retry_policy,
            transform_ids=transform_ids or [],
            rate_limit=rate_limit_obj,
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
        # Handle rate_limit dict -> RateLimit model
        if "rate_limit" in updates and isinstance(updates["rate_limit"], dict):
            rl = updates.pop("rate_limit")
            updates["rate_limit"] = RateLimit(
                max_requests=rl["max_requests"],
                period=RateLimitPeriod(rl.get("period", "minute")),
                burst=rl.get("burst", 0),
            )
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

    def update_relay_rule(self, rule_id: str, **updates: Any) -> RelayRule | None:
        """Update a relay rule. Only works with stores that support update_relay_rule."""
        if not hasattr(self._store, "update_relay_rule"):
            return None
        return self._store.update_relay_rule(rule_id, **updates)

    def receive_incoming(
        self,
        path: str,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        body: dict[str, Any] | str | None = None,
        query_params: dict[str, str] | None = None,
        source_ip: str | None = None,
        raw_body: bytes | str | None = None,
    ) -> list[str]:
        """Receive an incoming webhook and apply relay rules. Returns delivery IDs.

        Args:
            raw_body: Raw request body for signature verification.
        """
        return self._engine.apply_relay_rules(
            path=path,
            method=method,
            headers=headers or {},
            body=body,
            query_params=query_params,
            source_ip=source_ip,
            raw_body=raw_body,
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

    # ── Transforms ───────────────────────────────────────────────────

    def create_transform(
        self,
        name: str,
        type: str,
        config: dict[str, Any],
    ) -> PayloadTransform | None:
        """Create a new payload transform. Requires SQLite store."""
        if not hasattr(self._store, "add_transform"):
            return None
        transform = PayloadTransform(
            name=name,
            type=TransformType(type),
            config=config,
        )
        self._store.add_transform(transform)
        return transform

    def get_transform(self, transform_id: str) -> PayloadTransform | None:
        """Get a transform by ID. Requires SQLite store."""
        if not hasattr(self._store, "get_transform"):
            return None
        return self._store.get_transform(transform_id)

    def list_transforms(self, type: str | None = None) -> list[PayloadTransform]:
        """List all transforms. Requires SQLite store."""
        if not hasattr(self._store, "list_transforms"):
            return []
        return self._store.list_transforms(type=type)

    def update_transform(self, transform_id: str, **updates: Any) -> PayloadTransform | None:
        """Update a transform. Requires SQLite store."""
        if not hasattr(self._store, "update_transform"):
            return None
        return self._store.update_transform(transform_id, **updates)

    def delete_transform(self, transform_id: str) -> bool:
        """Delete a transform. Requires SQLite store."""
        if not hasattr(self._store, "delete_transform"):
            return False
        return self._store.delete_transform(transform_id)

    # ── Dead Letter Queue ────────────────────────────────────────────

    def list_dead_letter(
        self,
        endpoint_id: str | None = None,
        replayed: bool | None = None,
        limit: int = 100,
    ) -> list[DeadLetterEntry]:
        """List dead letter queue entries. Requires SQLite store."""
        if not hasattr(self._store, "list_dead_letter"):
            return []
        return self._store.list_dead_letter(endpoint_id=endpoint_id, replayed=replayed, limit=limit)

    def get_dead_letter(self, entry_id: str) -> DeadLetterEntry | None:
        """Get a dead letter entry by ID. Requires SQLite store."""
        if not hasattr(self._store, "get_dead_letter"):
            return None
        return self._store.get_dead_letter(entry_id)

    async def replay_dead_letter(self, entry_id: str) -> WebhookDelivery | None:
        """Replay a dead letter entry by creating a new delivery. Requires SQLite store."""
        if not hasattr(self._store, "get_dead_letter"):
            return None
        entry = self._store.get_dead_letter(entry_id)
        if entry is None:
            return None
        if entry.replayed:
            return None

        # Create new delivery from the original payload
        delivery = await self._engine.send(
            endpoint_id=entry.endpoint_id,
            payload=entry.payload,
            event_type=entry.event_type,
            metadata={"replayed_from_dlq": entry.id, "original_delivery_id": entry.delivery_id},
        )

        # Mark entry as replayed
        self._store.update_dead_letter(
            entry_id,
            replayed=True,
            replayed_delivery_id=delivery.id,
            replayed_at=datetime.now(timezone.utc),
        )

        return delivery

    def delete_dead_letter(self, entry_id: str) -> bool:
        """Delete a dead letter entry. Requires SQLite store."""
        if not hasattr(self._store, "delete_dead_letter"):
            return False
        return self._store.delete_dead_letter(entry_id)

    async def batch_replay_dead_letter(
        self,
        endpoint_id: str | None = None,
    ) -> list[WebhookDelivery]:
        """Replay all unreplayed dead letter entries, optionally filtered by endpoint. Requires SQLite store."""
        if not hasattr(self._store, "list_dead_letter"):
            return []
        entries = self._store.list_dead_letter(endpoint_id=endpoint_id, replayed=False, limit=1000)
        results = []
        for entry in entries:
            try:
                delivery = await self.replay_dead_letter(entry.id)
                if delivery is not None:
                    results.append(delivery)
            except Exception:
                pass  # Skip entries that fail to replay
        return results

    def dead_letter_count(self, endpoint_id: str | None = None) -> int:
        """Get dead letter count. Requires SQLite store."""
        if not hasattr(self._store, "dead_letter_count"):
            return 0
        return self._store.dead_letter_count(endpoint_id=endpoint_id)

    # ── Rate Limiting ────────────────────────────────────────────────

    def get_rate_limit_status(self, endpoint_id: str) -> dict[str, Any] | None:
        """Get rate limit status for an endpoint."""
        ep = self._store.get_endpoint(endpoint_id)
        if ep is None or ep.rate_limit is None:
            return None
        return self._engine._rate_limiter.get_status(endpoint_id, ep.rate_limit)

    # ── Migration ────────────────────────────────────────────────────

    def migrate_from_json(self, json_path: str) -> dict[str, int] | None:
        """Migrate from JSON store to SQLite store. Requires SQLite store."""
        if not hasattr(self._store, "migrate_from_json"):
            return None
        return self._store.migrate_from_json(json_path)

    # ── Circuit Breaker ──────────────────────────────────────────────

    def get_circuit_breaker_state(self, endpoint_id: str) -> dict[str, Any] | None:
        """Get circuit breaker state for an endpoint."""
        return self._engine.get_circuit_breaker_state(endpoint_id)

    def get_all_circuit_breaker_states(self) -> list[dict[str, Any]]:
        """Get circuit breaker states for all endpoints with breakers."""
        return self._engine.get_all_circuit_breaker_states()

    def reset_circuit_breaker(self, endpoint_id: str) -> dict[str, Any] | None:
        """Reset (force close) the circuit breaker for an endpoint."""
        return self._engine.reset_circuit_breaker(endpoint_id)

    # ── Incoming Webhook Verification ────────────────────────────────

    def verify_incoming_signature(
        self,
        raw_body: bytes | str,
        headers: dict[str, str],
        secret: str,
        provider: str = "generic",
        algorithm: str = "sha256",
        tolerance_seconds: int = 300,
    ) -> dict[str, Any]:
        """Verify an incoming webhook signature.

        Returns a dict with 'valid': bool and 'provider' / 'error' fields.
        """
        from .signature import SignatureVerifier, SignatureError

        verifier = SignatureVerifier(tolerance_seconds=tolerance_seconds)
        try:
            verifier.verify_or_raise(
                raw_body=raw_body,
                headers=headers,
                secret=secret,
                provider=provider,
                algorithm=algorithm,
            )
            return {"valid": True, "provider": provider}
        except SignatureError as e:
            return {"valid": False, "provider": provider, "error": str(e)}

    def detect_incoming_provider(self, headers: dict[str, str]) -> str | None:
        """Auto-detect the webhook provider from request headers."""
        from .signature import SignatureVerifier
        verifier = SignatureVerifier()
        return verifier.detect_provider(headers)

    async def close(self) -> None:
        await self._engine.close()
