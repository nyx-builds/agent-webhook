"""Tests for v0.3.0 features: SQLite store, transforms, rate limiting, dead letter queue."""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timezone
from typing import Any

import pytest

from agent_webhook.models import (
    DeadLetterEntry,
    DeliveryStatus,
    EventSubscription,
    EventLogEntry,
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
from agent_webhook.rate_limiter import RateLimiter
from agent_webhook.store import WebhookStore
from agent_webhook.store_sqlite import SQLiteStore
from agent_webhook.transforms import TransformEngine, _resolve_path, _substitute_template


# ── Fixtures ──────────────────────────────────────────────────────


@pytest.fixture
def sqlite_store(tmp_path):
    """Create a temporary SQLite store."""
    db_path = str(tmp_path / "test.db")
    return SQLiteStore(db_path)


@pytest.fixture
def json_store(tmp_path):
    """Create a temporary JSON store."""
    json_path = str(tmp_path / "test.json")
    return WebhookStore(json_path)


@pytest.fixture
def sample_endpoint():
    return WebhookEndpoint(
        name="test-endpoint",
        url="https://example.com/webhook",
        method=WebhookMethod.POST,
        tags=["test"],
    )


@pytest.fixture
def sample_endpoint_with_rate_limit():
    return WebhookEndpoint(
        name="rate-limited",
        url="https://example.com/webhook",
        rate_limit=RateLimit(max_requests=5, period=RateLimitPeriod.MINUTE),
    )


@pytest.fixture
def sample_endpoint_with_transforms():
    return WebhookEndpoint(
        name="with-transforms",
        url="https://example.com/webhook",
        transform_ids=["t1", "t2"],
    )


# ── SQLite Store Tests ────────────────────────────────────────────


class TestSQLiteStore:
    def test_create_and_get_endpoint(self, sqlite_store, sample_endpoint):
        sqlite_store.add_endpoint(sample_endpoint)
        result = sqlite_store.get_endpoint(sample_endpoint.id)
        assert result is not None
        assert result.name == "test-endpoint"
        assert result.url == "https://example.com/webhook"

    def test_list_endpoints(self, sqlite_store):
        for i in range(3):
            ep = WebhookEndpoint(name=f"ep-{i}", url=f"https://example.com/{i}")
            sqlite_store.add_endpoint(ep)
        endpoints = sqlite_store.list_endpoints()
        assert len(endpoints) == 3

    def test_list_endpoints_filter_status(self, sqlite_store):
        ep1 = WebhookEndpoint(name="active", url="https://a.com", status=WebhookStatus.ACTIVE)
        ep2 = WebhookEndpoint(name="paused", url="https://b.com", status=WebhookStatus.PAUSED)
        sqlite_store.add_endpoint(ep1)
        sqlite_store.add_endpoint(ep2)
        active = sqlite_store.list_endpoints(status=WebhookStatus.ACTIVE)
        assert len(active) == 1
        assert active[0].name == "active"

    def test_update_endpoint(self, sqlite_store, sample_endpoint):
        sqlite_store.add_endpoint(sample_endpoint)
        updated = sqlite_store.update_endpoint(sample_endpoint.id, name="new-name")
        assert updated is not None
        assert updated.name == "new-name"

    def test_delete_endpoint(self, sqlite_store, sample_endpoint):
        sqlite_store.add_endpoint(sample_endpoint)
        assert sqlite_store.delete_endpoint(sample_endpoint.id)
        assert sqlite_store.get_endpoint(sample_endpoint.id) is None

    def test_add_and_get_delivery(self, sqlite_store, sample_endpoint):
        sqlite_store.add_endpoint(sample_endpoint)
        delivery = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={"test": True})
        sqlite_store.add_delivery(delivery)
        result = sqlite_store.get_delivery(delivery.id)
        assert result is not None
        assert result.payload == {"test": True}

    def test_list_deliveries_filter(self, sqlite_store, sample_endpoint):
        sqlite_store.add_endpoint(sample_endpoint)
        for i in range(5):
            d = WebhookDelivery(
                endpoint_id=sample_endpoint.id,
                payload={"i": i},
                status=DeliveryStatus.SUCCESS if i < 3 else DeliveryStatus.FAILED,
            )
            sqlite_store.add_delivery(d)
        success = sqlite_store.list_deliveries(status=DeliveryStatus.SUCCESS)
        assert len(success) == 3
        failed = sqlite_store.list_deliveries(status=DeliveryStatus.FAILED)
        assert len(failed) == 2

    def test_add_and_list_subscriptions(self, sqlite_store, sample_endpoint):
        sqlite_store.add_endpoint(sample_endpoint)
        sub = EventSubscription(endpoint_id=sample_endpoint.id, event_types=["order.created"])
        sqlite_store.add_subscription(sub)
        subs = sqlite_store.list_subscriptions()
        assert len(subs) == 1
        assert subs[0].event_types == ["order.created"]

    def test_add_and_list_event_log(self, sqlite_store):
        entry = EventLogEntry(event_type="test.event", details={"key": "value"})
        sqlite_store.add_event_log(entry)
        entries = sqlite_store.list_event_log()
        assert len(entries) == 1

    def test_stats(self, sqlite_store, sample_endpoint):
        sqlite_store.add_endpoint(sample_endpoint)
        stats = sqlite_store.get_stats(sample_endpoint.id)
        assert stats["total_deliveries"] == 0
        assert stats["endpoint_name"] == "test-endpoint"

    def test_endpoint_with_rate_limit(self, sqlite_store):
        ep = WebhookEndpoint(
            name="rl-test",
            url="https://example.com",
            rate_limit=RateLimit(max_requests=10, period=RateLimitPeriod.MINUTE, burst=2),
        )
        sqlite_store.add_endpoint(ep)
        result = sqlite_store.get_endpoint(ep.id)
        assert result is not None
        assert result.rate_limit is not None
        assert result.rate_limit.max_requests == 10
        assert result.rate_limit.burst == 2

    def test_endpoint_with_transform_ids(self, sqlite_store):
        ep = WebhookEndpoint(
            name="transform-test",
            url="https://example.com",
            transform_ids=["t1", "t2"],
        )
        sqlite_store.add_endpoint(ep)
        result = sqlite_store.get_endpoint(ep.id)
        assert result is not None
        assert result.transform_ids == ["t1", "t2"]


# ── SQLite Transform CRUD Tests ───────────────────────────────────


class TestSQLiteTransforms:
    def test_create_transform(self, sqlite_store):
        t = PayloadTransform(name="rename-fields", type=TransformType.FIELD_MAP, config={"mapping": {"old": "new"}})
        sqlite_store.add_transform(t)
        result = sqlite_store.get_transform(t.id)
        assert result is not None
        assert result.name == "rename-fields"
        assert result.type == TransformType.FIELD_MAP

    def test_list_transforms(self, sqlite_store):
        for i in range(3):
            t = PayloadTransform(name=f"t-{i}", type=TransformType.FILTER, config={"include": [f"field{i}"]})
            sqlite_store.add_transform(t)
        transforms = sqlite_store.list_transforms()
        assert len(transforms) == 3

    def test_list_transforms_filter_type(self, sqlite_store):
        t1 = PayloadTransform(name="fm", type=TransformType.FIELD_MAP, config={"mapping": {}})
        t2 = PayloadTransform(name="flt", type=TransformType.FILTER, config={"include": ["a"]})
        sqlite_store.add_transform(t1)
        sqlite_store.add_transform(t2)
        field_maps = sqlite_store.list_transforms(type="field_map")
        assert len(field_maps) == 1
        assert field_maps[0].name == "fm"

    def test_update_transform(self, sqlite_store):
        t = PayloadTransform(name="old", type=TransformType.FILTER, config={"include": ["a"]})
        sqlite_store.add_transform(t)
        updated = sqlite_store.update_transform(t.id, name="new-name")
        assert updated is not None
        assert updated.name == "new-name"

    def test_delete_transform(self, sqlite_store):
        t = PayloadTransform(name="del-me", type=TransformType.FILTER, config={"include": ["a"]})
        sqlite_store.add_transform(t)
        assert sqlite_store.delete_transform(t.id)
        assert sqlite_store.get_transform(t.id) is None

    def test_get_transform_not_found(self, sqlite_store):
        assert sqlite_store.get_transform("nonexistent") is None


# ── SQLite Dead Letter Queue Tests ─────────────────────────────────


class TestSQLiteDeadLetterQueue:
    def test_add_and_get_dlq_entry(self, sqlite_store):
        entry = DeadLetterEntry(
            delivery_id="d1",
            endpoint_id="e1",
            payload={"test": True},
            reason="Max retries (3) exceeded",
            total_attempts=4,
        )
        sqlite_store.add_dead_letter(entry)
        result = sqlite_store.get_dead_letter(entry.id)
        assert result is not None
        assert result.reason == "Max retries (3) exceeded"
        assert result.total_attempts == 4

    def test_list_dlq_entries(self, sqlite_store):
        for i in range(3):
            entry = DeadLetterEntry(
                delivery_id=f"d{i}",
                endpoint_id=f"e{i}",
                payload={"i": i},
                reason=f"Failed {i}",
                total_attempts=i + 1,
            )
            sqlite_store.add_dead_letter(entry)
        entries = sqlite_store.list_dead_letter()
        assert len(entries) == 3

    def test_list_dlq_filter_endpoint(self, sqlite_store):
        for i in range(3):
            entry = DeadLetterEntry(
                delivery_id=f"d{i}",
                endpoint_id="e1" if i < 2 else "e2",
                payload={"i": i},
                reason="Failed",
                total_attempts=3,
            )
            sqlite_store.add_dead_letter(entry)
        e1_entries = sqlite_store.list_dead_letter(endpoint_id="e1")
        assert len(e1_entries) == 2

    def test_list_dlq_filter_replayed(self, sqlite_store):
        entry1 = DeadLetterEntry(
            delivery_id="d1", endpoint_id="e1", payload={}, reason="Failed", total_attempts=3, replayed=False,
        )
        entry2 = DeadLetterEntry(
            delivery_id="d2", endpoint_id="e1", payload={}, reason="Failed", total_attempts=3, replayed=True,
        )
        sqlite_store.add_dead_letter(entry1)
        sqlite_store.add_dead_letter(entry2)
        not_replayed = sqlite_store.list_dead_letter(replayed=False)
        assert len(not_replayed) == 1
        replayed = sqlite_store.list_dead_letter(replayed=True)
        assert len(replayed) == 1

    def test_update_dlq_replayed(self, sqlite_store):
        entry = DeadLetterEntry(
            delivery_id="d1", endpoint_id="e1", payload={}, reason="Failed", total_attempts=3,
        )
        sqlite_store.add_dead_letter(entry)
        sqlite_store.update_dead_letter(entry.id, replayed=True, replayed_delivery_id="new-d1")
        result = sqlite_store.get_dead_letter(entry.id)
        assert result is not None
        assert result.replayed is True
        assert result.replayed_delivery_id == "new-d1"

    def test_delete_dlq_entry(self, sqlite_store):
        entry = DeadLetterEntry(
            delivery_id="d1", endpoint_id="e1", payload={}, reason="Failed", total_attempts=3,
        )
        sqlite_store.add_dead_letter(entry)
        assert sqlite_store.delete_dead_letter(entry.id)
        assert sqlite_store.get_dead_letter(entry.id) is None

    def test_dlq_count(self, sqlite_store):
        for i in range(5):
            entry = DeadLetterEntry(
                delivery_id=f"d{i}", endpoint_id="e1" if i < 3 else "e2", payload={}, reason="Failed", total_attempts=3,
            )
            sqlite_store.add_dead_letter(entry)
        assert sqlite_store.dead_letter_count() == 5
        assert sqlite_store.dead_letter_count(endpoint_id="e1") == 3


# ── SQLite Migration Tests ────────────────────────────────────────


class TestSQLiteMigration:
    def test_migrate_from_json(self, sqlite_store, tmp_path):
        # Create a JSON store with some data
        json_path = str(tmp_path / "source.json")
        json_store = WebhookStore(json_path)

        ep = WebhookEndpoint(name="migrated-ep", url="https://example.com")
        json_store.add_endpoint(ep)

        sub = EventSubscription(endpoint_id=ep.id, event_types=["test.event"])
        json_store.add_subscription(sub)

        entry = EventLogEntry(event_type="test", details={"msg": "hello"})
        json_store.add_event_log(entry)

        # Migrate
        counts = sqlite_store.migrate_from_json(json_path)
        assert counts["endpoints"] == 1
        assert counts["subscriptions"] == 1
        assert counts["event_log"] == 1

        # Verify migrated data
        result = sqlite_store.get_endpoint(ep.id)
        assert result is not None
        assert result.name == "migrated-ep"

        subs = sqlite_store.list_subscriptions()
        assert len(subs) == 1

        entries = sqlite_store.list_event_log()
        assert len(entries) == 1


# ── Transform Engine Tests ────────────────────────────────────────


class TestTransformEngine:
    def test_field_map_rename(self):
        engine = TransformEngine()
        t = PayloadTransform(name="rename", type=TransformType.FIELD_MAP, config={"mapping": {"old_key": "new_key"}})
        result = engine.apply_one({"old_key": "value", "other": "keep"}, t)
        assert "new_key" in result
        assert result["new_key"] == "value"
        assert "other" in result  # keep_unmapped=True by default

    def test_field_map_no_keep_unmapped(self):
        engine = TransformEngine()
        t = PayloadTransform(name="rename", type=TransformType.FIELD_MAP, config={"mapping": {"old_key": "new_key"}, "keep_unmapped": False})
        result = engine.apply_one({"old_key": "value", "other": "drop"}, t)
        assert "new_key" in result
        assert "other" not in result

    def test_filter_include(self):
        engine = TransformEngine()
        t = PayloadTransform(name="inc", type=TransformType.FILTER, config={"include": ["a", "b"]})
        result = engine.apply_one({"a": 1, "b": 2, "c": 3}, t)
        assert result == {"a": 1, "b": 2}

    def test_filter_exclude(self):
        engine = TransformEngine()
        t = PayloadTransform(name="exc", type=TransformType.FILTER, config={"exclude": ["c"]})
        result = engine.apply_one({"a": 1, "b": 2, "c": 3}, t)
        assert result == {"a": 1, "b": 2}

    def test_template_fields(self):
        engine = TransformEngine()
        t = PayloadTransform(name="tmpl", type=TransformType.TEMPLATE, config={"fields": {"greeting": "Hello {{payload.name}}"}})
        result = engine.apply_one({"name": "World"}, t)
        assert result["greeting"] == "Hello World"
        assert result["name"] == "World"  # original preserved

    def test_chain_transforms(self):
        engine = TransformEngine()
        t1 = PayloadTransform(name="filter", type=TransformType.FILTER, config={"include": ["name", "email"]})
        t2 = PayloadTransform(name="rename", type=TransformType.FIELD_MAP, config={"mapping": {"email": "email_address"}})
        result = engine.apply({"name": "Alice", "email": "alice@example.com", "password": "secret"}, [t1, t2])
        assert result == {"name": "Alice", "email_address": "alice@example.com"}

    def test_empty_transform_list(self):
        engine = TransformEngine()
        result = engine.apply({"key": "value"}, [])
        assert result == {"key": "value"}


class TestTemplateSubstitution:
    def test_simple_substitution(self):
        result = _substitute_template("Hello {{payload.name}}", {"name": "World"})
        assert result == "Hello World"

    def test_nested_path(self):
        result = _substitute_template("{{payload.user.email}}", {"user": {"email": "test@example.com"}})
        assert result == "test@example.com"

    def test_resolve_path(self):
        assert _resolve_path({"a": {"b": {"c": 42}}}, "a.b.c") == 42
        assert _resolve_path({"items": [10, 20, 30]}, "items.1") == 20
        assert _resolve_path({"x": 1}, "y") is None

    def test_missing_path(self):
        result = _substitute_template("{{payload.missing}}", {"name": "test"})
        assert result == ""


# ── Rate Limiter Tests ────────────────────────────────────────────


class TestRateLimiter:
    def test_allows_under_limit(self):
        limiter = RateLimiter()
        rl = RateLimit(max_requests=5, period=RateLimitPeriod.MINUTE)
        for _ in range(5):
            assert limiter.is_allowed("ep1", rl)

    def test_blocks_over_limit(self):
        limiter = RateLimiter()
        rl = RateLimit(max_requests=3, period=RateLimitPeriod.MINUTE)
        for _ in range(3):
            assert limiter.is_allowed("ep1", rl)
        assert not limiter.is_allowed("ep1", rl)

    def test_burst_allows_extra(self):
        limiter = RateLimiter()
        rl = RateLimit(max_requests=3, period=RateLimitPeriod.MINUTE, burst=2)
        for _ in range(5):
            assert limiter.is_allowed("ep1", rl)
        assert not limiter.is_allowed("ep1", rl)

    def test_separate_endpoints(self):
        limiter = RateLimiter()
        rl = RateLimit(max_requests=2, period=RateLimitPeriod.MINUTE)
        assert limiter.is_allowed("ep1", rl)
        assert limiter.is_allowed("ep1", rl)
        assert not limiter.is_allowed("ep1", rl)
        # ep2 is separate
        assert limiter.is_allowed("ep2", rl)

    def test_reset(self):
        limiter = RateLimiter()
        rl = RateLimit(max_requests=2, period=RateLimitPeriod.MINUTE)
        limiter.is_allowed("ep1", rl)
        limiter.is_allowed("ep1", rl)
        limiter.reset("ep1")
        assert limiter.is_allowed("ep1", rl)

    def test_reset_all(self):
        limiter = RateLimiter()
        rl = RateLimit(max_requests=1, period=RateLimitPeriod.MINUTE)
        limiter.is_allowed("ep1", rl)
        limiter.is_allowed("ep2", rl)
        limiter.reset()
        assert limiter.is_allowed("ep1", rl)

    def test_get_status(self):
        limiter = RateLimiter()
        rl = RateLimit(max_requests=5, period=RateLimitPeriod.MINUTE, burst=2)
        limiter.is_allowed("ep1", rl)
        limiter.is_allowed("ep1", rl)
        status = limiter.get_status("ep1", rl)
        assert status["remaining"] == 5  # 5+2 - 2 = 5
        assert status["limit"] == 5
        assert status["burst"] == 2

    def test_period_seconds(self):
        assert RateLimit(max_requests=1, period=RateLimitPeriod.SECOND).period_seconds == 1.0
        assert RateLimit(max_requests=1, period=RateLimitPeriod.MINUTE).period_seconds == 60.0
        assert RateLimit(max_requests=1, period=RateLimitPeriod.HOUR).period_seconds == 3600.0


# ── Model Changes Tests ──────────────────────────────────────────


class TestModelChanges:
    def test_delivery_status_dead_letter(self):
        assert DeliveryStatus.DEAD_LETTER == "dead_letter"

    def test_webhook_delivery_dead_letter_fields(self):
        d = WebhookDelivery(
            endpoint_id="ep1",
            payload={"test": True},
            dead_letter_reason="Max retries exceeded",
            dead_lettered_at=datetime.now(timezone.utc),
        )
        assert d.dead_letter_reason == "Max retries exceeded"
        assert d.dead_lettered_at is not None

    def test_webhook_delivery_transformed_payload(self):
        d = WebhookDelivery(
            endpoint_id="ep1",
            payload={"original": True},
            transformed_payload={"transformed": True},
        )
        assert d.transformed_payload == {"transformed": True}

    def test_can_retry_excludes_dead_letter(self):
        d = WebhookDelivery(endpoint_id="ep1", payload={}, status=DeliveryStatus.DEAD_LETTER)
        rp = RetryPolicy(max_retries=3)
        assert not d.can_retry(rp)

    def test_endpoint_with_rate_limit(self):
        ep = WebhookEndpoint(
            name="test",
            url="https://example.com",
            rate_limit=RateLimit(max_requests=10, period=RateLimitPeriod.MINUTE, burst=3),
        )
        assert ep.rate_limit.max_requests == 10
        assert ep.rate_limit.burst == 3

    def test_endpoint_with_transform_ids(self):
        ep = WebhookEndpoint(
            name="test",
            url="https://example.com",
            transform_ids=["t1", "t2"],
        )
        assert ep.transform_ids == ["t1", "t2"]

    def test_dead_letter_entry_model(self):
        entry = DeadLetterEntry(
            delivery_id="d1",
            endpoint_id="e1",
            payload={"test": True},
            reason="Max retries exceeded",
            last_status_code=500,
            last_error="Internal Server Error",
            total_attempts=4,
        )
        assert entry.replayed is False
        assert entry.replayed_delivery_id is None


# ── Integration: Engine with SQLite Store ─────────────────────────


class TestEngineWithSQLite:
    @pytest.mark.asyncio
    async def test_failed_delivery_goes_to_dlq(self, sqlite_store):
        """When a delivery exhausts retries, it should go to DLQ instead of abandoned."""
        from agent_webhook.engine import DeliveryEngine

        # Create endpoint with 0 retries (so it fails immediately)
        ep = WebhookEndpoint(
            name="fail-test",
            url="https://nonexistent.invalid/hook",
            retry_policy=RetryPolicy(max_retries=0),
        )
        sqlite_store.add_endpoint(ep)

        engine = DeliveryEngine(sqlite_store)
        result = await engine.send(
            endpoint_id=ep.id,
            payload={"test": True},
        )
        await engine.close()

        # Should be dead_letter instead of abandoned
        assert result.status == DeliveryStatus.DEAD_LETTER
        assert result.dead_letter_reason is not None

        # Should have a DLQ entry
        dlq_entries = sqlite_store.list_dead_letter(endpoint_id=ep.id)
        assert len(dlq_entries) == 1
        assert dlq_entries[0].payload == {"test": True}

    @pytest.mark.asyncio
    async def test_transforms_applied_on_delivery(self, sqlite_store):
        """Transforms linked to endpoint should be applied."""
        from agent_webhook.engine import DeliveryEngine

        # Create a transform
        t = PayloadTransform(
            name="filter-fields",
            type=TransformType.FILTER,
            config={"include": ["message"]},
        )
        sqlite_store.add_transform(t)

        # Create endpoint linked to transform
        ep = WebhookEndpoint(
            name="transform-test",
            url="https://nonexistent.invalid/hook",
            transform_ids=[t.id],
            retry_policy=RetryPolicy(max_retries=0),
        )
        sqlite_store.add_endpoint(ep)

        engine = DeliveryEngine(sqlite_store)
        result = await engine.send(
            endpoint_id=ep.id,
            payload={"message": "hello", "secret": "should-be-filtered"},
        )
        await engine.close()

        # The delivery should have a transformed_payload
        delivery = sqlite_store.get_delivery(result.id)
        assert delivery is not None
        assert delivery.transformed_payload is not None
        assert "message" in delivery.transformed_payload
        assert "secret" not in delivery.transformed_payload

    @pytest.mark.asyncio
    async def test_rate_limit_blocks_delivery(self, sqlite_store):
        """Rate limited endpoint should block deliveries over limit."""
        from agent_webhook.engine import DeliveryEngine

        ep = WebhookEndpoint(
            name="rate-limited",
            url="https://nonexistent.invalid/hook",
            rate_limit=RateLimit(max_requests=2, period=RateLimitPeriod.MINUTE),
            retry_policy=RetryPolicy(max_retries=0),
        )
        sqlite_store.add_endpoint(ep)

        engine = DeliveryEngine(sqlite_store)

        # First two should work (even though they'll fail due to bad URL, they get attempted)
        r1 = await engine.send(endpoint_id=ep.id, payload={"i": 1})
        r2 = await engine.send(endpoint_id=ep.id, payload={"i": 2})

        # Third should be blocked by rate limit
        r3 = await engine.send(endpoint_id=ep.id, payload={"i": 3})
        assert r3.status == DeliveryStatus.DEAD_LETTER
        # The last attempt should have rate limit error
        last_attempt = r3.last_attempt()
        if last_attempt:
            assert "Rate limit" in (last_attempt.error_message or "")

        await engine.close()

    @pytest.mark.asyncio
    async def test_retry_policy_customization(self, sqlite_store):
        """Custom retry policy should be respected."""
        from agent_webhook.engine import DeliveryEngine

        ep = WebhookEndpoint(
            name="custom-retry",
            url="https://nonexistent.invalid/hook",
            retry_policy=RetryPolicy(
                max_retries=2,
                initial_delay_seconds=0.1,
                backoff_multiplier=3.0,
                retry_on_status_codes=[500, 502, 503],
            ),
        )
        sqlite_store.add_endpoint(ep)

        # Verify the endpoint has custom retry policy
        saved_ep = sqlite_store.get_endpoint(ep.id)
        assert saved_ep is not None
        assert saved_ep.retry_policy.max_retries == 2
        assert saved_ep.retry_policy.backoff_multiplier == 3.0


# ── SQLite Stats with Dead Letter ────────────────────────────────


class TestStatsWithDeadLetter:
    def test_stats_includes_dead_letter_count(self, sqlite_store):
        ep = WebhookEndpoint(name="stats-ep", url="https://example.com")
        sqlite_store.add_endpoint(ep)

        # Create a dead letter delivery
        d = WebhookDelivery(
            endpoint_id=ep.id,
            payload={"test": True},
            status=DeliveryStatus.DEAD_LETTER,
        )
        sqlite_store.add_delivery(d)

        stats = sqlite_store.get_stats(ep.id)
        assert stats["dead_letter"] == 1
