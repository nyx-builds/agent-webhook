"""Tests for agent-webhook store."""

import json
import tempfile
from pathlib import Path

import pytest

from agent_webhook.models import (
    DeliveryAttempt,
    DeliveryStatus,
    EventLogEntry,
    EventSubscription,
    Header,
    RelayRule,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookStatus,
)
from agent_webhook.store import WebhookStore


@pytest.fixture
def store(tmp_path):
    return WebhookStore(tmp_path / "test_store.json")


@pytest.fixture
def sample_endpoint():
    return WebhookEndpoint(
        name="Test Endpoint",
        url="https://example.com/webhook",
        tags=["test"],
    )


@pytest.fixture
def sample_endpoint2():
    return WebhookEndpoint(
        name="Second Endpoint",
        url="https://other.example.com/hook",
        method="PUT",
        tags=["production"],
    )


class TestEndpointCRUD:
    def test_add_and_get(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        result = store.get_endpoint(sample_endpoint.id)
        assert result is not None
        assert result.name == "Test Endpoint"
        assert result.url == "https://example.com/webhook"

    def test_get_nonexistent(self, store):
        assert store.get_endpoint("nonexistent") is None

    def test_list_endpoints(self, store, sample_endpoint, sample_endpoint2):
        store.add_endpoint(sample_endpoint)
        store.add_endpoint(sample_endpoint2)
        endpoints = store.list_endpoints()
        assert len(endpoints) == 2

    def test_list_by_status(self, store, sample_endpoint):
        sample_endpoint.status = WebhookStatus.PAUSED
        store.add_endpoint(sample_endpoint)
        active = store.list_endpoints(status=WebhookStatus.ACTIVE)
        assert len(active) == 0
        paused = store.list_endpoints(status=WebhookStatus.PAUSED)
        assert len(paused) == 1

    def test_list_by_tag(self, store, sample_endpoint, sample_endpoint2):
        store.add_endpoint(sample_endpoint)
        store.add_endpoint(sample_endpoint2)
        test_eps = store.list_endpoints(tag="test")
        assert len(test_eps) == 1
        assert test_eps[0].name == "Test Endpoint"

    def test_update_endpoint(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        updated = store.update_endpoint(sample_endpoint.id, name="Updated Name")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_update_sets_updated_at(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        original = sample_endpoint.updated_at
        updated = store.update_endpoint(sample_endpoint.id, name="Updated")
        assert updated.updated_at >= original

    def test_update_nonexistent(self, store):
        result = store.update_endpoint("nonexistent", name="X")
        assert result is None

    def test_delete_endpoint(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        assert store.delete_endpoint(sample_endpoint.id) is True
        assert store.get_endpoint(sample_endpoint.id) is None

    def test_delete_nonexistent(self, store):
        assert store.delete_endpoint("nonexistent") is False


class TestDeliveryCRUD:
    def test_add_and_get(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        delivery = WebhookDelivery(
            endpoint_id=sample_endpoint.id,
            payload={"event": "test"},
            event_type="test.event",
        )
        store.add_delivery(delivery)
        result = store.get_delivery(delivery.id)
        assert result is not None
        assert result.payload == {"event": "test"}

    def test_list_by_endpoint(self, store, sample_endpoint, sample_endpoint2):
        store.add_endpoint(sample_endpoint)
        store.add_endpoint(sample_endpoint2)
        d1 = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={"a": 1})
        d2 = WebhookDelivery(endpoint_id=sample_endpoint2.id, payload={"b": 2})
        store.add_delivery(d1)
        store.add_delivery(d2)
        result = store.list_deliveries(endpoint_id=sample_endpoint.id)
        assert len(result) == 1
        assert result[0].endpoint_id == sample_endpoint.id

    def test_list_by_status(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        d1 = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={}, status=DeliveryStatus.SUCCESS)
        d2 = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={}, status=DeliveryStatus.PENDING)
        store.add_delivery(d1)
        store.add_delivery(d2)
        result = store.list_deliveries(status=DeliveryStatus.SUCCESS)
        assert len(result) == 1

    def test_list_by_event_type(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        d1 = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={}, event_type="payment")
        d2 = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={}, event_type="notification")
        store.add_delivery(d1)
        store.add_delivery(d2)
        result = store.list_deliveries(event_type="payment")
        assert len(result) == 1
        assert result[0].event_type == "payment"

    def test_list_with_limit(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        for i in range(10):
            d = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={"i": i})
            store.add_delivery(d)
        result = store.list_deliveries(limit=5)
        assert len(result) == 5

    def test_update_delivery(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        delivery = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={})
        store.add_delivery(delivery)
        store.update_delivery(delivery.id, status=DeliveryStatus.SUCCESS)
        result = store.get_delivery(delivery.id)
        assert result.status == DeliveryStatus.SUCCESS

    def test_add_delivery_attempt(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        from agent_webhook.models import DeliveryAttempt
        delivery = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={})
        store.add_delivery(delivery)
        attempt = DeliveryAttempt(
            delivery_id=delivery.id,
            attempt_number=1,
            status=DeliveryStatus.SUCCESS,
            response_status_code=200,
        )
        store.add_delivery_attempt(delivery.id, attempt)
        result = store.get_delivery(delivery.id)
        assert len(result.attempts) == 1
        assert result.attempts[0].response_status_code == 200


class TestRelayRules:
    def test_add_and_get(self, store):
        rule = RelayRule(name="Test", path_pattern="/test/*", target_endpoint_ids=["ep-1"])
        store.add_relay_rule(rule)
        result = store.get_relay_rule(rule.id)
        assert result is not None
        assert result.name == "Test"

    def test_list_rules(self, store):
        r1 = RelayRule(name="Rule 1", path_pattern="/a/*", target_endpoint_ids=["ep-1"])
        r2 = RelayRule(name="Rule 2", path_pattern="/b/*", target_endpoint_ids=["ep-2"])
        store.add_relay_rule(r1)
        store.add_relay_rule(r2)
        rules = store.list_relay_rules()
        assert len(rules) == 2

    def test_active_only_filter(self, store):
        r1 = RelayRule(name="Active", path_pattern="/a/*", target_endpoint_ids=["ep-1"], active=True)
        r2 = RelayRule(name="Inactive", path_pattern="/b/*", target_endpoint_ids=["ep-2"], active=False)
        store.add_relay_rule(r1)
        store.add_relay_rule(r2)
        active = store.list_relay_rules(active_only=True)
        assert len(active) == 1
        assert active[0].name == "Active"

    def test_delete_rule(self, store):
        rule = RelayRule(name="Test", path_pattern="/test/*", target_endpoint_ids=["ep-1"])
        store.add_relay_rule(rule)
        assert store.delete_relay_rule(rule.id) is True
        assert store.get_relay_rule(rule.id) is None

    def test_delete_nonexistent_rule(self, store):
        assert store.delete_relay_rule("nonexistent") is False


class TestEventSubscriptions:
    def test_add_and_get(self, store):
        sub = EventSubscription(endpoint_id="ep-1", event_types=["order.created"])
        store.add_subscription(sub)
        result = store.get_subscription(sub.id)
        assert result is not None
        assert result.endpoint_id == "ep-1"
        assert "order.created" in result.event_types

    def test_list_subscriptions(self, store):
        s1 = EventSubscription(endpoint_id="ep-1", event_types=["a"])
        s2 = EventSubscription(endpoint_id="ep-2", event_types=["b"])
        store.add_subscription(s1)
        store.add_subscription(s2)
        subs = store.list_subscriptions()
        assert len(subs) == 2

    def test_list_by_endpoint(self, store):
        s1 = EventSubscription(endpoint_id="ep-1", event_types=["a"])
        s2 = EventSubscription(endpoint_id="ep-2", event_types=["b"])
        store.add_subscription(s1)
        store.add_subscription(s2)
        subs = store.list_subscriptions(endpoint_id="ep-1")
        assert len(subs) == 1
        assert subs[0].endpoint_id == "ep-1"

    def test_delete_subscription(self, store):
        sub = EventSubscription(endpoint_id="ep-1", event_types=["a"])
        store.add_subscription(sub)
        assert store.delete_subscription(sub.id) is True
        assert store.get_subscription(sub.id) is None

    def test_delete_nonexistent_subscription(self, store):
        assert store.delete_subscription("nonexistent") is False


class TestEventLog:
    def test_add_and_list(self, store):
        entry = EventLogEntry(
            event_type="endpoint.created",
            details={"name": "Test"},
            endpoint_id="ep-1",
        )
        store.add_event_log(entry)
        entries = store.list_event_log()
        assert len(entries) == 1
        assert entries[0].event_type == "endpoint.created"

    def test_list_by_event_type(self, store):
        e1 = EventLogEntry(event_type="endpoint.created")
        e2 = EventLogEntry(event_type="delivery.success")
        store.add_event_log(e1)
        store.add_event_log(e2)
        result = store.list_event_log(event_type="endpoint.created")
        assert len(result) == 1

    def test_list_by_endpoint_id(self, store):
        e1 = EventLogEntry(event_type="test", endpoint_id="ep-1")
        e2 = EventLogEntry(event_type="test", endpoint_id="ep-2")
        store.add_event_log(e1)
        store.add_event_log(e2)
        result = store.list_event_log(endpoint_id="ep-1")
        assert len(result) == 1

    def test_list_with_limit(self, store):
        for i in range(10):
            entry = EventLogEntry(event_type=f"event.{i}")
            store.add_event_log(entry)
        result = store.list_event_log(limit=5)
        assert len(result) == 5

    def test_event_log_sorted_newest_first(self, store):
        e1 = EventLogEntry(event_type="first")
        store.add_event_log(e1)
        e2 = EventLogEntry(event_type="second")
        store.add_event_log(e2)
        entries = store.list_event_log()
        assert entries[0].event_type == "second"
        assert entries[1].event_type == "first"

    def test_event_log_max_1000_entries(self, store):
        """Event log should be trimmed to 1000 entries."""
        for i in range(1010):
            entry = EventLogEntry(event_type=f"event.{i}")
            store.add_event_log(entry)
        # After trimming, should have 1000
        entries = store.list_event_log(limit=2000)
        assert len(entries) == 1000


class TestStats:
    def test_stats_empty(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        stats = store.get_stats(sample_endpoint.id)
        assert stats is not None
        assert stats["total_deliveries"] == 0
        assert stats["avg_duration_ms"] is None

    def test_stats_with_deliveries(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        from agent_webhook.models import DeliveryAttempt, DeliveryStatus
        d1 = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={}, status=DeliveryStatus.SUCCESS)
        d2 = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={}, status=DeliveryStatus.FAILED)
        d3 = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={}, status=DeliveryStatus.PENDING)
        store.add_delivery(d1)
        store.add_delivery(d2)
        store.add_delivery(d3)

        # Add attempts with durations
        a1 = DeliveryAttempt(delivery_id=d1.id, attempt_number=1, status=DeliveryStatus.SUCCESS, duration_ms=100.0)
        a2 = DeliveryAttempt(delivery_id=d2.id, attempt_number=1, status=DeliveryStatus.FAILED, duration_ms=200.0)
        store.add_delivery_attempt(d1.id, a1)
        store.add_delivery_attempt(d2.id, a2)

        stats = store.get_stats(sample_endpoint.id)
        assert stats["total_deliveries"] == 3
        assert stats["successful"] == 1
        assert stats["failed"] == 1
        assert stats["pending"] == 1
        assert stats["avg_duration_ms"] == 150.0

    def test_stats_nonexistent_endpoint(self, store):
        assert store.get_stats("nonexistent") is None

    def test_get_all_stats(self, store, sample_endpoint, sample_endpoint2):
        store.add_endpoint(sample_endpoint)
        store.add_endpoint(sample_endpoint2)
        stats = store.get_all_stats()
        assert len(stats) == 2


class TestPersistence:
    def test_data_survives_reload(self, tmp_path):
        store_path = tmp_path / "persist_test.json"
        store = WebhookStore(store_path)
        ep = WebhookEndpoint(name="Persistent", url="https://example.com")
        store.add_endpoint(ep)

        # Reload
        store2 = WebhookStore(store_path)
        result = store2.get_endpoint(ep.id)
        assert result is not None
        assert result.name == "Persistent"

    def test_subscriptions_survive_reload(self, tmp_path):
        store_path = tmp_path / "persist_subs.json"
        store = WebhookStore(store_path)
        sub = EventSubscription(endpoint_id="ep-1", event_types=["test.event"])
        store.add_subscription(sub)

        store2 = WebhookStore(store_path)
        result = store2.get_subscription(sub.id)
        assert result is not None
        assert "test.event" in result.event_types

    def test_event_log_survives_reload(self, tmp_path):
        store_path = tmp_path / "persist_log.json"
        store = WebhookStore(store_path)
        entry = EventLogEntry(event_type="test.persist", details={"key": "value"})
        store.add_event_log(entry)

        store2 = WebhookStore(store_path)
        entries = store2.list_event_log()
        assert len(entries) == 1
        assert entries[0].event_type == "test.persist"

    def test_pending_deliveries(self, store, sample_endpoint):
        store.add_endpoint(sample_endpoint)
        d1 = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={}, status=DeliveryStatus.PENDING)
        d2 = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={}, status=DeliveryStatus.SUCCESS)
        store.add_delivery(d1)
        store.add_delivery(d2)
        pending = store.pending_deliveries()
        assert len(pending) == 1
        assert pending[0].id == d1.id
