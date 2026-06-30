"""Tests for relay filter integration with the delivery engine."""

import pytest

from agent_webhook.engine import DeliveryEngine
from agent_webhook.models import (
    RelayRule,
    WebhookEndpoint,
    WebhookMethod,
    WebhookStatus,
)
from agent_webhook.store_sqlite import SQLiteStore


@pytest.fixture
def store(tmp_path):
    db_path = str(tmp_path / "relay_test.db")
    return SQLiteStore(db_path)


@pytest.fixture
def engine(store):
    return DeliveryEngine(store)


@pytest.fixture
def endpoint(store):
    ep = WebhookEndpoint(
        name="Test Target",
        url="https://target.example.com/hook",
        method=WebhookMethod.POST,
        status=WebhookStatus.ACTIVE,
    )
    store.add_endpoint(ep)
    return ep


class TestRelayFilterIntegration:
    """Test that relay rules with filters correctly filter incoming webhooks."""

    def test_no_filter_forwards_all(self, store, engine, endpoint):
        """Without filter_rules, all matching webhooks forward."""
        rule = RelayRule(
            name="No Filter",
            path_pattern="/test/*",
            target_endpoint_ids=[endpoint.id],
        )
        store.add_relay_rule(rule)

        delivery_ids = engine.apply_relay_rules(
            path="/test/event",
            method="POST",
            headers={},
            body={"any": "data"},
        )
        assert len(delivery_ids) == 1

    def test_passing_filter_forwards(self, store, engine, endpoint):
        rule = RelayRule(
            name="Filtered",
            path_pattern="/test/*",
            target_endpoint_ids=[endpoint.id],
            filter_rules={
                "logic": "all",
                "conditions": [
                    {"type": "payload", "field": "event", "operator": "equals", "value": "important"},
                ],
            },
        )
        store.add_relay_rule(rule)

        delivery_ids = engine.apply_relay_rules(
            path="/test/event",
            method="POST",
            headers={},
            body={"event": "important"},
        )
        assert len(delivery_ids) == 1

    def test_failing_filter_blocks(self, store, engine, endpoint):
        rule = RelayRule(
            name="Filtered",
            path_pattern="/test/*",
            target_endpoint_ids=[endpoint.id],
            filter_rules={
                "logic": "all",
                "conditions": [
                    {"type": "payload", "field": "event", "operator": "equals", "value": "important"},
                ],
            },
        )
        store.add_relay_rule(rule)

        delivery_ids = engine.apply_relay_rules(
            path="/test/event",
            method="POST",
            headers={},
            body={"event": "unimportant"},
        )
        assert len(delivery_ids) == 0

    def test_header_filter(self, store, engine, endpoint):
        rule = RelayRule(
            name="Header Filter",
            path_pattern="/test/*",
            target_endpoint_ids=[endpoint.id],
            filter_rules={
                "logic": "all",
                "conditions": [
                    {"type": "header", "field": "X-Source", "operator": "equals", "value": "trusted"},
                ],
            },
        )
        store.add_relay_rule(rule)

        # Matching header
        ids1 = engine.apply_relay_rules(
            path="/test/event",
            method="POST",
            headers={"X-Source": "trusted"},
            body={"data": 1},
        )
        assert len(ids1) == 1

        # Non-matching header
        ids2 = engine.apply_relay_rules(
            path="/test/event",
            method="POST",
            headers={"X-Source": "untrusted"},
            body={"data": 1},
        )
        assert len(ids2) == 0

    def test_any_logic_filter(self, store, engine, endpoint):
        rule = RelayRule(
            name="Any Filter",
            path_pattern="/test/*",
            target_endpoint_ids=[endpoint.id],
            filter_rules={
                "logic": "any",
                "conditions": [
                    {"type": "payload", "field": "priority", "operator": "equals", "value": "high"},
                    {"type": "payload", "field": "vip", "operator": "equals", "value": "true"},
                ],
            },
        )
        store.add_relay_rule(rule)

        # First condition matches
        ids1 = engine.apply_relay_rules(
            path="/test/event",
            method="POST",
            headers={},
            body={"priority": "high", "vip": "false"},
        )
        assert len(ids1) == 1

        # Second condition matches
        ids2 = engine.apply_relay_rules(
            path="/test/event",
            method="POST",
            headers={},
            body={"priority": "low", "vip": "true"},
        )
        assert len(ids2) == 1

        # Neither matches
        ids3 = engine.apply_relay_rules(
            path="/test/event",
            method="POST",
            headers={},
            body={"priority": "low", "vip": "false"},
        )
        assert len(ids3) == 0

    def test_filtered_webhook_still_recorded(self, store, engine, endpoint):
        """Filtered webhooks should still be recorded in incoming log."""
        rule = RelayRule(
            name="Strict",
            path_pattern="/test/*",
            target_endpoint_ids=[endpoint.id],
            filter_rules={
                "logic": "all",
                "conditions": [
                    {"type": "payload", "field": "blocked", "operator": "exists", "value": None},
                ],
            },
        )
        store.add_relay_rule(rule)

        delivery_ids = engine.apply_relay_rules(
            path="/test/event",
            method="POST",
            headers={},
            body={"data": "ok"},  # No "blocked" field
        )
        assert len(delivery_ids) == 0

        # Should still have recorded the incoming webhook
        incoming = store.list_incoming()
        assert len(incoming) == 1

    def test_filter_with_numeric_comparison(self, store, engine, endpoint):
        rule = RelayRule(
            name="High Value Only",
            path_pattern="/orders/*",
            target_endpoint_ids=[endpoint.id],
            filter_rules={
                "logic": "all",
                "conditions": [
                    {"type": "payload", "field": "amount", "operator": "gte", "value": 1000},
                ],
            },
        )
        store.add_relay_rule(rule)

        # High value — forwarded
        ids1 = engine.apply_relay_rules(
            path="/orders/new",
            method="POST",
            headers={},
            body={"amount": 5000},
        )
        assert len(ids1) == 1

        # Low value — blocked
        ids2 = engine.apply_relay_rules(
            path="/orders/new",
            method="POST",
            headers={},
            body={"amount": 500},
        )
        assert len(ids2) == 0

    def test_multiple_rules_independent_filters(self, store, engine):
        """Two rules with different filters on the same path."""
        ep1 = WebhookEndpoint(name="EP1", url="https://e1.example.com")
        ep2 = WebhookEndpoint(name="EP2", url="https://e2.example.com")
        store.add_endpoint(ep1)
        store.add_endpoint(ep2)

        rule1 = RelayRule(
            name="Rule1",
            path_pattern="/webhook/*",
            target_endpoint_ids=[ep1.id],
            filter_rules={
                "logic": "all",
                "conditions": [
                    {"type": "payload", "field": "type", "operator": "equals", "value": "a"},
                ],
            },
        )
        store.add_relay_rule(rule1)

        rule2 = RelayRule(
            name="Rule2",
            path_pattern="/webhook/*",
            target_endpoint_ids=[ep2.id],
            filter_rules={
                "logic": "all",
                "conditions": [
                    {"type": "payload", "field": "type", "operator": "equals", "value": "b"},
                ],
            },
        )
        store.add_relay_rule(rule2)

        # Type A → only EP1
        ids_a = engine.apply_relay_rules(
            path="/webhook/test",
            method="POST",
            headers={},
            body={"type": "a"},
        )
        assert len(ids_a) == 1

        # Type B → only EP2
        ids_b = engine.apply_relay_rules(
            path="/webhook/test",
            method="POST",
            headers={},
            body={"type": "b"},
        )
        assert len(ids_b) == 1

        # Type C → neither
        ids_c = engine.apply_relay_rules(
            path="/webhook/test",
            method="POST",
            headers={},
            body={"type": "c"},
        )
        assert len(ids_c) == 0
