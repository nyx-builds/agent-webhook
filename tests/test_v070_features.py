"""Tests for v0.7.0 features: alert rules, data retention, API key auth."""

from __future__ import annotations

import asyncio
import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from agent_webhook.alerts import (
    AlertCondition,
    AlertEvent,
    AlertManager,
    AlertRule,
    AlertSeverity,
    AlertStatus,
    CallbackChannel,
    LogChannel,
    WebhookChannel,
    default_alert_rules,
)
from agent_webhook.auth import APIKey, APIKeyManager, create_auth_dependency
from agent_webhook.models import (
    DeliveryStatus,
    WebhookEndpoint,
    WebhookStatus,
)
from agent_webhook.retention import (
    CleanupResult,
    RetentionManager,
    RetentionPolicy,
)
from agent_webhook.service import WebhookService
from agent_webhook.store_sqlite import SQLiteStore


# ── Alert Module Tests ────────────────────────────────────────────────


class TestAlertCondition:
    """Alert condition enum tests."""

    def test_all_conditions(self):
        assert AlertCondition.CIRCUIT_OPEN.value == "circuit_open"
        assert AlertCondition.DLQ_THRESHOLD.value == "dlq_threshold"
        assert AlertCondition.ENDPOINT_FAILURE_RATE.value == "endpoint_failure_rate"
        assert AlertCondition.ENDPOINT_DOWN.value == "endpoint_down"
        assert AlertCondition.DELIVERY_STALLED.value == "delivery_stalled"


class TestAlertRule:
    """AlertRule model tests."""

    def test_rule_defaults(self):
        rule = AlertRule(
            name="test",
            condition=AlertCondition.DLQ_THRESHOLD,
            severity=AlertSeverity.WARNING,
        )
        assert rule.enabled is True
        assert rule.threshold == 0
        assert rule.cooldown_seconds == 300
        assert rule.endpoint_id is None
        assert rule.tag is None
        assert len(rule.channels) == 1  # LogChannel by default

    def test_rule_with_custom_channels(self):
        rule = AlertRule(
            name="custom",
            condition=AlertCondition.CIRCUIT_OPEN,
            severity=AlertSeverity.CRITICAL,
            channels=[LogChannel(), WebhookChannel(endpoint_id="ep1")],
        )
        assert len(rule.channels) == 2

    def test_rule_id_is_unique(self):
        r1 = AlertRule(name="a", condition=AlertCondition.DLQ_THRESHOLD, severity=AlertSeverity.WARNING)
        r2 = AlertRule(name="a", condition=AlertCondition.DLQ_THRESHOLD, severity=AlertSeverity.WARNING)
        assert r1.id != r2.id


class TestAlertEvent:
    """AlertEvent model tests."""

    def test_event_creation(self):
        ev = AlertEvent(
            rule_id="rule-1",
            rule_name="DLQ Check",
            condition=AlertCondition.DLQ_THRESHOLD,
            severity=AlertSeverity.WARNING,
            status=AlertStatus.FIRING,
            message="DLQ has 15 entries",
            details={"count": 15},
        )
        assert ev.status == AlertStatus.FIRING
        assert ev.details["count"] == 15
        assert ev.timestamp is not None

    def test_event_to_dict(self):
        ev = AlertEvent(
            rule_id="r1",
            rule_name="Test",
            condition=AlertCondition.CIRCUIT_OPEN,
            severity=AlertSeverity.CRITICAL,
            status=AlertStatus.FIRING,
            message="Breaker open",
        )
        d = ev.to_dict()
        assert d["rule_name"] == "Test"
        assert d["status"] == "firing"
        assert d["severity"] == "critical"
        assert d["condition"] == "circuit_open"


class TestAlertManager:
    """AlertManager evaluation tests."""

    @pytest.fixture
    def service(self, tmp_path):
        store = SQLiteStore(str(tmp_path / "test.db"))
        return WebhookService(store=store)

    def test_add_and_remove_rule(self, service):
        mgr = AlertManager(service)
        rule = AlertRule(name="t", condition=AlertCondition.DLQ_THRESHOLD, severity=AlertSeverity.WARNING)
        mgr.add_rule(rule)
        assert len(mgr.rules) == 1
        retrieved = mgr.get_rule(rule.id)
        assert retrieved is not None
        mgr.remove_rule(rule.id)
        assert len(mgr.rules) == 0

    def test_clear_rules(self, service):
        mgr = AlertManager(service)
        for i in range(3):
            mgr.add_rule(AlertRule(name=f"r{i}", condition=AlertCondition.DLQ_THRESHOLD, severity=AlertSeverity.WARNING))
        assert len(mgr.rules) == 3
        mgr.clear_rules()
        assert len(mgr.rules) == 0

    def test_evaluate_dlq_threshold_no_data(self, service):
        """No DLQ entries → no alerts."""
        mgr = AlertManager(service)
        rule = AlertRule(
            name="DLQ",
            condition=AlertCondition.DLQ_THRESHOLD,
            severity=AlertSeverity.WARNING,
            threshold=10,
        )
        mgr.add_rule(rule)
        events = asyncio.run(mgr.evaluate_all())
        # No DLQ entries → should not fire
        firing = [e for e in events if e.status == AlertStatus.FIRING]
        assert len(firing) == 0

    def test_evaluate_dlq_threshold_triggers(self, service):
        """DLQ entries above threshold → fires."""
        # Add dead-letter entries
        ep = WebhookEndpoint(name="test", url="https://example.com/hook", secret="s")
        service.add_endpoint(ep)
        for i in range(15):
            service.add_to_dead_letter(
                endpoint_id=ep.id,
                payload={"i": i},
                error="connection refused",
                attempt_count=3,
            )
        mgr = AlertManager(service)
        rule = AlertRule(
            name="DLQ",
            condition=AlertCondition.DLQ_THRESHOLD,
            severity=AlertSeverity.CRITICAL,
            threshold=10,
        )
        mgr.add_rule(rule)
        events = asyncio.run(mgr.evaluate_all())
        firing = [e for e in events if e.status == AlertStatus.FIRING]
        assert len(firing) >= 1
        assert firing[0].condition == AlertCondition.DLQ_THRESHOLD

    def test_evaluate_endpoint_down(self, service):
        """Disabled endpoint → fires endpoint_down."""
        ep = WebhookEndpoint(name="down", url="https://down.example.com", secret="s")
        service.add_endpoint(ep)
        service.disable_endpoint(ep.id)
        mgr = AlertManager(service)
        rule = AlertRule(
            name="Down",
            condition=AlertCondition.ENDPOINT_DOWN,
            severity=AlertSeverity.WARNING,
        )
        mgr.add_rule(rule)
        events = asyncio.run(mgr.evaluate_all())
        firing = [e for e in events if e.status == AlertStatus.FIRING]
        assert len(firing) >= 1

    def test_evaluate_circuit_open_no_open(self, service):
        """No open circuit breakers → no alert."""
        ep = WebhookEndpoint(name="ok", url="https://ok.example.com", secret="s")
        service.add_endpoint(ep)
        mgr = AlertManager(service)
        rule = AlertRule(
            name="CB",
            condition=AlertCondition.CIRCUIT_OPEN,
            severity=AlertSeverity.CRITICAL,
        )
        mgr.add_rule(rule)
        events = asyncio.run(mgr.evaluate_all())
        firing = [e for e in events if e.status == AlertStatus.FIRING]
        assert len(firing) == 0

    def test_get_alert_summary(self, service):
        mgr = AlertManager(service)
        for r in default_alert_rules():
            mgr.add_rule(r)
        summary = mgr.get_alert_summary()
        assert "total_rules" in summary
        assert summary["total_rules"] == 5

    def test_default_alert_rules(self):
        rules = default_alert_rules()
        assert len(rules) == 5
        conditions = {r.condition for r in rules}
        assert AlertCondition.CIRCUIT_OPEN in conditions
        assert AlertCondition.DLQ_THRESHOLD in conditions
        assert AlertCondition.ENDPOINT_FAILURE_RATE in conditions
        assert AlertCondition.ENDPOINT_DOWN in conditions
        assert AlertCondition.DELIVERY_STALLED in conditions

    def test_default_alert_rules_with_notify(self):
        rules = default_alert_rules(notify_endpoint_id="ep-notify")
        # Each rule should have a webhook channel for notifications
        for r in rules:
            webhook_channels = [c for c in r.channels if isinstance(c, WebhookChannel)]
            assert len(webhook_channels) >= 1

    def test_active_alerts(self, service):
        mgr = AlertManager(service)
        for r in default_alert_rules():
            mgr.add_rule(r)
        active = mgr.get_active_alerts()
        assert isinstance(active, list)


class TestNotificationChannels:
    """Notification channel tests."""

    def test_log_channel_notify(self):
        ch = LogChannel()
        event = AlertEvent(
            rule_id="r1",
            rule_name="Test",
            condition=AlertCondition.DLQ_THRESHOLD,
            severity=AlertSeverity.WARNING,
            status=AlertStatus.FIRING,
            message="Test alert",
        )
        asyncio.run(ch.notify(event))  # Should not raise

    def test_webhook_channel_notify_no_service(self):
        ch = WebhookChannel(endpoint_id="ep1")
        event = AlertEvent(
            rule_id="r1",
            rule_name="Test",
            condition=AlertCondition.DLQ_THRESHOLD,
            severity=AlertSeverity.WARNING,
            status=AlertStatus.FIRING,
            message="Test alert",
        )
        # Without a service, it should not crash
        asyncio.run(ch.notify(event))

    def test_callback_channel(self):
        received = []

        async def cb(event):
            received.append(event)

        ch = CallbackChannel(callback=cb)
        event = AlertEvent(
            rule_id="r1",
            rule_name="CB",
            condition=AlertCondition.CIRCUIT_OPEN,
            severity=AlertSeverity.CRITICAL,
            status=AlertStatus.FIRING,
            message="Test",
        )
        asyncio.run(ch.notify(event))
        assert len(received) == 1
        assert received[0].message == "Test"


# ── Retention Module Tests ────────────────────────────────────────────


class TestRetentionPolicy:
    """Retention policy configuration tests."""

    def test_defaults(self):
        p = RetentionPolicy()
        assert p.delivery_retention_days == 30
        assert p.delivery_keep_failed is False
        assert p.event_log_retention_days == 90
        assert p.dead_letter_retention_days == 180
        assert p.dead_letter_delete_replayed is True
        assert p.incoming_retention_days == 7
        assert p.cleanup_batch_size == 5000

    def test_custom_policy(self):
        p = RetentionPolicy(
            delivery_retention_days=7,
            event_log_retention_days=30,
            dead_letter_retention_days=60,
            incoming_retention_days=3,
        )
        assert p.delivery_retention_days == 7
        assert p.event_log_retention_days == 30

    def test_from_dict(self):
        p = RetentionPolicy.from_dict({
            "delivery_retention_days": 14,
            "event_log_retention_days": 45,
        })
        assert p.delivery_retention_days == 14
        assert p.event_log_retention_days == 45
        # Others should be defaults
        assert p.dead_letter_retention_days == 180

    def test_from_dict_empty(self):
        p = RetentionPolicy.from_dict({})
        assert p.delivery_retention_days == 30


class TestRetentionManager:
    """Retention cleanup manager tests."""

    @pytest.fixture
    def service(self, tmp_path):
        store = SQLiteStore(str(tmp_path / "retention.db"))
        return WebhookService(store=store)

    def test_get_estimates_empty_store(self, service):
        mgr = RetentionManager(service)
        estimates = mgr.get_estimates()
        assert isinstance(estimates, dict)
        assert all(v == 0 for v in estimates.values())

    def test_run_cleanup_empty_store(self, service):
        mgr = RetentionManager(service)
        result = mgr.run_cleanup()
        assert isinstance(result, CleanupResult)
        assert result.total_deleted == 0
        assert len(result.errors) == 0

    def test_cleanup_old_deliveries(self, service):
        """Deliveries older than retention window are deleted."""
        ep = WebhookEndpoint(name="ep", url="https://example.com", secret="s")
        service.add_endpoint(ep)

        # Create an old delivery (manually set created_at in the past)
        from agent_webhook.models import WebhookDelivery
        old_time = datetime.now(timezone.utc) - timedelta(days=45)
        delivery = WebhookDelivery(
            endpoint_id=ep.id,
            payload={"msg": "old"},
            status=DeliveryStatus.SUCCESS,
            created_at=old_time,
        )
        service.store.add_delivery(delivery)

        mgr = RetentionManager(service, RetentionPolicy(delivery_retention_days=30))
        estimates = mgr.get_estimates()
        assert estimates.get("deleted_deliveries", 0) >= 1

        result = mgr.run_cleanup()
        assert result.deleted_deliveries >= 1

    def test_cleanup_keeps_recent(self, service):
        """Recent deliveries are not deleted."""
        ep = WebhookEndpoint(name="ep", url="https://example.com", secret="s")
        service.add_endpoint(ep)
        delivery = service.create_delivery(
            endpoint_id=ep.id,
            payload={"msg": "recent"},
        )
        mgr = RetentionManager(service, RetentionPolicy(delivery_retention_days=30))
        result = mgr.run_cleanup()
        assert result.deleted_deliveries == 0

    def test_cleanup_keep_failed(self, service):
        """Failed deliveries are kept when keep_failed=True."""
        ep = WebhookEndpoint(name="ep", url="https://example.com", secret="s")
        service.add_endpoint(ep)

        from agent_webhook.models import WebhookDelivery
        old_time = datetime.now(timezone.utc) - timedelta(days=45)
        failed_delivery = WebhookDelivery(
            endpoint_id=ep.id,
            payload={"msg": "old failed"},
            status=DeliveryStatus.FAILED,
            created_at=old_time,
        )
        service.store.add_delivery(failed_delivery)

        mgr = RetentionManager(service, RetentionPolicy(
            delivery_retention_days=30,
            delivery_keep_failed=True,
        ))
        result = mgr.run_cleanup()
        assert result.deleted_deliveries == 0

    def test_cleanup_dlq(self, service):
        """Old dead letter entries are cleaned up."""
        ep = WebhookEndpoint(name="ep", url="https://example.com", secret="s")
        service.add_endpoint(ep)

        # Add to DLQ
        for i in range(5):
            service.add_to_dead_letter(
                endpoint_id=ep.id,
                payload={"i": i},
                error="timeout",
                attempt_count=3,
            )
        assert service.dead_letter_count() == 5

        # Use 0-day DLQ retention to clean everything
        mgr = RetentionManager(service, RetentionPolicy(dead_letter_retention_days=0))
        result = mgr.run_cleanup()
        assert result.deleted_dead_letter >= 1

    def test_result_to_dict(self):
        r = CleanupResult(
            deleted_deliveries=5,
            deleted_event_logs=10,
            deleted_dead_letter=3,
            deleted_incoming=2,
        )
        d = r.to_dict()
        assert d["total_deleted"] == 20
        assert d["deleted_deliveries"] == 5
        assert "ran_at" in d
        assert "errors" in d


# ── API Key Auth Tests ────────────────────────────────────────────────


class TestAPIKey:
    """APIKey dataclass tests."""

    def test_key_defaults(self):
        k = APIKey(key_hash="abc123", name="test-key")
        assert k.name == "test-key"
        assert k.scopes == ["*"]
        assert k.active is True
        assert k.expires_at is None

    def test_key_is_valid(self):
        k = APIKey(key_hash="abc123", name="test")
        assert k.is_valid is True

    def test_key_revoked_not_valid(self):
        k = APIKey(key_hash="abc", name="test", active=False)
        assert k.is_valid is False

    def test_key_expired(self):
        past = datetime.now(timezone.utc).timestamp() - 100
        k = APIKey(key_hash="abc", name="test", expires_at=past)
        assert k.is_valid is False

    def test_has_scope(self):
        k = APIKey(key_hash="abc", name="test", scopes=["read", "write"])
        assert k.has_scope("read") is True
        assert k.has_scope("write") is True
        assert k.has_scope("admin") is False

    def test_has_scope_wildcard(self):
        k = APIKey(key_hash="abc", name="test", scopes=["*"])
        assert k.has_scope("anything") is True

    def test_to_dict(self):
        k = APIKey(key_hash="secret", name="test-key", scopes=["read"])
        d = k.to_dict()
        assert d["name"] == "test-key"
        assert d["scopes"] == ["read"]
        assert d["active"] is True
        assert "key_hash" not in d  # Hash should NOT be exposed


class TestAPIKeyManager:
    """APIKeyManager tests."""

    def test_create_key(self):
        mgr = APIKeyManager()
        raw_key, key_info = mgr.create_key(name="test-api")
        assert raw_key.startswith("whk_")
        assert key_info.name == "test-api"
        assert key_info.active is True

    def test_create_key_with_scopes(self):
        mgr = APIKeyManager()
        raw, info = mgr.create_key(name="scoped", scopes=["read", "write"])
        assert info.scopes == ["read", "write"]

    def test_create_key_with_expiry(self):
        mgr = APIKeyManager()
        raw, info = mgr.create_key(name="temp", expires_in_seconds=3600)
        assert info.expires_at is not None
        assert info.is_valid is True

    def test_verify_valid_key(self):
        mgr = APIKeyManager()
        raw, info = mgr.create_key(name="verifiable")
        verified = mgr.verify(raw)
        assert verified is not None
        assert verified.name == "verifiable"

    def test_verify_invalid_key(self):
        mgr = APIKeyManager()
        result = mgr.verify("whk_invalid_key_12345")
        assert result is None

    def test_verify_revoked_key(self):
        mgr = APIKeyManager()
        raw, info = mgr.create_key(name="to-revoke")
        assert mgr.revoke("to-revoke") is True
        result = mgr.verify(raw)
        assert result is None

    def test_revoke_nonexistent(self):
        mgr = APIKeyManager()
        assert mgr.revoke("nonexistent") is False

    def test_list_keys(self):
        mgr = APIKeyManager()
        mgr.create_key(name="key1")
        mgr.create_key(name="key2")
        keys = mgr.list_keys()
        assert len(keys) == 2
        assert all("key_hash" not in k for k in keys)  # No hashes exposed

    def test_get_key_by_name(self):
        mgr = APIKeyManager()
        mgr.create_key(name="findme")
        key = mgr.get_key_by_name("findme")
        assert key is not None
        assert key.name == "findme"

    def test_get_key_by_name_not_found(self):
        mgr = APIKeyManager()
        assert mgr.get_key_by_name("nope") is None

    def test_add_existing_key(self):
        mgr = APIKeyManager()
        mgr.add_existing_key("whk_preexisting_key", name="imported")
        verified = mgr.verify("whk_preexisting_key")
        assert verified is not None
        assert verified.name == "imported"

    def test_revoke_all(self):
        mgr = APIKeyManager()
        mgr.create_key(name="k1")
        mgr.create_key(name="k2")
        mgr.create_key(name="k3")
        count = mgr.revoke_all()
        assert count == 3
        assert len(mgr.list_keys()) == 0 or all(not k["active"] for k in mgr.list_keys())

    def test_key_hash_not_plaintext(self):
        """Key hash should be different from the raw key."""
        mgr = APIKeyManager()
        raw, info = mgr.create_key(name="security")
        assert info.key_hash != raw
        assert info.key_hash != raw.replace("whk_", "")


# ── Integration: Alert + Service ──────────────────────────────────────


class TestAlertServiceIntegration:
    """Integration tests for alert evaluation with real service."""

    def test_full_alert_flow(self, tmp_path):
        """End-to-end: create endpoint, disable it, evaluate alert, get event."""
        store = SQLiteStore(str(tmp_path / "integration.db"))
        service = WebhookService(store=store)

        ep = WebhookEndpoint(name="monitoring", url="https://example.com", secret="s")
        service.add_endpoint(ep)
        service.disable_endpoint(ep.id)

        mgr = AlertManager(service)
        mgr.add_rule(AlertRule(
            name="Endpoint Down Monitor",
            condition=AlertCondition.ENDPOINT_DOWN,
            severity=AlertSeverity.WARNING,
        ))

        events = asyncio.run(mgr.evaluate_all())
        assert len(events) > 0
        firing = [e for e in events if e.status == AlertStatus.FIRING]
        assert any(e.condition == AlertCondition.ENDPOINT_DOWN for e in firing)


# ── REST API Endpoint Tests ───────────────────────────────────────────


class TestAlertRestEndpoints:
    """Test alert-related REST endpoints."""

    @pytest.fixture
    def client(self, tmp_path):
        from fastapi.testclient import TestClient
        from agent_webhook.rest_api import create_app

        store = SQLiteStore(str(tmp_path / "rest.db"))
        service = WebhookService(store=store)
        app = create_app(service=service)
        return TestClient(app)

    def test_alert_summary_endpoint(self, client):
        resp = client.get("/alerts/summary")
        assert resp.status_code == 200
        data = resp.json()
        assert "total_rules" in data

    def test_alert_evaluate_endpoint(self, client):
        resp = client.post("/alerts/evaluate", json={})
        assert resp.status_code == 200
        data = resp.json()
        assert "evaluated" in data
        assert "events" in data

    def test_alert_evaluate_specific_condition(self, client):
        resp = client.post("/alerts/evaluate", json={
            "condition": "dlq_threshold",
            "threshold": 5,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["evaluated"] == 1


class TestRetentionRestEndpoints:
    """Test retention-related REST endpoints."""

    @pytest.fixture
    def client(self, tmp_path):
        from fastapi.testclient import TestClient
        from agent_webhook.rest_api import create_app

        store = SQLiteStore(str(tmp_path / "retention_rest.db"))
        service = WebhookService(store=store)
        app = create_app(service=service)
        return TestClient(app)

    def test_retention_estimate(self, client):
        resp = client.get("/retention/estimate")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, dict)

    def test_retention_cleanup_dry_run(self, client):
        resp = client.post("/retention/cleanup", json={"dry_run": True})
        assert resp.status_code == 200
        data = resp.json()
        assert data["dry_run"] is True

    def test_retention_cleanup_real(self, client):
        resp = client.post("/retention/cleanup", json={})
        assert resp.status_code == 200
        data = resp.json()
        assert "total_deleted" in data


class TestApiKeyRestEndpoint:
    """Test API key generation endpoint."""

    @pytest.fixture
    def client(self, tmp_path):
        from fastapi.testclient import TestClient
        from agent_webhook.rest_api import create_app

        store = SQLiteStore(str(tmp_path / "apikey.db"))
        service = WebhookService(store=store)
        app = create_app(service=service)
        return TestClient(app)

    def test_generate_key(self, client):
        resp = client.post("/apikeys/generate", params={"name": "test-key"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["key"].startswith("whk_")
        assert data["name"] == "test-key"
        assert "note" in data

    def test_generate_key_with_scopes(self, client):
        resp = client.post("/apikeys/generate", params={
            "name": "scoped",
            "scope": ["read", "write"],
        })
        assert resp.status_code == 200
        data = resp.json()
        assert "read" in data["scopes"]


# ── CLI Command Tests ─────────────────────────────────────────────────


class TestAlertCLI:
    """Test alert CLI commands."""

    def test_alert_list_presets(self):
        from click.testing import CliRunner
        from agent_webhook.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, ["alert", "list-presets"])
        assert result.exit_code == 0
        assert "Alert Conditions" in result.output or "circuit_open" in result.output.lower()

    def test_alert_summary(self, tmp_path):
        from click.testing import CliRunner
        from agent_webhook.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, [
            "alert", "summary",
            "--store", str(tmp_path / "cli_alert.db"),
        ])
        assert result.exit_code == 0

    def test_alert_evaluate(self, tmp_path):
        from click.testing import CliRunner
        from agent_webhook.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, [
            "alert", "evaluate",
            "--store", str(tmp_path / "cli_alert2.db"),
            "--condition", "dlq_threshold",
            "--threshold", "5",
        ])
        assert result.exit_code == 0


class TestRetentionCLI:
    """Test retention CLI commands."""

    def test_retention_show(self, tmp_path):
        from click.testing import CliRunner
        from agent_webhook.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, [
            "retention", "show",
            "--store", str(tmp_path / "cli_ret.db"),
        ])
        assert result.exit_code == 0
        assert "Retention Policy" in result.output

    def test_retention_run_dry_run(self, tmp_path):
        from click.testing import CliRunner
        from agent_webhook.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, [
            "retention", "run",
            "--store", str(tmp_path / "cli_ret2.db"),
            "--dry-run",
        ])
        assert result.exit_code == 0
        assert "Dry Run" in result.output

    def test_retention_run_real(self, tmp_path):
        from click.testing import CliRunner
        from agent_webhook.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, [
            "retention", "run",
            "--store", str(tmp_path / "cli_ret3.db"),
        ])
        assert result.exit_code == 0
        assert "Cleanup complete" in result.output


class TestApiKeyCLI:
    """Test API key CLI commands."""

    def test_apikey_generate(self):
        from click.testing import CliRunner
        from agent_webhook.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, [
            "apikey", "generate",
            "--name", "test-cli-key",
        ])
        assert result.exit_code == 0
        assert "whk_" in result.output

    def test_apikey_generate_with_scope(self):
        from click.testing import CliRunner
        from agent_webhook.cli import cli

        runner = CliRunner()
        result = runner.invoke(cli, [
            "apikey", "generate",
            "--name", "scoped",
            "--scope", "read",
            "--scope", "write",
        ])
        assert result.exit_code == 0
        assert "read" in result.output
