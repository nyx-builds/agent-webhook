"""Tests for agent-webhook CLI."""

import json
import tempfile

import pytest
from click.testing import CliRunner

from agent_webhook.cli import cli
from agent_webhook.models import (
    EventSubscription,
    RelayRule,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookStatus,
)
from agent_webhook.store import WebhookStore


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def store_path(tmp_path):
    return str(tmp_path / "test_cli_store.json")


@pytest.fixture
def store(store_path):
    return WebhookStore(store_path)


@pytest.fixture
def sample_endpoint(store):
    ep = WebhookEndpoint(name="CLI Test", url="https://example.com/webhook", tags=["test"])
    store.add_endpoint(ep)
    return ep


class TestEndpointCommands:
    def test_endpoint_add(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "add", "Test EP", "https://example.com/hook"])
        assert result.exit_code == 0
        assert "Endpoint created" in result.output
        assert "Test EP" in result.output

    def test_endpoint_add_with_options(self, runner, store_path):
        result = runner.invoke(cli, [
            "-s", store_path, "endpoint", "add", "Full EP", "https://example.com/hook",
            "--method", "PUT",
            "--tag", "production",
            "--secret", "my-secret",
            "--timeout", "60",
            "--description", "Test description",
            "--max-retries", "5",
        ])
        assert result.exit_code == 0
        assert "Full EP" in result.output

    def test_endpoint_add_with_header(self, runner, store_path):
        result = runner.invoke(cli, [
            "-s", store_path, "endpoint", "add", "Header EP", "https://example.com/hook",
            "--header", "Authorization: Bearer token",
        ])
        assert result.exit_code == 0

    def test_endpoint_add_invalid_header(self, runner, store_path):
        result = runner.invoke(cli, [
            "-s", store_path, "endpoint", "add", "Bad Header", "https://example.com/hook",
            "--header", "InvalidHeader",
        ])
        assert result.exit_code != 0

    def test_endpoint_list(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "list"])
        assert result.exit_code == 0
        assert "CLI Test" in result.output

    def test_endpoint_list_empty(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "list"])
        assert result.exit_code == 0
        assert "No endpoints found" in result.output

    def test_endpoint_list_by_status(self, runner, store_path, store, sample_endpoint):
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "list", "--status", "active"])
        assert result.exit_code == 0
        assert "CLI Test" in result.output

    def test_endpoint_list_by_tag(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "list", "--tag", "test"])
        assert result.exit_code == 0
        assert "CLI Test" in result.output

    def test_endpoint_show(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "show", sample_endpoint.id])
        assert result.exit_code == 0
        assert "CLI Test" in result.output
        assert "https://example.com/webhook" in result.output

    def test_endpoint_show_not_found(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "show", "nonexistent"])
        assert result.exit_code != 0

    def test_endpoint_show_partial_id(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "show", sample_endpoint.id[:8]])
        assert result.exit_code == 0
        assert "CLI Test" in result.output

    def test_endpoint_pause(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "pause", sample_endpoint.id])
        assert result.exit_code == 0
        assert "paused" in result.output.lower()

    def test_endpoint_resume(self, runner, store_path, sample_endpoint):
        runner.invoke(cli, ["-s", store_path, "endpoint", "pause", sample_endpoint.id])
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "resume", sample_endpoint.id])
        assert result.exit_code == 0
        assert "resumed" in result.output.lower()

    def test_endpoint_delete(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "delete", sample_endpoint.id])
        assert result.exit_code == 0
        assert "deleted" in result.output.lower()

    def test_endpoint_delete_not_found(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "endpoint", "delete", "nonexistent"])
        assert result.exit_code != 0


class TestDeliveryCommands:
    def test_delivery_list(self, runner, store_path, store, sample_endpoint):
        delivery = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={"test": True})
        store.add_delivery(delivery)
        result = runner.invoke(cli, ["-s", store_path, "delivery", "list"])
        assert result.exit_code == 0
        assert "Webhook Deliveries" in result.output

    def test_delivery_list_empty(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "delivery", "list"])
        assert result.exit_code == 0
        assert "No deliveries found" in result.output

    def test_delivery_show(self, runner, store_path, store, sample_endpoint):
        delivery = WebhookDelivery(
            endpoint_id=sample_endpoint.id,
            payload={"test": True},
            event_type="test.event",
        )
        store.add_delivery(delivery)
        result = runner.invoke(cli, ["-s", store_path, "delivery", "show", delivery.id])
        assert result.exit_code == 0
        assert "test.event" in result.output

    def test_delivery_show_not_found(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "delivery", "show", "nonexistent"])
        assert result.exit_code != 0

    def test_delivery_cancel(self, runner, store_path, store, sample_endpoint):
        from agent_webhook.models import DeliveryStatus
        delivery = WebhookDelivery(
            endpoint_id=sample_endpoint.id,
            payload={"test": True},
            status=DeliveryStatus.PENDING,
        )
        store.add_delivery(delivery)
        result = runner.invoke(cli, ["-s", store_path, "delivery", "cancel", delivery.id])
        assert result.exit_code == 0
        assert "cancelled" in result.output.lower()

    def test_delivery_cancel_non_cancelable(self, runner, store_path, store, sample_endpoint):
        from agent_webhook.models import DeliveryStatus
        delivery = WebhookDelivery(
            endpoint_id=sample_endpoint.id,
            payload={"test": True},
            status=DeliveryStatus.SUCCESS,
        )
        store.add_delivery(delivery)
        result = runner.invoke(cli, ["-s", store_path, "delivery", "cancel", delivery.id])
        assert result.exit_code != 0

    def test_delivery_cancel_not_found(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "delivery", "cancel", "nonexistent"])
        assert result.exit_code != 0


class TestSendCommand:
    def test_send_json_payload(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, [
            "-s", store_path, "send", sample_endpoint.id,
            '{"test": true}',
        ])
        # Will fail because no real endpoint, but should attempt
        assert result.exit_code == 0

    def test_send_not_found_endpoint(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "send", "nonexistent", '{"test": true}'])
        assert result.exit_code != 0

    def test_send_invalid_json(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, ["-s", store_path, "send", sample_endpoint.id, "not-json"])
        assert result.exit_code != 0


class TestBatchSendCommand:
    def test_batch_send(self, runner, store_path, sample_endpoint):
        ep2 = WebhookEndpoint(name="Second EP", url="https://other.example.com/hook")
        store = WebhookStore(store_path)
        store.add_endpoint(ep2)

        result = runner.invoke(cli, [
            "-s", store_path, "batch-send",
            '{"event": "broadcast"}',
            "--endpoint", sample_endpoint.id,
            "--endpoint", ep2.id,
        ])
        assert result.exit_code == 0
        assert "Batch sent" in result.output


class TestSubscriptionCommands:
    def test_subscription_add(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, [
            "-s", store_path, "subscription", "add", sample_endpoint.id,
            "--event-type", "order.created",
            "--event-type", "order.updated",
        ])
        assert result.exit_code == 0
        assert "Subscription created" in result.output

    def test_subscription_add_not_found(self, runner, store_path):
        result = runner.invoke(cli, [
            "-s", store_path, "subscription", "add", "nonexistent",
            "--event-type", "test",
        ])
        assert result.exit_code != 0

    def test_subscription_list(self, runner, store_path, store, sample_endpoint):
        sub = EventSubscription(endpoint_id=sample_endpoint.id, event_types=["test.event"])
        store.add_subscription(sub)
        result = runner.invoke(cli, ["-s", store_path, "subscription", "list"])
        assert result.exit_code == 0
        assert "test.event" in result.output

    def test_subscription_list_empty(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "subscription", "list"])
        assert result.exit_code == 0
        assert "No subscriptions found" in result.output

    def test_subscription_delete(self, runner, store_path, store, sample_endpoint):
        sub = EventSubscription(endpoint_id=sample_endpoint.id, event_types=["test"])
        store.add_subscription(sub)
        result = runner.invoke(cli, ["-s", store_path, "subscription", "delete", sub.id])
        assert result.exit_code == 0
        assert "deleted" in result.output.lower()


class TestRelayCommands:
    def test_relay_add(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, [
            "-s", store_path, "relay", "add", "Test Relay", "/test/*",
            "--target", sample_endpoint.id,
        ])
        assert result.exit_code == 0
        assert "Relay rule created" in result.output

    def test_relay_list(self, runner, store_path, store, sample_endpoint):
        rule = RelayRule(name="Test", path_pattern="/test/*", target_endpoint_ids=[sample_endpoint.id])
        store.add_relay_rule(rule)
        result = runner.invoke(cli, ["-s", store_path, "relay", "list"])
        assert result.exit_code == 0
        assert "Test" in result.output

    def test_relay_list_empty(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "relay", "list"])
        assert result.exit_code == 0
        assert "No relay rules found" in result.output

    def test_relay_delete(self, runner, store_path, store, sample_endpoint):
        rule = RelayRule(name="Test", path_pattern="/test/*", target_endpoint_ids=[sample_endpoint.id])
        store.add_relay_rule(rule)
        result = runner.invoke(cli, ["-s", store_path, "relay", "delete", rule.id])
        assert result.exit_code == 0
        assert "deleted" in result.output.lower()


class TestStatsCommand:
    def test_stats_all(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, ["-s", store_path, "stats"])
        assert result.exit_code == 0
        assert "CLI Test" in result.output

    def test_stats_specific(self, runner, store_path, sample_endpoint):
        result = runner.invoke(cli, ["-s", store_path, "stats", sample_endpoint.id])
        assert result.exit_code == 0

    def test_stats_not_found(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "stats", "nonexistent"])
        assert result.exit_code != 0


class TestIncomingCommands:
    def test_incoming_list_empty(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "incoming", "list"])
        assert result.exit_code == 0
        assert "No incoming webhooks found" in result.output


class TestEventLogCommand:
    def test_event_log_empty(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "event-log"])
        assert result.exit_code == 0
        assert "No event log entries found" in result.output

    def test_event_log_with_entries(self, runner, store_path, store):
        from agent_webhook.models import EventLogEntry
        entry = EventLogEntry(event_type="test.event", details={"key": "value"})
        store.add_event_log(entry)
        result = runner.invoke(cli, ["-s", store_path, "event-log"])
        assert result.exit_code == 0
        assert "test.event" in result.output

    def test_event_log_with_filter(self, runner, store_path, store):
        from agent_webhook.models import EventLogEntry
        store.add_event_log(EventLogEntry(event_type="type.a"))
        store.add_event_log(EventLogEntry(event_type="type.b"))
        result = runner.invoke(cli, ["-s", store_path, "event-log", "--event-type", "type.a"])
        assert result.exit_code == 0
        assert "type.a" in result.output


class TestProcessPendingCommand:
    def test_process_pending_empty(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "process-pending"])
        assert result.exit_code == 0
        assert "No pending deliveries" in result.output


class TestHealthCheckCommand:
    def test_health_check_not_found(self, runner, store_path):
        result = runner.invoke(cli, ["-s", store_path, "health-check", "nonexistent"])
        assert result.exit_code != 0
