"""Tests for agent-webhook delivery engine."""

import json
from datetime import datetime, timezone

import pytest

from agent_webhook.engine import DeliveryEngine
from agent_webhook.models import (
    DeliveryAttempt,
    DeliveryStatus,
    Header,
    RelayRule,
    RetryPolicy,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookMethod,
    WebhookStatus,
)
from agent_webhook.store import WebhookStore


@pytest.fixture
def store(tmp_path):
    return WebhookStore(tmp_path / "test_engine.json")


@pytest.fixture
def engine(store):
    return DeliveryEngine(store)


@pytest.fixture
def active_endpoint(store):
    ep = WebhookEndpoint(
        name="Test Target",
        url="https://httpbin.org/post",
        tags=["test"],
    )
    store.add_endpoint(ep)
    return ep


class TestHMACSignature:
    def test_sha256(self):
        sig = DeliveryEngine.generate_hmac_signature("secret", '{"test":true}', "sha256")
        assert sig.startswith("sha256=")
        assert len(sig) > 10

    def test_sha512(self):
        sig = DeliveryEngine.generate_hmac_signature("secret", '{"test":true}', "sha512")
        assert sig.startswith("sha512=")

    def test_sha1(self):
        sig = DeliveryEngine.generate_hmac_signature("secret", '{"test":true}', "sha1")
        assert sig.startswith("sha1=")

    def test_deterministic(self):
        sig1 = DeliveryEngine.generate_hmac_signature("secret", "payload", "sha256")
        sig2 = DeliveryEngine.generate_hmac_signature("secret", "payload", "sha256")
        assert sig1 == sig2

    def test_different_payloads(self):
        sig1 = DeliveryEngine.generate_hmac_signature("secret", "payload1", "sha256")
        sig2 = DeliveryEngine.generate_hmac_signature("secret", "payload2", "sha256")
        assert sig1 != sig2

    def test_unsupported_algorithm(self):
        with pytest.raises(ValueError):
            DeliveryEngine.generate_hmac_signature("secret", "payload", "md5")


class TestBuildHeaders:
    def test_basic_headers(self, engine, active_endpoint):
        delivery = WebhookDelivery(endpoint_id=active_endpoint.id, payload={"test": True})
        headers = DeliveryEngine.build_headers(active_endpoint, delivery)
        assert headers["Content-Type"] == "application/json"
        assert "X-Webhook-ID" in headers
        assert "X-Webhook-Event" in headers
        assert "X-Webhook-Timestamp" in headers

    def test_with_signature(self, engine, active_endpoint):
        active_endpoint.secret = "my-secret"
        delivery = WebhookDelivery(endpoint_id=active_endpoint.id, payload={"test": True})
        headers = DeliveryEngine.build_headers(active_endpoint, delivery, signature="sha256=abc123")
        assert headers["X-Webhook-Signature"] == "sha256=abc123"

    def test_custom_endpoint_headers(self, engine, active_endpoint):
        active_endpoint.headers = [Header(name="X-Custom", value="my-value")]
        delivery = WebhookDelivery(endpoint_id=active_endpoint.id, payload={})
        headers = DeliveryEngine.build_headers(active_endpoint, delivery)
        assert headers["X-Custom"] == "my-value"

    def test_delivery_headers_override(self, engine, active_endpoint):
        delivery = WebhookDelivery(
            endpoint_id=active_endpoint.id,
            payload={},
            payload_headers={"X-Override": "value"},
        )
        headers = DeliveryEngine.build_headers(active_endpoint, delivery)
        assert headers["X-Override"] == "value"


class TestDeliver:
    @pytest.mark.asyncio
    async def test_deliver_endpoint_not_found(self, engine, store):
        delivery = WebhookDelivery(endpoint_id="nonexistent", payload={"test": True})
        store.add_delivery(delivery)
        attempt = await engine.deliver(delivery)
        assert attempt.status == DeliveryStatus.FAILED
        assert "not found" in attempt.error_message

    @pytest.mark.asyncio
    async def test_deliver_paused_endpoint(self, engine, store):
        ep = WebhookEndpoint(name="Paused", url="https://example.com", status=WebhookStatus.PAUSED)
        store.add_endpoint(ep)
        delivery = WebhookDelivery(endpoint_id=ep.id, payload={})
        store.add_delivery(delivery)
        attempt = await engine.deliver(delivery)
        assert attempt.status == DeliveryStatus.FAILED
        assert "paused" in attempt.error_message.lower()

    @pytest.mark.asyncio
    async def test_deliver_connection_error(self, engine, store):
        ep = WebhookEndpoint(name="Bad Host", url="https://nonexistent.invalid.example.com/hook")
        store.add_endpoint(ep)
        delivery = WebhookDelivery(endpoint_id=ep.id, payload={"test": True})
        store.add_delivery(delivery)
        attempt = await engine.deliver(delivery)
        assert attempt.status == DeliveryStatus.FAILED
        assert attempt.error_message is not None
        assert attempt.completed_at is not None


class TestProcessDelivery:
    @pytest.mark.asyncio
    async def test_process_creates_attempt(self, engine, store, active_endpoint):
        delivery = WebhookDelivery(endpoint_id=active_endpoint.id, payload={"test": True})
        store.add_delivery(delivery)
        result = await engine.process_delivery(delivery.id)
        assert result is not None
        assert len(result.attempts) >= 1

    @pytest.mark.asyncio
    async def test_process_nonexistent_delivery(self, engine, store):
        result = await engine.process_delivery("nonexistent")
        assert result is None


class TestSend:
    @pytest.mark.asyncio
    async def test_send_creates_and_processes(self, engine, store, active_endpoint):
        result = await engine.send(
            endpoint_id=active_endpoint.id,
            payload={"event": "test"},
            event_type="test.event",
        )
        assert result is not None
        assert result.event_type == "test.event"
        assert len(result.attempts) >= 1

    @pytest.mark.asyncio
    async def test_send_with_metadata(self, engine, store, active_endpoint):
        result = await engine.send(
            endpoint_id=active_endpoint.id,
            payload={"test": True},
            metadata={"source": "unit-test"},
        )
        assert result.metadata["source"] == "unit-test"


class TestRelayRules:
    def test_apply_relay_no_rules(self, engine, store):
        delivery_ids = engine.apply_relay_rules(
            path="/stripe/events",
            method="POST",
            headers={"Content-Type": "application/json"},
            body={"type": "payment"},
        )
        assert delivery_ids == []

    def test_apply_relay_with_matching_rule(self, engine, store, active_endpoint):
        rule = RelayRule(
            name="Stripe",
            path_pattern="/stripe/*",
            target_endpoint_ids=[active_endpoint.id],
        )
        store.add_relay_rule(rule)

        delivery_ids = engine.apply_relay_rules(
            path="/stripe/events",
            method="POST",
            headers={"Content-Type": "application/json"},
            body={"type": "payment"},
        )
        assert len(delivery_ids) == 1
        # Check incoming was recorded
        incoming = store.list_incoming()
        assert len(incoming) == 1
        assert incoming[0].processed
        assert active_endpoint.id in incoming[0].forwarded_to

    def test_apply_relay_no_matching_rule(self, engine, store, active_endpoint):
        rule = RelayRule(
            name="GitHub",
            path_pattern="/github/*",
            target_endpoint_ids=[active_endpoint.id],
        )
        store.add_relay_rule(rule)

        delivery_ids = engine.apply_relay_rules(
            path="/stripe/events",
            method="POST",
            headers={},
            body={},
        )
        assert len(delivery_ids) == 0

    def test_apply_relay_inactive_rule(self, engine, store, active_endpoint):
        rule = RelayRule(
            name="Inactive",
            path_pattern="/stripe/*",
            target_endpoint_ids=[active_endpoint.id],
            active=False,
        )
        store.add_relay_rule(rule)

        delivery_ids = engine.apply_relay_rules(
            path="/stripe/events",
            method="POST",
            headers={},
            body={},
        )
        assert len(delivery_ids) == 0

    def test_apply_relay_catch_all(self, engine, store, active_endpoint):
        rule = RelayRule(
            name="All",
            path_pattern="/*",
            target_endpoint_ids=[active_endpoint.id],
        )
        store.add_relay_rule(rule)

        delivery_ids = engine.apply_relay_rules(
            path="/anything/goes",
            method="POST",
            headers={},
            body={},
        )
        assert len(delivery_ids) == 1

    def test_apply_relay_multiple_targets(self, engine, store):
        ep1 = WebhookEndpoint(name="Target 1", url="https://example.com/1")
        ep2 = WebhookEndpoint(name="Target 2", url="https://example.com/2")
        store.add_endpoint(ep1)
        store.add_endpoint(ep2)

        rule = RelayRule(
            name="Multi",
            path_pattern="/events/*",
            target_endpoint_ids=[ep1.id, ep2.id],
        )
        store.add_relay_rule(rule)

        delivery_ids = engine.apply_relay_rules(
            path="/events/test",
            method="POST",
            headers={},
            body={"test": True},
        )
        assert len(delivery_ids) == 2

    def test_apply_relay_string_body(self, engine, store, active_endpoint):
        rule = RelayRule(
            name="Raw",
            path_pattern="/raw/*",
            target_endpoint_ids=[active_endpoint.id],
        )
        store.add_relay_rule(rule)

        delivery_ids = engine.apply_relay_rules(
            path="/raw/data",
            method="POST",
            headers={},
            body="raw string body",
        )
        assert len(delivery_ids) == 1
        # Check the delivery payload wraps the raw body
        delivery = store.get_delivery(delivery_ids[0])
        assert "raw_body" in delivery.payload
