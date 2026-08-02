"""Tests for agent-webhook v0.8.0 — Jitter, Idempotency Keys, Timestamped Signatures."""

import hashlib
import hmac
import json
import time
from datetime import datetime, timezone

import pytest
import respx

from agent_webhook.engine import DeliveryEngine
from agent_webhook.models import (
    DeliveryStatus,
    Header,
    JitterType,
    RetryPolicy,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookMethod,
    WebhookStatus,
)
from agent_webhook.store import WebhookStore
from agent_webhook.service import WebhookService


# ─── Jitter Strategies ───────────────────────────────────────────────


class TestJitterStrategies:
    """Test all four jitter strategies for retry backoff."""

    def test_jitter_type_enum_values(self):
        assert JitterType.NONE.value == "none"
        assert JitterType.FULL.value == "full"
        assert JitterType.EQUAL.value == "equal"
        assert JitterType.DECORRELATED.value == "decorrelated"

    def test_default_jitter_is_full(self):
        """Default jitter should be 'full' (AWS recommended)."""
        policy = RetryPolicy()
        assert policy.jitter == JitterType.FULL

    def test_none_jitter_matches_base_exactly(self):
        policy = RetryPolicy(
            initial_delay_seconds=2.0,
            backoff_multiplier=3.0,
            jitter="none",
        )
        for attempt in range(6):
            assert policy.delay_for_attempt(attempt) == policy.base_delay_for_attempt(attempt)

    def test_full_jitter_within_bounds(self):
        """Full jitter: 0 <= delay <= base_delay."""
        policy = RetryPolicy(
            initial_delay_seconds=10.0,
            backoff_multiplier=2.0,
            max_delay_seconds=1000.0,
            jitter="full",
        )
        for attempt in range(6):
            base = policy.base_delay_for_attempt(attempt)
            delay = policy.delay_for_attempt(attempt)
            assert 0 <= delay <= base, f"Attempt {attempt}: delay {delay} not in [0, {base}]"

    def test_equal_jitter_within_bounds(self):
        """Equal jitter: base/2 <= delay <= base."""
        policy = RetryPolicy(
            initial_delay_seconds=10.0,
            backoff_multiplier=2.0,
            max_delay_seconds=1000.0,
            jitter="equal",
        )
        for attempt in range(6):
            base = policy.base_delay_for_attempt(attempt)
            delay = policy.delay_for_attempt(attempt)
            assert base / 2 <= delay <= base, f"Attempt {attempt}: delay {delay} not in [{base/2}, {base}]"

    def test_decorrelated_jitter_within_bounds(self):
        """Decorrelated jitter: initial <= delay <= min(max, prev * multiplier)."""
        policy = RetryPolicy(
            initial_delay_seconds=5.0,
            backoff_multiplier=2.0,
            max_delay_seconds=500.0,
            jitter="decorrelated",
        )
        prev = None
        for attempt in range(6):
            delay = policy.delay_for_attempt(attempt, prev_delay=prev)
            cap = min(policy.max_delay_seconds, (prev or policy.initial_delay_seconds) * policy.backoff_multiplier)
            assert policy.initial_delay_seconds <= delay <= cap, (
                f"Attempt {attempt}: delay {delay} not in [{policy.initial_delay_seconds}, {cap}]"
            )
            prev = delay

    def test_full_jitter_produces_different_values(self):
        """Full jitter should produce varied values (not all the same)."""
        policy = RetryPolicy(jitter="full", initial_delay_seconds=100.0)
        values = {policy.delay_for_attempt(3) for _ in range(50)}
        assert len(values) > 1, "Jitter should produce varied values"

    def test_none_jitter_produces_same_values(self):
        """No jitter should produce deterministic values."""
        policy = RetryPolicy(jitter="none", initial_delay_seconds=100.0)
        values = {policy.delay_for_attempt(3) for _ in range(50)}
        assert len(values) == 1, "No jitter should produce deterministic values"

    def test_jitter_respects_max_delay(self):
        """All jitter values should never exceed max_delay_seconds."""
        policy = RetryPolicy(
            initial_delay_seconds=100.0,
            backoff_multiplier=10.0,
            max_delay_seconds=50.0,
            jitter="full",
        )
        for attempt in range(5):
            delay = policy.delay_for_attempt(attempt)
            assert delay <= 50.0

    def test_base_delay_for_attempt(self):
        """Base delay follows pure exponential backoff without jitter."""
        policy = RetryPolicy(
            initial_delay_seconds=1.0,
            backoff_multiplier=2.0,
            max_delay_seconds=1000.0,
        )
        assert policy.base_delay_for_attempt(0) == 1.0
        assert policy.base_delay_for_attempt(1) == 2.0
        assert policy.base_delay_for_attempt(2) == 4.0
        assert policy.base_delay_for_attempt(3) == 8.0
        assert policy.base_delay_for_attempt(4) == 16.0

    def test_base_delay_capped(self):
        policy = RetryPolicy(
            initial_delay_seconds=1.0,
            backoff_multiplier=100.0,
            max_delay_seconds=10.0,
        )
        assert policy.base_delay_for_attempt(0) == 1.0
        assert policy.base_delay_for_attempt(1) == 10.0  # capped
        assert policy.base_delay_for_attempt(5) == 10.0  # still capped


# ─── Idempotency Keys ─────────────────────────────────────────────────


class TestIdempotencyKeys:
    """Test idempotency key generation and delivery."""

    def test_delivery_with_idempotency_key(self):
        delivery = WebhookDelivery(
            endpoint_id="ep-1",
            payload={"event": "test"},
            idempotency_key="abc-123-xyz",
        )
        assert delivery.idempotency_key == "abc-123-xyz"

    def test_delivery_without_idempotency_key(self):
        delivery = WebhookDelivery(
            endpoint_id="ep-1",
            payload={"event": "test"},
        )
        assert delivery.idempotency_key is None

    def test_idempotency_key_in_headers(self):
        """Idempotency-Key header should be set when delivery has a key."""
        endpoint = WebhookEndpoint(name="test", url="https://example.com/hook")
        delivery = WebhookDelivery(
            endpoint_id=endpoint.id,
            payload={"msg": "hello"},
            idempotency_key="key-abc-123",
        )
        headers = DeliveryEngine.build_headers(endpoint, delivery)
        assert headers["Idempotency-Key"] == "key-abc-123"

    def test_no_idempotency_header_when_key_absent(self):
        endpoint = WebhookEndpoint(name="test", url="https://example.com/hook")
        delivery = WebhookDelivery(
            endpoint_id=endpoint.id,
            payload={"msg": "hello"},
        )
        headers = DeliveryEngine.build_headers(endpoint, delivery)
        assert "Idempotency-Key" not in headers

    def test_endpoint_idempotency_keys_flag(self):
        """Endpoint with idempotency_keys=True should auto-generate keys."""
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            idempotency_keys=True,
        )
        assert endpoint.idempotency_keys is True

    @pytest.mark.asyncio
    @respx.mock
    async def test_auto_generate_idempotency_on_delivery(self, tmp_path):
        """When endpoint.idempotency_keys=True, delivery should get a UUID key."""
        store = WebhookStore(str(tmp_path / "test.json"))
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            idempotency_keys=True,
        )
        store.add_endpoint(endpoint)

        engine = DeliveryEngine(store)
        delivery = WebhookDelivery(
            endpoint_id=endpoint.id,
            payload={"event": "test"},
        )
        store.add_delivery(delivery)

        route = respx.post("https://example.com/hook").mock(
            return_value=__import__("httpx").Response(200, json={"ok": True})
        )

        await engine.deliver(delivery)

        # The idempotency key should have been auto-generated
        updated = store.get_delivery(delivery.id)
        assert updated.idempotency_key is not None
        assert len(updated.idempotency_key) == 36  # UUID length
        await engine.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_explicit_idempotency_key_preserved(self, tmp_path):
        """Explicitly-set idempotency key should not be overwritten."""
        store = WebhookStore(str(tmp_path / "test.json"))
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            idempotency_keys=True,
        )
        store.add_endpoint(endpoint)

        engine = DeliveryEngine(store)
        delivery = WebhookDelivery(
            endpoint_id=endpoint.id,
            payload={"event": "test"},
            idempotency_key="my-custom-key",
        )
        store.add_delivery(delivery)

        respx.post("https://example.com/hook").mock(
            return_value=__import__("httpx").Response(200, json={"ok": True})
        )

        await engine.deliver(delivery)
        updated = store.get_delivery(delivery.id)
        assert updated.idempotency_key == "my-custom-key"
        await engine.close()

    def test_idempotency_key_in_service_create_delivery(self, tmp_path):
        """create_delivery should accept and store idempotency_key."""
        svc = WebhookService(store=WebhookStore(str(tmp_path / "test.json")))
        ep = svc.create_endpoint(name="test", url="https://example.com/hook")

        delivery = svc.create_delivery(
            endpoint_id=ep.id,
            payload={"event": "test"},
            idempotency_key="idem-123",
        )
        assert delivery is not None
        assert delivery.idempotency_key == "idem-123"

    @pytest.mark.asyncio
    @respx.mock
    async def test_idempotency_key_sent_in_header(self, tmp_path):
        """The Idempotency-Key header should be sent in the actual HTTP request."""
        store = WebhookStore(str(tmp_path / "test.json"))
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
        )
        store.add_endpoint(endpoint)

        engine = DeliveryEngine(store)
        delivery = WebhookDelivery(
            endpoint_id=endpoint.id,
            payload={"event": "test"},
            idempotency_key="header-test-key",
        )
        store.add_delivery(delivery)

        route = respx.post("https://example.com/hook").mock(
            return_value=__import__("httpx").Response(200, json={"ok": True})
        )

        await engine.deliver(delivery)
        sent_headers = route.calls.last.request.headers
        assert sent_headers.get("Idempotency-Key") == "header-test-key"
        await engine.close()


# ─── Timestamped Signatures ───────────────────────────────────────────


class TestTimestampedSignatures:
    """Test timestamped HMAC signatures for replay protection."""

    def test_generate_timestamped_signature_format(self):
        """Signature should be in 't=<ts>,v1=<hex>' format."""
        sig, ts = DeliveryEngine.generate_timestamped_signature(
            secret="mysecret",
            payload='{"event":"test"}',
        )
        assert sig.startswith("t=")
        assert ",v1=" in sig
        assert ts > 0

    def test_generate_timestamped_signature_sha256(self):
        """Verify the SHA256 HMAC is correct for the signed message."""
        secret = "test-secret"
        payload = '{"event":"payment.created"}'
        sig, ts = DeliveryEngine.generate_timestamped_signature(secret, payload, algorithm="sha256")

        # Recompute manually
        signed_message = f"{ts}.{payload}"
        expected = hmac.new(secret.encode(), signed_message.encode(), hashlib.sha256).hexdigest()
        assert f"v1={expected}" in sig

    def test_generate_timestamped_signature_sha512(self):
        secret = "test-secret"
        payload = '{"event":"test"}'
        sig, ts = DeliveryEngine.generate_timestamped_signature(secret, payload, algorithm="sha512")

        signed_message = f"{ts}.{payload}"
        expected = hmac.new(secret.encode(), signed_message.encode(), hashlib.sha512).hexdigest()
        assert f"v1={expected}" in sig

    def test_verify_valid_timestamped_signature(self):
        """A freshly generated signature should verify successfully."""
        secret = "verify-secret"
        payload = '{"event":"order.created","amount":100}'

        sig, ts = DeliveryEngine.generate_timestamped_signature(secret, payload)

        valid = DeliveryEngine.verify_timestamped_signature(
            secret=secret,
            payload=payload,
            signature_header=sig,
            tolerance_seconds=300,
        )
        assert valid is True

    def test_verify_timestamped_wrong_secret(self):
        """A signature with the wrong secret should fail verification."""
        sig, ts = DeliveryEngine.generate_timestamped_signature(
            "correct-secret", '{"event":"test"}'
        )
        valid = DeliveryEngine.verify_timestamped_signature(
            secret="wrong-secret",
            payload='{"event":"test"}',
            signature_header=sig,
        )
        assert valid is False

    def test_verify_timestamped_wrong_payload(self):
        """A signature with modified payload should fail verification."""
        sig, ts = DeliveryEngine.generate_timestamped_signature(
            "secret", '{"event":"original"}'
        )
        valid = DeliveryEngine.verify_timestamped_signature(
            secret="secret",
            payload='{"event":"tampered"}',
            signature_header=sig,
        )
        assert valid is False

    def test_verify_timestamped_expired(self):
        """A signature older than tolerance should fail (replay protection)."""
        secret = "replay-secret"
        payload = '{"event":"old"}'

        # Manually craft a signature with an old timestamp
        old_ts = int(time.time()) - 600  # 10 minutes ago
        signed_message = f"{old_ts}.{payload}"
        digest = hmac.new(secret.encode(), signed_message.encode(), hashlib.sha256).hexdigest()
        sig = f"t={old_ts},v1={digest}"

        valid = DeliveryEngine.verify_timestamped_signature(
            secret=secret,
            payload=payload,
            signature_header=sig,
            tolerance_seconds=300,
        )
        assert valid is False

    def test_verify_timestamped_malformed_header(self):
        """Malformed signature header should fail gracefully."""
        valid = DeliveryEngine.verify_timestamped_signature(
            secret="secret",
            payload='{"event":"test"}',
            signature_header="garbage",
        )
        assert valid is False

    def test_verify_timestamped_missing_fields(self):
        """Signature header missing t or v1 should fail."""
        valid = DeliveryEngine.verify_timestamped_signature(
            secret="secret",
            payload='{"event":"test"}',
            signature_header="t=12345,missing_v1",
        )
        assert valid is False

    def test_endpoint_timestamped_signatures_flag(self):
        """Endpoint with timestamped_signatures=True."""
        ep = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            secret="my-secret",
            timestamped_signatures=True,
        )
        assert ep.timestamped_signatures is True

    @pytest.mark.asyncio
    @respx.mock
    async def test_timestamped_signature_sent_on_delivery(self, tmp_path):
        """When timestamped_signatures=True, the signature header should use t=,v1= format."""
        store = WebhookStore(str(tmp_path / "test.json"))
        secret = "ts-signature-secret"
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            secret=secret,
            timestamped_signatures=True,
        )
        store.add_endpoint(endpoint)

        engine = DeliveryEngine(store)
        delivery = WebhookDelivery(
            endpoint_id=endpoint.id,
            payload={"event": "signed"},
        )
        store.add_delivery(delivery)

        route = respx.post("https://example.com/hook").mock(
            return_value=__import__("httpx").Response(200, json={"ok": True})
        )

        await engine.deliver(delivery)

        sig_header = route.calls.last.request.headers.get("X-Webhook-Signature")
        assert sig_header is not None
        assert sig_header.startswith("t=")
        assert ",v1=" in sig_header
        await engine.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_plain_signature_when_timestamped_disabled(self, tmp_path):
        """When timestamped_signatures=False, signature should use plain algorithm=hex format."""
        store = WebhookStore(str(tmp_path / "test.json"))
        secret = "plain-secret"
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            secret=secret,
            timestamped_signatures=False,
        )
        store.add_endpoint(endpoint)

        engine = DeliveryEngine(store)
        delivery = WebhookDelivery(
            endpoint_id=endpoint.id,
            payload={"event": "signed"},
        )
        store.add_delivery(delivery)

        route = respx.post("https://example.com/hook").mock(
            return_value=__import__("httpx").Response(200, json={"ok": True})
        )

        await engine.deliver(delivery)

        sig_header = route.calls.last.request.headers.get("X-Webhook-Signature")
        assert sig_header is not None
        assert sig_header.startswith("sha256=")  # plain format
        assert "t=" not in sig_header
        await engine.close()

    def test_service_verify_outbound_signature(self, tmp_path):
        """Service layer should provide signature verification helper."""
        svc = WebhookService(store=WebhookStore(str(tmp_path / "test.json")))
        secret = "service-verify-secret"
        payload = '{"event":"test"}'

        sig, ts = DeliveryEngine.generate_timestamped_signature(secret, payload)
        result = svc.verify_outbound_signature(
            secret=secret,
            payload=payload,
            signature_header=sig,
        )
        assert result["valid"] is True

    def test_service_verify_outbound_signature_invalid(self, tmp_path):
        svc = WebhookService(store=WebhookStore(str(tmp_path / "test.json")))
        result = svc.verify_outbound_signature(
            secret="wrong",
            payload='{"event":"test"}',
            signature_header="t=123,v1=abc",
        )
        assert result["valid"] is False
        assert "error" in result


# ─── Jitter in Delivery Retry Flow ────────────────────────────────────


class TestJitterInRetryFlow:
    """Test that jitter is applied to retry delays in the delivery engine."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_retry_delay_with_full_jitter(self, tmp_path):
        """Retry delay should use jitter when set on the endpoint's retry policy."""
        store = WebhookStore(str(tmp_path / "test.json"))
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            retry_policy=RetryPolicy(
                max_retries=3,
                initial_delay_seconds=10.0,
                backoff_multiplier=2.0,
                jitter="full",
            ),
        )
        store.add_endpoint(endpoint)

        engine = DeliveryEngine(store)
        delivery = WebhookDelivery(
            endpoint_id=endpoint.id,
            payload={"event": "test"},
        )
        store.add_delivery(delivery)

        # Fail with 500 to trigger retry
        respx.post("https://example.com/hook").mock(
            return_value=__import__("httpx").Response(500, text="Server Error")
        )

        result = await engine.process_delivery(delivery.id)
        assert result.status == DeliveryStatus.RETRYING
        assert result.next_retry_at is not None

        # The delay should be <= base (full jitter)
        now = datetime.now(timezone.utc)
        delay = (result.next_retry_at - now).total_seconds()
        assert 0 <= delay <= 10.0 + 1.0  # Allow 1s slack for execution time
        await engine.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_retry_delay_with_none_jitter(self, tmp_path):
        """Retry delay with jitter=none should match base delay exactly."""
        store = WebhookStore(str(tmp_path / "test.json"))
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            retry_policy=RetryPolicy(
                max_retries=3,
                initial_delay_seconds=5.0,
                backoff_multiplier=2.0,
                jitter="none",
            ),
        )
        store.add_endpoint(endpoint)

        engine = DeliveryEngine(store)
        delivery = WebhookDelivery(
            endpoint_id=endpoint.id,
            payload={"event": "test"},
        )
        store.add_delivery(delivery)

        respx.post("https://example.com/hook").mock(
            return_value=__import__("httpx").Response(500, text="Server Error")
        )

        result = await engine.process_delivery(delivery.id)
        assert result.status == DeliveryStatus.RETRYING

        now = datetime.now(timezone.utc)
        delay = (result.next_retry_at - now).total_seconds()
        assert 4.0 <= delay <= 5.0 + 1.0  # ~5s with slack
        await engine.close()


# ─── Endpoint Config Integration ──────────────────────────────────────


class TestEndpointConfigIntegration:
    """Test endpoint creation with v0.8.0 fields via service layer."""

    def test_create_endpoint_with_jitter(self, tmp_path):
        svc = WebhookService(store=WebhookStore(str(tmp_path / "test.json")))
        ep = svc.create_endpoint(
            name="test",
            url="https://example.com/hook",
            jitter="decorrelated",
        )
        assert ep.retry_policy.jitter.value == "decorrelated"

    def test_create_endpoint_with_timestamped_signatures(self, tmp_path):
        svc = WebhookService(store=WebhookStore(str(tmp_path / "test.json")))
        ep = svc.create_endpoint(
            name="test",
            url="https://example.com/hook",
            secret="my-secret",
            timestamped_signatures=True,
        )
        assert ep.timestamped_signatures is True

    def test_create_endpoint_with_idempotency_keys(self, tmp_path):
        svc = WebhookService(store=WebhookStore(str(tmp_path / "test.json")))
        ep = svc.create_endpoint(
            name="test",
            url="https://example.com/hook",
            idempotency_keys=True,
        )
        assert ep.idempotency_keys is True

    def test_create_endpoint_all_features(self, tmp_path):
        """Create endpoint with all three v0.8.0 features enabled."""
        svc = WebhookService(store=WebhookStore(str(tmp_path / "test.json")))
        ep = svc.create_endpoint(
            name="production",
            url="https://api.example.com/webhooks",
            secret="prod-secret",
            jitter="equal",
            timestamped_signatures=True,
            idempotency_keys=True,
        )
        assert ep.retry_policy.jitter.value == "equal"
        assert ep.timestamped_signatures is True
        assert ep.idempotency_keys is True

    def test_simulate_delivery_shows_new_features(self, tmp_path):
        """Simulate should show timestamped_signatures and idempotency_keys status."""
        svc = WebhookService(store=WebhookStore(str(tmp_path / "test.json")))
        ep = svc.create_endpoint(
            name="test",
            url="https://example.com/hook",
            secret="secret",
            timestamped_signatures=True,
            idempotency_keys=True,
        )
        result = svc.simulate_delivery(
            endpoint_id=ep.id,
            payload={"event": "test"},
        )
        assert result["timestamped_signatures"] is True
        assert result["idempotency_keys_enabled"] is True

    def test_simulate_delivery_with_timestamped_shows_t_format(self, tmp_path):
        """Simulate should show t=,v1= format signature when timestamped_signatures=True."""
        svc = WebhookService(store=WebhookStore(str(tmp_path / "test.json")))
        ep = svc.create_endpoint(
            name="test",
            url="https://example.com/hook",
            secret="sim-secret",
            timestamped_signatures=True,
        )
        result = svc.simulate_delivery(
            endpoint_id=ep.id,
            payload={"event": "test"},
        )
        assert result["signature_present"] is True
        sig = result["headers"].get("X-Webhook-Signature", "")
        assert "t=" in sig
        assert "v1=" in sig


# ─── Model Serialization ──────────────────────────────────────────────


class TestModelSerialization:
    """Test that new fields serialize/deserialize correctly."""

    def test_retry_policy_with_jitter_serialization(self):
        policy = RetryPolicy(jitter="decorrelated")
        data = policy.model_dump(mode="json")
        assert data["jitter"] == "decorrelated"

        restored = RetryPolicy.model_validate(data)
        assert restored.jitter == JitterType.DECORRELATED

    def test_endpoint_with_new_fields_serialization(self):
        ep = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            secret="secret",
            timestamped_signatures=True,
            idempotency_keys=True,
        )
        data = ep.model_dump(mode="json")
        assert data["timestamped_signatures"] is True
        assert data["idempotency_keys"] is True

        restored = WebhookEndpoint.model_validate(data)
        assert restored.timestamped_signatures is True
        assert restored.idempotency_keys is True

    def test_delivery_with_idempotency_key_serialization(self):
        delivery = WebhookDelivery(
            endpoint_id="ep-1",
            payload={"event": "test"},
            idempotency_key="serialize-test-key",
        )
        data = delivery.model_dump(mode="json")
        assert data["idempotency_key"] == "serialize-test-key"

        restored = WebhookDelivery.model_validate(data)
        assert restored.idempotency_key == "serialize-test-key"

    def test_jitter_type_coercion_from_string(self):
        """Pydantic should coerce string values to JitterType enum."""
        policy = RetryPolicy(jitter="full")
        assert policy.jitter == JitterType.FULL

        policy2 = RetryPolicy(jitter="none")
        assert policy2.jitter == JitterType.NONE

        policy3 = RetryPolicy(jitter="equal")
        assert policy3.jitter == JitterType.EQUAL


# ─── End-to-End Integration ───────────────────────────────────────────


class TestEndToEndIntegration:
    """End-to-end tests combining all three features."""

    @pytest.mark.asyncio
    @respx.mock
    async def test_all_features_combined(self, tmp_path):
        """Endpoint with jitter + idempotency + timestamped signatures."""
        store = WebhookStore(str(tmp_path / "test.json"))
        endpoint = WebhookEndpoint(
            name="production",
            url="https://api.example.com/webhook",
            secret="e2e-secret",
            timestamped_signatures=True,
            idempotency_keys=True,
            retry_policy=RetryPolicy(
                max_retries=3,
                initial_delay_seconds=2.0,
                backoff_multiplier=2.0,
                jitter="full",
            ),
        )
        store.add_endpoint(endpoint)

        engine = DeliveryEngine(store)
        delivery = WebhookDelivery(
            endpoint_id=endpoint.id,
            payload={"event": "payment.completed", "amount": 4200},
        )
        store.add_delivery(delivery)

        route = respx.post("https://api.example.com/webhook").mock(
            return_value=__import__("httpx").Response(200, json={"received": True})
        )

        result = await engine.process_delivery(delivery.id)

        # Check delivery was successful
        assert result.status == DeliveryStatus.SUCCESS

        # Check headers
        sent_headers = route.calls.last.request.headers
        sig = sent_headers.get("X-Webhook-Signature", "")
        idem_key = sent_headers.get("Idempotency-Key")

        # Timestamped signature format
        assert "t=" in sig
        assert "v1=" in sig

        # Idempotency key was auto-generated
        assert idem_key is not None
        assert len(idem_key) == 36  # UUID

        # Verify the timestamped signature is valid
        payload_str = json.dumps(delivery.payload, default=str)
        valid = DeliveryEngine.verify_timestamped_signature(
            secret="e2e-secret",
            payload=payload_str,
            signature_header=sig,
        )
        assert valid is True
        await engine.close()

    @pytest.mark.asyncio
    @respx.mock
    async def test_retry_uses_jittered_delay(self, tmp_path):
        """Verify retry delay varies across multiple identical deliveries (jitter effect)."""
        store = WebhookStore(str(tmp_path / "test.json"))
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            retry_policy=RetryPolicy(
                max_retries=3,
                initial_delay_seconds=100.0,
                backoff_multiplier=2.0,
                jitter="full",
            ),
        )
        store.add_endpoint(endpoint)

        engine = DeliveryEngine(store)

        # Create multiple deliveries that will fail
        delays = set()
        for i in range(10):
            delivery = WebhookDelivery(
                endpoint_id=endpoint.id,
                payload={"index": i},
            )
            store.add_delivery(delivery)

            respx.post("https://example.com/hook").mock(
                return_value=__import__("httpx").Response(500, text="Error")
            )
            result = await engine.process_delivery(delivery.id)
            assert result.status == DeliveryStatus.RETRYING

            now = datetime.now(timezone.utc)
            delay = round((result.next_retry_at - now).total_seconds(), 1)
            delays.add(delay)

        # With full jitter on 100s initial delay, we should get varied delays
        assert len(delays) > 3, f"Expected varied delays due to jitter, got {len(delays)} unique values"
        await engine.close()
