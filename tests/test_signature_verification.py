"""Tests for incoming webhook signature verification (v0.5.0)."""

import hashlib
import hmac
import time
import json
import pytest

from agent_webhook.signature import SignatureVerifier, SignatureError


# ── Generic HMAC Verification ─────────────────────────────────────


class TestGenericSignatureVerification:
    def test_verify_generic_sha256_valid(self):
        secret = "my_secret"
        body = b'{"event": "test", "data": {"id": 123}}'
        expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier()
        result = verifier.verify(body, {"X-Webhook-Signature": expected}, secret, provider="generic")
        assert result is True

    def test_verify_generic_sha1_valid(self):
        secret = "my_secret"
        body = b'{"event": "test"}'
        expected = "sha1=" + hmac.new(secret.encode(), body, hashlib.sha1).hexdigest()

        verifier = SignatureVerifier()
        result = verifier.verify(body, {"X-Webhook-Signature": expected}, secret, provider="generic", algorithm="sha1")
        assert result is True

    def test_verify_generic_wrong_secret(self):
        secret = "correct_secret"
        body = b'{"event": "test"}'
        wrong_sig = "sha256=" + hmac.new("wrong_secret".encode(), body, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier()
        with pytest.raises(SignatureError, match="Signature"):
            # verify returns False but we use verify_or_raise for the error
            verifier.verify_or_raise(body, {"X-Webhook-Signature": wrong_sig}, secret)

    def test_verify_generic_missing_signature_header(self):
        verifier = SignatureVerifier()
        with pytest.raises(SignatureError, match="No signature header"):
            verifier.verify(b'{"test": 1}', {}, "secret")

    def test_verify_generic_x_signature_header(self):
        secret = "my_secret"
        body = b'{"event": "test"}'
        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier()
        result = verifier.verify(body, {"X-Signature": expected}, secret, provider="generic")
        assert result is True

    def test_verify_generic_tampered_body(self):
        secret = "my_secret"
        body = b'{"event": "test"}'
        tampered = b'{"event": "HACKED"}'
        expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier()
        with pytest.raises(SignatureError):
            verifier.verify_or_raise(tampered, {"X-Webhook-Signature": expected}, secret)

    def test_verify_or_raise_success(self):
        secret = "s"
        body = b"test"
        sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier()
        verifier.verify_or_raise(body, {"X-Webhook-Signature": sig}, secret)
        # Should not raise

    def test_case_insensitive_headers(self):
        secret = "my_secret"
        body = b'{"event": "test"}'
        expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier()
        result = verifier.verify(body, {"x-webhook-signature": expected}, secret)
        assert result is True


# ── GitHub Verification ───────────────────────────────────────────


class TestGitHubSignatureVerification:
    def test_verify_github_sha256(self):
        secret = "gh_secret"
        body = b'{"action": "opened", "number": 1}'
        expected = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier()
        result = verifier.verify(body, {"X-Hub-Signature-256": expected}, secret, provider="github")
        assert result is True

    def test_verify_github_sha1_legacy(self):
        secret = "gh_secret"
        body = b'{"action": "opened"}'
        expected = "sha1=" + hmac.new(secret.encode(), body, hashlib.sha1).hexdigest()

        verifier = SignatureVerifier()
        result = verifier.verify(body, {"X-Hub-Signature": expected}, secret, provider="github")
        assert result is True

    def test_verify_github_wrong_secret(self):
        secret = "correct"
        body = b'{"action": "opened"}'
        wrong_sig = "sha256=" + hmac.new("wrong".encode(), body, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier()
        with pytest.raises(SignatureError):
            verifier.verify_or_raise(body, {"X-Hub-Signature-256": wrong_sig}, secret, provider="github")

    def test_verify_github_missing_header(self):
        verifier = SignatureVerifier()
        with pytest.raises(SignatureError, match="No GitHub signature"):
            verifier.verify_or_raise(b'{"test": 1}', {}, "secret", provider="github")


# ── Stripe Verification ───────────────────────────────────────────


class TestStripeSignatureVerification:
    def test_verify_stripe_valid(self):
        secret = "stripe_secret"
        body = b'{"type": "payment_intent.succeeded"}'
        timestamp = int(time.time())

        signed_payload = f"{timestamp}.".encode() + body
        sig = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()

        header = f"t={timestamp},v1={sig}"

        verifier = SignatureVerifier()
        result = verifier.verify(body, {"Stripe-Signature": header}, secret, provider="stripe")
        assert result is True

    def test_verify_stripe_multiple_v1_signatures(self):
        """Stripe can send multiple v1 signatures — any match is valid."""
        secret = "stripe_secret"
        body = b'{"type": "charge.refunded"}'
        timestamp = int(time.time())

        signed_payload = f"{timestamp}.".encode() + body
        sig1 = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()
        sig2 = "deadbeef" * 8  # invalid signature

        header = f"t={timestamp},v1={sig2},v1={sig1}"

        verifier = SignatureVerifier()
        result = verifier.verify(body, {"Stripe-Signature": header}, secret, provider="stripe")
        assert result is True

    def test_verify_stripe_expired_timestamp(self):
        secret = "stripe_secret"
        body = b'{"type": "test"}'
        old_timestamp = int(time.time()) - 600  # 10 minutes ago

        signed_payload = f"{old_timestamp}.".encode() + body
        sig = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()
        header = f"t={old_timestamp},v1={sig}"

        verifier = SignatureVerifier(tolerance_seconds=300)
        with pytest.raises(SignatureError, match="tolerance"):
            verifier.verify_or_raise(body, {"Stripe-Signature": header}, secret, provider="stripe")

    def test_verify_stripe_missing_header(self):
        verifier = SignatureVerifier()
        with pytest.raises(SignatureError, match="No Stripe"):
            verifier.verify_or_raise(b'{"test": 1}', {}, "secret", provider="stripe")


# ── Slack Verification ────────────────────────────────────────────


class TestSlackSignatureVerification:
    def test_verify_slack_valid(self):
        secret = "slack_secret"
        body = b'{"type": "event_callback", "event": {"type": "message"}}'
        timestamp = int(time.time())

        base_string = f"v0:{timestamp}:".encode() + body
        sig = "v0=" + hmac.new(secret.encode(), base_string, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier()
        result = verifier.verify(
            body,
            {"X-Slack-Signature": sig, "X-Slack-Request-Timestamp": str(timestamp)},
            secret,
            provider="slack",
        )
        assert result is True

    def test_verify_slack_expired_timestamp(self):
        secret = "slack_secret"
        body = b'{"type": "event_callback"}'
        old_timestamp = int(time.time()) - 600

        base_string = f"v0:{old_timestamp}:".encode() + body
        sig = "v0=" + hmac.new(secret.encode(), base_string, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier(tolerance_seconds=300)
        with pytest.raises(SignatureError, match="tolerance"):
            verifier.verify_or_raise(
                body,
                {"X-Slack-Signature": sig, "X-Slack-Request-Timestamp": str(old_timestamp)},
                secret,
                provider="slack",
            )

    def test_verify_slack_missing_timestamp(self):
        secret = "slack_secret"
        body = b'{"type": "event_callback"}'
        sig = "v0=abc123"

        verifier = SignatureVerifier()
        with pytest.raises(SignatureError, match="No X-Slack-Request-Timestamp"):
            verifier.verify_or_raise(
                body,
                {"X-Slack-Signature": sig},
                secret,
                provider="slack",
            )


# ── Shopify Verification ──────────────────────────────────────────


class TestShopifySignatureVerification:
    def test_verify_shopify_hex_valid(self):
        secret = "shopify_secret"
        body = b'{"id": 12345, "total_price": "59.99"}'
        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier()
        result = verifier.verify(body, {"X-Shopify-Hmac-SHA256": expected}, secret, provider="shopify")
        assert result is True

    def test_verify_shopify_base64_valid(self):
        import base64
        secret = "shopify_secret"
        body = b'{"id": 12345}'
        expected = base64.b64encode(
            hmac.new(secret.encode(), body, hashlib.sha256).digest()
        ).decode()

        verifier = SignatureVerifier()
        result = verifier.verify(body, {"X-Shopify-Hmac-SHA256": expected}, secret, provider="shopify")
        assert result is True

    def test_verify_shopify_wrong_secret(self):
        secret = "correct"
        body = b'{"id": 12345}'
        wrong_sig = hmac.new("wrong".encode(), body, hashlib.sha256).hexdigest()

        verifier = SignatureVerifier()
        with pytest.raises(SignatureError, match="Shopify signature mismatch"):
            verifier.verify_or_raise(body, {"X-Shopify-Hmac-SHA256": wrong_sig}, secret, provider="shopify")


# ── Provider Detection ────────────────────────────────────────────


class TestProviderDetection:
    def test_detect_github(self):
        headers = {"X-Hub-Signature-256": "sha256=abc123"}
        verifier = SignatureVerifier()
        assert verifier.detect_provider(headers) == "github"

    def test_detect_github_legacy(self):
        headers = {"X-Hub-Signature": "sha1=abc123"}
        verifier = SignatureVerifier()
        assert verifier.detect_provider(headers) == "github"

    def test_detect_stripe(self):
        headers = {"Stripe-Signature": "t=123,v1=abc"}
        verifier = SignatureVerifier()
        assert verifier.detect_provider(headers) == "stripe"

    def test_detect_slack(self):
        headers = {"X-Slack-Signature": "v0=abc123"}
        verifier = SignatureVerifier()
        assert verifier.detect_provider(headers) == "slack"

    def test_detect_shopify(self):
        headers = {"X-Shopify-Hmac-SHA256": "abc123"}
        verifier = SignatureVerifier()
        assert verifier.detect_provider(headers) == "shopify"

    def test_detect_unknown(self):
        headers = {"Content-Type": "application/json"}
        verifier = SignatureVerifier()
        assert verifier.detect_provider(headers) is None


# ── Signature Generation ──────────────────────────────────────────


class TestSignatureGeneration:
    def test_generate_generic_roundtrip(self):
        secret = "roundtrip_secret"
        body = b'{"test": true}'
        verifier = SignatureVerifier()

        sig = verifier.generate_signature(body, secret, provider="generic")
        assert verifier.verify(body, {"X-Webhook-Signature": sig}, secret) is True

    def test_generate_github_roundtrip(self):
        secret = "gh_secret"
        body = b'{"action": "opened"}'
        verifier = SignatureVerifier()

        sig = verifier.generate_signature(body, secret, provider="github")
        assert verifier.verify(body, {"X-Hub-Signature-256": sig}, secret, provider="github") is True

    def test_generate_stripe_roundtrip(self):
        secret = "stripe_secret"
        body = b'{"type": "payment.succeeded"}'
        verifier = SignatureVerifier()

        sig = verifier.generate_signature(body, secret, provider="stripe")
        assert verifier.verify(body, {"Stripe-Signature": sig}, secret, provider="stripe") is True

    def test_generate_slack_roundtrip(self):
        secret = "slack_secret"
        body = b'{"type": "event"}'
        verifier = SignatureVerifier()

        sig = verifier.generate_signature(body, secret, provider="slack")
        ts = sig.split(",")[0].split("=")[1] if "," in sig else str(int(time.time()))
        # generate_signature for slack returns "v0=..." but we need the timestamp too
        # Actually generate_signature returns just the sig, not the timestamp header
        # Let's compute it manually
        timestamp = int(time.time())
        sig = verifier.generate_signature(body, secret, provider="slack", timestamp=timestamp)
        assert verifier.verify(
            body,
            {"X-Slack-Signature": sig, "X-Slack-Request-Timestamp": str(timestamp)},
            secret,
            provider="slack",
        ) is True

    def test_generate_shopify_roundtrip(self):
        secret = "shopify_secret"
        body = b'{"id": 12345}'
        verifier = SignatureVerifier()

        sig = verifier.generate_signature(body, secret, provider="shopify")
        assert verifier.verify(body, {"X-Shopify-Hmac-SHA256": sig}, secret, provider="shopify") is True

    def test_generate_sha1(self):
        secret = "s"
        body = b"test"
        verifier = SignatureVerifier()
        sig = verifier.generate_signature(body, secret, algorithm="sha1", provider="generic")
        assert sig.startswith("sha1=")
        assert verifier.verify(body, {"X-Webhook-Signature": sig}, secret, algorithm="sha1") is True


# ── Relay Rule Signature Verification ─────────────────────────────


class TestRelayRuleSignatureVerification:
    def test_relay_rule_has_verification_fields(self):
        from agent_webhook.models import RelayRule
        rule = RelayRule(
            name="test",
            path_pattern="/stripe/*",
            target_endpoint_ids=["ep1"],
        )
        assert rule.verify_signature is False
        assert rule.verify_secret is None
        assert rule.verify_provider == "generic"
        assert rule.verify_algorithm == "sha256"
        assert rule.verify_tolerance_seconds == 300

    def test_relay_rule_with_verification_config(self):
        from agent_webhook.models import RelayRule
        rule = RelayRule(
            name="stripe-relay",
            path_pattern="/stripe/*",
            target_endpoint_ids=["ep1"],
            verify_signature=True,
            verify_secret="my_secret",
            verify_provider="stripe",
            verify_tolerance_seconds=120,
        )
        assert rule.verify_signature is True
        assert rule.verify_secret == "my_secret"
        assert rule.verify_provider == "stripe"
        assert rule.verify_tolerance_seconds == 120


class TestRelayVerificationIntegration:
    def test_verified_relay_forwards_valid_signature(self):
        """A relay rule with verify_signature should forward webhooks with valid signatures."""
        from agent_webhook.store import WebhookStore
        from agent_webhook.engine import DeliveryEngine
        from agent_webhook.models import WebhookEndpoint, RelayRule, WebhookMethod, RetryPolicy

        store = WebhookStore("test_relay_verify_valid.json")
        ep = WebhookEndpoint(name="test", url="https://example.com/hook", retry_policy=RetryPolicy(max_retries=0))
        store.add_endpoint(ep)

        secret = "relay_secret"
        body = b'{"event": "test"}'
        sig = "sha256=" + hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        rule = RelayRule(
            name="verified",
            path_pattern="/api/*",
            target_endpoint_ids=[ep.id],
            verify_signature=True,
            verify_secret=secret,
        )
        store.add_relay_rule(rule)

        engine = DeliveryEngine(store)
        delivery_ids = engine.apply_relay_rules(
            path="/api/test",
            method="POST",
            headers={"X-Webhook-Signature": sig},
            body={"event": "test"},
            raw_body=body,
        )

        assert len(delivery_ids) == 1

        import os
        os.remove("test_relay_verify_valid.json")

    def test_verified_relay_blocks_invalid_signature(self):
        """A relay rule with verify_signature should NOT forward webhooks with invalid signatures."""
        from agent_webhook.store import WebhookStore
        from agent_webhook.engine import DeliveryEngine
        from agent_webhook.models import WebhookEndpoint, RelayRule, WebhookMethod, RetryPolicy

        store = WebhookStore("test_relay_verify_invalid.json")
        ep = WebhookEndpoint(name="test", url="https://example.com/hook", retry_policy=RetryPolicy(max_retries=0))
        store.add_endpoint(ep)

        rule = RelayRule(
            name="verified",
            path_pattern="/api/*",
            target_endpoint_ids=[ep.id],
            verify_signature=True,
            verify_secret="correct_secret",
        )
        store.add_relay_rule(rule)

        engine = DeliveryEngine(store)
        delivery_ids = engine.apply_relay_rules(
            path="/api/test",
            method="POST",
            headers={"X-Webhook-Signature": "sha256=invalid_hex_garbage"},
            body={"event": "test"},
            raw_body=b'{"event": "test"}',
        )

        # No deliveries should be created (signature verification failed)
        assert len(delivery_ids) == 0

        import os
        os.remove("test_relay_verify_invalid.json")

    def test_unverified_relay_forwards_without_check(self):
        """A relay rule without verify_signature should forward all webhooks."""
        from agent_webhook.store import WebhookStore
        from agent_webhook.engine import DeliveryEngine
        from agent_webhook.models import WebhookEndpoint, RelayRule, RetryPolicy

        store = WebhookStore("test_relay_unverified.json")
        ep = WebhookEndpoint(name="test", url="https://example.com/hook", retry_policy=RetryPolicy(max_retries=0))
        store.add_endpoint(ep)

        rule = RelayRule(
            name="unverified",
            path_pattern="/api/*",
            target_endpoint_ids=[ep.id],
        )
        store.add_relay_rule(rule)

        engine = DeliveryEngine(store)
        delivery_ids = engine.apply_relay_rules(
            path="/api/test",
            method="POST",
            headers={},
            body={"event": "test"},
        )

        assert len(delivery_ids) == 1

        import os
        os.remove("test_relay_unverified.json")

    def test_verified_relay_stripe_provider(self):
        """Test Stripe-specific relay verification."""
        from agent_webhook.store import WebhookStore
        from agent_webhook.engine import DeliveryEngine
        from agent_webhook.models import WebhookEndpoint, RelayRule, RetryPolicy

        store = WebhookStore("test_relay_stripe.json")
        ep = WebhookEndpoint(name="test", url="https://example.com/hook", retry_policy=RetryPolicy(max_retries=0))
        store.add_endpoint(ep)

        secret = "stripe_secret"
        body = b'{"type": "payment_intent.succeeded"}'
        timestamp = int(time.time())
        signed_payload = f"{timestamp}.".encode() + body
        sig = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()
        header = f"t={timestamp},v1={sig}"

        rule = RelayRule(
            name="stripe-relay",
            path_pattern="/stripe/*",
            target_endpoint_ids=[ep.id],
            verify_signature=True,
            verify_secret=secret,
            verify_provider="stripe",
        )
        store.add_relay_rule(rule)

        engine = DeliveryEngine(store)
        delivery_ids = engine.apply_relay_rules(
            path="/stripe/webhook",
            method="POST",
            headers={"Stripe-Signature": header},
            body={"type": "payment_intent.succeeded"},
            raw_body=body,
        )

        assert len(delivery_ids) == 1

        import os
        os.remove("test_relay_stripe.json")
