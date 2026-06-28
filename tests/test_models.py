"""Tests for agent-webhook models."""

import pytest
from datetime import datetime, timezone

from agent_webhook.models import (
    DeliveryAttempt,
    DeliveryStatus,
    Header,
    IncomingWebhook,
    RelayRule,
    RetryPolicy,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookMethod,
    WebhookStatus,
)


class TestRetryPolicy:
    def test_defaults(self):
        policy = RetryPolicy()
        assert policy.max_retries == 3
        assert policy.initial_delay_seconds == 1.0
        assert policy.max_delay_seconds == 300.0
        assert policy.backoff_multiplier == 2.0

    def test_delay_for_attempt(self):
        policy = RetryPolicy(initial_delay_seconds=1.0, backoff_multiplier=2.0)
        assert policy.delay_for_attempt(0) == 1.0
        assert policy.delay_for_attempt(1) == 2.0
        assert policy.delay_for_attempt(2) == 4.0
        assert policy.delay_for_attempt(3) == 8.0

    def test_delay_capped_at_max(self):
        policy = RetryPolicy(initial_delay_seconds=1.0, max_delay_seconds=5.0, backoff_multiplier=10.0)
        assert policy.delay_for_attempt(0) == 1.0
        assert policy.delay_for_attempt(1) == 5.0  # capped
        assert policy.delay_for_attempt(5) == 5.0  # still capped

    def test_validation(self):
        with pytest.raises(Exception):
            RetryPolicy(max_retries=-1)
        with pytest.raises(Exception):
            RetryPolicy(initial_delay_seconds=0.0)
        with pytest.raises(Exception):
            RetryPolicy(backoff_multiplier=0.5)


class TestHeader:
    def test_valid_header(self):
        h = Header(name="Content-Type", value="application/json")
        assert h.name == "Content-Type"
        assert h.value == "application/json"

    def test_invalid_header_name(self):
        with pytest.raises(Exception):
            Header(name="Bad Header:", value="test")

    def test_empty_name(self):
        with pytest.raises(Exception):
            Header(name="", value="test")


class TestWebhookEndpoint:
    def test_create_basic(self):
        ep = WebhookEndpoint(name="Test", url="https://example.com/webhook")
        assert ep.name == "Test"
        assert ep.url == "https://example.com/webhook"
        assert ep.method == WebhookMethod.POST
        assert ep.status == WebhookStatus.ACTIVE
        assert ep.is_active()

    def test_create_full(self):
        ep = WebhookEndpoint(
            name="Full",
            url="https://api.example.com/hook",
            method=WebhookMethod.PUT,
            headers=[Header(name="X-Custom", value="test")],
            tags=["production", "api"],
            secret="my-secret",
            timeout_seconds=60.0,
            description="Production endpoint",
        )
        assert ep.method == WebhookMethod.PUT
        assert len(ep.headers) == 1
        assert "production" in ep.tags
        assert ep.secret == "my-secret"

    def test_invalid_url(self):
        with pytest.raises(Exception):
            WebhookEndpoint(name="Bad", url="ftp://example.com")

    def test_invalid_tag(self):
        with pytest.raises(Exception):
            WebhookEndpoint(name="Bad", url="https://example.com", tags=["has space"])

    def test_is_active(self):
        ep = WebhookEndpoint(name="Test", url="https://example.com")
        assert ep.is_active()
        ep.status = WebhookStatus.PAUSED
        assert not ep.is_active()
        ep.status = WebhookStatus.DISABLED
        assert not ep.is_active()


class TestWebhookDelivery:
    def test_create_delivery(self):
        d = WebhookDelivery(
            endpoint_id="ep-123",
            payload={"event": "test", "data": "hello"},
            event_type="test.event",
        )
        assert d.endpoint_id == "ep-123"
        assert d.status == DeliveryStatus.PENDING
        assert d.event_type == "test.event"
        assert len(d.attempts) == 0

    def test_current_attempt_number(self):
        d = WebhookDelivery(endpoint_id="ep-1", payload={})
        assert d.current_attempt_number() == 0
        d.attempts.append(DeliveryAttempt(delivery_id=d.id, attempt_number=1))
        assert d.current_attempt_number() == 1

    def test_last_attempt(self):
        d = WebhookDelivery(endpoint_id="ep-1", payload={})
        assert d.last_attempt() is None
        a1 = DeliveryAttempt(delivery_id=d.id, attempt_number=1)
        d.attempts.append(a1)
        assert d.last_attempt() == a1

    def test_can_retry(self):
        policy = RetryPolicy(max_retries=3)
        d = WebhookDelivery(endpoint_id="ep-1", payload={})
        assert d.can_retry(policy)  # 0 attempts < 4 max (3+1)
        for i in range(4):
            d.attempts.append(DeliveryAttempt(delivery_id=d.id, attempt_number=i + 1))
        assert not d.can_retry(policy)  # 4 attempts = max+1

    def test_cannot_retry_success(self):
        policy = RetryPolicy(max_retries=3)
        d = WebhookDelivery(endpoint_id="ep-1", payload={}, status=DeliveryStatus.SUCCESS)
        assert not d.can_retry(policy)

    def test_cannot_retry_abandoned(self):
        policy = RetryPolicy(max_retries=3)
        d = WebhookDelivery(endpoint_id="ep-1", payload={}, status=DeliveryStatus.ABANDONED)
        assert not d.can_retry(policy)


class TestRelayRule:
    def test_create_rule(self):
        rule = RelayRule(
            name="Stripe relay",
            path_pattern="/stripe/*",
            target_endpoint_ids=["ep-1", "ep-2"],
        )
        assert rule.name == "Stripe relay"
        assert rule.active

    def test_invalid_path(self):
        with pytest.raises(Exception):
            RelayRule(name="Bad", path_pattern="no-slash", target_endpoint_ids=["ep-1"])

    def test_matches_exact(self):
        rule = RelayRule(name="Test", path_pattern="/api/webhook", target_endpoint_ids=["ep-1"])
        assert rule.matches_path("/api/webhook")
        assert not rule.matches_path("/api/other")

    def test_matches_wildcard(self):
        rule = RelayRule(name="Test", path_pattern="/stripe/*", target_endpoint_ids=["ep-1"])
        assert rule.matches_path("/stripe/events")
        assert rule.matches_path("/stripe/payments")
        assert not rule.matches_path("/github/events")

    def test_matches_catch_all(self):
        rule = RelayRule(name="All", path_pattern="/*", target_endpoint_ids=["ep-1"])
        assert rule.matches_path("/anything")
        assert rule.matches_path("/nested/path")


class TestIncomingWebhook:
    def test_create(self):
        iw = IncomingWebhook(
            path="/stripe/events",
            method="POST",
            headers={"Content-Type": "application/json"},
            body={"type": "payment_succeeded"},
        )
        assert not iw.processed
        assert len(iw.forwarded_to) == 0
