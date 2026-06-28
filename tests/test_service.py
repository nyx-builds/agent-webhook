"""Tests for agent-webhook service layer."""

import pytest

from agent_webhook.models import (
    DeliveryStatus,
    EventSubscription,
    Header,
    RelayRule,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookStatus,
)
from agent_webhook.service import WebhookService
from agent_webhook.store import WebhookStore


@pytest.fixture
def store(tmp_path):
    return WebhookStore(tmp_path / "test_service.json")


@pytest.fixture
def service(store):
    return WebhookService(store=store)


@pytest.fixture
def sample_endpoint(service):
    return service.create_endpoint(
        name="Test Endpoint",
        url="https://example.com/webhook",
        tags=["test"],
    )


class TestEndpointManagement:
    def test_create_endpoint(self, service):
        ep = service.create_endpoint(
            name="My Endpoint",
            url="https://api.example.com/hook",
            method="POST",
            tags=["production"],
        )
        assert ep.name == "My Endpoint"
        assert ep.url == "https://api.example.com/hook"
        assert "production" in ep.tags

    def test_create_endpoint_with_headers(self, service):
        ep = service.create_endpoint(
            name="Headered",
            url="https://example.com",
            headers={"Authorization": "Bearer token123"},
        )
        assert len(ep.headers) == 1
        assert ep.headers[0].name == "Authorization"

    def test_create_endpoint_with_secret(self, service):
        ep = service.create_endpoint(
            name="Secure",
            url="https://example.com",
            secret="my-secret",
        )
        assert ep.secret == "my-secret"

    def test_get_endpoint(self, service, sample_endpoint):
        result = service.get_endpoint(sample_endpoint.id)
        assert result is not None
        assert result.name == "Test Endpoint"

    def test_get_endpoint_nonexistent(self, service):
        assert service.get_endpoint("nonexistent") is None

    def test_list_endpoints(self, service, sample_endpoint):
        service.create_endpoint(name="Second", url="https://other.example.com")
        endpoints = service.list_endpoints()
        assert len(endpoints) == 2

    def test_list_endpoints_by_status(self, service, sample_endpoint):
        service.pause_endpoint(sample_endpoint.id)
        active = service.list_endpoints(status=WebhookStatus.ACTIVE)
        assert len(active) == 0
        paused = service.list_endpoints(status=WebhookStatus.PAUSED)
        assert len(paused) == 1

    def test_list_endpoints_by_tag(self, service, sample_endpoint):
        service.create_endpoint(name="Other", url="https://other.example.com", tags=["production"])
        tagged = service.list_endpoints(tag="test")
        assert len(tagged) == 1

    def test_update_endpoint(self, service, sample_endpoint):
        updated = service.update_endpoint(sample_endpoint.id, name="Updated Name")
        assert updated is not None
        assert updated.name == "Updated Name"

    def test_pause_endpoint(self, service, sample_endpoint):
        result = service.pause_endpoint(sample_endpoint.id)
        assert result is not None
        assert result.status == WebhookStatus.PAUSED

    def test_resume_endpoint(self, service, sample_endpoint):
        service.pause_endpoint(sample_endpoint.id)
        result = service.resume_endpoint(sample_endpoint.id)
        assert result.status == WebhookStatus.ACTIVE

    def test_disable_endpoint(self, service, sample_endpoint):
        result = service.disable_endpoint(sample_endpoint.id)
        assert result.status == WebhookStatus.DISABLED

    def test_delete_endpoint(self, service, sample_endpoint):
        assert service.delete_endpoint(sample_endpoint.id) is True
        assert service.get_endpoint(sample_endpoint.id) is None

    def test_delete_nonexistent_endpoint(self, service):
        assert service.delete_endpoint("nonexistent") is False

    def test_pause_nonexistent(self, service):
        assert service.pause_endpoint("nonexistent") is None

    def test_resume_nonexistent(self, service):
        assert service.resume_endpoint("nonexistent") is None


class TestDeliveryManagement:
    @pytest.mark.asyncio
    async def test_send_webhook(self, service, sample_endpoint):
        result = await service.send_webhook(
            endpoint_id=sample_endpoint.id,
            payload={"event": "test"},
            event_type="test.event",
        )
        assert result is not None
        assert result.event_type == "test.event"

    @pytest.mark.asyncio
    async def test_send_with_metadata(self, service, sample_endpoint):
        result = await service.send_webhook(
            endpoint_id=sample_endpoint.id,
            payload={"test": True},
            metadata={"source": "unit-test"},
        )
        assert result.metadata["source"] == "unit-test"

    @pytest.mark.asyncio
    async def test_batch_send(self, service, sample_endpoint):
        ep2 = service.create_endpoint(name="Second", url="https://other.example.com/hook")
        results = await service.batch_send(
            endpoint_ids=[sample_endpoint.id, ep2.id],
            payload={"event": "broadcast"},
            event_type="broadcast",
        )
        assert len(results) == 2

    @pytest.mark.asyncio
    async def test_batch_send_empty(self, service):
        results = await service.batch_send(
            endpoint_ids=[],
            payload={"event": "test"},
        )
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_retry_delivery(self, service, sample_endpoint):
        result = await service.send_webhook(
            endpoint_id=sample_endpoint.id,
            payload={"test": True},
        )
        # Even if it failed (no real endpoint), we can try retry
        retried = await service.retry_delivery(result.id)
        assert retried is not None

    @pytest.mark.asyncio
    async def test_retry_nonexistent_delivery(self, service):
        result = await service.retry_delivery("nonexistent")
        assert result is None

    def test_cancel_delivery(self, service, sample_endpoint):
        # Create a pending delivery directly
        delivery = WebhookDelivery(
            endpoint_id=sample_endpoint.id,
            payload={"test": True},
            status=DeliveryStatus.PENDING,
        )
        service.store.add_delivery(delivery)
        result = service.cancel_delivery(delivery.id)
        assert result is not None
        assert result.status == DeliveryStatus.ABANDONED

    def test_cancel_retrying_delivery(self, service, sample_endpoint):
        delivery = WebhookDelivery(
            endpoint_id=sample_endpoint.id,
            payload={"test": True},
            status=DeliveryStatus.RETRYING,
        )
        service.store.add_delivery(delivery)
        result = service.cancel_delivery(delivery.id)
        assert result.status == DeliveryStatus.ABANDONED

    def test_cancel_successful_delivery(self, service, sample_endpoint):
        delivery = WebhookDelivery(
            endpoint_id=sample_endpoint.id,
            payload={"test": True},
            status=DeliveryStatus.SUCCESS,
        )
        service.store.add_delivery(delivery)
        result = service.cancel_delivery(delivery.id)
        # Should not cancel a successful delivery
        assert result.status == DeliveryStatus.SUCCESS

    def test_cancel_nonexistent_delivery(self, service):
        result = service.cancel_delivery("nonexistent")
        assert result is None

    def test_get_delivery(self, service, sample_endpoint):
        delivery = WebhookDelivery(
            endpoint_id=sample_endpoint.id,
            payload={"test": True},
        )
        service.store.add_delivery(delivery)
        result = service.get_delivery(delivery.id)
        assert result is not None

    def test_list_deliveries(self, service, sample_endpoint):
        delivery = WebhookDelivery(
            endpoint_id=sample_endpoint.id,
            payload={"test": True},
        )
        service.store.add_delivery(delivery)
        deliveries = service.list_deliveries()
        assert len(deliveries) == 1

    def test_list_deliveries_by_endpoint(self, service, sample_endpoint):
        ep2 = service.create_endpoint(name="Other", url="https://other.example.com")
        d1 = WebhookDelivery(endpoint_id=sample_endpoint.id, payload={"a": 1})
        d2 = WebhookDelivery(endpoint_id=ep2.id, payload={"b": 2})
        service.store.add_delivery(d1)
        service.store.add_delivery(d2)
        result = service.list_deliveries(endpoint_id=sample_endpoint.id)
        assert len(result) == 1


class TestEventSubscriptions:
    def test_add_subscription(self, service, sample_endpoint):
        sub = service.add_subscription(
            endpoint_id=sample_endpoint.id,
            event_types=["order.created", "order.updated"],
        )
        assert sub is not None
        assert sub.endpoint_id == sample_endpoint.id
        assert len(sub.event_types) == 2

    def test_add_subscription_nonexistent_endpoint(self, service):
        sub = service.add_subscription(
            endpoint_id="nonexistent",
            event_types=["test"],
        )
        assert sub is None

    def test_remove_subscription(self, service, sample_endpoint):
        sub = service.add_subscription(
            endpoint_id=sample_endpoint.id,
            event_types=["test"],
        )
        assert service.remove_subscription(sub.id) is True

    def test_remove_nonexistent_subscription(self, service):
        assert service.remove_subscription("nonexistent") is False

    def test_list_subscriptions(self, service, sample_endpoint):
        service.add_subscription(endpoint_id=sample_endpoint.id, event_types=["a"])
        service.add_subscription(endpoint_id=sample_endpoint.id, event_types=["b"])
        subs = service.list_subscriptions()
        assert len(subs) == 2

    def test_list_subscriptions_by_endpoint(self, service, sample_endpoint):
        ep2 = service.create_endpoint(name="Other", url="https://other.example.com")
        service.add_subscription(endpoint_id=sample_endpoint.id, event_types=["a"])
        service.add_subscription(endpoint_id=ep2.id, event_types=["b"])
        subs = service.list_subscriptions(endpoint_id=sample_endpoint.id)
        assert len(subs) == 1

    @pytest.mark.asyncio
    async def test_send_to_subscribers(self, service, sample_endpoint):
        service.add_subscription(
            endpoint_id=sample_endpoint.id,
            event_types=["order.created"],
        )
        results = await service.send_to_subscribers(
            event_type="order.created",
            payload={"order_id": "123"},
        )
        assert len(results) >= 1

    @pytest.mark.asyncio
    async def test_send_to_subscribers_no_match(self, service, sample_endpoint):
        service.add_subscription(
            endpoint_id=sample_endpoint.id,
            event_types=["order.created"],
        )
        results = await service.send_to_subscribers(
            event_type="user.signup",
            payload={"user_id": "456"},
        )
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_send_to_subscribers_skips_paused(self, service, sample_endpoint):
        service.add_subscription(
            endpoint_id=sample_endpoint.id,
            event_types=["order.created"],
        )
        service.pause_endpoint(sample_endpoint.id)
        results = await service.send_to_subscribers(
            event_type="order.created",
            payload={"order_id": "789"},
        )
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_send_to_subscribers_multiple(self, service):
        ep1 = service.create_endpoint(name="EP1", url="https://example.com/1")
        ep2 = service.create_endpoint(name="EP2", url="https://example.com/2")
        service.add_subscription(endpoint_id=ep1.id, event_types=["order.created"])
        service.add_subscription(endpoint_id=ep2.id, event_types=["order.created"])
        results = await service.send_to_subscribers(
            event_type="order.created",
            payload={"order_id": "multi"},
        )
        assert len(results) == 2


class TestHealthCheck:
    @pytest.mark.asyncio
    async def test_health_check_nonexistent(self, service):
        result = await service.health_check("nonexistent")
        assert result["status"] == "not_found"

    @pytest.mark.asyncio
    async def test_health_check_creates_delivery(self, service, sample_endpoint):
        # Even though it'll fail (no real server), it should create a delivery
        result = await service.health_check(sample_endpoint.id)
        assert "healthy" in result
        assert "endpoint_name" in result
        assert result["endpoint_name"] == "Test Endpoint"
        assert "url" in result
        assert "timestamp" in result


class TestRelayRules:
    def test_add_relay_rule(self, service, sample_endpoint):
        rule = service.add_relay_rule(
            name="Test Relay",
            path_pattern="/test/*",
            target_endpoint_ids=[sample_endpoint.id],
        )
        assert rule.name == "Test Relay"

    def test_list_relay_rules(self, service, sample_endpoint):
        service.add_relay_rule(
            name="Rule 1",
            path_pattern="/a/*",
            target_endpoint_ids=[sample_endpoint.id],
        )
        rules = service.list_relay_rules()
        assert len(rules) == 1

    def test_delete_relay_rule(self, service, sample_endpoint):
        rule = service.add_relay_rule(
            name="Rule 1",
            path_pattern="/a/*",
            target_endpoint_ids=[sample_endpoint.id],
        )
        assert service.delete_relay_rule(rule.id) is True

    def test_receive_incoming(self, service, sample_endpoint):
        service.add_relay_rule(
            name="Test",
            path_pattern="/test/*",
            target_endpoint_ids=[sample_endpoint.id],
        )
        delivery_ids = service.receive_incoming(
            path="/test/event",
            method="POST",
            body={"data": "test"},
        )
        assert len(delivery_ids) == 1

    def test_receive_incoming_no_match(self, service, sample_endpoint):
        delivery_ids = service.receive_incoming(
            path="/nonexistent",
            method="POST",
            body={},
        )
        assert len(delivery_ids) == 0

    def test_list_incoming(self, service, sample_endpoint):
        service.add_relay_rule(
            name="Test",
            path_pattern="/test/*",
            target_endpoint_ids=[sample_endpoint.id],
        )
        service.receive_incoming(
            path="/test/event",
            method="POST",
            body={"data": "test"},
        )
        incoming = service.list_incoming()
        assert len(incoming) == 1


class TestStatistics:
    def test_get_stats(self, service, sample_endpoint):
        stats = service.get_stats(sample_endpoint.id)
        assert stats is not None
        assert stats["total_deliveries"] == 0

    def test_get_stats_nonexistent(self, service):
        assert service.get_stats("nonexistent") is None

    def test_get_all_stats(self, service, sample_endpoint):
        service.create_endpoint(name="Other", url="https://other.example.com")
        stats = service.get_all_stats()
        assert len(stats) == 2


class TestEventLog:
    def test_log_event(self, service):
        entry = service.log_event(
            event_type="endpoint.created",
            details={"name": "Test"},
            endpoint_id="ep-1",
        )
        assert entry is not None
        assert entry.event_type == "endpoint.created"

    def test_list_event_log(self, service):
        service.log_event(event_type="test.1")
        service.log_event(event_type="test.2")
        entries = service.list_event_log()
        assert len(entries) == 2

    def test_list_event_log_by_type(self, service):
        service.log_event(event_type="test.a")
        service.log_event(event_type="test.b")
        entries = service.list_event_log(event_type="test.a")
        assert len(entries) == 1

    def test_list_event_log_by_endpoint(self, service):
        service.log_event(event_type="test", endpoint_id="ep-1")
        service.log_event(event_type="test", endpoint_id="ep-2")
        entries = service.list_event_log(endpoint_id="ep-1")
        assert len(entries) == 1

    def test_list_event_log_with_limit(self, service):
        for i in range(10):
            service.log_event(event_type=f"event.{i}")
        entries = service.list_event_log(limit=5)
        assert len(entries) == 5
