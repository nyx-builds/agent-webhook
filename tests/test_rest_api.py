"""Integration tests for the REST API server."""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from agent_webhook.rest_api import create_app


@pytest.fixture
def app(tmp_path):
    """Create a test FastAPI app with a temporary SQLite store."""
    db_path = str(tmp_path / "test_webhook.db")
    return create_app(store_path=db_path)


@pytest.fixture
async def client(app):
    """Create an async test client for the FastAPI app."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.mark.asyncio
class TestHealthEndpoint:
    async def test_health(self, client):
        response = await client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "ok"
        assert data["version"] == "0.5.0"
        assert "timestamp" in data


@pytest.mark.asyncio
class TestEndpointCRUD:
    async def test_create_endpoint(self, client):
        response = await client.post("/endpoints", json={
            "name": "Test Endpoint",
            "url": "https://example.com/webhook",
            "method": "POST",
            "tags": ["test"],
        })
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Test Endpoint"
        assert data["url"] == "https://example.com/webhook"
        assert "test" in data["tags"]

    async def test_create_endpoint_validation(self, client):
        # Missing required fields
        response = await client.post("/endpoints", json={})
        assert response.status_code == 422

        # Invalid URL
        response = await client.post("/endpoints", json={
            "name": "Bad URL",
            "url": "not-a-url",
        })
        assert response.status_code == 400

    async def test_list_endpoints(self, client):
        # Create some endpoints
        await client.post("/endpoints", json={"name": "EP1", "url": "https://ep1.com"})
        await client.post("/endpoints", json={"name": "EP2", "url": "https://ep2.com"})

        response = await client.get("/endpoints")
        assert response.status_code == 200
        data = response.json()
        assert len(data) >= 2

    async def test_get_endpoint(self, client):
        create_resp = await client.post("/endpoints", json={
            "name": "Get Me",
            "url": "https://getme.com",
        })
        ep_id = create_resp.json()["id"]

        response = await client.get(f"/endpoints/{ep_id}")
        assert response.status_code == 200
        assert response.json()["name"] == "Get Me"

    async def test_get_endpoint_not_found(self, client):
        response = await client.get("/endpoints/nonexistent-id")
        assert response.status_code == 404

    async def test_update_endpoint(self, client):
        create_resp = await client.post("/endpoints", json={
            "name": "Original",
            "url": "https://original.com",
        })
        ep_id = create_resp.json()["id"]

        response = await client.patch(f"/endpoints/{ep_id}", json={
            "name": "Updated",
            "description": "New description",
        })
        assert response.status_code == 200
        data = response.json()
        assert data["name"] == "Updated"
        assert data["description"] == "New description"

    async def test_delete_endpoint(self, client):
        create_resp = await client.post("/endpoints", json={
            "name": "Delete Me",
            "url": "https://deleteme.com",
        })
        ep_id = create_resp.json()["id"]

        response = await client.delete(f"/endpoints/{ep_id}")
        assert response.status_code == 200

        # Verify deleted
        response = await client.get(f"/endpoints/{ep_id}")
        assert response.status_code == 404


@pytest.mark.asyncio
class TestSubscriptions:
    async def test_create_and_list_subscription(self, client):
        # Create endpoint first
        ep_resp = await client.post("/endpoints", json={
            "name": "Sub EP", "url": "https://sub.com",
        })
        ep_id = ep_resp.json()["id"]

        # Subscribe
        response = await client.post("/subscriptions", json={
            "endpoint_id": ep_id,
            "event_types": ["order.created", "order.updated"],
        })
        assert response.status_code == 201
        data = response.json()
        assert "order.created" in data["event_types"]

        # List
        response = await client.get("/subscriptions")
        assert response.status_code == 200
        assert len(response.json()) >= 1

    async def test_delete_subscription(self, client):
        ep_resp = await client.post("/endpoints", json={
            "name": "Del Sub EP", "url": "https://delsub.com",
        })
        ep_id = ep_resp.json()["id"]

        sub_resp = await client.post("/subscriptions", json={
            "endpoint_id": ep_id,
            "event_types": ["test.event"],
        })
        sub_id = sub_resp.json()["id"]

        response = await client.delete(f"/subscriptions/{sub_id}")
        assert response.status_code == 200


@pytest.mark.asyncio
class TestRelayRules:
    async def test_create_and_list_relay_rule(self, client):
        # Create endpoint first
        ep_resp = await client.post("/endpoints", json={
            "name": "Relay EP", "url": "https://relay.com",
        })
        ep_id = ep_resp.json()["id"]

        # Create relay rule
        response = await client.post("/relay-rules", json={
            "name": "Stripe Relay",
            "path_pattern": "/stripe/*",
            "target_endpoint_ids": [ep_id],
        })
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Stripe Relay"
        assert ep_id in data["target_endpoint_ids"]

        # List
        response = await client.get("/relay-rules")
        assert response.status_code == 200
        assert len(response.json()) >= 1

    async def test_get_relay_rule(self, client):
        ep_resp = await client.post("/endpoints", json={
            "name": "Get Relay EP", "url": "https://getrelay.com",
        })
        ep_id = ep_resp.json()["id"]

        rule_resp = await client.post("/relay-rules", json={
            "name": "Get Rule",
            "path_pattern": "/test/*",
            "target_endpoint_ids": [ep_id],
        })
        rule_id = rule_resp.json()["id"]

        response = await client.get(f"/relay-rules/{rule_id}")
        assert response.status_code == 200
        assert response.json()["name"] == "Get Rule"

    async def test_update_relay_rule(self, client):
        ep_resp = await client.post("/endpoints", json={
            "name": "Upd Relay EP", "url": "https://updrelay.com",
        })
        ep_id = ep_resp.json()["id"]

        rule_resp = await client.post("/relay-rules", json={
            "name": "Old Name",
            "path_pattern": "/old/*",
            "target_endpoint_ids": [ep_id],
        })
        rule_id = rule_resp.json()["id"]

        response = await client.patch(f"/relay-rules/{rule_id}", json={
            "name": "New Name",
            "active": False,
        })
        assert response.status_code == 200
        assert response.json()["name"] == "New Name"
        assert response.json()["active"] is False

    async def test_delete_relay_rule(self, client):
        ep_resp = await client.post("/endpoints", json={
            "name": "Del Relay EP", "url": "https://delrelay.com",
        })
        ep_id = ep_resp.json()["id"]

        rule_resp = await client.post("/relay-rules", json={
            "name": "Delete Me",
            "path_pattern": "/del/*",
            "target_endpoint_ids": [ep_id],
        })
        rule_id = rule_resp.json()["id"]

        response = await client.delete(f"/relay-rules/{rule_id}")
        assert response.status_code == 200


@pytest.mark.asyncio
class TestTransforms:
    async def test_create_and_list_transform(self, client):
        # Create a field_map transform
        response = await client.post("/transforms", json={
            "name": "Rename Fields",
            "type": "field_map",
            "config": {"mapping": {"old_key": "new_key"}, "keep_unmapped": True},
        })
        assert response.status_code == 201
        data = response.json()
        assert data["name"] == "Rename Fields"
        assert data["type"] == "field_map"

        # List
        response = await client.get("/transforms")
        assert response.status_code == 200
        assert len(response.json()) >= 1

    async def test_get_transform(self, client):
        create_resp = await client.post("/transforms", json={
            "name": "Filter Transform",
            "type": "filter",
            "config": {"include": ["id", "name"]},
        })
        t_id = create_resp.json()["id"]

        response = await client.get(f"/transforms/{t_id}")
        assert response.status_code == 200
        assert response.json()["name"] == "Filter Transform"

    async def test_delete_transform(self, client):
        create_resp = await client.post("/transforms", json={
            "name": "Delete Me",
            "type": "template",
            "config": {"fields": {"msg": "{{payload.text}}"}},
        })
        t_id = create_resp.json()["id"]

        response = await client.delete(f"/transforms/{t_id}")
        assert response.status_code == 200


@pytest.mark.asyncio
class TestDeadLetterQueue:
    async def test_list_dlq_empty(self, client):
        response = await client.get("/dlq")
        assert response.status_code == 200
        assert response.json() == []


@pytest.mark.asyncio
class TestStats:
    async def test_all_stats(self, client):
        response = await client.get("/stats")
        assert response.status_code == 200

    async def test_endpoint_stats_not_found(self, client):
        response = await client.get("/stats/nonexistent-id")
        assert response.status_code == 404


@pytest.mark.asyncio
class TestEventLog:
    async def test_list_event_log(self, client):
        response = await client.get("/event-log")
        assert response.status_code == 200


@pytest.mark.asyncio
class TestMetrics:
    async def test_get_metrics_json(self, client):
        response = await client.get("/metrics")
        assert response.status_code == 200
        data = response.json()
        assert "uptime_seconds" in data
        assert "deliveries_total" in data

    async def test_get_metrics_prometheus(self, client):
        response = await client.get("/metrics/prometheus")
        assert response.status_code == 200
        text = response.text
        assert "agent_webhook_up" in text
        assert "agent_webhook_deliveries_total" in text


@pytest.mark.asyncio
class TestIncomingWebhooks:
    async def test_receive_and_list_incoming(self, client):
        # Receive an incoming webhook
        response = await client.post("/incoming/receive", json={
            "path": "/stripe/webhook",
            "method": "POST",
            "body": {"event": "payment.created", "amount": 1000},
        })
        assert response.status_code == 200
        # No relay rules, so no forwarding
        data = response.json()
        assert data["count"] == 0

        # List incoming
        response = await client.get("/incoming")
        assert response.status_code == 200
        assert len(response.json()) >= 1
