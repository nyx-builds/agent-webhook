"""REST API server for agent-webhook — FastAPI-based HTTP API for programmatic webhook management."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel as PydanticModel
from pydantic import Field

from .models import (
    DeliveryStatus,
    RateLimitPeriod,
    SigningAlgorithm,
    TransformType,
    WebhookMethod,
    WebhookStatus,
)
from .service import WebhookService


# ── Request/Response Models ────────────────────────────────────────


class CreateEndpointRequest(PydanticModel):
    name: str = Field(..., min_length=1, max_length=100)
    url: str = Field(..., min_length=1)
    method: str = Field(default="POST")
    headers: dict[str, str] | None = None
    tags: list[str] | None = None
    secret: str | None = None
    signing_algorithm: str = Field(default="sha256")
    timeout_seconds: float = Field(default=30.0, ge=1.0, le=300.0)
    description: str | None = None
    max_retries: int = Field(default=3, ge=0, le=10)
    initial_delay_seconds: float = Field(default=1.0, ge=0.1)
    max_delay_seconds: float = Field(default=300.0, ge=1.0)
    backoff_multiplier: float = Field(default=2.0, ge=1.0)
    retry_on_status_codes: list[int] | None = None
    transform_ids: list[str] | None = None
    rate_limit: dict[str, Any] | None = None


class UpdateEndpointRequest(PydanticModel):
    name: str | None = None
    url: str | None = None
    method: str | None = None
    headers: dict[str, str] | None = None
    tags: list[str] | None = None
    secret: str | None = None
    signing_algorithm: str | None = None
    timeout_seconds: float | None = None
    description: str | None = None
    status: str | None = None
    transform_ids: list[str] | None = None
    rate_limit: dict[str, Any] | None = None


class SendWebhookRequest(PydanticModel):
    endpoint_id: str
    payload: dict[str, Any]
    event_type: str | None = None
    metadata: dict[str, Any] | None = None
    headers: dict[str, str] | None = None


class BatchSendRequest(PydanticModel):
    endpoint_ids: list[str]
    payload: dict[str, Any]
    event_type: str | None = None
    metadata: dict[str, Any] | None = None
    headers: dict[str, str] | None = None


class SubscribeRequest(PydanticModel):
    endpoint_id: str
    event_types: list[str] = Field(..., min_length=1)


class SendToSubscribersRequest(PydanticModel):
    event_type: str
    payload: dict[str, Any]
    metadata: dict[str, Any] | None = None
    headers: dict[str, str] | None = None


class AddRelayRuleRequest(PydanticModel):
    name: str = Field(..., min_length=1)
    path_pattern: str
    target_endpoint_ids: list[str] = Field(..., min_length=1)
    tags: list[str] | None = None


class UpdateRelayRuleRequest(PydanticModel):
    name: str | None = None
    path_pattern: str | None = None
    target_endpoint_ids: list[str] | None = None
    active: bool | None = None
    tags: list[str] | None = None


class CreateTransformRequest(PydanticModel):
    name: str = Field(..., min_length=1)
    type: str
    config: dict[str, Any]


class ReceiveIncomingRequest(PydanticModel):
    path: str
    method: str = "POST"
    headers: dict[str, str] | None = None
    body: dict[str, Any] | str | None = None
    source_ip: str | None = None


class BatchReplayDLQRequest(PydanticModel):
    endpoint_id: str | None = None


class VerifySignatureRequest(PydanticModel):
    raw_body: str
    headers: dict[str, str]
    secret: str
    provider: str = Field(default="generic")
    algorithm: str = Field(default="sha256")
    tolerance_seconds: int = Field(default=300)


class GenerateSignatureRequest(PydanticModel):
    raw_body: str
    secret: str
    provider: str = Field(default="generic")
    algorithm: str = Field(default="sha256")


# ── App Factory ────────────────────────────────────────────────────


def create_app(store_path: str = "webhook_store.db") -> FastAPI:
    """Create the FastAPI application."""

    service = WebhookService(store_path=store_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        yield
        await service.close()

    app = FastAPI(
        title="Agent Webhook",
        description="Webhook management, delivery, and relay REST API for autonomous agents",
        version="0.5.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # ── Health ───────────────────────────────────────────────────

    @app.get("/health")
    async def health():
        return {"status": "ok", "version": "0.5.0", "timestamp": datetime.now(timezone.utc).isoformat()}

    # ── Endpoints ────────────────────────────────────────────────

    @app.post("/endpoints", status_code=201)
    async def create_endpoint(req: CreateEndpointRequest):
        try:
            rl = None
            if req.rate_limit:
                rl = req.rate_limit
            ep = service.create_endpoint(
                name=req.name,
                url=req.url,
                method=req.method,
                headers=req.headers,
                tags=req.tags,
                secret=req.secret,
                timeout_seconds=req.timeout_seconds,
                description=req.description,
                max_retries=req.max_retries,
                initial_delay_seconds=req.initial_delay_seconds,
                max_delay_seconds=req.max_delay_seconds,
                backoff_multiplier=req.backoff_multiplier,
                retry_on_status_codes=req.retry_on_status_codes,
                transform_ids=req.transform_ids,
                rate_limit=rl,
            )
            # Update signing algorithm if specified
            if req.signing_algorithm and req.signing_algorithm != "sha256":
                service.update_endpoint(ep.id, signing_algorithm=SigningAlgorithm(req.signing_algorithm))
                ep = service.get_endpoint(ep.id)
            return ep.model_dump(mode="json")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.get("/endpoints")
    async def list_endpoints(
        status: str | None = Query(None),
        tag: str | None = Query(None),
    ):
        ws = WebhookStatus(status) if status else None
        endpoints = service.list_endpoints(status=ws, tag=tag)
        return [e.model_dump(mode="json") for e in endpoints]

    @app.get("/endpoints/{endpoint_id}")
    async def get_endpoint(endpoint_id: str):
        ep = service.get_endpoint(endpoint_id)
        if ep is None:
            raise HTTPException(status_code=404, detail="Endpoint not found")
        return ep.model_dump(mode="json")

    @app.patch("/endpoints/{endpoint_id}")
    async def update_endpoint(endpoint_id: str, req: UpdateEndpointRequest):
        updates: dict[str, Any] = {}
        for key in ["name", "url", "secret", "timeout_seconds", "description", "transform_ids"]:
            val = getattr(req, key, None)
            if val is not None:
                updates[key] = val
        if req.status is not None:
            updates["status"] = WebhookStatus(req.status)
        if req.method is not None:
            updates["method"] = WebhookMethod(req.method)
        if req.signing_algorithm is not None:
            updates["signing_algorithm"] = SigningAlgorithm(req.signing_algorithm)
        if req.rate_limit is not None:
            updates["rate_limit"] = req.rate_limit
        if req.tags is not None:
            updates["tags"] = req.tags
        if req.headers is not None:
            from .models import Header
            updates["headers"] = [Header(name=k, value=v) for k, v in req.headers.items()]

        ep = service.update_endpoint(endpoint_id, **updates)
        if ep is None:
            raise HTTPException(status_code=404, detail="Endpoint not found")
        return ep.model_dump(mode="json")

    @app.delete("/endpoints/{endpoint_id}")
    async def delete_endpoint(endpoint_id: str):
        if not service.delete_endpoint(endpoint_id):
            raise HTTPException(status_code=404, detail="Endpoint not found")
        return {"deleted": True}

    # ── Deliveries ────────────────────────────────────────────────

    @app.post("/deliveries/send")
    async def send_webhook(req: SendWebhookRequest):
        ep = service.get_endpoint(req.endpoint_id)
        if ep is None:
            raise HTTPException(status_code=404, detail="Endpoint not found")
        result = await service.send_webhook(
            endpoint_id=req.endpoint_id,
            payload=req.payload,
            event_type=req.event_type,
            metadata=req.metadata or {},
            headers=req.headers or {},
        )
        return result.model_dump(mode="json")

    @app.post("/deliveries/batch-send")
    async def batch_send(req: BatchSendRequest):
        results = await service.batch_send(
            endpoint_ids=req.endpoint_ids,
            payload=req.payload,
            event_type=req.event_type,
            metadata=req.metadata or {},
            headers=req.headers or {},
        )
        return {
            "total": len(results),
            "success": sum(1 for r in results if r.status == DeliveryStatus.SUCCESS),
            "failed": sum(1 for r in results if r.status in (DeliveryStatus.FAILED, DeliveryStatus.ABANDONED)),
            "retrying": sum(1 for r in results if r.status == DeliveryStatus.RETRYING),
            "deliveries": [{"id": r.id, "endpoint_id": r.endpoint_id, "status": r.status.value} for r in results],
        }

    @app.get("/deliveries")
    async def list_deliveries(
        endpoint_id: str | None = Query(None),
        status: str | None = Query(None),
        event_type: str | None = Query(None),
        limit: int = Query(50, ge=1, le=500),
    ):
        ds = DeliveryStatus(status) if status else None
        deliveries = service.list_deliveries(endpoint_id=endpoint_id, status=ds, event_type=event_type, limit=limit)
        return [d.model_dump(mode="json") for d in deliveries]

    @app.get("/deliveries/{delivery_id}")
    async def get_delivery(delivery_id: str):
        d = service.get_delivery(delivery_id)
        if d is None:
            raise HTTPException(status_code=404, detail="Delivery not found")
        return d.model_dump(mode="json")

    @app.post("/deliveries/{delivery_id}/retry")
    async def retry_delivery(delivery_id: str):
        result = await service.retry_delivery(delivery_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Delivery not found or cannot retry")
        return result.model_dump(mode="json")

    @app.post("/deliveries/{delivery_id}/cancel")
    async def cancel_delivery(delivery_id: str):
        result = service.cancel_delivery(delivery_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Delivery not found")
        return result.model_dump(mode="json")

    @app.post("/deliveries/process-pending")
    async def process_pending():
        results = await service.process_pending()
        return {
            "processed": len(results),
            "results": [{"id": r.id, "status": r.status.value} for r in results],
        }

    # ── Subscriptions ─────────────────────────────────────────────

    @app.post("/subscriptions", status_code=201)
    async def add_subscription(req: SubscribeRequest):
        sub = service.add_subscription(endpoint_id=req.endpoint_id, event_types=req.event_types)
        if sub is None:
            raise HTTPException(status_code=404, detail="Endpoint not found")
        return sub.model_dump(mode="json")

    @app.get("/subscriptions")
    async def list_subscriptions(endpoint_id: str | None = Query(None)):
        subs = service.list_subscriptions(endpoint_id=endpoint_id)
        return [s.model_dump(mode="json") for s in subs]

    @app.delete("/subscriptions/{subscription_id}")
    async def delete_subscription(subscription_id: str):
        if not service.remove_subscription(subscription_id):
            raise HTTPException(status_code=404, detail="Subscription not found")
        return {"deleted": True}

    @app.post("/subscriptions/broadcast")
    async def broadcast_to_subscribers(req: SendToSubscribersRequest):
        results = await service.send_to_subscribers(
            event_type=req.event_type,
            payload=req.payload,
            metadata=req.metadata or {},
            headers=req.headers or {},
        )
        return {
            "event_type": req.event_type,
            "delivered": len(results),
            "results": [{"id": r.id, "endpoint_id": r.endpoint_id, "status": r.status.value} for r in results],
        }

    # ── Relay Rules ───────────────────────────────────────────────

    @app.post("/relay-rules", status_code=201)
    async def add_relay_rule(req: AddRelayRuleRequest):
        rule = service.add_relay_rule(
            name=req.name,
            path_pattern=req.path_pattern,
            target_endpoint_ids=req.target_endpoint_ids,
            tags=req.tags,
        )
        return rule.model_dump(mode="json")

    @app.get("/relay-rules")
    async def list_relay_rules():
        rules = service.list_relay_rules()
        return [r.model_dump(mode="json") for r in rules]

    @app.get("/relay-rules/{rule_id}")
    async def get_relay_rule(rule_id: str):
        rule = service.store.get_relay_rule(rule_id)
        if rule is None:
            raise HTTPException(status_code=404, detail="Relay rule not found")
        return rule.model_dump(mode="json")

    @app.patch("/relay-rules/{rule_id}")
    async def update_relay_rule(rule_id: str, req: UpdateRelayRuleRequest):
        updates: dict[str, Any] = {}
        for key in ["name", "path_pattern", "target_endpoint_ids", "active", "tags"]:
            val = getattr(req, key, None)
            if val is not None:
                updates[key] = val
        rule = service.update_relay_rule(rule_id, **updates)
        if rule is None:
            raise HTTPException(status_code=404, detail="Relay rule not found or store does not support updates")
        return rule.model_dump(mode="json")

    @app.delete("/relay-rules/{rule_id}")
    async def delete_relay_rule(rule_id: str):
        if not service.delete_relay_rule(rule_id):
            raise HTTPException(status_code=404, detail="Relay rule not found")
        return {"deleted": True}

    # ── Incoming Webhooks ────────────────────────────────────────

    @app.post("/incoming/receive")
    async def receive_incoming(req: ReceiveIncomingRequest):
        delivery_ids = service.receive_incoming(
            path=req.path,
            method=req.method,
            headers=req.headers or {},
            body=req.body,
            source_ip=req.source_ip,
        )
        return {"forwarded_deliveries": delivery_ids, "count": len(delivery_ids)}

    @app.get("/incoming")
    async def list_incoming(
        path: str | None = Query(None),
        limit: int = Query(50, ge=1, le=500),
    ):
        incoming = service.list_incoming(path=path, limit=limit)
        return [i.model_dump(mode="json") for i in incoming]

    # ── Transforms ────────────────────────────────────────────────

    @app.post("/transforms", status_code=201)
    async def create_transform(req: CreateTransformRequest):
        t = service.create_transform(name=req.name, type=req.type, config=req.config)
        if t is None:
            raise HTTPException(status_code=400, detail="Transforms require SQLite store")
        return t.model_dump(mode="json")

    @app.get("/transforms")
    async def list_transforms(type: str | None = Query(None)):
        transforms = service.list_transforms(type=type)
        return [t.model_dump(mode="json") for t in transforms]

    @app.get("/transforms/{transform_id}")
    async def get_transform(transform_id: str):
        t = service.get_transform(transform_id)
        if t is None:
            raise HTTPException(status_code=404, detail="Transform not found")
        return t.model_dump(mode="json")

    @app.delete("/transforms/{transform_id}")
    async def delete_transform(transform_id: str):
        if not service.delete_transform(transform_id):
            raise HTTPException(status_code=404, detail="Transform not found")
        return {"deleted": True}

    # ── Dead Letter Queue ────────────────────────────────────────

    @app.get("/dlq")
    async def list_dlq(
        endpoint_id: str | None = Query(None),
        replayed: bool | None = Query(None),
        limit: int = Query(50, ge=1, le=500),
    ):
        entries = service.list_dead_letter(endpoint_id=endpoint_id, replayed=replayed, limit=limit)
        return [e.model_dump(mode="json") for e in entries]

    @app.get("/dlq/{entry_id}")
    async def get_dlq_entry(entry_id: str):
        entry = service.get_dead_letter(entry_id)
        if entry is None:
            raise HTTPException(status_code=404, detail="Dead letter entry not found")
        return entry.model_dump(mode="json")

    @app.post("/dlq/{entry_id}/replay")
    async def replay_dlq_entry(entry_id: str):
        result = await service.replay_dead_letter(entry_id)
        if result is None:
            raise HTTPException(status_code=404, detail="Entry not found or already replayed")
        return {
            "message": "Dead letter entry replayed",
            "entry_id": entry_id,
            "new_delivery_id": result.id,
            "status": result.status.value,
        }

    @app.post("/dlq/batch-replay")
    async def batch_replay_dlq(req: BatchReplayDLQRequest):
        results = await service.batch_replay_dead_letter(endpoint_id=req.endpoint_id)
        return {
            "replayed": len(results),
            "results": [{"id": r.id, "endpoint_id": r.endpoint_id, "status": r.status.value} for r in results],
        }

    @app.delete("/dlq/{entry_id}")
    async def delete_dlq_entry(entry_id: str):
        if not service.delete_dead_letter(entry_id):
            raise HTTPException(status_code=404, detail="Entry not found")
        return {"deleted": True}

    # ── Health Check ──────────────────────────────────────────────

    @app.post("/endpoints/{endpoint_id}/health-check")
    async def health_check(endpoint_id: str):
        result = await service.health_check(endpoint_id)
        if result.get("status") == "not_found":
            raise HTTPException(status_code=404, detail="Endpoint not found")
        return result

    # ── Stats ────────────────────────────────────────────────────

    @app.get("/stats")
    async def all_stats():
        return service.get_all_stats()

    @app.get("/stats/{endpoint_id}")
    async def endpoint_stats(endpoint_id: str):
        s = service.get_stats(endpoint_id)
        if s is None:
            raise HTTPException(status_code=404, detail="Endpoint not found")
        return s

    # ── Event Log ────────────────────────────────────────────────

    @app.get("/event-log")
    async def list_event_log(
        event_type: str | None = Query(None),
        endpoint_id: str | None = Query(None),
        limit: int = Query(50, ge=1, le=500),
    ):
        entries = service.list_event_log(event_type=event_type, endpoint_id=endpoint_id, limit=limit)
        return [e.model_dump(mode="json") for e in entries]

    # ── Rate Limit Status ─────────────────────────────────────────

    @app.get("/rate-limit-status/{endpoint_id}")
    async def rate_limit_status(endpoint_id: str):
        ep = service.get_endpoint(endpoint_id)
        if ep is None:
            raise HTTPException(status_code=404, detail="Endpoint not found")
        if ep.rate_limit is None:
            return {"endpoint_id": endpoint_id, "rate_limit": None, "status": None}
        status = service.get_rate_limit_status(endpoint_id)
        return status

    # ── Metrics ──────────────────────────────────────────────────

    @app.get("/metrics")
    async def get_metrics_json():
        from .metrics import get_metrics
        m = get_metrics()
        return m.get_json()

    @app.get("/metrics/prometheus")
    async def get_metrics_prometheus():
        from .metrics import get_metrics
        from fastapi.responses import PlainTextResponse
        m = get_metrics()
        return PlainTextResponse(content=m.generate_prometheus(), media_type="text/plain")

    # ── Circuit Breaker ────────────────────────────────────────────

    @app.get("/circuit-breakers")
    async def list_circuit_breakers():
        """Get circuit breaker states for all endpoints with active breakers."""
        states = service.get_all_circuit_breaker_states()
        return {"breakers": states, "count": len(states)}

    @app.get("/circuit-breakers/{endpoint_id}")
    async def get_circuit_breaker(endpoint_id: str):
        """Get circuit breaker state for a specific endpoint."""
        state = service.get_circuit_breaker_state(endpoint_id)
        if state is None:
            return {
                "endpoint_id": endpoint_id,
                "state": "closed",
                "message": "No circuit breaker tracked yet",
            }
        return state

    @app.post("/circuit-breakers/{endpoint_id}/reset")
    async def reset_circuit_breaker(endpoint_id: str):
        """Reset (force close) the circuit breaker for an endpoint."""
        result = service.reset_circuit_breaker(endpoint_id)
        if result is None:
            raise HTTPException(status_code=404, detail="No circuit breaker found for endpoint")
        return {"message": "Circuit breaker reset", **result}

    # ── Signature Verification ─────────────────────────────────────

    @app.post("/verify-signature")
    async def verify_signature(req: VerifySignatureRequest):
        """Verify an incoming webhook HMAC signature."""
        result = service.verify_incoming_signature(
            raw_body=req.raw_body,
            headers=req.headers,
            secret=req.secret,
            provider=req.provider,
            algorithm=req.algorithm,
            tolerance_seconds=req.tolerance_seconds,
        )
        return result

    @app.post("/generate-signature")
    async def generate_signature(req: GenerateSignatureRequest):
        """Generate a test HMAC signature for a payload."""
        from .signature import SignatureVerifier
        verifier = SignatureVerifier()
        sig = verifier.generate_signature(
            raw_body=req.raw_body,
            secret=req.secret,
            algorithm=req.algorithm,
            provider=req.provider,
        )
        return {"signature": sig, "provider": req.provider}

    @app.post("/detect-provider")
    async def detect_provider(headers: dict[str, str]):
        """Auto-detect webhook provider from request headers."""
        provider = service.detect_incoming_provider(headers)
        return {"detected_provider": provider}

    # ── Relay Rule Filters ──────────────────────────────────────────

    @app.put("/relay-rules/{rule_id}/filter")
    async def set_relay_filter(rule_id: str, filter_rules: dict):
        """Set filter rules on a relay rule for conditional forwarding."""
        from .filters import validate_filter_rules
        errors = validate_filter_rules(filter_rules)
        if errors:
            raise HTTPException(status_code=422, detail={"valid": False, "errors": errors})
        if not hasattr(service.store, "update_relay_rule"):
            raise HTTPException(status_code=400, detail="Store does not support relay rule updates")
        rule = service.store.update_relay_rule(rule_id, filter_rules=filter_rules)
        if rule is None:
            raise HTTPException(status_code=404, detail="Relay rule not found")
        return {
            "message": "Filter rules set successfully",
            "rule_id": rule_id,
            "filter_rules": rule.filter_rules,
        }

    @app.delete("/relay-rules/{rule_id}/filter")
    async def clear_relay_filter(rule_id: str):
        """Clear filter rules from a relay rule."""
        if not hasattr(service.store, "update_relay_rule"):
            raise HTTPException(status_code=400, detail="Store does not support relay rule updates")
        rule = service.store.update_relay_rule(rule_id, filter_rules=None)
        if rule is None:
            raise HTTPException(status_code=404, detail="Relay rule not found")
        return {"message": "Filter rules cleared", "rule_id": rule_id}

    @app.post("/relay-rules/validate-filter")
    async def validate_filter(filter_rules: dict):
        """Validate filter rules without applying them."""
        from .filters import validate_filter_rules
        errors = validate_filter_rules(filter_rules)
        result = {"valid": len(errors) == 0}
        if errors:
            result["errors"] = errors
        return result

    # ── Import/Export ───────────────────────────────────────────────

    @app.get("/export")
    async def export_config(
        endpoints: bool = True,
        relay_rules: bool = True,
        transforms: bool = True,
        subscriptions: bool = True,
    ):
        """Export all configuration to a portable format."""
        from .import_export import export_config as _export
        return _export(
            service.store,
            include_endpoints=endpoints,
            include_relay_rules=relay_rules,
            include_transforms=transforms,
            include_subscriptions=subscriptions,
        )

    @app.post("/import")
    async def import_config(
        data: dict,
        conflict_strategy: str = "skip",
        restore_secrets: bool = False,
    ):
        """Import configuration from an export format."""
        from .import_export import import_config as _import
        summary = _import(
            service.store,
            data,
            conflict_strategy=conflict_strategy,
            restore_secrets=restore_secrets,
        )
        return summary

    return app
