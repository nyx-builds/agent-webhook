"""MCP server for agent-webhook — exposes webhook management as MCP tools."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from .engine import DeliveryEngine
from .models import (
    DeadLetterEntry,
    DeliveryStatus,
    EventSubscription,
    Header,
    PayloadTransform,
    RateLimit,
    RateLimitPeriod,
    RelayRule,
    SigningAlgorithm,
    TransformType,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookMethod,
    WebhookStatus,
)
from .store import WebhookStore


def _get_sqlite_store(store_path: str):
    """Try to use SQLite store, fall back to JSON store."""
    if store_path.endswith(".json"):
        # Auto-migrate: use .db instead
        db_path = store_path.replace(".json", ".db")
    elif store_path.endswith(".db"):
        db_path = store_path
    else:
        db_path = store_path + ".db"

    try:
        from .store_sqlite import SQLiteStore
        return SQLiteStore(db_path)
    except Exception:
        return WebhookStore(store_path)


def create_server(store_path: str = "webhook_store.json") -> Server:
    """Create and configure the MCP server."""
    server = Server("agent-webhook")
    store = _get_sqlite_store(store_path)
    engine = DeliveryEngine(store)

    @server.list_tools()
    async def list_tools() -> list[Tool]:
        return [
            Tool(
                name="endpoint_add",
                description="Register a new webhook endpoint",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Human-readable name"},
                        "url": {"type": "string", "description": "Target URL"},
                        "method": {"type": "string", "enum": ["POST", "PUT", "PATCH", "GET", "DELETE"], "default": "POST"},
                        "headers": {"type": "object", "description": "Custom headers as key-value pairs"},
                        "tags": {"type": "array", "items": {"type": "string"}, "description": "Tags for filtering"},
                        "secret": {"type": "string", "description": "HMAC signing secret"},
                        "signing_algorithm": {"type": "string", "enum": ["sha1", "sha256", "sha512"], "default": "sha256", "description": "HMAC signing algorithm"},
                        "timeout_seconds": {"type": "number", "default": 30.0},
                        "description": {"type": "string", "description": "Optional description"},
                        "max_retries": {"type": "integer", "default": 3},
                        "initial_delay_seconds": {"type": "number", "default": 1.0},
                        "max_delay_seconds": {"type": "number", "default": 300.0},
                        "backoff_multiplier": {"type": "number", "default": 2.0},
                        "retry_on_status_codes": {"type": "array", "items": {"type": "integer"}, "description": "HTTP status codes that trigger retry"},
                        "transform_ids": {"type": "array", "items": {"type": "string"}, "description": "Transform IDs to apply before delivery"},
                        "rate_limit": {
                            "type": "object",
                            "description": "Rate limiting: {max_requests, period (second/minute/hour), burst}",
                            "properties": {
                                "max_requests": {"type": "integer"},
                                "period": {"type": "string", "enum": ["second", "minute", "hour"]},
                                "burst": {"type": "integer"},
                            },
                            "required": ["max_requests"],
                        },
                    },
                    "required": ["name", "url"],
                },
            ),
            Tool(
                name="endpoint_list",
                description="List all webhook endpoints",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "status": {"type": "string", "enum": ["active", "paused", "disabled"]},
                        "tag": {"type": "string", "description": "Filter by tag"},
                    },
                },
            ),
            Tool(
                name="endpoint_get",
                description="Get details of a webhook endpoint",
                inputSchema={
                    "type": "object",
                    "properties": {"endpoint_id": {"type": "string"}},
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="endpoint_update",
                description="Update a webhook endpoint",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string"},
                        "name": {"type": "string"},
                        "url": {"type": "string"},
                        "status": {"type": "string", "enum": ["active", "paused", "disabled"]},
                        "secret": {"type": "string"},
                        "signing_algorithm": {"type": "string", "enum": ["sha1", "sha256", "sha512"], "description": "HMAC signing algorithm"},
                        "timeout_seconds": {"type": "number"},
                        "description": {"type": "string"},
                        "transform_ids": {"type": "array", "items": {"type": "string"}},
                        "rate_limit": {
                            "type": "object",
                            "properties": {
                                "max_requests": {"type": "integer"},
                                "period": {"type": "string", "enum": ["second", "minute", "hour"]},
                                "burst": {"type": "integer"},
                            },
                        },
                    },
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="endpoint_delete",
                description="Delete a webhook endpoint",
                inputSchema={
                    "type": "object",
                    "properties": {"endpoint_id": {"type": "string"}},
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="webhook_send",
                description="Send a webhook delivery to an endpoint",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Target endpoint ID"},
                        "payload": {"type": "object", "description": "JSON payload to deliver"},
                        "event_type": {"type": "string", "description": "Event type tag"},
                        "headers": {"type": "object", "description": "Extra headers for this delivery"},
                        "metadata": {"type": "object", "description": "Extra metadata"},
                    },
                    "required": ["endpoint_id", "payload"],
                },
            ),
            Tool(
                name="webhook_batch_send",
                description="Send the same payload to multiple endpoints at once",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_ids": {"type": "array", "items": {"type": "string"}, "description": "Target endpoint IDs"},
                        "payload": {"type": "object", "description": "JSON payload to deliver"},
                        "event_type": {"type": "string", "description": "Event type tag"},
                        "headers": {"type": "object", "description": "Extra headers"},
                        "metadata": {"type": "object", "description": "Extra metadata"},
                    },
                    "required": ["endpoint_ids", "payload"],
                },
            ),
            Tool(
                name="delivery_list",
                description="List webhook deliveries",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string"},
                        "status": {"type": "string", "enum": ["pending", "in_progress", "success", "failed", "retrying", "abandoned", "dead_letter"]},
                        "event_type": {"type": "string"},
                        "limit": {"type": "integer", "default": 50},
                    },
                },
            ),
            Tool(
                name="delivery_get",
                description="Get delivery details including attempts",
                inputSchema={
                    "type": "object",
                    "properties": {"delivery_id": {"type": "string"}},
                    "required": ["delivery_id"],
                },
            ),
            Tool(
                name="delivery_retry",
                description="Retry a failed delivery",
                inputSchema={
                    "type": "object",
                    "properties": {"delivery_id": {"type": "string"}},
                    "required": ["delivery_id"],
                },
            ),
            Tool(
                name="delivery_cancel",
                description="Cancel a pending or retrying delivery",
                inputSchema={
                    "type": "object",
                    "properties": {"delivery_id": {"type": "string"}},
                    "required": ["delivery_id"],
                },
            ),
            Tool(
                name="process_pending",
                description="Process all pending webhook deliveries",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="health_check",
                description="Test endpoint connectivity by sending a health check payload",
                inputSchema={
                    "type": "object",
                    "properties": {"endpoint_id": {"type": "string"}},
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="stats",
                description="Get delivery statistics for an endpoint or all endpoints",
                inputSchema={
                    "type": "object",
                    "properties": {"endpoint_id": {"type": "string", "description": "Optional endpoint ID (omit for all)"}},
                },
            ),
            Tool(
                name="subscription_add",
                description="Subscribe an endpoint to specific event types",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID"},
                        "event_types": {"type": "array", "items": {"type": "string"}, "description": "Event types to subscribe to"},
                    },
                    "required": ["endpoint_id", "event_types"],
                },
            ),
            Tool(
                name="subscription_list",
                description="List event subscriptions",
                inputSchema={
                    "type": "object",
                    "properties": {"endpoint_id": {"type": "string", "description": "Filter by endpoint ID"}},
                },
            ),
            Tool(
                name="subscription_delete",
                description="Delete an event subscription",
                inputSchema={
                    "type": "object",
                    "properties": {"subscription_id": {"type": "string"}},
                    "required": ["subscription_id"],
                },
            ),
            Tool(
                name="send_to_subscribers",
                description="Send a payload to all endpoints subscribed to an event type",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "event_type": {"type": "string", "description": "Event type to match"},
                        "payload": {"type": "object", "description": "JSON payload to deliver"},
                        "metadata": {"type": "object", "description": "Extra metadata"},
                        "headers": {"type": "object", "description": "Extra headers"},
                    },
                    "required": ["event_type", "payload"],
                },
            ),
            Tool(
                name="relay_add",
                description="Add a relay rule to forward incoming webhooks to endpoints",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Rule name"},
                        "path_pattern": {"type": "string", "description": "URL path pattern (supports * wildcard, e.g. /stripe/*)"},
                        "target_endpoint_ids": {"type": "array", "items": {"type": "string"}, "description": "Endpoint IDs to forward to"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["name", "path_pattern", "target_endpoint_ids"],
                },
            ),
            Tool(
                name="relay_list",
                description="List all relay rules",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="relay_delete",
                description="Delete a relay rule",
                inputSchema={
                    "type": "object",
                    "properties": {"rule_id": {"type": "string"}},
                    "required": ["rule_id"],
                },
            ),
            Tool(
                name="incoming_list",
                description="List incoming webhooks received by the relay",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "limit": {"type": "integer", "default": 50},
                    },
                },
            ),
            Tool(
                name="incoming_receive",
                description="Receive an incoming webhook and apply relay rules",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "path": {"type": "string", "description": "URL path"},
                        "method": {"type": "string", "default": "POST"},
                        "headers": {"type": "object"},
                        "body": {"description": "Request body (object or string)"},
                        "source_ip": {"type": "string"},
                    },
                    "required": ["path"],
                },
            ),
            Tool(
                name="event_log",
                description="List event log entries for audit trail",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "event_type": {"type": "string", "description": "Filter by event type"},
                        "endpoint_id": {"type": "string", "description": "Filter by endpoint ID"},
                        "limit": {"type": "integer", "default": 50},
                    },
                },
            ),
            # ── v0.3.0 New Tools ──────────────────────────────────────
            Tool(
                name="transform_create",
                description="Create a payload transformation. Types: field_map (rename keys), filter (include/exclude keys), template (string template with {{payload.key}} substitution)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Transform name"},
                        "type": {"type": "string", "enum": ["field_map", "filter", "template"], "description": "Transform type"},
                        "config": {"type": "object", "description": "Transform config. field_map: {'mapping': {'old': 'new'}, 'keep_unmapped': true}. filter: {'include': ['key1']} or {'exclude': ['key1']}. template: {'fields': {'new_key': '{{payload.old_key}}'}}"},
                    },
                    "required": ["name", "type", "config"],
                },
            ),
            Tool(
                name="transform_list",
                description="List all payload transforms",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "type": {"type": "string", "enum": ["field_map", "filter", "template"], "description": "Filter by type"},
                    },
                },
            ),
            Tool(
                name="transform_get",
                description="Get details of a payload transform",
                inputSchema={
                    "type": "object",
                    "properties": {"transform_id": {"type": "string"}},
                    "required": ["transform_id"],
                },
            ),
            Tool(
                name="transform_delete",
                description="Delete a payload transform",
                inputSchema={
                    "type": "object",
                    "properties": {"transform_id": {"type": "string"}},
                    "required": ["transform_id"],
                },
            ),
            Tool(
                name="dead_letter_list",
                description="List entries in the dead letter queue (permanently failed deliveries)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Filter by endpoint ID"},
                        "replayed": {"type": "boolean", "description": "Filter by replayed status"},
                        "limit": {"type": "integer", "default": 50},
                    },
                },
            ),
            Tool(
                name="dead_letter_get",
                description="Get details of a dead letter entry",
                inputSchema={
                    "type": "object",
                    "properties": {"entry_id": {"type": "string"}},
                    "required": ["entry_id"],
                },
            ),
            Tool(
                name="dead_letter_replay",
                description="Replay a dead letter entry (create a new delivery attempt)",
                inputSchema={
                    "type": "object",
                    "properties": {"entry_id": {"type": "string"}},
                    "required": ["entry_id"],
                },
            ),
            Tool(
                name="dead_letter_delete",
                description="Delete a dead letter entry",
                inputSchema={
                    "type": "object",
                    "properties": {"entry_id": {"type": "string"}},
                    "required": ["entry_id"],
                },
            ),
            Tool(
                name="rate_limit_status",
                description="Get rate limit status for an endpoint",
                inputSchema={
                    "type": "object",
                    "properties": {"endpoint_id": {"type": "string"}},
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="migrate_json_to_sqlite",
                description="Migrate data from a JSON store file to SQLite",
                inputSchema={
                    "type": "object",
                    "properties": {"json_path": {"type": "string", "description": "Path to the JSON store file"}},
                    "required": ["json_path"],
                },
            ),
            # ── v0.4.0 New Tools ──────────────────────────────────
            Tool(
                name="relay_update",
                description="Update a relay rule (name, path_pattern, target_endpoint_ids, active, tags)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "rule_id": {"type": "string", "description": "Relay rule ID"},
                        "name": {"type": "string", "description": "New name"},
                        "path_pattern": {"type": "string", "description": "New path pattern"},
                        "target_endpoint_ids": {"type": "array", "items": {"type": "string"}, "description": "New target endpoint IDs"},
                        "active": {"type": "boolean", "description": "Enable/disable rule"},
                        "tags": {"type": "array", "items": {"type": "string"}, "description": "New tags"},
                    },
                    "required": ["rule_id"],
                },
            ),
            Tool(
                name="dead_letter_batch_replay",
                description="Replay all unreplayed dead letter entries, optionally filtered by endpoint",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Optional endpoint ID to filter"},
                    },
                },
            ),
            Tool(
                name="transform_update",
                description="Update a payload transform",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "transform_id": {"type": "string", "description": "Transform ID"},
                        "name": {"type": "string", "description": "New name"},
                        "config": {"type": "object", "description": "New config"},
                    },
                    "required": ["transform_id"],
                },
            ),
            Tool(
                name="metrics",
                description="Get webhook delivery metrics (counts, durations, rate limits)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "format": {"type": "string", "enum": ["json", "prometheus"], "default": "json", "description": "Output format"},
                    },
                },
            ),
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        try:
            if name == "endpoint_add":
                headers = [
                    Header(name=k, value=v)
                    for k, v in (arguments.get("headers") or {}).items()
                ]
                retry_policy_kwargs: dict[str, Any] = {
                    "max_retries": arguments.get("max_retries", 3),
                    "initial_delay_seconds": arguments.get("initial_delay_seconds", 1.0),
                    "max_delay_seconds": arguments.get("max_delay_seconds", 300.0),
                    "backoff_multiplier": arguments.get("backoff_multiplier", 2.0),
                }
                if "retry_on_status_codes" in arguments:
                    retry_policy_kwargs["retry_on_status_codes"] = arguments["retry_on_status_codes"]

                rate_limit_obj = None
                if "rate_limit" in arguments and arguments["rate_limit"]:
                    rl = arguments["rate_limit"]
                    rate_limit_obj = RateLimit(
                        max_requests=rl["max_requests"],
                        period=RateLimitPeriod(rl.get("period", "minute")),
                        burst=rl.get("burst", 0),
                    )

                signing_algo = SigningAlgorithm(arguments.get("signing_algorithm", "sha256"))

                endpoint_obj = WebhookEndpoint(
                    name=arguments["name"],
                    url=arguments["url"],
                    method=WebhookMethod(arguments.get("method", "POST")),
                    headers=headers,
                    tags=arguments.get("tags", []),
                    secret=arguments.get("secret"),
                    signing_algorithm=signing_algo,
                    timeout_seconds=arguments.get("timeout_seconds", 30.0),
                    description=arguments.get("description"),
                    retry_policy=retry_policy_kwargs,
                    transform_ids=arguments.get("transform_ids", []),
                    rate_limit=rate_limit_obj,
                )
                store.add_endpoint(endpoint_obj)
                return [TextContent(type="text", text=json.dumps(endpoint_obj.model_dump(mode="json"), default=str, indent=2))]

            elif name == "endpoint_list":
                ws = WebhookStatus(arguments["status"]) if "status" in arguments else None
                tag = arguments.get("tag")
                endpoints = store.list_endpoints(status=ws, tag=tag)
                return [TextContent(type="text", text=json.dumps(
                    [e.model_dump(mode="json") for e in endpoints], default=str, indent=2
                ))]

            elif name == "endpoint_get":
                ep = store.get_endpoint(arguments["endpoint_id"])
                if ep is None:
                    return [TextContent(type="text", text=f"Endpoint not found: {arguments['endpoint_id']}")]
                return [TextContent(type="text", text=json.dumps(ep.model_dump(mode="json"), default=str, indent=2))]

            elif name == "endpoint_update":
                updates = {}
                for key in ["name", "url", "secret", "timeout_seconds", "description", "transform_ids"]:
                    if key in arguments:
                        updates[key] = arguments[key]
                if "status" in arguments:
                    updates["status"] = WebhookStatus(arguments["status"])
                if "signing_algorithm" in arguments:
                    updates["signing_algorithm"] = SigningAlgorithm(arguments["signing_algorithm"])
                if "rate_limit" in arguments:
                    rl = arguments["rate_limit"]
                    updates["rate_limit"] = RateLimit(
                        max_requests=rl["max_requests"],
                        period=RateLimitPeriod(rl.get("period", "minute")),
                        burst=rl.get("burst", 0),
                    )
                ep = store.update_endpoint(arguments["endpoint_id"], **updates)
                if ep is None:
                    return [TextContent(type="text", text=f"Endpoint not found: {arguments['endpoint_id']}")]
                return [TextContent(type="text", text=json.dumps(ep.model_dump(mode="json"), default=str, indent=2))]

            elif name == "endpoint_delete":
                deleted = store.delete_endpoint(arguments["endpoint_id"])
                return [TextContent(type="text", text=f"Endpoint deleted: {arguments['endpoint_id']}" if deleted else f"Endpoint not found: {arguments['endpoint_id']}")]

            elif name == "webhook_send":
                result = await engine.send(
                    endpoint_id=arguments["endpoint_id"],
                    payload=arguments["payload"],
                    event_type=arguments.get("event_type"),
                    metadata=arguments.get("metadata", {}),
                    headers=arguments.get("headers", {}),
                )
                return [TextContent(type="text", text=json.dumps(result.model_dump(mode="json"), default=str, indent=2))]

            elif name == "webhook_batch_send":
                endpoint_ids = arguments["endpoint_ids"]
                payload = arguments["payload"]
                event_type = arguments.get("event_type")
                headers = arguments.get("headers", {})
                metadata = arguments.get("metadata", {})
                results = []
                for eid in endpoint_ids:
                    result = await engine.send(
                        endpoint_id=eid,
                        payload=payload,
                        event_type=event_type,
                        metadata=metadata,
                        headers=headers,
                    )
                    results.append(result)
                summary = {
                    "total": len(results),
                    "success": sum(1 for r in results if r.status == DeliveryStatus.SUCCESS),
                    "failed": sum(1 for r in results if r.status in (DeliveryStatus.FAILED, DeliveryStatus.ABANDONED)),
                    "retrying": sum(1 for r in results if r.status == DeliveryStatus.RETRYING),
                    "deliveries": [{"id": r.id, "endpoint_id": r.endpoint_id, "status": r.status.value} for r in results],
                }
                return [TextContent(type="text", text=json.dumps(summary, default=str, indent=2))]

            elif name == "delivery_list":
                ds = DeliveryStatus(arguments["status"]) if "status" in arguments else None
                deliveries = store.list_deliveries(
                    endpoint_id=arguments.get("endpoint_id"),
                    status=ds,
                    event_type=arguments.get("event_type"),
                    limit=arguments.get("limit", 50),
                )
                return [TextContent(type="text", text=json.dumps(
                    [d.model_dump(mode="json") for d in deliveries], default=str, indent=2
                ))]

            elif name == "delivery_get":
                d = store.get_delivery(arguments["delivery_id"])
                if d is None:
                    return [TextContent(type="text", text=f"Delivery not found: {arguments['delivery_id']}")]
                return [TextContent(type="text", text=json.dumps(d.model_dump(mode="json"), default=str, indent=2))]

            elif name == "delivery_retry":
                d = store.get_delivery(arguments["delivery_id"])
                if d is None:
                    return [TextContent(type="text", text=f"Delivery not found: {arguments['delivery_id']}")]
                store.update_delivery(d.id, status=DeliveryStatus.PENDING, next_retry_at=None)
                result = await engine.process_delivery(d.id)
                return [TextContent(type="text", text=json.dumps(result.model_dump(mode="json"), default=str, indent=2)) if result else TextContent(type="text", text=f"Failed to retry delivery: {arguments['delivery_id']}")]

            elif name == "delivery_cancel":
                d = store.get_delivery(arguments["delivery_id"])
                if d is None:
                    return [TextContent(type="text", text=f"Delivery not found: {arguments['delivery_id']}")]
                if d.status in (DeliveryStatus.PENDING, DeliveryStatus.RETRYING):
                    store.update_delivery(d.id, status=DeliveryStatus.ABANDONED)
                    return [TextContent(type="text", text=f"Delivery cancelled: {arguments['delivery_id']}")]
                else:
                    return [TextContent(type="text", text=f"Cannot cancel delivery with status: {d.status.value}")]

            elif name == "process_pending":
                results = await engine.process_pending()
                return [TextContent(type="text", text=json.dumps(
                    [r.model_dump(mode="json") for r in results], default=str, indent=2
                ))]

            elif name == "health_check":
                ep = store.get_endpoint(arguments["endpoint_id"])
                if ep is None:
                    return [TextContent(type="text", text=f"Endpoint not found: {arguments['endpoint_id']}")]
                test_payload = {
                    "ping": True,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                    "agent_webhook_health_check": True,
                }
                delivery = WebhookDelivery(
                    endpoint_id=arguments["endpoint_id"],
                    payload=test_payload,
                    event_type="health_check",
                    metadata={"health_check": True},
                )
                store.add_delivery(delivery)
                attempt = await engine.deliver(delivery)
                store.add_delivery_attempt(delivery.id, attempt)
                if attempt.status == DeliveryStatus.SUCCESS:
                    store.update_delivery(delivery.id, status=DeliveryStatus.SUCCESS)
                else:
                    store.update_delivery(delivery.id, status=DeliveryStatus.ABANDONED)
                result = {
                    "endpoint_id": arguments["endpoint_id"],
                    "endpoint_name": ep.name,
                    "url": ep.url,
                    "healthy": attempt.status == DeliveryStatus.SUCCESS,
                    "status_code": attempt.response_status_code,
                    "duration_ms": attempt.duration_ms,
                    "error": attempt.error_message,
                    "timestamp": datetime.now(timezone.utc).isoformat(),
                }
                return [TextContent(type="text", text=json.dumps(result, default=str, indent=2))]

            elif name == "stats":
                if "endpoint_id" in arguments:
                    s = store.get_stats(arguments["endpoint_id"])
                    if s is None:
                        return [TextContent(type="text", text=f"Endpoint not found: {arguments['endpoint_id']}")]
                else:
                    s = store.get_all_stats()
                return [TextContent(type="text", text=json.dumps(s, default=str, indent=2))]

            elif name == "subscription_add":
                ep = store.get_endpoint(arguments["endpoint_id"])
                if ep is None:
                    return [TextContent(type="text", text=f"Endpoint not found: {arguments['endpoint_id']}")]
                sub = EventSubscription(
                    endpoint_id=arguments["endpoint_id"],
                    event_types=arguments["event_types"],
                )
                store.add_subscription(sub)
                return [TextContent(type="text", text=json.dumps(sub.model_dump(mode="json"), default=str, indent=2))]

            elif name == "subscription_list":
                subs = store.list_subscriptions(endpoint_id=arguments.get("endpoint_id"))
                return [TextContent(type="text", text=json.dumps(
                    [s.model_dump(mode="json") for s in subs], default=str, indent=2
                ))]

            elif name == "subscription_delete":
                deleted = store.delete_subscription(arguments["subscription_id"])
                return [TextContent(type="text", text=f"Subscription deleted: {arguments['subscription_id']}" if deleted else f"Subscription not found: {arguments['subscription_id']}")]

            elif name == "send_to_subscribers":
                event_type = arguments["event_type"]
                payload = arguments["payload"]
                metadata = arguments.get("metadata", {})
                headers = arguments.get("headers", {})
                subs = store.list_subscriptions()
                matching_endpoint_ids = []
                for sub in subs:
                    if event_type in sub.event_types:
                        ep = store.get_endpoint(sub.endpoint_id)
                        if ep and ep.is_active():
                            matching_endpoint_ids.append(sub.endpoint_id)
                if not matching_endpoint_ids:
                    return [TextContent(type="text", text=json.dumps({"message": f"No active subscribers for event type: {event_type}", "delivered": 0}))]
                results = []
                for eid in matching_endpoint_ids:
                    result = await engine.send(
                        endpoint_id=eid,
                        payload=payload,
                        event_type=event_type,
                        metadata=metadata,
                        headers=headers,
                    )
                    results.append(result)
                summary = {
                    "event_type": event_type,
                    "subscribers": len(matching_endpoint_ids),
                    "success": sum(1 for r in results if r.status == DeliveryStatus.SUCCESS),
                    "failed": sum(1 for r in results if r.status in (DeliveryStatus.FAILED, DeliveryStatus.ABANDONED)),
                    "deliveries": [{"id": r.id, "endpoint_id": r.endpoint_id, "status": r.status.value} for r in results],
                }
                return [TextContent(type="text", text=json.dumps(summary, default=str, indent=2))]

            elif name == "relay_add":
                rule = RelayRule(
                    name=arguments["name"],
                    path_pattern=arguments["path_pattern"],
                    target_endpoint_ids=arguments["target_endpoint_ids"],
                    tags=arguments.get("tags", []),
                )
                store.add_relay_rule(rule)
                return [TextContent(type="text", text=json.dumps(rule.model_dump(mode="json"), default=str, indent=2))]

            elif name == "relay_list":
                rules = store.list_relay_rules()
                return [TextContent(type="text", text=json.dumps(
                    [r.model_dump(mode="json") for r in rules], default=str, indent=2
                ))]

            elif name == "relay_delete":
                deleted = store.delete_relay_rule(arguments["rule_id"])
                return [TextContent(type="text", text=f"Relay rule deleted: {arguments['rule_id']}" if deleted else f"Rule not found: {arguments['rule_id']}")]

            elif name == "incoming_list":
                incoming = store.list_incoming(
                    path=arguments.get("path"),
                    limit=arguments.get("limit", 50),
                )
                return [TextContent(type="text", text=json.dumps(
                    [i.model_dump(mode="json") for i in incoming], default=str, indent=2
                ))]

            elif name == "incoming_receive":
                body = arguments.get("body")
                delivery_ids = engine.apply_relay_rules(
                    path=arguments["path"],
                    method=arguments.get("method", "POST"),
                    headers=arguments.get("headers", {}),
                    body=body,
                    source_ip=arguments.get("source_ip"),
                )
                result = {"forwarded_deliveries": delivery_ids, "count": len(delivery_ids)}
                return [TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "event_log":
                entries = store.list_event_log(
                    event_type=arguments.get("event_type"),
                    endpoint_id=arguments.get("endpoint_id"),
                    limit=arguments.get("limit", 50),
                )
                return [TextContent(type="text", text=json.dumps(
                    [e.model_dump(mode="json") for e in entries], default=str, indent=2
                ))]

            # ── v0.3.0 New Tool Handlers ──────────────────────────────

            elif name == "transform_create":
                if not hasattr(store, "add_transform"):
                    return [TextContent(type="text", text="Error: Transforms require SQLite store. Use .db file extension.")]
                transform = PayloadTransform(
                    name=arguments["name"],
                    type=TransformType(arguments["type"]),
                    config=arguments["config"],
                )
                store.add_transform(transform)
                return [TextContent(type="text", text=json.dumps(transform.model_dump(mode="json"), default=str, indent=2))]

            elif name == "transform_list":
                if not hasattr(store, "list_transforms"):
                    return [TextContent(type="text", text="[]")]
                transforms = store.list_transforms(type=arguments.get("type"))
                return [TextContent(type="text", text=json.dumps(
                    [t.model_dump(mode="json") for t in transforms], default=str, indent=2
                ))]

            elif name == "transform_get":
                if not hasattr(store, "get_transform"):
                    return [TextContent(type="text", text="Transforms require SQLite store")]
                t = store.get_transform(arguments["transform_id"])
                if t is None:
                    return [TextContent(type="text", text=f"Transform not found: {arguments['transform_id']}")]
                return [TextContent(type="text", text=json.dumps(t.model_dump(mode="json"), default=str, indent=2))]

            elif name == "transform_delete":
                if not hasattr(store, "delete_transform"):
                    return [TextContent(type="text", text="Transforms require SQLite store")]
                deleted = store.delete_transform(arguments["transform_id"])
                return [TextContent(type="text", text=f"Transform deleted: {arguments['transform_id']}" if deleted else f"Transform not found: {arguments['transform_id']}")]

            elif name == "dead_letter_list":
                if not hasattr(store, "list_dead_letter"):
                    return [TextContent(type="text", text="[]")]
                entries = store.list_dead_letter(
                    endpoint_id=arguments.get("endpoint_id"),
                    replayed=arguments.get("replayed"),
                    limit=arguments.get("limit", 50),
                )
                return [TextContent(type="text", text=json.dumps(
                    [e.model_dump(mode="json") for e in entries], default=str, indent=2
                ))]

            elif name == "dead_letter_get":
                if not hasattr(store, "get_dead_letter"):
                    return [TextContent(type="text", text="Dead letter queue requires SQLite store")]
                entry = store.get_dead_letter(arguments["entry_id"])
                if entry is None:
                    return [TextContent(type="text", text=f"Dead letter entry not found: {arguments['entry_id']}")]
                return [TextContent(type="text", text=json.dumps(entry.model_dump(mode="json"), default=str, indent=2))]

            elif name == "dead_letter_replay":
                if not hasattr(store, "get_dead_letter"):
                    return [TextContent(type="text", text="Dead letter queue requires SQLite store")]
                entry = store.get_dead_letter(arguments["entry_id"])
                if entry is None:
                    return [TextContent(type="text", text=f"Dead letter entry not found: {arguments['entry_id']}")]
                if entry.replayed:
                    return [TextContent(type="text", text=f"Entry already replayed (delivery: {entry.replayed_delivery_id})")]
                # Create new delivery
                result = await engine.send(
                    endpoint_id=entry.endpoint_id,
                    payload=entry.payload,
                    event_type=entry.event_type,
                    metadata={"replayed_from_dlq": entry.id, "original_delivery_id": entry.delivery_id},
                )
                store.update_dead_letter(
                    arguments["entry_id"],
                    replayed=True,
                    replayed_delivery_id=result.id,
                    replayed_at=datetime.now(timezone.utc),
                )
                return [TextContent(type="text", text=json.dumps({
                    "message": "Dead letter entry replayed",
                    "entry_id": arguments["entry_id"],
                    "new_delivery_id": result.id,
                    "status": result.status.value,
                }, default=str, indent=2))]

            elif name == "dead_letter_delete":
                if not hasattr(store, "delete_dead_letter"):
                    return [TextContent(type="text", text="Dead letter queue requires SQLite store")]
                deleted = store.delete_dead_letter(arguments["entry_id"])
                return [TextContent(type="text", text=f"Dead letter entry deleted: {arguments['entry_id']}" if deleted else f"Entry not found: {arguments['entry_id']}")]

            elif name == "rate_limit_status":
                ep = store.get_endpoint(arguments["endpoint_id"])
                if ep is None:
                    return [TextContent(type="text", text=f"Endpoint not found: {arguments['endpoint_id']}")]
                if ep.rate_limit is None:
                    return [TextContent(type="text", text=json.dumps({"endpoint_id": arguments["endpoint_id"], "rate_limit": None}))]
                status = engine._rate_limiter.get_status(arguments["endpoint_id"], ep.rate_limit)
                return [TextContent(type="text", text=json.dumps(status, default=str, indent=2))]

            elif name == "migrate_json_to_sqlite":
                if not hasattr(store, "migrate_from_json"):
                    return [TextContent(type="text", text="Migration requires SQLite store")]
                counts = store.migrate_from_json(arguments["json_path"])
                return [TextContent(type="text", text=json.dumps({"migrated": counts}, indent=2))]

            # ── v0.4.0 New Tool Handlers ─────────────────────────

            elif name == "relay_update":
                rule_id = arguments["rule_id"]
                updates = {}
                for key in ["name", "path_pattern", "target_endpoint_ids", "active", "tags"]:
                    if key in arguments:
                        updates[key] = arguments[key]
                if hasattr(store, "update_relay_rule"):
                    rule = store.update_relay_rule(rule_id, **updates)
                else:
                    return [TextContent(type="text", text="Relay rule update not supported by this store")]
                if rule is None:
                    return [TextContent(type="text", text=f"Relay rule not found: {rule_id}")]
                return [TextContent(type="text", text=json.dumps(rule.model_dump(mode="json"), default=str, indent=2))]

            elif name == "dead_letter_batch_replay":
                endpoint_id = arguments.get("endpoint_id")
                # Get all unreplayed DLQ entries
                if hasattr(store, "list_dead_letter"):
                    entries = store.list_dead_letter(endpoint_id=endpoint_id, replayed=False, limit=1000)
                else:
                    return [TextContent(type="text", text="Dead letter queue not supported by this store")]
                results = []
                for entry in entries:
                    try:
                        if entry.replayed:
                            continue
                        # Create new delivery from the dead letter entry
                        result = await engine.send(
                            endpoint_id=entry.endpoint_id,
                            payload=entry.payload,
                            event_type=entry.event_type,
                            metadata={"replayed_from_dlq": entry.id, "original_delivery_id": entry.delivery_id},
                        )
                        store.update_dead_letter(
                            entry.id,
                            replayed=True,
                            replayed_delivery_id=result.id,
                            replayed_at=datetime.now(timezone.utc),
                        )
                        results.append(result)
                    except Exception:
                        pass
                return [TextContent(type="text", text=json.dumps({
                    "replayed": len(results),
                    "results": [{"id": r.id, "endpoint_id": r.endpoint_id, "status": r.status.value} for r in results],
                }, default=str, indent=2))]

            elif name == "transform_update":
                transform_id = arguments["transform_id"]
                updates = {}
                for key in ["name", "config"]:
                    if key in arguments:
                        updates[key] = arguments[key]
                if hasattr(store, "update_transform"):
                    t = store.update_transform(transform_id, **updates)
                else:
                    return [TextContent(type="text", text="Transform update not supported by this store")]
                if t is None:
                    return [TextContent(type="text", text=f"Transform not found: {transform_id}")]
                return [TextContent(type="text", text=json.dumps(t.model_dump(mode="json"), default=str, indent=2))]

            elif name == "metrics":
                fmt = arguments.get("format", "json")
                from .metrics import get_metrics
                m = get_metrics()
                if fmt == "prometheus":
                    text = m.generate_prometheus()
                    return [TextContent(type="text", text=text)]
                else:
                    data = m.get_json()
                    return [TextContent(type="text", text=json.dumps(data, default=str, indent=2))]

            else:
                return [TextContent(type="text", text=f"Unknown tool: {name}")]

        except Exception as e:
            return [TextContent(type="text", text=f"Error: {type(e).__name__}: {e}")]

    return server


async def main() -> None:
    """Run the MCP server via stdio."""
    server = create_server()
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())

if __name__ == "__main__":
    asyncio.run(main())
