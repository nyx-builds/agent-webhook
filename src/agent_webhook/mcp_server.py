"""MCP server for agent-webhook — exposes webhook management as MCP tools."""

from __future__ import annotations

import asyncio
import json
from typing import Any

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import TextContent, Tool

from .engine import DeliveryEngine
from .models import (
    DeliveryStatus,
    Header,
    RelayRule,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookMethod,
    WebhookStatus,
)
from .store import WebhookStore


def create_server(store_path: str = "webhook_store.json") -> Server:
    """Create and configure the MCP server."""
    server = Server("agent-webhook")
    store = WebhookStore(store_path)
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
                        "timeout_seconds": {"type": "number", "default": 30.0},
                        "description": {"type": "string", "description": "Optional description"},
                        "max_retries": {"type": "integer", "default": 3},
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
                        "timeout_seconds": {"type": "number"},
                        "description": {"type": "string"},
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
                name="delivery_list",
                description="List webhook deliveries",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string"},
                        "status": {"type": "string", "enum": ["pending", "in_progress", "success", "failed", "retrying", "abandoned"]},
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
                name="process_pending",
                description="Process all pending webhook deliveries",
                inputSchema={"type": "object", "properties": {}},
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
        ]

    @server.call_tool()
    async def call_tool(name: str, arguments: dict[str, Any]) -> list[TextContent]:
        try:
            if name == "endpoint_add":
                headers = [
                    Header(name=k, value=v)
                    for k, v in (arguments.get("headers") or {}).items()
                ]
                endpoint_obj = WebhookEndpoint(
                    name=arguments["name"],
                    url=arguments["url"],
                    method=WebhookMethod(arguments.get("method", "POST")),
                    headers=headers,
                    tags=arguments.get("tags", []),
                    secret=arguments.get("secret"),
                    timeout_seconds=arguments.get("timeout_seconds", 30.0),
                    description=arguments.get("description"),
                    retry_policy={"max_retries": arguments.get("max_retries", 3)},
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
                for key in ["name", "url", "secret", "timeout_seconds", "description"]:
                    if key in arguments:
                        updates[key] = arguments[key]
                if "status" in arguments:
                    updates["status"] = WebhookStatus(arguments["status"])
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
                # Reset status to retrying
                store.update_delivery(d.id, status=DeliveryStatus.PENDING, next_retry_at=None)
                result = await engine.process_delivery(d.id)
                return [TextContent(type="text", text=json.dumps(result.model_dump(mode="json"), default=str, indent=2)) if result else TextContent(type="text", text=f"Failed to retry delivery: {arguments['delivery_id']}")]

            elif name == "process_pending":
                results = await engine.process_pending()
                return [TextContent(type="text", text=json.dumps(
                    [r.model_dump(mode="json") for r in results], default=str, indent=2
                ))]

            elif name == "stats":
                if "endpoint_id" in arguments:
                    s = store.get_stats(arguments["endpoint_id"])
                    if s is None:
                        return [TextContent(type="text", text=f"Endpoint not found: {arguments['endpoint_id']}")]
                else:
                    s = store.get_all_stats()
                return [TextContent(type="text", text=json.dumps(s, default=str, indent=2))]

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
