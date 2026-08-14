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


def _parse_dt(val: str | None) -> datetime | None:
    """Parse an ISO 8601 datetime string to a timezone-aware datetime."""
    if val is None:
        return None
    try:
        dt = datetime.fromisoformat(val)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


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
                        "jitter": {
                            "type": "string",
                            "enum": ["none", "full", "equal", "decorrelated"],
                            "default": "full",
                            "description": "Jitter strategy for retry backoff. full=random(0,delay), equal=half+random(0,half), decorrelated=uses prev delay",
                        },
                        "timestamped_signatures": {
                            "type": "boolean",
                            "default": false,
                            "description": "Include timestamp in HMAC signature for replay protection (t=<ts>,v1=<hmac> format)",
                        },
                        "idempotency_keys": {
                            "type": "boolean",
                            "default": false,
                            "description": "Auto-generate Idempotency-Key header (UUID) for each delivery",
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
                        "idempotency_key": {"type": "string", "description": "Idempotency key for safe deduplication (sent as Idempotency-Key header)"},
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
            # ── v0.5.0 New Tools ──────────────────────────────────
            Tool(
                name="circuit_breaker_state",
                description="Get circuit breaker state for an endpoint (closed, open, half_open)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID"},
                    },
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="circuit_breaker_all",
                description="Get circuit breaker states for all endpoints with active breakers",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="circuit_breaker_reset",
                description="Reset (force close) the circuit breaker for an endpoint",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID"},
                    },
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="verify_signature",
                description="Verify an incoming webhook HMAC signature. Supports generic, github, stripe, slack, shopify providers.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "raw_body": {"type": "string", "description": "Raw request body (the exact bytes received)"},
                        "headers": {"type": "object", "description": "Request headers"},
                        "secret": {"type": "string", "description": "Shared secret for HMAC verification"},
                        "provider": {"type": "string", "enum": ["generic", "github", "stripe", "slack", "shopify"], "default": "generic"},
                        "algorithm": {"type": "string", "enum": ["sha256", "sha1"], "default": "sha256", "description": "Algorithm for generic provider"},
                        "tolerance_seconds": {"type": "integer", "default": 300, "description": "Max timestamp age for replay prevention"},
                    },
                    "required": ["raw_body", "headers", "secret"],
                },
            ),
            Tool(
                name="detect_provider",
                description="Auto-detect the webhook provider from request headers",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "headers": {"type": "object", "description": "Request headers"},
                    },
                    "required": ["headers"],
                },
            ),
            Tool(
                name="generate_signature",
                description="Generate a test HMAC signature for a payload (useful for testing relay verification)",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "raw_body": {"type": "string", "description": "Raw body to sign"},
                        "secret": {"type": "string", "description": "HMAC secret"},
                        "provider": {"type": "string", "enum": ["generic", "github", "stripe", "slack", "shopify"], "default": "generic"},
                        "algorithm": {"type": "string", "enum": ["sha256", "sha1"], "default": "sha256"},
                    },
                    "required": ["raw_body", "secret"],
                },
            ),
            # ── v0.5.0: Relay Rule Filters ────────────────────────
            Tool(
                name="relay_set_filter",
                description="Set filter rules on a relay rule. Filters allow conditional forwarding based on headers and payload fields. Operators: equals, not_equals, contains, starts_with, ends_with, regex, exists, not_exists (string); eq, ne, gt, gte, lt, lte (numeric); in, not_in (list). Logic: all (AND), any (OR), none (NOT any).",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "rule_id": {"type": "string", "description": "Relay rule ID"},
                        "filter_rules": {
                            "type": "object",
                            "description": "Filter configuration: {logic: 'all'|'any'|'none', conditions: [{type: 'header'|'payload', field: 'X', operator: 'equals', value: 'Y'}]}",
                        },
                    },
                    "required": ["rule_id"],
                },
            ),
            Tool(
                name="relay_validate_filter",
                description="Validate relay rule filter rules. Returns any validation errors.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "filter_rules": {"type": "object", "description": "Filter configuration to validate"},
                    },
                    "required": ["filter_rules"],
                },
            ),
            # ── v0.5.0: Import/Export ──────────────────────────────
            Tool(
                name="export_config",
                description="Export all configuration (endpoints, relay rules, transforms, subscriptions) to a portable format. Secrets are excluded by default.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "include_endpoints": {"type": "boolean", "default": True},
                        "include_relay_rules": {"type": "boolean", "default": True},
                        "include_transforms": {"type": "boolean", "default": True},
                        "include_subscriptions": {"type": "boolean", "default": True},
                    },
                },
            ),
            Tool(
                name="import_config",
                description="Import configuration from a previously exported format. Supports conflict strategies: skip (default), overwrite, rename.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "data": {"type": "object", "description": "Export data to import"},
                        "conflict_strategy": {"type": "string", "enum": ["skip", "overwrite", "rename"], "default": "skip"},
                        "restore_secrets": {"type": "boolean", "default": False, "description": "Restore HMAC secrets from export (default false for security)"},
                    },
                    "required": ["data"],
                },
            ),
            Tool(
                name="analytics_overview",
                description="Get comprehensive delivery analytics: success rates, latency percentiles (p50/p90/p95/p99), error breakdown, top/worst endpoints, hourly trends, and a health score (0-100).",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "default": 10000, "description": "Max deliveries to analyse"},
                    },
                },
            ),
            Tool(
                name="analytics_endpoint",
                description="Get delivery analytics for a single endpoint: success rate, latency percentiles, error breakdown, trend, and health score.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID to analyse"},
                    },
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="analytics_retry",
                description="Analyse retry patterns: how often retries are needed, retry success rate, dead letter rate, and attempt distribution.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "limit": {"type": "integer", "default": 10000},
                    },
                },
            ),
            Tool(
                name="template_list",
                description="List pre-built endpoint templates for popular services (Slack, Discord, Teams, Telegram, PagerDuty, etc.). Filter by tag.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "tag": {"type": "string", "description": "Filter by tag (e.g. messaging, automation, notifications)"},
                    },
                },
            ),
            Tool(
                name="template_get",
                description="Get details for a specific endpoint template by key (e.g. slack, discord, teams).",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "key": {"type": "string", "description": "Template key (e.g. slack, discord)"},
                    },
                    "required": ["key"],
                },
            ),
            Tool(
                name="endpoint_from_template",
                description="Create a webhook endpoint from a pre-built template. Requires template key and URL. Optional: custom name, secret, description, extra headers.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "key": {"type": "string", "description": "Template key (e.g. slack, discord, teams, telegram, github_api, zapier, pagerduty)"},
                        "url": {"type": "string", "description": "Webhook URL for the target service"},
                        "name": {"type": "string", "description": "Custom endpoint name"},
                        "secret": {"type": "string", "description": "HMAC signing secret"},
                        "description": {"type": "string"},
                        "extra_headers": {"type": "object", "description": "Additional headers as key-value pairs"},
                    },
                    "required": ["key", "url"],
                },
            ),
            Tool(
                name="webhook_schedule",
                description="Schedule a webhook delivery for a future time. The delivery stays PENDING until scheduled_at, then the worker processes it.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string"},
                        "payload": {"type": "object", "description": "JSON payload to deliver"},
                        "scheduled_at": {"type": "string", "description": "ISO 8601 datetime for when to deliver (e.g. 2025-12-25T10:00:00Z)"},
                        "event_type": {"type": "string"},
                        "metadata": {"type": "object"},
                        "headers": {"type": "object", "description": "Extra headers for this delivery"},
                    },
                    "required": ["endpoint_id", "payload", "scheduled_at"],
                },
            ),
            Tool(
                name="recurring_schedule_create",
                description="Create a recurring webhook delivery schedule. Periodically sends a payload to an endpoint at a fixed interval. Supports max_runs for finite schedules.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Human-readable schedule name"},
                        "endpoint_id": {"type": "string", "description": "Target endpoint ID"},
                        "payload": {"type": "object", "description": "JSON payload to deliver on each run"},
                        "interval_value": {"type": "integer", "description": "Interval magnitude (e.g. 5 for every 5 minutes)", "minimum": 1},
                        "interval_unit": {"type": "string", "enum": ["seconds", "minutes", "hours", "days"], "default": "minutes"},
                        "event_type": {"type": "string"},
                        "headers": {"type": "object", "description": "Per-delivery headers"},
                        "metadata": {"type": "object"},
                        "max_runs": {"type": "integer", "default": 0, "description": "Maximum runs (0 = unlimited)"},
                    },
                    "required": ["name", "endpoint_id", "payload", "interval_value"],
                },
            ),
            Tool(
                name="recurring_schedule_list",
                description="List recurring webhook schedules. Filter by endpoint or active status.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Filter by endpoint ID"},
                        "active_only": {"type": "boolean", "default": False},
                    },
                },
            ),
            Tool(
                name="recurring_schedule_pause",
                description="Pause a recurring webhook schedule (stops firing until resumed).",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "schedule_id": {"type": "string"},
                    },
                    "required": ["schedule_id"],
                },
            ),
            Tool(
                name="recurring_schedule_resume",
                description="Resume a paused recurring webhook schedule.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "schedule_id": {"type": "string"},
                    },
                    "required": ["schedule_id"],
                },
            ),
            Tool(
                name="recurring_schedule_delete",
                description="Delete a recurring webhook schedule.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "schedule_id": {"type": "string"},
                    },
                    "required": ["schedule_id"],
                },
            ),
            Tool(
                name="bulk_endpoint_pause",
                description="Pause multiple webhook endpoints at once. Specify endpoint_ids list and/or tag to select targets.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_ids": {"type": "array", "items": {"type": "string"}, "description": "List of endpoint IDs to pause"},
                        "tag": {"type": "string", "description": "Pause all endpoints with this tag"},
                    },
                },
            ),
            Tool(
                name="bulk_endpoint_resume",
                description="Resume multiple webhook endpoints at once. Specify endpoint_ids list and/or tag to select targets.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_ids": {"type": "array", "items": {"type": "string"}, "description": "List of endpoint IDs to resume"},
                        "tag": {"type": "string", "description": "Resume all endpoints with this tag"},
                    },
                },
            ),
            Tool(
                name="bulk_endpoint_disable",
                description="Disable multiple webhook endpoints at once. Specify endpoint_ids list and/or tag to select targets.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_ids": {"type": "array", "items": {"type": "string"}, "description": "List of endpoint IDs to disable"},
                        "tag": {"type": "string", "description": "Disable all endpoints with this tag"},
                    },
                },
            ),
            Tool(
                name="bulk_endpoint_delete",
                description="Delete multiple webhook endpoints at once. Specify endpoint_ids list and/or tag to select targets.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_ids": {"type": "array", "items": {"type": "string"}, "description": "List of endpoint IDs to delete"},
                        "tag": {"type": "string", "description": "Delete all endpoints with this tag"},
                    },
                },
            ),
            Tool(
                name="delivery_simulate",
                description="Simulate a webhook delivery without actually sending it. Shows what would be sent: URL, method, headers, HMAC signature, transformed payload, retry config, circuit breaker state.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Target endpoint ID"},
                        "payload": {"type": "object", "description": "JSON payload to simulate"},
                        "event_type": {"type": "string"},
                        "headers": {"type": "object", "description": "Per-delivery headers"},
                    },
                    "required": ["endpoint_id", "payload"],
                },
            ),
            Tool(
                name="alert_evaluate",
                description="Evaluate alert rules against current state. Checks circuit breakers, DLQ thresholds, failure rates, disabled endpoints, and stalled deliveries. Returns fired/resolved events.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "condition": {
                            "type": "string",
                            "enum": ["circuit_open", "dlq_threshold", "endpoint_failure_rate", "endpoint_down", "delivery_stalled"],
                            "description": "Specific condition to evaluate. If omitted, evaluates all default rules.",
                        },
                        "threshold": {"type": "number", "description": "Threshold (count, percentage, or minutes depending on condition)"},
                        "severity": {"type": "string", "enum": ["info", "warning", "critical"], "default": "warning"},
                        "endpoint_id": {"type": "string", "description": "Scope alert to this endpoint"},
                        "tag": {"type": "string", "description": "Scope alert to endpoints with this tag"},
                        "notify_endpoint_id": {"type": "string", "description": "Endpoint ID to send alert notifications to"},
                    },
                },
            ),
            Tool(
                name="alert_summary",
                description="Get a quick alert summary — counts of open circuit breakers, DLQ entries, stalled deliveries, and active rules.",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="retention_estimate",
                description="Preview how many records would be deleted by a retention cleanup (dry run). Shows deliveries, event logs, DLQ, and incoming counts.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "delivery_days": {"type": "integer", "default": 30, "description": "Delivery retention in days"},
                        "event_log_days": {"type": "integer", "default": 90, "description": "Event log retention in days"},
                        "dlq_days": {"type": "integer", "default": 180, "description": "Dead letter retention in days"},
                        "incoming_days": {"type": "integer", "default": 7, "description": "Incoming webhook retention in days"},
                        "keep_failed": {"type": "boolean", "default": false, "description": "Don't count failed/dead-letter deliveries"},
                    },
                },
            ),
            Tool(
                name="retention_cleanup",
                description="Run data retention cleanup — permanently deletes old deliveries, event logs, dead letter entries, and incoming webhook records based on retention policy.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "delivery_retention_days": {"type": "integer", "default": 30},
                        "delivery_keep_failed": {"type": "boolean", "default": false},
                        "event_log_retention_days": {"type": "integer", "default": 90},
                        "dead_letter_retention_days": {"type": "integer", "default": 180},
                        "dead_letter_delete_replayed": {"type": "boolean", "default": true},
                        "incoming_retention_days": {"type": "integer", "default": 7},
                        "cleanup_batch_size": {"type": "integer", "default": 5000},
                        "dry_run": {"type": "boolean", "default": false, "description": "Preview without deleting"},
                    },
                },
            ),
            Tool(
                name="apikey_generate",
                description="Generate a new API key for REST API authentication. Returns the raw key (shown only once) with name, scopes, and expiration.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "description": "Name/label for this key"},
                        "scope": {"type": "array", "items": {"type": "string"}, "description": "Scopes (default: [*] = all)"},
                        "expires_in": {"type": "integer", "description": "Expire in N seconds (optional)"},
                    },
                    "required": ["name"],
                },
            ),
            Tool(
                name="idempotency_key_generate",
                description="Generate a UUID idempotency key for safe retry deduplication. "
                "Pass this key when creating deliveries so receivers can deduplicate redeliveries.",
                inputSchema={
                    "type": "object",
                    "properties": {},
                },
            ),
            Tool(
                name="verify_timestamped_signature",
                description="Verify a timestamped HMAC signature (t=<ts>,v1=<hmac> format). "
                "Used to verify outbound webhook authenticity and prevent replay attacks. "
                "Pass the shared secret, the original payload, and the signature header value.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "secret": {"type": "string", "description": "Shared HMAC secret"},
                        "payload": {"type": "string", "description": "Raw payload string that was signed"},
                        "signature": {"type": "string", "description": "Signature header value (t=<ts>,v1=<hmac>)"},
                        "tolerance_seconds": {"type": "integer", "default": 300, "description": "Max age in seconds"},
                    },
                    "required": ["secret", "payload", "signature"],
                },
            ),
            Tool(
                name="backoff_curve_preview",
                description="Preview the retry backoff curve for a given jitter strategy. "
                "Returns the base delay and 10 jittered samples for each attempt, so you can "
                "see the spread of retry delays before configuring an endpoint.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "initial_delay_seconds": {"type": "number", "default": 1.0},
                        "max_delay_seconds": {"type": "number", "default": 300.0},
                        "backoff_multiplier": {"type": "number", "default": 2.0},
                        "max_retries": {"type": "integer", "default": 5},
                        "jitter": {"type": "string", "enum": ["none", "full", "equal", "decorrelated"], "default": "full"},
                    },
                },
            ),
            Tool(
                name="retry_after_parse",
                description="Parse a Retry-After HTTP header value (RFC 7231). "
                "Supports both integer seconds ('120') and HTTP-date format "
                "('Fri, 31 Dec 2026 23:59:59 GMT'). Returns the delay in seconds.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "header_value": {"type": "string", "description": "The Retry-After header value to parse"},
                    },
                    "required": ["header_value"],
                },
            ),
            Tool(
                name="secret_rotation_initiate",
                description="Begin rotating the signing secret for an endpoint (zero-downtime rotation). "
                "The current primary secret becomes 'previous' (still usable for verification), "
                "and the new secret becomes the primary in 'rotating' status. "
                "Delivers are signed with the new secret immediately.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID"},
                        "new_secret": {"type": "string", "description": "The new HMAC secret to rotate to"},
                    },
                    "required": ["endpoint_id", "new_secret"],
                },
            ),
            Tool(
                name="secret_rotation_verify",
                description="Confirm that the new signing secret is working correctly. "
                "Transitions the new primary from 'rotating' to 'active' and "
                "retires the old previous secret (still usable during grace period).",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID"},
                    },
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="secret_rotation_complete",
                description="Complete rotation by cleaning up expired retired secrets after the grace period. "
                "Returns the number of old secrets removed.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID"},
                    },
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="secret_rotation_status",
                description="Get the current secret rotation status for an endpoint. "
                "Shows all active secrets, their roles (primary/previous), statuses, and versions.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID"},
                    },
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="secret_rotation_cancel",
                description="Cancel an in-progress rotation and revert to the previous primary secret.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID"},
                    },
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="verify_endpoint_create",
                description="Create an endpoint verification challenge. "
                "Generates a random token that the endpoint must echo back in a response "
                "header (X-Webhook-Verify-Challenge) or JSON body field to prove ownership. "
                "Used before activating a new endpoint to prevent misconfigured or malicious URLs.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID to verify"},
                        "method": {"type": "string", "enum": ["GET", "POST"], "default": "POST", "description": "HTTP method for verification request"},
                        "location": {"type": "string", "enum": ["header", "body", "either"], "default": "either", "description": "Where endpoint must echo the token"},
                        "header_name": {"type": "string", "default": "X-Webhook-Verify-Challenge", "description": "Response header to check"},
                        "body_field": {"type": "string", "default": "challenge", "description": "JSON body field to check"},
                        "ttl_seconds": {"type": "integer", "default": 600, "description": "Challenge validity period"},
                        "max_attempts": {"type": "integer", "default": 3, "description": "Max verification attempts"},
                    },
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="verify_endpoint_validate",
                description="Validate an endpoint's response to a verification challenge. "
                "Checks the response header and/or body for the challenge token. "
                "The token must be echoed correctly for verification to succeed.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID being verified"},
                        "response_headers": {"type": "object", "description": "Headers from the endpoint's response"},
                        "response_body": {"description": "Body from the endpoint's response (object or string)"},
                    },
                    "required": ["endpoint_id"],
                },
            ),
            Tool(
                name="verify_endpoint_status",
                description="Get the verification challenge status for an endpoint. "
                "Returns pending/verified/failed/expired status and details.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "endpoint_id": {"type": "string", "description": "Endpoint ID"},
                    },
                    "required": ["endpoint_id"],
                },
            ),
            # ── Outbox & Event Replay ────────────────────────────────────
            Tool(
                name="outbox_list",
                description="List outbox entries with optional filters (event_type, source, status, endpoint_id, time range). "
                "The outbox durably records every outbound webhook event before delivery for at-least-once semantics and replay.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "event_type": {"type": "string", "description": "Filter by event type"},
                        "source": {"type": "string", "description": "Filter by source (delivery, schedule, broadcast, subscription, manual)"},
                        "status": {"type": "string", "enum": ["pending", "delivered", "failed", "partial", "skipped"]},
                        "endpoint_id": {"type": "string", "description": "Filter by target endpoint ID"},
                        "start_time": {"type": "string", "description": "ISO 8601 start datetime"},
                        "end_time": {"type": "string", "description": "ISO 8601 end datetime"},
                        "limit": {"type": "integer", "default": 100},
                    },
                },
            ),
            Tool(
                name="outbox_get",
                description="Get a single outbox entry by ID, including payload, delivery results, and validation status.",
                inputSchema={
                    "type": "object",
                    "properties": {"entry_id": {"type": "string"}},
                    "required": ["entry_id"],
                },
            ),
            Tool(
                name="outbox_stats",
                description="Get aggregate outbox statistics: total entries, delivery rate, breakdown by status.",
                inputSchema={"type": "object", "properties": {}},
            ),
            Tool(
                name="replay_create",
                description="Create a replay batch to re-deliver outbox events. Filters by time range, event type, endpoint, or source. "
                "Optionally override target endpoints to redirect events to new endpoints (e.g., during migration). "
                "Returns a batch with preview of matched entries. Use replay_execute to actually deliver.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "start_time": {"type": "string", "description": "ISO 8601 — replay events from this time"},
                        "end_time": {"type": "string", "description": "ISO 8601 — replay events until this time"},
                        "event_type": {"type": "string", "description": "Only replay this event type"},
                        "endpoint_id": {"type": "string", "description": "Only replay events for this endpoint"},
                        "source": {"type": "string", "description": "Filter by source"},
                        "target_endpoint_ids": {
                            "type": "array",
                            "items": {"type": "string"},
                            "description": "Override target endpoints (omit to replay to original endpoints)",
                        },
                    },
                },
            ),
            Tool(
                name="replay_execute",
                description="Execute a replay batch — actually re-deliver all selected outbox events. "
                "This creates new deliveries for each outbox entry. The batch tracks progress and results.",
                inputSchema={
                    "type": "object",
                    "properties": {"batch_id": {"type": "string"}},
                    "required": ["batch_id"],
                },
            ),
            Tool(
                name="replay_status",
                description="Get the status and progress of a replay batch — total entries, processed, succeeded, failed.",
                inputSchema={
                    "type": "object",
                    "properties": {"batch_id": {"type": "string"}},
                    "required": ["batch_id"],
                },
            ),
            Tool(
                name="replay_list",
                description="List replay batches, optionally filtered by status.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "status": {"type": "string", "enum": ["pending", "in_progress", "completed", "failed", "cancelled"]},
                        "limit": {"type": "integer", "default": 50},
                    },
                },
            ),
            Tool(
                name="replay_cancel",
                description="Cancel a pending or in-progress replay batch.",
                inputSchema={
                    "type": "object",
                    "properties": {"batch_id": {"type": "string"}},
                    "required": ["batch_id"],
                },
            ),
            # ── Schema Validation ───────────────────────────────────────
            Tool(
                name="schema_create",
                description="Register a JSON Schema for validating event payloads before delivery. "
                "Supports versioning — multiple versions can coexist for schema evolution.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "event_type": {"type": "string", "description": "Event type to validate (e.g. order.created)"},
                        "version": {"type": "string", "description": "Schema version (e.g. '1.0')"},
                        "schema": {"type": "object", "description": "JSON Schema definition"},
                        "strict": {"type": "boolean", "default": False, "description": "Strict mode blocks delivery on validation failure"},
                        "description": {"type": "string"},
                    },
                    "required": ["event_type", "version", "schema"],
                },
            ),
            Tool(
                name="schema_get",
                description="Get the active JSON Schema for an event type, optionally for a specific version.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "event_type": {"type": "string"},
                        "version": {"type": "string", "description": "Specific version (omit for latest active)"},
                    },
                    "required": ["event_type"],
                },
            ),
            Tool(
                name="schema_list",
                description="List registered schema versions, optionally filtered by event type.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "event_type": {"type": "string"},
                        "active_only": {"type": "boolean", "default": False},
                    },
                },
            ),
            Tool(
                name="schema_validate",
                description="Validate a payload against the active schema for an event type. "
                "Returns validation result with detailed error messages.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "event_type": {"type": "string"},
                        "payload": {"type": "object", "description": "Payload to validate"},
                    },
                    "required": ["event_type", "payload"],
                },
            ),
            Tool(
                name="schema_delete",
                description="Delete a schema version by ID.",
                inputSchema={
                    "type": "object",
                    "properties": {"schema_id": {"type": "string"}},
                    "required": ["schema_id"],
                },
            ),
            # ── Fan-Out Delivery Groups ──────────────────────────────
            Tool(
                name="group_create",
                description="Create a named delivery group for fan-out: send one event to N endpoints simultaneously. "
                    "Strategies: parallel, sequential, weighted. Failure strategies: all_must_succeed, "
                    "any_success, majority_success, threshold.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "name": {"type": "string", "minLength": 1, "maxLength": 200, "description": "Human-readable group name"},
                        "description": {"type": "string", "description": "Optional description"},
                        "members": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "endpoint_id": {"type": "string"},
                                    "weight": {"type": "integer", "minimum": 1, "default": 1},
                                    "headers": {"type": "object", "description": "Per-member header overrides"},
                                    "enabled": {"type": "boolean", "default": True},
                                },
                                "required": ["endpoint_id"],
                            },
                            "description": "Endpoint members with optional weight and headers",
                        },
                        "strategy": {"type": "string", "enum": ["parallel", "sequential", "weighted"], "default": "parallel"},
                        "failure_strategy": {
                            "type": "string",
                            "enum": ["all_must_succeed", "any_success", "majority_success", "threshold"],
                            "default": "all_must_succeed",
                        },
                        "success_threshold": {"type": "number", "minimum": 0.0, "maximum": 1.0, "default": 1.0,
                            "description": "Fraction of members that must succeed (threshold strategy only)"},
                        "max_concurrent": {"type": "integer", "minimum": 0, "default": 0,
                            "description": "Max concurrent sends for parallel (0 = unlimited)"},
                        "tags": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["name"],
                },
            ),
            Tool(
                name="group_get",
                description="Get a delivery group by ID, including all members and configuration.",
                inputSchema={
                    "type": "object",
                    "properties": {"group_id": {"type": "string"}},
                    "required": ["group_id"],
                },
            ),
            Tool(
                name="group_list",
                description="List delivery groups, optionally filtered by status or tag.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "status": {"type": "string", "enum": ["active", "paused", "disabled"]},
                        "tag": {"type": "string"},
                    },
                },
            ),
            Tool(
                name="group_update",
                description="Update a delivery group's configuration (strategy, failure strategy, status, etc.).",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "group_id": {"type": "string"},
                        "name": {"type": "string"},
                        "description": {"type": "string"},
                        "strategy": {"type": "string", "enum": ["parallel", "sequential", "weighted"]},
                        "failure_strategy": {"type": "string", "enum": ["all_must_succeed", "any_success", "majority_success", "threshold"]},
                        "success_threshold": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                        "status": {"type": "string", "enum": ["active", "paused", "disabled"]},
                        "max_concurrent": {"type": "integer", "minimum": 0},
                        "tags": {"type": "array", "items": {"type": "string"}},
                    },
                    "required": ["group_id"],
                },
            ),
            Tool(
                name="group_delete",
                description="Delete a delivery group by ID.",
                inputSchema={
                    "type": "object",
                    "properties": {"group_id": {"type": "string"}},
                    "required": ["group_id"],
                },
            ),
            Tool(
                name="group_add_member",
                description="Add an endpoint as a member to a delivery group.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "group_id": {"type": "string"},
                        "endpoint_id": {"type": "string"},
                        "weight": {"type": "integer", "minimum": 1, "default": 1},
                        "headers": {"type": "object", "description": "Per-member header overrides"},
                    },
                    "required": ["group_id", "endpoint_id"],
                },
            ),
            Tool(
                name="group_remove_member",
                description="Remove an endpoint from a delivery group.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "group_id": {"type": "string"},
                        "endpoint_id": {"type": "string"},
                    },
                    "required": ["group_id", "endpoint_id"],
                },
            ),
            Tool(
                name="group_deliver",
                description="Deliver a payload to all active members of a delivery group. "
                    "Uses the group's strategy (parallel/sequential/weighted) and evaluates "
                    "the aggregate result using the failure strategy. Set dry_run=true to simulate.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "group_id": {"type": "string"},
                        "payload": {"type": "object", "description": "JSON payload to deliver"},
                        "event_type": {"type": "string"},
                        "headers": {"type": "object", "description": "Global headers for all members"},
                        "dry_run": {"type": "boolean", "default": False, "description": "Simulate without sending"},
                    },
                    "required": ["group_id", "payload"],
                },
            ),
            Tool(
                name="group_deliveries_list",
                description="List delivery results for a delivery group.",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "group_id": {"type": "string"},
                        "status": {"type": "string", "enum": ["success", "failed", "partial"]},
                        "limit": {"type": "integer", "minimum": 1, "maximum": 1000, "default": 100},
                    },
                    "required": ["group_id"],
                },
            ),
            Tool(
                name="group_stats",
                description="Get aggregate delivery statistics for a group (total, success rate, etc.).",
                inputSchema={
                    "type": "object",
                    "properties": {"group_id": {"type": "string"}},
                    "required": ["group_id"],
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
                    "jitter": arguments.get("jitter", "full"),
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
                    timestamped_signatures=arguments.get("timestamped_signatures", False),
                    idempotency_keys=arguments.get("idempotency_keys", False),
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
                    idempotency_key=arguments.get("idempotency_key"),
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

            # ── v0.5.0 New Tool Handlers ─────────────────────────────

            elif name == "circuit_breaker_state":
                state = engine.get_circuit_breaker_state(arguments["endpoint_id"])
                if state is None:
                    return [TextContent(type="text", text=json.dumps({"endpoint_id": arguments["endpoint_id"], "state": "closed", "message": "No circuit breaker tracked yet (all endpoints start in closed state)"}))]
                return [TextContent(type="text", text=json.dumps(state, default=str, indent=2))]

            elif name == "circuit_breaker_all":
                states = engine.get_all_circuit_breaker_states()
                if not states:
                    return [TextContent(type="text", text=json.dumps({"message": "No circuit breakers tracked", "breakers": []}))]
                return [TextContent(type="text", text=json.dumps(states, default=str, indent=2))]

            elif name == "circuit_breaker_reset":
                result = engine.reset_circuit_breaker(arguments["endpoint_id"])
                if result is None:
                    return [TextContent(type="text", text=f"No circuit breaker found for endpoint: {arguments['endpoint_id']}")]
                return [TextContent(type="text", text=json.dumps({"message": "Circuit breaker reset", **result}, default=str, indent=2))]

            elif name == "verify_signature":
                from .signature import SignatureVerifier, SignatureError
                verifier = SignatureVerifier(tolerance_seconds=arguments.get("tolerance_seconds", 300))
                try:
                    verifier.verify_or_raise(
                        raw_body=arguments["raw_body"],
                        headers=arguments["headers"],
                        secret=arguments["secret"],
                        provider=arguments.get("provider", "generic"),
                        algorithm=arguments.get("algorithm", "sha256"),
                    )
                    return [TextContent(type="text", text=json.dumps({"valid": True, "provider": arguments.get("provider", "generic")}, indent=2))]
                except SignatureError as e:
                    return [TextContent(type="text", text=json.dumps({"valid": False, "provider": arguments.get("provider", "generic"), "error": str(e)}, indent=2))]

            elif name == "detect_provider":
                from .signature import SignatureVerifier
                verifier = SignatureVerifier()
                provider = verifier.detect_provider(arguments["headers"])
                return [TextContent(type="text", text=json.dumps({"detected_provider": provider}, indent=2))]

            elif name == "generate_signature":
                from .signature import SignatureVerifier
                verifier = SignatureVerifier()
                sig = verifier.generate_signature(
                    raw_body=arguments["raw_body"],
                    secret=arguments["secret"],
                    algorithm=arguments.get("algorithm", "sha256"),
                    provider=arguments.get("provider", "generic"),
                )
                return [TextContent(type="text", text=json.dumps({"signature": sig, "provider": arguments.get("provider", "generic")}, indent=2))]

            # ── v0.5.0: Relay Rule Filters ─────────────────────────

            elif name == "relay_set_filter":
                from .filters import validate_filter_rules
                rule_id = arguments["rule_id"]
                filter_rules = arguments.get("filter_rules", {})

                # Validate first
                errors = validate_filter_rules(filter_rules)
                if errors:
                    return [TextContent(type="text", text=json.dumps({"valid": False, "errors": errors}, indent=2))]

                if hasattr(store, "update_relay_rule"):
                    rule = store.update_relay_rule(rule_id, filter_rules=filter_rules)
                else:
                    return [TextContent(type="text", text="Relay rule updates not supported by this store")]
                if rule is None:
                    return [TextContent(type="text", text=f"Relay rule not found: {rule_id}")]
                return [TextContent(type="text", text=json.dumps({
                    "message": "Filter rules set successfully",
                    "rule_id": rule_id,
                    "filter_rules": rule.filter_rules,
                }, default=str, indent=2))]

            elif name == "relay_validate_filter":
                from .filters import validate_filter_rules
                errors = validate_filter_rules(arguments["filter_rules"])
                result = {"valid": len(errors) == 0}
                if errors:
                    result["errors"] = errors
                return [TextContent(type="text", text=json.dumps(result, indent=2))]

            # ── v0.5.0: Import/Export ──────────────────────────────

            elif name == "export_config":
                from .import_export import export_config
                data = export_config(
                    store,
                    include_endpoints=arguments.get("include_endpoints", True),
                    include_relay_rules=arguments.get("include_relay_rules", True),
                    include_transforms=arguments.get("include_transforms", True),
                    include_subscriptions=arguments.get("include_subscriptions", True),
                )
                return [TextContent(type="text", text=json.dumps(data, default=str, indent=2))]

            elif name == "import_config":
                from .import_export import import_config
                summary = import_config(
                    store,
                    arguments["data"],
                    conflict_strategy=arguments.get("conflict_strategy", "skip"),
                    restore_secrets=arguments.get("restore_secrets", False),
                )
                return [TextContent(type="text", text=json.dumps(summary, default=str, indent=2))]

            elif name == "analytics_overview":
                from .analytics import AnalyticsEngine
                from .service import WebhookService
                svc = WebhookService(store=store)
                ae = AnalyticsEngine(svc)
                report = ae.overall_report(limit=arguments.get("limit", 10000))
                return [TextContent(type="text", text=json.dumps(report, default=str, indent=2))]

            elif name == "analytics_endpoint":
                from .analytics import AnalyticsEngine
                from .service import WebhookService
                svc = WebhookService(store=store)
                ae = AnalyticsEngine(svc)
                report = ae.endpoint_report(arguments["endpoint_id"])
                if report is None:
                    return [TextContent(type="text", text=f"Endpoint not found: {arguments['endpoint_id']}")]
                return [TextContent(type="text", text=json.dumps(report, default=str, indent=2))]

            elif name == "analytics_retry":
                from .analytics import AnalyticsEngine
                from .service import WebhookService
                svc = WebhookService(store=store)
                ae = AnalyticsEngine(svc)
                report = ae.retry_analysis(limit=arguments.get("limit", 10000))
                return [TextContent(type="text", text=json.dumps(report, default=str, indent=2))]

            elif name == "template_list":
                from .templates import TemplateRegistry
                registry = TemplateRegistry()
                tag = arguments.get("tag")
                templates = registry.list_templates(tag=tag)
                return [TextContent(type="text", text=json.dumps(templates, default=str, indent=2))]

            elif name == "template_get":
                from .templates import TemplateRegistry
                registry = TemplateRegistry()
                template = registry.get_template(arguments["key"])
                if template is None:
                    return [TextContent(type="text", text=f"Template not found: {arguments['key']}\nAvailable: {', '.join(registry.keys)}")]
                return [TextContent(type="text", text=json.dumps(template.to_dict(), default=str, indent=2))]

            elif name == "endpoint_from_template":
                from .templates import TemplateRegistry
                registry = TemplateRegistry()
                endpoint_obj = registry.create_endpoint(
                    key=arguments["key"],
                    url=arguments["url"],
                    name=arguments.get("name"),
                    secret=arguments.get("secret"),
                    description=arguments.get("description"),
                    extra_headers=arguments.get("extra_headers"),
                )
                if endpoint_obj is None:
                    return [TextContent(type="text", text=f"Template not found: {arguments['key']}\nAvailable: {', '.join(registry.keys)}")]
                store.add_endpoint(endpoint_obj)
                return [TextContent(type="text", text=json.dumps(endpoint_obj.model_dump(mode="json"), default=str, indent=2))]

            elif name == "webhook_schedule":
                from datetime import datetime as _dt
                from .service import WebhookService
                svc = WebhookService(store=store)
                scheduled_at = _dt.fromisoformat(arguments["scheduled_at"].replace("Z", "+00:00"))
                delivery = svc.schedule_webhook(
                    endpoint_id=arguments["endpoint_id"],
                    payload=arguments["payload"],
                    scheduled_at=scheduled_at,
                    event_type=arguments.get("event_type"),
                    metadata=arguments.get("metadata"),
                    headers=arguments.get("headers"),
                )
                if delivery is None:
                    return [TextContent(type="text", text=f"Endpoint not found: {arguments['endpoint_id']}")]
                return [TextContent(type="text", text=json.dumps(delivery.model_dump(mode="json"), default=str, indent=2))]

            elif name == "recurring_schedule_create":
                from .service import WebhookService
                svc = WebhookService(store=store)
                schedule = svc.create_schedule(
                    name=arguments["name"],
                    endpoint_id=arguments["endpoint_id"],
                    payload=arguments["payload"],
                    interval_value=arguments["interval_value"],
                    interval_unit=arguments.get("interval_unit", "minutes"),
                    event_type=arguments.get("event_type"),
                    headers=arguments.get("headers"),
                    metadata=arguments.get("metadata"),
                    max_runs=arguments.get("max_runs", 0),
                )
                if schedule is None:
                    return [TextContent(type="text", text=f"Endpoint not found: {arguments['endpoint_id']}")]
                return [TextContent(type="text", text=json.dumps(schedule.model_dump(mode="json"), default=str, indent=2))]

            elif name == "recurring_schedule_list":
                from .service import WebhookService
                svc = WebhookService(store=store)
                schedules = svc.list_schedules(
                    endpoint_id=arguments.get("endpoint_id"),
                    active_only=arguments.get("active_only", False),
                )
                return [TextContent(type="text", text=json.dumps([s.model_dump(mode="json") for s in schedules], default=str, indent=2))]

            elif name == "recurring_schedule_pause":
                from .service import WebhookService
                svc = WebhookService(store=store)
                schedule = svc.pause_schedule(arguments["schedule_id"])
                if schedule is None:
                    return [TextContent(type="text", text=f"Schedule not found: {arguments['schedule_id']}")]
                return [TextContent(type="text", text=json.dumps(schedule.model_dump(mode="json"), default=str, indent=2))]

            elif name == "recurring_schedule_resume":
                from .service import WebhookService
                svc = WebhookService(store=store)
                schedule = svc.resume_schedule(arguments["schedule_id"])
                if schedule is None:
                    return [TextContent(type="text", text=f"Schedule not found: {arguments['schedule_id']}")]
                return [TextContent(type="text", text=json.dumps(schedule.model_dump(mode="json"), default=str, indent=2))]

            elif name == "recurring_schedule_delete":
                from .service import WebhookService
                svc = WebhookService(store=store)
                deleted = svc.delete_schedule(arguments["schedule_id"])
                return [TextContent(type="text", text=json.dumps({"deleted": deleted, "schedule_id": arguments["schedule_id"]}))]

            elif name == "bulk_endpoint_pause":
                from .service import WebhookService
                svc = WebhookService(store=store)
                paused = svc.bulk_pause(
                    endpoint_ids=arguments.get("endpoint_ids"),
                    tag=arguments.get("tag"),
                )
                return [TextContent(type="text", text=json.dumps({"paused_count": len(paused), "paused_endpoint_ids": paused}))]

            elif name == "bulk_endpoint_resume":
                from .service import WebhookService
                svc = WebhookService(store=store)
                resumed = svc.bulk_resume(
                    endpoint_ids=arguments.get("endpoint_ids"),
                    tag=arguments.get("tag"),
                )
                return [TextContent(type="text", text=json.dumps({"resumed_count": len(resumed), "resumed_endpoint_ids": resumed}))]

            elif name == "bulk_endpoint_disable":
                from .service import WebhookService
                svc = WebhookService(store=store)
                disabled = svc.bulk_disable(
                    endpoint_ids=arguments.get("endpoint_ids"),
                    tag=arguments.get("tag"),
                )
                return [TextContent(type="text", text=json.dumps({"disabled_count": len(disabled), "disabled_endpoint_ids": disabled}))]

            elif name == "bulk_endpoint_delete":
                from .service import WebhookService
                svc = WebhookService(store=store)
                deleted = svc.bulk_delete(
                    endpoint_ids=arguments.get("endpoint_ids"),
                    tag=arguments.get("tag"),
                )
                return [TextContent(type="text", text=json.dumps({"deleted_count": len(deleted), "deleted_endpoint_ids": deleted}))]

            elif name == "delivery_simulate":
                from .service import WebhookService
                svc = WebhookService(store=store)
                result = svc.simulate_delivery(
                    endpoint_id=arguments["endpoint_id"],
                    payload=arguments["payload"],
                    event_type=arguments.get("event_type"),
                    headers=arguments.get("headers"),
                )
                return [TextContent(type="text", text=json.dumps(result, default=str, indent=2))]

            elif name == "alert_evaluate":
                from .alerts import (
                    AlertCondition,
                    AlertManager,
                    AlertRule,
                    AlertSeverity,
                    LogChannel,
                    WebhookChannel,
                    default_alert_rules,
                )
                from .service import WebhookService
                svc = WebhookService(store=store)
                manager = AlertManager(svc)

                condition = arguments.get("condition")
                if condition:
                    channels = [LogChannel()]
                    notify_ep = arguments.get("notify_endpoint_id")
                    if notify_ep:
                        channels.append(WebhookChannel(endpoint_id=notify_ep))
                    rule = AlertRule(
                        name=f"MCP-{condition}",
                        condition=AlertCondition(condition),
                        severity=AlertSeverity(arguments.get("severity", "warning")),
                        threshold=arguments.get("threshold", 0),
                        endpoint_id=arguments.get("endpoint_id"),
                        tag=arguments.get("tag"),
                        channels=channels,
                    )
                    manager.add_rule(rule)
                else:
                    for r in default_alert_rules(notify_endpoint_id=arguments.get("notify_endpoint_id")):
                        if arguments.get("endpoint_id"):
                            r.endpoint_id = arguments["endpoint_id"]
                        if arguments.get("tag"):
                            r.tag = arguments["tag"]
                        manager.add_rule(r)

                events = await manager.evaluate_all()
                result = {
                    "evaluated": len(manager.rules),
                    "events": len(events),
                    "firing": [e.to_dict() for e in events if e.status.value == "firing"],
                    "resolved": [e.to_dict() for e in events if e.status.value == "resolved"],
                }
                return [TextContent(type="text", text=json.dumps(result, default=str, indent=2))]

            elif name == "alert_summary":
                from .alerts import AlertManager, default_alert_rules
                from .service import WebhookService
                svc = WebhookService(store=store)
                manager = AlertManager(svc)
                for r in default_alert_rules():
                    manager.add_rule(r)
                await manager.evaluate_all()
                return [TextContent(type="text", text=json.dumps(manager.get_alert_summary(), default=str, indent=2))]

            elif name == "retention_estimate":
                from .retention import RetentionPolicy, RetentionManager
                from .service import WebhookService
                svc = WebhookService(store=store)
                policy = RetentionPolicy(
                    delivery_retention_days=arguments.get("delivery_days", 30),
                    delivery_keep_failed=arguments.get("keep_failed", False),
                    event_log_retention_days=arguments.get("event_log_days", 90),
                    dead_letter_retention_days=arguments.get("dlq_days", 180),
                    incoming_retention_days=arguments.get("incoming_days", 7),
                )
                manager = RetentionManager(svc, policy)
                return [TextContent(type="text", text=json.dumps(manager.get_estimates(), default=str, indent=2))]

            elif name == "retention_cleanup":
                from .retention import RetentionPolicy, RetentionManager
                from .service import WebhookService
                svc = WebhookService(store=store)
                dry_run = arguments.get("dry_run", False)
                policy_data = {k: v for k, v in arguments.items() if k != "dry_run"}
                policy = RetentionPolicy.from_dict(policy_data) if policy_data else RetentionPolicy()
                manager = RetentionManager(svc, policy)
                if dry_run:
                    result = {"dry_run": True, **manager.get_estimates()}
                else:
                    result = manager.run_cleanup().to_dict()
                return [TextContent(type="text", text=json.dumps(result, default=str, indent=2))]

            elif name == "apikey_generate":
                from .auth import APIKeyManager
                manager = APIKeyManager()
                raw_key, key_info = manager.create_key(
                    name=arguments["name"],
                    scopes=arguments.get("scope"),
                    expires_in_seconds=arguments.get("expires_in"),
                )
                result = {
                    "key": raw_key,
                    "name": key_info.name,
                    "scopes": key_info.scopes,
                    "expires_at": key_info.expires_at,
                    "note": "Store this key securely. It will not be shown again.",
                }
                return [TextContent(type="text", text=json.dumps(result, default=str, indent=2))]

            elif name == "idempotency_key_generate":
                import uuid as _uuid
                key = str(_uuid.uuid4())
                return [TextContent(type="text", text=json.dumps({
                    "idempotency_key": key,
                    "usage": "Pass this as idempotency_key when creating deliveries. "
                             "Receivers should store it to deduplicate redeliveries.",
                }, indent=2))]

            elif name == "verify_timestamped_signature":
                valid = engine.verify_timestamped_signature(
                    secret=arguments["secret"],
                    payload=arguments["payload"],
                    signature_header=arguments["signature"],
                    tolerance_seconds=arguments.get("tolerance_seconds", 300),
                )
                result = {"valid": valid}
                if not valid:
                    result["error"] = "Signature verification failed (invalid HMAC or timestamp outside tolerance)"
                return [TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "backoff_curve_preview":
                from .models import RetryPolicy, JitterType
                policy = RetryPolicy(
                    initial_delay_seconds=arguments.get("initial_delay_seconds", 1.0),
                    max_delay_seconds=arguments.get("max_delay_seconds", 300.0),
                    backoff_multiplier=arguments.get("backoff_multiplier", 2.0),
                    jitter=arguments.get("jitter", "full"),
                )
                max_retries = arguments.get("max_retries", 5)
                curve = []
                prev_delay = None
                for attempt in range(max_retries + 1):
                    base = policy.base_delay_for_attempt(attempt)
                    samples = []
                    for _ in range(10):
                        d = policy.delay_for_attempt(attempt, prev_delay=prev_delay)
                        samples.append(round(d, 3))
                    curve.append({
                        "attempt": attempt,
                        "base_delay_seconds": round(base, 3),
                        "jittered_samples": sorted(samples),
                        "min_sample": min(samples),
                        "max_sample": max(samples),
                    })
                    # Use the last sample as prev_delay for decorrelated
                    prev_delay = samples[-1]
                return [TextContent(type="text", text=json.dumps({
                    "jitter": policy.jitter.value,
                    "initial_delay_seconds": policy.initial_delay_seconds,
                    "max_delay_seconds": policy.max_delay_seconds,
                    "backoff_multiplier": policy.backoff_multiplier,
                    "curve": curve,
                }, indent=2))]

            elif name == "retry_after_parse":
                delay = engine.parse_retry_after(arguments["header_value"])
                result: dict[str, Any] = {"input": arguments["header_value"]}
                if delay is not None:
                    result["delay_seconds"] = round(delay, 3)
                    result["parsed"] = True
                else:
                    result["parsed"] = False
                    result["error"] = "Could not parse Retry-After header value"
                return [TextContent(type="text", text=json.dumps(result, indent=2))]

            elif name == "secret_rotation_initiate":
                mgr = engine.secret_rotation
                eid = arguments["endpoint_id"]
                # Register initial secret if endpoint has one but rotation not yet initialized
                if mgr.get_rotation_status(eid)["secrets"] == []:
                    ep = store.get_endpoint(eid)
                    if ep and ep.secret:
                        mgr.register_secret(eid, ep.secret)
                    else:
                        return [TextContent(type="text", text=json.dumps(
                            {"error": "Endpoint has no existing secret. Register a secret first."}
                        ))]
                new_rs = mgr.initiate_rotation(eid, arguments["new_secret"])
                return [TextContent(type="text", text=json.dumps({
                    "endpoint_id": eid,
                    "message": "Rotation initiated. New primary is in 'rotating' status.",
                    "new_secret_version": new_rs.version,
                    "status": mgr.get_rotation_status(eid),
                }, default=str, indent=2))]

            elif name == "secret_rotation_verify":
                mgr = engine.secret_rotation
                eid = arguments["endpoint_id"]
                verified = mgr.verify_rotation(eid)
                return [TextContent(type="text", text=json.dumps({
                    "endpoint_id": eid,
                    "verified": verified,
                    "status": mgr.get_rotation_status(eid),
                }, default=str, indent=2))]

            elif name == "secret_rotation_complete":
                mgr = engine.secret_rotation
                eid = arguments["endpoint_id"]
                removed = mgr.complete_rotation(eid)
                return [TextContent(type="text", text=json.dumps({
                    "endpoint_id": eid,
                    "secrets_removed": removed,
                    "status": mgr.get_rotation_status(eid),
                }, default=str, indent=2))]

            elif name == "secret_rotation_status":
                mgr = engine.secret_rotation
                status = mgr.get_rotation_status(arguments["endpoint_id"])
                return [TextContent(type="text", text=json.dumps(status, default=str, indent=2))]

            elif name == "secret_rotation_cancel":
                mgr = engine.secret_rotation
                eid = arguments["endpoint_id"]
                cancelled = mgr.cancel_rotation(eid)
                return [TextContent(type="text", text=json.dumps({
                    "endpoint_id": eid,
                    "cancelled": cancelled,
                    "status": mgr.get_rotation_status(eid),
                }, default=str, indent=2))]

            elif name == "verify_endpoint_create":
                cm = engine.challenge_manager
                from .verification import ChallengeLocation
                eid = arguments["endpoint_id"]
                location = ChallengeLocation(arguments.get("location", "either"))
                challenge = cm.create_challenge(
                    endpoint_id=eid,
                    method=arguments.get("method", "POST"),
                    location=location,
                    header_name=arguments.get("header_name", "X-Webhook-Verify-Challenge"),
                    body_field=arguments.get("body_field", "challenge"),
                    ttl_override=arguments.get("ttl_seconds"),
                    max_attempts=arguments.get("max_attempts", 3),
                )
                return [TextContent(type="text", text=json.dumps({
                    "endpoint_id": eid,
                    "token": challenge.token,
                    "message": (
                        f"Send a {challenge.method} request to the endpoint with this token. "
                        f"The endpoint must echo it in header '{challenge.header_name}' "
                        f"or body field '{challenge.body_field}'. "
                        f"Use verify_endpoint_validate to check the response."
                    ),
                    "expires_at": challenge.expires_at.isoformat(),
                    "max_attempts": challenge.max_attempts,
                }, indent=2))]

            elif name == "verify_endpoint_validate":
                cm = engine.challenge_manager
                eid = arguments["endpoint_id"]
                valid = cm.validate_response(
                    endpoint_id=eid,
                    response_headers=arguments.get("response_headers"),
                    response_body=arguments.get("response_body"),
                )
                ch = cm.get_challenge(eid)
                return [TextContent(type="text", text=json.dumps({
                    "endpoint_id": eid,
                    "verified": valid,
                    "status": ch.status.value if ch else "none",
                    "detail": ch.response_detail if ch else "No challenge found",
                    "attempts": ch.attempts if ch else 0,
                }, indent=2))]

            elif name == "verify_endpoint_status":
                cm = engine.challenge_manager
                eid = arguments["endpoint_id"]
                ch = cm.get_challenge(eid)
                if ch is None:
                    return [TextContent(type="text", text=json.dumps(
                        {"endpoint_id": eid, "status": "none", "message": "No verification challenge exists for this endpoint."}
                    ))]
                return [TextContent(type="text", text=json.dumps(ch.to_dict(), default=str, indent=2))]

            # ── Outbox & Event Replay ────────────────────────────────────
            elif name == "outbox_list":
                from .service import WebhookService
                svc = WebhookService(store=store)
                start_dt = _parse_dt(arguments.get("start_time"))
                end_dt = _parse_dt(arguments.get("end_time"))
                status_val = arguments.get("status")
                from .models import OutboxStatus as _OS
                entries = svc.list_outbox(
                    event_type=arguments.get("event_type"),
                    source=arguments.get("source"),
                    status=_OS(status_val) if status_val else None,
                    endpoint_id=arguments.get("endpoint_id"),
                    start_time=start_dt,
                    end_time=end_dt,
                    limit=arguments.get("limit", 100),
                )
                return [TextContent(type="text", text=json.dumps(
                    [e.model_dump(mode="json") for e in entries], default=str, indent=2
                ))]

            elif name == "outbox_get":
                from .service import WebhookService
                svc = WebhookService(store=store)
                entry = svc.get_outbox_entry(arguments["entry_id"])
                if entry is None:
                    return [TextContent(type="text", text=f"Outbox entry not found: {arguments['entry_id']}")]
                return [TextContent(type="text", text=json.dumps(entry.model_dump(mode="json"), default=str, indent=2))]

            elif name == "outbox_stats":
                from .service import WebhookService
                svc = WebhookService(store=store)
                stats = svc.outbox_stats()
                return [TextContent(type="text", text=json.dumps(stats, default=str, indent=2))]

            elif name == "replay_create":
                from .service import WebhookService
                svc = WebhookService(store=store)
                batch = svc.create_replay_batch(
                    start_time=_parse_dt(arguments.get("start_time")),
                    end_time=_parse_dt(arguments.get("end_time")),
                    event_type=arguments.get("event_type"),
                    endpoint_id=arguments.get("endpoint_id"),
                    source=arguments.get("source"),
                    target_endpoint_ids=arguments.get("target_endpoint_ids"),
                )
                if batch is None:
                    return [TextContent(type="text", text="Outbox requires SQLite store. Use .db file extension.")]
                return [TextContent(type="text", text=json.dumps(batch.model_dump(mode="json"), default=str, indent=2))]

            elif name == "replay_execute":
                from .service import WebhookService
                svc = WebhookService(store=store)
                batch = await svc.execute_replay_batch(arguments["batch_id"])
                if batch is None:
                    return [TextContent(type="text", text=f"Replay batch not found: {arguments['batch_id']}")]
                return [TextContent(type="text", text=json.dumps(batch.model_dump(mode="json"), default=str, indent=2))]

            elif name == "replay_status":
                from .service import WebhookService
                svc = WebhookService(store=store)
                batch = svc.get_replay_batch(arguments["batch_id"])
                if batch is None:
                    return [TextContent(type="text", text=f"Replay batch not found: {arguments['batch_id']}")]
                return [TextContent(type="text", text=json.dumps(batch.model_dump(mode="json"), default=str, indent=2))]

            elif name == "replay_list":
                from .service import WebhookService
                svc = WebhookService(store=store)
                batches = svc.list_replay_batches(status=arguments.get("status"))
                return [TextContent(type="text", text=json.dumps(
                    [b.model_dump(mode="json") for b in batches], default=str, indent=2
                ))]

            elif name == "replay_cancel":
                from .service import WebhookService
                svc = WebhookService(store=store)
                batch = svc.cancel_replay_batch(arguments["batch_id"])
                if batch is None:
                    return [TextContent(type="text", text=f"Replay batch not found or already completed: {arguments['batch_id']}")]
                return [TextContent(type="text", text=json.dumps(batch.model_dump(mode="json"), default=str, indent=2))]

            # ── Schema Validation ───────────────────────────────────────
            elif name == "schema_create":
                from .service import WebhookService
                svc = WebhookService(store=store)
                sv = svc.create_schema(
                    event_type=arguments["event_type"],
                    version=arguments["version"],
                    schema=arguments["schema"],
                    strict=arguments.get("strict", False),
                    description=arguments.get("description"),
                )
                if sv is None:
                    return [TextContent(type="text", text="Schema storage requires SQLite store. Use .db file extension.")]
                return [TextContent(type="text", text=json.dumps(sv.model_dump(mode="json"), default=str, indent=2))]

            elif name == "schema_get":
                from .service import WebhookService
                svc = WebhookService(store=store)
                sv = svc.get_schema(arguments["event_type"], arguments.get("version"))
                if sv is None:
                    return [TextContent(type="text", text=f"No schema found for event type: {arguments['event_type']}")]
                return [TextContent(type="text", text=json.dumps(sv.model_dump(mode="json"), default=str, indent=2))]

            elif name == "schema_list":
                from .service import WebhookService
                svc = WebhookService(store=store)
                svs = svc.list_schemas(arguments.get("event_type"), arguments.get("active_only", False))
                if not svs:
                    return [TextContent(type="text", text="[]")]
                return [TextContent(type="text", text=json.dumps(
                    [s.model_dump(mode="json") for s in svs], default=str, indent=2
                ))]

            elif name == "schema_validate":
                from .service import WebhookService
                svc = WebhookService(store=store)
                result = svc.validate_payload(arguments["event_type"], arguments["payload"])
                return [TextContent(type="text", text=json.dumps(result, default=str, indent=2))]

            elif name == "schema_delete":
                from .service import WebhookService
                svc = WebhookService(store=store)
                deleted = svc.delete_schema(arguments["schema_id"])
                return [TextContent(type="text", text=f"Schema deleted: {arguments['schema_id']}" if deleted else f"Schema not found: {arguments['schema_id']}")]

            # ── Fan-Out Delivery Groups ──────────────────────────────

            elif name == "group_create":
                from .service import WebhookService
                svc = WebhookService(store=store)
                group = svc.create_delivery_group(
                    name=arguments["name"],
                    members=arguments.get("members", []),
                    strategy=arguments.get("strategy", "parallel"),
                    failure_strategy=arguments.get("failure_strategy", "all_must_succeed"),
                    success_threshold=arguments.get("success_threshold", 1.0),
                    description=arguments.get("description"),
                    max_concurrent=arguments.get("max_concurrent", 0),
                    tags=arguments.get("tags"),
                )
                return [TextContent(type="text", text=json.dumps(group.model_dump(mode="json"), default=str, indent=2))]

            elif name == "group_get":
                from .service import WebhookService
                svc = WebhookService(store=store)
                group = svc.get_delivery_group(arguments["group_id"])
                if group is None:
                    return [TextContent(type="text", text=f"Group not found: {arguments['group_id']}")]
                return [TextContent(type="text", text=json.dumps(group.model_dump(mode="json"), default=str, indent=2))]

            elif name == "group_list":
                from .service import WebhookService
                svc = WebhookService(store=store)
                groups = svc.list_delivery_groups(
                    status=arguments.get("status"),
                    tag=arguments.get("tag"),
                )
                if not groups:
                    return [TextContent(type="text", text="[]")]
                return [TextContent(type="text", text=json.dumps(
                    [g.model_dump(mode="json") for g in groups], default=str, indent=2
                ))]

            elif name == "group_update":
                from .service import WebhookService
                svc = WebhookService(store=store)
                updates = {k: v for k, v in arguments.items() if k != "group_id"}
                group = svc.update_delivery_group(arguments["group_id"], **updates)
                if group is None:
                    return [TextContent(type="text", text=f"Group not found: {arguments['group_id']}")]
                return [TextContent(type="text", text=json.dumps(group.model_dump(mode="json"), default=str, indent=2))]

            elif name == "group_delete":
                from .service import WebhookService
                svc = WebhookService(store=store)
                deleted = svc.delete_delivery_group(arguments["group_id"])
                return [TextContent(type="text", text=f"Group deleted: {arguments['group_id']}" if deleted else f"Group not found: {arguments['group_id']}")]

            elif name == "group_add_member":
                from .service import WebhookService
                svc = WebhookService(store=store)
                group = svc.add_group_member(
                    arguments["group_id"],
                    arguments["endpoint_id"],
                    arguments.get("weight", 1),
                    arguments.get("headers"),
                )
                if group is None:
                    return [TextContent(type="text", text=f"Group not found: {arguments['group_id']}")]
                return [TextContent(type="text", text=json.dumps(group.model_dump(mode="json"), default=str, indent=2))]

            elif name == "group_remove_member":
                from .service import WebhookService
                svc = WebhookService(store=store)
                removed = svc.remove_group_member(arguments["group_id"], arguments["endpoint_id"])
                return [TextContent(type="text", text=json.dumps({"removed": removed}))]

            elif name == "group_deliver":
                from .service import WebhookService
                svc = WebhookService(store=store)
                if arguments.get("dry_run", False):
                    result = await svc.fanout_deliver_dry_run(
                        arguments["group_id"],
                        arguments["payload"],
                        arguments.get("event_type"),
                    )
                else:
                    result = await svc.fanout_deliver(
                        arguments["group_id"],
                        arguments["payload"],
                        arguments.get("event_type"),
                        arguments.get("headers"),
                    )
                return [TextContent(type="text", text=json.dumps(result.model_dump(mode="json"), default=str, indent=2))]

            elif name == "group_deliveries_list":
                from .service import WebhookService
                svc = WebhookService(store=store)
                deliveries = svc.list_group_deliveries(
                    group_id=arguments["group_id"],
                    status=arguments.get("status"),
                    limit=arguments.get("limit", 100),
                )
                if not deliveries:
                    return [TextContent(type="text", text="[]")]
                return [TextContent(type="text", text=json.dumps(
                    [d.model_dump(mode="json") for d in deliveries], default=str, indent=2
                ))]

            elif name == "group_stats":
                from .service import WebhookService
                svc = WebhookService(store=store)
                stats = svc.group_delivery_stats(arguments["group_id"])
                return [TextContent(type="text", text=json.dumps(stats, default=str, indent=2))]

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
