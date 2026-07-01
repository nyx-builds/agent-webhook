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
