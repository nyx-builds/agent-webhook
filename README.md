<div align="center">

# Agent Webhook

**Webhook management, delivery, and relay for autonomous AI agents**

[![Python 3.10+](https://img.shields.io/badge/python-3.10+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![CI](https://github.com/nyx-builds/agent-webhook/actions/workflows/ci.yml/badge.svg)](https://github.com/nyx-builds/agent-webhook/actions/workflows/ci.yml)
[![Tests: 743](https://img.shields.io/badge/tests-743%20passing-brightgreen.svg)](#testing)
[![MCP](https://img.shields.io/badge/MCP-server-7c3aed)](https://modelcontextprotocol.io)
[![Version: 0.9.0](https://img.shields.io/badge/version-0.9.0-blue.svg)](#changelog)

</div>

---

**Webhook management, delivery, and relay service for autonomous agents — MCP server + CLI**

Built for the agentic economy — by [Nyx Builds](https://github.com/nyx-builds).

## Features

- **Endpoint Management** — Register, configure, and organize webhook endpoints with custom headers, secrets, and tags
- **Reliable Delivery** — Send webhooks with automatic retries, exponential backoff, and HMAC signature generation
- **Circuit Breaker** — Automatic failure detection per endpoint: stops wasting resources on failing targets, tests recovery with half-open state
- **Batch Delivery** — Send the same payload to multiple endpoints at once
- **Event Subscriptions** — Subscribe endpoints to specific event types and broadcast to subscribers
- **Delivery Cancellation** — Cancel pending or retrying deliveries
- **Recurring Schedules** — Set up periodic webhook delivery (cron-like intervals) for heartbeats, polling, and check-ins. Configurable interval (seconds/minutes/hours/days), max-run limits, pause/resume, and auto-deactivation on exhaustion
- **Bulk Endpoint Operations** — Mass pause, resume, disable, or delete endpoints by ID list or tag — essential for fleet management
- **Dry-Run Simulation** — Preview exactly what would be sent (URL, method, headers, HMAC signature, transformed payload, retry policy) without making any HTTP request
- **Dead Letter Queue** — Permanently failed deliveries are captured for inspection and replay
- **Payload Transforms** — Rename, filter, or template webhook payloads before delivery
- **Relay Filters** — Conditional forwarding rules based on headers and payload fields (equals, regex, numeric, list operators)
- **Incoming Signature Verification** — Verify HMAC signatures from GitHub, Stripe, Slack, Shopify, or generic providers with replay-attack prevention
- **Alert Rules** — Proactive alerting on circuit breaker opens, DLQ thresholds, endpoint failure rates, endpoint downs, and stalled deliveries — with webhook, log, and callback notification channels
- **Data Retention** — Automatic cleanup of old deliveries, event logs, dead-letter entries, and incoming webhooks with configurable policies and dry-run preview
- **API Key Authentication** — SHA-256 hashed API keys with scopes, expiration, and revocation for securing the REST API
- **Rate Limiting** — Per-endpoint rate limits with burst capacity
- **Prometheus Metrics** — Delivery counts, durations, rate-limit and dead-letter counters exposed via `/metrics`
- **Retry-After Header Support** — Automatically respects server-requested retry delays via RFC 7231 `Retry-After` header on 429/503 responses — overrides backoff when the server signals how long to wait
- **Multi-Secret Rotation** — Zero-downtime HMAC signing key rotation with grace-period overlap: initiate → verify → complete lifecycle, old secrets remain valid for verification during transition
- **Endpoint Verification Challenge** — Prove endpoint ownership before activation via challenge-response flow: endpoint must echo a random token in a header or JSON body field, with configurable TTL, location, and max attempts
- **Import / Export** — Portable config export (secrets excluded by default) with skip/overwrite/rename conflict resolution
- **Health Checks** — Test endpoint connectivity with ping payloads
- **Event Audit Log** — Timestamped log of all webhook events for audit trails
- **Delivery Tracking** — Full execution history with status codes, response bodies, and timing
- **Relay Server** — Forward incoming webhooks to registered endpoints using path-based routing rules
- **Webhook Templates** — Pre-configured endpoint templates for common services (Slack, Discord, generic JSON, etc.)
- **Analytics Dashboard** — Endpoint health scoring, duration stats, failure patterns, and retry analytics
- **Background Worker Pool** — Async worker pool for processing deliveries, schedules, and retries in the background
- **REST API** — Optional FastAPI server for HTTP access to all operations
- **Service Layer** — Clean business logic API on top of store and engine
- **MCP Server** — 82 tools for full webhook management from any MCP-compatible agent
- **Rich CLI** — Beautiful terminal interface with tables, colors, and filtering
- **SQLite Backend** — Default persistent storage with JSON-to-SQLite migration; JSON file backend also supported

## Quick Start

```bash
pip install agent-webhook

# Register a webhook endpoint
agent-webhook endpoint add "Slack Notifications" https://hooks.slack.com/services/XXX --tag "notifications"

# Send a webhook
agent-webhook send <endpoint-id> '{"text": "Hello from agent-webhook!"}'

# List endpoints
agent-webhook endpoint list

# Check delivery stats
agent-webhook stats

# Subscribe an endpoint to events
agent-webhook subscription add <endpoint-id> --event-type order.created --event-type order.updated
```

## CLI Reference

### Endpoint Management

```bash
# Add an endpoint
agent-webhook endpoint add <name> <url> [options]
  --method, -m        HTTP method (POST, PUT, PATCH, GET, DELETE) [default: POST]
  --header, -H        Custom header in 'Name: Value' format (repeatable)
  --tag, -t           Tag for filtering (repeatable)
  --secret            HMAC signing secret
  --timeout           Request timeout in seconds [default: 30]
  --description, -d   Description
  --max-retries       Max retry attempts [default: 3]

# List endpoints
agent-webhook endpoint list [--status active|paused|disabled] [--tag TAG]

# Show endpoint details (including subscriptions)
agent-webhook endpoint show <endpoint-id>

# Pause/Resume/Delete
agent-webhook endpoint pause <endpoint-id>
agent-webhook endpoint resume <endpoint-id>
agent-webhook endpoint delete <endpoint-id>
```

### Sending Webhooks

```bash
# Send a payload
agent-webhook send <endpoint-id> '<json-payload>'
agent-webhook send <endpoint-id> - < input.json  # from stdin

  --event-type, -e   Event type tag
  --header, -H       Extra headers in 'Name: Value' format

# Batch send to multiple endpoints
agent-webhook batch-send '<json-payload>' --endpoint <id1> --endpoint <id2>
  --event-type, -t   Event type tag
  --header, -H       Extra headers
```

### Delivery Tracking

```bash
# List deliveries
agent-webhook delivery list [--endpoint ID] [--status STATUS] [--event-type TYPE] [--limit N]

# Show delivery details with attempts
agent-webhook delivery show <delivery-id>

# Cancel a pending or retrying delivery
agent-webhook delivery cancel <delivery-id>
```

### Event Subscriptions

```bash
# Subscribe an endpoint to event types
agent-webhook subscription add <endpoint-id> --event-type order.created --event-type order.updated

# List subscriptions
agent-webhook subscription list [--endpoint ID]

# Delete a subscription
agent-webhook subscription delete <subscription-id>
```

### Health Check

```bash
# Test endpoint connectivity
agent-webhook health-check <endpoint-id>
```

### Event Audit Log

```bash
# View event log
agent-webhook event-log [--event-type TYPE] [--endpoint ID] [--limit N]
```

### Relay Rules

```bash
# Add a relay rule (forward incoming webhooks to endpoints)
agent-webhook relay add <name> <path-pattern> --target <endpoint-id> [--tag TAG]

# List rules
agent-webhook relay list

# Delete a rule
agent-webhook relay delete <rule-id>
```

### Incoming Webhooks

```bash
# List received webhooks
agent-webhook incoming list [--path PATH] [--limit N]
```

### Statistics

```bash
# Stats for all endpoints
agent-webhook stats

# Stats for a specific endpoint
agent-webhook stats <endpoint-id>
```

### Process Pending

```bash
# Process all pending/ready deliveries
agent-webhook process-pending
```

## MCP Server

Run the MCP server for agent integration:

```bash
agent-webhook-mcp
```

Or configure in your MCP client:

```json
{
  "mcpServers": {
    "agent-webhook": {
      "command": "agent-webhook-mcp",
      "args": []
    }
  }
}
```

### MCP Tools (70)

| Tool | Description |
|------|-------------|
| `endpoint_add` | Register a new webhook endpoint |
| `endpoint_list` | List all webhook endpoints |
| `endpoint_get` | Get endpoint details |
| `endpoint_update` | Update an endpoint |
| `endpoint_delete` | Delete an endpoint |
| `endpoint_from_template` | Create endpoint from a pre-configured template |
| `webhook_send` | Send a webhook delivery |
| `webhook_batch_send` | Send a payload to multiple endpoints |
| `webhook_schedule` | Schedule a delivery for a future time |
| `delivery_list` | List deliveries |
| `delivery_get` | Get delivery details with attempts |
| `delivery_retry` | Retry a failed delivery |
| `delivery_cancel` | Cancel a pending/retrying delivery |
| `delivery_simulate` | Dry-run preview of what would be sent (no HTTP) |
| `process_pending` | Process all pending deliveries |
| `health_check` | Test endpoint connectivity |
| `stats` | Get delivery statistics |
| `subscription_add` | Subscribe an endpoint to event types |
| `subscription_list` | List event subscriptions |
| `subscription_delete` | Delete an event subscription |
| `send_to_subscribers` | Send to all endpoints subscribed to an event type |
| `relay_add` | Add a relay rule |
| `relay_list` | List relay rules |
| `relay_delete` | Delete a relay rule |
| `relay_update` | Update a relay rule (name, path, targets, active, tags) |
| `relay_set_filter` | Set conditional filter rules on a relay rule |
| `relay_validate_filter` | Validate relay filter rules before applying |
| `incoming_list` | List incoming webhooks |
| `incoming_receive` | Receive & relay an incoming webhook |
| `event_log` | List event audit log entries |
| `transform_create` | Create a payload transform (field_map / filter / template) |
| `transform_list` | List all payload transforms |
| `transform_get` | Get transform details |
| `transform_update` | Update a payload transform |
| `transform_delete` | Delete a payload transform |
| `dead_letter_list` | List entries in the dead letter queue |
| `dead_letter_get` | Get dead letter entry details |
| `dead_letter_replay` | Replay a dead letter entry (new delivery) |
| `dead_letter_batch_replay` | Replay all unreplayed dead letter entries |
| `dead_letter_delete` | Delete a dead letter entry |
| `rate_limit_status` | Get rate limit status for an endpoint |
| `circuit_breaker_state` | Get circuit breaker state for an endpoint |
| `circuit_breaker_all` | Get circuit breaker states for all endpoints |
| `circuit_breaker_reset` | Force-close an endpoint's circuit breaker |
| `metrics` | Get delivery metrics (JSON or Prometheus format) |
| `verify_signature` | Verify an incoming webhook HMAC signature |
| `detect_provider` | Auto-detect webhook provider from headers |
| `generate_signature` | Generate a test HMAC signature for a payload |
| `export_config` | Export configuration to portable format |
| `import_config` | Import configuration with conflict strategies |
| `migrate_json_to_sqlite` | Migrate JSON store to SQLite backend |
| `recurring_schedule_create` | Create a recurring delivery schedule |
| `recurring_schedule_list` | List recurring schedules |
| `recurring_schedule_pause` | Pause a recurring schedule |
| `recurring_schedule_resume` | Resume a paused schedule |
| `recurring_schedule_delete` | Delete a recurring schedule |
| `bulk_endpoint_pause` | Bulk pause endpoints by IDs or tag |
| `bulk_endpoint_resume` | Bulk resume endpoints by IDs or tag |
| `bulk_endpoint_disable` | Bulk disable endpoints by IDs or tag |
| `bulk_endpoint_delete` | Bulk delete endpoints by IDs or tag |
| `template_list` | List available webhook templates |
| `template_get` | Get template details |
| `analytics_overview` | Analytics dashboard: global health, top endpoints |
| `analytics_endpoint` | Per-endpoint analytics: health score, duration stats |
| `analytics_retry` | Retry analytics: success rates, avg attempts |
| `alert_summary` | Alert state summary (rules, firing, resolved) |
| `alert_evaluate` | Evaluate alert rules and get fired events |
| `retention_estimate` | Preview retention cleanup impact (dry run) |
| `retention_cleanup` | Run data retention cleanup |
| `apikey_generate` | Generate a new API key with optional scopes/expiry |

## Python API

### Using the Service Layer

```python
from agent_webhook.service import WebhookService

# Setup
service = WebhookService(store_path="webhooks.json")

# Create an endpoint
endpoint = service.create_endpoint(
    name="My Service",
    url="https://api.example.com/webhook",
    secret="shared-secret",
    tags=["production"],
)

# Subscribe to events
service.add_subscription(endpoint.id, event_types=["order.created", "order.updated"])

# Send a webhook
import asyncio
result = asyncio.run(service.send_webhook(
    endpoint_id=endpoint.id,
    payload={"event": "order.created", "order_id": "12345"},
    event_type="order.created",
))
print(f"Status: {result.status}")

# Batch send to multiple endpoints
results = asyncio.run(service.batch_send(
    endpoint_ids=[endpoint.id, other_endpoint.id],
    payload={"broadcast": True, "message": "Hello all!"},
))

# Send to all subscribers of an event type
results = asyncio.run(service.send_to_subscribers(
    event_type="order.created",
    payload={"order_id": "12345"},
))

# Health check
health = asyncio.run(service.health_check(endpoint.id))
print(f"Healthy: {health['healthy']}")

# View audit log
entries = service.list_event_log(event_type="delivery.success", limit=10)

# Cancel a delivery
service.cancel_delivery(delivery_id)

# Close
asyncio.run(service.close())
```

### Using the Engine Directly

```python
from agent_webhook.store import WebhookStore
from agent_webhook.engine import DeliveryEngine
from agent_webhook.models import WebhookEndpoint

# Setup
store = WebhookStore("webhooks.json")
engine = DeliveryEngine(store)

# Register endpoint
endpoint = WebhookEndpoint(
    name="My Service",
    url="https://api.example.com/webhook",
    secret="shared-secret",
    tags=["production"],
)
store.add_endpoint(endpoint)

# Send a webhook
import asyncio
result = asyncio.run(engine.send(
    endpoint_id=endpoint.id,
    payload={"event": "order.created", "order_id": "12345"},
    event_type="order.created",
))
print(f"Status: {result.status}")
```

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   CLI / MCP  │────▶│   Service    │────▶│   Endpoints  │
│   Interface  │     │    Layer     │     │  (external)  │
└──────────────┘     └──────┬───────┘     └──────────────┘
                            │
                     ┌──────┴──────┐
                     │             │
                     ▼             ▼
              ┌──────────────┐  ┌──────────────┐
              │    Engine    │  │    Store     │
              │  (delivery)  │  │  (persist)   │
              └──────────────┘  └──────────────┘
                     │
           ┌─────────┼─────────┐
           │         │         │
           ▼         ▼         ▼
    ┌──────────┐ ┌──────────┐ ┌──────────┐
    │  Relay   │ │ Incoming │ │  Event   │
    │  Rules   │ │ Webhooks │ │   Log    │
    └──────────┘ └──────────┘ └──────────┘
```

### Key Concepts

- **Endpoint** — A registered webhook target with URL, method, headers, secret, and retry policy
- **Delivery** — An attempt to send a payload to an endpoint, with full attempt history
- **DeliveryAttempt** — A single HTTP request, tracking status code, response, and timing
- **EventSubscription** — Links an endpoint to specific event types for targeted delivery
- **RelayRule** — Routes incoming webhooks by path pattern to one or more endpoints
- **IncomingWebhook** — A webhook received by the relay, with forwarding tracking
- **EventLogEntry** — An audit trail entry recording webhook system events

### Retry Logic

Failed deliveries are automatically retried with exponential backoff:

- Configurable max retries (default: 3)
- Initial delay: 1s, doubles each attempt (2s, 4s, 8s...)
- Max delay cap: 300s
- Retry on status codes: 408, 429, 500, 502, 503, 504
- Connection errors trigger retry
- Non-retryable errors (4xx) are abandoned

### HMAC Signatures

When a secret is configured on an endpoint, all deliveries include an `X-Webhook-Signature` header:

```
X-Webhook-Signature: sha256=<hex-digest>
```

Receivers can verify the signature to authenticate the webhook source.

### Event Subscriptions

Endpoints can subscribe to specific event types. When you broadcast an event using `send_to_subscribers`, all active endpoints subscribed to that event type receive the payload. This enables pub/sub patterns:

```python
# Subscribe
service.add_subscription(endpoint_id, event_types=["order.created"])

# Broadcast to all subscribers
results = asyncio.run(service.send_to_subscribers(
    event_type="order.created",
    payload={"order_id": "12345"},
))
```

### Event Audit Log

All significant webhook events are recorded in the audit log with timestamps, event types, and details. The log is capped at 1000 entries to prevent unbounded growth:

```python
entries = service.list_event_log(event_type="delivery.success", limit=10)
for entry in entries:
    print(f"[{entry.timestamp}] {entry.event_type}: {entry.details}")
```

### Circuit Breaker

Each endpoint can have a circuit breaker that automatically stops delivery after consecutive failures, giving the downstream service time to recover:

- **CLOSED** — Normal operation. Failures are counted.
- **OPEN** — All deliveries blocked for a cooldown period (default 60s).
- **HALF_OPEN** — Trial deliveries allowed. After enough successes the circuit closes; any failure re-opens it.

```python
# Configure via endpoint creation
endpoint = WebhookEndpoint(
    name="Flaky Service",
    url="https://api.example.com/webhook",
    circuit_breaker_enabled=True,
    circuit_breaker_config={
        "failure_threshold": 5,      # open after 5 consecutive failures
        "recovery_timeout": 60.0,    # wait 60s before half-open
        "half_open_max_calls": 3,    # allow 3 trial deliveries
        "success_threshold": 2,      # 2 successes to close
    },
)

# Check state via CLI
# agent-webhook circuit-breaker state <endpoint-id>
# agent-webhook circuit-breaker reset <endpoint-id>
```

### Incoming Signature Verification

Relay rules can verify HMAC signatures on incoming webhooks to prevent spoofing and replay attacks:

```python
# Relay rule with GitHub signature verification
rule = RelayRule(
    name="GitHub Webhooks",
    path_pattern="/github/*",
    target_endpoint_ids=[endpoint.id],
    verify_signature=True,
    verify_secret="your-webhook-secret",
    verify_provider="github",  # or: stripe, slack, shopify, generic
    verify_tolerance_seconds=300,
)
```

Supported providers auto-extract the signature and timestamp from the correct headers and use the correct hashing scheme. Use the `detect_provider` MCP tool to auto-identify a provider from request headers.

## REST API

Run the REST API server (requires `pip install agent-webhook[rest]`):

```bash
# Default: SQLite backend on port 8000
agent-webhook serve --host 0.0.0.0 --port 8000

# Key endpoints
GET  /health                    # health check
GET  /metrics                   # JSON metrics
GET  /metrics/prometheus        # Prometheus scrape format
GET  /api/endpoints             # list endpoints
POST /api/endpoints             # create endpoint
POST /api/deliveries            # send a delivery
GET  /api/deliveries            # list deliveries
POST /webhooks/{path:path}      # relay receiver (matches relay rules)
```

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run specific test
pytest tests/test_service.py -v
```

## Changelog

### v0.9.0
- **Retry-After Header Support (RFC 7231)** — When a downstream server returns a 429, 503, or other retryable status with a `Retry-After` header, the engine now parses both integer-seconds (`Retry-After: 120`) and HTTP-date (`Retry-After: Fri, 31 Dec 2026 23:59:59 GMT`) formats and uses the server-requested delay instead of the computed backoff. The delay is clamped to `max_delay_seconds` for safety. MCP tool: `retry_after_parse`.
- **Multi-Secret Rotation** — Zero-downtime HMAC signing key rotation via `SecretRotationManager`. Full lifecycle: `initiate_rotation` (demotes current primary to "previous", promotes new secret to "rotating") → `verify_rotation` (confirms new secret working, retires old with grace period) → `complete_rotation` (cleans up expired secrets). Old secrets remain valid for incoming verification during the grace period (default 24h). Cancel mid-rotation with `cancel_rotation`. MCP tools: `secret_rotation_initiate`, `secret_rotation_verify`, `secret_rotation_complete`, `secret_rotation_status`, `secret_rotation_cancel`.
- **Endpoint Verification Challenge** — Prove endpoint ownership before activation via `ChallengeManager`. Generates a cryptographically secure random token (256-bit) that the endpoint must echo back in a response header (`X-Webhook-Verify-Challenge`) or JSON body field (`{"challenge": "<token>"}`). Configurable verification location (header/body/either), TTL (default 10min), and max attempts (default 3). Constant-time token comparison prevents timing attacks. MCP tools: `verify_endpoint_create`, `verify_endpoint_validate`, `verify_endpoint_status`.
- **DeliveryAttempt Metadata** — Added `metadata` field to `DeliveryAttempt` model for storing per-attempt data (e.g. `retry_after_seconds` from server responses).
- **MCP Server** — Expanded from 73 to 82 tools (+9 new).
- **Tests** — Expanded from 661 to 743 tests (+82 new in `test_v090_features.py`).

### v0.8.0
- **Jitter Strategies** — Four retry backoff jitter strategies to prevent thundering-herd retries: `none` (pure exponential), `full` (AWS-recommended uniform random), `equal` (half-base + random), `decorrelated` (previous-delay-based). MCP tool: `backoff_curve_preview`.
- **Timestamped Signatures** — Outbound HMAC signatures include timestamp + nonce for replay protection (`t=<unix_ts>,v1=<hmac>` format). MCP tool: `verify_timestamped_signature`.
- **Idempotency Keys** — Auto-generate `Idempotency-Key` header (UUID) for each delivery so receivers can safely deduplicate redeliveries. MCP tool: `idempotency_key_generate`.

### v0.7.0
- **Alert Rules** — Proactive alerting system with 5 condition types (circuit_open, dlq_threshold, endpoint_failure_rate, endpoint_down, delivery_stalled), cooldown periods to prevent alert fatigue, and 3 notification channels (LogChannel for audit log, WebhookChannel for forwarding, CallbackChannel for custom handlers). Preset default rules and custom rule creation. REST API: `/alerts/summary`, `/alerts/evaluate`. CLI: `alert list-presets`, `alert summary`, `alert evaluate`. MCP tools: `alert_summary`, `alert_evaluate`.
- **Data Retention** — Automatic cleanup of old deliveries, event log entries, dead-letter entries, and incoming webhooks. Configurable per-type retention windows, keep-failed option, batch-size limits, dry-run preview estimates. REST API: `/retention/estimate`, `/retention/cleanup`. CLI: `retention show`, `retention run`. MCP tools: `retention_estimate`, `retention_cleanup`.
- **API Key Authentication** — SHA-256 hashed API keys with scopes (`*` wildcard or specific), expiration timestamps, and revocation. FastAPI middleware for `X-API-Key`, `Authorization: Bearer`, and `?api_key=` query param. REST API: `/apikeys/generate`. CLI: `apikey generate`. MCP tool: `apikey_generate`.
- **Bug Fixes** — Fixed `AlertRule.id` uniqueness (UUID instead of name-derived slug), fixed `AlertRule` default channels (auto-attach LogChannel), fixed `APIKey.is_valid` as property, fixed `create_app()` to accept `service=` parameter, fixed DLQ cleanup with 0-day retention, aligned `__init__.py` version with pyproject.toml.
- **Documentation** — Updated README with 70 MCP tools (was 46), added v0.7.0 features and changelog.
- **Tests** — Fixed all 14 v0.7.0 test failures + 8 errors. 612 tests passing.

### v0.6.0
- **Recurring Schedules** — Periodic webhook delivery (cron-like intervals: seconds/minutes/hours/days) with configurable max-runs, pause/resume, start_at delay, auto-deactivation on exhaustion, and automatic worker integration
- **Bulk Endpoint Operations** — Mass pause, resume, disable, and delete endpoints by ID list or tag — essential for fleet management
- **Dry-Run Simulation** — Preview exactly what would be sent (URL, method, headers, HMAC signature, transformed payload, retry policy, rate limit) without making any HTTP request
- **CLI** — New command groups: `schedule` (create/list/pause/resume/delete/show/fire), `bulk` (pause/resume/disable/delete), and `simulate`
- **MCP Server** — Expanded from 42 to 46 tools (added recurring_schedule_*, bulk_*, simulate_delivery)
- **Tests** — Expanded from 524 to 545 tests across 15 test files

### v0.5.0
- **Circuit Breaker** — Per-endpoint automatic failure detection (CLOSED → OPEN → HALF_OPEN), configurable threshold/recovery, force-reset via CLI/MCP
- **Incoming Signature Verification** — Verify HMAC signatures from GitHub, Stripe, Slack, Shopify, and generic providers with timestamp-based replay-attack prevention
- **Relay Filters** — Conditional forwarding rules based on header and payload fields with operators (equals, not_equals, contains, starts_with, ends_with, regex, exists, eq/ne/gt/gte/lt/lte, in/not_in) and all/any/none logic
- **Import / Export** — Portable config export (secrets excluded by default) with skip/overwrite/rename conflict strategies on import
- **Metrics** — Delivery counters, duration histogram, rate-limit and dead-letter metrics exposed via CLI and REST `/metrics` (Prometheus format supported)
- **REST API additions** — `/metrics` and `/metrics/prometheus` endpoints
- **Bug Fixes** — Dead letter batch replay MCP bug, WebhookService store detection for SQLite
- **Tests** — Expanded to 524 tests across 14 test files

### v0.4.0
- **Dead Letter Queue** — Permanently failed deliveries captured for inspection and replay
- **Payload Transforms** — field_map (rename), filter (include/exclude), template (string interpolation)
- **Rate Limiting** — Per-endpoint limits with configurable period and burst
- **Prometheus Metrics** — Delivery counters and duration histograms
- **SQLite Backend** — Persistent storage with JSON-to-SQLite migration tool
- **REST API** — Full FastAPI server with 24 integration tests
- **MCP Server** — Expanded from 24 to 36 tools
- **Tests** — 290 tests

### v0.3.0
- **Transforms, Rate Limiting, Dead Letter Queue, SQLite backend, Prometheus metrics, REST API**

### v0.2.0
- **Event Subscriptions** — Subscribe endpoints to event types, broadcast to subscribers
- **Batch Delivery** — Send the same payload to multiple endpoints at once
- **Delivery Cancellation** — Cancel pending or retrying deliveries
- **Health Checks** — Test endpoint connectivity with ping payloads
- **Event Audit Log** — Timestamped log of all webhook events (capped at 1000)
- **Service Layer** — Clean business logic API (`WebhookService`)
- **MCP Server** — Expanded from 17 to 24 tools
- **CLI** — New commands: `subscription`, `batch-send`, `health-check`, `event-log`, `delivery cancel`
- **Tests** — 209 tests (up from ~85), added `test_service.py` and `test_cli.py`
- **Bug Fix** — Fixed asyncio event loop issues in CLI commands

### v0.1.0
- Initial release with endpoint management, delivery, relay, MCP server, and CLI

## License

MIT
