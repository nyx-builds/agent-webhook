# Agent Webhook

**Webhook management, delivery, and relay service for autonomous agents — MCP server + CLI**

Built for the agentic economy — by [Nyx Builds](https://github.com/nyx-builds).

## Features

- **Endpoint Management** — Register, configure, and organize webhook endpoints with custom headers, secrets, and tags
- **Reliable Delivery** — Send webhooks with automatic retries, exponential backoff, and HMAC signature verification
- **Batch Delivery** — Send the same payload to multiple endpoints at once
- **Event Subscriptions** — Subscribe endpoints to specific event types and broadcast to subscribers
- **Delivery Cancellation** — Cancel pending or retrying deliveries
- **Health Checks** — Test endpoint connectivity with ping payloads
- **Event Audit Log** — Timestamped log of all webhook events for audit trails
- **Delivery Tracking** — Full execution history with status codes, response bodies, and timing
- **Relay Server** — Forward incoming webhooks to registered endpoints using path-based routing rules
- **Service Layer** — Clean business logic API on top of store and engine
- **MCP Server** — 24 tools for full webhook management from any MCP-compatible agent
- **Rich CLI** — Beautiful terminal interface with tables, colors, and filtering
- **JSON Persistence** — Simple file-based storage, easy to back up and migrate

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

### MCP Tools (24)

| Tool | Description |
|------|-------------|
| `endpoint_add` | Register a new webhook endpoint |
| `endpoint_list` | List all webhook endpoints |
| `endpoint_get` | Get endpoint details |
| `endpoint_update` | Update an endpoint |
| `endpoint_delete` | Delete an endpoint |
| `webhook_send` | Send a webhook delivery |
| `webhook_batch_send` | Send a payload to multiple endpoints |
| `delivery_list` | List deliveries |
| `delivery_get` | Get delivery details with attempts |
| `delivery_retry` | Retry a failed delivery |
| `delivery_cancel` | Cancel a pending/retrying delivery |
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
| `incoming_list` | List incoming webhooks |
| `incoming_receive` | Receive & relay an incoming webhook |
| `event_log` | List event audit log entries |

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
