# Agent Webhook

**Webhook management, delivery, and relay service for autonomous agents — MCP server + CLI**

Built for the agentic economy — by [Nyx Builds](https://github.com/nyx-builds).

## Features

- **Endpoint Management** — Register, configure, and organize webhook endpoints with custom headers, secrets, and tags
- **Reliable Delivery** — Send webhooks with automatic retries, exponential backoff, and HMAC signature verification
- **Delivery Tracking** — Full execution history with status codes, response bodies, and timing
- **Relay Server** — Forward incoming webhooks to registered endpoints using path-based routing rules
- **MCP Server** — 17 tools for full webhook management from any MCP-compatible agent
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

# Show endpoint details
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
```

### Delivery Tracking

```bash
# List deliveries
agent-webhook delivery list [--endpoint ID] [--status STATUS] [--event-type TYPE] [--limit N]

# Show delivery details with attempts
agent-webhook delivery show <delivery-id>
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

### MCP Tools (17)

| Tool | Description |
|------|-------------|
| `endpoint_add` | Register a new webhook endpoint |
| `endpoint_list` | List all webhook endpoints |
| `endpoint_get` | Get endpoint details |
| `endpoint_update` | Update an endpoint |
| `endpoint_delete` | Delete an endpoint |
| `webhook_send` | Send a webhook delivery |
| `delivery_list` | List deliveries |
| `delivery_get` | Get delivery details with attempts |
| `delivery_retry` | Retry a failed delivery |
| `process_pending` | Process all pending deliveries |
| `stats` | Get delivery statistics |
| `relay_add` | Add a relay rule |
| `relay_list` | List relay rules |
| `relay_delete` | Delete a relay rule |
| `incoming_list` | List incoming webhooks |
| `incoming_receive` | Receive & relay an incoming webhook |

## Architecture

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   CLI / MCP  │────▶│    Engine    │────▶│   Endpoints  │
│   Interface  │     │  (delivery)  │     │  (external)  │
└──────────────┘     └──────────────┘     └──────────────┘
       │                    │
       │           ┌────────┴────────┐
       │           │                 │
       ▼           ▼                 ▼
┌──────────────┐  ┌──────────────┐  ┌──────────────┐
│    Store     │  │   Relay      │  │   Incoming   │
│  (persist)   │  │   Rules      │  │   Webhooks   │
└──────────────┘  └──────────────┘  └──────────────┘
```

### Key Concepts

- **Endpoint** — A registered webhook target with URL, method, headers, secret, and retry policy
- **Delivery** — An attempt to send a payload to an endpoint, with full attempt history
- **DeliveryAttempt** — A single HTTP request, tracking status code, response, and timing
- **RelayRule** — Routes incoming webhooks by path pattern to one or more endpoints
- **IncomingWebhook** — A webhook received by the relay, with forwarding tracking

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

## Python API

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

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Run specific test
pytest tests/test_models.py -v
```

## License

MIT
