"""Pre-built endpoint templates for popular webhook services.

Provides ``EndpointTemplate`` definitions and a ``TEMPLATE_REGISTRY`` so that
agents can quickly register endpoints for common targets without remembering
URL patterns or required headers.

Usage::

    from agent_webhook.templates import TemplateRegistry

    registry = TemplateRegistry()
    # List available templates
    for t in registry.list_templates():
        print(t["name"], t["description"])

    # Instantiate a template with the user-provided URL
    endpoint = registry.create_endpoint("slack", url="https://hooks.slack.com/services/XXX")
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .models import Header, RetryPolicy, WebhookEndpoint, WebhookMethod, WebhookStatus


@dataclass
class EndpointTemplate:
    """A pre-built endpoint configuration template."""

    key: str
    name: str
    description: str
    method: WebhookMethod = WebhookMethod.POST
    headers: dict[str, str] = field(default_factory=dict)
    timeout_seconds: float = 10.0
    default_url: str | None = None
    url_placeholder: str | None = None  # Hint text for the URL field
    tags: list[str] = field(default_factory=list)
    max_retries: int = 3
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 60.0
    backoff_multiplier: float = 2.0
    retry_on_status_codes: list[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])
    notes: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialise template metadata (excludes secrets)."""
        return {
            "key": self.key,
            "name": self.name,
            "description": self.description,
            "method": self.method.value,
            "headers": self.headers,
            "timeout_seconds": self.timeout_seconds,
            "default_url": self.default_url,
            "url_placeholder": self.url_placeholder,
            "tags": self.tags,
            "max_retries": self.max_retries,
            "initial_delay_seconds": self.initial_delay_seconds,
            "max_delay_seconds": self.max_delay_seconds,
            "backoff_multiplier": self.backoff_multiplier,
            "retry_on_status_codes": self.retry_on_status_codes,
            "notes": self.notes,
        }

    def create_endpoint(
        self,
        url: str,
        name: str | None = None,
        secret: str | None = None,
        description: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> WebhookEndpoint:
        """Instantiate a ``WebhookEndpoint`` from this template."""
        header_objs = [Header(name=k, value=v) for k, v in self.headers.items()]
        if extra_headers:
            header_objs.extend(Header(name=k, value=v) for k, v in extra_headers.items())

        retry_policy = RetryPolicy(
            max_retries=self.max_retries,
            initial_delay_seconds=self.initial_delay_seconds,
            max_delay_seconds=self.max_delay_seconds,
            backoff_multiplier=self.backoff_multiplier,
            retry_on_status_codes=self.retry_on_status_codes,
        )

        return WebhookEndpoint(
            name=name or self.name,
            url=url,
            method=self.method,
            headers=header_objs,
            timeout_seconds=self.timeout_seconds,
            tags=list(self.tags),
            secret=secret,
            description=description or self.description,
            retry_policy=retry_policy,
        )


# ── Built-in Templates ───────────────────────────────────────────────

_BUILTIN_TEMPLATES: list[EndpointTemplate] = [
    EndpointTemplate(
        key="slack",
        name="Slack Incoming Webhook",
        description="Send messages to a Slack channel via Incoming Webhook",
        timeout_seconds=10.0,
        tags=["messaging", "notifications"],
        url_placeholder="https://hooks.slack.com/services/T000/B000/XXXX",
        notes="Payload format: {\"text\": \"Your message\"}. Supports blocks and attachments.",
        max_retries=3,
        retry_on_status_codes=[429, 500, 502, 503],
    ),
    EndpointTemplate(
        key="discord",
        name="Discord Webhook",
        description="Send messages to a Discord channel via webhook URL",
        timeout_seconds=10.0,
        tags=["messaging", "notifications", "gaming"],
        url_placeholder="https://discord.com/api/webhooks/XXXX/XXXX",
        notes="Payload: {\"content\": \"message\"}. Supports embeds. Rate limit: 5 req/sec/channel.",
        max_retries=3,
        retry_on_status_codes=[429, 500, 502, 503],
    ),
    EndpointTemplate(
        key="teams",
        name="Microsoft Teams",
        description="Send messages to a MS Teams channel via Workflows/Connectors webhook",
        timeout_seconds=10.0,
        tags=["messaging", "notifications", "microsoft"],
        url_placeholder="https://outlook.office.com/webhook/XXXX",
        notes="Payload: Adaptive Card or simple text message format.",
        max_retries=2,
        retry_on_status_codes=[429, 500, 502, 503],
    ),
    EndpointTemplate(
        key="telegram",
        name="Telegram Bot",
        description="Send messages via Telegram Bot API",
        timeout_seconds=15.0,
        tags=["messaging", "notifications"],
        url_placeholder="https://api.telegram.org/bot<TOKEN>/sendMessage",
        notes="Requires 'chat_id' in payload. Use getUpdates to find chat_id.",
        max_retries=3,
        retry_on_status_codes=[429, 500, 502, 503],
    ),
    EndpointTemplate(
        key="generic",
        name="Generic HTTP Webhook",
        description="Standard HTTP POST to any URL with JSON body",
        timeout_seconds=30.0,
        tags=["generic"],
        url_placeholder="https://example.com/webhook",
        notes="Default configuration suitable for most HTTP webhook targets.",
        max_retries=3,
    ),
    EndpointTemplate(
        key="github_api",
        name="GitHub API",
        description="Interact with GitHub REST API (create issues, comments, etc.)",
        method=WebhookMethod.POST,
        timeout_seconds=15.0,
        headers={"Accept": "application/vnd.github+json"},
        tags=["github", "devtools", "api"],
        url_placeholder="https://api.github.com/repos/OWNER/REPO/issues",
        notes="Requires Authorization header with token. Set via --secret or custom header.",
        max_retries=2,
        retry_on_status_codes=[500, 502, 503],
    ),
    EndpointTemplate(
        key="stripe",
        name="Stripe Webhook (outbound)",
        description="Mirror events to a Stripe-compatible webhook consumer",
        timeout_seconds=10.0,
        tags=["payments", "stripe"],
        url_placeholder="https://your-server.com/stripe-webhook",
        notes="Outbound: deliver Stripe-formatted events. Set up signing secret for verification.",
        max_retries=3,
        retry_on_status_codes=[409, 500, 502, 503, 504],
    ),
    EndpointTemplate(
        key="zapier",
        name="Zapier Webhook",
        description="Trigger a Zapier workflow via webhook",
        timeout_seconds=20.0,
        tags=["automation", "integration"],
        url_placeholder="https://hooks.zapier.com/hooks/catch/XXXX/XXXX",
        notes="Send JSON data that matches your Zap input fields.",
        max_retries=2,
        retry_on_status_codes=[500, 502, 503],
    ),
    EndpointTemplate(
        key="make",
        name="Make.com (Integromat)",
        description="Trigger a Make.com scenario via webhook",
        timeout_seconds=20.0,
        tags=["automation", "integration"],
        url_placeholder="https://hook.us1.make.com/XXXX",
        notes="Send JSON data that matches your scenario input.",
        max_retries=2,
        retry_on_status_codes=[500, 502, 503],
    ),
    EndpointTemplate(
        key="email_smtp",
        name="Email via Webhook Bridge",
        description="Send email notifications via a webhook-to-email bridge (e.g. Resend, Postmark)",
        timeout_seconds=10.0,
        tags=["email", "notifications"],
        url_placeholder="https://api.resend.com/emails",
        notes="Requires API key in Authorization header. Payload: {from, to, subject, html}.",
        max_retries=2,
        retry_on_status_codes=[429, 500, 502, 503],
    ),
    EndpointTemplate(
        key="n8n",
        name="n8n Webhook",
        description="Trigger an n8n workflow via webhook node",
        timeout_seconds=20.0,
        tags=["automation", "integration", "self-hosted"],
        url_placeholder="https://your-n8n.com/webhook/workflow-id",
        notes="Self-hosted or cloud n8n. Send JSON data matching workflow input.",
        max_retries=2,
        retry_on_status_codes=[500, 502, 503],
    ),
    EndpointTemplate(
        key="pagerduty",
        name="PagerDuty Events API v2",
        description="Trigger or resolve PagerDuty incidents",
        timeout_seconds=10.0,
        headers={
            "Content-Type": "application/json",
            "X-Routing-Key": "YOUR_ROUTING_KEY",
        },
        tags=["alerting", "incident", "oncall"],
        url_placeholder="https://events.pagerduty.com/v2/enqueue",
        notes="Payload: {routing_key, event_action, dedup_key, payload: {summary, severity, source}}",
        max_retries=5,
        initial_delay_seconds=2.0,
        retry_on_status_codes=[429, 500, 502, 503, 504],
    ),
]


class TemplateRegistry:
    """Registry for endpoint templates.

    Allows looking up templates by key, listing all templates, and
    creating ``WebhookEndpoint`` instances from a template.
    """

    def __init__(self) -> None:
        self._templates: dict[str, EndpointTemplate] = {
            t.key: t for t in _BUILTIN_TEMPLATES
        }

    def get_template(self, key: str) -> EndpointTemplate | None:
        """Get a template by its key (e.g. ``"slack"``)."""
        return self._templates.get(key)

    def list_templates(self, tag: str | None = None) -> list[dict[str, Any]]:
        """List all available templates as metadata dicts.

        Args:
            tag: Filter by tag (e.g. ``"messaging"``, ``"automation"``).
        """
        templates = list(self._templates.values())
        if tag:
            templates = [t for t in templates if tag in t.tags]
        return [t.to_dict() for t in templates]

    def create_endpoint(
        self,
        key: str,
        url: str,
        name: str | None = None,
        secret: str | None = None,
        description: str | None = None,
        extra_headers: dict[str, str] | None = None,
    ) -> WebhookEndpoint | None:
        """Create an endpoint from a template.

        Returns ``None`` if the template key is not found.
        """
        template = self._templates.get(key)
        if template is None:
            return None
        return template.create_endpoint(
            url=url,
            name=name,
            secret=secret,
            description=description,
            extra_headers=extra_headers,
        )

    def register_template(self, template: EndpointTemplate) -> None:
        """Register a custom template."""
        self._templates[template.key] = template

    @property
    def keys(self) -> list[str]:
        """All available template keys."""
        return sorted(self._templates.keys())
