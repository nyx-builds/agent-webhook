"""Core data models for agent-webhook."""

from __future__ import annotations

import re
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator


class WebhookMethod(str, Enum):
    """HTTP methods allowed for webhook delivery."""
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    GET = "GET"
    DELETE = "DELETE"


class DeliveryStatus(str, Enum):
    """Status of a webhook delivery attempt."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    FAILED = "failed"
    RETRYING = "retrying"
    ABANDONED = "abandoned"


class WebhookStatus(str, Enum):
    """Status of a webhook endpoint."""
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"


class RetryPolicy(BaseModel):
    """Retry policy for failed webhook deliveries."""
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum number of retry attempts")
    initial_delay_seconds: float = Field(default=1.0, ge=0.1, description="Initial delay before first retry")
    max_delay_seconds: float = Field(default=300.0, ge=1.0, description="Maximum delay between retries")
    backoff_multiplier: float = Field(default=2.0, ge=1.0, description="Exponential backoff multiplier")
    retry_on_status_codes: list[int] = Field(
        default=[408, 429, 500, 502, 503, 504],
        description="HTTP status codes that trigger a retry",
    )

    def delay_for_attempt(self, attempt: int) -> float:
        """Calculate delay in seconds for a given retry attempt (0-indexed)."""
        delay = self.initial_delay_seconds * (self.backoff_multiplier ** attempt)
        return min(delay, self.max_delay_seconds)


class Header(BaseModel):
    """A single HTTP header."""
    name: str = Field(..., min_length=1, description="Header name")
    value: str = Field(..., description="Header value")

    @field_validator("name")
    @classmethod
    def validate_header_name(cls, v: str) -> str:
        if not re.match(r"^[A-Za-z0-9!#$%&'*+\-.^_|~]+$", v):
            raise ValueError(f"Invalid header name: {v}")
        return v


class WebhookEndpoint(BaseModel):
    """A registered webhook endpoint that can receive deliveries."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., min_length=1, max_length=100, description="Human-readable name")
    url: str = Field(..., min_length=1, description="Target URL for webhook deliveries")
    method: WebhookMethod = Field(default=WebhookMethod.POST, description="HTTP method for delivery")
    headers: list[Header] = Field(default_factory=list, description="Custom headers to send with each delivery")
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy, description="Retry policy for failed deliveries")
    status: WebhookStatus = Field(default=WebhookStatus.ACTIVE, description="Current endpoint status")
    tags: list[str] = Field(default_factory=list, description="Tags for filtering and grouping")
    secret: str | None = Field(default=None, description="Secret for HMAC signature generation")
    timeout_seconds: float = Field(default=30.0, ge=1.0, le=300.0, description="Request timeout")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    description: str | None = Field(default=None, description="Optional description")

    @field_validator("url")
    @classmethod
    def validate_url(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("URL must start with http:// or https://")
        return v

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v: list[str]) -> list[str]:
        for tag in v:
            if not re.match(r"^[a-zA-Z0-9_-]+$", tag):
                raise ValueError(f"Invalid tag: {tag}. Use alphanumeric, hyphens, underscores only.")
        return v

    def is_active(self) -> bool:
        return self.status == WebhookStatus.ACTIVE


class DeliveryAttempt(BaseModel):
    """A single attempt to deliver a webhook."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    delivery_id: str = Field(..., description="Parent delivery ID")
    attempt_number: int = Field(..., ge=1, description="Attempt number (1-indexed)")
    status: DeliveryStatus = Field(default=DeliveryStatus.PENDING)
    started_at: datetime | None = None
    completed_at: datetime | None = None
    response_status_code: int | None = None
    response_body: str | None = None
    response_headers: dict[str, str] | None = None
    error_message: str | None = None
    duration_ms: float | None = None


class WebhookDelivery(BaseModel):
    """A webhook delivery event — tracks the lifecycle of sending a payload to an endpoint."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    endpoint_id: str = Field(..., description="Target endpoint ID")
    payload: dict[str, Any] = Field(..., description="JSON payload to deliver")
    payload_headers: dict[str, str] = Field(default_factory=dict, description="Extra headers for this delivery only")
    status: DeliveryStatus = Field(default=DeliveryStatus.PENDING)
    attempts: list[DeliveryAttempt] = Field(default_factory=list, description="All delivery attempts")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    next_retry_at: datetime | None = None
    event_type: str | None = Field(default=None, description="Optional event type tag")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Extra metadata")

    def current_attempt_number(self) -> int:
        return len(self.attempts)

    def last_attempt(self) -> DeliveryAttempt | None:
        return self.attempts[-1] if self.attempts else None

    def can_retry(self, retry_policy: RetryPolicy) -> bool:
        if self.status in (DeliveryStatus.SUCCESS, DeliveryStatus.ABANDONED):
            return False
        return self.current_attempt_number() < retry_policy.max_retries + 1


class WebhookStats(BaseModel):
    """Statistics for a webhook endpoint."""
    endpoint_id: str
    endpoint_name: str
    total_deliveries: int = 0
    successful: int = 0
    failed: int = 0
    pending: int = 0
    retrying: int = 0
    abandoned: int = 0
    avg_duration_ms: float | None = None
    last_delivery_at: datetime | None = None
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None

    @property
    def success_rate(self) -> float | None:
        completed = self.successful + self.failed + self.abandoned
        if completed == 0:
            return None
        return self.successful / completed


class IncomingWebhook(BaseModel):
    """An incoming webhook received by the relay server."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    path: str = Field(..., description="URL path that received the webhook")
    method: str = Field(..., description="HTTP method used")
    headers: dict[str, str] = Field(default_factory=dict)
    body: dict[str, Any] | str | None = None
    query_params: dict[str, str] = Field(default_factory=dict)
    source_ip: str | None = None
    received_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    processed: bool = False
    forwarded_to: list[str] = Field(default_factory=list, description="Endpoint IDs forwarded to")
    tags: list[str] = Field(default_factory=list)


class RelayRule(BaseModel):
    """A rule that forwards incoming webhooks to registered endpoints."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., min_length=1, description="Rule name")
    path_pattern: str = Field(..., description="URL path pattern (supports * wildcard)")
    target_endpoint_ids: list[str] = Field(..., min_length=1, description="Endpoint IDs to forward to")
    active: bool = True
    transform: dict[str, Any] | None = Field(default=None, description="Optional payload transformation rules")
    filter_rules: dict[str, Any] | None = Field(default=None, description="Optional filter rules for incoming webhooks")
    tags: list[str] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("path_pattern")
    @classmethod
    def validate_path(cls, v: str) -> str:
        if not v.startswith("/"):
            raise ValueError("Path pattern must start with /")
        return v

    def matches_path(self, path: str) -> bool:
        """Check if a given path matches this rule's pattern."""
        if self.path_pattern == "/*":
            return True
        pattern = re.escape(self.path_pattern).replace(r"\*", ".*")
        return bool(re.fullmatch(pattern, path))
