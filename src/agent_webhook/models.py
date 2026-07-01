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
    DEAD_LETTER = "dead_letter"


class WebhookStatus(str, Enum):
    """Status of a webhook endpoint."""
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"


class TransformType(str, Enum):
    """Types of payload transformations."""
    FIELD_MAP = "field_map"
    TEMPLATE = "template"
    FILTER = "filter"
    JQ = "jq"


class RateLimitPeriod(str, Enum):
    """Rate limit time periods."""
    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"


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


class PayloadTransform(BaseModel):
    """A payload transformation to apply before delivery."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., min_length=1, description="Human-readable name for the transform")
    type: TransformType = Field(..., description="Type of transformation")
    config: dict[str, Any] = Field(
        default_factory=dict,
        description="Transform configuration. "
        "field_map: {'mapping': {'old_key': 'new_key', ...}} "
        "filter: {'include': ['key1', ...]} or {'exclude': ['key1', ...]} "
        "template: {'template': 'key1={{payload.key1}}&key2={{payload.key2}}'} "
        "jq: {'expression': '.data | .[]'}",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("config")
    @classmethod
    def validate_config(cls, v: dict[str, Any], info) -> dict[str, Any]:
        # Basic validation — ensure config has expected keys
        return v


class RateLimit(BaseModel):
    """Rate limiting configuration for an endpoint."""
    max_requests: int = Field(..., ge=1, description="Maximum number of requests allowed")
    period: RateLimitPeriod = Field(default=RateLimitPeriod.MINUTE, description="Time period for the limit")
    burst: int = Field(default=0, ge=0, description="Allow burst above limit (0 = no burst)")

    @property
    def period_seconds(self) -> float:
        """Convert period to seconds for rate calculation."""
        return {
            RateLimitPeriod.SECOND: 1.0,
            RateLimitPeriod.MINUTE: 60.0,
            RateLimitPeriod.HOUR: 3600.0,
        }[self.period]


class SigningAlgorithm(str, Enum):
    """HMAC signing algorithms."""
    SHA1 = "sha1"
    SHA256 = "sha256"
    SHA512 = "sha512"


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
    signing_algorithm: SigningAlgorithm = Field(default=SigningAlgorithm.SHA256, description="HMAC signing algorithm")
    timeout_seconds: float = Field(default=30.0, ge=1.0, le=300.0, description="Request timeout")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    description: str | None = Field(default=None, description="Optional description")
    transform_ids: list[str] = Field(default_factory=list, description="Transform IDs to apply before delivery")
    rate_limit: RateLimit | None = Field(default=None, description="Rate limiting configuration")
    circuit_breaker_enabled: bool = Field(default=True, description="Enable circuit breaker for this endpoint")
    circuit_breaker_config: dict[str, Any] | None = Field(
        default=None,
        description="Circuit breaker config: {failure_threshold, recovery_timeout, half_open_max_calls, success_threshold}",
    )

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
    transformed_payload: dict[str, Any] | None = Field(default=None, description="Payload after transforms applied")
    dead_letter_reason: str | None = Field(default=None, description="Reason delivery went to dead letter queue")
    dead_lettered_at: datetime | None = Field(default=None, description="When delivery was moved to dead letter queue")
    scheduled_at: datetime | None = Field(default=None, description="When this delivery should be processed (future scheduling)")

    def current_attempt_number(self) -> int:
        return len(self.attempts)

    def last_attempt(self) -> DeliveryAttempt | None:
        return self.attempts[-1] if self.attempts else None

    def is_due(self) -> bool:
        """Check if a scheduled delivery is ready to be processed."""
        if self.scheduled_at is None:
            return True
        return datetime.now(timezone.utc) >= self.scheduled_at

    def can_retry(self, retry_policy: RetryPolicy) -> bool:
        if self.status in (DeliveryStatus.SUCCESS, DeliveryStatus.ABANDONED, DeliveryStatus.DEAD_LETTER):
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
    dead_letter: int = 0
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


class EventSubscription(BaseModel):
    """Subscription linking an endpoint to specific event types."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    endpoint_id: str = Field(..., description="Target endpoint ID")
    event_types: list[str] = Field(..., min_length=1, description="Event types to subscribe to")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("event_types")
    @classmethod
    def validate_event_types(cls, v: list[str]) -> list[str]:
        for et in v:
            if not re.match(r"^[a-zA-Z0-9._-]+$", et):
                raise ValueError(f"Invalid event type: {et}. Use alphanumeric, dots, hyphens, underscores only.")
        return v


class EventLogEntry(BaseModel):
    """An entry in the webhook event audit log."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = Field(..., description="Type of event (e.g. endpoint.created, delivery.success)")
    details: dict[str, Any] = Field(default_factory=dict, description="Event details")
    endpoint_id: str | None = Field(default=None, description="Related endpoint ID")
    delivery_id: str | None = Field(default=None, description="Related delivery ID")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


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
    verify_signature: bool = Field(default=False, description="Verify incoming webhook HMAC signatures")
    verify_secret: str | None = Field(default=None, description="Secret for incoming signature verification")
    verify_provider: str = Field(default="generic", description="Signature provider: generic, github, stripe, slack, shopify")
    verify_algorithm: str = Field(default="sha256", description="HMAC algorithm for generic verification (sha256/sha1)")
    verify_tolerance_seconds: int = Field(default=300, description="Max age for timestamps to prevent replay attacks")

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


class DeadLetterEntry(BaseModel):
    """An entry in the dead letter queue for permanently failed deliveries."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    delivery_id: str = Field(..., description="Original delivery ID")
    endpoint_id: str = Field(..., description="Target endpoint ID")
    payload: dict[str, Any] = Field(..., description="Original payload")
    transformed_payload: dict[str, Any] | None = Field(default=None, description="Payload after transforms")
    event_type: str | None = None
    reason: str = Field(..., description="Reason for dead-lettering")
    last_status_code: int | None = None
    last_error: str | None = None
    total_attempts: int = 0
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    replayed: bool = False
    replayed_delivery_id: str | None = Field(default=None, description="ID of the replayed delivery")
    replayed_at: datetime | None = None


class ScheduleInterval(str, Enum):
    """Supported recurring schedule intervals."""
    SECONDS = "seconds"
    MINUTES = "minutes"
    HOURS = "hours"
    DAYS = "days"


class WebhookSchedule(BaseModel):
    """A recurring webhook delivery schedule.

    Periodically sends a fixed payload to an endpoint at a specified interval.
    Supports max_runs for finite schedules and can be paused/resumed.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., min_length=1, max_length=200, description="Human-readable schedule name")
    endpoint_id: str = Field(..., description="Target endpoint ID")
    payload: dict[str, Any] = Field(..., description="JSON payload to deliver on each run")
    interval_value: int = Field(..., ge=1, description="Interval magnitude (e.g. 5 for every 5 minutes)")
    interval_unit: ScheduleInterval = Field(default=ScheduleInterval.MINUTES, description="Interval unit")
    event_type: str | None = Field(default=None, description="Event type tag for deliveries")
    headers: dict[str, str] = Field(default_factory=dict, description="Per-delivery headers")
    metadata: dict[str, Any] = Field(default_factory=dict, description="Extra metadata for deliveries")
    active: bool = Field(default=True, description="Whether the schedule is active")
    max_runs: int = Field(default=0, ge=0, description="Maximum number of runs (0 = unlimited)")
    run_count: int = Field(default=0, ge=0, description="Number of times this schedule has fired")
    next_run_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), description="When the next run should occur")
    last_run_at: datetime | None = Field(default=None, description="When the last run occurred")
    last_delivery_id: str | None = Field(default=None, description="ID of the last delivery created")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @property
    def interval_seconds(self) -> float:
        """Convert interval to seconds."""
        multipliers = {
            ScheduleInterval.SECONDS: 1.0,
            ScheduleInterval.MINUTES: 60.0,
            ScheduleInterval.HOURS: 3600.0,
            ScheduleInterval.DAYS: 86400.0,
        }
        return self.interval_value * multipliers[self.interval_unit]

    def is_due(self, now: datetime | None = None) -> bool:
        """Check if the schedule is due to run."""
        if not self.active:
            return False
        if self.max_runs > 0 and self.run_count >= self.max_runs:
            return False
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        # Handle naive next_run_at by treating it as UTC
        next_run = self.next_run_at
        if next_run.tzinfo is None:
            next_run = next_run.replace(tzinfo=timezone.utc)
        return now >= next_run

    def is_exhausted(self) -> bool:
        """Check if the schedule has reached its max_runs limit."""
        return self.max_runs > 0 and self.run_count >= self.max_runs

    def compute_next_run(self, base: datetime | None = None) -> datetime:
        """Compute the next run time from a base datetime."""
        from datetime import timedelta
        base = base or datetime.now(timezone.utc)
        if base.tzinfo is None:
            base = base.replace(tzinfo=timezone.utc)
        return base + timedelta(seconds=self.interval_seconds)
