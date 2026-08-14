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


class JitterType(str, Enum):
    """Jitter strategy for retry backoff.

    - **none**: No jitter. Pure exponential backoff.
      (High thundering-herd risk on shared downstream outages.)
    - **full**: Full jitter. Delay = random(0, exponential_delay).
      (Recommended by AWS Architecture Blog — maximizes spread.)
    - **equal**: Equal jitter. Delay = exponential_delay/2 + random(0, exponential_delay/2).
      (Preserves minimum delay while capping variance.)
    - **decorrelated**: Decorrelated jitter. Uses previous delay as the base for random range.
      (Best for large retry counts — prevents tight retry storms.)
    """
    NONE = "none"
    FULL = "full"
    EQUAL = "equal"
    DECORRELATED = "decorrelated"


class RetryPolicy(BaseModel):
    """Retry policy for failed webhook deliveries.

    Supports exponential backoff with configurable jitter strategies
    to prevent thundering-herd retries when downstream services recover
    from an outage.
    """
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum number of retry attempts")
    initial_delay_seconds: float = Field(default=1.0, ge=0.1, description="Initial delay before first retry")
    max_delay_seconds: float = Field(default=300.0, ge=1.0, description="Maximum delay between retries")
    backoff_multiplier: float = Field(default=2.0, ge=1.0, description="Exponential backoff multiplier")
    jitter: JitterType = Field(default=JitterType.FULL, description="Jitter strategy to apply to retry delays")
    retry_on_status_codes: list[int] = Field(
        default=[408, 429, 500, 502, 503, 504],
        description="HTTP status codes that trigger a retry",
    )

    def base_delay_for_attempt(self, attempt: int) -> float:
        """Calculate the *un-jittered* delay in seconds for a given retry attempt (0-indexed)."""
        delay = self.initial_delay_seconds * (self.backoff_multiplier ** attempt)
        return min(delay, self.max_delay_seconds)

    def delay_for_attempt(self, attempt: int, prev_delay: float | None = None) -> float:
        """Calculate the jittered delay in seconds for a given retry attempt.

        For ``decorrelated`` jitter, pass the previous delay returned by this
        method (or ``None`` for the first attempt).

        Args:
            attempt: Retry attempt number (0-indexed).
            prev_delay: Previous jittered delay (only used for ``decorrelated`` jitter).

        Returns:
            Delay in seconds, clamped to ``[0, max_delay_seconds]``.
        """
        import random

        base = self.base_delay_for_attempt(attempt)

        if self.jitter == JitterType.NONE:
            return base

        if self.jitter == JitterType.FULL:
            # Full jitter: uniform [0, base]
            return random.uniform(0, base)

        if self.jitter == JitterType.EQUAL:
            # Equal jitter: base/2 + uniform(0, base/2)
            return base / 2 + random.uniform(0, base / 2)

        if self.jitter == JitterType.DECORRELATED:
            # Decorrelated jitter: uniform [initial, min(max, prev * multiplier)]
            prev = prev_delay if prev_delay is not None else self.initial_delay_seconds
            cap = min(self.max_delay_seconds, prev * self.backoff_multiplier)
            return random.uniform(self.initial_delay_seconds, cap)

        return base  # Fallback (shouldn't reach here)


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
    timestamped_signatures: bool = Field(
        default=False,
        description="When True, outbound HMAC signatures include a timestamp and nonce "
        "for replay protection. Format: 't=<unix_ts>,v1=<hmac>'. "
        "The signed message is '<timestamp>.<payload>'.",
    )
    idempotency_keys: bool = Field(
        default=False,
        description="When True, automatically generate an Idempotency-Key header (UUID) "
        "for each delivery so receivers can safely deduplicate redeliveries.",
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
    metadata: dict[str, Any] = Field(default_factory=dict, description="Extra metadata (e.g. retry_after_seconds from server)")


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
    idempotency_key: str | None = Field(
        default=None,
        max_length=255,
        description="Idempotency key sent as Idempotency-Key header. "
        "When set, the receiver can safely deduplicate redeliveries.",
    )

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


class OutboxStatus(str, Enum):
    """Status of an outbox entry."""
    PENDING = "pending"
    DELIVERED = "delivered"
    FAILED = "failed"
    PARTIAL = "partial"  # Some endpoints succeeded, some failed
    SKIPPED = "skipped"  # Validation failed, never attempted


class OutboxEntry(BaseModel):
    """An append-only outbox entry recording every outbound webhook event.

    The outbox pattern guarantees that every event is durably recorded BEFORE
    delivery is attempted. This enables:

    - **At-least-once delivery semantics**: If the process crashes mid-delivery,
      the outbox entry survives and can be replayed.
    - **Event replay**: Re-deliver events from a time range to recover from
      downstream data loss or migration.
    - **Audit trail**: Every outbound event is recorded with its payload,
      target endpoints, and delivery outcome.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = Field(..., description="Event type tag (e.g. order.created)")
    payload: dict[str, Any] = Field(..., description="JSON payload that was sent")
    source: str = Field(
        default="delivery",
        description="What created this entry: delivery, schedule, broadcast, subscription, manual",
    )
    endpoint_ids: list[str] = Field(
        default_factory=list,
        description="Target endpoint IDs for this event",
    )
    delivery_ids: list[str] = Field(
        default_factory=list,
        description="Delivery IDs created from this outbox entry",
    )
    status: OutboxStatus = Field(
        default=OutboxStatus.PENDING,
        description="Aggregate delivery status for this entry",
    )
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Headers sent with the delivery",
    )
    metadata: dict[str, Any] = Field(
        default_factory=dict,
        description="Extra metadata",
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    delivered_at: datetime | None = Field(
        default=None,
        description="When the event was fully delivered",
    )
    # Aggregate results
    success_count: int = Field(default=0, ge=0)
    failure_count: int = Field(default=0, ge=0)
    # Validation
    validation_status: str = Field(
        default="not_checked",
        description="Schema validation result: not_checked, valid, invalid",
    )
    validation_errors: list[str] = Field(default_factory=list)

    def is_replayable(self) -> bool:
        """Whether this entry can be replayed."""
        return self.status in (
            OutboxStatus.DELIVERED,
            OutboxStatus.FAILED,
            OutboxStatus.PARTIAL,
        )


class ReplayBatchStatus(str, Enum):
    """Status of a replay batch operation."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ReplayBatch(BaseModel):
    """Tracks a batch replay operation — re-delivering outbox events to endpoints.

    A replay batch captures the intent, progress, and outcome of replaying
    a set of outbox entries. This provides an auditable record of what was
    replayed, when, and whether it succeeded.
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    # Filters that selected the entries to replay
    start_time: datetime | None = Field(default=None, description="Replay entries from this time")
    end_time: datetime | None = Field(default=None, description="Replay entries until this time")
    event_type: str | None = Field(default=None, description="Filter by event type")
    endpoint_id: str | None = Field(
        default=None,
        description="Only replay entries targeting this endpoint",
    )
    source: str | None = Field(default=None, description="Filter by source (delivery, schedule, etc.)")
    # Target override: replay to a DIFFERENT endpoint than the original
    target_endpoint_ids: list[str] | None = Field(
        default=None,
        description="Override target endpoints (None = use original endpoints)",
    )
    # Progress
    status: ReplayBatchStatus = Field(default=ReplayBatchStatus.PENDING)
    total_entries: int = Field(default=0, ge=0)
    processed: int = Field(default=0, ge=0)
    succeeded: int = Field(default=0, ge=0)
    failed: int = Field(default=0, ge=0)
    replayed_entry_ids: list[str] = Field(default_factory=list)
    created_delivery_ids: list[str] = Field(default_factory=list)
    error_message: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None

    @property
    def progress_pct(self) -> float:
        if self.total_entries == 0:
            return 0.0
        return round(self.processed / self.total_entries * 100, 1)


class SchemaVersion(BaseModel):
    """A versioned JSON Schema for an event type.

    Enables payload validation before delivery. Multiple versions can coexist
    to support schema evolution (e.g., adding new fields while maintaining
    backward compatibility).
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str = Field(..., description="Event type this schema validates")
    version: str = Field(..., description="Schema version (e.g. '1.0', '2.0')")
    schema_def: dict[str, Any] = Field(
        ...,
        alias="schema",
        description="JSON Schema definition",
    )
    strict: bool = Field(
        default=False,
        description="In strict mode, invalid payloads block delivery. "
        "In non-strict mode, they are delivered with a warning.",
    )
    active: bool = Field(default=True, description="Whether this schema version is active")
    description: str | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    model_config = {"populate_by_name": True}


# ── Fan-Out Delivery Groups (v1.0.0) ───────────────────────────────


class FanoutStrategy(str, Enum):
    """Delivery strategy for fan-out groups.

    - **parallel**: All members receive simultaneously (up to ``max_concurrent``).
    - **sequential**: Members are called one at a time; stops early if the
      failure strategy is satisfied before all members have been tried.
    - **weighted**: Members are selected by weight for each delivery,
      useful for canary / blue-green traffic splitting.
    """
    PARALLEL = "parallel"
    SEQUENTIAL = "sequential"
    WEIGHTED = "weighted"


class GroupFailureStrategy(str, Enum):
    """How to evaluate the success of a group delivery.

    - **all_must_succeed**: Every member must succeed, otherwise the group
      delivery is marked ``failed``.
    - **any_success**: At least one member must succeed.
    - **majority_success**: Strictly more than half must succeed.
    - **threshold**: At least ``success_threshold`` fraction of attempted
      members must succeed (0.0 – 1.0).
    """
    ALL_MUST_SUCCEED = "all_must_succeed"
    ANY_SUCCESS = "any_success"
    MAJORITY_SUCCESS = "majority_success"
    THRESHOLD = "threshold"


class GroupStatus(str, Enum):
    """Lifecycle status of a delivery group."""
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"


class GroupDeliveryStatus(str, Enum):
    """Outcome status of a group delivery."""
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    SUCCESS = "success"
    PARTIAL = "partial"
    FAILED = "failed"
    TIMEOUT = "timeout"


class GroupMemberStatus(str, Enum):
    """Per-member delivery status within a group delivery."""
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"
    TIMEOUT = "timeout"


class DeliveryGroupMember(BaseModel):
    """A single endpoint reference within a delivery group.

    Supports per-member header overrides (e.g., routing tokens) and
    weights for the ``weighted`` fan-out strategy.
    """
    endpoint_id: str = Field(..., description="Target endpoint ID")
    weight: int = Field(default=1, ge=1, description="Weight for weighted / canary routing")
    headers: dict[str, str] = Field(
        default_factory=dict,
        description="Per-member header overrides merged on top of delivery headers",
    )
    enabled: bool = Field(default=True, description="Temporarily disable a member without removing it")

    def model_dump(self, **kwargs: Any) -> dict[str, Any]:
        return super().model_dump(**kwargs)


class DeliveryGroup(BaseModel):
    """A named group of endpoints that receives fan-out deliveries.

    Example::

        group = DeliveryGroup(
            name="payment-events",
            members=[
                DeliveryGroupMember(endpoint_id="ep-1", weight=3),
                DeliveryGroupMember(endpoint_id="ep-2", weight=1),
            ],
            strategy=FanoutStrategy.WEIGHTED,
            failure_strategy=GroupFailureStrategy.MAJORITY_SUCCESS,
            max_concurrent=5,
        )
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str = Field(..., min_length=1, max_length=200, description="Human-readable group name")
    description: str | None = Field(default=None, description="Optional description")
    members: list[DeliveryGroupMember] = Field(
        default_factory=list,
        description="Endpoint members with per-member overrides",
    )
    strategy: FanoutStrategy = Field(
        default=FanoutStrategy.PARALLEL,
        description="How to deliver to members: parallel, sequential, or weighted",
    )
    failure_strategy: GroupFailureStrategy = Field(
        default=GroupFailureStrategy.ALL_MUST_SUCCEED,
        description="How to evaluate group delivery success",
    )
    success_threshold: float = Field(
        default=1.0,
        ge=0.0,
        le=1.0,
        description="Fraction of members that must succeed (only for threshold strategy)",
    )
    status: GroupStatus = Field(default=GroupStatus.ACTIVE, description="Group lifecycle status")
    max_concurrent: int = Field(
        default=0,
        ge=0,
        description="Max concurrent sends for parallel strategy (0 = unlimited)",
    )
    tags: list[str] = Field(default_factory=list, description="Tags for filtering")
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    @field_validator("tags")
    @classmethod
    def validate_tags(cls, v: list[str]) -> list[str]:
        for tag in v:
            if not re.match(r"^[a-zA-Z0-9_-]+$", tag):
                raise ValueError(f"Invalid tag: {tag}. Use alphanumeric, hyphens, underscores only.")
        return v

    def active_members(self) -> list[DeliveryGroupMember]:
        """Return only enabled members."""
        return [m for m in self.members if m.enabled]

    def total_weight(self) -> int:
        """Sum of weights across active members."""
        return sum(m.weight for m in self.active_members())

    def is_active(self) -> bool:
        return self.status == GroupStatus.ACTIVE

    def add_member(self, endpoint_id: str, weight: int = 1, headers: dict[str, str] | None = None) -> None:
        """Add or update a member in-place."""
        for m in self.members:
            if m.endpoint_id == endpoint_id:
                m.weight = weight
                if headers is not None:
                    m.headers = headers
                m.enabled = True
                self.updated_at = datetime.now(timezone.utc)
                return
        self.members.append(DeliveryGroupMember(
            endpoint_id=endpoint_id, weight=weight, headers=headers or {},
        ))
        self.updated_at = datetime.now(timezone.utc)

    def remove_member(self, endpoint_id: str) -> bool:
        """Remove a member. Returns True if a member was removed."""
        before = len(self.members)
        self.members = [m for m in self.members if m.endpoint_id != endpoint_id]
        if len(self.members) < before:
            self.updated_at = datetime.now(timezone.utc)
            return True
        return False


class GroupMemberResult(BaseModel):
    """Result of delivering to a single group member."""
    endpoint_id: str
    delivery_id: str | None = None
    status: GroupMemberStatus = Field(default=GroupMemberStatus.PENDING)
    status_code: int | None = None
    error: str | None = None
    duration_ms: float | None = None
    attempt: int = 0  # Which sequential attempt (0-indexed)


class GroupDeliveryResult(BaseModel):
    """Complete result of a fan-out delivery to a group.

    Captures per-member outcomes and an aggregate status determined by
    the group's ``failure_strategy``.
    """
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    group_id: str = Field(..., description="Source delivery group ID")
    group_name: str = Field(default="", description="Snapshot of group name")
    event_type: str | None = None
    payload: dict[str, Any] = Field(default_factory=dict, description="Payload that was sent")
    strategy: FanoutStrategy = Field(default=FanoutStrategy.PARALLEL)
    failure_strategy: GroupFailureStrategy = Field(default=GroupFailureStrategy.ALL_MUST_SUCCEED)
    success_threshold: float = Field(default=1.0)
    member_results: list[GroupMemberResult] = Field(default_factory=list)
    success_count: int = Field(default=0, ge=0)
    failure_count: int = Field(default=0, ge=0)
    skipped_count: int = Field(default=0, ge=0)
    total_members: int = Field(default=0, ge=0)
    status: GroupDeliveryStatus = Field(default=GroupDeliveryStatus.PENDING)
    error: str | None = None
    started_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    completed_at: datetime | None = None
    duration_ms: float | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)

    @property
    def attempted_count(self) -> int:
        """Number of members that were actually attempted (not skipped)."""
        return self.success_count + self.failure_count

    @property
    def success_rate(self) -> float | None:
        """Fraction of attempted members that succeeded."""
        if self.attempted_count == 0:
            return None
        return round(self.success_count / self.attempted_count, 4)

    def evaluate_status(self) -> GroupDeliveryStatus:
        """Determine aggregate status from member results using the failure strategy."""
        fs = self.failure_strategy
        attempted = self.attempted_count

        if attempted == 0:
            return GroupDeliveryStatus.FAILED

        if fs == GroupFailureStrategy.ALL_MUST_SUCCEED:
            if self.failure_count == 0:
                return GroupDeliveryStatus.SUCCESS
            return GroupDeliveryStatus.FAILED

        if fs == GroupFailureStrategy.ANY_SUCCESS:
            if self.success_count >= 1:
                return GroupDeliveryStatus.SUCCESS
            return GroupDeliveryStatus.FAILED

        if fs == GroupFailureStrategy.MAJORITY_SUCCESS:
            if self.success_count > attempted / 2:
                return GroupDeliveryStatus.SUCCESS
            return GroupDeliveryStatus.FAILED

        if fs == GroupFailureStrategy.THRESHOLD:
            needed = attempted * self.success_threshold
            if self.success_count >= needed:
                return GroupDeliveryStatus.SUCCESS
            return GroupDeliveryStatus.FAILED

        return GroupDeliveryStatus.PARTIAL


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
