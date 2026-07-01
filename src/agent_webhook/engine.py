"""Webhook delivery engine — handles HTTP delivery with retries, HMAC signing, transforms, rate limiting, and dead letter queue."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from .models import (
    DeadLetterEntry,
    DeliveryAttempt,
    DeliveryStatus,
    RelayRule,
    RetryPolicy,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookStatus,
)
from .rate_limiter import RateLimiter
from .store import WebhookStore
from .transforms import TransformEngine
from .circuit_breaker import CircuitBreaker, CircuitBreakerConfig, CircuitState


class DeliveryEngine:
    """Handles webhook delivery with retries, HMAC signing, transforms, rate limiting, circuit breaker, and DLQ."""

    def __init__(self, store: WebhookStore, default_timeout: float = 30.0):
        self._store = store
        self._default_timeout = default_timeout
        self._client: httpx.AsyncClient | None = None
        self._rate_limiter = RateLimiter()
        self._transform_engine = TransformEngine()
        self._circuit_breakers: dict[str, CircuitBreaker] = {}

    def _record_metric(self, metric_type: str, **kwargs: Any) -> None:
        """Record a metric event. Non-blocking — ignores errors."""
        try:
            from .metrics import get_metrics
            m = get_metrics()
            if metric_type == "delivery_success":
                m.inc_delivery_success()
                if kwargs.get("duration_ms"):
                    m.observe_duration(kwargs["duration_ms"])
            elif metric_type == "delivery_failed":
                m.inc_delivery_failed()
                if kwargs.get("duration_ms"):
                    m.observe_duration(kwargs["duration_ms"])
            elif metric_type == "delivery_abandoned":
                m.inc_delivery_abandoned()
            elif metric_type == "delivery_dead_letter":
                m.inc_delivery_dead_letter()
            elif metric_type == "delivery_retried":
                m.inc_delivery_retried()
            elif metric_type == "delivery_created":
                m.inc_deliveries_created()
            elif metric_type == "rate_limited":
                m.inc_rate_limited()
        except Exception:
            pass

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            self._client = httpx.AsyncClient(timeout=self._default_timeout)
        return self._client

    async def close(self) -> None:
        if self._client and not self._client.is_closed:
            await self._client.aclose()

    @staticmethod
    def generate_hmac_signature(secret: str, payload: str, algorithm: str = "sha256") -> str:
        """Generate HMAC signature for a payload."""
        if algorithm == "sha256":
            digest = hmac.new(secret.encode(), payload.encode(), hashlib.sha256)
        elif algorithm == "sha512":
            digest = hmac.new(secret.encode(), payload.encode(), hashlib.sha512)
        elif algorithm == "sha1":
            digest = hmac.new(secret.encode(), payload.encode(), hashlib.sha1)
        else:
            raise ValueError(f"Unsupported algorithm: {algorithm}")
        return f"{algorithm}={digest.hexdigest()}"

    @staticmethod
    def build_headers(
        endpoint: WebhookEndpoint,
        delivery: WebhookDelivery,
        signature: str | None = None,
    ) -> dict[str, str]:
        """Build headers for a webhook delivery."""
        headers = {
            "Content-Type": "application/json",
            "User-Agent": "agent-webhook/0.6.0",
            "X-Webhook-ID": delivery.id,
            "X-Webhook-Event": delivery.event_type or "generic",
            "X-Webhook-Timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if signature:
            headers["X-Webhook-Signature"] = signature
        # Add endpoint-level custom headers
        for h in endpoint.headers:
            headers[h.name] = h.value
        # Add delivery-level headers (override endpoint-level)
        headers.update(delivery.payload_headers)
        return headers

    def _apply_transforms(self, payload: dict[str, Any], endpoint: WebhookEndpoint) -> dict[str, Any]:
        """Apply payload transforms configured for an endpoint."""
        if not endpoint.transform_ids:
            return payload

        # Load transforms from store if available
        transforms = []
        if hasattr(self._store, "get_transform"):
            for tid in endpoint.transform_ids:
                t = self._store.get_transform(tid)
                if t is not None:
                    transforms.append(t)

        if not transforms:
            return payload

        return self._transform_engine.apply(payload, transforms)

    def _check_rate_limit(self, endpoint: WebhookEndpoint) -> bool:
        """Check if the endpoint is within rate limits. Returns True if allowed."""
        if endpoint.rate_limit is None:
            return True
        return self._rate_limiter.is_allowed(endpoint.id, endpoint.rate_limit)

    def _get_circuit_breaker(self, endpoint: WebhookEndpoint) -> CircuitBreaker:
        """Get or create a circuit breaker for an endpoint."""
        if endpoint.id not in self._circuit_breakers:
            config = CircuitBreakerConfig()
            if endpoint.circuit_breaker_config:
                config = CircuitBreakerConfig.from_dict(endpoint.circuit_breaker_config)
            self._circuit_breakers[endpoint.id] = CircuitBreaker(config)
        return self._circuit_breakers[endpoint.id]

    def _check_circuit_breaker(self, endpoint: WebhookEndpoint) -> bool:
        """Check if the circuit breaker allows a delivery. Returns True if allowed."""
        if not endpoint.circuit_breaker_enabled:
            return True
        breaker = self._get_circuit_breaker(endpoint)
        return breaker.is_allowed(endpoint.id)

    def get_circuit_breaker_state(self, endpoint_id: str) -> dict[str, Any] | None:
        """Get circuit breaker state for an endpoint."""
        if endpoint_id not in self._circuit_breakers:
            return None
        return self._circuit_breakers[endpoint_id].get_state(endpoint_id)

    def get_all_circuit_breaker_states(self) -> list[dict[str, Any]]:
        """Get circuit breaker states for all endpoints with breakers."""
        results = []
        for eid, breaker in self._circuit_breakers.items():
            results.append(breaker.get_state(eid))
        return results

    def reset_circuit_breaker(self, endpoint_id: str) -> dict[str, Any] | None:
        """Reset (force close) the circuit breaker for an endpoint."""
        if endpoint_id not in self._circuit_breakers:
            return None
        return self._circuit_breakers[endpoint_id].reset(endpoint_id)

    async def deliver(self, delivery: WebhookDelivery) -> DeliveryAttempt:
        """Execute a single delivery attempt."""
        endpoint = self._store.get_endpoint(delivery.endpoint_id)
        if endpoint is None:
            attempt = DeliveryAttempt(
                delivery_id=delivery.id,
                attempt_number=delivery.current_attempt_number() + 1,
                status=DeliveryStatus.FAILED,
                error_message=f"Endpoint {delivery.endpoint_id} not found",
                started_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
            )
            return attempt

        if not endpoint.is_active():
            attempt = DeliveryAttempt(
                delivery_id=delivery.id,
                attempt_number=delivery.current_attempt_number() + 1,
                status=DeliveryStatus.FAILED,
                error_message=f"Endpoint '{endpoint.name}' is {endpoint.status.value}",
                started_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
            )
            return attempt

        # Check circuit breaker
        if not self._check_circuit_breaker(endpoint):
            cb_state = self._get_circuit_breaker(endpoint).get_state(endpoint.id)
            attempt = DeliveryAttempt(
                delivery_id=delivery.id,
                attempt_number=delivery.current_attempt_number() + 1,
                status=DeliveryStatus.FAILED,
                error_message=f"Circuit breaker open for endpoint '{endpoint.name}' (state: {cb_state['state']})",
                started_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
            )
            return attempt

        # Check rate limit
        if not self._check_rate_limit(endpoint):
            self._record_metric("rate_limited")
            attempt = DeliveryAttempt(
                delivery_id=delivery.id,
                attempt_number=delivery.current_attempt_number() + 1,
                status=DeliveryStatus.FAILED,
                error_message=f"Rate limit exceeded for endpoint '{endpoint.name}'",
                started_at=datetime.now(timezone.utc),
                completed_at=datetime.now(timezone.utc),
            )
            return attempt

        # Apply transforms
        transformed_payload = self._apply_transforms(delivery.payload, endpoint)
        if transformed_payload != delivery.payload:
            delivery.transformed_payload = transformed_payload
            self._store.update_delivery(delivery.id, transformed_payload=transformed_payload)

        payload_str = json.dumps(transformed_payload, default=str)

        # Generate HMAC signature if secret is configured
        signature = None
        if endpoint.secret:
            signature = self.generate_hmac_signature(endpoint.secret, payload_str, algorithm=endpoint.signing_algorithm.value)

        headers = self.build_headers(endpoint, delivery, signature)

        attempt = DeliveryAttempt(
            delivery_id=delivery.id,
            attempt_number=delivery.current_attempt_number() + 1,
            status=DeliveryStatus.IN_PROGRESS,
            started_at=datetime.now(timezone.utc),
        )

        try:
            client = await self._get_client()
            start = time.monotonic()
            response = await client.request(
                method=endpoint.method.value,
                url=endpoint.url,
                content=payload_str,
                headers=headers,
                timeout=endpoint.timeout_seconds,
            )
            duration_ms = (time.monotonic() - start) * 1000

            attempt.duration_ms = round(duration_ms, 2)
            attempt.response_status_code = response.status_code
            attempt.response_body = response.text[:10000]  # Truncate large responses
            attempt.response_headers = dict(response.headers)
            attempt.completed_at = datetime.now(timezone.utc)

            # Determine if successful
            if 200 <= response.status_code < 300:
                attempt.status = DeliveryStatus.SUCCESS
            elif response.status_code in endpoint.retry_policy.retry_on_status_codes:
                attempt.status = DeliveryStatus.FAILED
                attempt.error_message = f"Retryable status code: {response.status_code}"
            else:
                attempt.status = DeliveryStatus.FAILED
                attempt.error_message = f"Non-retryable status code: {response.status_code}"

        except httpx.TimeoutException as e:
            attempt.status = DeliveryStatus.FAILED
            attempt.error_message = f"Timeout: {type(e).__name__}"
            attempt.completed_at = datetime.now(timezone.utc)
        except httpx.RequestError as e:
            attempt.status = DeliveryStatus.FAILED
            attempt.error_message = f"Request error: {type(e).__name__}: {e}"
            attempt.completed_at = datetime.now(timezone.utc)
        except Exception as e:
            attempt.status = DeliveryStatus.FAILED
            attempt.error_message = f"Unexpected error: {type(e).__name__}: {e}"
            attempt.completed_at = datetime.now(timezone.utc)

        return attempt

    def _move_to_dead_letter(self, delivery: WebhookDelivery, endpoint: WebhookEndpoint, reason: str) -> None:
        """Move a failed delivery to the dead letter queue."""
        last_attempt = delivery.last_attempt()
        entry = DeadLetterEntry(
            delivery_id=delivery.id,
            endpoint_id=delivery.endpoint_id,
            payload=delivery.payload,
            transformed_payload=delivery.transformed_payload,
            event_type=delivery.event_type,
            reason=reason,
            last_status_code=last_attempt.response_status_code if last_attempt else None,
            last_error=last_attempt.error_message if last_attempt else None,
            total_attempts=delivery.current_attempt_number(),
        )

        if hasattr(self._store, "add_dead_letter"):
            self._store.add_dead_letter(entry)

        delivery.status = DeliveryStatus.DEAD_LETTER
        delivery.dead_letter_reason = reason
        delivery.dead_lettered_at = datetime.now(timezone.utc)
        self._store.update_delivery(
            delivery.id,
            status=DeliveryStatus.DEAD_LETTER,
            dead_letter_reason=reason,
            dead_lettered_at=delivery.dead_lettered_at,
        )

    async def process_delivery(self, delivery_id: str) -> WebhookDelivery | None:
        """Process a delivery: execute attempt and handle retry logic."""
        delivery = self._store.get_delivery(delivery_id)
        if delivery is None:
            return None

        endpoint = self._store.get_endpoint(delivery.endpoint_id)
        if endpoint is None:
            delivery.status = DeliveryStatus.ABANDONED
            self._store.update_delivery(delivery.id, status=DeliveryStatus.ABANDONED)
            return delivery

        # Execute attempt
        attempt = await self.deliver(delivery)
        # NOTE: add_delivery_attempt appends to the delivery internally,
        # so we must NOT also append to delivery.attempts here — doing so
        # with the JSON store (which returns the same object reference)
        # would double-count the attempt and exhaust retries prematurely.
        self._store.add_delivery_attempt(delivery.id, attempt)
        # Refresh delivery to reflect the attempt count from the store
        delivery = self._store.get_delivery(delivery_id)
        if delivery is None:
            return None

        if attempt.status == DeliveryStatus.SUCCESS:
            delivery.status = DeliveryStatus.SUCCESS
            self._store.update_delivery(delivery.id, status=DeliveryStatus.SUCCESS)
            self._record_metric("delivery_success", duration_ms=attempt.duration_ms or 0)
            # Record success to circuit breaker
            if endpoint.circuit_breaker_enabled:
                breaker = self._get_circuit_breaker(endpoint)
                breaker.record_success(endpoint.id)
        else:
            # Record failure to circuit breaker
            if endpoint.circuit_breaker_enabled:
                breaker = self._get_circuit_breaker(endpoint)
                breaker.record_failure(endpoint.id)
            # Check if we can retry
            retry_policy = endpoint.retry_policy
            if delivery.can_retry(retry_policy):
                delay = retry_policy.delay_for_attempt(attempt.attempt_number)
                from datetime import timedelta
                next_retry_dt = datetime.now(timezone.utc) + timedelta(seconds=delay)
                delivery.status = DeliveryStatus.RETRYING
                delivery.next_retry_at = next_retry_dt
                self._store.update_delivery(
                    delivery.id,
                    status=DeliveryStatus.RETRYING,
                    next_retry_at=next_retry_dt,
                )
                self._record_metric("delivery_retried")
            else:
                # Move to dead letter queue instead of just abandoning
                reason = f"Max retries ({retry_policy.max_retries}) exceeded"
                if attempt.error_message:
                    reason += f": {attempt.error_message}"
                self._move_to_dead_letter(delivery, endpoint, reason)
                self._record_metric("delivery_dead_letter")

        return self._store.get_delivery(delivery_id)

    async def send(
        self,
        endpoint_id: str,
        payload: dict[str, Any],
        event_type: str | None = None,
        metadata: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> WebhookDelivery:
        """Create and immediately process a webhook delivery."""
        delivery = WebhookDelivery(
            endpoint_id=endpoint_id,
            payload=payload,
            event_type=event_type,
            metadata=metadata or {},
            payload_headers=headers or {},
        )
        self._store.add_delivery(delivery)
        result = await self.process_delivery(delivery.id)
        return result or delivery

    async def process_pending(self) -> list[WebhookDelivery]:
        """Process all pending/ready deliveries."""
        pending = self._store.pending_deliveries()
        results = []
        for delivery in pending:
            result = await self.process_delivery(delivery.id)
            if result:
                results.append(result)
        return results

    def apply_relay_rules(
        self,
        path: str,
        method: str,
        headers: dict[str, str],
        body: dict[str, Any] | str | None,
        query_params: dict[str, str] | None = None,
        source_ip: str | None = None,
        raw_body: bytes | str | None = None,
    ) -> list[str]:
        """Apply relay rules to an incoming webhook and create deliveries.

        If a relay rule has verify_signature=True, the incoming webhook's
        HMAC signature is verified before forwarding. If verification fails,
        the webhook is not forwarded.

        Args:
            raw_body: The raw request body bytes (before JSON parsing).
                Required for signature verification. If not provided and
                verification is needed, body will be JSON-serialized.
        """
        from .models import IncomingWebhook
        from .signature import SignatureVerifier, SignatureError

        # Record incoming webhook
        incoming = IncomingWebhook(
            path=path,
            method=method,
            headers=headers,
            body=body,
            query_params=query_params or {},
            source_ip=source_ip,
        )
        self._store.add_incoming(incoming)

        # Find matching relay rules
        rules = self._store.list_relay_rules(active_only=True)
        matching_rules = [r for r in rules if r.matches_path(path)]

        if not matching_rules:
            incoming.processed = True
            return []

        # Create deliveries for each target endpoint
        delivery_ids = []
        payload = body if isinstance(body, dict) else {"raw_body": body}

        for rule in matching_rules:
            # Verify signature if configured
            if rule.verify_signature and rule.verify_secret:
                verifier = SignatureVerifier(tolerance_seconds=rule.verify_tolerance_seconds)

                # Use raw_body if provided, otherwise serialize body
                sig_body = raw_body
                if sig_body is None:
                    import json as _json
                    sig_body = _json.dumps(body) if body is not None else ""

                try:
                    verifier.verify_or_raise(
                        raw_body=sig_body,
                        headers=headers,
                        secret=rule.verify_secret,
                        provider=rule.verify_provider,
                        algorithm=rule.verify_algorithm,
                    )
                except SignatureError:
                    # Signature verification failed — skip this rule
                    incoming.tags.append(f"sig_verification_failed:{rule.id}")
                    continue

            # Apply filter rules if configured
            if rule.filter_rules:
                from .filters import evaluate_filter
                if not evaluate_filter(rule.filter_rules, headers, payload):
                    incoming.tags.append(f"filter_rejected:{rule.id}")
                    continue

            for endpoint_id in rule.target_endpoint_ids:
                endpoint = self._store.get_endpoint(endpoint_id)
                if endpoint is None or not endpoint.is_active():
                    continue
                delivery = WebhookDelivery(
                    endpoint_id=endpoint_id,
                    payload=payload,
                    event_type=f"relay:{rule.name}",
                    metadata={"incoming_id": incoming.id, "rule_id": rule.id, "path": path},
                )
                self._store.add_delivery(delivery)
                delivery_ids.append(delivery.id)
                incoming.forwarded_to.append(endpoint_id)

        incoming.processed = True
        return delivery_ids
