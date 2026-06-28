"""Webhook delivery engine — handles HTTP delivery with retries and HMAC signing."""

from __future__ import annotations

import hashlib
import hmac
import json
import time
from datetime import datetime, timezone
from typing import Any

import httpx

from .models import (
    DeliveryAttempt,
    DeliveryStatus,
    RelayRule,
    RetryPolicy,
    WebhookDelivery,
    WebhookEndpoint,
    WebhookStatus,
)
from .store import WebhookStore


class DeliveryEngine:
    """Handles webhook delivery with retries, HMAC signing, and execution tracking."""

    def __init__(self, store: WebhookStore, default_timeout: float = 30.0):
        self._store = store
        self._default_timeout = default_timeout
        self._client: httpx.AsyncClient | None = None

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
            "User-Agent": "agent-webhook/0.2.0",
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

        payload_str = json.dumps(delivery.payload, default=str)

        # Generate HMAC signature if secret is configured
        signature = None
        if endpoint.secret:
            signature = self.generate_hmac_signature(endpoint.secret, payload_str)

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
        self._store.add_delivery_attempt(delivery.id, attempt)

        if attempt.status == DeliveryStatus.SUCCESS:
            delivery.status = DeliveryStatus.SUCCESS
            self._store.update_delivery(delivery.id, status=DeliveryStatus.SUCCESS)
        else:
            # Check if we can retry
            retry_policy = endpoint.retry_policy
            if delivery.can_retry(retry_policy):
                delay = retry_policy.delay_for_attempt(attempt.attempt_number)
                next_retry = datetime.now(timezone.utc).timestamp() + delay
                from datetime import timedelta
                next_retry_dt = datetime.now(timezone.utc) + timedelta(seconds=delay)
                delivery.status = DeliveryStatus.RETRYING
                delivery.next_retry_at = next_retry_dt
                self._store.update_delivery(
                    delivery.id,
                    status=DeliveryStatus.RETRYING,
                    next_retry_at=next_retry_dt,
                )
            else:
                delivery.status = DeliveryStatus.ABANDONED
                self._store.update_delivery(delivery.id, status=DeliveryStatus.ABANDONED)

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
    ) -> list[str]:
        """Apply relay rules to an incoming webhook and create deliveries."""
        from .models import IncomingWebhook

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
