"""Incoming webhook signature verification.

Verifies HMAC signatures on incoming webhooks from major providers:
  - Generic HMAC (sha256/sha1)
  - GitHub (X-Hub-Signature-256 / X-Hub-Signature)
  - Stripe (Stripe-Signature)
  - Slack (X-Slack-Signature)
  - Shopify (X-Shopify-Hmac-SHA256)

Supports replay attack prevention via timestamp validation.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import time
from typing import Any

from .models import WebhookEndpoint


class SignatureError(Exception):
    """Raised when webhook signature verification fails."""


class SignatureVerifier:
    """Verifies HMAC signatures on incoming webhooks.

    This is used by the relay server to verify that incoming webhooks
    are authentic before forwarding them to registered endpoints.
    """

    # Provider-specific header names
    GITHUB_HEADERS = ["X-Hub-Signature-256", "X-Hub-Signature"]
    STRIPE_HEADER = "Stripe-Signature"
    SLACK_HEADER = "X-Slack-Signature"
    SLACK_TIMESTAMP_HEADER = "X-Slack-Request-Timestamp"
    SHOPIFY_HEADER = "X-Shopify-Hmac-SHA256"
    GENERIC_HEADERS = ["X-Webhook-Signature", "X-Signature"]

    def __init__(self, tolerance_seconds: int = 300) -> None:
        """Initialize the verifier.

        Args:
            tolerance_seconds: Maximum age (in seconds) for timestamps to
                prevent replay attacks. Default 300 (5 minutes).
        """
        if tolerance_seconds < 0:
            raise ValueError("tolerance_seconds must be >= 0")
        self._tolerance = tolerance_seconds

    # ── Public API ─────────────────────────────────────────────────

    def verify(
        self,
        raw_body: bytes | str,
        headers: dict[str, str],
        secret: str,
        provider: str = "generic",
        algorithm: str = "sha256",
    ) -> bool:
        """Verify a webhook signature.

        Args:
            raw_body: The raw request body (bytes or str).
            headers: Request headers (case-insensitive lookup performed).
            secret: The shared secret for HMAC verification.
            provider: Provider type for provider-specific verification logic.
                One of: 'generic', 'github', 'stripe', 'slack', 'shopify'.
            algorithm: HMAC algorithm for generic verification ('sha256' or 'sha1').

        Returns:
            True if signature is valid.

        Raises:
            SignatureError: If signature is missing, malformed, or doesn't match.
        """
        normalized_headers = self._normalize_headers(headers)
        body_bytes = raw_body if isinstance(raw_body, bytes) else raw_body.encode("utf-8")

        if provider == "github":
            return self._verify_github(body_bytes, normalized_headers, secret)
        elif provider == "stripe":
            return self._verify_stripe(body_bytes, normalized_headers, secret)
        elif provider == "slack":
            return self._verify_slack(body_bytes, normalized_headers, secret)
        elif provider == "shopify":
            return self._verify_shopify(body_bytes, normalized_headers, secret)
        else:
            return self._verify_generic(body_bytes, normalized_headers, secret, algorithm)

    def verify_or_raise(
        self,
        raw_body: bytes | str,
        headers: dict[str, str],
        secret: str,
        provider: str = "generic",
        algorithm: str = "sha256",
    ) -> None:
        """Verify and raise SignatureError on failure."""
        if not self.verify(raw_body, headers, secret, provider, algorithm):
            raise SignatureError(f"Signature verification failed for provider: {provider}")

    def detect_provider(self, headers: dict[str, str]) -> str | None:
        """Auto-detect the webhook provider from request headers.

        Returns the provider name ('github', 'stripe', 'slack', 'shopify')
        or None if the provider can't be determined.
        """
        normalized = self._normalize_headers(headers)

        # GitHub
        for h in self.GITHUB_HEADERS:
            if h.lower() in normalized:
                return "github"

        # Stripe
        if self.STRIPE_HEADER.lower() in normalized:
            return "stripe"

        # Slack
        if self.SLACK_HEADER.lower() in normalized:
            return "slack"

        # Shopify
        if self.SHOPIFY_HEADER.lower() in normalized:
            return "shopify"

        return None

    def generate_signature(
        self,
        raw_body: bytes | str,
        secret: str,
        algorithm: str = "sha256",
        provider: str = "generic",
        timestamp: int | None = None,
    ) -> str:
        """Generate a signature for outgoing/incoming webhook testing.

        Useful for testing relay verification.
        """
        body_bytes = raw_body if isinstance(raw_body, bytes) else raw_body.encode("utf-8")

        if provider == "github":
            algo = hashlib.sha256 if algorithm == "sha256" else hashlib.sha1
            digest = hmac.new(secret.encode(), body_bytes, algo)
            prefix = "sha256=" if algorithm == "sha256" else "sha1="
            return prefix + digest.hexdigest()

        elif provider == "stripe":
            ts = timestamp or int(time.time())
            signed_payload = f"{ts}.".encode() + body_bytes
            digest = hmac.new(secret.encode(), signed_payload, hashlib.sha256)
            return f"t={ts},v1={digest.hexdigest()}"

        elif provider == "slack":
            ts = timestamp or int(time.time())
            base = f"v0:{ts}:".encode() + body_bytes
            digest = hmac.new(secret.encode(), base, hashlib.sha256)
            return "v0=" + digest.hexdigest()

        elif provider == "shopify":
            digest = hmac.new(secret.encode(), body_bytes, hashlib.sha256)
            return digest.hexdigest()

        else:  # generic
            algo = hashlib.sha256 if algorithm == "sha256" else hashlib.sha1
            digest = hmac.new(secret.encode(), body_bytes, algo)
            prefix = "sha256=" if algorithm == "sha256" else "sha1="
            return prefix + digest.hexdigest()

    # ── Provider-Specific Verification ─────────────────────────────

    def _verify_generic(
        self,
        body: bytes,
        headers: dict[str, str],
        secret: str,
        algorithm: str,
    ) -> bool:
        """Verify a generic HMAC signature.

        Supports headers like:
          X-Webhook-Signature: sha256=<hex>
          X-Signature: <hex>
        """
        signature = None
        for h in self.GENERIC_HEADERS:
            if h.lower() in headers:
                signature = headers[h.lower()]
                break

        if signature is None:
            raise SignatureError("No signature header found")

        # Parse algorithm prefix if present (sha256=...)
        sig_algo = algorithm
        clean_sig = signature
        if "=" in signature and signature.split("=")[0] in ("sha256", "sha1", "sha512"):
            parts = signature.split("=", 1)
            sig_algo = parts[0]
            clean_sig = parts[1]

        algo = self._get_hash_algo(sig_algo)
        expected = hmac.new(secret.encode(), body, algo).hexdigest()

        return hmac.compare_digest(clean_sig, expected)

    def _verify_github(self, body: bytes, headers: dict[str, str], secret: str) -> bool:
        """Verify GitHub webhook signature.

        GitHub sends: X-Hub-Signature-256: sha256=<hex>
        Legacy:       X-Hub-Signature: sha1=<hex>
        """
        signature = None
        algorithm = None

        gh256 = "x-hub-signature-256"
        gh1 = "x-hub-signature"

        if gh256 in headers:
            signature = headers[gh256]
            algorithm = "sha256"
        elif gh1 in headers:
            signature = headers[gh1]
            algorithm = "sha1"
        else:
            raise SignatureError("No GitHub signature header found")

        # Parse prefix
        if "=" in signature:
            prefix, sig = signature.split("=", 1)
            if prefix != algorithm:
                raise SignatureError(f"Algorithm mismatch: expected {algorithm}, got {prefix}")
        else:
            sig = signature

        algo = self._get_hash_algo(algorithm)
        expected = hmac.new(secret.encode(), body, algo).hexdigest()
        return hmac.compare_digest(sig, expected)

    def _verify_stripe(self, body: bytes, headers: dict[str, str], secret: str) -> bool:
        """Verify Stripe webhook signature.

        Stripe sends: Stripe-Signature: t=<timestamp>,v1=<hex>
        """
        sig_header = headers.get("stripe-signature")
        if sig_header is None:
            raise SignatureError("No Stripe-Signature header found")

        # Parse the header: t=12345,v1=abc123,v1=def456
        timestamp = None
        signatures: list[str] = []

        for part in sig_header.split(","):
            part = part.strip()
            if "=" not in part:
                continue
            key, value = part.split("=", 1)
            if key == "t":
                try:
                    timestamp = int(value)
                except ValueError:
                    raise SignatureError("Invalid Stripe timestamp")
            elif key == "v1":
                signatures.append(value)

        if timestamp is None:
            raise SignatureError("No timestamp in Stripe signature")
        if not signatures:
            raise SignatureError("No v1 signatures in Stripe header")

        # Check timestamp tolerance (replay protection)
        self._check_timestamp(timestamp, "Stripe")

        # Compute expected signature
        signed_payload = f"{timestamp}.".encode() + body
        expected = hmac.new(secret.encode(), signed_payload, hashlib.sha256).hexdigest()

        # Check against all provided signatures
        for sig in signatures:
            if hmac.compare_digest(sig, expected):
                return True

        raise SignatureError("Stripe signature mismatch")

    def _verify_slack(self, body: bytes, headers: dict[str, str], secret: str) -> bool:
        """Verify Slack webhook signature.

        Slack uses: X-Slack-Signature: v0=<hex>
                    X-Slack-Request-Timestamp: <unix_timestamp>

        Signing base string: v0:<timestamp>:<body>
        """
        sig_header = headers.get("x-slack-signature")
        ts_header = headers.get("x-slack-request-timestamp")

        if sig_header is None:
            raise SignatureError("No X-Slack-Signature header found")
        if ts_header is None:
            raise SignatureError("No X-Slack-Request-Timestamp header found")

        try:
            timestamp = int(ts_header)
        except ValueError:
            raise SignatureError("Invalid Slack timestamp")

        # Check timestamp tolerance
        self._check_timestamp(timestamp, "Slack")

        # Parse signature
        if sig_header.startswith("v0="):
            provided_sig = sig_header[3:]
        else:
            provided_sig = sig_header

        # Compute expected: HMAC-SHA256(secret, "v0:<timestamp>:<body>")
        base_string = f"v0:{timestamp}:".encode() + body
        expected = hmac.new(secret.encode(), base_string, hashlib.sha256).hexdigest()

        return hmac.compare_digest(provided_sig, expected)

    def _verify_shopify(self, body: bytes, headers: dict[str, str], secret: str) -> bool:
        """Verify Shopify webhook signature.

        Shopify sends: X-Shopify-Hmac-SHA256: <base64 or hex>
        Uses HMAC-SHA256 with the secret.
        """
        sig_header = headers.get("x-shopify-hmac-sha256")
        if sig_header is None:
            raise SignatureError("No X-Shopify-Hmac-SHA256 header found")

        expected = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()

        # Shopify sometimes sends base64, sometimes hex
        # Try hex first, then base64
        if hmac.compare_digest(sig_header, expected):
            return True

        # Try base64
        import base64
        expected_b64 = base64.b64encode(
            hmac.new(secret.encode(), body, hashlib.sha256).digest()
        ).decode()
        if hmac.compare_digest(sig_header, expected_b64):
            return True

        raise SignatureError("Shopify signature mismatch")

    # ── Helpers ────────────────────────────────────────────────────

    @staticmethod
    def _normalize_headers(headers: dict[str, str]) -> dict[str, str]:
        """Normalize header keys to lowercase for case-insensitive lookup."""
        return {k.lower(): v for k, v in headers.items()}

    @staticmethod
    def _get_hash_algo(algorithm: str):
        if algorithm == "sha256":
            return hashlib.sha256
        elif algorithm == "sha1":
            return hashlib.sha1
        elif algorithm == "sha512":
            return hashlib.sha512
        else:
            raise SignatureError(f"Unsupported algorithm: {algorithm}")

    def _check_timestamp(self, timestamp: int, provider: str) -> None:
        """Check that a timestamp is within tolerance to prevent replay attacks."""
        if self._tolerance == 0:
            return

        current = int(time.time())
        age = abs(current - timestamp)
        if age > self._tolerance:
            raise SignatureError(
                f"{provider} webhook timestamp outside tolerance: "
                f"age={age}s, tolerance={self._tolerance}s"
            )
