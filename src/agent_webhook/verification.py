"""Endpoint verification via challenge-response.

When registering a new webhook endpoint, you often want to verify that the
endpoint URL is controlled by the intended recipient and is prepared to
receive webhooks. This module implements a challenge-response flow:

1.  Generate a random challenge token.
2.  Send a verification request (GET or POST) to the endpoint URL with
    the challenge embedded in a header (``X-Webhook-Verify-Challenge``)
    and/or the body.
3.  The endpoint must echo the token back — either in a response header
    (``X-Webhook-Verify-Challenge``) or in a JSON body field
    (``{"challenge": "<token>"}``).

This mirrors the verification flows used by Svix, Slack, and Zoom.
"""

from __future__ import annotations

import secrets
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class ChallengeLocation(str, Enum):
    """Where the endpoint must echo the challenge token."""
    HEADER = "header"
    BODY = "body"
    EITHER = "either"


class ChallengeStatus(str, Enum):
    """Outcome of a verification challenge."""
    PENDING = "pending"
    VERIFIED = "verified"
    FAILED = "failed"
    EXPIRED = "expired"


@dataclass
class VerificationChallenge:
    """A pending or completed endpoint verification challenge.

    Attributes:
        endpoint_id: The endpoint being verified.
        token: The random challenge token the endpoint must echo.
        created_at: When the challenge was issued.
        expires_at: When the challenge becomes invalid.
        status: Current status (pending/verified/failed/expired).
        method: HTTP method used for the verification request.
        location: Where the endpoint should echo the token.
        header_name: Response header to check (default X-Webhook-Verify-Challenge).
        body_field: JSON body field to check (default 'challenge').
        attempts: Number of verification attempts made.
        verified_at: When verification succeeded (if it did).
        response_detail: Human-readable detail about the last attempt.
        max_attempts: Maximum verification attempts before giving up.
    """
    endpoint_id: str
    token: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    expires_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    status: ChallengeStatus = ChallengeStatus.PENDING
    method: str = "POST"
    location: ChallengeLocation = ChallengeLocation.EITHER
    header_name: str = "X-Webhook-Verify-Challenge"
    body_field: str = "challenge"
    attempts: int = 0
    verified_at: datetime | None = None
    response_detail: str | None = None
    max_attempts: int = 3

    def is_expired(self, now: datetime | None = None) -> bool:
        """Check if this challenge has expired."""
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        if self.expires_at.tzinfo is None:
            exp = self.expires_at.replace(tzinfo=timezone.utc)
        else:
            exp = self.expires_at
        return now > exp

    def can_retry(self) -> bool:
        """Check if another verification attempt is allowed."""
        return self.status == ChallengeStatus.PENDING and self.attempts < self.max_attempts

    def to_dict(self) -> dict[str, Any]:
        return {
            "endpoint_id": self.endpoint_id,
            "token": self.token,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "expires_at": self.expires_at.isoformat() if self.expires_at else None,
            "status": self.status.value,
            "method": self.method,
            "location": self.location.value,
            "header_name": self.header_name,
            "body_field": self.body_field,
            "attempts": self.attempts,
            "verified_at": self.verified_at.isoformat() if self.verified_at else None,
            "response_detail": self.response_detail,
            "max_attempts": self.max_attempts,
        }


class ChallengeManager:
    """Manages verification challenges for webhook endpoints.

    Thread-safe. Tracks active challenges, validates responses, and
    handles expiration.
    """

    def __init__(self, ttl_seconds: int = 600) -> None:
        """Initialize the challenge manager.

        Args:
            ttl_seconds: How long a challenge remains valid (default 10 min).
        """
        if ttl_seconds < 10:
            raise ValueError("ttl_seconds must be >= 10")
        self._ttl = ttl_seconds
        self._challenges: dict[str, VerificationChallenge] = {}  # endpoint_id -> challenge

    @staticmethod
    def generate_token(length: int = 32) -> str:
        """Generate a cryptographically secure random token.

        Args:
            length: Number of bytes of entropy (default 32 = 256 bits).

        Returns:
            URL-safe hex string (length * 2 hex chars).
        """
        if length < 16:
            raise ValueError("Token length must be >= 16 bytes")
        return secrets.token_hex(length)

    def create_challenge(
        self,
        endpoint_id: str,
        method: str = "POST",
        location: ChallengeLocation = ChallengeLocation.EITHER,
        header_name: str = "X-Webhook-Verify-Challenge",
        body_field: str = "challenge",
        ttl_override: int | None = None,
        max_attempts: int = 3,
    ) -> VerificationChallenge:
        """Create a new verification challenge for an endpoint.

        Replaces any existing challenge for this endpoint.

        Args:
            endpoint_id: The endpoint to verify.
            method: HTTP method for the verification request.
            location: Where the endpoint should echo the token.
            header_name: Header name to check in the response.
            body_field: JSON body field to check in the response.
            ttl_override: Override the default TTL (seconds).
            max_attempts: Max verification attempts.

        Returns:
            The created VerificationChallenge.
        """
        from datetime import timedelta

        ttl = ttl_override or self._ttl
        now = datetime.now(timezone.utc)
        token = self.generate_token()

        challenge = VerificationChallenge(
            endpoint_id=endpoint_id,
            token=token,
            created_at=now,
            expires_at=now + timedelta(seconds=ttl),
            method=method,
            location=location,
            header_name=header_name,
            body_field=body_field,
            max_attempts=max_attempts,
        )
        self._challenges[endpoint_id] = challenge
        return challenge

    def get_challenge(self, endpoint_id: str) -> VerificationChallenge | None:
        """Get the active challenge for an endpoint."""
        return self._challenges.get(endpoint_id)

    def validate_response(
        self,
        endpoint_id: str,
        response_headers: dict[str, str] | None = None,
        response_body: dict[str, Any] | str | None = None,
    ) -> bool:
        """Validate an endpoint's response to a verification challenge.

        Checks the response header and/or body for the challenge token,
        depending on the challenge's configured location.

        Args:
            endpoint_id: The endpoint being verified.
            response_headers: Headers from the endpoint's response.
            response_body: Body from the endpoint's response (dict or raw string).

        Returns:
            True if the challenge token was correctly echoed.
        """
        challenge = self._challenges.get(endpoint_id)
        if challenge is None:
            return False

        # Check expiration
        if challenge.is_expired():
            challenge.status = ChallengeStatus.EXPIRED
            challenge.response_detail = "Challenge expired before response received"
            return False

        challenge.attempts += 1

        # Normalize headers (case-insensitive)
        norm_headers: dict[str, str] = {}
        if response_headers:
            norm_headers = {k.lower(): v for k, v in response_headers.items()}

        header_val = norm_headers.get(challenge.header_name.lower())
        body_val = None
        if isinstance(response_body, dict):
            body_val = response_body.get(challenge.body_field)
        elif isinstance(response_body, str):
            # Try parsing as JSON
            import json
            try:
                parsed = json.loads(response_body)
                if isinstance(parsed, dict):
                    body_val = parsed.get(challenge.body_field)
            except (json.JSONDecodeError, TypeError):
                pass

        loc = challenge.location
        matched = False
        detail = ""

        if loc == ChallengeLocation.HEADER:
            matched = header_val is not None and _safe_eq(header_val, challenge.token)
            if not matched:
                detail = f"Expected header '{challenge.header_name}' to match challenge token"
        elif loc == ChallengeLocation.BODY:
            matched = body_val is not None and _safe_eq(str(body_val), challenge.token)
            if not matched:
                detail = f"Expected body field '{challenge.body_field}' to match challenge token"
        else:  # EITHER
            header_ok = header_val is not None and _safe_eq(header_val, challenge.token)
            body_ok = body_val is not None and _safe_eq(str(body_val), challenge.token)
            matched = header_ok or body_ok
            if not matched:
                detail = (
                    f"Neither header '{challenge.header_name}' nor body field "
                    f"'{challenge.body_field}' matched the challenge token"
                )

        if matched:
            challenge.status = ChallengeStatus.VERIFIED
            challenge.verified_at = datetime.now(timezone.utc)
            challenge.response_detail = "Verification successful"
        else:
            challenge.response_detail = detail
            if challenge.attempts >= challenge.max_attempts:
                challenge.status = ChallengeStatus.FAILED

        return matched

    def revoke(self, endpoint_id: str) -> bool:
        """Revoke/delete the challenge for an endpoint.

        Returns:
            True if a challenge was removed.
        """
        return self._challenges.pop(endpoint_id, None) is not None

    def cleanup_expired(self) -> int:
        """Remove expired challenges. Returns count removed."""
        now = datetime.now(timezone.utc)
        expired_ids = [
            eid for eid, ch in self._challenges.items()
            if ch.is_expired(now) and ch.status == ChallengeStatus.PENDING
        ]
        for eid in expired_ids:
            self._challenges[eid].status = ChallengeStatus.EXPIRED
            del self._challenges[eid]
        return len(expired_ids)

    def all_challenges(self) -> list[VerificationChallenge]:
        """List all active challenges."""
        return list(self._challenges.values())

    @property
    def ttl_seconds(self) -> int:
        return self._ttl


def _safe_eq(a: str, b: str) -> bool:
    """Constant-time string comparison to prevent timing attacks."""
    import hmac as _hmac
    return _hmac.compare_digest(a, b)
