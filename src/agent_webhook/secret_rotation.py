"""Multi-secret rotation for zero-downtime webhook signing key rotation.

When rotating HMAC signing secrets, there is a window where deliveries
sent with the old secret have not yet been processed by the receiver.
If the receiver updates their secret at exactly the same time, valid
deliveries will fail signature verification.

The solution is **multi-secret rotation**: during the rotation window,
the endpoint signs each delivery with *both* the old and new secrets
(and verifies against either). Once the rotation is complete, the old
secret is removed.

This module provides:
    - SecretRotationManager: Tracks multiple secrets per endpoint with roles
      (primary, secondary, previous) and lifecycle (active, rotating, retired).
    - Rotation phases: ``initiate → verify → complete → cleanup``
    - Automatic expiry of stale rotation secrets.

Usage in engine:
    For signing: use the primary secret.
    For verification: try all active secrets until one matches.

    When ``verify_inbound=True``, incoming webhooks are verified against
    all non-retired secrets for the endpoint, not just the primary.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any


class SecretRole(str, Enum):
    """Role of a secret in the rotation lifecycle."""
    PRIMARY = "primary"
    SECONDARY = "secondary"
    PREVIOUS = "previous"  # old secret during rotation


class SecretStatus(str, Enum):
    """Lifecycle status of a rotation secret."""
    ACTIVE = "active"
    ROTATING = "rotating"      # new secret being verified
    RETIRED = "retired"        # old secret no longer used
    EXPIRED = "expired"        # auto-expired past grace period


@dataclass
class RotationSecret:
    """A single secret in a rotation set.

    Attributes:
        secret: The actual secret value (HMAC key).
        role: Role in the rotation lifecycle.
        status: Current status.
        activated_at: When this secret became active.
        retired_at: When this secret was retired (if applicable).
        version: Rotation version counter (starts at 1).
    """
    secret: str
    role: SecretRole = SecretRole.PRIMARY
    status: SecretStatus = SecretStatus.ACTIVE
    activated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    retired_at: datetime | None = None
    version: int = 1

    def is_usable_for_signing(self) -> bool:
        """Can this secret be used to sign outgoing webhooks?"""
        return self.status in (SecretStatus.ACTIVE, SecretStatus.ROTATING)

    def is_usable_for_verification(self) -> bool:
        """Can this secret be used to verify incoming webhooks?"""
        return self.status in (SecretStatus.ACTIVE, SecretStatus.ROTATING, SecretStatus.RETIRED)

    def is_expired(self, grace_seconds: int, now: datetime | None = None) -> bool:
        """Check if a retired secret has exceeded its grace period."""
        if self.status != SecretStatus.RETIRED or self.retired_at is None:
            return False
        now = now or datetime.now(timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        retired = self.retired_at
        if retired.tzinfo is None:
            retired = retired.replace(tzinfo=timezone.utc)
        return (now - retired).total_seconds() > grace_seconds

    def to_dict(self) -> dict[str, Any]:
        return {
            "secret": "***REDACTED***",
            "role": self.role.value,
            "status": self.status.value,
            "activated_at": self.activated_at.isoformat() if self.activated_at else None,
            "retired_at": self.retired_at.isoformat() if self.retired_at else None,
            "version": self.version,
        }


class SecretRotationManager:
    """Manages multi-secret rotation for webhook endpoints.

    Rotation flow::

        manager.initiate_rotation(endpoint_id, new_secret)
        # → old primary becomes PREVIOUS (still verifies), new becomes PRIMARY (ROTATING)
        # → deliveries are signed with PRIMARY (new secret)

        manager.verify_rotation(endpoint_id)
        # → new secret is confirmed working, PRIMARY becomes ACTIVE
        # → old PREVIOUS secret becomes RETIRED (still verifies during grace)

        manager.complete_rotation(endpoint_id)
        # → RETIRED secret is removed after grace period
    """

    def __init__(self, grace_period_seconds: int = 86400) -> None:
        """Initialize the rotation manager.

        Args:
            grace_period_seconds: How long retired secrets remain usable
                for verification (default 24 hours).
        """
        if grace_period_seconds < 60:
            raise ValueError("grace_period_seconds must be >= 60")
        self._grace = grace_period_seconds
        # endpoint_id -> list of RotationSecret (chronological)
        self._secrets: dict[str, list[RotationSecret]] = {}
        self._rotation_version: dict[str, int] = {}

    def register_secret(self, endpoint_id: str, secret: str) -> RotationSecret:
        """Register the initial primary secret for an endpoint.

        Args:
            endpoint_id: The endpoint ID.
            secret: The HMAC secret value.

        Returns:
            The created RotationSecret.
        """
        version = self._rotation_version.get(endpoint_id, 0) + 1
        self._rotation_version[endpoint_id] = version
        rs = RotationSecret(
            secret=secret,
            role=SecretRole.PRIMARY,
            status=SecretStatus.ACTIVE,
            version=version,
        )
        self._secrets[endpoint_id] = [rs]
        return rs

    def initiate_rotation(self, endpoint_id: str, new_secret: str) -> RotationSecret:
        """Begin rotating to a new secret.

        The current primary becomes PREVIOUS (still usable for verification).
        The new secret becomes the PRIMARY with ROTATING status.

        Args:
            endpoint_id: The endpoint ID.
            new_secret: The new HMAC secret value.

        Returns:
            The new primary RotationSecret.

        Raises:
            KeyError: If the endpoint has no existing secrets.
        """
        if endpoint_id not in self._secrets or not self._secrets[endpoint_id]:
            raise KeyError(f"No existing secrets for endpoint {endpoint_id}")

        secrets_list = self._secrets[endpoint_id]

        # Demote current primary to PREVIOUS
        for s in secrets_list:
            if s.role == SecretRole.PRIMARY:
                s.role = SecretRole.PREVIOUS
                # Keep ACTIVE status for verification during rotation

        version = self._rotation_version.get(endpoint_id, 0) + 1
        self._rotation_version[endpoint_id] = version

        # Create new primary (in ROTATING status until verified)
        new_rs = RotationSecret(
            secret=new_secret,
            role=SecretRole.PRIMARY,
            status=SecretStatus.ROTATING,
            version=version,
        )
        secrets_list.append(new_rs)
        return new_rs

    def verify_rotation(self, endpoint_id: str) -> bool:
        """Confirm that the new secret is working correctly.

        Transitions the new PRIMARY from ROTATING to ACTIVE and
        retires the old PREVIOUS secret.

        Args:
            endpoint_id: The endpoint ID.

        Returns:
            True if rotation was verified and old secret retired.
        """
        if endpoint_id not in self._secrets:
            return False

        secrets_list = self._secrets[endpoint_id]
        now = datetime.now(timezone.utc)

        verified = False
        for s in secrets_list:
            if s.role == SecretRole.PRIMARY and s.status == SecretStatus.ROTATING:
                s.status = SecretStatus.ACTIVE
                verified = True
            elif s.role == SecretRole.PREVIOUS and s.status == SecretStatus.ACTIVE:
                s.status = SecretStatus.RETIRED
                s.retired_at = now

        return verified

    def complete_rotation(self, endpoint_id: str) -> int:
        """Complete rotation by removing expired retired secrets.

        Args:
            endpoint_id: The endpoint ID.

        Returns:
            Number of secrets cleaned up.
        """
        if endpoint_id not in self._secrets:
            return 0

        secrets_list = self._secrets[endpoint_id]
        now = datetime.now(timezone.utc)
        to_keep = []

        for s in secrets_list:
            if s.is_expired(self._grace, now):
                s.status = SecretStatus.EXPIRED
                # Don't keep expired secrets
            else:
                to_keep.append(s)

        removed = len(secrets_list) - len(to_keep)
        self._secrets[endpoint_id] = to_keep
        return removed

    def get_signing_secret(self, endpoint_id: str) -> str | None:
        """Get the current signing secret (primary, usable for signing).

        Args:
            endpoint_id: The endpoint ID.

        Returns:
            The secret string, or None if no signing secret exists.
        """
        if endpoint_id not in self._secrets:
            return None
        for s in self._secrets[endpoint_id]:
            if s.role == SecretRole.PRIMARY and s.is_usable_for_signing():
                return s.secret
        return None

    def get_signing_secret_info(self, endpoint_id: str) -> dict[str, Any] | None:
        """Get metadata about the current signing secret (no secret value)."""
        if endpoint_id not in self._secrets:
            return None
        for s in self._secrets[endpoint_id]:
            if s.role == SecretRole.PRIMARY and s.is_usable_for_signing():
                return {
                    "version": s.version,
                    "status": s.status.value,
                    "activated_at": s.activated_at.isoformat() if s.activated_at else None,
                }
        return None

    def get_verification_secrets(self, endpoint_id: str) -> list[str]:
        """Get all secrets usable for verification (primary + retired-during-grace).

        Args:
            endpoint_id: The endpoint ID.

        Returns:
            List of secret strings.
        """
        if endpoint_id not in self._secrets:
            return []
        return [
            s.secret for s in self._secrets[endpoint_id]
            if s.is_usable_for_verification()
        ]

    def get_rotation_status(self, endpoint_id: str) -> dict[str, Any]:
        """Get the full rotation status for an endpoint.

        Args:
            endpoint_id: The endpoint ID.

        Returns:
            Dict with secrets list (redacted), rotation version, and grace period.
        """
        if endpoint_id not in self._secrets:
            return {
                "endpoint_id": endpoint_id,
                "secrets": [],
                "rotation_version": 0,
                "grace_period_seconds": self._grace,
                "is_rotating": False,
            }

        secrets_list = self._secrets[endpoint_id]
        is_rotating = any(
            s.role == SecretRole.PRIMARY and s.status == SecretStatus.ROTATING
            for s in secrets_list
        )

        return {
            "endpoint_id": endpoint_id,
            "secrets": [s.to_dict() for s in secrets_list],
            "rotation_version": self._rotation_version.get(endpoint_id, 0),
            "grace_period_seconds": self._grace,
            "is_rotating": is_rotating,
        }

    def cancel_rotation(self, endpoint_id: str) -> bool:
        """Cancel an in-progress rotation, reverting to the previous primary.

        Args:
            endpoint_id: The endpoint ID.

        Returns:
            True if a rotation was cancelled.
        """
        if endpoint_id not in self._secrets:
            return False

        secrets_list = self._secrets[endpoint_id]
        cancelled = False

        # Find and remove the new primary (ROTATING status)
        to_keep = []
        for s in secrets_list:
            if s.role == SecretRole.PRIMARY and s.status == SecretStatus.ROTATING:
                cancelled = True
                # Remove this secret entirely
            else:
                to_keep.append(s)

        # Restore PREVIOUS to PRIMARY
        for s in to_keep:
            if s.role == SecretRole.PREVIOUS:
                s.role = SecretRole.PRIMARY
                s.status = SecretStatus.ACTIVE

        self._secrets[endpoint_id] = to_keep
        if cancelled:
            self._rotation_version[endpoint_id] = max(
                (s.version for s in to_keep), default=0
            )
        return cancelled

    def cleanup_all_expired(self) -> int:
        """Remove all expired retired secrets across all endpoints.

        Returns:
            Total number of secrets cleaned up.
        """
        total = 0
        for eid in list(self._secrets.keys()):
            total += self.complete_rotation(eid)
        return total

    @property
    def grace_period_seconds(self) -> int:
        return self._grace
