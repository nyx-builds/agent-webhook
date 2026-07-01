"""API key authentication middleware for agent-webhook REST API.

Provides a FastAPI dependency that validates API keys via:
1. ``Authorization: Bearer <key>`` header
2. ``X-API-Key: <key>`` header
3. ``api_key`` query parameter

Keys can be configured via environment variable ``WEBHOOK_API_KEY`` (single key)
or managed programmatically via the ``APIKeyManager``.

Usage in FastAPI::

    from agent_webhook.auth import create_auth_dependency
    from .rest_api import create_app

    app = create_app(store_path="webhooks.db")
    # Protect all routes:
    verify_key = create_auth_dependency(["my-secret-key"])
    for route in app.routes:
        if hasattr(route, "dependencies"):
            route.dependencies = route.dependencies + [Depends(verify_key)]
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from dataclasses import dataclass, field
from typing import Any

from fastapi import Depends, HTTPException, Request, Security
from fastapi.security import APIKeyHeader, HTTPAuthorizationCredentials, HTTPBearer


# ─── API Key Manager ─────────────────────────────────────────────────


@dataclass
class APIKey:
    """A single API key with metadata."""

    key_hash: str  # Store hash, not the raw key
    name: str
    scopes: list[str] = field(default_factory=lambda: ["*"])  # "*" = all scopes
    created_at: float = field(default_factory=time.time)
    last_used: float | None = None
    expires_at: float | None = None
    active: bool = True
    # We store the raw key only transiently when first created
    _raw_key: str | None = field(default=None, repr=False)

    def has_scope(self, scope: str) -> bool:
        return "*" in self.scopes or scope in self.scopes

    def is_expired(self) -> bool:
        if self.expires_at is None:
            return False
        return time.time() > self.expires_at

    @property
    def is_valid(self) -> bool:
        return self.active and not self.is_expired()

    def to_dict(self, include_key: bool = False) -> dict[str, Any]:
        d = {
            "name": self.name,
            "scopes": self.scopes,
            "created_at": self.created_at,
            "last_used": self.last_used,
            "expires_at": self.expires_at,
            "active": self.active,
            "key_prefix": self._raw_key[:8] + "..." if self._raw_key else None,
        }
        if include_key and self._raw_key:
            d["key"] = self._raw_key
        return d


class APIKeyManager:
    """Manages API keys with hashing, scopes, and expiration.

    Keys are hashed with SHA-256 + salt. The raw key is only available
    at creation time.
    """

    def __init__(self) -> None:
        self._keys: dict[str, APIKey] = {}  # key_hash -> APIKey

    @staticmethod
    def _hash_key(raw_key: str) -> str:
        """Hash an API key for secure storage."""
        return hashlib.sha256(raw_key.encode()).hexdigest()

    def create_key(
        self,
        name: str,
        scopes: list[str] | None = None,
        expires_in_seconds: float | None = None,
    ) -> tuple[str, APIKey]:
        """Generate a new API key. Returns (raw_key, key_metadata).

        The raw key is only available at creation time — store it securely.
        """
        raw_key = "whk_" + secrets.token_urlsafe(32)
        key_hash = self._hash_key(raw_key)
        expires_at = time.time() + expires_in_seconds if expires_in_seconds else None

        api_key = APIKey(
            key_hash=key_hash,
            name=name,
            scopes=scopes or ["*"],
            expires_at=expires_at,
            _raw_key=raw_key,
        )
        self._keys[key_hash] = api_key
        return raw_key, api_key

    def add_existing_key(
        self,
        raw_key: str,
        name: str = "imported",
        scopes: list[str] | None = None,
    ) -> APIKey:
        """Add a pre-existing key (e.g. from environment variable)."""
        key_hash = self._hash_key(raw_key)
        api_key = APIKey(
            key_hash=key_hash,
            name=name,
            scopes=scopes or ["*"],
            _raw_key=raw_key,
        )
        self._keys[key_hash] = api_key
        return api_key

    def verify(self, raw_key: str) -> APIKey | None:
        """Verify a raw API key. Returns the key metadata if valid, None otherwise."""
        key_hash = self._hash_key(raw_key)
        api_key = self._keys.get(key_hash)
        if api_key is None:
            return None
        if not api_key.is_valid:
            return None
        api_key.last_used = time.time()
        return api_key

    def revoke(self, name: str) -> bool:
        """Revoke a key by name."""
        for key_hash, key in list(self._keys.items()):
            if key.name == name:
                key.active = False
                return True
        return False

    def revoke_all(self) -> int:
        count = len(self._keys)
        for key in self._keys.values():
            key.active = False
        return count

    def list_keys(self) -> list[dict[str, Any]]:
        """List all keys (without raw key values)."""
        return [k.to_dict() for k in self._keys.values()]

    def get_key_by_name(self, name: str) -> APIKey | None:
        for key in self._keys.values():
            if key.name == name:
                return key
        return None


# ─── FastAPI Dependency ──────────────────────────────────────────────


# Header-based key extraction
_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
_bearer_scheme = HTTPBearer(auto_error=False)


def create_auth_dependency(
    valid_keys: list[str],
    key_manager: APIKeyManager | None = None,
) -> Any:
    """Create a FastAPI dependency that validates API keys.

    Args:
        valid_keys: List of raw API key strings that are accepted.
        key_manager: Optional APIKeyManager for more advanced key management.
            If provided, takes precedence over ``valid_keys``.

    Returns:
        A FastAPI dependency function.

    Usage::

        from fastapi import Depends
        verify_key = create_auth_dependency(["my-secret"])

        @app.get("/protected", dependencies=[Depends(verify_key)])
        async def protected():
            return {"ok": True}
    """
    # Build a quick lookup set for raw keys
    key_set = set(valid_keys)

    async def verify_api_key(
        x_api_key: str | None = Security(_api_key_header),
        bearer: HTTPAuthorizationCredentials | None = Security(_bearer_scheme),
        request: Request | None = None,
    ) -> str:
        # Try X-API-Key header first
        candidate = x_api_key

        # Then try Bearer token
        if candidate is None and bearer is not None:
            candidate = bearer.credentials

        # Then try query param
        if candidate is None and request is not None:
            candidate = request.query_params.get("api_key")

        if candidate is None:
            raise HTTPException(
                status_code=401,
                detail="Missing API key. Provide via 'X-API-Key' header, 'Authorization: Bearer <key>', or '?api_key=' query param.",
                headers={"WWW-Authenticate": 'ApiKey realm="agent-webhook"'},
            )

        # Check key manager first (supports hashed keys, scopes)
        if key_manager is not None:
            api_key = key_manager.verify(candidate)
            if api_key is not None:
                return candidate  # Valid key

        # Fall back to raw key set
        if candidate in key_set:
            return candidate

        raise HTTPException(
            status_code=403,
            detail="Invalid API key",
        )

    return verify_api_key


def create_optional_auth_dependency(
    valid_keys: list[str],
    key_manager: APIKeyManager | None = None,
) -> Any:
    """Like create_auth_dependency but doesn't fail if no key is provided.

    Useful for health endpoints that should work without auth, but log usage
    if a key is present.
    """
    key_set = set(valid_keys)

    async def verify_optional(
        x_api_key: str | None = Security(_api_key_header),
        bearer: HTTPAuthorizationCredentials | None = Security(_bearer_scheme),
    ) -> str | None:
        candidate = x_api_key
        if candidate is None and bearer is not None:
            candidate = bearer.credentials

        if candidate is None:
            return None  # No key provided — allowed

        if key_manager is not None:
            if key_manager.verify(candidate) is not None:
                return candidate

        if candidate in key_set:
            return candidate

        raise HTTPException(status_code=403, detail="Invalid API key")

    return verify_optional
