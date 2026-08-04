"""Tests for v0.9.0 features: Retry-After parsing, multi-secret rotation, endpoint verification."""

import json
import time
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest

from agent_webhook.engine import DeliveryEngine
from agent_webhook.models import (
    DeliveryAttempt,
    DeliveryStatus,
    JitterType,
    RetryPolicy,
    WebhookEndpoint,
    WebhookDelivery,
    WebhookMethod,
    WebhookStatus,
)
from agent_webhook.secret_rotation import (
    RotationSecret,
    SecretRole,
    SecretRotationManager,
    SecretStatus,
)
from agent_webhook.verification import (
    ChallengeLocation,
    ChallengeManager,
    ChallengeStatus,
    VerificationChallenge,
)
from agent_webhook.store import WebhookStore


# ─── Helper ──────────────────────────────────────────────

def _make_store(tmp_path):
    """Create a fresh JSON store for testing."""
    return WebhookStore(str(tmp_path / "test_store.json"))


# ═══════════════════════════════════════════════════════════
# 1. RETRY-AFTER HEADER PARSING (RFC 7231)
# ═══════════════════════════════════════════════════════════

class TestRetryAfterParsing:
    """Test RFC 7231 Retry-After header parsing."""

    def test_parse_integer_seconds(self):
        assert DeliveryEngine.parse_retry_after("120") == 120.0

    def test_parse_zero(self):
        assert DeliveryEngine.parse_retry_after("0") == 0.0

    def test_parse_large_integer(self):
        assert DeliveryEngine.parse_retry_after("86400") == 86400.0

    def test_parse_with_whitespace(self):
        assert DeliveryEngine.parse_retry_after("  60  ") == 60.0

    def test_parse_none(self):
        assert DeliveryEngine.parse_retry_after(None) is None

    def test_parse_empty_string(self):
        assert DeliveryEngine.parse_retry_after("") is None

    def test_parse_http_date_future(self):
        """Parse HTTP-date format (future time)."""
        future = datetime.now(timezone.utc) + timedelta(seconds=120)
        date_str = format_datetime(future, usegmt=True)
        delay = DeliveryEngine.parse_retry_after(date_str)
        assert delay is not None
        # Should be approximately 120 seconds (within 10s tolerance for parsing overhead)
        assert 100 <= delay <= 140

    def test_parse_http_date_past(self):
        """Parse HTTP-date format (past time → clamped to 0)."""
        past = datetime.now(timezone.utc) - timedelta(seconds=60)
        date_str = format_datetime(past, usegmt=True)
        delay = DeliveryEngine.parse_retry_after(date_str)
        assert delay is not None
        assert delay == 0.0

    def test_parse_garbage(self):
        assert DeliveryEngine.parse_retry_after("not-a-date-or-number") is None

    def test_parse_negative_integer(self):
        """Negative integers should not parse."""
        assert DeliveryEngine.parse_retry_after("-5") is None

    def test_parse_float_rejected(self):
        """Floats are not valid Retry-After values (must be integer seconds)."""
        result = DeliveryEngine.parse_retry_after("1.5")
        # Python int() raises ValueError on "1.5", falls through to date parsing which fails
        assert result is None


# ═══════════════════════════════════════════════════════════
# 2. MULTI-SECRET ROTATION
# ═══════════════════════════════════════════════════════════

class TestSecretRotationManager:
    """Test the SecretRotationManager."""

    def test_register_initial_secret(self):
        mgr = SecretRotationManager()
        rs = mgr.register_secret("ep1", "secret-old")
        assert rs.secret == "secret-old"
        assert rs.role == SecretRole.PRIMARY
        assert rs.status == SecretStatus.ACTIVE
        assert rs.version == 1

    def test_initiate_rotation(self):
        mgr = SecretRotationManager()
        mgr.register_secret("ep1", "secret-old")
        new_rs = mgr.initiate_rotation("ep1", "secret-new")

        assert new_rs.secret == "secret-new"
        assert new_rs.role == SecretRole.PRIMARY
        assert new_rs.status == SecretStatus.ROTATING
        assert new_rs.version == 2

        # Old secret should now be PREVIOUS
        status = mgr.get_rotation_status("ep1")
        roles = [s["role"] for s in status["secrets"]]
        assert "previous" in roles
        assert status["is_rotating"] is True

    def test_signing_secret_returns_new_primary(self):
        mgr = SecretRotationManager()
        mgr.register_secret("ep1", "secret-old")
        mgr.initiate_rotation("ep1", "secret-new")

        # Signing should use the new secret
        assert mgr.get_signing_secret("ep1") == "secret-new"

    def test_verification_secrets_include_both(self):
        mgr = SecretRotationManager()
        mgr.register_secret("ep1", "secret-old")
        mgr.initiate_rotation("ep1", "secret-new")

        verify_secrets = mgr.get_verification_secrets("ep1")
        assert "secret-old" in verify_secrets
        assert "secret-new" in verify_secrets
        assert len(verify_secrets) == 2

    def test_verify_rotation(self):
        mgr = SecretRotationManager()
        mgr.register_secret("ep1", "secret-old")
        mgr.initiate_rotation("ep1", "secret-new")

        verified = mgr.verify_rotation("ep1")
        assert verified is True

        status = mgr.get_rotation_status("ep1")
        assert status["is_rotating"] is False
        # Primary should now be ACTIVE
        primary = [s for s in status["secrets"] if s["role"] == "primary"][0]
        assert primary["status"] == "active"
        # Old should be RETIRED
        old = [s for s in status["secrets"] if s["role"] == "previous"][0]
        assert old["status"] == "retired"

    def test_verify_rotation_retired_still_usable_for_verification(self):
        mgr = SecretRotationManager()
        mgr.register_secret("ep1", "secret-old")
        mgr.initiate_rotation("ep1", "secret-new")
        mgr.verify_rotation("ep1")

        # Old retired secret should still be usable for verification (during grace)
        verify_secrets = mgr.get_verification_secrets("ep1")
        assert "secret-old" in verify_secrets
        assert "secret-new" in verify_secrets

    def test_complete_rotation_after_grace(self):
        """Test that retired secrets are removed after the grace period."""
        mgr = SecretRotationManager(grace_period_seconds=60)  # 60 second grace
        mgr.register_secret("ep1", "secret-old")
        mgr.initiate_rotation("ep1", "secret-new")
        mgr.verify_rotation("ep1")

        # Manually set retired_at to past
        status = mgr.get_rotation_status("ep1")
        old_rs = [s for s in mgr._secrets["ep1"] if s.role == SecretRole.PREVIOUS][0]
        old_rs.retired_at = datetime.now(timezone.utc) - timedelta(seconds=120)

        removed = mgr.complete_rotation("ep1")
        assert removed == 1

        # Only the new primary should remain
        verify_secrets = mgr.get_verification_secrets("ep1")
        assert verify_secrets == ["secret-new"]

    def test_complete_rotation_within_grace_keeps_old(self):
        mgr = SecretRotationManager(grace_period_seconds=3600)
        mgr.register_secret("ep1", "secret-old")
        mgr.initiate_rotation("ep1", "secret-new")
        mgr.verify_rotation("ep1")

        removed = mgr.complete_rotation("ep1")
        assert removed == 0

    def test_cancel_rotation(self):
        mgr = SecretRotationManager()
        mgr.register_secret("ep1", "secret-old")
        mgr.initiate_rotation("ep1", "secret-new")

        cancelled = mgr.cancel_rotation("ep1")
        assert cancelled is True

        # Primary should be reverted to old secret
        assert mgr.get_signing_secret("ep1") == "secret-old"

        status = mgr.get_rotation_status("ep1")
        assert status["is_rotating"] is False
        roles = [s["role"] for s in status["secrets"]]
        assert "primary" in roles
        assert "previous" not in roles

    def test_cancel_rotation_when_not_rotating(self):
        mgr = SecretRotationManager()
        mgr.register_secret("ep1", "secret-old")
        cancelled = mgr.cancel_rotation("ep1")
        assert cancelled is False

    def test_initiate_rotation_no_existing_secret_raises(self):
        mgr = SecretRotationManager()
        with pytest.raises(KeyError):
            mgr.initiate_rotation("ep1", "secret-new")

    def test_get_signing_secret_none_for_unknown(self):
        mgr = SecretRotationManager()
        assert mgr.get_signing_secret("unknown") is None

    def test_get_signing_secret_info(self):
        mgr = SecretRotationManager()
        mgr.register_secret("ep1", "secret-old")
        info = mgr.get_signing_secret_info("ep1")
        assert info is not None
        assert info["version"] == 1
        assert info["status"] == "active"
        assert "activated_at" in info

    def test_get_rotation_status_unknown_endpoint(self):
        mgr = SecretRotationManager()
        status = mgr.get_rotation_status("unknown")
        assert status["secrets"] == []
        assert status["is_rotating"] is False

    def test_secret_redacted_in_status(self):
        mgr = SecretRotationManager()
        mgr.register_secret("ep1", "super-secret-value")
        status = mgr.get_rotation_status("ep1")
        for s in status["secrets"]:
            assert s["secret"] == "***REDACTED***"

    def test_cleanup_all_expired(self):
        mgr = SecretRotationManager(grace_period_seconds=60)
        mgr.register_secret("ep1", "old1")
        mgr.initiate_rotation("ep1", "new1")
        mgr.verify_rotation("ep1")
        # Expire ep1's retired secret
        for s in mgr._secrets.get("ep1", []):
            if s.role == SecretRole.PREVIOUS:
                s.retired_at = datetime.now(timezone.utc) - timedelta(seconds=120)

        mgr.register_secret("ep2", "old2")
        mgr.initiate_rotation("ep2", "new2")
        mgr.verify_rotation("ep2")
        # Don't expire ep2's retired secret

        removed = mgr.cleanup_all_expired()
        assert removed == 1  # Only ep1's expired secret removed

    def test_grace_period_minimum_validation(self):
        with pytest.raises(ValueError, match="grace_period_seconds"):
            SecretRotationManager(grace_period_seconds=30)

    def test_full_rotation_lifecycle(self):
        """Complete lifecycle: register → initiate → verify → complete."""
        mgr = SecretRotationManager(grace_period_seconds=60)

        # 1. Register
        mgr.register_secret("ep1", "v1-secret")
        assert mgr.get_signing_secret("ep1") == "v1-secret"

        # 2. Initiate rotation
        mgr.initiate_rotation("ep1", "v2-secret")
        assert mgr.get_signing_secret("ep1") == "v2-secret"
        assert "v1-secret" in mgr.get_verification_secrets("ep1")

        # 3. Verify
        mgr.verify_rotation("ep1")
        assert mgr.get_signing_secret("ep1") == "v2-secret"

        # 4. Complete (simulate time passage)
        for s in mgr._secrets["ep1"]:
            if s.status == SecretStatus.RETIRED:
                s.retired_at = datetime.now(timezone.utc) - timedelta(seconds=120)
        removed = mgr.complete_rotation("ep1")
        assert removed == 1
        assert mgr.get_verification_secrets("ep1") == ["v2-secret"]


class TestRotationSecretModel:
    """Test the RotationSecret dataclass."""

    def test_is_usable_for_signing_active(self):
        rs = RotationSecret(secret="x", status=SecretStatus.ACTIVE)
        assert rs.is_usable_for_signing() is True

    def test_is_usable_for_signing_rotating(self):
        rs = RotationSecret(secret="x", status=SecretStatus.ROTATING)
        assert rs.is_usable_for_signing() is True

    def test_is_usable_for_signing_retired(self):
        rs = RotationSecret(secret="x", status=SecretStatus.RETIRED)
        assert rs.is_usable_for_signing() is False

    def test_is_usable_for_verification_retired(self):
        rs = RotationSecret(secret="x", status=SecretStatus.RETIRED)
        assert rs.is_usable_for_verification() is True

    def test_is_usable_for_verification_expired(self):
        rs = RotationSecret(secret="x", status=SecretStatus.EXPIRED)
        assert rs.is_usable_for_verification() is False

    def test_is_expired_not_retired(self):
        rs = RotationSecret(secret="x", status=SecretStatus.ACTIVE)
        assert rs.is_expired(grace_seconds=60) is False

    def test_is_expired_within_grace(self):
        rs = RotationSecret(
            secret="x",
            status=SecretStatus.RETIRED,
            retired_at=datetime.now(timezone.utc) - timedelta(seconds=30),
        )
        assert rs.is_expired(grace_seconds=60) is False

    def test_is_expired_past_grace(self):
        rs = RotationSecret(
            secret="x",
            status=SecretStatus.RETIRED,
            retired_at=datetime.now(timezone.utc) - timedelta(seconds=120),
        )
        assert rs.is_expired(grace_seconds=60) is True

    def test_to_dict_redacts_secret(self):
        rs = RotationSecret(secret="super-secret")
        d = rs.to_dict()
        assert d["secret"] == "***REDACTED***"
        assert d["role"] == "primary"
        assert d["status"] == "active"


# ═══════════════════════════════════════════════════════════
# 3. ENDPOINT VERIFICATION CHALLENGE
# ═══════════════════════════════════════════════════════════

class TestChallengeManager:
    """Test the ChallengeManager for endpoint verification."""

    def test_generate_token_length(self):
        token = ChallengeManager.generate_token(32)
        assert len(token) == 64  # 32 bytes = 64 hex chars

    def test_generate_token_custom_length(self):
        token = ChallengeManager.generate_token(16)
        assert len(token) == 32  # 16 bytes = 32 hex chars

    def test_generate_token_uniqueness(self):
        tokens = {ChallengeManager.generate_token() for _ in range(100)}
        assert len(tokens) == 100  # All unique

    def test_generate_token_minimum_length(self):
        with pytest.raises(ValueError, match="Token length"):
            ChallengeManager.generate_token(8)

    def test_create_challenge(self):
        cm = ChallengeManager()
        ch = cm.create_challenge("ep1")
        assert ch.endpoint_id == "ep1"
        assert len(ch.token) == 64
        assert ch.status == ChallengeStatus.PENDING
        assert ch.method == "POST"
        assert ch.location == ChallengeLocation.EITHER

    def test_create_challenge_custom_params(self):
        cm = ChallengeManager()
        ch = cm.create_challenge(
            "ep1",
            method="GET",
            location=ChallengeLocation.HEADER,
            header_name="X-Custom-Verify",
            body_field="verify_token",
            ttl_override=300,
            max_attempts=5,
        )
        assert ch.method == "GET"
        assert ch.location == ChallengeLocation.HEADER
        assert ch.header_name == "X-Custom-Verify"
        assert ch.body_field == "verify_token"
        assert ch.max_attempts == 5

    def test_create_challenge_replaces_existing(self):
        cm = ChallengeManager()
        ch1 = cm.create_challenge("ep1")
        ch2 = cm.create_challenge("ep1")
        assert ch1.token != ch2.token
        assert cm.get_challenge("ep1").token == ch2.token

    def test_validate_response_header_success(self):
        cm = ChallengeManager()
        ch = cm.create_challenge("ep1", location=ChallengeLocation.HEADER)
        valid = cm.validate_response(
            "ep1",
            response_headers={ch.header_name: ch.token},
        )
        assert valid is True
        assert cm.get_challenge("ep1").status == ChallengeStatus.VERIFIED

    def test_validate_response_body_success(self):
        cm = ChallengeManager()
        ch = cm.create_challenge("ep1", location=ChallengeLocation.BODY)
        valid = cm.validate_response(
            "ep1",
            response_body={ch.body_field: ch.token},
        )
        assert valid is True

    def test_validate_response_either_header_matches(self):
        cm = ChallengeManager()
        ch = cm.create_challenge("ep1", location=ChallengeLocation.EITHER)
        valid = cm.validate_response(
            "ep1",
            response_headers={ch.header_name: ch.token},
        )
        assert valid is True

    def test_validate_response_either_body_matches(self):
        cm = ChallengeManager()
        ch = cm.create_challenge("ep1", location=ChallengeLocation.EITHER)
        valid = cm.validate_response(
            "ep1",
            response_body={ch.body_field: ch.token},
        )
        assert valid is True

    def test_validate_response_wrong_token(self):
        cm = ChallengeManager()
        cm.create_challenge("ep1")
        valid = cm.validate_response(
            "ep1",
            response_headers={"X-Webhook-Verify-Challenge": "wrong-token"},
        )
        assert valid is False
        assert cm.get_challenge("ep1").status == ChallengeStatus.PENDING  # Still has attempts left

    def test_validate_response_missing_token(self):
        cm = ChallengeManager()
        cm.create_challenge("ep1")
        valid = cm.validate_response("ep1", response_headers={}, response_body={})
        assert valid is False

    def test_validate_response_no_challenge(self):
        cm = ChallengeManager()
        valid = cm.validate_response("unknown", response_headers={})
        assert valid is False

    def test_validate_response_case_insensitive_headers(self):
        cm = ChallengeManager()
        ch = cm.create_challenge("ep1")
        valid = cm.validate_response(
            "ep1",
            response_headers={"x-webhook-verify-challenge": ch.token},  # lowercase
        )
        assert valid is True

    def test_validate_response_json_string_body(self):
        cm = ChallengeManager()
        ch = cm.create_challenge("ep1")
        valid = cm.validate_response(
            "ep1",
            response_body=json.dumps({"challenge": ch.token}),
        )
        assert valid is True

    def test_validate_response_max_attempts_reached(self):
        cm = ChallengeManager()
        cm.create_challenge("ep1", max_attempts=2)
        # First failed attempt
        cm.validate_response("ep1", response_headers={"X-Webhook-Verify-Challenge": "wrong"})
        assert cm.get_challenge("ep1").status == ChallengeStatus.PENDING
        # Second failed attempt → exhausts attempts
        cm.validate_response("ep1", response_headers={"X-Webhook-Verify-Challenge": "wrong"})
        assert cm.get_challenge("ep1").status == ChallengeStatus.FAILED

    def test_challenge_expired(self):
        cm = ChallengeManager(ttl_seconds=10)
        ch = cm.create_challenge("ep1")
        # Manually expire
        ch.expires_at = datetime.now(timezone.utc) - timedelta(seconds=10)
        valid = cm.validate_response(
            "ep1",
            response_headers={ch.header_name: ch.token},
        )
        assert valid is False
        assert cm.get_challenge("ep1").status == ChallengeStatus.EXPIRED

    def test_revoke_challenge(self):
        cm = ChallengeManager()
        cm.create_challenge("ep1")
        assert cm.revoke("ep1") is True
        assert cm.get_challenge("ep1") is None

    def test_revoke_nonexistent(self):
        cm = ChallengeManager()
        assert cm.revoke("unknown") is False

    def test_cleanup_expired(self):
        cm = ChallengeManager(ttl_seconds=10)
        cm.create_challenge("ep1")
        cm.create_challenge("ep2")
        # Expire both
        for ch in cm.all_challenges():
            ch.expires_at = datetime.now(timezone.utc) - timedelta(seconds=10)
        removed = cm.cleanup_expired()
        assert removed == 2
        assert cm.get_challenge("ep1") is None
        assert cm.get_challenge("ep2") is None

    def test_cleanup_expired_keeps_active(self):
        cm = ChallengeManager()
        cm.create_challenge("ep1")
        cm.create_challenge("ep2")
        # Expire only ep1
        cm.get_challenge("ep1").expires_at = datetime.now(timezone.utc) - timedelta(seconds=10)
        removed = cm.cleanup_expired()
        assert removed == 1
        assert cm.get_challenge("ep1") is None
        assert cm.get_challenge("ep2") is not None

    def test_can_retry_after_failure(self):
        cm = ChallengeManager()
        ch = cm.create_challenge("ep1", max_attempts=3)
        cm.validate_response("ep1", response_headers={"X-Webhook-Verify-Challenge": "wrong"})
        assert ch.can_retry() is True

    def test_cannot_retry_after_max_attempts(self):
        cm = ChallengeManager()
        ch = cm.create_challenge("ep1", max_attempts=1)
        cm.validate_response("ep1", response_headers={"X-Webhook-Verify-Challenge": "wrong"})
        assert ch.can_retry() is False

    def test_cannot_retry_after_verified(self):
        cm = ChallengeManager()
        ch = cm.create_challenge("ep1")
        cm.validate_response("ep1", response_headers={ch.header_name: ch.token})
        assert ch.can_retry() is False

    def test_challenge_to_dict(self):
        cm = ChallengeManager()
        ch = cm.create_challenge("ep1")
        d = ch.to_dict()
        assert d["endpoint_id"] == "ep1"
        assert d["token"] == ch.token
        assert d["status"] == "pending"
        assert "expires_at" in d
        assert "created_at" in d

    def test_ttl_minimum_validation(self):
        with pytest.raises(ValueError, match="ttl_seconds"):
            ChallengeManager(ttl_seconds=5)


class TestVerificationChallengeModel:
    """Test the VerificationChallenge dataclass directly."""

    def test_is_expired_false(self):
        ch = VerificationChallenge(
            endpoint_id="ep1",
            token="abc",
            expires_at=datetime.now(timezone.utc) + timedelta(seconds=300),
        )
        assert ch.is_expired() is False

    def test_is_expired_true(self):
        ch = VerificationChallenge(
            endpoint_id="ep1",
            token="abc",
            expires_at=datetime.now(timezone.utc) - timedelta(seconds=10),
        )
        assert ch.is_expired() is True


# ═══════════════════════════════════════════════════════════
# 4. ENGINE INTEGRATION
# ═══════════════════════════════════════════════════════════

class TestEngineRetryAfterIntegration:
    """Test that the engine correctly uses Retry-After in retry scheduling."""

    @pytest.mark.asyncio
    async def test_engine_has_parse_retry_after(self):
        """Engine should expose parse_retry_after as a static method."""
        assert hasattr(DeliveryEngine, "parse_retry_after")

    @pytest.mark.asyncio
    async def test_engine_has_secret_rotation(self, tmp_path):
        store = _make_store(tmp_path)
        engine = DeliveryEngine(store)
        assert engine.secret_rotation is not None
        assert engine.challenge_manager is not None

    @pytest.mark.asyncio
    async def test_delivery_attempt_has_metadata(self):
        """DeliveryAttempt should have a metadata field for retry_after data."""
        attempt = DeliveryAttempt(
            delivery_id="d1",
            attempt_number=1,
        )
        assert attempt.metadata == {}
        attempt.metadata["retry_after_seconds"] = 30.0
        assert attempt.metadata["retry_after_seconds"] == 30.0


class TestEngineSecretRotationIntegration:
    """Test engine integration with SecretRotationManager."""

    @pytest.mark.asyncio
    async def test_engine_secret_rotation_lifecycle(self, tmp_path):
        store = _make_store(tmp_path)
        engine = DeliveryEngine(store)

        mgr = engine.secret_rotation
        mgr.register_secret("ep1", "old-secret")
        mgr.initiate_rotation("ep1", "new-secret")

        # Engine's signing secret should be the new one
        signing = mgr.get_signing_secret("ep1")
        assert signing == "new-secret"

        # Both secrets should be usable for verification
        verify = mgr.get_verification_secrets("ep1")
        assert "old-secret" in verify
        assert "new-secret" in verify


# ═══════════════════════════════════════════════════════════
# 5. DELIVERY ATTEMPT METADATA SERIALIZATION
# ═══════════════════════════════════════════════════════════

class TestDeliveryAttemptMetadata:
    """Test DeliveryAttempt metadata field."""

    def test_default_metadata_empty(self):
        attempt = DeliveryAttempt(delivery_id="d1", attempt_number=1)
        assert attempt.metadata == {}

    def test_metadata_with_retry_after(self):
        attempt = DeliveryAttempt(
            delivery_id="d1",
            attempt_number=1,
            metadata={"retry_after_seconds": 60.0, "retry_after_raw": "60"},
        )
        assert attempt.metadata["retry_after_seconds"] == 60.0

    def test_metadata_serialization(self):
        attempt = DeliveryAttempt(
            delivery_id="d1",
            attempt_number=1,
            metadata={"retry_after_seconds": 30.0},
        )
        d = attempt.model_dump()
        assert d["metadata"]["retry_after_seconds"] == 30.0

    def test_metadata_json_roundtrip(self):
        attempt = DeliveryAttempt(
            delivery_id="d1",
            attempt_number=1,
            metadata={"key": "value"},
        )
        j = attempt.model_dump_json()
        restored = DeliveryAttempt.model_validate_json(j)
        assert restored.metadata == {"key": "value"}


# ═══════════════════════════════════════════════════════════
# 6. MCP SERVER TOOL REGISTRATION
# ═══════════════════════════════════════════════════════════

class TestMCPServerV090Tools:
    """Test that the new v0.9.0 MCP tools are registered."""

    def test_tool_count_increased(self):
        """Total MCP tools should be 73 + 9 new = 82."""
        import re
        # Count Tool( definitions in the source
        import agent_webhook.mcp_server as mod
        import inspect
        source = inspect.getsource(mod)
        tool_count = len(re.findall(r'Tool\(\s*name="', source))
        assert tool_count >= 82

    def test_new_tools_registered(self):
        """All 9 new v0.9.0 tools should be present in the source."""
        import agent_webhook.mcp_server as mod
        import inspect
        source = inspect.getsource(mod)

        expected_new = [
            "retry_after_parse",
            "secret_rotation_initiate",
            "secret_rotation_verify",
            "secret_rotation_complete",
            "secret_rotation_status",
            "secret_rotation_cancel",
            "verify_endpoint_create",
            "verify_endpoint_validate",
            "verify_endpoint_status",
        ]
        for tool_name in expected_new:
            assert f'name="{tool_name}"' in source, f"Tool '{tool_name}' not found in mcp_server.py"

    def test_new_tools_have_call_handlers(self):
        """All new tools should have corresponding call_tool handlers."""
        import agent_webhook.mcp_server as mod
        import inspect
        source = inspect.getsource(mod)

        expected_new = [
            "retry_after_parse",
            "secret_rotation_initiate",
            "secret_rotation_verify",
            "secret_rotation_complete",
            "secret_rotation_status",
            "secret_rotation_cancel",
            "verify_endpoint_create",
            "verify_endpoint_validate",
            "verify_endpoint_status",
        ]
        for tool_name in expected_new:
            # Check for handler pattern: elif name == "tool_name":
            assert f'name == "{tool_name}"' in source, \
                f"Handler for tool '{tool_name}' not found in call_tool"


# ═══════════════════════════════════════════════════════════
# 7. INTEGRATION: RETRY-AFTER IN DELIVERY FLOW (MOCKED)
# ═══════════════════════════════════════════════════════════

class TestRetryAfterInDeliveryFlow:
    """Test that Retry-After is captured and used in the delivery retry flow."""

    @pytest.mark.asyncio
    async def test_retry_after_stored_in_attempt_metadata(self, tmp_path):
        """When a server returns 429 with Retry-After, the attempt metadata should capture it."""
        from unittest.mock import AsyncMock, MagicMock, patch

        store = _make_store(tmp_path)
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            retry_policy=RetryPolicy(max_retries=3, retry_on_status_codes=[429]),
        )
        store.add_endpoint(endpoint)
        engine = DeliveryEngine(store)

        # Mock response with 429 + Retry-After
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.text = "Too Many Requests"
        mock_response.headers = {"Retry-After": "45"}

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.is_closed = False

        with patch.object(engine, "_get_client", return_value=mock_client):
            delivery = WebhookDelivery(
                endpoint_id=endpoint.id,
                payload={"event": "test"},
            )
            store.add_delivery(delivery)
            attempt = await engine.deliver(delivery)

        assert attempt.status == DeliveryStatus.FAILED
        assert attempt.metadata.get("retry_after_seconds") == 45.0
        assert attempt.metadata.get("retry_after_raw") == "45"
        assert "Retry-After: 45.0s" in attempt.error_message

    @pytest.mark.asyncio
    async def test_retry_after_respected_in_retry_scheduling(self, tmp_path):
        """When scheduling a retry, Retry-After should override backoff delay."""
        from unittest.mock import AsyncMock, MagicMock, patch

        store = _make_store(tmp_path)
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            retry_policy=RetryPolicy(
                max_retries=3,
                initial_delay_seconds=1.0,
                max_delay_seconds=300.0,
                retry_on_status_codes=[429],
            ),
        )
        store.add_endpoint(endpoint)
        engine = DeliveryEngine(store)

        # Mock response with 429 + Retry-After: 120
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.text = "Too Many Requests"
        mock_response.headers = {"Retry-After": "120"}

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.is_closed = False

        with patch.object(engine, "_get_client", return_value=mock_client):
            delivery = WebhookDelivery(
                endpoint_id=endpoint.id,
                payload={"event": "test"},
            )
            store.add_delivery(delivery)
            result = await engine.process_delivery(delivery.id)

        # The delivery should be in RETRYING status
        assert result is not None
        assert result.status == DeliveryStatus.RETRYING

        # next_retry_at should be approximately 120 seconds from now
        # (not 1 second which is what the default backoff would give)
        now = datetime.now(timezone.utc)
        delay = (result.next_retry_at - now).total_seconds()
        assert delay >= 100, f"Expected delay ~120s, got {delay}s"
        assert delay <= 130

    @pytest.mark.asyncio
    async def test_retry_after_absent_uses_normal_backoff(self, tmp_path):
        """When no Retry-After header, normal backoff should be used."""
        from unittest.mock import AsyncMock, MagicMock, patch

        store = _make_store(tmp_path)
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            retry_policy=RetryPolicy(
                max_retries=3,
                initial_delay_seconds=2.0,
                max_delay_seconds=300.0,
                backoff_multiplier=2.0,
                jitter=JitterType.NONE,  # Deterministic for testing
                retry_on_status_codes=[429],
            ),
        )
        store.add_endpoint(endpoint)
        engine = DeliveryEngine(store)

        # Mock response with 429, no Retry-After
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.text = "Too Many Requests"
        mock_response.headers = {}

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.is_closed = False

        with patch.object(engine, "_get_client", return_value=mock_client):
            delivery = WebhookDelivery(
                endpoint_id=endpoint.id,
                payload={"event": "test"},
            )
            store.add_delivery(delivery)
            result = await engine.process_delivery(delivery.id)

        assert result is not None
        assert result.status == DeliveryStatus.RETRYING

        # Should use normal backoff: 2.0 seconds for first attempt
        now = datetime.now(timezone.utc)
        delay = (result.next_retry_at - now).total_seconds()
        assert 1.5 <= delay <= 3.0  # ~2 seconds

    @pytest.mark.asyncio
    async def test_retry_after_clamped_to_max_delay(self, tmp_path):
        """Retry-After exceeding max_delay should be clamped."""
        from unittest.mock import AsyncMock, MagicMock, patch

        store = _make_store(tmp_path)
        endpoint = WebhookEndpoint(
            name="test",
            url="https://example.com/hook",
            retry_policy=RetryPolicy(
                max_retries=3,
                max_delay_seconds=10.0,  # Very low max
                retry_on_status_codes=[429],
            ),
        )
        store.add_endpoint(endpoint)
        engine = DeliveryEngine(store)

        # Server requests 9999 second delay
        mock_response = MagicMock()
        mock_response.status_code = 429
        mock_response.text = "Too Many Requests"
        mock_response.headers = {"Retry-After": "9999"}

        mock_client = AsyncMock()
        mock_client.request = AsyncMock(return_value=mock_response)
        mock_client.is_closed = False

        with patch.object(engine, "_get_client", return_value=mock_client):
            delivery = WebhookDelivery(
                endpoint_id=endpoint.id,
                payload={"event": "test"},
            )
            store.add_delivery(delivery)
            result = await engine.process_delivery(delivery.id)

        assert result is not None
        assert result.status == DeliveryStatus.RETRYING

        # Delay should be clamped to max_delay_seconds=10
        now = datetime.now(timezone.utc)
        delay = (result.next_retry_at - now).total_seconds()
        assert delay <= 12  # ~10 seconds with some tolerance
