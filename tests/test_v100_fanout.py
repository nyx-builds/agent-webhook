"""Tests for v1.0.0 Fan-Out Delivery Groups feature.

Tests cover:
- DeliveryGroup / DeliveryGroupMember model validation and methods
- GroupDeliveryResult status evaluation logic
- FanoutEngine: CRUD, parallel delivery, sequential delivery with early-stop,
  weighted selection, dry-run, stats
- WebhookService integration: create/list/update/delete groups, fanout_deliver,
  fanout_deliver_dry_run, group member management
- REST API endpoints for groups and group deliveries
"""

from __future__ import annotations

import asyncio
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from agent_webhook.models import (
    DeliveryGroup,
    DeliveryGroupMember,
    FanoutStrategy,
    GroupDeliveryResult,
    GroupDeliveryStatus,
    GroupFailureStrategy,
    GroupMemberResult,
    GroupMemberStatus,
    GroupStatus,
    WebhookEndpoint,
)
from agent_webhook.fanout import FanoutEngine
from agent_webhook.service import WebhookService
from agent_webhook.store_sqlite import SQLiteStore


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture
def tmp_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    yield path
    Path(path).unlink(missing_ok=True)


@pytest.fixture
def store(tmp_db):
    return SQLiteStore(tmp_db)


@pytest.fixture
def service(tmp_db):
    return WebhookService(store_path=tmp_db)


@pytest.fixture
def fanout_engine(store):
    return FanoutEngine(store)


@pytest.fixture
def endpoint(service):
    return service.create_endpoint(name="Test EP", url="https://example.com/hook")


@pytest.fixture
def multi_endpoints(service):
    """Create 5 endpoints."""
    return [
        service.create_endpoint(name=f"EP-{i}", url=f"https://example.com/hook-{i}")
        for i in range(5)
    ]


# ── Model Tests ─────────────────────────────────────────────────────


class TestDeliveryGroupModel:
    """Tests for the DeliveryGroup model."""

    def test_create_basic_group(self):
        group = DeliveryGroup(name="my-group")
        assert group.name == "my-group"
        assert group.members == []
        assert group.strategy == FanoutStrategy.PARALLEL
        assert group.failure_strategy == GroupFailureStrategy.ALL_MUST_SUCCEED
        assert group.status == GroupStatus.ACTIVE
        assert group.max_concurrent == 0
        assert group.tags == []

    def test_create_with_members(self):
        group = DeliveryGroup(
            name="multi",
            members=[
                DeliveryGroupMember(endpoint_id="ep1", weight=3),
                DeliveryGroupMember(endpoint_id="ep2", weight=1),
            ],
        )
        assert len(group.members) == 2
        assert group.total_weight() == 4

    def test_create_with_strategy(self):
        group = DeliveryGroup(
            name="weighted",
            strategy=FanoutStrategy.WEIGHTED,
            failure_strategy=GroupFailureStrategy.MAJORITY_SUCCESS,
            max_concurrent=5,
        )
        assert group.strategy == FanoutStrategy.WEIGHTED
        assert group.failure_strategy == GroupFailureStrategy.MAJORITY_SUCCESS
        assert group.max_concurrent == 5

    def test_create_with_threshold(self):
        group = DeliveryGroup(
            name="threshold-group",
            failure_strategy=GroupFailureStrategy.THRESHOLD,
            success_threshold=0.8,
        )
        assert group.failure_strategy == GroupFailureStrategy.THRESHOLD
        assert group.success_threshold == 0.8

    def test_invalid_threshold_too_high(self):
        with pytest.raises(Exception):
            DeliveryGroup(name="bad", success_threshold=1.5)

    def test_invalid_threshold_negative(self):
        with pytest.raises(Exception):
            DeliveryGroup(name="bad", success_threshold=-0.1)

    def test_invalid_tag(self):
        with pytest.raises(Exception):
            DeliveryGroup(name="bad", tags=["invalid tag with spaces"])

    def test_valid_tags(self):
        group = DeliveryGroup(name="tagged", tags=["prod", "v2", "us-east"])
        assert len(group.tags) == 3

    def test_name_required(self):
        with pytest.raises(Exception):
            DeliveryGroup()

    def test_name_too_long(self):
        with pytest.raises(Exception):
            DeliveryGroup(name="x" * 201)

    def test_add_member_new(self):
        group = DeliveryGroup(name="test")
        group.add_member("ep1", weight=2)
        assert len(group.members) == 1
        assert group.members[0].endpoint_id == "ep1"
        assert group.members[0].weight == 2

    def test_add_member_updates_existing(self):
        group = DeliveryGroup(name="test")
        group.add_member("ep1", weight=1)
        group.add_member("ep1", weight=5, headers={"X-Env": "prod"})
        assert len(group.members) == 1
        assert group.members[0].weight == 5
        assert group.members[0].headers == {"X-Env": "prod"}

    def test_remove_member(self):
        group = DeliveryGroup(name="test")
        group.add_member("ep1")
        group.add_member("ep2")
        removed = group.remove_member("ep1")
        assert removed is True
        assert len(group.members) == 1
        assert group.members[0].endpoint_id == "ep2"

    def test_remove_nonexistent_member(self):
        group = DeliveryGroup(name="test")
        removed = group.remove_member("ep1")
        assert removed is False

    def test_active_members_filters_disabled(self):
        group = DeliveryGroup(name="test")
        group.add_member("ep1")
        group.add_member("ep2")
        group.members[1].enabled = False
        active = group.active_members()
        assert len(active) == 1
        assert active[0].endpoint_id == "ep1"

    def test_total_weight_excludes_disabled(self):
        group = DeliveryGroup(name="test")
        group.add_member("ep1", weight=3)
        group.add_member("ep2", weight=5)
        group.members[1].enabled = False
        assert group.total_weight() == 3

    def test_is_active(self):
        group = DeliveryGroup(name="test")
        assert group.is_active() is True
        group.status = GroupStatus.PAUSED
        assert group.is_active() is False

    def test_description_optional(self):
        group = DeliveryGroup(name="test", description="A test group")
        assert group.description == "A test group"


class TestDeliveryGroupMemberModel:
    """Tests for DeliveryGroupMember."""

    def test_defaults(self):
        m = DeliveryGroupMember(endpoint_id="ep1")
        assert m.weight == 1
        assert m.headers == {}
        assert m.enabled is True

    def test_with_weight(self):
        m = DeliveryGroupMember(endpoint_id="ep1", weight=10)
        assert m.weight == 10

    def test_invalid_weight_zero(self):
        with pytest.raises(Exception):
            DeliveryGroupMember(endpoint_id="ep1", weight=0)

    def test_invalid_weight_negative(self):
        with pytest.raises(Exception):
            DeliveryGroupMember(endpoint_id="ep1", weight=-1)

    def test_with_headers(self):
        m = DeliveryGroupMember(
            endpoint_id="ep1",
            headers={"X-Token": "abc123"},
        )
        assert m.headers == {"X-Token": "abc123"}


# ── GroupDeliveryResult Status Evaluation ───────────────────────────


class TestGroupDeliveryResultEvaluation:
    """Tests for the evaluate_status() method."""

    def _make_result(self, failure_strategy, successes, failures, threshold=1.0):
        return GroupDeliveryResult(
            group_id="grp-1",
            failure_strategy=failure_strategy,
            success_threshold=threshold,
            success_count=successes,
            failure_count=failures,
        )

    def test_all_must_succeed_pass(self):
        r = self._make_result(GroupFailureStrategy.ALL_MUST_SUCCEED, 3, 0)
        assert r.evaluate_status() == GroupDeliveryStatus.SUCCESS

    def test_all_must_succeed_fail(self):
        r = self._make_result(GroupFailureStrategy.ALL_MUST_SUCCEED, 2, 1)
        assert r.evaluate_status() == GroupDeliveryStatus.FAILED

    def test_any_success_pass(self):
        r = self._make_result(GroupFailureStrategy.ANY_SUCCESS, 1, 4)
        assert r.evaluate_status() == GroupDeliveryStatus.SUCCESS

    def test_any_success_fail(self):
        r = self._make_result(GroupFailureStrategy.ANY_SUCCESS, 0, 5)
        assert r.evaluate_status() == GroupDeliveryStatus.FAILED

    def test_majority_success_pass(self):
        r = self._make_result(GroupFailureStrategy.MAJORITY_SUCCESS, 3, 2)
        assert r.evaluate_status() == GroupDeliveryStatus.SUCCESS

    def test_majority_success_fail(self):
        r = self._make_result(GroupFailureStrategy.MAJORITY_SUCCESS, 2, 3)
        assert r.evaluate_status() == GroupDeliveryStatus.FAILED

    def test_majority_tie_fails(self):
        # Exactly half is NOT majority
        r = self._make_result(GroupFailureStrategy.MAJORITY_SUCCESS, 2, 2)
        assert r.evaluate_status() == GroupDeliveryStatus.FAILED

    def test_threshold_pass(self):
        # 4 out of 5 = 80% >= 80% threshold
        r = self._make_result(GroupFailureStrategy.THRESHOLD, 4, 1, threshold=0.8)
        assert r.evaluate_status() == GroupDeliveryStatus.SUCCESS

    def test_threshold_fail(self):
        # 3 out of 5 = 60% < 80% threshold
        r = self._make_result(GroupFailureStrategy.THRESHOLD, 3, 2, threshold=0.8)
        assert r.evaluate_status() == GroupDeliveryStatus.FAILED

    def test_threshold_zero(self):
        # 0% threshold means always succeed if at least one attempt
        r = self._make_result(GroupFailureStrategy.THRESHOLD, 0, 3, threshold=0.0)
        assert r.evaluate_status() == GroupDeliveryStatus.SUCCESS

    def test_no_attempts_is_failed(self):
        r = self._make_result(GroupFailureStrategy.ALL_MUST_SUCCEED, 0, 0)
        assert r.evaluate_status() == GroupDeliveryStatus.FAILED

    def test_success_rate_property(self):
        r = self._make_result(GroupFailureStrategy.ALL_MUST_SUCCEED, 3, 1)
        assert r.success_rate == 0.75

    def test_success_rate_none_when_no_attempts(self):
        r = self._make_result(GroupFailureStrategy.ALL_MUST_SUCCEED, 0, 0)
        assert r.success_rate is None

    def test_attempted_count(self):
        r = self._make_result(GroupFailureStrategy.ALL_MUST_SUCCEED, 3, 2)
        assert r.attempted_count == 5


# ── FanoutEngine Store Tests ────────────────────────────────────────


class TestFanoutEngineStore:
    """Tests for group CRUD in the FanoutEngine."""

    def test_create_and_get_group(self, fanout_engine):
        group = fanout_engine.create_group(
            name="test-group",
            members=[{"endpoint_id": "ep1"}, {"endpoint_id": "ep2"}],
        )
        assert group.id is not None
        retrieved = fanout_engine.get_group(group.id)
        assert retrieved is not None
        assert retrieved.name == "test-group"
        assert len(retrieved.members) == 2

    def test_list_groups(self, fanout_engine):
        fanout_engine.create_group(name="group-1", members=[])
        fanout_engine.create_group(name="group-2", members=[])
        groups = fanout_engine.list_groups()
        assert len(groups) == 2

    def test_list_groups_by_status(self, fanout_engine):
        g1 = fanout_engine.create_group(name="active", members=[])
        g2 = fanout_engine.create_group(name="paused", members=[])
        fanout_engine.update_group(g2.id, status=GroupStatus.PAUSED)
        active = fanout_engine.list_groups(status="active")
        assert len(active) == 1
        assert active[0].name == "active"

    def test_list_groups_by_tag(self, fanout_engine):
        fanout_engine.create_group(name="prod", members=[], tags=["production"])
        fanout_engine.create_group(name="staging", members=[], tags=["staging"])
        prod = fanout_engine.list_groups(tag="production")
        assert len(prod) == 1
        assert prod[0].name == "prod"

    def test_update_group(self, fanout_engine):
        group = fanout_engine.create_group(name="test", members=[])
        updated = fanout_engine.update_group(group.id, description="Updated desc")
        assert updated is not None
        assert updated.description == "Updated desc"

    def test_delete_group(self, fanout_engine):
        group = fanout_engine.create_group(name="test", members=[])
        assert fanout_engine.delete_group(group.id) is True
        assert fanout_engine.get_group(group.id) is None

    def test_delete_nonexistent(self, fanout_engine):
        assert fanout_engine.delete_group("nonexistent") is False

    def test_add_member(self, fanout_engine):
        group = fanout_engine.create_group(name="test", members=[])
        updated = fanout_engine.add_member(group.id, "ep1", weight=3)
        assert updated is not None
        assert len(updated.members) == 1
        assert updated.members[0].weight == 3

    def test_add_member_with_headers(self, fanout_engine):
        group = fanout_engine.create_group(name="test", members=[])
        fanout_engine.add_member(group.id, "ep1", headers={"X-Route": "v2"})
        g = fanout_engine.get_group(group.id)
        assert g.members[0].headers == {"X-Route": "v2"}

    def test_remove_member(self, fanout_engine):
        group = fanout_engine.create_group(
            name="test", members=[{"endpoint_id": "ep1"}, {"endpoint_id": "ep2"}]
        )
        assert fanout_engine.remove_member(group.id, "ep1") is True
        g = fanout_engine.get_group(group.id)
        assert len(g.members) == 1
        assert g.members[0].endpoint_id == "ep2"

    def test_remove_nonexistent_member(self, fanout_engine):
        group = fanout_engine.create_group(name="test", members=[])
        assert fanout_engine.remove_member(group.id, "ep1") is False

    def test_update_nonexistent_group(self, fanout_engine):
        assert fanout_engine.update_group("nonexistent", name="x") is None


# ── FanoutEngine Delivery Tests ─────────────────────────────────────


class TestFanoutDelivery:
    """Tests for the fan-out delivery engine."""

    @pytest.mark.asyncio
    async def test_dry_run_all_success(self, fanout_engine):
        """Dry-run without callback simulates all members succeeding."""
        group = fanout_engine.create_group(
            name="test",
            members=[{"endpoint_id": "ep1"}, {"endpoint_id": "ep2"}, {"endpoint_id": "ep3"}],
        )
        result = await fanout_engine.deliver(group.id, payload={"event": "test"})
        assert result.status == GroupDeliveryStatus.SUCCESS
        assert result.success_count == 3
        assert result.failure_count == 0
        assert result.total_members == 3

    @pytest.mark.asyncio
    async def test_dry_run_persisted(self, fanout_engine):
        group = fanout_engine.create_group(
            name="test", members=[{"endpoint_id": "ep1"}],
        )
        result = await fanout_engine.deliver(group.id, payload={"x": 1})
        # Should be retrievable
        fetched = fanout_engine.get_delivery(result.id)
        assert fetched is not None
        assert fetched.status == GroupDeliveryStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_deliver_nonexistent_group(self, fanout_engine):
        result = await fanout_engine.deliver("nonexistent", payload={})
        assert result.status == GroupDeliveryStatus.FAILED
        assert "not found" in result.error.lower()

    @pytest.mark.asyncio
    async def test_deliver_paused_group(self, fanout_engine):
        group = fanout_engine.create_group(name="paused", members=[{"endpoint_id": "ep1"}])
        fanout_engine.update_group(group.id, status=GroupStatus.PAUSED)
        result = await fanout_engine.deliver(group.id, payload={})
        assert result.status == GroupDeliveryStatus.FAILED
        assert "not active" in result.error.lower()

    @pytest.mark.asyncio
    async def test_deliver_no_active_members(self, fanout_engine):
        group = fanout_engine.create_group(
            name="test",
            members=[{"endpoint_id": "ep1", "enabled": False}],
        )
        result = await fanout_engine.deliver(group.id, payload={})
        assert result.status == GroupDeliveryStatus.FAILED
        assert "no active members" in result.error.lower()

    @pytest.mark.asyncio
    async def test_parallel_all_success(self, fanout_engine):
        """Test parallel delivery with a mock callback."""
        group = fanout_engine.create_group(
            name="parallel",
            members=[{"endpoint_id": f"ep{i}"} for i in range(5)],
            strategy="parallel",
            failure_strategy="all_must_succeed",
        )

        async def callback(endpoint_id, payload, headers=None):
            return {"delivery_id": f"del-{endpoint_id}", "success": True, "status_code": 200}

        result = await fanout_engine.deliver(group.id, {"test": True}, delivery_callback=callback)
        assert result.status == GroupDeliveryStatus.SUCCESS
        assert result.success_count == 5
        assert result.failure_count == 0
        assert len(result.member_results) == 5

    @pytest.mark.asyncio
    async def test_parallel_partial_failure(self, fanout_engine):
        """Some members fail, all_must_succeed → FAILED."""
        group = fanout_engine.create_group(
            name="partial",
            members=[{"endpoint_id": "ep1"}, {"endpoint_id": "ep2"}, {"endpoint_id": "ep3"}],
            failure_strategy="all_must_succeed",
        )

        async def callback(endpoint_id, payload, headers=None):
            success = endpoint_id != "ep2"
            return {"success": success, "status_code": 200 if success else 500}

        result = await fanout_engine.deliver(group.id, {}, delivery_callback=callback)
        assert result.status == GroupDeliveryStatus.FAILED
        assert result.success_count == 2
        assert result.failure_count == 1

    @pytest.mark.asyncio
    async def test_parallel_any_success(self, fanout_engine):
        """Any one success → SUCCESS."""
        group = fanout_engine.create_group(
            name="any",
            members=[{"endpoint_id": "ep1"}, {"endpoint_id": "ep2"}],
            failure_strategy="any_success",
        )

        async def callback(endpoint_id, payload, headers=None):
            success = endpoint_id == "ep1"
            return {"success": success}

        result = await fanout_engine.deliver(group.id, {}, delivery_callback=callback)
        assert result.status == GroupDeliveryStatus.SUCCESS

    @pytest.mark.asyncio
    async def test_sequential_early_stop_any_success(self, fanout_engine):
        """Sequential with any_success stops after first success."""
        group = fanout_engine.create_group(
            name="seq",
            members=[{"endpoint_id": f"ep{i}"} for i in range(5)],
            strategy="sequential",
            failure_strategy="any_success",
        )

        delivered_to = []

        async def callback(endpoint_id, payload, headers=None):
            delivered_to.append(endpoint_id)
            return {"success": endpoint_id == "ep1"}

        result = await fanout_engine.deliver(group.id, {}, delivery_callback=callback)
        assert result.status == GroupDeliveryStatus.SUCCESS
        # Should only have tried ep0 and ep1 (stops after first success)
        assert len(delivered_to) <= 2
        assert "ep1" in delivered_to

    @pytest.mark.asyncio
    async def test_sequential_early_stop_all_must_succeed(self, fanout_engine):
        """Sequential with all_must_succeed stops after first failure."""
        group = fanout_engine.create_group(
            name="seq-all",
            members=[{"endpoint_id": f"ep{i}"} for i in range(5)],
            strategy="sequential",
            failure_strategy="all_must_succeed",
        )

        delivered_to = []

        async def callback(endpoint_id, payload, headers=None):
            delivered_to.append(endpoint_id)
            success = endpoint_id != "ep1"
            return {"success": success}

        result = await fanout_engine.deliver(group.id, {}, delivery_callback=callback)
        assert result.status == GroupDeliveryStatus.FAILED
        # Should stop after ep1 fails (ep0 succeeds, ep1 fails → stop)
        assert len(delivered_to) <= 2

    @pytest.mark.asyncio
    async def test_callback_exception_handled(self, fanout_engine):
        """If callback raises, it's treated as a failure."""
        group = fanout_engine.create_group(
            name="error",
            members=[{"endpoint_id": "ep1"}],
        )

        async def callback(endpoint_id, payload, headers=None):
            raise ConnectionError("Network down")

        result = await fanout_engine.deliver(group.id, {}, delivery_callback=callback)
        assert result.status == GroupDeliveryStatus.FAILED
        assert result.failure_count == 1
        assert "Network down" in result.member_results[0].error

    @pytest.mark.asyncio
    async def test_max_concurrent_limit(self, fanout_engine):
        """Parallel with max_concurrent limits concurrency."""
        group = fanout_engine.create_group(
            name="limited",
            members=[{"endpoint_id": f"ep{i}"} for i in range(5)],
            strategy="parallel",
            max_concurrent=2,
        )

        concurrent = {"current": 0, "max": 0}

        async def callback(endpoint_id, payload, headers=None):
            concurrent["current"] += 1
            concurrent["max"] = max(concurrent["max"], concurrent["current"])
            await asyncio.sleep(0.05)
            concurrent["current"] -= 1
            return {"success": True}

        result = await fanout_engine.deliver(group.id, {}, delivery_callback=callback)
        assert result.status == GroupDeliveryStatus.SUCCESS
        assert concurrent["max"] <= 2

    @pytest.mark.asyncio
    async def test_weighted_selection(self, fanout_engine):
        """Weighted strategy selects a subset of members."""
        group = fanout_engine.create_group(
            name="weighted",
            members=[
                {"endpoint_id": "ep1", "weight": 1},
                {"endpoint_id": "ep2", "weight": 100},  # Very high weight
            ],
            strategy="weighted",
        )

        async def callback(endpoint_id, payload, headers=None):
            return {"success": True}

        result = await fanout_engine.deliver(group.id, {}, delivery_callback=callback)
        assert result.status == GroupDeliveryStatus.SUCCESS
        assert result.total_members == 2
        # At least 1 member should have been selected
        assert result.attempted_count >= 1

    @pytest.mark.asyncio
    async def test_member_header_override(self, fanout_engine):
        """Per-member headers are merged with delivery headers."""
        group = fanout_engine.create_group(
            name="headers",
            members=[{"endpoint_id": "ep1", "headers": {"X-Route": "v2"}}],
        )

        received_headers = {}

        async def callback(endpoint_id, payload, headers=None):
            received_headers.update(headers or {})
            return {"success": True}

        await fanout_engine.deliver(
            group.id, {}, headers={"X-Env": "prod"}, delivery_callback=callback,
        )
        assert "X-Env" in received_headers
        assert "X-Route" in received_headers

    @pytest.mark.asyncio
    async def test_result_has_duration(self, fanout_engine):
        group = fanout_engine.create_group(
            name="test", members=[{"endpoint_id": "ep1"}],
        )

        async def callback(endpoint_id, payload, headers=None):
            await asyncio.sleep(0.01)
            return {"success": True, "duration_ms": 10.0}

        result = await fanout_engine.deliver(group.id, {}, delivery_callback=callback)
        assert result.duration_ms is not None
        assert result.duration_ms > 0


# ── FanoutEngine Query Tests ────────────────────────────────────────


class TestFanoutQuery:
    """Tests for querying group deliveries and stats."""

    @pytest.mark.asyncio
    async def test_list_deliveries_by_group(self, fanout_engine):
        g1 = fanout_engine.create_group(name="g1", members=[{"endpoint_id": "ep1"}])
        g2 = fanout_engine.create_group(name="g2", members=[{"endpoint_id": "ep2"}])

        await fanout_engine.deliver(g1.id, {})
        await fanout_engine.deliver(g2.id, {})
        await fanout_engine.deliver(g1.id, {})

        g1_deliveries = fanout_engine.list_deliveries(group_id=g1.id)
        assert len(g1_deliveries) == 2

    @pytest.mark.asyncio
    async def test_list_deliveries_by_status(self, fanout_engine):
        group = fanout_engine.create_group(
            name="test",
            members=[{"endpoint_id": "ep1"}],
        )
        await fanout_engine.deliver(group.id, {})
        deliveries = fanout_engine.list_deliveries(status="success")
        assert len(deliveries) == 1

    @pytest.mark.asyncio
    async def test_list_deliveries_with_limit(self, fanout_engine):
        group = fanout_engine.create_group(name="test", members=[{"endpoint_id": "ep1"}])
        for _ in range(5):
            await fanout_engine.deliver(group.id, {})
        deliveries = fanout_engine.list_deliveries(limit=3)
        assert len(deliveries) <= 3

    @pytest.mark.asyncio
    async def test_group_stats_empty(self, fanout_engine):
        group = fanout_engine.create_group(name="test", members=[])
        stats = fanout_engine.group_stats(group.id)
        assert stats["total_deliveries"] == 0
        assert stats["success_rate"] is None

    @pytest.mark.asyncio
    async def test_group_stats_with_deliveries(self, fanout_engine):
        group = fanout_engine.create_group(
            name="test", members=[{"endpoint_id": "ep1"}],
        )
        await fanout_engine.deliver(group.id, {})
        await fanout_engine.deliver(group.id, {})
        stats = fanout_engine.group_stats(group.id)
        assert stats["total_deliveries"] == 2
        assert stats["successful"] == 2
        assert stats["success_rate"] == 1.0
        assert stats["last_delivery_at"] is not None


# ── WebhookService Integration ──────────────────────────────────────


class TestFanoutServiceIntegration:
    """Tests for fan-out features through WebhookService."""

    def test_service_has_fanout(self, service):
        assert hasattr(service, "fanout")
        assert service.fanout is not None

    def test_create_group_with_string_members(self, service, multi_endpoints):
        group = service.create_delivery_group(
            name="multi",
            members=[ep.id for ep in multi_endpoints[:3]],
        )
        assert group.name == "multi"
        assert len(group.members) == 3

    def test_create_group_with_dict_members(self, service, multi_endpoints):
        group = service.create_delivery_group(
            name="weighted",
            members=[
                {"endpoint_id": multi_endpoints[0].id, "weight": 3},
                {"endpoint_id": multi_endpoints[1].id, "weight": 1},
            ],
            strategy="weighted",
        )
        assert group.total_weight() == 4

    def test_get_group(self, service):
        group = service.create_delivery_group(name="test", members=[])
        retrieved = service.get_delivery_group(group.id)
        assert retrieved is not None
        assert retrieved.name == "test"

    def test_list_groups(self, service):
        service.create_delivery_group(name="g1", members=[])
        service.create_delivery_group(name="g2", members=[])
        assert len(service.list_delivery_groups()) == 2

    def test_update_group(self, service):
        group = service.create_delivery_group(name="test", members=[])
        updated = service.update_delivery_group(group.id, description="Updated")
        assert updated.description == "Updated"

    def test_delete_group(self, service):
        group = service.create_delivery_group(name="test", members=[])
        assert service.delete_delivery_group(group.id) is True
        assert service.get_delivery_group(group.id) is None

    def test_add_member_via_service(self, service, endpoint):
        group = service.create_delivery_group(name="test", members=[])
        updated = service.add_group_member(group.id, endpoint.id, weight=2)
        assert len(updated.members) == 1

    def test_remove_member_via_service(self, service, endpoint):
        group = service.create_delivery_group(name="test", members=[endpoint.id])
        assert service.remove_group_member(group.id, endpoint.id) is True
        g = service.get_delivery_group(group.id)
        assert len(g.members) == 0

    @pytest.mark.asyncio
    async def test_fanout_deliver_dry_run(self, service, multi_endpoints):
        group = service.create_delivery_group(
            name="test",
            members=[ep.id for ep in multi_endpoints[:3]],
        )
        result = await service.fanout_deliver_dry_run(
            group.id, payload={"event": "test"}, event_type="order.created",
        )
        assert result.status == GroupDeliveryStatus.SUCCESS
        assert result.success_count == 3
        assert result.event_type == "order.created"

    @pytest.mark.asyncio
    async def test_list_group_deliveries(self, service, multi_endpoints):
        group = service.create_delivery_group(
            name="test",
            members=[ep.id for ep in multi_endpoints[:2]],
        )
        await service.fanout_deliver_dry_run(group.id, {})
        await service.fanout_deliver_dry_run(group.id, {})
        deliveries = service.list_group_deliveries(group_id=group.id)
        assert len(deliveries) == 2

    @pytest.mark.asyncio
    async def test_get_group_delivery(self, service, multi_endpoints):
        group = service.create_delivery_group(
            name="test", members=[multi_endpoints[0].id],
        )
        result = await service.fanout_deliver_dry_run(group.id, {})
        fetched = service.get_group_delivery(result.id)
        assert fetched is not None
        assert fetched.id == result.id

    @pytest.mark.asyncio
    async def test_group_delivery_stats(self, service, multi_endpoints):
        group = service.create_delivery_group(
            name="test", members=[ep.id for ep in multi_endpoints[:3]],
        )
        await service.fanout_deliver_dry_run(group.id, {})
        stats = service.group_delivery_stats(group.id)
        assert stats["total_deliveries"] == 1
        assert stats["successful"] == 1

    @pytest.mark.asyncio
    async def test_json_store_fallback(self):
        """Fan-out should work even with JSON store (no SQLite)."""
        import tempfile
        path = tempfile.mktemp(suffix=".json")
        try:
            svc = WebhookService(store_path=path)
            group = svc.create_delivery_group(
                name="test", members=["ep1", "ep2"],
            )
            assert group is not None
            result = await svc.fanout_deliver_dry_run(group.id, {"x": 1})
            assert result.status == GroupDeliveryStatus.SUCCESS
        finally:
            Path(path).unlink(missing_ok=True)


# ── REST API Tests ──────────────────────────────────────────────────


class TestFanoutRestAPI:
    """Tests for the fan-out REST API endpoints."""

    @pytest.fixture
    def client(self, tmp_db):
        from fastapi.testclient import TestClient
        from agent_webhook.rest_api import create_app

        app = create_app(store_path=tmp_db)
        return TestClient(app)

    def test_create_group_endpoint(self, client):
        resp = client.post("/groups", json={
            "name": "test-group",
            "members": [{"endpoint_id": "ep1"}, {"endpoint_id": "ep2"}],
            "strategy": "parallel",
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["name"] == "test-group"
        assert len(data["members"]) == 2
        assert "id" in data

    def test_get_group_endpoint(self, client):
        create_resp = client.post("/groups", json={"name": "test", "members": []})
        group_id = create_resp.json()["id"]
        resp = client.get(f"/groups/{group_id}")
        assert resp.status_code == 200
        assert resp.json()["name"] == "test"

    def test_get_group_not_found(self, client):
        resp = client.get("/groups/nonexistent")
        assert resp.status_code == 404

    def test_list_groups_endpoint(self, client):
        client.post("/groups", json={"name": "g1", "members": []})
        client.post("/groups", json={"name": "g2", "members": []})
        resp = client.get("/groups")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] == 2

    def test_update_group_endpoint(self, client):
        create_resp = client.post("/groups", json={"name": "test", "members": []})
        group_id = create_resp.json()["id"]
        resp = client.patch(f"/groups/{group_id}", json={"description": "Updated"})
        assert resp.status_code == 200
        assert resp.json()["description"] == "Updated"

    def test_delete_group_endpoint(self, client):
        create_resp = client.post("/groups", json={"name": "test", "members": []})
        group_id = create_resp.json()["id"]
        resp = client.delete(f"/groups/{group_id}")
        assert resp.status_code == 200
        # Verify deleted
        resp = client.get(f"/groups/{group_id}")
        assert resp.status_code == 404

    def test_add_group_member_endpoint(self, client):
        create_resp = client.post("/groups", json={"name": "test", "members": []})
        group_id = create_resp.json()["id"]
        resp = client.post(f"/groups/{group_id}/members", json={
            "endpoint_id": "ep1",
            "weight": 3,
        })
        assert resp.status_code == 200
        assert len(resp.json()["members"]) == 1

    def test_remove_group_member_endpoint(self, client):
        create_resp = client.post("/groups", json={
            "name": "test",
            "members": [{"endpoint_id": "ep1"}],
        })
        group_id = create_resp.json()["id"]
        resp = client.delete(f"/groups/{group_id}/members/ep1")
        assert resp.status_code == 200
        assert resp.json()["removed"] is True

    def test_group_deliver_dry_run_endpoint(self, client):
        # Create group
        create_resp = client.post("/groups", json={
            "name": "test",
            "members": [{"endpoint_id": "ep1"}, {"endpoint_id": "ep2"}],
        })
        group_id = create_resp.json()["id"]

        # Dry-run delivery
        resp = client.post(f"/groups/{group_id}/deliver", json={
            "payload": {"event": "test"},
            "event_type": "order.created",
            "dry_run": True,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "success"
        assert data["success_count"] == 2

    def test_list_group_deliveries_endpoint(self, client):
        create_resp = client.post("/groups", json={
            "name": "test",
            "members": [{"endpoint_id": "ep1"}],
        })
        group_id = create_resp.json()["id"]

        # Do a delivery
        client.post(f"/groups/{group_id}/deliver", json={
            "payload": {}, "dry_run": True,
        })

        resp = client.get(f"/groups/{group_id}/deliveries")
        assert resp.status_code == 200
        assert resp.json()["count"] >= 1

    def test_group_stats_endpoint(self, client):
        create_resp = client.post("/groups", json={
            "name": "test",
            "members": [{"endpoint_id": "ep1"}],
        })
        group_id = create_resp.json()["id"]

        # Do a delivery
        client.post(f"/groups/{group_id}/deliver", json={
            "payload": {}, "dry_run": True,
        })

        resp = client.get(f"/groups/{group_id}/stats")
        assert resp.status_code == 200
        assert resp.json()["total_deliveries"] == 1
