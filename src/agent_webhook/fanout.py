"""Fan-out delivery engine — delivers a single payload to a group of endpoints.

Supports three strategies:

- **parallel**: All active members receive concurrently (bounded by ``max_concurrent``).
- **sequential**: Members are called one at a time; stops early if the failure
  strategy is already satisfied (e.g. any_success after the first hit).
- **weighted**: Members are selected by weight — only the weighted-fraction
  of members receive the event, useful for canary / blue-green rollouts.
"""

from __future__ import annotations

import asyncio
import random
from datetime import datetime, timezone
from typing import Any, Protocol

from .models import (
    DeliveryGroup,
    DeliveryGroupMember,
    FanoutStrategy,
    GroupDeliveryResult,
    GroupDeliveryStatus,
    GroupFailureStrategy,
    GroupMemberResult,
    GroupMemberStatus,
    GroupStatus,
)


class DeliveryCallable(Protocol):
    """Protocol for the delivery callback used by the fan-out engine."""

    async def __call__(
        self,
        endpoint_id: str,
        payload: dict[str, Any],
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Deliver to one endpoint.

        Returns a dict with keys: ``delivery_id`` (str|None),
        ``success`` (bool), ``status_code`` (int|None),
        ``error`` (str|None), ``duration_ms`` (float|None).
        """
        ...


class FanoutEngine:
    """Engine that orchestrates fan-out deliveries to endpoint groups."""

    def __init__(self, store: Any):
        self._store = store

    # ── Group CRUD passthrough ──────────────────────────────────────

    def create_group(
        self,
        name: str,
        members: list[dict[str, Any]],
        strategy: str = "parallel",
        failure_strategy: str = "all_must_succeed",
        success_threshold: float = 1.0,
        description: str | None = None,
        max_concurrent: int = 0,
        tags: list[str] | None = None,
    ) -> DeliveryGroup:
        """Create a delivery group from raw kwargs."""
        group = DeliveryGroup(
            name=name,
            description=description,
            members=[DeliveryGroupMember(**m) for m in members],
            strategy=FanoutStrategy(strategy),
            failure_strategy=GroupFailureStrategy(failure_strategy),
            success_threshold=success_threshold,
            max_concurrent=max_concurrent,
            tags=tags or [],
        )
        return self._store_group(group)

    def _store_group(self, group: DeliveryGroup) -> DeliveryGroup:
        """Store a group using whatever store method is available."""
        if hasattr(self._store, "add_delivery_group"):
            return self._store.add_delivery_group(group)
        # Fallback for JSON store: keep in-memory
        if not hasattr(self._store, "_groups"):
            self._store._groups = {}
        self._store._groups[group.id] = group
        return group

    def get_group(self, group_id: str) -> DeliveryGroup | None:
        if hasattr(self._store, "get_delivery_group"):
            return self._store.get_delivery_group(group_id)
        if hasattr(self._store, "_groups"):
            return self._store._groups.get(group_id)
        return None

    def list_groups(
        self,
        status: str | GroupStatus | None = None,
        tag: str | None = None,
    ) -> list[DeliveryGroup]:
        if hasattr(self._store, "list_delivery_groups"):
            return self._store.list_delivery_groups(status=status, tag=tag)
        if hasattr(self._store, "_groups"):
            groups = list(self._store._groups.values())
            if status is not None:
                status_val = status.value if isinstance(status, GroupStatus) else status
                groups = [g for g in groups if g.status.value == status_val]
            if tag is not None:
                groups = [g for g in groups if tag in g.tags]
            return groups
        return []

    def update_group(self, group_id: str, **updates: Any) -> DeliveryGroup | None:
        if hasattr(self._store, "update_delivery_group"):
            return self._store.update_delivery_group(group_id, **updates)
        if hasattr(self._store, "_groups"):
            group = self._store._groups.get(group_id)
            if group is None:
                return None
            for k, v in updates.items():
                if hasattr(group, k):
                    setattr(group, k, v)
            group.updated_at = datetime.now(timezone.utc)
            return group
        return None

    def delete_group(self, group_id: str) -> bool:
        if hasattr(self._store, "delete_delivery_group"):
            return self._store.delete_delivery_group(group_id)
        if hasattr(self._store, "_groups"):
            if group_id in self._store._groups:
                del self._store._groups[group_id]
                return True
            return False
        return False

    def add_member(self, group_id: str, endpoint_id: str, weight: int = 1,
                   headers: dict[str, str] | None = None) -> DeliveryGroup | None:
        group = self.get_group(group_id)
        if group is None:
            return None
        group.add_member(endpoint_id, weight=weight, headers=headers)
        return self._replace_group(group)

    def remove_member(self, group_id: str, endpoint_id: str) -> bool:
        group = self.get_group(group_id)
        if group is None:
            return False
        removed = group.remove_member(endpoint_id)
        if removed:
            self._replace_group(group)
        return removed

    def _replace_group(self, group: DeliveryGroup) -> DeliveryGroup:
        """Persist a fully-modified group object back to the store."""
        group.updated_at = datetime.now(timezone.utc)
        if hasattr(self._store, "delete_delivery_group") and hasattr(self._store, "add_delivery_group"):
            self._store.delete_delivery_group(group.id)
            return self._store.add_delivery_group(group)
        if hasattr(self._store, "_groups"):
            self._store._groups[group.id] = group
        return group

    def _save_group(self, group: DeliveryGroup) -> DeliveryGroup:
        """Re-save an updated group (legacy path, less reliable)."""
        return self._replace_group(group)

    # ── Delivery ────────────────────────────────────────────────────

    async def deliver(
        self,
        group_id: str,
        payload: dict[str, Any],
        event_type: str | None = None,
        headers: dict[str, str] | None = None,
        delivery_callback: DeliveryCallable | None = None,
    ) -> GroupDeliveryResult:
        """Deliver ``payload`` to all active members of the named group.

        Returns a :class:`GroupDeliveryResult` with per-member outcomes.
        """
        group = self.get_group(group_id)
        started = datetime.now(timezone.utc)

        # Build base result
        result = GroupDeliveryResult(
            group_id=group_id,
            group_name=group.name if group else "",
            event_type=event_type,
            payload=payload,
            strategy=group.strategy if group else FanoutStrategy.PARALLEL,
            failure_strategy=group.failure_strategy if group else GroupFailureStrategy.ALL_MUST_SUCCEED,
            success_threshold=group.success_threshold if group else 1.0,
            status=GroupDeliveryStatus.IN_PROGRESS,
            started_at=started,
        )

        if group is None:
            result.status = GroupDeliveryStatus.FAILED
            result.error = f"Group not found: {group_id}"
            result.completed_at = datetime.now(timezone.utc)
            self._persist_result(result)
            return result

        if not group.is_active():
            result.status = GroupDeliveryStatus.FAILED
            result.error = f"Group '{group.name}' is not active (status={group.status.value})"
            result.completed_at = datetime.now(timezone.utc)
            self._persist_result(result)
            return result

        active = group.active_members()
        result.total_members = len(active)

        if not active:
            result.status = GroupDeliveryStatus.FAILED
            result.error = f"Group '{group.name}' has no active members"
            result.completed_at = datetime.now(timezone.utc)
            self._persist_result(result)
            return result

        # Select members based on strategy
        if group.strategy == FanoutStrategy.WEIGHTED:
            selected = self._weighted_select(active)
        else:
            selected = active

        # If no delivery callback, simulate (dry-run)
        if delivery_callback is None:
            await self._dry_run(result, selected)
        elif group.strategy == FanoutStrategy.SEQUENTIAL:
            await self._deliver_sequential(result, selected, payload, headers, delivery_callback, group)
        else:
            await self._deliver_parallel(result, selected, payload, headers, delivery_callback, group)

        # Evaluate aggregate status
        result.status = result.evaluate_status()
        result.completed_at = datetime.now(timezone.utc)
        result.duration_ms = (result.completed_at - started).total_seconds() * 1000

        self._persist_result(result)
        return result

    async def _dry_run(
        self,
        result: GroupDeliveryResult,
        members: list[DeliveryGroupMember],
    ) -> None:
        """Simulate delivery without actually sending."""
        for member in members:
            result.member_results.append(GroupMemberResult(
                endpoint_id=member.endpoint_id,
                status=GroupMemberStatus.SUCCESS,
            ))
            result.success_count += 1

    async def _deliver_parallel(
        self,
        result: GroupDeliveryResult,
        members: list[DeliveryGroupMember],
        payload: dict[str, Any],
        headers: dict[str, str] | None,
        callback: DeliveryCallable,
        group: DeliveryGroup,
    ) -> None:
        """Deliver to all members concurrently (bounded by max_concurrent)."""
        max_concurrent = group.max_concurrent if group.max_concurrent > 0 else len(members)
        semaphore = asyncio.Semaphore(max_concurrent)

        async def _deliver_one(member: DeliveryGroupMember) -> GroupMemberResult:
            merged_headers = {**(headers or {}), **member.headers}
            async with semaphore:
                return await self._deliver_single(member, payload, merged_headers, callback)

        tasks = [asyncio.create_task(_deliver_one(m)) for m in members]
        outcomes = await asyncio.gather(*tasks, return_exceptions=False)

        for outcome in outcomes:
            result.member_results.append(outcome)
            if outcome.status == GroupMemberStatus.SUCCESS:
                result.success_count += 1
            elif outcome.status == GroupMemberStatus.SKIPPED:
                result.skipped_count += 1
            else:
                result.failure_count += 1

    async def _deliver_sequential(
        self,
        result: GroupDeliveryResult,
        members: list[DeliveryGroupMember],
        payload: dict[str, Any],
        headers: dict[str, str] | None,
        callback: DeliveryCallable,
        group: DeliveryGroup,
    ) -> None:
        """Deliver to members one at a time, with early-stop optimization."""
        fs = group.failure_strategy

        for i, member in enumerate(members):
            merged_headers = {**(headers or {}), **member.headers}
            member_result = await self._deliver_single(member, payload, merged_headers, callback)
            member_result.attempt = i
            result.member_results.append(member_result)

            if member_result.status == GroupMemberStatus.SUCCESS:
                result.success_count += 1
            elif member_result.status == GroupMemberStatus.SKIPPED:
                result.skipped_count += 1
            else:
                result.failure_count += 1

            # Early-stop optimization: if the failure strategy is already
            # satisfied, skip the remaining members.
            if self._can_stop_early(fs, result.success_count, result.failure_count,
                                     len(members) - i - 1, group.success_threshold):
                remaining = len(members) - i - 1
                for _ in range(remaining):
                    result.member_results.append(GroupMemberResult(
                        endpoint_id=members[i + 1 + len([
                            r for r in result.member_results
                            if r.endpoint_id == members[i + 1].endpoint_id
                        ])].endpoint_id if i + 1 < len(members) else "",
                        status=GroupMemberStatus.SKIPPED,
                    ))
                    result.skipped_count += 1
                break

    def _can_stop_early(
        self,
        fs: GroupFailureStrategy,
        successes: int,
        failures: int,
        remaining: int,
        threshold: float,
    ) -> bool:
        """Check if we can stop early given current results.

        For ANY_SUCCESS, stop as soon as one succeeds.
        For ALL_MUST_SUCCEED, stop as soon as one fails.
        """
        if fs == GroupFailureStrategy.ANY_SUCCESS:
            return successes >= 1
        if fs == GroupFailureStrategy.ALL_MUST_SUCCEED:
            return failures >= 1
        return False

    async def _deliver_single(
        self,
        member: DeliveryGroupMember,
        payload: dict[str, Any],
        headers: dict[str, str] | None,
        callback: DeliveryCallable,
    ) -> GroupMemberResult:
        """Deliver to a single member. Returns the member result."""
        try:
            outcome = await callback(
                endpoint_id=member.endpoint_id,
                payload=payload,
                headers=headers,
            )
            success = outcome.get("success", False)
            return GroupMemberResult(
                endpoint_id=member.endpoint_id,
                delivery_id=outcome.get("delivery_id"),
                status=GroupMemberStatus.SUCCESS if success else GroupMemberStatus.FAILED,
                status_code=outcome.get("status_code"),
                error=outcome.get("error"),
                duration_ms=outcome.get("duration_ms"),
            )
        except asyncio.TimeoutError:
            return GroupMemberResult(
                endpoint_id=member.endpoint_id,
                status=GroupMemberStatus.TIMEOUT,
                error="Delivery timed out",
            )
        except Exception as exc:
            return GroupMemberResult(
                endpoint_id=member.endpoint_id,
                status=GroupMemberStatus.FAILED,
                error=str(exc),
            )

    def _weighted_select(self, members: list[DeliveryGroupMember]) -> list[DeliveryGroupMember]:
        """Select members based on their weights.

        Uses weighted random selection to determine which members receive
        the event. Each member is independently selected with probability
        proportional to its weight.

        At minimum one member is always selected (the highest-weight one).
        """
        if not members:
            return []
        if len(members) == 1:
            return members

        total_weight = sum(m.weight for m in members)
        selected: list[DeliveryGroupMember] = []

        for member in members:
            # Probability of selecting this member = weight / total_weight
            prob = member.weight / total_weight
            if random.random() < prob:
                selected.append(member)

        # Ensure at least one member is selected
        if not selected:
            selected.append(max(members, key=lambda m: m.weight))

        return selected

    def _persist_result(self, result: GroupDeliveryResult) -> None:
        """Persist delivery result if the store supports it."""
        if hasattr(self._store, "add_group_delivery"):
            try:
                self._store.add_group_delivery(result)
            except Exception:
                pass
        elif hasattr(self._store, "_group_deliveries"):
            self._store._group_deliveries[result.id] = result

    # ── Query ───────────────────────────────────────────────────────

    def get_delivery(self, delivery_id: str) -> GroupDeliveryResult | None:
        if hasattr(self._store, "get_group_delivery"):
            return self._store.get_group_delivery(delivery_id)
        if hasattr(self._store, "_group_deliveries"):
            return self._store._group_deliveries.get(delivery_id)
        return None

    def list_deliveries(
        self,
        group_id: str | None = None,
        status: str | GroupDeliveryStatus | None = None,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[GroupDeliveryResult]:
        if hasattr(self._store, "list_group_deliveries"):
            return self._store.list_group_deliveries(
                group_id=group_id, status=status, event_type=event_type, limit=limit,
            )
        if hasattr(self._store, "_group_deliveries"):
            results = list(self._store._group_deliveries.values())
            if group_id is not None:
                results = [r for r in results if r.group_id == group_id]
            if status is not None:
                status_val = status.value if isinstance(status, GroupDeliveryStatus) else status
                results = [r for r in results if r.status.value == status_val]
            if event_type is not None:
                results = [r for r in results if r.event_type == event_type]
            results.sort(key=lambda r: r.started_at, reverse=True)
            return results[:limit]
        return []

    def group_stats(self, group_id: str) -> dict[str, Any]:
        """Aggregate stats for a delivery group."""
        deliveries = self.list_deliveries(group_id=group_id, limit=10000)
        total = len(deliveries)
        success = sum(1 for d in deliveries if d.status == GroupDeliveryStatus.SUCCESS)
        failed = sum(1 for d in deliveries if d.status == GroupDeliveryStatus.FAILED)
        partial = sum(1 for d in deliveries if d.status == GroupDeliveryStatus.PARTIAL)
        total_member_deliveries = sum(d.attempted_count for d in deliveries)
        total_member_success = sum(d.success_count for d in deliveries)

        return {
            "group_id": group_id,
            "total_deliveries": total,
            "successful": success,
            "failed": failed,
            "partial": partial,
            "success_rate": round(success / total, 4) if total > 0 else None,
            "member_delivery_rate": round(total_member_success / total_member_deliveries, 4) if total_member_deliveries > 0 else None,
            "last_delivery_at": deliveries[0].started_at.isoformat() if deliveries else None,
        }
