"""Delivery analytics — success rates, latency percentiles, error analysis, trends.

Provides actionable insights into webhook delivery performance:

* **Overall metrics** — total deliveries, success rate, latency percentiles.
* **Time-bucketed trends** — throughput and success rate over hourly windows.
* **Error analysis** — breakdown of failures by HTTP status code and error message.
* **Endpoint ranking** — top endpoints by volume, worst by failure rate.
* **Health scoring** — 0-100 score combining reliability, latency, and error factors.

Usage::

    from agent_webhook.analytics import AnalyticsEngine
    from agent_webhook.service import WebhookService

    service = WebhookService(store_path="webhooks.db")
    engine = AnalyticsEngine(service)
    report = engine.overall_report()
    print(report["summary"])
"""

from __future__ import annotations

import math
from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any

from .models import DeliveryStatus, WebhookDelivery
from .service import WebhookService


class AnalyticsEngine:
    """Computes analytics from the delivery store.

    All methods return plain ``dict`` / ``list`` structures suitable for
    JSON serialisation (MCP tools, REST API, CLI output).
    """

    def __init__(self, service: WebhookService) -> None:
        self._service = service

    # ── Helpers ────────────────────────────────────────────────────────

    @staticmethod
    def _percentile(values: list[float], p: float) -> float | None:
        """Return the *p*-th percentile (0-100) of *values*, or ``None`` if empty."""
        if not values:
            return None
        s = sorted(values)
        if len(s) == 1:
            return round(s[0], 2)
        k = (len(s) - 1) * (p / 100.0)
        f = math.floor(k)
        c = math.ceil(k)
        if f == c:
            return round(s[int(k)], 2)
        # Linear interpolation
        return round(s[f] + (s[c] - s[f]) * (k - f), 2)

    @staticmethod
    def _round_dt(dt: datetime, to: str = "hour") -> datetime:
        """Truncate a datetime to the given granularity."""
        if to == "minute":
            return dt.replace(second=0, microsecond=0)
        if to == "day":
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        # hour (default)
        return dt.replace(minute=0, second=0, microsecond=0)

    def _all_deliveries(self, limit: int = 10000) -> list[WebhookDelivery]:
        """Fetch deliveries from the store (capped to avoid memory blow-up)."""
        return self._service.store.list_deliveries(limit=limit)

    # ── Overall report ────────────────────────────────────────────────

    def overall_report(self, limit: int = 10000) -> dict[str, Any]:
        """Generate a comprehensive analytics report.

        Returns a dict with ``summary``, ``latency``, ``error_breakdown``,
        ``top_endpoints``, and ``trend`` sub-objects.
        """
        deliveries = self._all_deliveries(limit)

        total = len(deliveries)
        statuses: Counter = Counter()
        durations: list[float] = []
        status_codes: Counter = Counter()
        error_messages: Counter = Counter()

        for d in deliveries:
            statuses[d.status.value] += 1
            for attempt in d.attempts:
                if attempt.duration_ms is not None:
                    durations.append(attempt.duration_ms)
                if attempt.response_status_code is not None:
                    status_codes[attempt.response_status_code] += 1
                if attempt.error_message:
                    # Truncate long errors for grouping
                    err = attempt.error_message[:200]
                    error_messages[err] += 1

        successful = statuses.get(DeliveryStatus.SUCCESS.value, 0)
        failed = statuses.get(DeliveryStatus.FAILED.value, 0)
        dead_letter = statuses.get(DeliveryStatus.DEAD_LETTER.value, 0)
        abandoned = statuses.get(DeliveryStatus.ABANDONED.value, 0)
        pending = statuses.get(DeliveryStatus.PENDING.value, 0)
        retrying = statuses.get(DeliveryStatus.RETRYING.value, 0)
        completed = successful + failed + dead_letter + abandoned

        success_rate = (successful / completed * 100) if completed > 0 else None

        latency: dict[str, Any] = {}
        if durations:
            latency = {
                "count": len(durations),
                "min_ms": round(min(durations), 2),
                "max_ms": round(max(durations), 2),
                "avg_ms": round(sum(durations) / len(durations), 2),
                "p50_ms": self._percentile(durations, 50),
                "p90_ms": self._percentile(durations, 90),
                "p95_ms": self._percentile(durations, 95),
                "p99_ms": self._percentile(durations, 99),
            }
        else:
            latency = {"count": 0, "min_ms": None, "max_ms": None,
                       "avg_ms": None, "p50_ms": None, "p90_ms": None,
                       "p95_ms": None, "p99_ms": None}

        # Error breakdown
        error_breakdown = {
            "by_status_code": [
                {"status_code": sc, "count": cnt}
                for sc, cnt in status_codes.most_common(20)
            ],
            "by_message": [
                {"error": msg, "count": cnt}
                for msg, cnt in error_messages.most_common(20)
            ],
        }

        # Top/worst endpoints
        per_endpoint = self._per_endpoint_stats(deliveries)
        top_by_volume = sorted(
            per_endpoint.values(),
            key=lambda x: x["total"],
            reverse=True,
        )[:10]
        worst_by_failure = sorted(
            [e for e in per_endpoint.values() if e["total"] >= 3],
            key=lambda x: x["failure_rate"],
            reverse=True,
        )[:10]

        # Time trend (hourly buckets)
        trend = self._trend(deliveries, granularity="hour")

        # Health score
        health_score = self._compute_health_score(
            success_rate=success_rate,
            avg_latency=latency.get("avg_ms"),
            p99_latency=latency.get("p99_ms"),
            dead_letter_rate=(dead_letter / total * 100) if total > 0 else 0,
            retry_rate=(retrying / total * 100) if total > 0 else 0,
        )

        return {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "summary": {
                "total_deliveries": total,
                "successful": successful,
                "failed": failed,
                "pending": pending,
                "retrying": retrying,
                "dead_letter": dead_letter,
                "abandoned": abandoned,
                "completed": completed,
                "success_rate_pct": round(success_rate, 2) if success_rate is not None else None,
                "failure_rate_pct": round(100 - success_rate, 2) if success_rate is not None else None,
                "total_attempts": sum(len(d.attempts) for d in deliveries),
                "avg_attempts_per_delivery": round(
                    sum(len(d.attempts) for d in deliveries) / total, 2
                ) if total > 0 else 0,
                "health_score": health_score,
            },
            "latency": latency,
            "error_breakdown": error_breakdown,
            "top_endpoints_by_volume": top_by_volume,
            "worst_endpoints_by_failure": worst_by_failure,
            "trend": trend,
        }

    # ── Per-endpoint detail ───────────────────────────────────────────

    def endpoint_report(self, endpoint_id: str) -> dict[str, Any] | None:
        """Analytics for a single endpoint."""
        ep = self._service.get_endpoint(endpoint_id)
        if ep is None:
            return None

        deliveries = self._service.store.list_deliveries(endpoint_id=endpoint_id, limit=10000)

        total = len(deliveries)
        statuses: Counter = Counter()
        durations: list[float] = []
        status_codes: Counter = Counter()
        error_messages: Counter = Counter()

        for d in deliveries:
            statuses[d.status.value] += 1
            for attempt in d.attempts:
                if attempt.duration_ms is not None:
                    durations.append(attempt.duration_ms)
                if attempt.response_status_code is not None:
                    status_codes[attempt.response_status_code] += 1
                if attempt.error_message:
                    error_messages[attempt.error_message[:200]] += 1

        successful = statuses.get(DeliveryStatus.SUCCESS.value, 0)
        failed = statuses.get(DeliveryStatus.FAILED.value, 0)
        dead_letter = statuses.get(DeliveryStatus.DEAD_LETTER.value, 0)
        abandoned = statuses.get(DeliveryStatus.ABANDONED.value, 0)
        completed = successful + failed + dead_letter + abandoned
        success_rate = (successful / completed * 100) if completed > 0 else None

        latency = {}
        if durations:
            latency = {
                "count": len(durations),
                "min_ms": round(min(durations), 2),
                "max_ms": round(max(durations), 2),
                "avg_ms": round(sum(durations) / len(durations), 2),
                "p50_ms": self._percentile(durations, 50),
                "p90_ms": self._percentile(durations, 90),
                "p95_ms": self._percentile(durations, 95),
                "p99_ms": self._percentile(durations, 99),
            }

        health_score = self._compute_health_score(
            success_rate=success_rate,
            avg_latency=latency.get("avg_ms"),
            p99_latency=latency.get("p99_ms"),
            dead_letter_rate=(dead_letter / total * 100) if total > 0 else 0,
            retry_rate=(statuses.get(DeliveryStatus.RETRYING.value, 0) / total * 100) if total > 0 else 0,
        )

        return {
            "endpoint_id": endpoint_id,
            "endpoint_name": ep.name,
            "summary": {
                "total_deliveries": total,
                "successful": successful,
                "failed": failed,
                "dead_letter": dead_letter,
                "abandoned": abandoned,
                "success_rate_pct": round(success_rate, 2) if success_rate is not None else None,
                "avg_attempts": round(sum(len(d.attempts) for d in deliveries) / total, 2) if total > 0 else 0,
                "health_score": health_score,
            },
            "latency": latency,
            "error_breakdown": {
                "by_status_code": [
                    {"status_code": sc, "count": cnt}
                    for sc, cnt in status_codes.most_common(20)
                ],
                "by_message": [
                    {"error": msg, "count": cnt}
                    for msg, cnt in error_messages.most_common(20)
                ],
            },
            "trend": self._trend(deliveries, granularity="hour"),
        }

    # ── Trend ──────────────────────────────────────────────────────────

    def _trend(
        self,
        deliveries: list[WebhookDelivery],
        granularity: str = "hour",
        buckets: int = 24,
    ) -> list[dict[str, Any]]:
        """Time-bucketed throughput and success rate.

        Args:
            deliveries: List of deliveries to bucket.
            granularity: ``minute``, ``hour``, or ``day``.
            buckets: Maximum number of time buckets to return.
        """
        now = datetime.now(timezone.utc)
        # Build empty buckets going backwards
        bucket_map: dict[str, dict[str, Any]] = {}

        for d in deliveries:
            dt = d.created_at
            bucket_dt = self._round_dt(dt, granularity)
            key = bucket_dt.isoformat()
            if key not in bucket_map:
                bucket_map[key] = {
                    "bucket": bucket_dt.isoformat(),
                    "total": 0,
                    "successful": 0,
                    "failed": 0,
                    "retrying": 0,
                    "dead_letter": 0,
                    "pending": 0,
                    "abandoned": 0,
                }
            b = bucket_map[key]
            b["total"] += 1
            b[d.status.value] += 1

        # Sort by bucket time and limit
        result = sorted(bucket_map.values(), key=lambda x: x["bucket"])[-buckets:]
        # Add success rate
        for b in result:
            completed = b["successful"] + b["failed"] + b["dead_letter"] + b["abandoned"]
            b["success_rate_pct"] = round(b["successful"] / completed * 100, 2) if completed > 0 else None
        return result

    # ── Health score ───────────────────────────────────────────────────

    @staticmethod
    def _compute_health_score(
        success_rate: float | None,
        avg_latency: float | None,
        p99_latency: float | None,
        dead_letter_rate: float,
        retry_rate: float,
    ) -> int:
        """Compute a 0-100 health score.

        Weighting:
            - Success rate reliability: 45 points
            - Average latency:          20 points
            - P99 latency:              15 points
            - Dead letter rate:         10 points
            - Retry rate:               10 points
        """
        score = 0.0

        # Reliability (45 pts)
        if success_rate is not None:
            score += (success_rate / 100) * 45

        # Average latency (20 pts) — under 500ms is full marks
        if avg_latency is not None:
            if avg_latency <= 500:
                score += 20
            elif avg_latency <= 1000:
                score += 15
            elif avg_latency <= 2000:
                score += 10
            elif avg_latency <= 5000:
                score += 5
            # > 5s gets 0

        # P99 latency (15 pts) — under 2s is full marks
        if p99_latency is not None:
            if p99_latency <= 2000:
                score += 15
            elif p99_latency <= 5000:
                score += 10
            elif p99_latency <= 10000:
                score += 5

        # Dead letter rate (10 pts) — 0% is full, 20%+ is 0
        dl_penalty = min(dead_letter_rate / 20.0, 1.0)
        score += (1 - dl_penalty) * 10

        # Retry rate (10 pts) — 0% is full, 30%+ is 0
        retry_penalty = min(retry_rate / 30.0, 1.0)
        score += (1 - retry_penalty) * 10

        return round(score)

    # ── Per-endpoint aggregator ───────────────────────────────────────

    @staticmethod
    def _per_endpoint_stats(
        deliveries: list[WebhookDelivery],
    ) -> dict[str, dict[str, Any]]:
        """Aggregate stats per endpoint."""
        result: dict[str, dict[str, Any]] = {}

        for d in deliveries:
            eid = d.endpoint_id
            if eid not in result:
                result[eid] = {
                    "endpoint_id": eid,
                    "total": 0,
                    "successful": 0,
                    "failed": 0,
                    "dead_letter": 0,
                    "pending": 0,
                    "retrying": 0,
                    "abandoned": 0,
                    "completed": 0,
                    "failure_rate": 0.0,
                    "success_rate": 0.0,
                    "avg_duration_ms": None,
                    "_durations": [],
                }
            ep_stats = result[eid]
            ep_stats["total"] += 1
            ep_stats[d.status.value] += 1

            for attempt in d.attempts:
                if attempt.duration_ms is not None:
                    ep_stats["_durations"].append(attempt.duration_ms)

        # Finalise
        for ep_stats in result.values():
            ep_stats["completed"] = (
                ep_stats["successful"]
                + ep_stats["failed"]
                + ep_stats["dead_letter"]
                + ep_stats["abandoned"]
            )
            if ep_stats["completed"] > 0:
                ep_stats["failure_rate"] = round(
                    (ep_stats["failed"] + ep_stats["dead_letter"] + ep_stats["abandoned"])
                    / ep_stats["completed"] * 100, 2
                )
                ep_stats["success_rate"] = round(
                    ep_stats["successful"] / ep_stats["completed"] * 100, 2
                )
            else:
                ep_stats["failure_rate"] = 0.0
                ep_stats["success_rate"] = 0.0
            if ep_stats["_durations"]:
                ep_stats["avg_duration_ms"] = round(
                    sum(ep_stats["_durations"]) / len(ep_stats["_durations"]), 2
                )
            del ep_stats["_durations"]

        return result

    # ── Funnel / retry analysis ──────────────────────────────────────

    def retry_analysis(self, limit: int = 10000) -> dict[str, Any]:
        """Analyse retry patterns: how often retries are needed and how often they succeed."""
        deliveries = self._all_deliveries(limit)

        total = len(deliveries)
        retried = sum(1 for d in deliveries if len(d.attempts) > 1)
        succeeded_after_retry = sum(
            1 for d in deliveries
            if len(d.attempts) > 1 and d.status == DeliveryStatus.SUCCESS
        )
        dead_lettered = sum(1 for d in deliveries if d.status == DeliveryStatus.DEAD_LETTER)

        # Distribution of attempt counts
        attempt_counts: Counter = Counter()
        for d in deliveries:
            attempt_counts[len(d.attempts)] += 1

        return {
            "total_deliveries": total,
            "deliveries_retried": retried,
            "retry_rate_pct": round(retried / total * 100, 2) if total > 0 else 0,
            "succeeded_after_retry": succeeded_after_retry,
            "retry_success_rate_pct": round(succeeded_after_retry / retried * 100, 2) if retried > 0 else None,
            "dead_lettered": dead_lettered,
            "dead_letter_rate_pct": round(dead_lettered / total * 100, 2) if total > 0 else 0,
            "attempt_distribution": [
                {"attempts": k, "count": v}
                for k, v in sorted(attempt_counts.items())
            ],
            "avg_attempts": round(sum(len(d.attempts) for d in deliveries) / total, 2) if total > 0 else 0,
        }
