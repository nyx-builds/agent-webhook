"""Prometheus metrics for agent-webhook — exposes metrics via /metrics endpoint."""

from __future__ import annotations

import threading
import time
from typing import Any


class MetricsCollector:
    """Thread-safe Prometheus-style metrics collector for webhook operations."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        # Counters
        self._deliveries_total: dict[str, int] = {
            "success": 0,
            "failed": 0,
            "abandoned": 0,
            "dead_letter": 0,
            "retried": 0,
        }
        self._deliveries_created: int = 0
        self._endpoints_total: int = 0
        self._subscriptions_total: int = 0
        self._relay_rules_total: int = 0
        self._incoming_total: int = 0
        self._transforms_total: int = 0

        # Histogram-like buckets for delivery duration (milliseconds)
        self._duration_buckets = [10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000, float("inf")]
        self._duration_counts: list[int] = [0] * len(self._duration_buckets)
        self._duration_sum: float = 0.0
        self._duration_count: int = 0

        # Rate limit tracking
        self._rate_limited_total: int = 0

        # Start time
        self._start_time: float = time.time()

    # ── Counter Increments ───────────────────────────────────────

    def inc_delivery_success(self, n: int = 1) -> None:
        with self._lock:
            self._deliveries_total["success"] += n

    def inc_delivery_failed(self, n: int = 1) -> None:
        with self._lock:
            self._deliveries_total["failed"] += n

    def inc_delivery_abandoned(self, n: int = 1) -> None:
        with self._lock:
            self._deliveries_total["abandoned"] += n

    def inc_delivery_dead_letter(self, n: int = 1) -> None:
        with self._lock:
            self._deliveries_total["dead_letter"] += n

    def inc_delivery_retried(self, n: int = 1) -> None:
        with self._lock:
            self._deliveries_total["retried"] += n

    def inc_deliveries_created(self, n: int = 1) -> None:
        with self._lock:
            self._deliveries_created += n

    def inc_rate_limited(self, n: int = 1) -> None:
        with self._lock:
            self._rate_limited_total += n

    # ── Gauge-like Updates ────────────────────────────────────────

    def set_endpoints_total(self, n: int) -> None:
        with self._lock:
            self._endpoints_total = n

    def set_subscriptions_total(self, n: int) -> None:
        with self._lock:
            self._subscriptions_total = n

    def set_relay_rules_total(self, n: int) -> None:
        with self._lock:
            self._relay_rules_total = n

    def set_incoming_total(self, n: int) -> None:
        with self._lock:
            self._incoming_total = n

    def set_transforms_total(self, n: int) -> None:
        with self._lock:
            self._transforms_total = n

    # ── Duration Tracking ────────────────────────────────────────

    def observe_duration(self, duration_ms: float) -> None:
        with self._lock:
            self._duration_sum += duration_ms
            self._duration_count += 1
            for i, bucket in enumerate(self._duration_buckets):
                if duration_ms <= bucket:
                    self._duration_counts[i] += 1

    # ── Export ────────────────────────────────────────────────────

    def generate_prometheus(self) -> str:
        """Generate Prometheus exposition format text."""
        with self._lock:
            lines: list[str] = []
            now = time.time()
            uptime = now - self._start_time

            lines.append("# HELP agent_webhook_up Whether the webhook service is running")
            lines.append("# TYPE agent_webhook_up gauge")
            lines.append("agent_webhook_up 1")

            lines.append("")
            lines.append("# HELP agent_webhook_uptime_seconds Service uptime in seconds")
            lines.append("# TYPE agent_webhook_uptime_seconds gauge")
            lines.append(f"agent_webhook_uptime_seconds {uptime:.0f}")

            # Delivery counters
            lines.append("")
            lines.append("# HELP agent_webhook_deliveries_total Total webhook deliveries by status")
            lines.append("# TYPE agent_webhook_deliveries_total counter")
            for status, count in self._deliveries_total.items():
                lines.append(f'agent_webhook_deliveries_total{{status="{status}"}} {count}')

            lines.append("")
            lines.append("# HELP agent_webhook_deliveries_created_total Total deliveries created")
            lines.append("# TYPE agent_webhook_deliveries_created_total counter")
            lines.append(f"agent_webhook_deliveries_created_total {self._deliveries_created}")

            lines.append("")
            lines.append("# HELP agent_webhook_rate_limited_total Total deliveries rejected by rate limiting")
            lines.append("# TYPE agent_webhook_rate_limited_total counter")
            lines.append(f"agent_webhook_rate_limited_total {self._rate_limited_total}")

            # Duration histogram
            lines.append("")
            lines.append("# HELP agent_webhook_delivery_duration_ms Webhook delivery duration in milliseconds")
            lines.append("# TYPE agent_webhook_delivery_duration_ms histogram")

            cumulative = 0
            for i, bucket in enumerate(self._duration_buckets):
                cumulative += self._duration_counts[i]
                if bucket == float("inf"):
                    le_str = "+Inf"
                else:
                    le_str = str(bucket)
                lines.append(f'agent_webhook_delivery_duration_ms_bucket{{le="{le_str}"}} {cumulative}')

            lines.append(f"agent_webhook_delivery_duration_ms_count {self._duration_count}")
            lines.append(f"agent_webhook_delivery_duration_ms_sum {self._duration_sum:.2f}")

            # Gauges
            lines.append("")
            lines.append("# HELP agent_webhook_endpoints_total Total registered endpoints")
            lines.append("# TYPE agent_webhook_endpoints_total gauge")
            lines.append(f"agent_webhook_endpoints_total {self._endpoints_total}")

            lines.append("")
            lines.append("# HELP agent_webhook_subscriptions_total Total event subscriptions")
            lines.append("# TYPE agent_webhook_subscriptions_total gauge")
            lines.append(f"agent_webhook_subscriptions_total {self._subscriptions_total}")

            lines.append("")
            lines.append("# HELP agent_webhook_relay_rules_total Total relay rules")
            lines.append("# TYPE agent_webhook_relay_rules_total gauge")
            lines.append(f"agent_webhook_relay_rules_total {self._relay_rules_total}")

            lines.append("")
            lines.append("# HELP agent_webhook_incoming_total Total incoming webhooks received")
            lines.append("# TYPE agent_webhook_incoming_total gauge")
            lines.append(f"agent_webhook_incoming_total {self._incoming_total}")

            lines.append("")
            lines.append("# HELP agent_webhook_transforms_total Total payload transforms")
            lines.append("# TYPE agent_webhook_transforms_total gauge")
            lines.append(f"agent_webhook_transforms_total {self._transforms_total}")

            return "\n".join(lines) + "\n"

    def get_json(self) -> dict[str, Any]:
        """Get metrics as a JSON-serializable dict."""
        with self._lock:
            return {
                "uptime_seconds": time.time() - self._start_time,
                "deliveries_total": dict(self._deliveries_total),
                "deliveries_created": self._deliveries_created,
                "rate_limited_total": self._rate_limited_total,
                "duration": {
                    "count": self._duration_count,
                    "sum_ms": round(self._duration_sum, 2),
                    "avg_ms": round(self._duration_sum / self._duration_count, 2) if self._duration_count > 0 else None,
                },
                "endpoints_total": self._endpoints_total,
                "subscriptions_total": self._subscriptions_total,
                "relay_rules_total": self._relay_rules_total,
                "incoming_total": self._incoming_total,
                "transforms_total": self._transforms_total,
            }


# Global metrics instance
_metrics: MetricsCollector | None = None


def get_metrics() -> MetricsCollector:
    """Get the global metrics collector."""
    global _metrics
    if _metrics is None:
        _metrics = MetricsCollector()
    return _metrics


def reset_metrics() -> None:
    """Reset the global metrics collector (for testing)."""
    global _metrics
    _metrics = MetricsCollector()
