"""Token bucket rate limiter for webhook endpoints."""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from .models import RateLimit, RateLimitPeriod


class RateLimiter:
    """Token bucket rate limiter. Tracks per-endpoint request rates."""

    def __init__(self) -> None:
        # endpoint_id -> list of timestamps
        self._requests: dict[str, list[float]] = defaultdict(list)

    def is_allowed(self, endpoint_id: str, rate_limit: RateLimit) -> bool:
        """Check if a request is allowed under the rate limit.

        Uses a sliding window algorithm.
        """
        now = time.monotonic()
        window_seconds = rate_limit.period_seconds
        window_start = now - window_seconds

        # Clean up old timestamps
        self._requests[endpoint_id] = [
            ts for ts in self._requests[endpoint_id] if ts > window_start
        ]

        current_count = len(self._requests[endpoint_id])

        # Check if we're within the base limit
        if current_count < rate_limit.max_requests:
            self._requests[endpoint_id].append(now)
            return True

        # Check burst allowance
        if rate_limit.burst > 0 and current_count < rate_limit.max_requests + rate_limit.burst:
            self._requests[endpoint_id].append(now)
            return True

        return False

    def get_wait_time(self, endpoint_id: str, rate_limit: RateLimit) -> float:
        """Get the time in seconds until the next request would be allowed."""
        now = time.monotonic()
        window_seconds = rate_limit.period_seconds
        window_start = now - window_seconds

        # Clean up old timestamps
        self._requests[endpoint_id] = [
            ts for ts in self._requests[endpoint_id] if ts > window_start
        ]

        current_count = len(self._requests[endpoint_id])
        max_allowed = rate_limit.max_requests + rate_limit.burst

        if current_count < max_allowed:
            return 0.0

        # Find the oldest timestamp in the window — when it expires, a slot opens
        oldest = min(self._requests[endpoint_id])
        wait = (oldest + window_seconds) - now
        return max(0.0, wait)

    def reset(self, endpoint_id: str | None = None) -> None:
        """Reset rate limit tracking for an endpoint or all endpoints."""
        if endpoint_id is None:
            self._requests.clear()
        else:
            self._requests.pop(endpoint_id, None)

    def get_status(self, endpoint_id: str, rate_limit: RateLimit) -> dict[str, Any]:
        """Get rate limit status for an endpoint."""
        now = time.monotonic()
        window_seconds = rate_limit.period_seconds
        window_start = now - window_seconds

        # Clean up old timestamps
        self._requests[endpoint_id] = [
            ts for ts in self._requests[endpoint_id] if ts > window_start
        ]

        current_count = len(self._requests[endpoint_id])
        remaining = max(0, rate_limit.max_requests + rate_limit.burst - current_count)
        reset_at = None
        if self._requests[endpoint_id]:
            oldest = min(self._requests[endpoint_id])
            reset_at = oldest + window_seconds

        return {
            "endpoint_id": endpoint_id,
            "limit": rate_limit.max_requests,
            "burst": rate_limit.burst,
            "period": rate_limit.period.value,
            "remaining": remaining,
            "current_count": current_count,
            "reset_at": reset_at,
        }
