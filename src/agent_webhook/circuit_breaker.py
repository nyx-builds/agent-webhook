"""Circuit breaker pattern for webhook delivery — auto-stop failing endpoints.

States:
    CLOSED    — Normal operation. Deliveries proceed. Failures are counted.
    OPEN      — Circuit tripped. All deliveries are blocked for a cooldown period.
    HALF_OPEN — Testing recovery. A limited number of trial deliveries are allowed.
                If they succeed, the circuit closes. If they fail, it re-opens.

This prevents wasting resources on endpoints that are consistently failing and
gives downstream services time to recover.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from enum import Enum
from typing import Any


class CircuitState(str, Enum):
    """Circuit breaker states."""
    CLOSED = "closed"
    OPEN = "open"
    HALF_OPEN = "half_open"


class CircuitBreakerConfig:
    """Configuration for a circuit breaker instance.

    Attributes:
        failure_threshold: Number of consecutive failures before opening the circuit.
        recovery_timeout: Seconds to wait before transitioning from OPEN to HALF_OPEN.
        half_open_max_calls: Maximum number of trial deliveries in HALF_OPEN state.
        success_threshold: Consecutive successes in HALF_OPEN required to close the circuit.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        half_open_max_calls: int = 3,
        success_threshold: int = 2,
    ) -> None:
        if failure_threshold < 1:
            raise ValueError("failure_threshold must be >= 1")
        if recovery_timeout < 1.0:
            raise ValueError("recovery_timeout must be >= 1.0")
        if half_open_max_calls < 1:
            raise ValueError("half_open_max_calls must be >= 1")
        if success_threshold < 1:
            raise ValueError("success_threshold must be >= 1")

        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.half_open_max_calls = half_open_max_calls
        self.success_threshold = success_threshold

    def to_dict(self) -> dict[str, Any]:
        return {
            "failure_threshold": self.failure_threshold,
            "recovery_timeout": self.recovery_timeout,
            "half_open_max_calls": self.half_open_max_calls,
            "success_threshold": self.success_threshold,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> CircuitBreakerConfig:
        return cls(
            failure_threshold=data.get("failure_threshold", 5),
            recovery_timeout=data.get("recovery_timeout", 60.0),
            half_open_max_calls=data.get("half_open_max_calls", 3),
            success_threshold=data.get("success_threshold", 2),
        )


class CircuitBreaker:
    """Per-endpoint circuit breaker tracking failures and recovery.

    Thread-safe. Each endpoint ID gets its own breaker state.
    """

    def __init__(self, config: CircuitBreakerConfig | None = None) -> None:
        self._config = config or CircuitBreakerConfig()
        self._lock = threading.Lock()
        # Per-endpoint state: endpoint_id -> dict with state tracking
        self._states: dict[str, dict[str, Any]] = {}

    @property
    def config(self) -> CircuitBreakerConfig:
        return self._config

    def _get_or_init(self, endpoint_id: str) -> dict[str, Any]:
        """Get or initialize state for an endpoint."""
        if endpoint_id not in self._states:
            self._states[endpoint_id] = {
                "state": CircuitState.CLOSED,
                "consecutive_failures": 0,
                "consecutive_successes": 0,
                "half_open_calls": 0,
                "opened_at": None,        # datetime when circuit opened
                "last_failure_at": None,   # datetime of last failure
                "last_success_at": None,   # datetime of last success
                "total_trips": 0,          # how many times circuit has opened
            }
        return self._states[endpoint_id]

    def is_allowed(self, endpoint_id: str) -> bool:
        """Check if a delivery is allowed for this endpoint.

        In CLOSED: always allowed.
        In OPEN: blocked until recovery_timeout elapses, then transitions to HALF_OPEN.
        In HALF_OPEN: allowed only if under half_open_max_calls.
        """
        with self._lock:
            state = self._get_or_init(endpoint_id)
            current = state["state"]

            if current == CircuitState.CLOSED:
                return True

            if current == CircuitState.OPEN:
                # Check if recovery timeout has elapsed
                if state["opened_at"] is not None:
                    elapsed = (datetime.now(timezone.utc) - state["opened_at"]).total_seconds()
                    if elapsed >= self._config.recovery_timeout:
                        # Transition to HALF_OPEN
                        state["state"] = CircuitState.HALF_OPEN
                        state["half_open_calls"] = 0
                        state["consecutive_successes"] = 0
                        return True
                return False

            if current == CircuitState.HALF_OPEN:
                # Allow limited trial deliveries
                if state["half_open_calls"] < self._config.half_open_max_calls:
                    return True
                return False

            return True  # Shouldn't reach here

    def record_success(self, endpoint_id: str) -> None:
        """Record a successful delivery for this endpoint."""
        with self._lock:
            state = self._get_or_init(endpoint_id)
            now = datetime.now(timezone.utc)
            state["last_success_at"] = now

            current = state["state"]

            if current == CircuitState.HALF_OPEN:
                state["consecutive_successes"] += 1
                if state["consecutive_successes"] >= self._config.success_threshold:
                    # Enough successes — close the circuit
                    state["state"] = CircuitState.CLOSED
                    state["consecutive_failures"] = 0
                    state["consecutive_successes"] = 0
                    state["half_open_calls"] = 0
                    state["opened_at"] = None
            elif current == CircuitState.CLOSED:
                # Reset failure counter on success
                state["consecutive_failures"] = 0

    def record_failure(self, endpoint_id: str) -> None:
        """Record a failed delivery for this endpoint."""
        with self._lock:
            state = self._get_or_init(endpoint_id)
            now = datetime.now(timezone.utc)
            state["last_failure_at"] = now
            state["consecutive_failures"] += 1

            current = state["state"]

            if current == CircuitState.HALF_OPEN:
                # Failure during half-open → re-open the circuit
                state["state"] = CircuitState.OPEN
                state["opened_at"] = now
                state["total_trips"] += 1
                state["half_open_calls"] = 0
                state["consecutive_successes"] = 0
            elif current == CircuitState.CLOSED:
                if state["consecutive_failures"] >= self._config.failure_threshold:
                    # Trip the circuit
                    state["state"] = CircuitState.OPEN
                    state["opened_at"] = now
                    state["total_trips"] += 1

            # Track half-open call count
            if state["state"] == CircuitState.HALF_OPEN:
                state["half_open_calls"] += 1

    def record_half_open_attempt(self, endpoint_id: str) -> None:
        """Record that a half-open trial delivery was made."""
        with self._lock:
            state = self._get_or_init(endpoint_id)
            if state["state"] == CircuitState.HALF_OPEN:
                state["half_open_calls"] += 1

    def get_state(self, endpoint_id: str) -> dict[str, Any]:
        """Get the current circuit breaker state for an endpoint."""
        with self._lock:
            state = self._get_or_init(endpoint_id)
            current = state["state"]

            # Auto-transition OPEN → HALF_OPEN for reporting if timeout elapsed
            if current == CircuitState.OPEN and state["opened_at"] is not None:
                elapsed = (datetime.now(timezone.utc) - state["opened_at"]).total_seconds()
                if elapsed >= self._config.recovery_timeout:
                    state["state"] = CircuitState.HALF_OPEN
                    state["half_open_calls"] = 0
                    state["consecutive_successes"] = 0

            # Calculate time remaining if OPEN
            time_until_half_open = None
            if state["state"] == CircuitState.OPEN and state["opened_at"] is not None:
                elapsed = (datetime.now(timezone.utc) - state["opened_at"]).total_seconds()
                time_until_half_open = max(0, self._config.recovery_timeout - elapsed)

            return {
                "endpoint_id": endpoint_id,
                "state": state["state"].value,
                "consecutive_failures": state["consecutive_failures"],
                "consecutive_successes": state["consecutive_successes"],
                "total_trips": state["total_trips"],
                "opened_at": state["opened_at"],
                "last_failure_at": state["last_failure_at"],
                "last_success_at": state["last_success_at"],
                "time_until_half_open_seconds": round(time_until_half_open, 1) if time_until_half_open is not None else None,
                "half_open_calls": state["half_open_calls"],
                "config": self._config.to_dict(),
            }

    def get_all_states(self) -> list[dict[str, Any]]:
        """Get circuit breaker states for all tracked endpoints."""
        with self._lock:
            return [self.get_state(eid) for eid in list(self._states.keys())]

    def reset(self, endpoint_id: str) -> dict[str, Any]:
        """Reset the circuit breaker for an endpoint (force close)."""
        with self._lock:
            if endpoint_id in self._states:
                self._states[endpoint_id] = {
                    "state": CircuitState.CLOSED,
                    "consecutive_failures": 0,
                    "consecutive_successes": 0,
                    "half_open_calls": 0,
                    "opened_at": None,
                    "last_failure_at": None,
                    "last_success_at": None,
                    "total_trips": 0,
                }
            return self.get_state(endpoint_id)

    def reset_all(self) -> None:
        """Reset all circuit breakers."""
        with self._lock:
            self._states.clear()
