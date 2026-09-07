from __future__ import annotations

import threading
import time
from collections.abc import Callable


class CircuitOpenError(RuntimeError):
    pass


class CircuitBreaker:
    """Thread-safe CLOSED/OPEN/HALF_OPEN circuit breaker.

    Only one probe is allowed while HALF_OPEN, which keeps concurrent requests
    from stampeding the external provider during recovery.
    """

    def __init__(
        self,
        failure_threshold: int,
        recovery_timeout_seconds: float,
        clock: Callable[[], float] = time.monotonic,
        on_transition: Callable[[str, str], None] | None = None,
    ):
        self.failure_threshold = failure_threshold
        self.recovery_timeout_seconds = recovery_timeout_seconds
        self._clock = clock
        self._on_transition = on_transition
        self._lock = threading.Lock()
        self._state = "CLOSED"
        self._failures = 0
        self._opened_at = 0.0
        self._probe_in_flight = False

    def _transition(self, new_state: str) -> None:
        old_state = self._state
        if old_state == new_state:
            return
        self._state = new_state
        if self._on_transition:
            self._on_transition(old_state, new_state)

    def acquire_permission(self) -> None:
        with self._lock:
            if self._state == "OPEN":
                if self._clock() - self._opened_at < self.recovery_timeout_seconds:
                    raise CircuitOpenError("circuit is OPEN")
                self._transition("HALF_OPEN")
                self._probe_in_flight = True
                return
            if self._state == "HALF_OPEN":
                if self._probe_in_flight:
                    raise CircuitOpenError("HALF_OPEN probe already in progress")
                self._probe_in_flight = True

    def record_success(self) -> None:
        with self._lock:
            self._failures = 0
            self._probe_in_flight = False
            self._transition("CLOSED")

    def record_failure(self) -> None:
        with self._lock:
            self._failures += 1
            self._probe_in_flight = False
            if self._state == "HALF_OPEN" or self._failures >= self.failure_threshold:
                self._opened_at = self._clock()
                self._transition("OPEN")

    def snapshot(self) -> dict:
        with self._lock:
            return {
                "state": self._state,
                "failures": self._failures,
                "failureThreshold": self.failure_threshold,
                "recoveryTimeoutSeconds": self.recovery_timeout_seconds,
            }

    def reset(self) -> None:
        with self._lock:
            self._failures = 0
            self._opened_at = 0.0
            self._probe_in_flight = False
            self._transition("CLOSED")
