from __future__ import annotations

import time
from collections.abc import Callable


class InstanceState:
    def __init__(
        self,
        instance_id: str,
        url: str,
        failure_threshold: int,
        shadow_health_successes: int,
        shadow_validations: int,
        shadow_min_seconds: float,
        clock: Callable[[], float] = time.monotonic,
    ):
        self.instance_id = instance_id
        self.url = url
        self.failure_threshold = failure_threshold
        self.shadow_health_successes_required = shadow_health_successes
        self.shadow_validations_required = shadow_validations
        self.shadow_min_seconds = shadow_min_seconds
        self.clock = clock
        self.state = "ACTIVE"
        self.failures = 0
        self.health_successes = 0
        self.shadow_validations = 0
        self.shadow_mismatches = 0
        self.shadow_entered_at: float | None = None
        self.failure_detected_at: float | None = None
        self.down_at: float | None = None
        self.last_error: str | None = None

    def observe_health(self, healthy: bool, error: str | None = None) -> tuple[str, str] | None:
        old_state = self.state
        now = self.clock()
        if not healthy:
            if self.failures == 0:
                self.failure_detected_at = now
            self.failures += 1
            self.health_successes = 0
            self.last_error = error
            if self.failures >= self.failure_threshold and self.state != "DOWN":
                self.state = "DOWN"
                self.down_at = now
                self.shadow_entered_at = None
                self.shadow_validations = 0
                self.shadow_mismatches = 0
        else:
            self.last_error = None
            self.failures = 0
            if self.state == "DOWN":
                self.state = "SHADOW"
                self.shadow_entered_at = now
                self.health_successes = 1
                self.shadow_validations = 0
                self.shadow_mismatches = 0
            elif self.state == "SHADOW":
                self.health_successes += 1
                self._maybe_promote()
            else:
                self.health_successes = 0
        return (old_state, self.state) if old_state != self.state else None

    def observe_shadow_validation(self, matches: bool) -> tuple[str, str] | None:
        old_state = self.state
        if self.state != "SHADOW":
            return None
        self.shadow_validations += 1
        if not matches:
            self.shadow_mismatches += 1
        self._maybe_promote()
        return (old_state, self.state) if old_state != self.state else None

    def _maybe_promote(self) -> None:
        if self.state != "SHADOW" or self.shadow_entered_at is None:
            return
        elapsed = self.clock() - self.shadow_entered_at
        if (
            self.health_successes >= self.shadow_health_successes_required
            and self.shadow_validations >= self.shadow_validations_required
            and self.shadow_mismatches == 0
            and elapsed >= self.shadow_min_seconds
        ):
            self.state = "ACTIVE"

    def snapshot(self) -> dict:
        detection_ms = None
        if self.failure_detected_at is not None and self.down_at is not None:
            detection_ms = round((self.down_at - self.failure_detected_at) * 1000, 1)
        shadow_duration_ms = None
        if self.shadow_entered_at is not None:
            shadow_duration_ms = round((self.clock() - self.shadow_entered_at) * 1000, 1)
        return {
            "url": self.url,
            "state": self.state,
            "failures": self.failures,
            "healthSuccesses": self.health_successes,
            "shadowValidations": self.shadow_validations,
            "shadowMismatches": self.shadow_mismatches,
            "detectionTimeMs": detection_ms,
            "shadowDurationMs": shadow_duration_ms,
            "lastError": self.last_error,
        }
