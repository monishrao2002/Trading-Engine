"""
API Health Monitor (v3): tracks API performance, failures, latency, and rate limits.
"""

import time
from typing import Any, Dict, List

from bot.config.settings import API_FAILURE_THRESHOLD
from bot.logs.logger import log_info, log_warning
from bot.storage import database as db


class APIHealthMonitor:
    """Monitors API health metrics for the engine guard system."""

    def __init__(self) -> None:
        self._total_calls: int = 0
        self._total_failures: int = 0
        self._consecutive_failures: int = 0
        self._latencies: List[float] = []
        self._last_failure_time: float = 0.0
        self._token_expiry_detected: bool = False
        self._rate_limit_warnings: int = 0

    def record_call(self, latency_ms: float, success: bool) -> None:
        """Record an API call result."""
        self._total_calls += 1
        self._latencies.append(latency_ms)
        # Keep only last 100 latencies
        if len(self._latencies) > 100:
            self._latencies = self._latencies[-100:]

        if success:
            self._consecutive_failures = 0
        else:
            self._total_failures += 1
            self._consecutive_failures += 1
            self._last_failure_time = time.time()

    def record_token_expiry(self) -> None:
        """Record that a token expiry was detected."""
        self._token_expiry_detected = True
        log_warning("[API_HEALTH] Token expiry detected", "api_health")

    def clear_token_expiry(self) -> None:
        """Clear token expiry flag after refresh."""
        self._token_expiry_detected = False

    def record_rate_limit(self) -> None:
        """Record a rate limit warning."""
        self._rate_limit_warnings += 1
        log_warning(f"[API_HEALTH] Rate limit warning #{self._rate_limit_warnings}", "api_health")

    @property
    def avg_latency_ms(self) -> float:
        """Average API latency in milliseconds."""
        if not self._latencies:
            return 0.0
        return sum(self._latencies) / len(self._latencies)

    @property
    def should_pause(self) -> bool:
        """Check if engine should be paused due to API issues."""
        return (
            self._consecutive_failures >= API_FAILURE_THRESHOLD
            or self._token_expiry_detected
        )

    @property
    def is_healthy(self) -> bool:
        """Check if API is healthy."""
        return self._consecutive_failures < 3 and not self._token_expiry_detected

    def get_status(self) -> Dict[str, Any]:
        """Return health status for dashboard display."""
        return {
            "total_calls": self._total_calls,
            "total_failures": self._total_failures,
            "consecutive_failures": self._consecutive_failures,
            "avg_latency_ms": round(self.avg_latency_ms, 1),
            "token_expiry": self._token_expiry_detected,
            "rate_limit_warnings": self._rate_limit_warnings,
            "is_healthy": self.is_healthy,
            "should_pause": self.should_pause,
        }

    def reset(self) -> None:
        """Reset all counters."""
        self._total_calls = 0
        self._total_failures = 0
        self._consecutive_failures = 0
        self._latencies.clear()
        self._token_expiry_detected = False
        self._rate_limit_warnings = 0
