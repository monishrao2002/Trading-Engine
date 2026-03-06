"""
Engine Guard System (v3): global protection system.
Pauses engine if critical failures are detected.

Conditions for pause:
- API failure > 10
- Token expired
- Database failure
- Strategy crash
"""

import time
from typing import Any, Dict, Optional

from bot.core.api_health import APIHealthMonitor
from bot.logs.logger import log_error, log_info, log_warning
from bot.storage import database as db


class EngineGuard:
    """Global protection system that pauses the engine on critical failures."""

    def __init__(self, api_health: APIHealthMonitor) -> None:
        self.api_health = api_health
        self._paused: bool = False
        self._pause_reason: str = ""
        self._pause_time: float = 0.0
        self._db_healthy: bool = True
        self._strategy_crash_count: int = 0

    def check_health(self) -> bool:
        """
        Check all health conditions. Returns True if engine should continue.
        If False, engine should pause.
        """
        # Check API health
        if self.api_health.should_pause:
            self._pause("API failures exceeded threshold or token expired")
            return False

        # Check DB health
        if not self._db_healthy:
            self._pause("Database failure detected")
            return False

        # Check strategy crashes
        if self._strategy_crash_count > 5:
            self._pause("Too many strategy crashes")
            return False

        # If previously paused but conditions cleared, unpause
        if self._paused and self.api_health.is_healthy and self._db_healthy:
            self.resume()

        return not self._paused

    def _pause(self, reason: str) -> None:
        """Pause the engine with a reason."""
        if not self._paused:
            self._paused = True
            self._pause_reason = reason
            self._pause_time = time.time()
            log_warning(f"[ENGINE_GUARD] Engine paused: {reason}", "engine_guard")
            try:
                db.insert_system_log("CRITICAL", "engine_guard", f"Engine paused: {reason}")
                db.insert_error_log(
                    "engine_guard", f"Engine paused: {reason}",
                    error_type="ENGINE_GUARD",
                )
            except Exception:
                pass

    def resume(self) -> None:
        """Resume the engine."""
        if self._paused:
            self._paused = False
            log_info("[ENGINE_GUARD] Engine resumed", "engine_guard")
            try:
                db.insert_system_log("INFO", "engine_guard", "Engine resumed")
            except Exception:
                pass
            self._pause_reason = ""
            self._strategy_crash_count = 0

    def record_db_failure(self) -> None:
        """Record a database failure."""
        self._db_healthy = False

    def record_db_success(self) -> None:
        """Record a successful database operation."""
        self._db_healthy = True

    def record_strategy_crash(self) -> None:
        """Record a strategy crash."""
        self._strategy_crash_count += 1

    @property
    def is_paused(self) -> bool:
        return self._paused

    def get_status(self) -> Dict[str, Any]:
        """Return guard status for dashboard."""
        return {
            "paused": self._paused,
            "pause_reason": self._pause_reason,
            "pause_time": self._pause_time,
            "db_healthy": self._db_healthy,
            "strategy_crashes": self._strategy_crash_count,
            "api_health": self.api_health.get_status(),
        }
