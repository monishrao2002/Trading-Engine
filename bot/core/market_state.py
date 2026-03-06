"""
Market state engine (v3): classifies current market conditions.
States: TREND_UP, TREND_DOWN, RANGE, VOLATILE, NEWS_RISK
Trading behavior adapts based on market state.
"""

from typing import Any, Dict, List, Optional

from bot.config.settings import RANGE_THRESHOLD, VOLATILITY_THRESHOLD
from bot.logs.logger import log_info
from bot.storage import database as db


class MarketStateEngine:
    """Classifies market state for each index based on candle data and news."""

    # Market states
    TREND_UP = "TREND_UP"
    TREND_DOWN = "TREND_DOWN"
    RANGE = "RANGE"
    VOLATILE = "VOLATILE"
    NEWS_RISK = "NEWS_RISK"

    def __init__(self) -> None:
        self._states: Dict[str, str] = {}
        self._news_risk: bool = False

    def classify(
        self,
        index_name: str,
        candles: List[Dict[str, Any]],
        ema_fast: Optional[float] = None,
        ema_slow: Optional[float] = None,
    ) -> str:
        """
        Classify market state based on candle data and indicators.

        Logic:
        - If news risk flag is set -> NEWS_RISK
        - Calculate intraday range % from candles
        - If range > VOLATILITY_THRESHOLD -> VOLATILE
        - If range < RANGE_THRESHOLD -> RANGE
        - If EMA fast > EMA slow -> TREND_UP
        - If EMA fast < EMA slow -> TREND_DOWN
        - Default -> RANGE
        """
        if self._news_risk:
            self._states[index_name] = self.NEWS_RISK
            return self.NEWS_RISK

        if not candles or len(candles) < 5:
            self._states[index_name] = self.RANGE
            return self.RANGE

        # Calculate intraday range
        day_high = max(c["high"] for c in candles[-20:])
        day_low = min(c["low"] for c in candles[-20:])
        mid_price = (day_high + day_low) / 2.0
        if mid_price <= 0:
            self._states[index_name] = self.RANGE
            return self.RANGE

        range_pct = ((day_high - day_low) / mid_price) * 100.0

        # Volatile check
        if range_pct > VOLATILITY_THRESHOLD:
            self._states[index_name] = self.VOLATILE
            log_info(f"[MARKET] {index_name} = VOLATILE (range={range_pct:.2f}%)", "market_state")
            return self.VOLATILE

        # Range check
        if range_pct < RANGE_THRESHOLD:
            self._states[index_name] = self.RANGE
            log_info(f"[MARKET] {index_name} = RANGE (range={range_pct:.2f}%)", "market_state")
            return self.RANGE

        # Trend check using EMA
        if ema_fast is not None and ema_slow is not None:
            if ema_fast > ema_slow:
                self._states[index_name] = self.TREND_UP
                log_info(f"[MARKET] {index_name} = TREND_UP (EMA {ema_fast:.2f} > {ema_slow:.2f})", "market_state")
                return self.TREND_UP
            elif ema_fast < ema_slow:
                self._states[index_name] = self.TREND_DOWN
                log_info(f"[MARKET] {index_name} = TREND_DOWN (EMA {ema_fast:.2f} < {ema_slow:.2f})", "market_state")
                return self.TREND_DOWN

        self._states[index_name] = self.RANGE
        return self.RANGE

    def set_news_risk(self, active: bool) -> None:
        """Set or clear the news risk flag (high-impact news detected)."""
        self._news_risk = active
        if active:
            log_info("[MARKET] NEWS_RISK activated - trading paused", "market_state")
            db.insert_system_log("WARNING", "market_state", "NEWS_RISK activated - high-impact news detected")

    def get_state(self, index_name: str) -> str:
        """Return current market state for an index."""
        return self._states.get(index_name, self.RANGE)

    def get_all_states(self) -> Dict[str, str]:
        """Return all market states."""
        return dict(self._states)

    def should_trade(self, index_name: str) -> bool:
        """Check if trading is allowed based on market state."""
        state = self.get_state(index_name)
        # Do not trade in RANGE or NEWS_RISK states
        return state not in (self.RANGE, self.NEWS_RISK)

    def get_allowed_option_type(self, index_name: str) -> Optional[str]:
        """
        Return the option type allowed by market state.
        TREND_UP -> CE only
        TREND_DOWN -> PE only
        VOLATILE -> both but reduced size (handled by risk engine)
        """
        state = self.get_state(index_name)
        if state == self.TREND_UP:
            return "CE"
        elif state == self.TREND_DOWN:
            return "PE"
        elif state == self.VOLATILE:
            return None  # both allowed, risk engine will reduce size
        return None

    @property
    def is_news_risk(self) -> bool:
        return self._news_risk
