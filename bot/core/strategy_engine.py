"""
Strategy engine: EMA crossover, VWAP, volume spike, break of structure.
Uses 15-minute candles to generate trade signals.
"""

from typing import Any, Dict, List, Optional, Tuple


class StrategySignal:
    """Represents a trade signal produced by the strategy engine."""

    def __init__(
        self,
        index_name: str,
        strike: int,
        option_type: str,
        trading_symbol: str,
        direction: str = "BUY",
        signal_strength: float = 0.0,
        reasons: Optional[List[str]] = None,
    ) -> None:
        self.index_name = index_name
        self.strike = strike
        self.option_type = option_type
        self.trading_symbol = trading_symbol
        self.direction = direction
        self.signal_strength = signal_strength
        self.reasons = reasons or []

    def __repr__(self) -> str:
        return (
            f"Signal({self.index_name} {self.strike}{self.option_type} "
            f"{self.direction} strength={self.signal_strength:.2f})"
        )


class StrategyEngine:
    """
    Implements indicator-based strategy for F&O trading.
    Indicators:
      - EMA crossover (fast/slow)
      - VWAP comparison
      - Volume spike detection
      - Break of structure (higher high / lower low)
    """

    def __init__(
        self,
        ema_fast: int = 9,
        ema_slow: int = 21,
        volume_spike_mult: float = 1.5,
    ) -> None:
        self.ema_fast = ema_fast
        self.ema_slow = ema_slow
        self.volume_spike_mult = volume_spike_mult
        # Track last signal candle to avoid duplicate trades in same candle
        self._last_signal_candle: Dict[str, str] = {}

    # ------------------------------------------------------------------
    # EMA calculation
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_ema(prices: List[float], period: int) -> List[float]:
        """Calculate Exponential Moving Average for a list of prices."""
        if len(prices) < period:
            return []
        ema_values: List[float] = []
        multiplier = 2.0 / (period + 1)

        # Seed with SMA
        sma = sum(prices[:period]) / period
        ema_values.append(sma)

        for i in range(period, len(prices)):
            ema_val = (prices[i] - ema_values[-1]) * multiplier + ema_values[-1]
            ema_values.append(ema_val)

        return ema_values

    # ------------------------------------------------------------------
    # VWAP calculation
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_vwap(candles: List[Dict[str, Any]]) -> float:
        """
        Calculate Volume Weighted Average Price from candle data.
        VWAP = sum(typical_price * volume) / sum(volume)
        """
        total_tp_vol = 0.0
        total_vol = 0
        for c in candles:
            typical_price = (c["high"] + c["low"] + c["close"]) / 3.0
            vol = c.get("volume", 0)
            total_tp_vol += typical_price * vol
            total_vol += vol
        if total_vol == 0:
            return 0.0
        return total_tp_vol / total_vol

    # ------------------------------------------------------------------
    # Volume spike detection
    # ------------------------------------------------------------------

    def detect_volume_spike(self, candles: List[Dict[str, Any]], lookback: int = 10) -> bool:
        """
        Check if the latest candle's volume is significantly above average.
        """
        if len(candles) < lookback + 1:
            return False
        recent_volumes = [c.get("volume", 0) for c in candles[-(lookback + 1):-1]]
        avg_volume = sum(recent_volumes) / len(recent_volumes) if recent_volumes else 0
        if avg_volume == 0:
            return False
        latest_volume = candles[-1].get("volume", 0)
        return latest_volume > avg_volume * self.volume_spike_mult

    # ------------------------------------------------------------------
    # Break of structure
    # ------------------------------------------------------------------

    @staticmethod
    def detect_break_of_structure(
        candles: List[Dict[str, Any]], lookback: int = 5
    ) -> Tuple[bool, bool]:
        """
        Detect break of structure:
        - Bullish BOS: latest high > highest high of previous N candles
        - Bearish BOS: latest low < lowest low of previous N candles
        Returns (bullish_bos, bearish_bos)
        """
        if len(candles) < lookback + 1:
            return False, False

        prev_candles = candles[-(lookback + 1):-1]
        latest = candles[-1]

        prev_highs = [c["high"] for c in prev_candles]
        prev_lows = [c["low"] for c in prev_candles]

        highest_high = max(prev_highs) if prev_highs else 0
        lowest_low = min(prev_lows) if prev_lows else float("inf")

        bullish = latest["high"] > highest_high
        bearish = latest["low"] < lowest_low

        return bullish, bearish

    # ------------------------------------------------------------------
    # Core strategy evaluation
    # ------------------------------------------------------------------

    def evaluate(
        self,
        index_name: str,
        candles: List[Dict[str, Any]],
    ) -> Optional[str]:
        """
        Evaluate the strategy on 15-min candles for a given index.
        Returns:
            "CE" for bullish signal (buy call)
            "PE" for bearish signal (buy put)
            None for no signal
        """
        if len(candles) < self.ema_slow + 2:
            return None

        # Check for duplicate candle signal
        latest_ts = str(candles[-1].get("timestamp", ""))
        if self._last_signal_candle.get(index_name) == latest_ts:
            return None

        closes = [c["close"] for c in candles]

        # 1. EMA crossover
        ema_fast_vals = self.calculate_ema(closes, self.ema_fast)
        ema_slow_vals = self.calculate_ema(closes, self.ema_slow)

        if not ema_fast_vals or not ema_slow_vals:
            return None

        # Align: ema_slow starts later, so offset ema_fast
        offset = self.ema_slow - self.ema_fast
        if offset < 0 or offset >= len(ema_fast_vals):
            return None

        ema_fast_aligned = ema_fast_vals[offset:]
        if len(ema_fast_aligned) < 2 or len(ema_slow_vals) < 2:
            return None

        # Current and previous EMA values
        curr_fast = ema_fast_aligned[-1]
        prev_fast = ema_fast_aligned[-2]
        curr_slow = ema_slow_vals[-1]
        prev_slow = ema_slow_vals[-2]

        bullish_cross = prev_fast <= prev_slow and curr_fast > curr_slow
        bearish_cross = prev_fast >= prev_slow and curr_fast < curr_slow

        # 2. VWAP
        vwap = self.calculate_vwap(candles)
        current_price = closes[-1]
        above_vwap = current_price > vwap
        below_vwap = current_price < vwap

        # 3. Volume spike
        vol_spike = self.detect_volume_spike(candles)

        # 4. Break of structure
        bullish_bos, bearish_bos = self.detect_break_of_structure(candles)

        # Combined signal evaluation
        bullish_score = 0
        bearish_score = 0
        conditions: List[str] = []

        if bullish_cross:
            bullish_score += 2
            conditions.append(f"EMA9={curr_fast:.2f}>EMA21={curr_slow:.2f} (bullish cross)")
        if bearish_cross:
            bearish_score += 2
            conditions.append(f"EMA9={curr_fast:.2f}<EMA21={curr_slow:.2f} (bearish cross)")

        if above_vwap:
            bullish_score += 1
            conditions.append(f"Price {current_price:.2f} > VWAP {vwap:.2f}")
        if below_vwap:
            bearish_score += 1
            conditions.append(f"Price {current_price:.2f} < VWAP {vwap:.2f}")

        if vol_spike:
            bullish_score += 1
            bearish_score += 1
            conditions.append("Volume spike detected")

        if bullish_bos:
            bullish_score += 1
            conditions.append("Bullish break of structure")
        if bearish_bos:
            bearish_score += 1
            conditions.append("Bearish break of structure")

        # Require at least 3 points for a signal (EMA cross + 1 confirmation)
        signal: Optional[str] = None
        if bullish_score >= 3 and bullish_score > bearish_score:
            signal = "CE"
        elif bearish_score >= 3 and bearish_score > bullish_score:
            signal = "PE"

        if signal:
            self._last_signal_candle[index_name] = latest_ts
            # v3: Enhanced signal logging with conditions
            from bot.logs.logger import log_info
            cond_str = ", ".join(conditions)
            if signal == "CE":
                log_info(
                    f"[SIGNAL] {index_name} CE breakout detected: "
                    f"EMA9={curr_fast:.2f}, EMA21={curr_slow:.2f}. Conditions: {cond_str}",
                    "strategy",
                )
            else:
                log_info(
                    f"[SIGNAL] {index_name} PE breakdown detected: "
                    f"EMA9={curr_fast:.2f}, EMA21={curr_slow:.2f}. Conditions: {cond_str}",
                    "strategy",
                )

        return signal

    # ------------------------------------------------------------------
    # ATR calculation (v3: for dynamic stop-loss)
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_atr(candles: List[Dict[str, Any]], period: int = 14) -> float:
        """Calculate Average True Range from candle data."""
        if len(candles) < period + 1:
            return 0.0

        true_ranges: List[float] = []
        for i in range(1, len(candles)):
            high = candles[i]["high"]
            low = candles[i]["low"]
            prev_close = candles[i - 1]["close"]
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            true_ranges.append(tr)

        if len(true_ranges) < period:
            return sum(true_ranges) / len(true_ranges) if true_ranges else 0.0

        return sum(true_ranges[-period:]) / period

    def get_signal_details(
        self,
        index_name: str,
        candles: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Return detailed signal analysis including all indicator values.
        Useful for UI display and logging.
        """
        result: Dict[str, Any] = {
            "index": index_name,
            "signal": None,
            "ema_fast": None,
            "ema_slow": None,
            "vwap": None,
            "volume_spike": False,
            "bullish_bos": False,
            "bearish_bos": False,
            "bullish_score": 0,
            "bearish_score": 0,
            "current_price": None,
            "candle_count": len(candles),
            "atr": 0.0,
        }

        if len(candles) < self.ema_slow + 2:
            return result

        closes = [c["close"] for c in candles]
        result["current_price"] = closes[-1]

        ema_fast_vals = self.calculate_ema(closes, self.ema_fast)
        ema_slow_vals = self.calculate_ema(closes, self.ema_slow)

        if ema_fast_vals:
            result["ema_fast"] = round(ema_fast_vals[-1], 2)
        if ema_slow_vals:
            result["ema_slow"] = round(ema_slow_vals[-1], 2)

        result["vwap"] = round(self.calculate_vwap(candles), 2)
        result["volume_spike"] = self.detect_volume_spike(candles)
        result["atr"] = round(self.calculate_atr(candles), 2)

        bullish_bos, bearish_bos = self.detect_break_of_structure(candles)
        result["bullish_bos"] = bullish_bos
        result["bearish_bos"] = bearish_bos

        signal = self.evaluate(index_name, candles)
        result["signal"] = signal

        return result
