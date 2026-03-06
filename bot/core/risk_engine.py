"""
Risk engine v3: dynamic SL (structure/ATR/trailing/time), position sizing (1% risk),
trade cooldown per index, daily limits.
"""

import time
from typing import Any, Dict, List, Optional

from bot.config.settings import (
    ATR_SL_MULTIPLIER,
    INDEX_COOLDOWN_SECONDS,
    LOT_SIZES,
    MAX_CONSECUTIVE_LOSSES,
    MAX_DAILY_LOSS_PERCENT,
    MAX_OPEN_TRADES,
    MAX_RISK_PER_TRADE_PERCENT,
    MAX_TRADES_PER_DAY,
    MAX_TRADES_PER_INDEX,
    PAPER_INITIAL_CAPITAL,
    RISK_PER_TRADE_PERCENT,
    STOP_LOSS_PERCENT,
    TARGET_PERCENT,
    TIME_SL_MINUTES,
    TRAILING_SL_TRIGGER_PERCENT,
)
from bot.logs.logger import log_info, log_warning
from bot.storage import database as db


class RiskEngine:
    """
    Manages risk for the trading system (v3 enhanced).
    - Dynamic stop-loss: structure SL, ATR SL, trailing SL, time SL
    - Position sizing: 1% risk per trade
    - Trade cooldown: 10 min between same index trades
    - Daily limits: max 10 trades/day, max 3% daily loss, max 2 per index
    - Consecutive loss protection
    - Max open trades enforcement
    """

    def __init__(self, engine_mode: str = "paper") -> None:
        self.engine_mode = engine_mode
        self.consecutive_losses = 0
        self.is_idle = False
        self._last_trade_close_time: float = 0.0
        # v3: Per-index cooldown tracking
        self._last_trade_time_per_index: Dict[str, float] = {}

    # ------------------------------------------------------------------
    # Consecutive loss tracking
    # ------------------------------------------------------------------

    def update_loss_tracker(self) -> None:
        """
        Check recent closed trades to count consecutive losses.
        If 3 consecutive losses, enter idle mode.
        """
        recent = db.get_recent_closed_trades(self.engine_mode, count=MAX_CONSECUTIVE_LOSSES)
        if len(recent) < MAX_CONSECUTIVE_LOSSES:
            self.consecutive_losses = 0
            self.is_idle = False
            return

        loss_count = 0
        for trade in recent:
            pnl = trade.get("pnl", 0)
            if pnl is not None and pnl < 0:
                loss_count += 1
            else:
                break

        self.consecutive_losses = loss_count
        if loss_count >= MAX_CONSECUTIVE_LOSSES:
            if not self.is_idle:
                log_warning(
                    f"3 consecutive losses detected. Entering idle mode.",
                    "risk_engine",
                )
                db.insert_system_log("WARNING", "risk_engine", "Entered idle mode: 3 consecutive losses")
            self.is_idle = True
        else:
            self.is_idle = False

    def reset_idle(self) -> None:
        """Manually reset idle mode."""
        self.is_idle = False
        self.consecutive_losses = 0
        log_info("Idle mode manually reset", "risk_engine")

    # ------------------------------------------------------------------
    # Trade validation
    # ------------------------------------------------------------------

    def can_open_trade(self, index_name: str = "") -> bool:
        """Check if a new trade can be opened (v3: includes daily limits and cooldown)."""
        if self.is_idle:
            return False
        open_count = db.count_open_trades(self.engine_mode)
        if open_count >= MAX_OPEN_TRADES:
            return False

        # v3: Check daily limits
        if not self.check_daily_limits(index_name):
            return False

        # v3: Check index cooldown
        if index_name and not self.check_cooldown(index_name):
            return False

        return True

    # ------------------------------------------------------------------
    # v3: Daily limits
    # ------------------------------------------------------------------

    def check_daily_limits(self, index_name: str = "") -> bool:
        """Check if daily trading limits are exceeded."""
        # Max trades per day
        trades_today = db.get_trades_today(self.engine_mode)
        if trades_today >= MAX_TRADES_PER_DAY:
            log_warning(f"Daily trade limit reached: {trades_today}/{MAX_TRADES_PER_DAY}", "risk_engine")
            return False

        # Max daily loss
        daily_pnl = db.get_daily_pnl(self.engine_mode)
        capital = PAPER_INITIAL_CAPITAL  # base reference
        max_loss = capital * (MAX_DAILY_LOSS_PERCENT / 100.0)
        if daily_pnl < -max_loss:
            log_warning(f"Daily loss limit reached: {daily_pnl:.2f} (max -{max_loss:.2f})", "risk_engine")
            return False

        # Max trades per index
        if index_name:
            index_trades = db.get_trades_today_for_index(index_name, self.engine_mode)
            if index_trades >= MAX_TRADES_PER_INDEX:
                log_warning(
                    f"Index trade limit for {index_name}: {index_trades}/{MAX_TRADES_PER_INDEX}",
                    "risk_engine",
                )
                return False

        return True

    # ------------------------------------------------------------------
    # v3: Cooldown per index
    # ------------------------------------------------------------------

    def check_cooldown(self, index_name: str) -> bool:
        """Check if cooldown period has elapsed for a specific index."""
        last_time = self._last_trade_time_per_index.get(index_name, 0.0)
        if last_time == 0:
            return True
        elapsed = time.time() - last_time
        if elapsed < INDEX_COOLDOWN_SECONDS:
            remaining = INDEX_COOLDOWN_SECONDS - elapsed
            log_info(
                f"Index cooldown for {index_name}: {remaining:.0f}s remaining",
                "risk_engine",
            )
            return False
        return True

    def record_trade_close(self, index_name: str) -> None:
        """Record trade close time for cooldown tracking."""
        self._last_trade_time_per_index[index_name] = time.time()

    # ------------------------------------------------------------------
    # Position sizing
    # ------------------------------------------------------------------

    def calculate_position_size(
        self,
        available_capital: float,
        option_ltp: float,
        index_name: str,
        sl_distance: float = 0.0,
    ) -> int:
        """
        v3: Dynamic position sizing.
        Formula: Risk per trade = 1% of capital, Qty = Risk / SL distance.
        Returns the number of shares (quantity), must be a multiple of lot size.
        """
        if option_ltp <= 0 or available_capital <= 0:
            return 0

        lot_size = LOT_SIZES.get(index_name, 50)

        # Cost for one lot
        cost_per_lot = option_ltp * lot_size
        if cost_per_lot <= 0 or cost_per_lot > available_capital:
            return 0

        # v3: Dynamic position sizing using 1% risk
        risk_amount = available_capital * (RISK_PER_TRADE_PERCENT / 100.0)

        if sl_distance > 0:
            # Qty = Risk / SL_distance
            qty_by_risk = int(risk_amount / sl_distance)
        else:
            # Fallback: use SL% for distance
            sl_dist = option_ltp * (STOP_LOSS_PERCENT / 100.0)
            qty_by_risk = int(risk_amount / sl_dist) if sl_dist > 0 else lot_size

        # Ensure multiple of lot size
        lots_by_risk = max(1, qty_by_risk // lot_size)
        lots_by_capital = int(available_capital / cost_per_lot)

        lots = min(lots_by_risk, lots_by_capital)
        quantity = lots * lot_size

        log_info(
            f"Position size: capital={available_capital:.0f}, risk={risk_amount:.0f}, "
            f"SL_dist={sl_distance:.2f}, qty={quantity}, lots={lots}",
            "risk_engine",
        )

        return quantity

    def find_affordable_strike(
        self,
        strikes_with_ltp: Dict[int, float],
        available_capital: float,
        index_name: str,
        preferred_strike: int,
        option_type: str = "CE",
    ) -> Optional[int]:
        """
        Find the best affordable strike closest to ATM.
        If preferred strike is too expensive, move to cheaper strikes.
        """
        lot_size = LOT_SIZES.get(index_name, 50)

        # Sort by distance from preferred strike
        sorted_strikes = sorted(
            strikes_with_ltp.keys(),
            key=lambda s: abs(s - preferred_strike),
        )

        for strike in sorted_strikes:
            ltp = strikes_with_ltp[strike]
            if ltp <= 0:
                continue
            cost = ltp * lot_size
            if cost <= available_capital:
                return strike

        return None

    # ------------------------------------------------------------------
    # Stop loss / target
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_stop_loss(entry_price: float, sl_percent: float = STOP_LOSS_PERCENT) -> float:
        """Calculate stop loss price (structure SL - default)."""
        return round(entry_price * (1 - sl_percent / 100.0), 2)

    @staticmethod
    def calculate_target(entry_price: float, target_percent: float = TARGET_PERCENT) -> float:
        """Calculate target price."""
        return round(entry_price * (1 + target_percent / 100.0), 2)

    # ------------------------------------------------------------------
    # v3: Dynamic stop-loss types
    # ------------------------------------------------------------------

    @staticmethod
    def calculate_atr_sl(entry_price: float, atr: float) -> float:
        """ATR-based stop loss: entry - (ATR * multiplier)."""
        return round(entry_price - atr * ATR_SL_MULTIPLIER, 2)

    @staticmethod
    def get_dynamic_sl(
        entry_price: float,
        candles: Optional[List[Dict[str, Any]]] = None,
        atr: float = 0.0,
    ) -> float:
        """
        Get the best dynamic stop-loss from multiple SL types.
        Uses the tightest (highest) SL to limit risk.
        """
        sl_values: List[float] = []

        # Structure SL (default percentage)
        structure_sl = round(entry_price * (1 - STOP_LOSS_PERCENT / 100.0), 2)
        sl_values.append(structure_sl)

        # ATR SL
        if atr > 0:
            atr_sl = round(entry_price - atr * ATR_SL_MULTIPLIER, 2)
            sl_values.append(atr_sl)

        # Structure SL from recent candle low
        if candles and len(candles) >= 3:
            recent_low = min(c["low"] for c in candles[-3:])
            if recent_low > 0 and recent_low < entry_price:
                sl_values.append(round(recent_low, 2))

        # Return the tightest (highest) SL to minimize risk
        if sl_values:
            return max(sl_values)
        return structure_sl

    @staticmethod
    def check_trailing_sl(
        entry_price: float,
        current_price: float,
        current_sl: float,
    ) -> float:
        """
        Trailing SL: if profit > 30%, move SL to cost (entry price).
        Returns the updated SL.
        """
        if entry_price <= 0:
            return current_sl
        profit_pct = ((current_price - entry_price) / entry_price) * 100.0
        if profit_pct >= TRAILING_SL_TRIGGER_PERCENT:
            # Move SL to cost
            new_sl = max(current_sl, entry_price)
            if new_sl > current_sl:
                log_info(
                    f"Trailing SL triggered: profit={profit_pct:.1f}%, SL moved to cost {new_sl:.2f}",
                    "risk_engine",
                )
            return new_sl
        return current_sl

    @staticmethod
    def check_time_sl(entry_time_str: str) -> bool:
        """
        Time SL: return True if trade has been open longer than TIME_SL_MINUTES.
        """
        from datetime import datetime, timezone
        try:
            entry_time = datetime.fromisoformat(entry_time_str.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            try:
                entry_time = datetime.strptime(entry_time_str, "%Y-%m-%d %H:%M:%S")
                entry_time = entry_time.replace(tzinfo=timezone.utc)
            except (ValueError, AttributeError):
                return False
        now = datetime.now(timezone.utc)
        elapsed_minutes = (now - entry_time).total_seconds() / 60.0
        return elapsed_minutes >= TIME_SL_MINUTES

    # ------------------------------------------------------------------
    # Trade monitoring
    # ------------------------------------------------------------------

    def check_exit_conditions(
        self, trade: Dict[str, Any], current_ltp: float
    ) -> Optional[str]:
        """
        Check if a trade should be exited (v3: enhanced with trailing and time SL).
        Returns 'SL', 'TRAILING_SL', 'TARGET', 'TIME_SL', or None.
        """
        entry_price = trade.get("entry_price", 0)
        stop_loss = trade.get("stop_loss", 0)
        target = trade.get("target", 0)

        # v3: Update trailing SL
        if stop_loss and entry_price and current_ltp > entry_price:
            new_sl = self.check_trailing_sl(entry_price, current_ltp, stop_loss)
            if new_sl > stop_loss:
                trade["stop_loss"] = new_sl
                stop_loss = new_sl

        # Regular SL check
        if stop_loss and current_ltp <= stop_loss:
            if stop_loss >= entry_price:
                return "TRAILING_SL"
            return "SL"

        # Target check
        if target and current_ltp >= target:
            return "TARGET"

        # v3: Time SL check
        timestamp = trade.get("timestamp", trade.get("entry_time", ""))
        if timestamp and self.check_time_sl(str(timestamp)):
            return "TIME_SL"

        return None

    def calculate_pnl(self, trade: Dict[str, Any], exit_price: float) -> float:
        """Calculate P&L for a trade."""
        entry_price = trade.get("entry_price", 0)
        quantity = trade.get("quantity", 0)
        direction = trade.get("direction", "BUY")

        if direction == "BUY":
            return round((exit_price - entry_price) * quantity, 2)
        else:
            return round((entry_price - exit_price) * quantity, 2)
