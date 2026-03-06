"""
Risk engine: position sizing, capital management, loss protection.
"""

from typing import Any, Dict, List, Optional

from bot.config.settings import (
    LOT_SIZES,
    MAX_CONSECUTIVE_LOSSES,
    MAX_OPEN_TRADES,
    MAX_RISK_PER_TRADE_PERCENT,
    STOP_LOSS_PERCENT,
    TARGET_PERCENT,
)
from bot.logs.logger import log_info, log_warning
from bot.storage import database as db


class RiskEngine:
    """
    Manages risk for the trading system.
    - Position sizing based on available capital
    - Stop loss and target calculation
    - Consecutive loss protection
    - Max open trades enforcement
    """

    def __init__(self, engine_mode: str = "paper") -> None:
        self.engine_mode = engine_mode
        self.consecutive_losses = 0
        self.is_idle = False
        self._last_trade_close_time: float = 0.0

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

    def can_open_trade(self) -> bool:
        """Check if a new trade can be opened."""
        if self.is_idle:
            return False
        open_count = db.count_open_trades(self.engine_mode)
        if open_count >= MAX_OPEN_TRADES:
            return False
        return True

    # ------------------------------------------------------------------
    # Position sizing
    # ------------------------------------------------------------------

    def calculate_position_size(
        self,
        available_capital: float,
        option_ltp: float,
        index_name: str,
    ) -> int:
        """
        Calculate how many lots to trade based on available capital and risk limits.
        Returns the number of shares (quantity), which must be a multiple of lot size.
        """
        if option_ltp <= 0 or available_capital <= 0:
            return 0

        lot_size = LOT_SIZES.get(index_name, 50)
        max_risk_amount = available_capital * (MAX_RISK_PER_TRADE_PERCENT / 100.0)

        # Cost for one lot
        cost_per_lot = option_ltp * lot_size

        if cost_per_lot <= 0:
            return 0

        # Start with 1 lot and check if affordable
        if cost_per_lot > available_capital:
            return 0

        # Check risk: max risk per trade
        max_lots_by_risk = int(max_risk_amount / (option_ltp * STOP_LOSS_PERCENT / 100.0 * lot_size))
        max_lots_by_capital = int(available_capital / cost_per_lot)

        lots = min(max(1, max_lots_by_risk), max_lots_by_capital)
        quantity = lots * lot_size

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
        """Calculate stop loss price."""
        return round(entry_price * (1 - sl_percent / 100.0), 2)

    @staticmethod
    def calculate_target(entry_price: float, target_percent: float = TARGET_PERCENT) -> float:
        """Calculate target price."""
        return round(entry_price * (1 + target_percent / 100.0), 2)

    # ------------------------------------------------------------------
    # Trade monitoring
    # ------------------------------------------------------------------

    def check_exit_conditions(
        self, trade: Dict[str, Any], current_ltp: float
    ) -> Optional[str]:
        """
        Check if a trade should be exited.
        Returns 'SL' for stop loss, 'TARGET' for target hit, None otherwise.
        """
        stop_loss = trade.get("stop_loss", 0)
        target = trade.get("target", 0)

        if stop_loss and current_ltp <= stop_loss:
            return "SL"
        if target and current_ltp >= target:
            return "TARGET"
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
