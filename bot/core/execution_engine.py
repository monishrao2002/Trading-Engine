"""
Execution engine: paper trading and live trading order management.
Handles trade placement, monitoring, and exit logic.
"""

import time
from typing import Any, Dict, List, Optional

from bot.api.groww_client import GrowwClientWrapper
from bot.config.settings import LOT_SIZES, POST_CLOSE_WAIT_SECONDS
from bot.core.risk_engine import RiskEngine
from bot.logs.logger import log_error, log_info, log_trade
from bot.storage import database as db


class PaperTrade:
    """Represents a simulated trade in paper mode."""

    def __init__(
        self,
        trade_id: int,
        index_name: str,
        symbol: str,
        quantity: int,
        entry_price: float,
        stop_loss: float,
        target: float,
        direction: str = "BUY",
    ) -> None:
        self.trade_id = trade_id
        self.index_name = index_name
        self.symbol = symbol
        self.quantity = quantity
        self.entry_price = entry_price
        self.stop_loss = stop_loss
        self.target = target
        self.direction = direction


class ExecutionEngine:
    """
    Manages trade execution for both paper and live modes.
    """

    def __init__(
        self,
        client: GrowwClientWrapper,
        risk_engine: RiskEngine,
        mode: str = "paper",
        paper_capital: float = 500000.0,
    ) -> None:
        self.client = client
        self.risk_engine = risk_engine
        self.mode = mode
        self.paper_capital = paper_capital
        self._last_close_time: float = 0.0

    # ------------------------------------------------------------------
    # Trade cooldown
    # ------------------------------------------------------------------

    def is_in_cooldown(self) -> bool:
        """Check if we're in the post-close wait period."""
        if self._last_close_time == 0:
            return False
        elapsed = time.time() - self._last_close_time
        return elapsed < POST_CLOSE_WAIT_SECONDS

    # ------------------------------------------------------------------
    # Paper trading
    # ------------------------------------------------------------------

    def paper_open_trade(
        self,
        index_name: str,
        symbol: str,
        entry_price: float,
        quantity: int,
        stop_loss: float,
        target: float,
        candle_timestamp: str = "",
    ) -> Optional[int]:
        """Open a paper trade."""
        if not self.risk_engine.can_open_trade():
            log_info("Cannot open trade: risk limits exceeded", "execution")
            return None

        if self.is_in_cooldown():
            log_info("Cannot open trade: in cooldown period", "execution")
            return None

        # Check paper capital
        cost = entry_price * quantity
        if cost > self.paper_capital:
            log_info(
                f"Insufficient paper capital: need {cost:.2f}, have {self.paper_capital:.2f}",
                "execution",
            )
            return None

        # Deduct capital
        self.paper_capital -= cost

        # Store in DB
        trade_id = db.insert_trade(
            index_name=index_name,
            symbol=symbol,
            quantity=quantity,
            entry_price=entry_price,
            stop_loss=stop_loss,
            target=target,
            engine_mode="paper",
            candle_timestamp=candle_timestamp,
        )

        log_trade(
            f"PAPER OPEN: {symbol} qty={quantity} entry={entry_price:.2f} "
            f"SL={stop_loss:.2f} target={target:.2f} capital={self.paper_capital:.2f}"
        )
        db.insert_system_log(
            "INFO", "execution",
            f"Paper trade opened: {symbol} qty={quantity} @ {entry_price:.2f}",
        )

        # Record equity curve point
        db.insert_equity_point(self.paper_capital, "paper")

        return trade_id

    def paper_close_trade(self, trade: Dict[str, Any], exit_price: float, reason: str = "") -> float:
        """Close a paper trade and calculate P&L."""
        pnl = self.risk_engine.calculate_pnl(trade, exit_price)
        trade_id = trade["id"]

        # Update capital
        cost_basis = trade["entry_price"] * trade["quantity"]
        proceeds = exit_price * trade["quantity"]
        self.paper_capital += proceeds

        db.close_trade(trade_id, exit_price, pnl)
        self._last_close_time = time.time()

        log_trade(
            f"PAPER CLOSE: {trade['symbol']} exit={exit_price:.2f} "
            f"P&L={pnl:.2f} reason={reason} capital={self.paper_capital:.2f}"
        )
        db.insert_system_log(
            "INFO", "execution",
            f"Paper trade closed: {trade['symbol']} P&L={pnl:.2f} ({reason})",
        )

        # Record equity curve point
        db.insert_equity_point(self.paper_capital, "paper")

        # Update loss tracker
        self.risk_engine.update_loss_tracker()

        return pnl

    # ------------------------------------------------------------------
    # Live trading
    # ------------------------------------------------------------------

    def live_open_trade(
        self,
        index_name: str,
        symbol: str,
        entry_price: float,
        quantity: int,
        stop_loss: float,
        target: float,
        candle_timestamp: str = "",
    ) -> Optional[int]:
        """Open a live trade via Groww API."""
        if not self.risk_engine.can_open_trade():
            log_info("Cannot open live trade: risk limits exceeded", "execution")
            return None

        if self.is_in_cooldown():
            log_info("Cannot open live trade: in cooldown period", "execution")
            return None

        # Place order via API
        order_resp = self.client.place_order(
            trading_symbol=symbol,
            quantity=quantity,
            transaction_type="BUY",
            order_type="MARKET",
            product="MIS",
        )

        if not order_resp:
            log_error(f"Live order placement failed for {symbol}", "execution")
            db.insert_error_log("execution", f"Order failed: {symbol}", "No response from API")
            return None

        # Store in DB
        trade_id = db.insert_trade(
            index_name=index_name,
            symbol=symbol,
            quantity=quantity,
            entry_price=entry_price,
            stop_loss=stop_loss,
            target=target,
            engine_mode="live",
            candle_timestamp=candle_timestamp,
        )

        log_trade(
            f"LIVE OPEN: {symbol} qty={quantity} entry={entry_price:.2f} "
            f"SL={stop_loss:.2f} target={target:.2f} order_resp={order_resp}"
        )
        db.insert_system_log(
            "INFO", "execution",
            f"Live trade opened: {symbol} qty={quantity} @ {entry_price:.2f}",
        )

        return trade_id

    def live_close_trade(self, trade: Dict[str, Any], exit_price: float, reason: str = "") -> float:
        """Close a live trade via Groww API."""
        # Place sell order
        order_resp = self.client.place_order(
            trading_symbol=trade["symbol"],
            quantity=trade["quantity"],
            transaction_type="SELL",
            order_type="MARKET",
            product="MIS",
        )

        pnl = self.risk_engine.calculate_pnl(trade, exit_price)
        trade_id = trade["id"]

        db.close_trade(trade_id, exit_price, pnl)
        self._last_close_time = time.time()

        log_trade(
            f"LIVE CLOSE: {trade['symbol']} exit={exit_price:.2f} "
            f"P&L={pnl:.2f} reason={reason}"
        )
        db.insert_system_log(
            "INFO", "execution",
            f"Live trade closed: {trade['symbol']} P&L={pnl:.2f} ({reason})",
        )

        self.risk_engine.update_loss_tracker()

        return pnl

    # ------------------------------------------------------------------
    # Unified interface
    # ------------------------------------------------------------------

    def open_trade(
        self,
        index_name: str,
        symbol: str,
        entry_price: float,
        quantity: int,
        stop_loss: float,
        target: float,
        candle_timestamp: str = "",
    ) -> Optional[int]:
        """Open a trade (paper or live depending on mode)."""
        if self.mode == "paper":
            return self.paper_open_trade(
                index_name, symbol, entry_price, quantity,
                stop_loss, target, candle_timestamp,
            )
        else:
            return self.live_open_trade(
                index_name, symbol, entry_price, quantity,
                stop_loss, target, candle_timestamp,
            )

    def close_trade(self, trade: Dict[str, Any], exit_price: float, reason: str = "") -> float:
        """Close a trade (paper or live depending on mode)."""
        if self.mode == "paper":
            return self.paper_close_trade(trade, exit_price, reason)
        else:
            return self.live_close_trade(trade, exit_price, reason)

    def get_available_capital(self) -> float:
        """Get available capital for trading."""
        if self.mode == "paper":
            return self.paper_capital
        else:
            margin = self.client.get_available_margin()
            fno = margin.get("fno_margin_details", {})
            return float(fno.get("option_buy_balance_available", 0.0))

    # ------------------------------------------------------------------
    # Trade monitoring
    # ------------------------------------------------------------------

    def monitor_open_trades(self, ltp_map: Dict[str, float]) -> List[Dict[str, Any]]:
        """
        Check all open trades against current LTP and close if SL/target hit.
        Returns list of closed trade details.
        """
        open_trades = db.get_open_trades(self.mode)
        closed: List[Dict[str, Any]] = []

        for trade in open_trades:
            symbol = trade["symbol"]
            current_ltp = ltp_map.get(symbol)
            if current_ltp is None:
                continue

            exit_reason = self.risk_engine.check_exit_conditions(trade, current_ltp)
            if exit_reason:
                pnl = self.close_trade(trade, current_ltp, exit_reason)
                closed.append({
                    "trade_id": trade["id"],
                    "symbol": symbol,
                    "exit_price": current_ltp,
                    "pnl": pnl,
                    "reason": exit_reason,
                })

        return closed
