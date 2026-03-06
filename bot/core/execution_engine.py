"""
Execution engine v3: paper + live trading, live capital fetch,
position recovery on restart.
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
        # v3: Live capital tracking
        self._live_capital: float = 0.0
        self._live_margin_details: Dict[str, float] = {}

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
        if not self.risk_engine.can_open_trade(index_name):
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

        # v3: Track in open_positions for recovery
        if trade_id:
            db.upsert_open_position(
                symbol=symbol, index_name=index_name, entry_price=entry_price,
                quantity=quantity, stop_loss=stop_loss, target=target,
                engine_mode="paper", trade_id=trade_id,
            )

        log_trade(
            f"PAPER OPEN: {symbol} qty={quantity} entry={entry_price:.2f} "
            f"SL={stop_loss:.2f} target={target:.2f} capital={self.paper_capital:.2f}"
        )
        db.insert_system_log(
            "INFO", "execution",
            f"Paper trade opened: {symbol} qty={quantity} @ {entry_price:.2f}",
        )

        # Record equity curve point with v3 enhanced fields
        daily_pnl = db.get_daily_pnl("paper")
        total_pnl = db.get_total_pnl("paper")
        db.insert_equity_point(self.paper_capital, "paper", daily_pnl, total_pnl)

        return trade_id

    def paper_close_trade(self, trade: Dict[str, Any], exit_price: float, reason: str = "") -> float:
        """Close a paper trade and calculate P&L."""
        pnl = self.risk_engine.calculate_pnl(trade, exit_price)
        trade_id = trade["id"]
        index_name = trade.get("index_name", "")

        # Update capital
        proceeds = exit_price * trade["quantity"]
        self.paper_capital += proceeds

        db.close_trade(trade_id, exit_price, pnl)
        self._last_close_time = time.time()

        # v3: Remove from open_positions and record cooldown
        db.remove_open_position(trade_id)
        if index_name:
            self.risk_engine.record_trade_close(index_name)

        log_trade(
            f"PAPER CLOSE: {trade['symbol']} exit={exit_price:.2f} "
            f"P&L={pnl:.2f} reason={reason} capital={self.paper_capital:.2f}"
        )
        db.insert_system_log(
            "INFO", "execution",
            f"Paper trade closed: {trade['symbol']} P&L={pnl:.2f} ({reason})",
        )

        # Record equity curve point with v3 enhanced fields
        daily_pnl = db.get_daily_pnl("paper")
        total_pnl = db.get_total_pnl("paper")
        drawdown = max(0, 500000.0 - self.paper_capital) if self.paper_capital < 500000.0 else 0.0
        db.insert_equity_point(self.paper_capital, "paper", daily_pnl, total_pnl, drawdown)

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
        if not self.risk_engine.can_open_trade(index_name):
            log_info("Cannot open live trade: risk limits exceeded", "execution")
            return None

        if self.is_in_cooldown():
            log_info("Cannot open live trade: in cooldown period", "execution")
            return None

        # v3: Fetch live capital before each trade
        self.refresh_live_capital()

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
            db.insert_error_log(
                "execution", f"Order failed: {symbol}", "No response from API",
                symbol=symbol, error_type="ORDER_FAILED",
            )
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

        # v3: Track in open_positions for recovery
        if trade_id:
            db.upsert_open_position(
                symbol=symbol, index_name=index_name, entry_price=entry_price,
                quantity=quantity, stop_loss=stop_loss, target=target,
                engine_mode="live", trade_id=trade_id,
            )

        log_trade(
            f"LIVE OPEN: {symbol} qty={quantity} entry={entry_price:.2f} "
            f"SL={stop_loss:.2f} target={target:.2f} order_resp={order_resp}"
        )
        db.insert_system_log(
            "INFO", "execution",
            f"Live trade opened: {symbol} qty={quantity} @ {entry_price:.2f}",
        )

        # v3: Refresh capital after trade
        self.refresh_live_capital()

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
        index_name = trade.get("index_name", "")

        db.close_trade(trade_id, exit_price, pnl)
        self._last_close_time = time.time()

        # v3: Remove from open_positions and record cooldown
        db.remove_open_position(trade_id)
        if index_name:
            self.risk_engine.record_trade_close(index_name)

        log_trade(
            f"LIVE CLOSE: {trade['symbol']} exit={exit_price:.2f} "
            f"P&L={pnl:.2f} reason={reason}"
        )
        db.insert_system_log(
            "INFO", "execution",
            f"Live trade closed: {trade['symbol']} P&L={pnl:.2f} ({reason})",
        )

        self.risk_engine.update_loss_tracker()

        # v3: Refresh capital after trade close
        self.refresh_live_capital()

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
            if self._live_capital > 0:
                return self._live_capital
            self.refresh_live_capital()
            return self._live_capital

    # ------------------------------------------------------------------
    # v3: Live capital management
    # ------------------------------------------------------------------

    def refresh_live_capital(self) -> None:
        """Fetch live capital from Groww API (called before/after each live trade)."""
        try:
            margin = self.client.get_available_margin()
            fno = margin.get("fno_margin_details", {})
            self._live_capital = float(fno.get("option_buy_balance_available", 0.0))
            self._live_margin_details = {
                "clear_cash": float(fno.get("clear_cash", 0.0)),
                "net_margin_available": float(fno.get("net_margin_available", 0.0)),
                "option_buy_balance_available": self._live_capital,
                "used_margin": float(fno.get("used_margin", 0.0)),
            }
            log_info(
                f"Live capital refreshed: available={self._live_capital:.2f}",
                "execution",
            )
        except Exception as exc:
            log_error(f"Failed to fetch live capital: {exc}", "execution", exc)

    def get_capital_details(self) -> Dict[str, Any]:
        """Return capital details for dashboard display."""
        if self.mode == "paper":
            from bot.config.settings import PAPER_INITIAL_CAPITAL
            used = PAPER_INITIAL_CAPITAL - self.paper_capital
            return {
                "mode": "paper",
                "available": round(self.paper_capital, 2),
                "used_margin": round(max(0, used), 2),
                "remaining": round(self.paper_capital, 2),
                "initial": PAPER_INITIAL_CAPITAL,
            }
        else:
            return {
                "mode": "live",
                "available": round(self._live_capital, 2),
                "used_margin": round(self._live_margin_details.get("used_margin", 0.0), 2),
                "remaining": round(self._live_capital, 2),
                "clear_cash": round(self._live_margin_details.get("clear_cash", 0.0), 2),
                "net_margin_available": round(self._live_margin_details.get("net_margin_available", 0.0), 2),
            }

    # ------------------------------------------------------------------
    # v3: Position recovery
    # ------------------------------------------------------------------

    def recover_positions(self) -> List[Dict[str, Any]]:
        """
        Recover open positions on restart.
        Fetches saved positions from DB and syncs with current state.
        """
        positions = db.get_open_positions(self.mode)
        if not positions:
            log_info("No open positions to recover", "execution")
            return []

        log_info(f"Recovering {len(positions)} open positions", "execution")
        db.insert_system_log(
            "INFO", "execution",
            f"Position recovery: found {len(positions)} open positions",
        )

        # If live mode, also fetch from Groww API to validate
        if self.mode == "live":
            try:
                api_positions = self.client.get_positions()
                log_info(
                    f"API positions: {len(api_positions) if api_positions else 0}",
                    "execution",
                )
            except Exception as exc:
                log_error(f"Failed to fetch API positions for recovery: {exc}", "execution", exc)

        return positions

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
