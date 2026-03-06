"""
Cycle manager: orchestrates the round-robin scanning of NIFTY, BANKNIFTY, FINNIFTY.
Each cycle handles one index: fetch data, evaluate strategy, place trade if signal.
"""

import time
import threading
from typing import Any, Dict, List, Optional

from bot.api.groww_client import GrowwClientWrapper
from bot.config.settings import (
    AppConfig,
    LOT_SIZES,
    PAPER_INITIAL_CAPITAL,
    POLL_INTERVAL_SECONDS,
    SUPPORTED_INDICES,
)
from bot.core.data_layer import DataLayer
from bot.core.execution_engine import ExecutionEngine
from bot.core.risk_engine import RiskEngine
from bot.core.strategy_engine import StrategyEngine
from bot.logs.logger import log_error, log_info, log_warning
from bot.storage import database as db


class CycleManager:
    """
    Main orchestrator that runs the trading loop.
    Cycles through indices in round-robin fashion (5s per cycle).
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self.client = GrowwClientWrapper(config)
        self.data_layer = DataLayer(self.client)
        self.strategy = StrategyEngine()
        self.risk_engine = RiskEngine(engine_mode=config.mode)
        self.execution = ExecutionEngine(
            client=self.client,
            risk_engine=self.risk_engine,
            mode=config.mode,
            paper_capital=config.paper_capital,
        )

        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._current_index_pos = 0
        self._cycle_count = 0
        self._last_signals: Dict[str, Dict[str, Any]] = {}
        self._error_count = 0

    # ------------------------------------------------------------------
    # State
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def engine_state(self) -> str:
        if self._running:
            return "running"
        if self.risk_engine.is_idle:
            return "idle"
        return "stopped"

    @property
    def current_index(self) -> str:
        return SUPPORTED_INDICES[self._current_index_pos % len(SUPPORTED_INDICES)]

    # ------------------------------------------------------------------
    # Start / Stop
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Start the trading cycle in a background thread."""
        if self._running:
            log_warning("Cycle manager already running", "cycle_manager")
            return

        self._running = True
        self.config.engine_state = "running"
        self._thread = threading.Thread(target=self._run_loop, daemon=True)
        self._thread.start()
        log_info("Cycle manager started", "cycle_manager")
        db.insert_system_log("INFO", "cycle_manager", "Engine started")

    def stop(self) -> None:
        """Stop the trading cycle."""
        self._running = False
        self.config.engine_state = "stopped"
        log_info("Cycle manager stopped", "cycle_manager")
        db.insert_system_log("INFO", "cycle_manager", "Engine stopped")

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    def _run_loop(self) -> None:
        """Main trading loop - runs in background thread."""
        while self._running:
            try:
                self._execute_cycle()
            except Exception as exc:
                self._error_count += 1
                log_error(f"Cycle error: {exc}", "cycle_manager", exc)
                db.insert_error_log("cycle_manager", f"Cycle error: {exc}", str(exc))

                # If too many errors, slow down
                if self._error_count > 10:
                    log_warning("Too many errors, sleeping 30s", "cycle_manager")
                    time.sleep(30)
                    self._error_count = 0

            time.sleep(POLL_INTERVAL_SECONDS)

    def _execute_cycle(self) -> None:
        """Execute one trading cycle for the current index."""
        index_name = self.current_index
        self._cycle_count += 1

        log_info(
            f"Cycle {self._cycle_count}: Processing {index_name}",
            "cycle_manager",
        )

        # Step 1: Fetch index LTP
        index_ltp = self.data_layer.fetch_index_ltp()
        if index_name not in index_ltp:
            log_warning(f"No LTP available for {index_name}", "cycle_manager")
            self._advance_index()
            return

        # Step 2: Monitor existing open trades
        self._monitor_trades(index_name)

        # Step 3: Check if we can open new trades
        if not self.risk_engine.can_open_trade():
            log_info(f"Cannot open new trades (idle={self.risk_engine.is_idle})", "cycle_manager")
            self._advance_index()
            return

        if self.execution.is_in_cooldown():
            log_info("In cooldown period after trade close", "cycle_manager")
            self._advance_index()
            return

        # Step 4: Fetch candles and evaluate strategy
        candles = self.data_layer.fetch_index_candles(index_name)
        if not candles:
            log_warning(f"No candle data for {index_name}", "cycle_manager")
            self._advance_index()
            return

        signal_details = self.strategy.get_signal_details(index_name, candles)
        self._last_signals[index_name] = signal_details
        signal = signal_details.get("signal")

        if signal is None:
            self._advance_index()
            return

        log_info(f"Signal detected for {index_name}: {signal}", "cycle_manager")

        # Step 5: Fetch option chain and find tradeable strike
        expiry = self.data_layer.get_cached_expiry(index_name)
        if not expiry:
            expiry = self.data_layer.fetch_nearest_expiry(index_name)
        if not expiry:
            log_error(f"No expiry found for {index_name}", "cycle_manager")
            self._advance_index()
            return

        chain = self.data_layer.fetch_option_chain(index_name, expiry)
        if not chain:
            log_warning(f"No option chain for {index_name}", "cycle_manager")
            self._advance_index()
            return

        # Step 6: Get ATM strikes and their LTP
        strikes = self.data_layer.get_atm_strikes(index_name)
        if not strikes:
            self._advance_index()
            return

        option_type = signal  # CE or PE
        strikes_ltp = self.data_layer.get_option_ltp_for_strikes(
            index_name, strikes, expiry, option_type
        )

        if not strikes_ltp:
            log_warning(f"No option LTP data for {index_name} strikes", "cycle_manager")
            self._advance_index()
            return

        # Step 7: Check capital and find affordable strike
        available_capital = self.execution.get_available_capital()
        atm_strike = self.data_layer.detect_atm_strike(
            index_ltp[index_name],
            self.data_layer.client.config.tokens[0].role_id if self.data_layer.client.config.tokens else 50,
        )
        # Use actual strike step
        from bot.config.settings import STRIKE_STEP
        atm_strike = self.data_layer.detect_atm_strike(
            index_ltp[index_name], STRIKE_STEP[index_name]
        )

        affordable_strike = self.risk_engine.find_affordable_strike(
            strikes_ltp, available_capital, index_name, atm_strike, option_type
        )

        if affordable_strike is None:
            log_info(
                f"No affordable strike for {index_name}. Capital={available_capital:.2f}",
                "cycle_manager",
            )
            self._advance_index()
            return

        # Step 8: Determine quantity and prices
        option_ltp = strikes_ltp[affordable_strike]
        quantity = self.risk_engine.calculate_position_size(
            available_capital, option_ltp, index_name
        )

        if quantity <= 0:
            self._advance_index()
            return

        trading_symbol = self.data_layer.get_trading_symbol_for_strike(
            index_name, affordable_strike, option_type
        )
        if not trading_symbol:
            log_warning(f"No trading symbol for {index_name} {affordable_strike}{option_type}", "cycle_manager")
            self._advance_index()
            return

        stop_loss = self.risk_engine.calculate_stop_loss(option_ltp)
        target = self.risk_engine.calculate_target(option_ltp)
        candle_ts = str(candles[-1].get("timestamp", "")) if candles else ""

        # Step 9: Place trade
        log_info(
            f"Placing trade: {trading_symbol} qty={quantity} "
            f"entry={option_ltp:.2f} SL={stop_loss:.2f} TGT={target:.2f}",
            "cycle_manager",
        )

        trade_id = self.execution.open_trade(
            index_name=index_name,
            symbol=trading_symbol,
            entry_price=option_ltp,
            quantity=quantity,
            stop_loss=stop_loss,
            target=target,
            candle_timestamp=candle_ts,
        )

        if trade_id:
            log_info(f"Trade placed successfully: ID={trade_id}", "cycle_manager")
            self._error_count = 0
        else:
            log_warning(f"Trade placement returned no ID", "cycle_manager")

        # Step 10: Advance to next index
        self._advance_index()

    def _monitor_trades(self, index_name: str) -> None:
        """Monitor open trades for the current index and close if exit conditions met."""
        open_trades = db.get_open_trades(self.execution.mode)
        index_trades = [t for t in open_trades if t["index_name"] == index_name]

        if not index_trades:
            return

        # Build LTP map for open trade symbols
        symbols = [t["symbol"] for t in index_trades]
        ltp_map: Dict[str, float] = {}

        # Try batch LTP first
        if symbols:
            role_map = {"NIFTY": 2, "BANKNIFTY": 3, "FINNIFTY": 4}
            role_id = role_map.get(index_name, 2)
            batch_resp = self.client.get_option_ltp_batch(
                tuple(f"NSE_{s}" if not s.startswith("NSE_") else s for s in symbols),
                role_id=role_id,
            )
            for sym in symbols:
                key_with_prefix = f"NSE_{sym}" if not sym.startswith("NSE_") else sym
                if key_with_prefix in batch_resp:
                    ltp_map[sym] = batch_resp[key_with_prefix]
                elif sym in batch_resp:
                    ltp_map[sym] = batch_resp[sym]

            # Fallback to individual quotes for missing
            for sym in symbols:
                if sym not in ltp_map:
                    ltp_val = self.client.get_weekly_option_ltp(sym, role_id=role_id)
                    if ltp_val is not None:
                        ltp_map[sym] = ltp_val

        # Check exit conditions
        closed = self.execution.monitor_open_trades(ltp_map)
        for c in closed:
            log_info(
                f"Trade closed: {c['symbol']} @ {c['exit_price']:.2f} "
                f"P&L={c['pnl']:.2f} ({c['reason']})",
                "cycle_manager",
            )

    def _advance_index(self) -> None:
        """Move to the next index in the rotation."""
        self._current_index_pos = (self._current_index_pos + 1) % len(SUPPORTED_INDICES)

    # ------------------------------------------------------------------
    # Status / Info for UI
    # ------------------------------------------------------------------

    def get_status(self) -> Dict[str, Any]:
        """Get current engine status for UI display."""
        return {
            "engine_state": self.engine_state,
            "mode": self.execution.mode,
            "current_index": self.current_index,
            "cycle_count": self._cycle_count,
            "paper_capital": round(self.execution.paper_capital, 2),
            "consecutive_losses": self.risk_engine.consecutive_losses,
            "is_idle": self.risk_engine.is_idle,
            "active_tokens": self.client.get_active_token_count(),
            "connected": self.client.is_connected(),
            "error_count": self._error_count,
            "last_signals": dict(self._last_signals),
        }

    def get_index_ltp(self) -> Dict[str, float]:
        """Get cached index LTP data."""
        return self.data_layer.get_cached_index_ltp()

    def update_token(self, token: str, role_id: int = 1) -> bool:
        """
        Update a token at runtime (e.g., daily manual refresh).
        If only one token provided, use it for all roles.
        """
        success = self.client.reinitialize_token(role_id, token)
        if success:
            log_info(f"Token updated for role {role_id}", "cycle_manager")
            db.insert_system_log("INFO", "cycle_manager", f"Token refreshed for role {role_id}")
        return success

    def update_all_tokens(self, token: str) -> None:
        """Update all token roles with a single token (common use case)."""
        for t_cfg in self.config.tokens:
            self.client.reinitialize_token(t_cfg.role_id, token)
        log_info("All tokens updated", "cycle_manager")
        db.insert_system_log("INFO", "cycle_manager", "All tokens refreshed with new daily token")
