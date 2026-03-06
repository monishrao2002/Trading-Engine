"""
Flask dashboard v3 for the Groww Multi-Index F&O AutoTrader.
Displays index LTP, trades, logs, engine state, candles, market state,
capital details, API health, and provides controls.
"""

import os
from typing import Any, Dict

from flask import Flask, jsonify, render_template, request

from bot.config.settings import SUPPORTED_INDICES
from bot.storage import database as db

# Will be set by main.py after cycle_manager is created
_cycle_manager = None


def create_app() -> Flask:
    """Create and configure the Flask application."""
    template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
    static_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

    app = Flask(
        __name__,
        template_folder=template_dir,
        static_folder=static_dir,
    )
    app.secret_key = os.environ.get("FLASK_SECRET", "groww-autotrader-secret-key-v3")

    # ------------------------------------------------------------------
    # Pages
    # ------------------------------------------------------------------

    @app.route("/")
    def index():
        """Main dashboard page."""
        return render_template("dashboard.html")

    # ------------------------------------------------------------------
    # API endpoints
    # ------------------------------------------------------------------

    @app.route("/api/status")
    def api_status():
        """Get engine status."""
        if _cycle_manager is None:
            return jsonify({"engine_state": "not_initialized", "mode": "paper"})
        return jsonify(_cycle_manager.get_status())

    @app.route("/api/index_ltp")
    def api_index_ltp():
        """Get current index LTP values."""
        if _cycle_manager is None:
            return jsonify({})
        return jsonify(_cycle_manager.get_index_ltp())

    @app.route("/api/trades/open")
    def api_open_trades():
        """Get open trades."""
        mode = request.args.get("mode", "paper")
        trades = db.get_open_trades(mode)
        return jsonify(trades)

    @app.route("/api/trades/closed")
    def api_closed_trades():
        """Get closed trades."""
        mode = request.args.get("mode", "paper")
        limit = int(request.args.get("limit", "100"))
        trades = db.get_closed_trades(mode, limit)
        return jsonify(trades)

    @app.route("/api/equity_curve")
    def api_equity_curve():
        """Get equity curve data."""
        mode = request.args.get("mode", "paper")
        data = db.get_equity_curve(mode)
        return jsonify(data)

    @app.route("/api/logs/system")
    def api_system_logs():
        """Get system logs."""
        limit = int(request.args.get("limit", "200"))
        logs = db.get_system_logs(limit)
        return jsonify(logs)

    @app.route("/api/logs/errors")
    def api_error_logs():
        """Get error logs."""
        limit = int(request.args.get("limit", "200"))
        logs = db.get_error_logs(limit)
        return jsonify(logs)

    @app.route("/api/news")
    def api_news():
        """Get news feed."""
        limit = int(request.args.get("limit", "50"))
        news = db.get_news_feed(limit)
        return jsonify(news)

    # ------------------------------------------------------------------
    # Control endpoints
    # ------------------------------------------------------------------

    @app.route("/api/engine/start", methods=["POST"])
    def api_start_engine():
        """Start the trading engine."""
        if _cycle_manager is None:
            return jsonify({"error": "Engine not initialized. Add a token first."}), 400
        _cycle_manager.start()
        return jsonify({"status": "started"})

    @app.route("/api/engine/stop", methods=["POST"])
    def api_stop_engine():
        """Stop the trading engine."""
        if _cycle_manager is None:
            return jsonify({"error": "Engine not initialized"}), 400
        _cycle_manager.stop()
        return jsonify({"status": "stopped"})

    @app.route("/api/engine/reset_idle", methods=["POST"])
    def api_reset_idle():
        """Reset idle mode (after 3 consecutive losses)."""
        if _cycle_manager is None:
            return jsonify({"error": "Engine not initialized"}), 400
        _cycle_manager.risk_engine.reset_idle()
        return jsonify({"status": "idle_reset"})

    @app.route("/api/engine/mode", methods=["POST"])
    def api_set_mode():
        """Switch between paper and live mode."""
        data = request.get_json()
        if not data or "mode" not in data:
            return jsonify({"error": "mode required"}), 400
        mode = data["mode"]
        if mode not in ("paper", "live"):
            return jsonify({"error": "mode must be 'paper' or 'live'"}), 400
        if _cycle_manager is not None:
            if _cycle_manager.is_running:
                return jsonify({"error": "Stop engine before changing mode"}), 400
            _cycle_manager.config.mode = mode
            _cycle_manager.execution.mode = mode
            _cycle_manager.risk_engine.engine_mode = mode
        return jsonify({"status": f"mode set to {mode}"})

    @app.route("/api/token", methods=["POST"])
    def api_update_token():
        """
        Update the API token. User must manually enter a new token daily
        (Groww tokens expire at 6 AM).
        Accepts: {"token": "YOUR_TOKEN"}
        This updates all 5 token roles with the single token.
        """
        data = request.get_json()
        if not data or "token" not in data:
            return jsonify({"error": "token required"}), 400

        token = data["token"].strip()
        if not token:
            return jsonify({"error": "token cannot be empty"}), 400

        global _cycle_manager
        if _cycle_manager is None:
            # First time: initialize the cycle manager with this token
            from bot.config.settings import AppConfig, PAPER_INITIAL_CAPITAL
            config = AppConfig(mode="paper", paper_capital=PAPER_INITIAL_CAPITAL)
            for role_id in range(1, 6):
                config.add_token(token, role_id)

            from bot.core.cycle_manager import CycleManager
            _cycle_manager = CycleManager(config)
            db.insert_system_log("INFO", "dashboard", "Engine initialized with new token")
            return jsonify({"status": "initialized", "message": "Engine initialized. You can now start trading."})
        else:
            _cycle_manager.update_all_tokens(token)
            return jsonify({"status": "updated", "message": "All tokens refreshed successfully."})

    @app.route("/api/token/status")
    def api_token_status():
        """Check if a token has been configured."""
        if _cycle_manager is None:
            return jsonify({"configured": False, "active_count": 0})
        return jsonify({
            "configured": True,
            "active_count": _cycle_manager.client.get_active_token_count(),
            "connected": _cycle_manager.client.is_connected(),
        })

    # ------------------------------------------------------------------
    # v3: New API endpoints
    # ------------------------------------------------------------------

    @app.route("/api/candles/<index_name>")
    def api_candles(index_name: str):
        """Return recent fetched candle info and latest OHLC for an index."""
        if _cycle_manager is None:
            return jsonify({"error": "Engine not initialized"}), 400
        ohlc = _cycle_manager.get_latest_candle_ohlc(index_name)
        candle_info = _cycle_manager.get_candle_info()
        info = candle_info.get(index_name, {})
        return jsonify({
            "index": index_name,
            "count": info.get("count", 0),
            "latest_ts": info.get("latest_ts", ""),
            "source": info.get("source", ""),
            "ohlc": ohlc,
        })

    @app.route("/api/candles")
    def api_all_candles():
        """Return candle info for all indices."""
        if _cycle_manager is None:
            return jsonify({})
        result = {}
        candle_info = _cycle_manager.get_candle_info()
        for idx in SUPPORTED_INDICES:
            ohlc = _cycle_manager.get_latest_candle_ohlc(idx)
            info = candle_info.get(idx, {})
            result[idx] = {
                "count": info.get("count", 0),
                "latest_ts": info.get("latest_ts", ""),
                "source": info.get("source", ""),
                "ohlc": ohlc,
            }
        return jsonify(result)

    @app.route("/api/market_state")
    def api_market_state():
        """Return market state for each index."""
        if _cycle_manager is None:
            return jsonify({})
        return jsonify(_cycle_manager.get_market_states())

    @app.route("/api/capital")
    def api_capital():
        """Return available capital details (refreshed after each trade)."""
        if _cycle_manager is None:
            return jsonify({"mode": "paper", "available": 0, "used_margin": 0, "remaining": 0})
        return jsonify(_cycle_manager.get_capital_details())

    @app.route("/api/api_health")
    def api_api_health():
        """Return API latency, failures, token status."""
        if _cycle_manager is None:
            return jsonify({
                "total_calls": 0, "total_failures": 0, "consecutive_failures": 0,
                "avg_latency_ms": 0, "token_expiry": False, "is_healthy": False,
            })
        return jsonify(_cycle_manager.get_api_health())

    @app.route("/api/position_recovery")
    def api_position_recovery():
        """Return recovered/open positions on restart."""
        if _cycle_manager is None:
            return jsonify([])
        return jsonify(_cycle_manager.get_recovered_positions())

    @app.route("/api/performance_stats")
    def api_performance_stats():
        """Return latest performance stats."""
        mode = request.args.get("mode", "paper")
        stats = db.get_latest_performance_stats(mode)
        if not stats:
            return jsonify({
                "win_rate": 0, "profit_factor": 0, "avg_rr": 0,
                "max_drawdown": 0, "trades_today": 0, "daily_pnl": 0, "total_pnl": 0,
            })
        return jsonify(stats)

    # ------------------------------------------------------------------
    # Manual paper trade endpoint
    # ------------------------------------------------------------------

    @app.route("/api/manual_trade", methods=["POST"])
    def api_manual_trade():
        """
        Open a manual paper trade for testing.
        Fetches real LTP and candle data for the specified option symbol.
        Accepts: {"index": "NIFTY", "strike": 24500, "option_type": "CE", "quantity": 65}
        """
        if _cycle_manager is None:
            return jsonify({"error": "Engine not initialized. Add a token first."}), 400

        data = request.get_json()
        if not data:
            return jsonify({"error": "JSON body required"}), 400

        index_name = data.get("index", "NIFTY")
        strike = int(data.get("strike", 0))
        option_type = data.get("option_type", "CE")
        quantity = int(data.get("quantity", 0))

        if not strike:
            return jsonify({"error": "strike is required"}), 400

        from bot.config.settings import LOT_SIZES, STOP_LOSS_PERCENT, TARGET_PERCENT
        lot_size = LOT_SIZES.get(index_name, 50)
        if quantity <= 0:
            quantity = lot_size

        # Step 1: Get the trading symbol for this strike
        dl = _cycle_manager.data_layer
        expiry = dl.get_cached_expiry(index_name)
        if not expiry:
            expiry = dl.fetch_nearest_expiry(index_name)
        if not expiry:
            return jsonify({"error": f"No expiry found for {index_name}"}), 400

        trading_symbol = dl.get_trading_symbol_for_strike(
            index_name, strike, option_type
        )
        if not trading_symbol:
            # Try fetching option chain first
            dl.fetch_option_chain(index_name, expiry)
            trading_symbol = dl.get_trading_symbol_for_strike(
                index_name, strike, option_type
            )
        if not trading_symbol:
            return jsonify({"error": f"No symbol found for {index_name} {strike}{option_type}"}), 400

        # Step 2: Fetch LTP for this option
        role_map = {"NIFTY": 2, "BANKNIFTY": 3, "FINNIFTY": 4}
        role_id = role_map.get(index_name, 2)
        option_ltp = _cycle_manager.client.get_weekly_option_ltp(
            trading_symbol, role_id=role_id
        )
        if option_ltp is None or option_ltp <= 0:
            # Try batch LTP
            sym_key = f"NSE_{trading_symbol}" if not trading_symbol.startswith("NSE_") else trading_symbol
            batch = _cycle_manager.client.get_option_ltp_batch(
                (sym_key,), role_id=role_id
            )
            option_ltp = batch.get(sym_key, batch.get(trading_symbol, 0.0))
        if not option_ltp or option_ltp <= 0:
            return jsonify({"error": f"Could not fetch LTP for {trading_symbol}"}), 400

        # Step 3: Fetch option candles (15min)
        candles = dl.fetch_option_candles(trading_symbol)
        candle_count = len(candles)
        latest_candle = candles[-1] if candles else None

        # Step 4: Calculate SL and target
        stop_loss = round(option_ltp * (1 - STOP_LOSS_PERCENT / 100.0), 2)
        target = round(option_ltp * (1 + TARGET_PERCENT / 100.0), 2)

        # Step 5: Place the paper trade
        trade_id = _cycle_manager.execution.open_trade(
            index_name=index_name,
            symbol=trading_symbol,
            entry_price=option_ltp,
            quantity=quantity,
            stop_loss=stop_loss,
            target=target,
            candle_timestamp=str(latest_candle.get("timestamp", "")) if latest_candle else "",
        )

        if not trade_id:
            return jsonify({"error": "Trade placement failed (risk limits or insufficient capital)"}), 400

        # Step 6: Update dashboard state
        dl.set_selected_atm(index_name, strike)
        dl.set_selected_symbol(index_name, trading_symbol)

        result = {
            "status": "trade_opened",
            "trade_id": trade_id,
            "symbol": trading_symbol,
            "ltp": option_ltp,
            "quantity": quantity,
            "entry_price": option_ltp,
            "stop_loss": stop_loss,
            "target": target,
            "candle_count": candle_count,
            "latest_candle": latest_candle,
        }
        return jsonify(result)

    return app


def set_cycle_manager(cm: Any) -> None:
    """Set the cycle manager reference for the dashboard."""
    global _cycle_manager
    _cycle_manager = cm
