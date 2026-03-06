"""
Flask dashboard for the Groww Multi-Index F&O AutoTrader.
Displays index LTP, trades, logs, engine state, and provides controls.
"""

import os
from typing import Any, Dict

from flask import Flask, jsonify, render_template, request

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
    app.secret_key = os.environ.get("FLASK_SECRET", "groww-autotrader-secret-key")

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

    return app


def set_cycle_manager(cm: Any) -> None:
    """Set the cycle manager reference for the dashboard."""
    global _cycle_manager
    _cycle_manager = cm
