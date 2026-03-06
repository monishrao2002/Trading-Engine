"""
Main entry point for the Groww Multi-Index F&O AutoTrader.
Initializes database, creates Flask app, and starts the server.
"""

import os
import sys
import threading

# Ensure the project root is on the path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from bot.config.settings import AppConfig, PAPER_INITIAL_CAPITAL
from bot.logs.logger import log_info
from bot.storage.database import init_db
from bot.ui.dashboard import create_app, set_cycle_manager


def main() -> None:
    """Start the AutoTrader application."""
    log_info("Initializing Groww Multi-Index F&O AutoTrader...", "main")

    # Step 1: Initialize database
    init_db()
    log_info("Database initialized", "main")

    # Step 2: Create Flask app (engine will be initialized when token is provided via UI)
    app = create_app()

    # Step 3: Check for token from environment variable
    token = os.environ.get("GROWW_API_TOKEN", "")
    if token:
        log_info("Found GROWW_API_TOKEN in environment, initializing engine...", "main")
        config = AppConfig(mode="paper", paper_capital=PAPER_INITIAL_CAPITAL)
        for role_id in range(1, 6):
            config.add_token(token, role_id)

        from bot.core.cycle_manager import CycleManager
        cm = CycleManager(config)
        set_cycle_manager(cm)
        log_info("Engine initialized with environment token", "main")

    # Step 4: Start Flask server
    host = os.environ.get("FLASK_HOST", "0.0.0.0")
    port = int(os.environ.get("FLASK_PORT", "5000"))
    debug = os.environ.get("FLASK_DEBUG", "false").lower() == "true"

    log_info(f"Starting dashboard on {host}:{port}", "main")
    app.run(host=host, port=port, debug=debug, threaded=True)


if __name__ == "__main__":
    main()
