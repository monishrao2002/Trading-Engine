"""
Centralized logging for the trading system.
Logs to both file and database via the storage module.
"""

import logging
import os
import sys
from datetime import datetime, timezone
from typing import Optional


LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "logs_output")
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "autotrader.log")
ERROR_LOG_FILE = os.path.join(LOG_DIR, "errors.log")


def _setup_logger(name: str, log_file: str, level: int = logging.DEBUG) -> logging.Logger:
    """Create a logger with file and console handlers."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(level)
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(log_file, mode="a")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


# Main application logger
app_logger = _setup_logger("autotrader", LOG_FILE)
# Separate error logger
error_logger = _setup_logger("autotrader.errors", ERROR_LOG_FILE, logging.ERROR)


def log_info(message: str, module: str = "system") -> None:
    """Log an informational message."""
    app_logger.info("[%s] %s", module, message)


def log_warning(message: str, module: str = "system") -> None:
    """Log a warning message."""
    app_logger.warning("[%s] %s", module, message)


def log_error(message: str, module: str = "system", exc: Optional[Exception] = None) -> None:
    """Log an error message (also writes to error log)."""
    msg = f"[{module}] {message}"
    if exc:
        msg += f" | Exception: {exc}"
    app_logger.error(msg)
    error_logger.error(msg)


def log_trade(message: str) -> None:
    """Log a trade-related message."""
    app_logger.info("[TRADE] %s", message)


def log_api(message: str) -> None:
    """Log an API-related message."""
    app_logger.debug("[API] %s", message)


def get_timestamp() -> str:
    """Return current UTC timestamp string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
