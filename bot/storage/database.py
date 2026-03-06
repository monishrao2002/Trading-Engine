"""
SQLite database layer for persistent storage.
Tables: trades, equity_curve, system_logs, error_logs, token_status, news_feed
"""

import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bot.config.settings import DB_PATH


_local = threading.local()


def _get_connection() -> sqlite3.Connection:
    """Get a thread-local database connection."""
    if not hasattr(_local, "conn") or _local.conn is None:
        _local.conn = sqlite3.connect(DB_PATH, check_same_thread=False)
        _local.conn.row_factory = sqlite3.Row
        _local.conn.execute("PRAGMA journal_mode=WAL")
        _local.conn.execute("PRAGMA foreign_keys=ON")
    return _local.conn


def init_db() -> None:
    """Create all required tables if they do not exist."""
    conn = _get_connection()
    cursor = conn.cursor()

    cursor.executescript("""
        CREATE TABLE IF NOT EXISTS trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            index_name TEXT NOT NULL,
            symbol TEXT NOT NULL,
            direction TEXT NOT NULL DEFAULT 'BUY',
            quantity INTEGER NOT NULL,
            entry_price REAL NOT NULL,
            stop_loss REAL,
            target REAL,
            exit_price REAL,
            pnl REAL,
            status TEXT NOT NULL DEFAULT 'OPEN',
            engine_mode TEXT NOT NULL DEFAULT 'paper',
            candle_timestamp TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            closed_at TEXT
        );

        CREATE TABLE IF NOT EXISTS equity_curve (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            capital REAL NOT NULL,
            engine_mode TEXT NOT NULL DEFAULT 'paper',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS system_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            level TEXT NOT NULL,
            module TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS error_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            module TEXT NOT NULL,
            message TEXT NOT NULL,
            exception TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS token_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            role_id INTEGER NOT NULL UNIQUE,
            token_hash TEXT NOT NULL,
            is_active INTEGER NOT NULL DEFAULT 1,
            failure_count INTEGER NOT NULL DEFAULT 0,
            last_used TEXT,
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS news_feed (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            headline TEXT NOT NULL,
            source TEXT,
            keywords_matched TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
    """)
    conn.commit()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------

def insert_trade(
    index_name: str,
    symbol: str,
    quantity: int,
    entry_price: float,
    stop_loss: float,
    target: float,
    engine_mode: str = "paper",
    candle_timestamp: str = "",
    direction: str = "BUY",
) -> int:
    """Insert a new trade and return its ID."""
    conn = _get_connection()
    cursor = conn.cursor()
    ts = _now()
    cursor.execute(
        """INSERT INTO trades
           (timestamp, index_name, symbol, direction, quantity, entry_price,
            stop_loss, target, status, engine_mode, candle_timestamp)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'OPEN', ?, ?)""",
        (ts, index_name, symbol, direction, quantity, entry_price,
         stop_loss, target, engine_mode, candle_timestamp),
    )
    conn.commit()
    return cursor.lastrowid  # type: ignore[return-value]


def close_trade(trade_id: int, exit_price: float, pnl: float) -> None:
    """Close a trade with exit price and P&L."""
    conn = _get_connection()
    ts = _now()
    conn.execute(
        """UPDATE trades SET exit_price=?, pnl=?, status='CLOSED', closed_at=?
           WHERE id=?""",
        (exit_price, pnl, ts, trade_id),
    )
    conn.commit()


def get_open_trades(engine_mode: str = "paper") -> List[Dict[str, Any]]:
    """Return all open trades for the given engine mode."""
    conn = _get_connection()
    rows = conn.execute(
        "SELECT * FROM trades WHERE status='OPEN' AND engine_mode=? ORDER BY id",
        (engine_mode,),
    ).fetchall()
    return [dict(r) for r in rows]


def get_closed_trades(engine_mode: str = "paper", limit: int = 100) -> List[Dict[str, Any]]:
    """Return closed trades, most recent first."""
    conn = _get_connection()
    rows = conn.execute(
        "SELECT * FROM trades WHERE status='CLOSED' AND engine_mode=? ORDER BY id DESC LIMIT ?",
        (engine_mode, limit),
    ).fetchall()
    return [dict(r) for r in rows]


def get_recent_closed_trades(engine_mode: str = "paper", count: int = 3) -> List[Dict[str, Any]]:
    """Return the N most recently closed trades."""
    conn = _get_connection()
    rows = conn.execute(
        "SELECT * FROM trades WHERE status='CLOSED' AND engine_mode=? ORDER BY id DESC LIMIT ?",
        (engine_mode, count),
    ).fetchall()
    return [dict(r) for r in rows]


def count_open_trades(engine_mode: str = "paper") -> int:
    """Count how many trades are currently open."""
    conn = _get_connection()
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM trades WHERE status='OPEN' AND engine_mode=?",
        (engine_mode,),
    ).fetchone()
    return row["cnt"] if row else 0


# ---------------------------------------------------------------------------
# Equity curve
# ---------------------------------------------------------------------------

def insert_equity_point(capital: float, engine_mode: str = "paper") -> None:
    """Record a point on the equity curve."""
    conn = _get_connection()
    conn.execute(
        "INSERT INTO equity_curve (timestamp, capital, engine_mode) VALUES (?, ?, ?)",
        (_now(), capital, engine_mode),
    )
    conn.commit()


def get_equity_curve(engine_mode: str = "paper", limit: int = 500) -> List[Dict[str, Any]]:
    """Return equity curve data."""
    conn = _get_connection()
    rows = conn.execute(
        "SELECT * FROM equity_curve WHERE engine_mode=? ORDER BY id DESC LIMIT ?",
        (engine_mode, limit),
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# System logs
# ---------------------------------------------------------------------------

def insert_system_log(level: str, module: str, message: str) -> None:
    """Insert a system log entry."""
    conn = _get_connection()
    conn.execute(
        "INSERT INTO system_logs (timestamp, level, module, message) VALUES (?, ?, ?, ?)",
        (_now(), level, module, message),
    )
    conn.commit()


def get_system_logs(limit: int = 200) -> List[Dict[str, Any]]:
    """Return recent system logs."""
    conn = _get_connection()
    rows = conn.execute(
        "SELECT * FROM system_logs ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Error logs
# ---------------------------------------------------------------------------

def insert_error_log(module: str, message: str, exception: str = "") -> None:
    """Insert an error log entry."""
    conn = _get_connection()
    conn.execute(
        "INSERT INTO error_logs (timestamp, module, message, exception) VALUES (?, ?, ?, ?)",
        (_now(), module, message, exception),
    )
    conn.commit()


def get_error_logs(limit: int = 200) -> List[Dict[str, Any]]:
    """Return recent error logs."""
    conn = _get_connection()
    rows = conn.execute(
        "SELECT * FROM error_logs ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Token status
# ---------------------------------------------------------------------------

def upsert_token_status(role_id: int, token_hash: str, is_active: bool, failure_count: int) -> None:
    """Insert or update token status."""
    conn = _get_connection()
    ts = _now()
    conn.execute(
        """INSERT INTO token_status (role_id, token_hash, is_active, failure_count, last_used, updated_at)
           VALUES (?, ?, ?, ?, ?, ?)
           ON CONFLICT(role_id) DO UPDATE SET
             is_active=excluded.is_active,
             failure_count=excluded.failure_count,
             last_used=excluded.last_used,
             updated_at=excluded.updated_at""",
        (role_id, token_hash, int(is_active), failure_count, ts, ts),
    )
    conn.commit()


# ---------------------------------------------------------------------------
# News feed
# ---------------------------------------------------------------------------

def insert_news(headline: str, source: str = "", keywords_matched: str = "") -> None:
    """Insert a news item."""
    conn = _get_connection()
    conn.execute(
        "INSERT INTO news_feed (timestamp, headline, source, keywords_matched) VALUES (?, ?, ?, ?)",
        (_now(), headline, source, keywords_matched),
    )
    conn.commit()


def get_news_feed(limit: int = 50) -> List[Dict[str, Any]]:
    """Return recent news items."""
    conn = _get_connection()
    rows = conn.execute(
        "SELECT * FROM news_feed ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]
