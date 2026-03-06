"""
SQLite database layer for persistent storage.
Tables: trades, equity_curve, system_logs, error_logs, token_status,
        news_feed, open_positions, performance_stats
"""

import os
import shutil
import sqlite3
import threading
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bot.config.settings import DB_BACKUP_DIR, DB_PATH


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
            daily_pnl REAL DEFAULT 0.0,
            total_pnl REAL DEFAULT 0.0,
            drawdown REAL DEFAULT 0.0,
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
            symbol TEXT DEFAULT '',
            error_type TEXT DEFAULT '',
            message TEXT NOT NULL,
            exception TEXT,
            api_response TEXT DEFAULT '',
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
            impact_level TEXT DEFAULT 'LOW',
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS open_positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            index_name TEXT NOT NULL,
            entry_price REAL NOT NULL,
            quantity INTEGER NOT NULL,
            stop_loss REAL,
            target REAL,
            entry_time TEXT NOT NULL,
            engine_mode TEXT NOT NULL DEFAULT 'paper',
            trade_id INTEGER,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        );

        CREATE TABLE IF NOT EXISTS performance_stats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT NOT NULL,
            win_rate REAL DEFAULT 0.0,
            profit_factor REAL DEFAULT 0.0,
            avg_rr REAL DEFAULT 0.0,
            max_drawdown REAL DEFAULT 0.0,
            trades_today INTEGER DEFAULT 0,
            daily_pnl REAL DEFAULT 0.0,
            total_pnl REAL DEFAULT 0.0,
            engine_mode TEXT NOT NULL DEFAULT 'paper',
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

def insert_equity_point(
    capital: float,
    engine_mode: str = "paper",
    daily_pnl: float = 0.0,
    total_pnl: float = 0.0,
    drawdown: float = 0.0,
) -> None:
    """Record a point on the equity curve."""
    conn = _get_connection()
    conn.execute(
        """INSERT INTO equity_curve (timestamp, capital, daily_pnl, total_pnl, drawdown, engine_mode)
           VALUES (?, ?, ?, ?, ?, ?)""",
        (_now(), capital, daily_pnl, total_pnl, drawdown, engine_mode),
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

def insert_error_log(
    module: str,
    message: str,
    exception: str = "",
    symbol: str = "",
    error_type: str = "",
    api_response: str = "",
) -> None:
    """Insert an error log entry with v3 enhanced fields."""
    conn = _get_connection()
    conn.execute(
        """INSERT INTO error_logs (timestamp, module, symbol, error_type, message, exception, api_response)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        (_now(), module, symbol, error_type, message, exception, api_response),
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

def insert_news(headline: str, source: str = "", keywords_matched: str = "", impact_level: str = "LOW") -> None:
    """Insert a news item."""
    conn = _get_connection()
    conn.execute(
        "INSERT INTO news_feed (timestamp, headline, source, keywords_matched, impact_level) VALUES (?, ?, ?, ?, ?)",
        (_now(), headline, source, keywords_matched, impact_level),
    )
    conn.commit()


def get_news_feed(limit: int = 50) -> List[Dict[str, Any]]:
    """Return recent news items."""
    conn = _get_connection()
    rows = conn.execute(
        "SELECT * FROM news_feed ORDER BY id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Open positions (v3)
# ---------------------------------------------------------------------------

def upsert_open_position(
    symbol: str,
    index_name: str,
    entry_price: float,
    quantity: int,
    stop_loss: float,
    target: float,
    engine_mode: str = "paper",
    trade_id: int = 0,
) -> None:
    """Insert or update an open position for recovery on restart."""
    conn = _get_connection()
    conn.execute(
        """INSERT INTO open_positions (symbol, index_name, entry_price, quantity, stop_loss, target, entry_time, engine_mode, trade_id)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (symbol, index_name, entry_price, quantity, stop_loss, target, _now(), engine_mode, trade_id),
    )
    conn.commit()


def remove_open_position(trade_id: int) -> None:
    """Remove an open position after it is closed."""
    conn = _get_connection()
    conn.execute("DELETE FROM open_positions WHERE trade_id=?", (trade_id,))
    conn.commit()


def get_open_positions(engine_mode: str = "paper") -> List[Dict[str, Any]]:
    """Return all open positions for recovery."""
    conn = _get_connection()
    rows = conn.execute(
        "SELECT * FROM open_positions WHERE engine_mode=? ORDER BY id",
        (engine_mode,),
    ).fetchall()
    return [dict(r) for r in rows]


def clear_open_positions(engine_mode: str = "paper") -> None:
    """Clear all open positions (used after full sync)."""
    conn = _get_connection()
    conn.execute("DELETE FROM open_positions WHERE engine_mode=?", (engine_mode,))
    conn.commit()


# ---------------------------------------------------------------------------
# Performance stats (v3)
# ---------------------------------------------------------------------------

def insert_performance_stats(
    win_rate: float,
    profit_factor: float,
    avg_rr: float,
    max_drawdown: float,
    trades_today: int,
    daily_pnl: float,
    total_pnl: float,
    engine_mode: str = "paper",
) -> None:
    """Insert a performance stats snapshot."""
    conn = _get_connection()
    conn.execute(
        """INSERT INTO performance_stats
           (timestamp, win_rate, profit_factor, avg_rr, max_drawdown, trades_today, daily_pnl, total_pnl, engine_mode)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        (_now(), win_rate, profit_factor, avg_rr, max_drawdown, trades_today, daily_pnl, total_pnl, engine_mode),
    )
    conn.commit()


def get_latest_performance_stats(engine_mode: str = "paper") -> Optional[Dict[str, Any]]:
    """Return the latest performance stats."""
    conn = _get_connection()
    row = conn.execute(
        "SELECT * FROM performance_stats WHERE engine_mode=? ORDER BY id DESC LIMIT 1",
        (engine_mode,),
    ).fetchone()
    return dict(row) if row else None


def update_trade_sl(trade_id: int, new_sl: float) -> None:
    """Update the stop-loss for an open trade (used by trailing SL)."""
    conn = _get_connection()
    conn.execute(
        "UPDATE trades SET stop_loss=? WHERE id=?",
        (new_sl, trade_id),
    )
    conn.commit()


def get_trades_today(engine_mode: str = "paper") -> int:
    """Count trades opened today (both open and closed)."""
    conn = _get_connection()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM trades WHERE engine_mode=? AND timestamp LIKE ?",
        (engine_mode, f"{today}%"),
    ).fetchone()
    return row["cnt"] if row else 0


def get_daily_pnl(engine_mode: str = "paper") -> float:
    """Get total P&L for today."""
    conn = _get_connection()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT COALESCE(SUM(pnl), 0.0) as total FROM trades WHERE engine_mode=? AND status='CLOSED' AND closed_at LIKE ?",
        (engine_mode, f"{today}%"),
    ).fetchone()
    return float(row["total"]) if row else 0.0


def get_total_pnl(engine_mode: str = "paper") -> float:
    """Get total P&L across all closed trades."""
    conn = _get_connection()
    row = conn.execute(
        "SELECT COALESCE(SUM(pnl), 0.0) as total FROM trades WHERE engine_mode=? AND status='CLOSED'",
        (engine_mode,),
    ).fetchone()
    return float(row["total"]) if row else 0.0


def get_trades_today_for_index(index_name: str, engine_mode: str = "paper") -> int:
    """Count trades for a specific index today."""
    conn = _get_connection()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT COUNT(*) as cnt FROM trades WHERE engine_mode=? AND index_name=? AND timestamp LIKE ?",
        (engine_mode, index_name, f"{today}%"),
    ).fetchone()
    return row["cnt"] if row else 0


# ---------------------------------------------------------------------------
# Database backup (v3)
# ---------------------------------------------------------------------------

def backup_database() -> str:
    """Create a backup of the database. Returns backup path."""
    os.makedirs(DB_BACKUP_DIR, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    backup_path = os.path.join(DB_BACKUP_DIR, f"trades_backup_{ts}.db")
    try:
        conn = _get_connection()
        backup_conn = sqlite3.connect(backup_path)
        conn.backup(backup_conn)
        backup_conn.close()
        return backup_path
    except Exception:
        # Fallback to file copy
        if os.path.exists(DB_PATH):
            shutil.copy2(DB_PATH, backup_path)
        return backup_path
