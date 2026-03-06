"""
Configuration settings for the Groww Multi-Index F&O AutoTrader.
All system-wide constants, API config, and trading parameters.
"""

import os
from dataclasses import dataclass, field
from typing import Dict, List


# ---------------------------------------------------------------------------
# Index definitions
# ---------------------------------------------------------------------------
SUPPORTED_INDICES: List[str] = ["NIFTY", "BANKNIFTY", "FINNIFTY"]

INDEX_SYMBOLS: Dict[str, str] = {
    "NIFTY": "NSE_NIFTY",
    "BANKNIFTY": "NSE_BANKNIFTY",
    "FINNIFTY": "NSE_FINNIFTY",
}

LOT_SIZES: Dict[str, int] = {
    "NIFTY": 65,
    "BANKNIFTY": 30,
    "FINNIFTY": 60,
}

STRIKE_STEP: Dict[str, int] = {
    "NIFTY": 50,
    "BANKNIFTY": 100,
    "FINNIFTY": 50,
}

# ---------------------------------------------------------------------------
# Polling & cycle
# ---------------------------------------------------------------------------
POLL_INTERVAL_SECONDS: int = 5
NEWS_POLL_INTERVAL_SECONDS: int = 60
CANDLE_INTERVAL: str = "MIN_15"

# ---------------------------------------------------------------------------
# Trade limits
# ---------------------------------------------------------------------------
MAX_OPEN_TRADES: int = 5
POST_CLOSE_WAIT_SECONDS: int = 5
MAX_CONSECUTIVE_LOSSES: int = 3

# ---------------------------------------------------------------------------
# Strategy parameters
# ---------------------------------------------------------------------------
EMA_FAST_PERIOD: int = 9
EMA_SLOW_PERIOD: int =  21
VOLUME_SPIKE_MULTIPLIER: float = 1.5
ATM_STRIKES_ABOVE: int = 5
ATM_STRIKES_BELOW: int = 5

# ---------------------------------------------------------------------------
# Paper trading defaults
# ---------------------------------------------------------------------------
PAPER_INITIAL_CAPITAL: float = 500000.0

# ---------------------------------------------------------------------------
# Risk management
# ---------------------------------------------------------------------------
STOP_LOSS_PERCENT: float = 1.5
TARGET_PERCENT: float = 3.0
MAX_RISK_PER_TRADE_PERCENT: float = 2.0

# ---------------------------------------------------------------------------
# LTP batch limits
# ---------------------------------------------------------------------------
LTP_BATCH_SIZE: int = 50  # max symbols per get_ltp() call

# ---------------------------------------------------------------------------
# News keywords
# ---------------------------------------------------------------------------
NEWS_KEYWORDS: List[str] = ["RBI", "FED", "War", "GDP", "Budget", "Inflation"]

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DB_PATH: str = os.environ.get("TRADING_DB_PATH", "bot/storage/trading.db")

# ---------------------------------------------------------------------------
# Multi-token architecture
# ---------------------------------------------------------------------------
TOKEN_ROLES: Dict[int, str] = {
    1: "Index LTP + Account balance",
    2: "NIFTY option scanning",
    3: "BANKNIFTY option scanning",
    4: "FINNIFTY option scanning",
    5: "Position monitoring",
}


@dataclass
class TokenConfig:
    """Holds one API token and its role."""
    token: str
    role_id: int
    role_description: str = ""
    is_active: bool = True
    last_used: float = 0.0
    failure_count: int = 0


@dataclass
class AppConfig:
    """Runtime configuration loaded from environment / UI."""
    mode: str = "paper"  # "paper" or "live"
    tokens: List[TokenConfig] = field(default_factory=list)
    paper_capital: float = PAPER_INITIAL_CAPITAL
    engine_state: str = "idle"  # idle | running | stopped

    def get_token_for_role(self, role_id: int) -> str:
        """Return the token string assigned to a given role, falling back to first token."""
        for t in self.tokens:
            if t.role_id == role_id and t.is_active:
                return t.token
        # Fallback: use the first active token
        for t in self.tokens:
            if t.is_active:
                return t.token
        return ""

    def add_token(self, token: str, role_id: int) -> None:
        desc = TOKEN_ROLES.get(role_id, "Unknown")
        self.tokens.append(TokenConfig(token=token, role_id=role_id, role_description=desc))
