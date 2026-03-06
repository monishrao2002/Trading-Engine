"""
Data layer: fetches LTP, candle data, option chains, and manages ATM detection.
v3: Added candle cache system to reduce API load by 80-90%.
"""

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from bot.api.groww_client import GrowwClientWrapper
from bot.config.settings import (
    ATM_STRIKES_ABOVE,
    ATM_STRIKES_BELOW,
    INDEX_SYMBOLS,
    LOT_SIZES,
    STRIKE_STEP,
    SUPPORTED_INDICES,
)
from bot.logs.logger import log_api, log_error, log_info
from bot.storage import database as db


class CandleCache:
    """Cache for candle data to avoid redundant API calls.

    Logic:
    - If candle already fetched and next candle not yet formed, use cached candles.
    - Otherwise fetch new candles.
    - 15-min candles form at :00, :15, :30, :45 of each hour.
    """

    def __init__(self) -> None:
        self._cache: Dict[str, List[Dict[str, Any]]] = {}
        self._last_fetch_time: Dict[str, float] = {}
        self._last_candle_ts: Dict[str, str] = {}

    def get_cached(self, symbol: str) -> Optional[List[Dict[str, Any]]]:
        """Return cached candles if still valid (next 15-min candle not yet formed)."""
        if symbol not in self._cache:
            return None

        last_fetch = self._last_fetch_time.get(symbol, 0)
        now = time.time()

        # Check if we are still within the same 15-min window
        now_dt = datetime.now(timezone.utc) + timedelta(hours=5, minutes=30)  # IST
        current_minute = now_dt.minute
        # Next candle forms at next multiple of 15
        next_candle_minute = ((current_minute // 15) + 1) * 15
        seconds_until_next = (next_candle_minute - current_minute) * 60 - now_dt.second

        if seconds_until_next < 0:
            seconds_until_next += 900  # 15 min

        # If we fetched within this candle window, use cache
        elapsed = now - last_fetch
        if elapsed < 900 and elapsed < seconds_until_next + 30:
            return self._cache[symbol]

        return None

    def update(self, symbol: str, candles: List[Dict[str, Any]]) -> None:
        """Store candles in cache."""
        self._cache[symbol] = candles
        self._last_fetch_time[symbol] = time.time()
        if candles:
            self._last_candle_ts[symbol] = str(candles[-1].get("timestamp", ""))

    def get_last_candle_ts(self, symbol: str) -> str:
        """Return the timestamp of the last cached candle."""
        return self._last_candle_ts.get(symbol, "")

    def get_candle_count(self, symbol: str) -> int:
        """Return number of cached candles."""
        return len(self._cache.get(symbol, []))


class DataLayer:
    """Handles all market data retrieval and processing."""

    def __init__(self, client: GrowwClientWrapper) -> None:
        self.client = client
        self._index_ltp: Dict[str, float] = {}
        self._option_chains: Dict[str, Dict[str, Any]] = {}
        self._nearest_expiry: Dict[str, str] = {}
        self.candle_cache = CandleCache()
        # v3: Track selected ATM strikes and option symbols for dashboard
        self._selected_atm: Dict[str, int] = {}
        self._selected_symbols: Dict[str, str] = {}
        self._candle_info: Dict[str, Dict[str, Any]] = {}  # per-index candle fetch info

    # ------------------------------------------------------------------
    # Index LTP
    # ------------------------------------------------------------------

    def fetch_index_ltp(self) -> Dict[str, float]:
        """Fetch LTP for all supported indices."""
        symbols = tuple(INDEX_SYMBOLS[idx] for idx in SUPPORTED_INDICES)
        ltp_data = self.client.get_index_ltp(symbols)
        for idx in SUPPORTED_INDICES:
            sym = INDEX_SYMBOLS[idx]
            if sym in ltp_data:
                self._index_ltp[idx] = ltp_data[sym]
        return dict(self._index_ltp)

    def get_cached_index_ltp(self) -> Dict[str, float]:
        """Return last fetched index LTP values."""
        return dict(self._index_ltp)

    # ------------------------------------------------------------------
    # ATM strike detection
    # ------------------------------------------------------------------

    @staticmethod
    def detect_atm_strike(index_ltp: float, step: int) -> int:
        """Round the index LTP to the nearest strike price."""
        return int(round(index_ltp / step) * step)

    def get_atm_strikes(self, index_name: str) -> List[int]:
        """
        Compute ATM and surrounding strikes for an index.
        Returns list of strikes: 5 below ATM, ATM, 5 above ATM.
        """
        ltp = self._index_ltp.get(index_name)
        if ltp is None:
            return []
        step = STRIKE_STEP[index_name]
        atm = self.detect_atm_strike(ltp, step)
        strikes = []
        for i in range(-ATM_STRIKES_BELOW, ATM_STRIKES_ABOVE + 1):
            strikes.append(atm + i * step)
        return strikes

    # ------------------------------------------------------------------
    # Expiry detection
    # ------------------------------------------------------------------

    def fetch_nearest_expiry(self, index_name: str) -> Optional[str]:
        """Fetch and cache the nearest expiry date for an index."""
        now = datetime.now(timezone.utc)
        expiries = self.client.get_expiries(
            underlying=index_name,
            year=now.year,
            month=now.month,
        )
        if not expiries:
            # Try next month
            next_month = now.month + 1 if now.month < 12 else 1
            next_year = now.year if now.month < 12 else now.year + 1
            expiries = self.client.get_expiries(
                underlying=index_name,
                year=next_year,
                month=next_month,
            )
        if not expiries:
            log_error(f"No expiries found for {index_name}", "data_layer")
            return None

        # Find nearest future expiry
        today_str = now.strftime("%Y-%m-%d")
        future_expiries = [e for e in expiries if e >= today_str]
        if not future_expiries:
            future_expiries = expiries

        nearest = min(future_expiries)
        self._nearest_expiry[index_name] = nearest
        log_info(f"Nearest expiry for {index_name}: {nearest}", "data_layer")
        return nearest

    def get_cached_expiry(self, index_name: str) -> Optional[str]:
        """Return cached nearest expiry for an index."""
        return self._nearest_expiry.get(index_name)

    # ------------------------------------------------------------------
    # Option chain
    # ------------------------------------------------------------------

    def fetch_option_chain(self, index_name: str, expiry_date: str) -> Dict[str, Any]:
        """Fetch the option chain for an index and expiry."""
        role_map = {"NIFTY": 2, "BANKNIFTY": 3, "FINNIFTY": 4}
        role_id = role_map.get(index_name, 2)
        chain = self.client.get_option_chain(
            underlying=index_name,
            expiry_date=expiry_date,
            role_id=role_id,
        )
        if chain:
            self._option_chains[index_name] = chain
        return chain

    def get_option_ltp_for_strikes(
        self,
        index_name: str,
        strikes: List[int],
        expiry_date: str,
        option_type: str = "CE",
    ) -> Dict[int, float]:
        """
        Get LTP for a list of strikes from the cached option chain.
        Falls back to individual quote calls if needed.
        """
        result: Dict[int, float] = {}
        chain = self._option_chains.get(index_name, {})
        chain_strikes = chain.get("strikes", {})

        for strike in strikes:
            strike_str = str(strike)
            if strike_str in chain_strikes:
                option_data = chain_strikes[strike_str].get(option_type, {})
                ltp = option_data.get("ltp")
                if ltp is not None:
                    result[strike] = float(ltp)
                    continue

            # Fallback: try individual quote
            trading_symbol = chain_strikes.get(strike_str, {}).get(option_type, {}).get("trading_symbol")
            if trading_symbol:
                role_map = {"NIFTY": 2, "BANKNIFTY": 3, "FINNIFTY": 4}
                role_id = role_map.get(index_name, 2)
                ltp_val = self.client.get_weekly_option_ltp(trading_symbol, role_id=role_id)
                if ltp_val is not None:
                    result[strike] = ltp_val

        return result

    def get_trading_symbol_for_strike(
        self, index_name: str, strike: int, option_type: str = "CE"
    ) -> Optional[str]:
        """Get the trading symbol for a specific strike from cached option chain."""
        chain = self._option_chains.get(index_name, {})
        strike_data = chain.get("strikes", {}).get(str(strike), {})
        option_data = strike_data.get(option_type, {})
        return option_data.get("trading_symbol")

    # ------------------------------------------------------------------
    # Historical candles
    # ------------------------------------------------------------------

    def fetch_index_candles(
        self,
        index_name: str,
        lookback_minutes: int = 300,
        interval: str = "MIN_15",
    ) -> List[Dict[str, Any]]:
        """
        Fetch historical 15-min candles for an index.
        Uses candle cache to reduce API load by 80-90%.
        Returns list of dicts with keys: timestamp, open, high, low, close, volume.
        """
        cache_key = f"index_{index_name}_{interval}"

        # Check cache first
        cached = self.candle_cache.get_cached(cache_key)
        if cached is not None:
            log_info(
                f"[CANDLES] {index_name} using cached {len(cached)} candles. "
                f"Latest: {self.candle_cache.get_last_candle_ts(cache_key)}",
                "data_layer",
            )
            self._candle_info[index_name] = {
                "count": len(cached),
                "latest_ts": self.candle_cache.get_last_candle_ts(cache_key),
                "source": "cache",
            }
            return cached

        now = datetime.now(timezone.utc)
        # IST offset
        ist_offset = timedelta(hours=5, minutes=30)
        now_ist = now + ist_offset
        end_time = now_ist.strftime("%Y-%m-%d %H:%M:%S")
        start_ist = now_ist - timedelta(minutes=lookback_minutes)
        start_time = start_ist.strftime("%Y-%m-%d %H:%M:%S")

        resp = self.client.get_index_candles(
            index_symbol=index_name,
            start_time=start_time,
            end_time=end_time,
            candle_interval=interval,
        )
        candles = self._parse_candles(resp)

        if candles:
            self.candle_cache.update(cache_key, candles)
            latest_ts = str(candles[-1].get("timestamp", ""))
            log_info(
                f"[CANDLES] {index_name} fetched {len(candles)} candles. Latest: {latest_ts}",
                "data_layer",
            )
            db.insert_system_log(
                "INFO", "data_layer",
                f"[CANDLES] {index_name} fetched {len(candles)} candles. Latest: {latest_ts}",
            )
            self._candle_info[index_name] = {
                "count": len(candles),
                "latest_ts": latest_ts,
                "source": "api",
            }

        return candles

    def fetch_option_candles(
        self,
        groww_symbol: str,
        lookback_minutes: int = 300,
        interval: str = "MIN_15",
    ) -> List[Dict[str, Any]]:
        """Fetch historical candles for an option contract."""
        now = datetime.now(timezone.utc)
        ist_offset = timedelta(hours=5, minutes=30)
        now_ist = now + ist_offset
        end_time = now_ist.strftime("%Y-%m-%d %H:%M:%S")
        start_ist = now_ist - timedelta(minutes=lookback_minutes)
        start_time = start_ist.strftime("%Y-%m-%d %H:%M:%S")

        resp = self.client.get_historical_candles(
            groww_symbol=groww_symbol,
            start_time=start_time,
            end_time=end_time,
            segment="FNO",
            candle_interval=interval,
        )
        return self._parse_candles(resp)

    @staticmethod
    def _parse_candles(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Parse candle response into list of dicts."""
        candles_raw = resp.get("candles", [])
        parsed = []
        for c in candles_raw:
            if len(c) >= 6:
                parsed.append({
                    "timestamp": c[0],
                    "open": float(c[1]),
                    "high": float(c[2]),
                    "low": float(c[3]),
                    "close": float(c[4]),
                    "volume": int(c[5]) if c[5] is not None else 0,
                    "oi": float(c[6]) if len(c) > 6 and c[6] is not None else 0.0,
                })
        return parsed

    # ------------------------------------------------------------------
    # Margin
    # ------------------------------------------------------------------

    def fetch_margin(self) -> Dict[str, Any]:
        """Fetch available margin from Groww."""
        return self.client.get_available_margin()

    def get_fno_available_margin(self) -> float:
        """Get the F&O option buy balance available."""
        margin_data = self.fetch_margin()
        fno = margin_data.get("fno_margin_details", {})
        return float(fno.get("option_buy_balance_available", 0.0))

    def get_margin_details(self) -> Dict[str, Any]:
        """Get detailed margin info for dashboard display."""
        margin_data = self.fetch_margin()
        fno = margin_data.get("fno_margin_details", {})
        return {
            "clear_cash": float(fno.get("clear_cash", 0.0)),
            "net_margin_available": float(fno.get("net_margin_available", 0.0)),
            "option_buy_balance_available": float(fno.get("option_buy_balance_available", 0.0)),
            "used_margin": float(fno.get("used_margin", 0.0)),
        }

    # ------------------------------------------------------------------
    # v3: Dashboard info helpers
    # ------------------------------------------------------------------

    def get_candle_info(self) -> Dict[str, Dict[str, Any]]:
        """Return candle fetch info for all indices."""
        return dict(self._candle_info)

    def get_selected_atm(self) -> Dict[str, int]:
        """Return selected ATM strikes per index."""
        return dict(self._selected_atm)

    def get_selected_symbols(self) -> Dict[str, str]:
        """Return selected option symbols per index."""
        return dict(self._selected_symbols)

    def set_selected_atm(self, index_name: str, strike: int) -> None:
        """Record the ATM strike selected for trading."""
        self._selected_atm[index_name] = strike

    def set_selected_symbol(self, index_name: str, symbol: str) -> None:
        """Record the option symbol selected for trading."""
        self._selected_symbols[index_name] = symbol

    def get_latest_candle_ohlc(self, index_name: str) -> Optional[Dict[str, Any]]:
        """Return OHLC of the latest cached candle for an index."""
        cache_key = f"index_{index_name}_MIN_15"
        cached = self.candle_cache._cache.get(cache_key, [])
        if cached:
            c = cached[-1]
            return {
                "timestamp": c.get("timestamp", ""),
                "open": c.get("open", 0),
                "high": c.get("high", 0),
                "low": c.get("low", 0),
                "close": c.get("close", 0),
                "volume": c.get("volume", 0),
            }
        return None
