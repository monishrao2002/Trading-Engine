"""
Groww API client wrapper with multi-token support.
Handles token rotation, failure detection, and retry logic.
"""

import hashlib
import time
from typing import Any, Dict, List, Optional, Tuple

from bot.config.settings import AppConfig, TokenConfig
from bot.logs.logger import log_api, log_error, log_warning
from bot.storage import database as db

# Maximum consecutive failures before marking a token as inactive
MAX_TOKEN_FAILURES = 3


class GrowwClientWrapper:
    """
    Wraps the growwapi.GrowwAPI SDK with multi-token management.
    Each token is assigned a role (1-5) per the multi-token architecture.
    """

    def __init__(self, config: AppConfig) -> None:
        self.config = config
        self._clients: Dict[int, Any] = {}
        self._initialize_clients()

    def _initialize_clients(self) -> None:
        """Create GrowwAPI instances for each configured token."""
        try:
            from growwapi import GrowwAPI
        except ImportError:
            log_warning("growwapi SDK not installed. Running in paper-only mode.", "groww_client")
            return

        for token_cfg in self.config.tokens:
            if token_cfg.is_active:
                try:
                    client = GrowwAPI(token_cfg.token)
                    self._clients[token_cfg.role_id] = client
                    log_api(f"Initialized GrowwAPI client for role {token_cfg.role_id}: {token_cfg.role_description}")
                except Exception as exc:
                    log_error(f"Failed to init client for role {token_cfg.role_id}", "groww_client", exc)
                    token_cfg.is_active = False

    def _get_client(self, role_id: int) -> Optional[Any]:
        """Get the GrowwAPI client for a given role, with fallback."""
        client = self._clients.get(role_id)
        if client is not None:
            return client
        # Fallback to any available client
        for rid, c in self._clients.items():
            return c
        return None

    def _handle_token_failure(self, role_id: int, exc: Exception) -> None:
        """Track token failures and deactivate if threshold exceeded."""
        for token_cfg in self.config.tokens:
            if token_cfg.role_id == role_id:
                token_cfg.failure_count += 1
                log_warning(
                    f"Token role {role_id} failure #{token_cfg.failure_count}: {exc}",
                    "groww_client",
                )
                if token_cfg.failure_count >= MAX_TOKEN_FAILURES:
                    token_cfg.is_active = False
                    log_error(
                        f"Token role {role_id} deactivated after {MAX_TOKEN_FAILURES} failures",
                        "groww_client",
                    )
                    if role_id in self._clients:
                        del self._clients[role_id]
                # Persist status
                token_hash = hashlib.sha256(token_cfg.token[:8].encode()).hexdigest()[:16]
                db.upsert_token_status(role_id, token_hash, token_cfg.is_active, token_cfg.failure_count)
                break

    def _record_success(self, role_id: int) -> None:
        """Reset failure count on successful call."""
        for token_cfg in self.config.tokens:
            if token_cfg.role_id == role_id:
                token_cfg.failure_count = 0
                token_cfg.last_used = time.time()
                break

    # ------------------------------------------------------------------
    # Index LTP (Role 1)
    # ------------------------------------------------------------------

    def get_index_ltp(self, exchange_trading_symbols: Tuple[str, ...]) -> Dict[str, float]:
        """
        Fetch LTP for index symbols using SEGMENT_CASH.
        e.g. ("NSE_NIFTY", "NSE_BANKNIFTY", "NSE_FINNIFTY")
        """
        role_id = 1
        client = self._get_client(role_id)
        if client is None:
            log_warning("No client available for index LTP", "groww_client")
            return {}
        try:
            from growwapi import GrowwAPI
            resp = client.get_ltp(
                segment=GrowwAPI.SEGMENT_CASH,
                exchange_trading_symbols=exchange_trading_symbols,
            )
            self._record_success(role_id)
            log_api(f"Index LTP response: {resp}")
            if isinstance(resp, dict):
                return {k: float(v) for k, v in resp.items() if v is not None}
            return {}
        except Exception as exc:
            self._handle_token_failure(role_id, exc)
            return {}

    # ------------------------------------------------------------------
    # Option LTP via get_ltp (monthly) - batch up to 50 (Roles 2-4)
    # ------------------------------------------------------------------

    def get_option_ltp_batch(
        self, symbols: Tuple[str, ...], role_id: int = 2
    ) -> Dict[str, float]:
        """
        Fetch LTP for option symbols (monthly) using SEGMENT_FNO.
        Up to 50 symbols per call.
        """
        client = self._get_client(role_id)
        if client is None:
            return {}
        try:
            from growwapi import GrowwAPI
            resp = client.get_ltp(
                segment=GrowwAPI.SEGMENT_FNO,
                exchange_trading_symbols=symbols,
            )
            self._record_success(role_id)
            log_api(f"Option LTP batch response for role {role_id}: keys={list(resp.keys()) if isinstance(resp, dict) else 'N/A'}")
            if isinstance(resp, dict):
                return {k: float(v) for k, v in resp.items() if v is not None}
            return {}
        except Exception as exc:
            self._handle_token_failure(role_id, exc)
            return {}

    # ------------------------------------------------------------------
    # Weekly option LTP via get_quote (1 at a time)
    # ------------------------------------------------------------------

    def get_weekly_option_ltp(self, trading_symbol: str, role_id: int = 2) -> Optional[float]:
        """
        Fetch LTP for a weekly option using get_quote (one symbol at a time).
        """
        client = self._get_client(role_id)
        if client is None:
            return None
        try:
            from growwapi import GrowwAPI
            resp = client.get_quote(
                exchange=GrowwAPI.EXCHANGE_NSE,
                segment=GrowwAPI.SEGMENT_FNO,
                trading_symbol=trading_symbol,
            )
            self._record_success(role_id)
            log_api(f"Weekly option quote for {trading_symbol}: last_price={resp.get('last_price')}")
            if isinstance(resp, dict) and "last_price" in resp:
                return float(resp["last_price"])
            return None
        except Exception as exc:
            self._handle_token_failure(role_id, exc)
            return None

    # ------------------------------------------------------------------
    # Option chain
    # ------------------------------------------------------------------

    def get_option_chain(
        self, underlying: str, expiry_date: str, role_id: int = 2
    ) -> Dict[str, Any]:
        """Fetch the complete option chain for an underlying and expiry."""
        client = self._get_client(role_id)
        if client is None:
            return {}
        try:
            from growwapi import GrowwAPI
            resp = client.get_option_chain(
                exchange=GrowwAPI.EXCHANGE_NSE,
                underlying=underlying,
                expiry_date=expiry_date,
            )
            self._record_success(role_id)
            log_api(f"Option chain for {underlying} expiry {expiry_date}: got response")
            return resp if isinstance(resp, dict) else {}
        except Exception as exc:
            self._handle_token_failure(role_id, exc)
            return {}

    # ------------------------------------------------------------------
    # Expiries & contracts
    # ------------------------------------------------------------------

    def get_expiries(self, underlying: str, year: int, month: int) -> List[str]:
        """Fetch available expiry dates."""
        client = self._get_client(1)
        if client is None:
            return []
        try:
            from growwapi import GrowwAPI
            resp = client.get_expiries(
                exchange=GrowwAPI.EXCHANGE_NSE,
                underlying_symbol=underlying,
                year=year,
                month=month,
            )
            self._record_success(1)
            return resp.get("expiries", []) if isinstance(resp, dict) else []
        except Exception as exc:
            self._handle_token_failure(1, exc)
            return []

    def get_contracts(self, underlying: str, expiry_date: str) -> List[str]:
        """Fetch available contracts for a given expiry."""
        client = self._get_client(1)
        if client is None:
            return []
        try:
            from growwapi import GrowwAPI
            resp = client.get_contracts(
                exchange=GrowwAPI.EXCHANGE_NSE,
                underlying_symbol=underlying,
                expiry_date=expiry_date,
            )
            self._record_success(1)
            return resp.get("contracts", []) if isinstance(resp, dict) else []
        except Exception as exc:
            self._handle_token_failure(1, exc)
            return []

    # ------------------------------------------------------------------
    # Historical candles
    # ------------------------------------------------------------------

    def get_historical_candles(
        self,
        groww_symbol: str,
        start_time: str,
        end_time: str,
        segment: str = "FNO",
        candle_interval: str = "MIN_15",
    ) -> Dict[str, Any]:
        """Fetch historical candle data."""
        client = self._get_client(1)
        if client is None:
            return {}
        try:
            from growwapi import GrowwAPI
            seg = GrowwAPI.SEGMENT_FNO if segment == "FNO" else GrowwAPI.SEGMENT_CASH
            interval_map = {
                "MIN_1": GrowwAPI.CANDLE_INTERVAL_MIN_1,
                "MIN_5": GrowwAPI.CANDLE_INTERVAL_MIN_5,
                "MIN_15": GrowwAPI.CANDLE_INTERVAL_MIN_15,
                "MIN_30": GrowwAPI.CANDLE_INTERVAL_MIN_30,
                "HOUR_1": GrowwAPI.CANDLE_INTERVAL_HOUR_1,
            }
            interval = interval_map.get(candle_interval, GrowwAPI.CANDLE_INTERVAL_MIN_15)
            resp = client.get_historical_candles(
                exchange=GrowwAPI.EXCHANGE_NSE,
                segment=seg,
                groww_symbol=groww_symbol,
                start_time=start_time,
                end_time=end_time,
                candle_interval=interval,
            )
            self._record_success(1)
            return resp if isinstance(resp, dict) else {}
        except Exception as exc:
            self._handle_token_failure(1, exc)
            return {}

    # ------------------------------------------------------------------
    # Index candles (CASH segment)
    # ------------------------------------------------------------------

    def get_index_candles(
        self,
        index_symbol: str,
        start_time: str,
        end_time: str,
        candle_interval: str = "MIN_15",
    ) -> Dict[str, Any]:
        """Fetch historical candle data for an index (CASH segment)."""
        # The groww_symbol for index is like "NSE-NIFTY"
        groww_symbol = f"NSE-{index_symbol}"
        return self.get_historical_candles(
            groww_symbol=groww_symbol,
            start_time=start_time,
            end_time=end_time,
            segment="CASH",
            candle_interval=candle_interval,
        )

    # ------------------------------------------------------------------
    # Margin (Role 1)
    # ------------------------------------------------------------------

    def get_available_margin(self) -> Dict[str, Any]:
        """Fetch available margin details."""
        client = self._get_client(1)
        if client is None:
            return {}
        try:
            resp = client.get_available_margin_details()
            self._record_success(1)
            log_api(f"Margin response: {resp}")
            return resp if isinstance(resp, dict) else {}
        except Exception as exc:
            self._handle_token_failure(1, exc)
            return {}

    # ------------------------------------------------------------------
    # Order placement (Role 5 or fallback)
    # ------------------------------------------------------------------

    def place_order(
        self,
        trading_symbol: str,
        quantity: int,
        transaction_type: str = "BUY",
        order_type: str = "MARKET",
        price: float = 0.0,
        product: str = "MIS",
    ) -> Dict[str, Any]:
        """Place an order through the Groww API."""
        role_id = 5
        client = self._get_client(role_id)
        if client is None:
            log_error("No client available for order placement", "groww_client")
            return {}
        try:
            from growwapi import GrowwAPI
            order_type_map = {
                "MARKET": GrowwAPI.ORDER_TYPE_MARKET,
                "LIMIT": GrowwAPI.ORDER_TYPE_LIMIT,
            }
            tx_type_map = {
                "BUY": GrowwAPI.TRANSACTION_TYPE_BUY,
                "SELL": GrowwAPI.TRANSACTION_TYPE_SELL,
            }
            product_map = {
                "MIS": GrowwAPI.PRODUCT_MIS,
                "NRML": GrowwAPI.PRODUCT_NRML,
            }

            kwargs: Dict[str, Any] = {
                "trading_symbol": trading_symbol,
                "quantity": quantity,
                "validity": GrowwAPI.VALIDITY_DAY,
                "exchange": GrowwAPI.EXCHANGE_NSE,
                "segment": GrowwAPI.SEGMENT_FNO,
                "product": product_map.get(product, GrowwAPI.PRODUCT_MIS),
                "order_type": order_type_map.get(order_type, GrowwAPI.ORDER_TYPE_MARKET),
                "transaction_type": tx_type_map.get(transaction_type, GrowwAPI.TRANSACTION_TYPE_BUY),
            }
            if order_type == "LIMIT" and price > 0:
                kwargs["price"] = price

            resp = client.place_order(**kwargs)
            self._record_success(role_id)
            log_api(f"Order placed: {resp}")
            return resp if isinstance(resp, dict) else {}
        except Exception as exc:
            self._handle_token_failure(role_id, exc)
            log_error(f"Order placement failed: {exc}", "groww_client", exc)
            return {}

    # ------------------------------------------------------------------
    # Positions (Role 5)
    # ------------------------------------------------------------------

    def get_positions(self) -> List[Dict[str, Any]]:
        """Fetch current positions."""
        role_id = 5
        client = self._get_client(role_id)
        if client is None:
            return []
        try:
            from growwapi import GrowwAPI
            resp = client.get_positions_for_user(segment=GrowwAPI.SEGMENT_FNO)
            self._record_success(role_id)
            if isinstance(resp, dict) and "positions" in resp:
                return resp["positions"]
            if isinstance(resp, list):
                return resp
            return []
        except Exception as exc:
            self._handle_token_failure(role_id, exc)
            return []

    # ------------------------------------------------------------------
    # Utility
    # ------------------------------------------------------------------

    def is_connected(self) -> bool:
        """Check if at least one client is available."""
        return len(self._clients) > 0

    def get_active_token_count(self) -> int:
        """Return the number of active tokens."""
        return sum(1 for t in self.config.tokens if t.is_active)

    def reinitialize_token(self, role_id: int, new_token: str) -> bool:
        """Replace a token and reinitialize its client."""
        for token_cfg in self.config.tokens:
            if token_cfg.role_id == role_id:
                token_cfg.token = new_token
                token_cfg.is_active = True
                token_cfg.failure_count = 0
                break
        else:
            return False

        try:
            from growwapi import GrowwAPI
            self._clients[role_id] = GrowwAPI(new_token)
            log_api(f"Reinitialized token for role {role_id}")
            return True
        except Exception as exc:
            log_error(f"Failed to reinitialize token for role {role_id}", "groww_client", exc)
            return False
