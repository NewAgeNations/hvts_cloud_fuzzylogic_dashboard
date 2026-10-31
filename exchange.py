# exchange.py – Gate.io Futures (swap) with Circuit Breaker & Native Take-Profit
# ============================================================================
# v18.2 – MULTI-SYMBOL SUPPORT
#
#   • ADDED: min_safe_balance_override parameter on execute_trade() and
#            validate_margin_usage() / _validate_fixed_position().
#            Each engine can pass its OWN per-symbol MIN_SAFE floor
#            instead of relying on the module-level global. This fixes
#            the cross-symbol contamination bug where one symbol's
#            ratcheted floor would block entries on OTHER symbols.
#
#   • ADDED: set_runtime_min_safe_balance() still exists for backward
#            compatibility, but the engine no longer calls it. The
#            override parameter is now the canonical mechanism.
#
#   • UNCHANGED: All circuit breakers, timeouts, limit-order fallback,
#                already-closed detection, fixed-mode enforcement,
#                cross-margin enforcement, fetch_positions,
#                fetch_closing_fill_price, native TP placement.
#
#   • UNCHANGED: FIXED_POSITION_SIZE, LEVERAGE, MIN_SAFE_BALANCE_USDT,
#                EMERGENCY_* env vars — still read from os.environ
#                at import time (with override at call time for
#                MIN_SAFE via the new parameter).
# ============================================================================

import asyncio
import logging
import os
import time
import math
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Any, Tuple
from dotenv import load_dotenv

import hashlib
import hmac
import json

import ccxt
import pandas as pd
import numpy as np

# Force load .env with override
load_dotenv(override=True)

logger = logging.getLogger(__name__)

# ============================================================================
# TIMEOUT CONSTANTS (Gate.io can be slower at peak times)
# ============================================================================

LOAD_MARKETS_TIMEOUT = 60   # seconds – raised from 20s; Gate.io can be slow on first connect
API_CALL_TIMEOUT     = 20   # seconds for any single ccxt call (raised from 15s)
BALANCE_TIMEOUT      = 20   # seconds for fetch_balance
OHLCV_TIMEOUT        = 30   # seconds for fetch_ohlcv (raised from 20s)

# ============================================================================
# DIRECT ENVIRONMENT VARIABLE READING - NO CONFIG FILE DEPENDENCY
# ============================================================================

# Read fixed position size DIRECTLY from environment
FIXED_POSITION_SIZE = os.getenv('FIXED_POSITION_SIZE')
if FIXED_POSITION_SIZE is not None:
    try:
        FIXED_POSITION_SIZE = float(FIXED_POSITION_SIZE)
        if FIXED_POSITION_SIZE <= 0:
            FIXED_POSITION_SIZE = None
            print("⚠️ FIXED_POSITION_SIZE is 0 or negative, using dynamic mode")
        else:
            print(f"🔧 FIXED POSITION SIZE: {FIXED_POSITION_SIZE} USDT margin per position")
            print(f"🔒 STRICT ENFORCEMENT: Fixed position size will be enforced with 5% tolerance")
    except ValueError:
        FIXED_POSITION_SIZE = None
        print("⚠️ Invalid FIXED_POSITION_SIZE value, using dynamic mode")
else:
    FIXED_POSITION_SIZE = None
    print("📊 DYNAMIC MODE: No FIXED_POSITION_SIZE set, using 1% rule")

# Read leverage directly from environment
LEVERAGE = os.getenv('LEVERAGE')
if LEVERAGE is not None:
    try:
        LEVERAGE = int(LEVERAGE)
        if LEVERAGE <= 0 or LEVERAGE > 100:
            LEVERAGE = 25
            print(f"⚠️ LEVERAGE out of range (1-100), using default 25")
        else:
            print(f"🔧 LEVERAGE: {LEVERAGE}x")
    except ValueError:
        LEVERAGE = 25
        print("⚠️ Invalid LEVERAGE value, using default 25")
else:
    LEVERAGE = 25
    print(f"📊 Using default leverage: {LEVERAGE}x")

# Read MIN_SAFE_BALANCE_USDT DIRECTLY from environment - STRICT ENFORCEMENT
# This is the GLOBAL default used only when no per-call override is given.
MIN_SAFE_BALANCE_USDT = float(os.getenv("MIN_SAFE_BALANCE_USDT", "50.0"))
print(f"🔒 MINIMUM SAFE BALANCE (global default): ${MIN_SAFE_BALANCE_USDT:.2f} USDT")

# 🔥 Read emergency exit settings
EMERGENCY_MAX_SLIPPAGE_PERCENT = float(os.getenv("EMERGENCY_MAX_SLIPPAGE_PERCENT", "0.5"))
EMERGENCY_FORCE_EXIT_LOSS_LIMIT = float(os.getenv("EMERGENCY_FORCE_EXIT_LOSS_LIMIT", "2.0"))

# 🚨 Read emergency limit wait timeout from environment
EMERGENCY_LIMIT_WAIT_SECONDS = float(os.getenv("EMERGENCY_LIMIT_WAIT_SECONDS", "5.0"))
print(f"🚨 EMERGENCY LIMIT WAIT: {EMERGENCY_LIMIT_WAIT_SECONDS}s (before MARKET fallback)")


def set_runtime_min_safe_balance(new_min_safe_balance_usdt: float) -> None:
    """
    Legacy support: update the module-level MIN_SAFE_BALANCE_USDT global.

    ⚠️ In v18.2, this is DEPRECATED. Each engine should pass its own
    floor via the `min_safe_balance_override` parameter on
    execute_trade() / validate_margin_usage() instead. Calling this
    still works but affects ALL symbols sharing the process.

    Kept for backward compatibility with pre-v18.2 callers.
    """
    global MIN_SAFE_BALANCE_USDT
    try:
        new_value = float(new_min_safe_balance_usdt)
    except (TypeError, ValueError):
        print(f"⚠️ set_runtime_min_safe_balance: invalid value {new_min_safe_balance_usdt!r}, ignoring")
        return
    old_value = MIN_SAFE_BALANCE_USDT
    MIN_SAFE_BALANCE_USDT = new_value
    if new_value != old_value:
        print(f"🔒 MIN_SAFE_BALANCE_USDT global updated: ${old_value:.2f} → ${new_value:.2f}")


def _resolve_min_safe_balance(override: Optional[float]) -> float:
    """
    Return the effective MIN_SAFE floor for a single trade call.

    Precedence:
        1. Per-call override (from the per-symbol engine)
        2. Module-level global (from .env / set_runtime_min_safe_balance)
    """
    if override is not None:
        try:
            v = float(override)
            if v >= 0:
                return v
        except (TypeError, ValueError):
            pass
    return MIN_SAFE_BALANCE_USDT


# Log final fixed mode status
USE_FIXED_MODE = (FIXED_POSITION_SIZE is not None and FIXED_POSITION_SIZE > 0)
if USE_FIXED_MODE:
    NOTIONAL_PER_TRADE = FIXED_POSITION_SIZE * LEVERAGE
    print(f"✅ FIXED MODE ACTIVE: {FIXED_POSITION_SIZE} USDT margin = {NOTIONAL_PER_TRADE:.2f} USDT notional @ {LEVERAGE}x")
else:
    print(f"✅ DYNAMIC MODE ACTIVE: Using 1% of balance per position")

# Legacy compatibility variable (for imports from trading_config)
FIXED_POSITION_SIZE_USDT = FIXED_POSITION_SIZE

# Fixed position size tolerance (5% max deviation due to exchange minimums)
FIXED_SIZE_TOLERANCE = 0.05  # 5%

# ============================================================================
# "ALREADY CLOSED" DETECTION (v10.5 — phantom position fix)
# ============================================================================
#
# Gate.io returns one of these labels/messages when a reduce-only order is
# sent for a symbol that has no position on the book. Semantically, for a
# closing order, this is a SUCCESS (the position is already gone), not an
# exchange error.
#
# Match is case-insensitive substring on the raw exception message.
ALREADY_CLOSED_MARKERS = (
    "empty position",
    "increase_position",
    "position does not exist",
    "position is empty",
    "no position to",
    "no position",
)

# Gate.io error labels that explicitly confirm "nothing to close"
REDUCE_ONLY_ALREADY_FLAT_LABELS = (
    "INCREASE_POSITION",
    "POSITION_NOT_FOUND",
    "POSITION_EMPTY",
)


def _looks_like_already_closed(error_message: str) -> bool:
    """
    Return True if `error_message` (str of an exchange exception) indicates
    a reduce-only order was sent for a position that no longer exists.

    This is a *success* condition for a close order, NOT a real failure.
    """
    if not error_message:
        return False
    lowered = error_message.lower()
    return any(marker in lowered for marker in ALREADY_CLOSED_MARKERS)


class GateConnection:
    """
    Gate.io Futures connection manager with ABSOLUTE fixed position enforcement.

    🔧 MULTI-SYMBOL (v18.2):
      All methods accept the standard symbol (BTC/USDT, etc.). The
      connection is a singleton; the exchange maintains one NET position
      per symbol (Gate.io one-way mode), and every check is scoped to
      the passed symbol. No per-symbol state is held here except
      caches keyed by symbol.

    🔧 PER-CALL MIN_SAFE OVERRIDE (v18.2):
      execute_trade() and its helpers accept `min_safe_balance_override`.
      When provided, it replaces the module-level global for THAT CALL
      only. This lets each engine enforce its own ratcheted floor
      without polluting other symbols.

    🔧 FIXED MODE: When FIXED_POSITION_SIZE > 0, ALL position sizes are
      calculated as: size_usdt = FIXED_POSITION_SIZE * LEVERAGE (no
      exceptions, no reductions).

    🔧 CLOSING TRADES: Always use reduce_only=True to prevent opening
      opposite positions.

    🔧 HANG FIX: Every ccxt network call is wrapped in asyncio.wait_for()
      so a stalled TCP connection or slow exchange response never
      freezes the process.

    🔧 POSITION SYNC: fetch_positions() retrieves actual open positions
      from the exchange.

    🔥 NEW: Limit order support for emergency exits with slippage
      protection.

    🆕 v10.5: reduce-only "empty position" responses are recognised as
      ALREADY-CLOSED and returned as a synthetic successful result.
    """
    _instance = None
    _exchange = None
    _markets_loaded: bool = False
    _circuit_breakers: Dict[str, Dict] = {}
    _circuit_failure_limit = 3
    _circuit_timeout_seconds = 6 * 3600   # 6 hours

    # Track margin mode per symbol to avoid redundant API calls
    _margin_mode_verified: Dict[str, bool] = {}

    # Cache for balance to avoid excessive API calls
    _cached_balance: Optional[Dict] = None
    _balance_cache_time: float = 0
    _balance_cache_ttl: int = 30           # 30 seconds

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    # ------------------------------------------------------------------
    # get_connection() — load_markets wrapped in wait_for
    # ------------------------------------------------------------------
    async def get_connection(self) -> Optional[ccxt.gateio]:
        """
        Get exchange instance if circuit is closed.

        load_markets() is wrapped in asyncio.wait_for() to prevent hangs.
        A load_markets timeout does NOT trip the global circuit breaker —
        that would lock out retries for 6 hours on a simple slow-network blip.
        """
        if self._is_circuit_open('global'):
            logger.warning("🌐 Global circuit breaker open – exchange unavailable")
            return None

        if self._exchange is None:
            try:
                exchange = ccxt.gateio({
                    'apiKey': os.getenv('GATE_API_KEY', ''),
                    'secret': os.getenv('GATE_SECRET', ''),
                    'enableRateLimit': True,
                    'timeout': API_CALL_TIMEOUT * 1000,
                    'options': {
                        'defaultType': 'swap',
                    }
                })

                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(exchange.load_markets),
                        timeout=LOAD_MARKETS_TIMEOUT
                    )
                    self._markets_loaded = True
                    logger.info("✅ Gate.io Futures markets loaded successfully")
                except asyncio.TimeoutError:
                    logger.error(
                        f"❌ load_markets() timed out after {LOAD_MARKETS_TIMEOUT}s. "
                        "Check network connectivity or Gate.io status."
                    )
                    return None

                self._exchange = exchange
                logger.info("✅ Gate.io Futures connection established")

            except Exception as e:
                logger.error(f"❌ Exchange connection failed: {e}")
                self._record_failure('global')
                self._exchange = None

        return self._exchange

    async def close_connection(self):
        """Close exchange connection."""
        if self._exchange:
            try:
                await asyncio.to_thread(self._exchange.close)
            except Exception as e:
                logger.error(f"Error closing exchange: {e}")
            finally:
                self._exchange = None
        self._markets_loaded = False
        self._circuit_breakers.clear()
        self._margin_mode_verified.clear()
        self._cached_balance = None
        logger.info("Gate.io Futures connection closed")

    # ------------------------------------------------------------------
    # Startup connectivity helper
    # ------------------------------------------------------------------
    async def check_startup_connectivity(self) -> bool:
        """
        Perform a full startup check: credentials present, markets load,
        and balance is fetchable.
        """
        gate_key    = os.getenv('GATE_API_KEY', '')
        gate_secret = os.getenv('GATE_SECRET', '')

        if not gate_key or not gate_secret:
            logger.error(
                "❌ GATE_API_KEY or GATE_SECRET missing from .env\n"
                "   Set them and restart, or use --dry-run for simulation."
            )
            return False

        print(f"🔌 Connecting to Gate.io Futures (timeout: {LOAD_MARKETS_TIMEOUT}s)...")
        exchange = await self.get_connection()
        if exchange is None:
            logger.error(
                "❌ Could not establish exchange connection.\n"
                "   • Verify GATE_API_KEY / GATE_SECRET are correct\n"
                "   • Verify network access to api.gateio.ws\n"
                "   • Use --dry-run to test without live connectivity"
            )
            return False

        print("🔌 Fetching account balance...")
        balance = await self.get_balance(use_cache=False)
        if balance is None:
            logger.error(
                "❌ Could not fetch account balance.\n"
                "   Check API key permissions (futures read access required)."
            )
            return False

        free_usdt = balance.get('USDT', 0.0)
        print(f"✅ Exchange connected. Free USDT balance: ${free_usdt:.2f}")
        return True

    # ---------- Circuit Breaker ----------
    def _is_circuit_open(self, key: str) -> bool:
        if key not in self._circuit_breakers:
            return False
        cb = self._circuit_breakers[key]
        if cb['failures'] >= self._circuit_failure_limit:
            if datetime.now() < cb['open_until']:
                return True
            else:
                del self._circuit_breakers[key]
        return False

    def _record_failure(self, key: str):
        now = datetime.now()
        if key not in self._circuit_breakers:
            self._circuit_breakers[key] = {'failures': 0, 'open_until': None}
        cb = self._circuit_breakers[key]
        cb['failures'] += 1
        if cb['failures'] >= self._circuit_failure_limit:
            cb['open_until'] = now + timedelta(seconds=self._circuit_timeout_seconds)
            logger.warning(f"⚠️ Circuit breaker opened for {key} for 6 hours")

    def _reset_circuit(self, key: str):
        if key in self._circuit_breakers:
            del self._circuit_breakers[key]
            logger.debug(f"Circuit breaker reset for {key}")

    # ---------- Symbol Formatting ----------
    def _format_futures_symbol(self, symbol: str) -> str:
        """
        Convert symbol to Gate.io Futures format.
        Gate.io linear swaps use "BTC/USDT:USDT" (base/quote:settle).
        """
        if ":" in symbol:
            return symbol
        base, quote = symbol.split('/')
        if quote == "USDT":
            return f"{base}/{quote}:{quote}"
        else:
            return f"{symbol}:USDT"

    # ---------- Balance Management with Caching ----------
    async def get_balance(self, use_cache: bool = True) -> Optional[Dict[str, float]]:
        """
        Fetch USDT balance from futures account with timeout guard.
        """
        if use_cache and self._cached_balance:
            if time.time() - self._balance_cache_time < self._balance_cache_ttl:
                return self._cached_balance.copy()

        if self._is_circuit_open('balance'):
            logger.warning("⏸️ Circuit open for balance, cannot fetch")
            return self._cached_balance if self._cached_balance else None

        exchange = await self.get_connection()
        if not exchange:
            return self._cached_balance if self._cached_balance else None

        try:
            balance = await asyncio.wait_for(
                asyncio.to_thread(exchange.fetch_balance, {'type': 'swap'}),
                timeout=BALANCE_TIMEOUT
            )
            usdt = balance.get('USDT', {})
            free = float(usdt.get('free', 0))
            total = float(usdt.get('total', 0))

            result = {'USDT': free, 'total': total}

            self._cached_balance = result
            self._balance_cache_time = time.time()

            self._reset_circuit('balance')
            return result

        except asyncio.TimeoutError:
            logger.error(f"⏱️ fetch_balance timed out after {BALANCE_TIMEOUT}s")
            self._record_failure('balance')
            return self._cached_balance if self._cached_balance else None
        except Exception as e:
            logger.error(f"Error fetching balance: {e}")
            self._record_failure('balance')
            return self._cached_balance if self._cached_balance else None

    async def get_free_usdt_balance(self, use_cache: bool = True) -> Optional[float]:
        """Get free USDT balance."""
        balance = await self.get_balance(use_cache=use_cache)
        if balance:
            return balance.get('USDT', 0.0)
        return None

    # ---------- Fixed Position Size Validation ----------
    async def _validate_fixed_position(
        self,
        symbol: str,
        current_total_margin: float = 0.0,
        max_total_margin_percent: float = 0.50,
        min_safe_balance: Optional[float] = None,      # v18.2 NEW
    ) -> Tuple[bool, str, float]:
        """
        Validate that a fixed position can be opened.

        v18.2: `min_safe_balance` lets the caller (per-symbol engine)
        pass its OWN floor. If None, the module-level global is used.
        """
        if not USE_FIXED_MODE:
            return True, "Not in fixed mode", 0.0

        free_usdt = await self.get_free_usdt_balance(use_cache=False)
        if free_usdt is None:
            return False, "Cannot fetch balance", 0.0

        effective_min = _resolve_min_safe_balance(min_safe_balance)

        if free_usdt < effective_min:
            return False, (
                f"❌ Balance ${free_usdt:.2f} below MIN_SAFE_BALANCE_USDT "
                f"(${effective_min:.2f})"
            ), 0.0

        if FIXED_POSITION_SIZE > free_usdt:
            return False, (
                f"❌ FIXED MODE: Required margin ${FIXED_POSITION_SIZE:.2f} "
                f"exceeds free balance ${free_usdt:.2f}"
            ), 0.0

        if current_total_margin > 0:
            max_total_margin = free_usdt * max_total_margin_percent
            if current_total_margin + FIXED_POSITION_SIZE > max_total_margin:
                return False, (
                    f"❌ GLOBAL MARGIN LIMIT: Used ${current_total_margin:.2f}, "
                    f"max ${max_total_margin:.2f}, cannot add ${FIXED_POSITION_SIZE:.2f}"
                ), 0.0

        current_price = await self.get_current_price(symbol)
        if not current_price:
            return False, f"Cannot get price for {symbol}", 0.0

        size_usdt = FIXED_POSITION_SIZE * LEVERAGE

        exchange = await self.get_connection()
        if exchange:
            futures_symbol = self._format_futures_symbol(symbol)
            market = exchange.market(futures_symbol)
            contract_size = market.get('contractSize') or 1
            min_amount = market['limits']['amount']['min']
            min_notional = min_amount * contract_size * current_price

            cost_limits = market.get('limits', {}).get('cost') or {}
            min_cost = cost_limits.get('min')
            if min_cost and min_cost > min_notional:
                min_notional = min_cost

            if size_usdt < min_notional:
                return False, (
                    f"❌ Fixed size ${size_usdt:.2f} notional is below this symbol's "
                    f"exchange minimum ${min_notional:.2f} notional "
                    f"(amount min {min_amount} × contractSize {contract_size}"
                    f"{f', cost min {min_cost}' if min_cost else ''} @ ${current_price:.6f}). "
                    f"Skipping trade instead of oversizing."
                ), 0.0

        logger.debug(
            f"✅ Fixed mode validation PASSED: ${FIXED_POSITION_SIZE:.2f} margin → "
            f"${size_usdt:.2f} notional @ {LEVERAGE}x | Free: ${free_usdt:.2f}"
        )

        return True, "OK", size_usdt

    # ---------- Margin Validation ----------
    async def validate_margin_usage(
        self,
        size_usdt: float,
        leverage: int,
        current_total_margin: float = 0.0,
        max_total_margin_percent: float = 0.50,
        min_safe_balance: Optional[float] = None,      # v18.2 NEW
    ) -> Tuple[bool, str, float]:
        """
        Enforce margin limits.

        v18.2: `min_safe_balance` lets the caller (per-symbol engine)
        pass its OWN floor. If None, the module-level global is used.
        """
        free_usdt = await self.get_free_usdt_balance(use_cache=False)

        if free_usdt is None:
            return False, "Cannot get balance - margin validation failed", 0.0

        if free_usdt <= 0:
            return False, f"Balance is ${free_usdt:.2f} - insufficient funds", 0.0

        effective_min = _resolve_min_safe_balance(min_safe_balance)

        if free_usdt < effective_min:
            return False, (
                f"❌ Balance ${free_usdt:.2f} below MIN_SAFE_BALANCE_USDT "
                f"(${effective_min:.2f})"
            ), 0.0

        required_margin = size_usdt / leverage
        use_fixed_mode = USE_FIXED_MODE

        if use_fixed_mode:
            if required_margin > free_usdt:
                return False, (
                    f"❌ FIXED MODE: Required margin ${required_margin:.2f} "
                    f"exceeds free balance ${free_usdt:.2f}"
                ), 0.0
            max_per_position_notional = required_margin * leverage
            logger.debug(
                f"🔒 Fixed mode margin check PASSED: ${required_margin:.2f} margin "
                f"(fixed size mode, 1% rule DISABLED)"
            )
        else:
            max_per_position_margin = free_usdt * 0.01
            max_per_position_notional = max_per_position_margin * leverage

            if required_margin > max_per_position_margin * 1.01:
                return False, (
                    f"❌ 1% RULE VIOLATION: Required margin ${required_margin:.2f} "
                    f"(size ${size_usdt:.2f} @ {leverage}x) > 1% of balance (${max_per_position_margin:.2f})"
                ), max_per_position_notional

            margin_percent = (required_margin / free_usdt) * 100
            logger.info(
                f"🔒 Margin check PASSED: ${required_margin:.2f} margin "
                f"({margin_percent:.2f}% of ${free_usdt:.2f} free USDT) | "
                f"Notional: ${size_usdt:.2f} @ {leverage}x"
            )

        # Global total margin limit (per-symbol; the engine also enforces
        # an account-wide check before calling us)
        if current_total_margin > 0:
            new_total_margin = current_total_margin + required_margin
            max_total_margin = free_usdt * max_total_margin_percent

            if new_total_margin > max_total_margin * 1.01:
                return False, (
                    f"❌ GLOBAL MARGIN LIMIT: New total ${new_total_margin:.2f} "
                    f"exceeds ${max_total_margin:.2f} ({max_total_margin_percent*100:.0f}% of balance)"
                ), 0.0

        max_allowed = (required_margin if use_fixed_mode else free_usdt * 0.01) * leverage
        return True, "OK", max_allowed

    async def get_safe_position_size(
        self,
        leverage: int,
        current_total_margin: float = 0.0,
        max_total_margin_percent: float = 0.50,
        min_safe_balance: Optional[float] = None,      # v18.2 NEW
    ) -> Tuple[float, str]:
        """Calculate maximum safe position size in USDT (notional)."""
        free_usdt = await self.get_free_usdt_balance(use_cache=False)

        if free_usdt is None:
            return 0.0, "Cannot get balance"
        if free_usdt <= 0:
            return 0.0, f"Balance is ${free_usdt:.2f}"

        effective_min = _resolve_min_safe_balance(min_safe_balance)

        if free_usdt < effective_min:
            return 0.0, f"Balance ${free_usdt:.2f} below minimum ${effective_min:.2f}"

        use_fixed_mode = USE_FIXED_MODE

        if use_fixed_mode:
            fixed_margin = FIXED_POSITION_SIZE

            if fixed_margin > free_usdt:
                return 0.0, f"Fixed margin ${fixed_margin:.2f} exceeds balance ${free_usdt:.2f}"

            max_total_margin = free_usdt * max_total_margin_percent
            if current_total_margin + fixed_margin > max_total_margin:
                remaining = max_total_margin - current_total_margin
                if remaining <= 0:
                    return 0.0, f"No margin remaining (used ${current_total_margin:.2f} of ${max_total_margin:.2f})"
                if remaining < fixed_margin:
                    fixed_margin = remaining

            allowed_notional = fixed_margin * leverage
            logger.info(
                f"📊 Fixed position size: ${allowed_notional:.2f} USDT notional "
                f"(margin ${fixed_margin:.2f} fixed, {leverage}x leverage)"
            )
            return allowed_notional, "OK (fixed mode)"
        else:
            max_per_position_margin = free_usdt * 0.01
            max_per_position_notional = max_per_position_margin * leverage

            max_total_margin = free_usdt * max_total_margin_percent
            remaining_margin = max_total_margin - current_total_margin

            if remaining_margin <= 0:
                return 0.0, f"No margin remaining (used ${current_total_margin:.2f} of ${max_total_margin:.2f})"

            allowed_margin = min(max_per_position_margin, remaining_margin)
            allowed_notional = allowed_margin * leverage

            logger.info(
                f"📊 Safe position size: ${allowed_notional:.2f} USDT notional "
                f"(margin ${allowed_margin:.2f} = 1% of ${free_usdt:.2f}, "
                f"remaining margin ${remaining_margin:.2f})"
            )
            return allowed_notional, "OK"

    # ---------- Cross Margin Enforcement ----------
    async def _ensure_cross_margin(self, symbol: str, leverage: int) -> bool:
        """Configure cross margin + leverage for a symbol via Gate.io CCXT."""
        exchange = await self.get_connection()
        if not exchange:
            return False

        futures_symbol = self._format_futures_symbol(symbol)
        cache_key = f"{futures_symbol}:{leverage}"

        if self._margin_mode_verified.get(cache_key, False):
            return True

        try:
            await asyncio.wait_for(
                asyncio.to_thread(
                    exchange.set_margin_mode,
                    'cross',
                    futures_symbol
                ),
                timeout=API_CALL_TIMEOUT
            )
            logger.info(f"✅ Cross margin set for {futures_symbol}")

            await asyncio.wait_for(
                asyncio.to_thread(
                    exchange.set_leverage,
                    leverage,
                    futures_symbol
                ),
                timeout=API_CALL_TIMEOUT
            )
            logger.info(f"✅ Leverage {leverage}x set for {futures_symbol}")

        except asyncio.TimeoutError:
            logger.warning(
                f"⏱️ set_margin_mode/set_leverage timed out for {futures_symbol} after {API_CALL_TIMEOUT}s "
                f"– cross margin will still be enforced via order params"
            )
        except Exception as e:
            logger.warning(
                f"⚠️ set_margin_mode/set_leverage warning for {futures_symbol}: {e} "
                f"– cross margin will still be enforced via order params"
            )

        self._margin_mode_verified[cache_key] = True
        return True

    # ---------- Price Methods ----------
    async def get_current_price(self, symbol: str) -> Optional[float]:
        """Get current mark price with timeout guard."""
        if self._is_circuit_open(symbol):
            logger.warning(f"⏸️ Circuit open for {symbol}, cannot fetch price")
            return None

        exchange = await self.get_connection()
        if not exchange:
            logger.warning(f"No exchange connection for {symbol}")
            return None

        futures_symbol = self._format_futures_symbol(symbol)

        try:
            ticker = await asyncio.wait_for(
                asyncio.to_thread(exchange.fetch_ticker, futures_symbol),
                timeout=API_CALL_TIMEOUT
            )
            price = ticker.get('last') or ticker.get('mark')
            if price is None or price <= 0:
                raise ValueError(f"Invalid price {price}")
            self._reset_circuit(symbol)
            return float(price)
        except asyncio.TimeoutError:
            logger.error(f"⏱️ fetch_ticker timed out for {symbol} after {API_CALL_TIMEOUT}s")
            self._record_failure(symbol)
            return None
        except Exception as e:
            logger.error(f"Error fetching price for {symbol}: {e}")
            self._record_failure(symbol)
            return None

    # ---------- OHLCV Data ----------

    # MT5-style timeframe labels → CCXT interval strings
    _MT5_TO_CCXT_TIMEFRAME = {
        'M1': '1m', 'M5': '5m', 'M15': '15m', 'M30': '30m',
        'H1': '1h', 'H4': '4h', 'D1': '1d', 'W1': '1w', 'MN1': '1M',
    }

    def _normalize_timeframe(self, timeframe: str) -> str:
        normalized = self._MT5_TO_CCXT_TIMEFRAME.get(timeframe, timeframe)
        if normalized != timeframe:
            logger.debug(f"fetch_ohlcv: normalized MT5-style timeframe '{timeframe}' -> '{normalized}'")
        return normalized

    async def fetch_ohlcv(self, symbol: str, timeframe: str = '1h', limit: int = 200) -> Optional[List]:
        """Fetch OHLCV data with timeout guard."""
        if self._is_circuit_open(symbol):
            logger.warning(f"⏸️ Circuit open for {symbol}, cannot fetch OHLCV")
            return None

        exchange = await self.get_connection()
        if not exchange:
            logger.warning(f"No exchange connection for {symbol}")
            return None

        futures_symbol = self._format_futures_symbol(symbol)
        ccxt_timeframe = self._normalize_timeframe(timeframe)

        try:
            ohlcv = await asyncio.wait_for(
                asyncio.to_thread(
                    exchange.fetch_ohlcv,
                    futures_symbol, ccxt_timeframe, limit=limit
                ),
                timeout=OHLCV_TIMEOUT
            )
            if not ohlcv or len(ohlcv) < 50:
                logger.debug(f"Insufficient data for {symbol} ({len(ohlcv) if ohlcv else 0} bars)")
                return ohlcv
            self._reset_circuit(symbol)
            return ohlcv
        except asyncio.TimeoutError:
            logger.error(f"⏱️ fetch_ohlcv timed out for {symbol} ({ccxt_timeframe}) after {OHLCV_TIMEOUT}s")
            self._record_failure(symbol)
            return None
        except Exception as e:
            error_msg = str(e)
            if "not enough data" in error_msg.lower() or "no data" in error_msg.lower():
                logger.debug(f"fetch_ohlcv: insufficient data for {symbol} ({ccxt_timeframe}) - {error_msg}")
            else:
                logger.error(f"Error fetching OHLCV for {symbol} (timeframe='{timeframe}' -> '{ccxt_timeframe}'): {e}")
            self._record_failure(symbol)
            return None

    # ---------- Fetch Positions ----------
    async def fetch_positions(self, symbols: List[str] = None) -> Optional[Dict[str, Dict]]:
        """
        Fetch all open positions from Gate.io Futures.

        Returns:
            dict: {symbol: {'side', 'size', 'entry_price', 'leverage',
                            'unrealized_pnl', 'liquidation_price', 'margin'}}
            or empty dict if no positions.
            Returns None on error.

        NOTE: Gate.io Futures runs in one-way (net) position mode by
        default, so at most one entry per symbol is returned.
        """
        if self._is_circuit_open('positions'):
            logger.warning("⏸️ Circuit open for positions, cannot fetch")
            return None

        exchange = await self.get_connection()
        if not exchange:
            logger.warning("No exchange connection to fetch positions")
            return None

        try:
            positions = await asyncio.wait_for(
                asyncio.to_thread(exchange.fetch_positions, symbols),
                timeout=API_CALL_TIMEOUT
            )

            result = {}
            for pos in positions:
                symbol = pos.get('symbol')
                if not symbol:
                    continue

                if symbol.endswith(':USDT'):
                    clean_symbol = symbol[:-6]
                else:
                    clean_symbol = symbol

                side = pos.get('side', '')
                contracts = pos.get('contracts')
                if contracts is None:
                    contracts = 0.0
                else:
                    try:
                        contracts = float(contracts)
                    except (ValueError, TypeError):
                        contracts = 0.0

                if side in ('long', 'short') and contracts > 0:
                    entry_price = pos.get('entryPrice')
                    if entry_price is None:
                        entry_price = 0.0
                    else:
                        try:
                            entry_price = float(entry_price)
                        except (ValueError, TypeError):
                            entry_price = 0.0

                    leverage = pos.get('leverage')
                    if leverage is None:
                        leverage = 0
                    else:
                        try:
                            leverage = int(leverage)
                        except (ValueError, TypeError):
                            leverage = 0

                    unrealized_pnl = pos.get('unrealizedPnl')
                    if unrealized_pnl is None:
                        unrealized_pnl = 0.0
                    else:
                        try:
                            unrealized_pnl = float(unrealized_pnl)
                        except (ValueError, TypeError):
                            unrealized_pnl = 0.0

                    liquidation_price = pos.get('liquidationPrice')
                    if liquidation_price is None:
                        liquidation_price = 0.0
                    else:
                        try:
                            liquidation_price = float(liquidation_price)
                        except (ValueError, TypeError):
                            liquidation_price = 0.0

                    margin = pos.get('initialMargin')
                    if margin is None:
                        margin = 0.0
                    else:
                        try:
                            margin = float(margin)
                        except (ValueError, TypeError):
                            margin = 0.0

                    result[clean_symbol] = {
                        'side': side,
                        'size': contracts,
                        'entry_price': entry_price,
                        'leverage': leverage,
                        'unrealized_pnl': unrealized_pnl,
                        'liquidation_price': liquidation_price,
                        'margin': margin,
                    }

            self._reset_circuit('positions')
            return result

        except asyncio.TimeoutError:
            logger.error(f"⏱️ fetch_positions timed out after {API_CALL_TIMEOUT}s")
            self._record_failure('positions')
            return None
        except Exception as e:
            logger.error(f"Error fetching positions: {e}")
            self._record_failure('positions')
            return None

    # ---------- Trade Execution ----------
    async def execute_trade(
        self,
        symbol: str,
        side: str,
        size_usdt: float,
        leverage: int = 10,
        dry_run: bool = False,
        is_closing: bool = False,
        take_profit_price: Optional[float] = None,
        current_total_margin: float = 0.0,
        max_total_margin_percent: float = 0.50,
        reduce_only: bool = False,
        limit_price: Optional[float] = None,
        strategy_id: Optional[str] = None,
        strategy_name: Optional[str] = None,
        min_safe_balance_override: Optional[float] = None,   # v18.2 NEW
    ) -> Optional[Dict]:
        """
        Execute a market or limit order on Gate.io Futures.

        v18.2 NEW:
            min_safe_balance_override : Optional per-call MIN_SAFE floor.
                When provided, replaces the module-level global for THIS
                CALL ONLY. This is how each per-symbol engine enforces
                its own ratcheted floor without contaminating other
                symbols. If None, the module-level global is used
                (backward compatible with single-symbol pre-v18.2 callers).

        Other semantics (unchanged):
            • FIXED MODE: size_usdt IGNORED for opens, replaced with
              FIXED_POSITION_SIZE * LEVERAGE.
            • CLOSING TRADES (is_closing=True) SKIP all margin validation.
            • MIN_SAFE_BALANCE enforced before every trade.
            • limit_price enables LIMIT order w/ MARKET fallback.
            • reduce-only "empty position" responses returned as
              ALREADY-CLOSED success (synthetic result).
        """
        # ================================================================
        # Resolve effective MIN_SAFE for this call
        # ================================================================
        effective_min_safe = _resolve_min_safe_balance(min_safe_balance_override)

        # ================================================================
        # STRICT: Check MIN_SAFE before ANY trade
        # ================================================================
        free_usdt = await self.get_free_usdt_balance(use_cache=False)
        if free_usdt is not None and free_usdt < effective_min_safe:
            logger.error(
                f"❌ Balance ${free_usdt:.2f} below MIN_SAFE_BALANCE_USDT "
                f"(${effective_min_safe:.2f}). Trade rejected."
            )
            return None

        # ================================================================
        # FIXED MODE: Override size_usdt for opening trades
        # ================================================================
        use_fixed_mode = USE_FIXED_MODE and not is_closing

        if use_fixed_mode:
            is_valid, reason, calculated_size = await self._validate_fixed_position(
                symbol, current_total_margin, max_total_margin_percent,
                min_safe_balance=effective_min_safe,
            )
            if not is_valid:
                logger.error(f"❌ Fixed position validation FAILED for {symbol}: {reason}")
                return None
            size_usdt = calculated_size
            actual_margin = size_usdt / leverage
            logger.debug(
                f"🔧 FIXED MODE: Using {FIXED_POSITION_SIZE} USDT margin → "
                f"${size_usdt:.2f} USDT notional"
            )
        else:
            actual_margin = size_usdt / leverage

        # ================================================================
        # DRY RUN MODE
        # ================================================================
        if dry_run:
            action = "close" if is_closing else "open"
            direction = "LONG" if side == "buy" else "SHORT"
            tp_msg = f" TP@{take_profit_price:.2f}" if take_profit_price and not is_closing else ""
            ro_msg = " [REDUCE_ONLY]" if reduce_only else ""
            mode_str = "FIXED" if use_fixed_mode else "DYNAMIC"
            margin_str = f" | Margin: ${actual_margin:.2f}" if not is_closing else ""
            strat_msg = f" | Strategy: {strategy_id}" if strategy_id else ""
            limit_msg = f" [LIMIT @ ${limit_price:.2f}]" if limit_price else " [MARKET]"

            if not is_closing and not use_fixed_mode:
                free_usdt_check = await self.get_free_usdt_balance()
                if free_usdt_check:
                    required_margin = size_usdt / leverage
                    max_allowed = free_usdt_check * 0.01
                    if required_margin > max_allowed * 1.01:
                        logger.error(
                            f"[DRY RUN] ❌ Would REJECT {direction} ${size_usdt:.2f} - "
                            f"margin ${required_margin:.2f} > 1% (${max_allowed:.2f})"
                        )
                        return None

            logger.info(
                f"[DRY RUN] Would {action} {direction} ${size_usdt:.2f} of {symbol} "
                f"({leverage}x){tp_msg}{ro_msg}{margin_str} [{mode_str}]{limit_msg}{strat_msg}"
            )
            return {
                'id': f'dry_{int(time.time() * 1000)}',
                'side': side,
                'size': size_usdt,
                'price': None,
                'dry_run': True,
                'margin_used': actual_margin if not is_closing else 0,
                'strategy_id': strategy_id,
                'strategy_name': strategy_name,
            }

        # ================================================================
        # CIRCUIT BREAKER CHECK
        # ================================================================
        if self._is_circuit_open(symbol):
            logger.warning(f"⏸️ Circuit open for {symbol}, cannot trade")
            return None

        exchange = await self.get_connection()
        if not exchange:
            logger.error(f"No exchange connection – cannot execute trade for {symbol}")
            return None

        futures_symbol = self._format_futures_symbol(symbol)

        try:
            # ------------------------------------------------------------
            # 1. Get current price
            # ------------------------------------------------------------
            ticker = await asyncio.wait_for(
                asyncio.to_thread(exchange.fetch_ticker, futures_symbol),
                timeout=API_CALL_TIMEOUT
            )
            current_price = ticker.get('last') or ticker.get('mark')
            if not current_price or current_price <= 0:
                raise ValueError("No valid price")

            # ------------------------------------------------------------
            # 2. Margin validation — SKIP for closing trades
            # ------------------------------------------------------------
            if not is_closing:
                is_valid, reason, max_safe_size = await self.validate_margin_usage(
                    size_usdt=size_usdt,
                    leverage=leverage,
                    current_total_margin=current_total_margin,
                    max_total_margin_percent=max_total_margin_percent,
                    min_safe_balance=effective_min_safe,
                )
                if not is_valid:
                    logger.error(f"❌ Margin validation FAILED for {symbol}: {reason}")
                    return None
                if size_usdt > max_safe_size and max_safe_size > 0 and not use_fixed_mode:
                    logger.warning(
                        f"⚠️ Adjusting position size from ${size_usdt:.2f} to "
                        f"${max_safe_size:.2f} to respect margin limits"
                    )
                    size_usdt = max_safe_size
                    actual_margin = size_usdt / leverage
            else:
                logger.debug(f"🔓 CLOSING TRADE: Skipping margin validation for {symbol}")

            # ------------------------------------------------------------
            # 3. Enforce cross margin — skip for closing trades
            # ------------------------------------------------------------
            if not is_closing:
                cross_margin_ok = await self._ensure_cross_margin(symbol, leverage)
                if not cross_margin_ok:
                    logger.error(f"❌ Failed to set cross margin for {futures_symbol} - aborting")
                    return None
                logger.info(f"🔒 Cross margin enforced for {futures_symbol} at {leverage}x")
            else:
                logger.debug(f"🔓 CLOSING TRADE: Skipping cross margin setup for {futures_symbol}")

            # ------------------------------------------------------------
            # 4. Calculate contract amount
            # ------------------------------------------------------------
            market = exchange.market(futures_symbol)
            contract_size = market.get('contractSize') or 1
            amount_base = size_usdt / (current_price * contract_size)
            min_amount = market['limits']['amount']['min']

            cost_limits = market.get('limits', {}).get('cost') or {}
            min_cost = cost_limits.get('min')
            if min_cost and size_usdt < min_cost:
                logger.error(
                    f"❌ {symbol}: requested ${size_usdt:.2f} notional is below "
                    f"Gate.io's min notional ${min_cost:.2f}. "
                    f"Skipping trade instead of oversizing."
                )
                self._record_failure(symbol)
                return None

            if amount_base < min_amount:
                min_notional_here = min_amount * contract_size * current_price
                logger.error(
                    f"❌ {symbol}: requested {amount_base:.8f} contracts "
                    f"(${size_usdt:.2f} notional) is below exchange minimum "
                    f"{min_amount} contracts (${min_notional_here:.2f} notional). "
                    f"Skipping trade instead of oversizing."
                )
                self._record_failure(symbol)
                return None

            # ------------------------------------------------------------
            # 5. Build order params
            # ------------------------------------------------------------
            params = {
                'marginMode': 'cross',
                'leverage': leverage,
            }
            if reduce_only or is_closing:
                params['reduce_only'] = True
                logger.debug(f"🔒 Setting reduce_only=True for {symbol}")

            # ------------------------------------------------------------
            # 6. Place order — MARKET or LIMIT
            # ------------------------------------------------------------
            order = None
            order_type = "LIMIT" if limit_price else "MARKET"
            logger.info(f"📊 Placing {order_type} order for {symbol}: {side} {amount_base:.8f} contracts @ {'$' + str(limit_price) if limit_price else 'MARKET'}")

            try:
                if limit_price:
                    order = await asyncio.wait_for(
                        asyncio.to_thread(
                            exchange.create_limit_order,
                            futures_symbol, side, amount_base, limit_price,
                            params=params
                        ),
                        timeout=API_CALL_TIMEOUT
                    )
                    logger.info(f"✅ LIMIT order placed: {side} {amount_base:.8f} @ ${limit_price:.2f}")

                    if order and order.get('status') == 'open':
                        logger.info(f"⏳ Limit order pending: {order.get('id')} - waiting for fill...")
                        await asyncio.sleep(EMERGENCY_LIMIT_WAIT_SECONDS)
                        try:
                            updated_order = await asyncio.wait_for(
                                asyncio.to_thread(exchange.fetch_order, order['id'], futures_symbol),
                                timeout=API_CALL_TIMEOUT
                            )
                            if updated_order and updated_order.get('status') == 'open':
                                logger.warning(f"⚠️ Limit order still open after {EMERGENCY_LIMIT_WAIT_SECONDS}s - falling back to market")
                                logger.info(f"🔄 Cancelling limit order and falling back to MARKET...")
                                await asyncio.wait_for(
                                    asyncio.to_thread(exchange.cancel_order, order['id'], futures_symbol),
                                    timeout=API_CALL_TIMEOUT
                                )
                                order = await asyncio.wait_for(
                                    asyncio.to_thread(
                                        exchange.create_market_order,
                                        futures_symbol, side, amount_base,
                                        params=params
                                    ),
                                    timeout=API_CALL_TIMEOUT
                                )
                                logger.info(f"✅ MARKET order placed as fallback: {side} {amount_base:.8f} @ MARKET")
                        except Exception as e:
                            logger.warning(f"⚠️ Error checking limit order status: {e}")
                else:
                    order = await asyncio.wait_for(
                        asyncio.to_thread(
                            exchange.create_market_order,
                            futures_symbol, side, amount_base,
                            params=params
                        ),
                        timeout=API_CALL_TIMEOUT
                    )
                    logger.info(f"✅ MARKET order placed: {side} {amount_base:.8f} @ MARKET")

            except Exception as order_exc:
                error_msg = str(order_exc)

                if (reduce_only or is_closing) and _looks_like_already_closed(error_msg):
                    logger.info(
                        f"✅ {symbol}: reduce-only {side} treated as ALREADY CLOSED "
                        f"(exchange reports no position to reduce): {error_msg}"
                    )
                    self._cached_balance = None
                    return {
                        'id': f'already_closed_{int(time.time() * 1000)}',
                        'side': side,
                        'size': size_usdt,
                        'price': None,
                        'already_closed': True,
                        'margin_used': 0,
                        'order_type': 'ALREADY_CLOSED',
                        'limit_price': None,
                        'strategy_id': strategy_id,
                        'strategy_name': strategy_name,
                    }

                raise

            if not order or not order.get('id'):
                logger.error(f"❌ Order placement failed for {symbol}")
                self._record_failure(symbol)
                return None

            direction = "LONG" if side == "buy" else "SHORT"
            action = "closed" if is_closing else "opened"
            mode_str = "FIXED" if use_fixed_mode else "1% RULE"
            ro_str = " [REDUCE_ONLY]" if (reduce_only or is_closing) else ""
            order_type_str = "LIMIT" if limit_price else "MARKET"
            strat_msg = f" [Strategy: {strategy_id}]" if strategy_id else ""

            margin_percent = 0.0
            if not is_closing:
                free_usdt2 = await self.get_free_usdt_balance()
                margin_percent = (actual_margin / free_usdt2 * 100) if free_usdt2 and free_usdt2 > 0 else 0
                margin_info = f" | Margin: ${actual_margin:.2f} ({margin_percent:.2f}% of balance) [{mode_str}]"
            else:
                margin_info = ""

            logger.info(
                f"✅ Position {action}: {direction} {amount_base:.8f} contracts of {symbol} "
                f"(${size_usdt:.2f} notional) at ~${current_price:.2f} ({leverage}x cross){ro_str}{margin_info} [{order_type_str}]{strat_msg}"
            )

            # ------------------------------------------------------------
            # 7. Take-profit as separate reduce-only limit order
            # ------------------------------------------------------------
            tp_order_id = None
            if take_profit_price and not is_closing and take_profit_price > 0:
                try:
                    close_side = 'sell' if side == 'buy' else 'buy'
                    filled_amount = order.get('filled') or order.get('amount') or amount_base
                    tp_params = {
                        'reduce_only': True,
                        'timeInForce': 'GTC',
                        'marginMode': 'cross',
                        'leverage': leverage,
                    }
                    tp_order = await asyncio.wait_for(
                        asyncio.to_thread(
                            exchange.create_limit_order,
                            futures_symbol, close_side, filled_amount, take_profit_price,
                            params=tp_params
                        ),
                        timeout=API_CALL_TIMEOUT
                    )
                    tp_order_id = tp_order.get('id')
                    logger.info(
                        f"🎯 Take-profit order placed: {close_side.upper()} "
                        f"{filled_amount:.8f} {symbol} @ ${take_profit_price:.4f} "
                        f"(order id: {tp_order_id})"
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        f"⏱️ Take-profit placement timed out for {symbol} – "
                        "position is open but TP must be set manually"
                    )
                except Exception as tp_error:
                    logger.error(
                        f"⚠️ Take-profit placement failed for {symbol}: {tp_error} – "
                        "position is open but TP must be set manually"
                    )

            # ------------------------------------------------------------
            # 8. Return result
            # ------------------------------------------------------------
            self._reset_circuit(symbol)
            self._cached_balance = None

            return {
                'id': order['id'],
                'side': side,
                'size': size_usdt,
                'price': current_price,
                'amount_base': amount_base,
                'margin_used': actual_margin if not is_closing else 0,
                'margin_percent': margin_percent if not is_closing else 0,
                'take_profit': take_profit_price if not is_closing else None,
                'tp_order_id': tp_order_id if not is_closing else None,
                'order_type': order_type_str,
                'limit_price': limit_price if limit_price else None,
                'strategy_id': strategy_id,
                'strategy_name': strategy_name,
            }

        except asyncio.TimeoutError:
            logger.error(f"⏱️ execute_trade({symbol}) timed out after {API_CALL_TIMEOUT}s")
            self._record_failure(symbol)
            return None
        except Exception as e:
            error_msg = str(e)

            if (reduce_only or is_closing) and _looks_like_already_closed(error_msg):
                logger.info(
                    f"✅ {symbol}: reduce-only {side} treated as ALREADY CLOSED "
                    f"(outer handler): {error_msg}"
                )
                self._cached_balance = None
                return {
                    'id': f'already_closed_{int(time.time() * 1000)}',
                    'side': side,
                    'size': size_usdt,
                    'price': None,
                    'already_closed': True,
                    'margin_used': 0,
                    'order_type': 'ALREADY_CLOSED',
                    'limit_price': None,
                    'strategy_id': strategy_id,
                    'strategy_name': strategy_name,
                }

            if "INSUFFICIENT_AVAILABLE" in error_msg or "insufficient" in error_msg.lower():
                logger.error(
                    f"❌ INSUFFICIENT BALANCE for {symbol}: {error_msg}\n"
                    f"   Requested: ${size_usdt:.2f} notional (${actual_margin:.2f} margin @ {leverage}x)\n"
                    f"   Check your free USDT balance."
                )
            else:
                logger.error(f"❌ Trade execution error for {symbol}: {error_msg}")
            self._record_failure(symbol)
            return None

    # ---------- Cancel Take-Profit ----------
    async def cancel_take_profit(self, symbol: str, position_id: str) -> bool:
        """Cancel an existing take-profit order for a position."""
        if self._is_circuit_open(symbol):
            logger.warning(f"⏸️ Circuit open for {symbol}, cannot cancel TP")
            return False

        exchange = await self.get_connection()
        if not exchange:
            return False

        futures_symbol = self._format_futures_symbol(symbol)

        try:
            open_orders = await asyncio.wait_for(
                asyncio.to_thread(exchange.fetch_open_orders, futures_symbol),
                timeout=API_CALL_TIMEOUT
            )
            tp_orders = [
                o for o in open_orders
                if o.get('reduce_only') is True
            ]

            cancelled = 0
            for tp_order in tp_orders:
                try:
                    await asyncio.wait_for(
                        asyncio.to_thread(exchange.cancel_order, tp_order['id'], futures_symbol),
                        timeout=API_CALL_TIMEOUT
                    )
                    cancelled += 1
                    logger.info(f"✅ Cancelled take-profit order {tp_order['id']} for {symbol}")
                except Exception as e:
                    logger.warning(f"⚠️ Could not cancel TP order {tp_order['id']}: {e}")

            if cancelled > 0:
                return True
            else:
                logger.debug(f"No take-profit orders found for {symbol} position {position_id}")
                return True

        except asyncio.TimeoutError:
            logger.error(f"⏱️ cancel_take_profit timed out for {symbol}")
            return False
        except Exception as e:
            logger.error(f"Error cancelling take-profit for {symbol}: {e}")
            return False

    async def fetch_closing_fill_price(
        self,
        symbol: str,
        since_ts: float,
        min_qty: float = 0.0,
        closing_side: str = 'sell',
    ) -> Optional[Tuple[float, float]]:
        """
        Look up Gate.io's trade history to recover the fill price of a
        reduce-only closing order. Used for out-of-band TP fills.
        """
        if self._is_circuit_open(symbol):
            logger.warning(f"⏸️ Circuit open for {symbol}, cannot fetch closing fills")
            return None

        exchange = await self.get_connection()
        if not exchange:
            return None

        futures_symbol = self._format_futures_symbol(symbol)
        since_ms = int(max(0.0, since_ts - 60) * 1000)

        try:
            trades = await asyncio.wait_for(
                asyncio.to_thread(exchange.fetch_my_trades, futures_symbol, since_ms),
                timeout=API_CALL_TIMEOUT
            )
        except asyncio.TimeoutError:
            logger.error(f"⏱️ fetch_closing_fill_price timed out for {symbol}")
            return None
        except Exception as e:
            logger.error(f"Error fetching my_trades for {symbol}: {e}")
            return None

        if not trades:
            return None

        closing_trades = []
        for t in trades:
            side = (t.get('side') or '').lower()
            if side != closing_side:
                continue
            info = t.get('info', {}) or {}
            reduce_only = info.get('reduce_only')
            if reduce_only is not None and str(reduce_only).lower() not in ('true', '1'):
                continue
            closing_trades.append(t)

        if not closing_trades:
            return None

        total_qty = 0.0
        total_notional = 0.0
        for t in closing_trades:
            try:
                qty = float(t.get('amount') or 0.0)
                price = float(t.get('price') or 0.0)
            except (TypeError, ValueError):
                continue
            if qty <= 0 or price <= 0:
                continue
            total_qty += qty
            total_notional += qty * price

        if total_qty <= 0 or total_qty < min_qty:
            return None

        vwap_price = total_notional / total_qty
        return vwap_price, total_qty


# ---------- Singleton instance ----------
_gate_conn = None


def get_exchange_connection() -> GateConnection:
    """Get or create the singleton GateConnection instance."""
    global _gate_conn
    if _gate_conn is None:
        _gate_conn = GateConnection()
    return _gate_conn


# ============================================================================
# Module-level convenience wrappers (all accept min_safe_balance_override)
# ============================================================================

async def get_current_price(symbol: str) -> Optional[float]:
    """Get current price for a symbol."""
    conn = get_exchange_connection()
    return await conn.get_current_price(symbol)


async def fetch_ohlcv(symbol: str, timeframe: str = '1h', limit: int = 200) -> Optional[List]:
    """Fetch OHLCV data for a symbol."""
    conn = get_exchange_connection()
    return await conn.fetch_ohlcv(symbol, timeframe, limit)


async def execute_trade(
    symbol: str,
    side: str,
    size_usdt: float,
    dry_run: bool = False,
    leverage: int = 10,
    is_closing: bool = False,
    take_profit_price: Optional[float] = None,
    current_total_margin: float = 0.0,
    max_total_margin_percent: float = 0.50,
    reduce_only: bool = False,
    limit_price: Optional[float] = None,
    strategy_id: Optional[str] = None,
    strategy_name: Optional[str] = None,
    min_safe_balance_override: Optional[float] = None,   # v18.2 NEW
) -> Optional[Dict]:
    """
    Execute a trade on Gate.io Futures.

    v18.2: min_safe_balance_override lets the caller (per-symbol engine)
    pass its own ratcheted MIN_SAFE floor for this call.
    """
    conn = get_exchange_connection()
    return await conn.execute_trade(
        symbol=symbol,
        side=side,
        size_usdt=size_usdt,
        leverage=leverage,
        dry_run=dry_run,
        is_closing=is_closing,
        take_profit_price=take_profit_price,
        current_total_margin=current_total_margin,
        max_total_margin_percent=max_total_margin_percent,
        reduce_only=reduce_only,
        limit_price=limit_price,
        strategy_id=strategy_id,
        strategy_name=strategy_name,
        min_safe_balance_override=min_safe_balance_override,
    )


async def set_cross_margin(symbol: str, leverage: int = 10) -> bool:
    """Explicitly set cross margin mode for a symbol."""
    conn = get_exchange_connection()
    return await conn._ensure_cross_margin(symbol, leverage)


async def cancel_take_profit(symbol: str, position_id: str) -> bool:
    """Cancel an existing take-profit order."""
    conn = get_exchange_connection()
    return await conn.cancel_take_profit(symbol, position_id)


async def fetch_closing_fill_price(
    symbol: str, since_ts: float, min_qty: float = 0.0, closing_side: str = 'sell'
) -> Optional[Tuple[float, float]]:
    """Recover the fill price of a reduce-only closing order."""
    conn = get_exchange_connection()
    return await conn.fetch_closing_fill_price(
        symbol, since_ts, min_qty=min_qty, closing_side=closing_side
    )


async def get_balance(use_cache: bool = True) -> Optional[Dict[str, float]]:
    """Get USDT balance from futures account."""
    conn = get_exchange_connection()
    return await conn.get_balance(use_cache=use_cache)


async def get_free_usdt_balance(use_cache: bool = True) -> Optional[float]:
    """Get free USDT balance from futures account."""
    conn = get_exchange_connection()
    return await conn.get_free_usdt_balance(use_cache=use_cache)


async def fetch_positions(symbols: List[str] = None) -> Optional[Dict[str, Dict]]:
    """Fetch all open positions from Gate.io Futures."""
    conn = get_exchange_connection()
    return await conn.fetch_positions(symbols)


async def validate_margin_before_trade(
    size_usdt: float,
    leverage: int,
    current_total_margin: float = 0.0,
    max_total_margin_percent: float = 0.50,
    min_safe_balance: Optional[float] = None,      # v18.2 NEW
) -> Tuple[bool, str, float]:
    """Validate margin before attempting a trade."""
    conn = get_exchange_connection()
    return await conn.validate_margin_usage(
        size_usdt=size_usdt,
        leverage=leverage,
        current_total_margin=current_total_margin,
        max_total_margin_percent=max_total_margin_percent,
        min_safe_balance=min_safe_balance,
    )


async def get_safe_position_size(
    leverage: int,
    current_total_margin: float = 0.0,
    max_total_margin_percent: float = 0.50,
    min_safe_balance: Optional[float] = None,      # v18.2 NEW
) -> Tuple[float, str]:
    """Calculate the maximum safe position size."""
    conn = get_exchange_connection()
    return await conn.get_safe_position_size(
        leverage=leverage,
        current_total_margin=current_total_margin,
        max_total_margin_percent=max_total_margin_percent,
        min_safe_balance=min_safe_balance,
    )


# ============================================================================
# Test functions
# ============================================================================

async def test_margin_validation():
    """Test the margin validation system."""
    print("\n" + "=" * 70)
    print("  🔒 MARGIN VALIDATION TEST (1% RULE vs FIXED MODE)")
    print("=" * 70)

    conn = get_exchange_connection()
    balance = await conn.get_balance(use_cache=False)

    if not balance:
        print("❌ Could not fetch balance")
        return False

    free_usdt = balance['USDT']
    use_fixed_mode = USE_FIXED_MODE

    print(f"💰 Free USDT: ${free_usdt:.2f}")
    print(f"🔒 MIN_SAFE_BALANCE_USDT: ${MIN_SAFE_BALANCE_USDT:.2f}")
    if use_fixed_mode:
        print(f"🔧 FIXED MODE ACTIVE: ${FIXED_POSITION_SIZE:.2f} margin per position")
        print(f"   Notional per position: ${FIXED_POSITION_SIZE * LEVERAGE:.2f} @ {LEVERAGE}x")
    else:
        print(f"📊 1% margin limit: ${free_usdt * 0.01:.2f}")
    print(f"📊 50% total margin limit: ${free_usdt * 0.50:.2f}")

    leverage = LEVERAGE
    if use_fixed_mode:
        test_sizes = [
            FIXED_POSITION_SIZE * leverage * 0.5,
            FIXED_POSITION_SIZE * leverage,
            FIXED_POSITION_SIZE * leverage * 2,
        ]
    else:
        test_sizes = [
            free_usdt * 0.10,
            free_usdt * 0.25,
            free_usdt * 0.50,
            free_usdt * 1.00,
        ]

    print(f"\n📊 Margin Validation Results ({leverage}x leverage):")
    print("-" * 70)

    for test_size in test_sizes:
        margin = test_size / leverage
        margin_percent = (margin / free_usdt) * 100
        is_valid, reason, max_size = await conn.validate_margin_usage(
            size_usdt=test_size, leverage=leverage
        )
        status = "✅ PASS" if is_valid else "❌ FAIL"
        print(f"  {status} | Notional: ${test_size:8.2f} | Margin: ${margin:6.2f} ({margin_percent:.2f}%) | {reason[:50]}")

    print("\n📊 Safe Position Size Calculation:")
    print("-" * 70)
    max_size, reason = await conn.get_safe_position_size(leverage=leverage)
    print(f"  Max safe notional: ${max_size:.2f} (margin: ${max_size/leverage:.2f})")
    print(f"  Reason: {reason}")
    print("=" * 70 + "\n")
    return True


async def test_cross_margin(symbol: str = 'BTC/USDT', leverage: int = 10) -> bool:
    """Test cross margin configuration."""
    print(f"\n🔍 Testing cross margin for {symbol} at {leverage}x...")

    conn = get_exchange_connection()
    exchange = await conn.get_connection()

    if not exchange:
        print("❌ Failed to get exchange connection")
        return False

    futures_symbol = conn._format_futures_symbol(symbol)

    try:
        result = await conn._ensure_cross_margin(symbol, leverage)

        if result:
            print(f"✅ Cross margin successfully set for {futures_symbol}")

            positions = await asyncio.wait_for(
                asyncio.to_thread(exchange.fetch_positions, [futures_symbol]),
                timeout=API_CALL_TIMEOUT
            )
            if positions:
                for pos in positions:
                    if pos.get('symbol') == futures_symbol:
                        print(
                            f"   Position info: leverage={pos.get('leverage')}x, "
                            f"margin_mode={pos.get('info', {}).get('margin_mode', 'unknown')}"
                        )
            else:
                print("   No open position to verify (this is fine)")
            return True
        else:
            print(f"❌ Failed to set cross margin for {futures_symbol}")
            return False

    except asyncio.TimeoutError:
        print(f"⏱️ test_cross_margin timed out after {API_CALL_TIMEOUT}s")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False


async def test_long_trade():
    """Test a long trade with margin validation."""
    print("\n" + "=" * 70)
    print("  🔒 LONG TRADE TEST WITH MARGIN VALIDATION")
    print("=" * 70)

    use_fixed_mode = USE_FIXED_MODE
    test_size = FIXED_POSITION_SIZE * LEVERAGE if use_fixed_mode else 10.0

    print(f"\n📈 Testing LONG trade (buy) with size ${test_size:.2f}:")
    result = await execute_trade(
        symbol='BTC/USDT', side='buy', size_usdt=test_size,
        dry_run=True, leverage=LEVERAGE, take_profit_price=70000.0
    )
    if result:
        print(f"  ✅ LONG dry-run successful: {result.get('id')}")
    else:
        print("  ❌ LONG dry-run failed (margin validation)")

    print("=" * 70 + "\n")


async def test_close_trade_skip_margin():
    """Test that closing trades skip margin validation."""
    print("\n" + "=" * 70)
    print("  🔧 CLOSING TRADE MARGIN SKIP TEST")
    print("=" * 70)

    print("\n📊 Testing that closing trades bypass margin validation:")

    result = await execute_trade(
        symbol='BTC/USDT', side='buy', size_usdt=5.0,
        dry_run=True, leverage=LEVERAGE,
        is_closing=True, reduce_only=True
    )

    if result:
        print(f"  ✅ Closing trade dry-run successful (margin validation SKIPPED)")
        print(f"     Trade ID: {result.get('id')}")
        print(f"     Size: ${result.get('size', 0):.2f}")
    else:
        print(f"  ❌ Closing trade dry-run FAILED – margin validation still running!")

    print("=" * 70 + "\n")


async def test_fetch_positions():
    """Test fetching positions from exchange."""
    print("\n" + "=" * 70)
    print("  🔍 FETCH POSITIONS TEST")
    print("=" * 70)

    conn = get_exchange_connection()
    positions = await conn.fetch_positions()

    if positions is None:
        print("❌ Failed to fetch positions (connection error)")
        return False

    if not positions:
        print("📭 No open positions on exchange")
    else:
        print(f"📊 Found {len(positions)} open positions:")
        for symbol, pos in positions.items():
            print(f"   {symbol}:")
            print(f"      Size: {pos['size']:.8f}")
            print(f"      Entry: ${pos['entry_price']:.2f}")
            print(f"      Leverage: {pos['leverage']}x")
            print(f"      Unrealized PnL: ${pos['unrealized_pnl']:.2f}")
            print(f"      Liquidation: ${pos['liquidation_price']:.2f}")

    print("=" * 70 + "\n")
    return True


async def test_limit_order_support():
    """Test limit order support for emergency exits."""
    print("\n" + "=" * 70)
    print("  🔥 LIMIT ORDER SUPPORT TEST (Emergency Exit)")
    print("=" * 70)

    print("\n📊 Testing limit order with slippage protection:")

    result = await execute_trade(
        symbol='BTC/USDT',
        side='sell',
        size_usdt=10.0,
        dry_run=True,
        leverage=10,
        is_closing=True,
        reduce_only=True,
        limit_price=65000.0,
    )

    if result:
        print(f"  ✅ Limit order dry-run successful")
        print(f"     Trade ID: {result.get('id')}")
        print(f"     Size: ${result.get('size', 0):.2f}")
        print(f"     Order Type: {result.get('order_type', 'UNKNOWN')}")
        print(f"     Limit Price: ${result.get('limit_price', 0):.2f}")
    else:
        print(f"  ❌ Limit order dry-run FAILED")

    print("=" * 70 + "\n")


async def test_already_closed_detection():
    """Unit test for the "empty position" already-closed detection."""
    print("\n" + "=" * 70)
    print("  🧪 ALREADY-CLOSED DETECTION TEST (v10.5 phantom short fix)")
    print("=" * 70)

    test_cases = [
        ('{"label":"INCREASE_POSITION","message":"empty position"}', True),
        ('gateio {"label":"INCREASE_POSITION","message":"empty position"}', True),
        ('INCREASE_POSITION: empty position', True),
        ('position does not exist', True),
        ('no position to close', True),
        ('insufficient balance', False),
        ('network timeout', False),
        ('', False),
    ]

    all_passed = True
    for msg, expected in test_cases:
        actual = _looks_like_already_closed(msg)
        status = "✅" if actual == expected else "❌"
        if actual != expected:
            all_passed = False
        print(f"  {status} _looks_like_already_closed({msg!r}) -> {actual} (expected {expected})")

    print("=" * 70)
    print(f"  Result: {'✅ ALL PASSED' if all_passed else '❌ SOME FAILED'}")
    print("=" * 70 + "\n")
    return all_passed


async def test_min_safe_override():
    """
    v18.2 NEW: Verify per-call MIN_SAFE override works.

    Tests that:
      1. A high override rejects a dry-run trade
      2. The module-level global is NOT mutated by the override
    """
    print("\n" + "=" * 70)
    print("  🧪 MIN_SAFE OVERRIDE TEST (v18.2)")
    print("=" * 70)

    conn = get_exchange_connection()
    balance = await conn.get_balance(use_cache=False)
    if not balance:
        print("❌ Cannot fetch balance — skipping")
        return False

    free_usdt = balance['USDT']
    global_before = MIN_SAFE_BALANCE_USDT

    # 1. High override should reject (free < 1e9)
    high_override = 1e9
    result = await execute_trade(
        symbol='BTC/USDT', side='buy', size_usdt=10.0,
        dry_run=True, leverage=LEVERAGE,
        min_safe_balance_override=high_override,
    )
    rejected = result is None
    print(f"  {'✅' if rejected else '❌'} High override ({high_override:.0f}) "
          f"rejected trade: {rejected}")

    # 2. Global should be unchanged
    global_after = MIN_SAFE_BALANCE_USDT
    unchanged = (global_before == global_after)
    print(f"  {'✅' if unchanged else '❌'} Global MIN_SAFE unchanged: "
          f"${global_before:.2f} → ${global_after:.2f}")

    print("=" * 70 + "\n")
    return rejected and unchanged


async def test_connection():
    """Test the Gate.io connection, cross margin, and print balance."""
    print("\n🔍 Testing Gate.io Futures connection...")

    if USE_FIXED_MODE:
        print(f"🔧 FIXED POSITION MODE ENABLED: {FIXED_POSITION_SIZE} USDT margin per position")
        print(f"   → Notional per position: {FIXED_POSITION_SIZE * LEVERAGE:.2f} USDT @ {LEVERAGE}x")
    else:
        print(f"📊 DYNAMIC MODE (1% rule per position)")

    print(f"🔒 MIN_SAFE_BALANCE_USDT (global default): ${MIN_SAFE_BALANCE_USDT:.2f}")
    print(f"🔥 EMERGENCY SETTINGS: Slippage={EMERGENCY_MAX_SLIPPAGE_PERCENT}%, Loss Limit={EMERGENCY_FORCE_EXIT_LOSS_LIMIT}%")
    print(f"🚨 EMERGENCY LIMIT WAIT: {EMERGENCY_LIMIT_WAIT_SECONDS}s")
    print(f"🆕 v18.2: per-call MIN_SAFE override supported")

    conn = get_exchange_connection()
    ok = await conn.check_startup_connectivity()
    if not ok:
        print("❌ Startup connectivity check failed – aborting test")
        return False

    price = await get_current_price('BTC/USDT')
    if price:
        print(f"✅ BTC/USDT price: ${price:.2f}")
    else:
        print("❌ Failed to fetch BTC price")

    await test_already_closed_detection()
    await test_min_safe_override()
    await test_margin_validation()
    await test_long_trade()
    await test_close_trade_skip_margin()
    await test_fetch_positions()
    await test_limit_order_support()

    cross_ok = await test_cross_margin('BTC/USDT', LEVERAGE)
    if cross_ok:
        print("✅ Cross margin test passed")
    else:
        print("⚠️ Cross margin test failed – positions may use isolated margin")

    balance = await get_balance()
    if balance:
        print(f"✅ USDT Balance: ${balance['USDT']:.2f}")
        print(f"   Total Balance: ${balance['total']:.2f}")
    else:
        print("❌ Failed to fetch balance")

    return price is not None and balance is not None


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    asyncio.run(test_connection())