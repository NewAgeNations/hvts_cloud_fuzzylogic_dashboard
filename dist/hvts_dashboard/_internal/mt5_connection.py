# mt5_connection.py - COMPLETE FIXED VERSION WITH DYNAMIC FILLING MODES
# ============================================================================
# UPDATED (2026-10-14 — SuperTrend Oracle migration):
#   • ADDED: 'H12' to the timeframe map in get_ohlcv()
#   • ADDED: 'H12' to get_available_timeframes()
#   These additions are required by the SuperTrend Oracle (which primes
#   from M30/H1/H12 frames). No other behaviour changed.
#
# UPDATED (2026-08-12):
#   • ADDED: get_tick() method for Classic DCA strategy
#   • ADDED: get_current_price() method with proper error handling
#   • ADDED: get_ask() and get_bid() methods
#   • ADDED: get_symbol_info_sync() for synchronous access
#   • FIXED: All methods now use _ensure_mt5_connected() for reliability
#   • RETAINED: Dynamic filling mode fallback (RETURN → FOK → IOC)
#   • RETAINED: Magic number validation for close_position and cancel_order
#   • RETAINED: WAL mode and performance PRAGMAs
# ============================================================================

import MetaTrader5 as mt5
import pandas as pd
import logging
import time
import asyncio
from datetime import datetime, timedelta
import numpy as np
import os
from typing import Optional, List, Dict, Any, Set, Union

# Import bot magic numbers from trading_config if available
try:
    from trading_config import BOT_MAGIC_NUMBERS
except ImportError:
    # Fallback magic numbers if trading_config not available
    BOT_MAGIC_NUMBERS = {
        12345,   # Grid strategy
        12346,   # ATR Grid strategy
        12347,   # Accumulator strategy
        12348,   # Fibonacci staging strategy
        12349,   # DCA strategy
    }

logger = logging.getLogger(__name__)


class MT5Connection:
    """MT5 connection wrapper with magic number validation for safe position management."""

    _instance = None
    _connected = False
    _server = None  # Store server name for reconnection

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    async def initialize(self, account=None, password=None, server=None):
        """Initialize MT5 connection and store server for reconnection"""
        try:
            # Store server for future reconnection attempts
            self._server = server

            logger.info(f"🔌 Attempting MT5 connection to {server}...")

            # First shutdown any existing connection
            try:
                mt5.shutdown()
            except:
                pass

            # Initialize MT5
            if not mt5.initialize():
                error = mt5.last_error()
                logger.error(f"❌ MT5 initialization failed. Error: {error}")
                return False

            logger.info("✅ MT5 initialized successfully")

            # Login if credentials provided
            if account and password and server:
                logger.info(f"🔐 Attempting login - Account: {account}, Server: {server}")

                login_success = mt5.login(login=int(account), password=password, server=server)

                if not login_success:
                    error = mt5.last_error()
                    logger.error(f"❌ MT5 login failed. Error: {error}")
                    return False

                logger.info("✅ Login successful")

                # Verify account info
                account_info = mt5.account_info()
                if account_info:
                    logger.info(f"💰 Connected to: {account_info.login}, Balance: {account_info.balance}")
                else:
                    logger.warning("⚠️  Could not get account info")

            self._connected = True
            return True

        except Exception as e:
            logger.error(f"❌ Exception during MT5 initialization: {str(e)}")
            return False

    async def shutdown(self):
        """Shutdown MT5 connection"""
        try:
            mt5.shutdown()
            self._connected = False
            logger.info("MT5 connection closed")
        except Exception as e:
            logger.error(f"Error shutting down MT5: {str(e)}")

    def is_connected(self):
        return self._connected

    async def _ensure_mt5_connected(self) -> bool:
        """Ensure MT5 connection is active."""
        if not self._connected:
            return False

        try:
            # Quick check if MT5 is still connected
            account_info = mt5.account_info()
            if account_info is None:
                logger.warning("MT5 connection lost, attempting reconnect...")
                # Flag was stale (terminal was shut down elsewhere) - clear it
                # so ensure_connection() actually re-initializes.
                self._connected = False
                return await self.ensure_connection()
            return True
        except Exception:
            self._connected = False
            return await self.ensure_connection()

    async def ensure_connection(self):
        """
        Ensure connection is established.
        UPDATED: Uses stored server value instead of reading from .env
        """
        if not self._connected:
            from dotenv import load_dotenv
            load_dotenv()

            mt5_account = os.getenv("MT5_ACCOUNT")
            mt5_password = os.getenv("MT5_PASSWORD")
            # Use stored server value (from trading_config)
            mt5_server = self._server or os.getenv("MT5_SERVER")

            if mt5_account and mt5_password and mt5_server:
                return await self.initialize(int(mt5_account), mt5_password, mt5_server)
        return self._connected

    async def get_account_info(self):
        """Get account information"""
        try:
            if not await self._ensure_mt5_connected():
                logger.error("MT5 not connected")
                return None

            account_info = mt5.account_info()
            if account_info is None:
                logger.error("Failed to get account info")
                return None
            return account_info
        except Exception as e:
            logger.error(f"Error getting account info: {str(e)}")
            return None

    async def get_balance(self):
        """Get account balance"""
        try:
            account_info = await self.get_account_info()
            if account_info:
                return {
                    'balance': account_info.balance,
                    'equity': account_info.equity,
                    'margin': account_info.margin,
                    'free_margin': account_info.margin_free,
                    'currency': account_info.currency
                }
            return {'balance': 0, 'equity': 0, 'margin': 0, 'free_margin': 0, 'currency': 'USD'}
        except Exception as e:
            logger.error(f"Error getting balance: {str(e)}")
            return {'balance': 0, 'equity': 0, 'margin': 0, 'free_margin': 0, 'currency': 'USD'}

    async def get_symbol_info(self, symbol):
        """Get symbol information"""
        try:
            if not symbol:
                logger.error("No symbol provided")
                return None

            if not await self._ensure_mt5_connected():
                return None

            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                logger.warning(f"Symbol {symbol} not found in MT5")
                return None
            return symbol_info
        except Exception as e:
            logger.error(f"Error getting symbol info for {symbol}: {str(e)}")
            return None

    def get_symbol_info_sync(self, symbol):
        """
        Synchronous version of get_symbol_info.
        Returns symbol info or None if symbol not found.
        ADDED: For accumulator strategy price validation
        """
        try:
            if not symbol:
                return None
            return mt5.symbol_info(symbol)
        except Exception as e:
            logger.error(f"Error in get_symbol_info_sync for {symbol}: {str(e)}")
            return None

    # ============================================================================
    # TICK AND PRICE METHODS
    # ============================================================================

    async def get_tick(self, symbol: str):
        """
        Get current tick data for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Tick data with bid, ask, etc. or None if failed
        """
        try:
            if not symbol:
                logger.error("No symbol provided for tick data")
                return None

            if not await self._ensure_mt5_connected():
                logger.error(f"MT5 not connected for {symbol}")
                return None

            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                error = mt5.last_error()
                logger.error(f"Failed to get tick data for {symbol}: {error}")
                return None

            return tick
        except Exception as e:
            logger.error(f"Error getting tick for {symbol}: {e}")
            return None

    async def get_current_price(self, symbol: str) -> Optional[float]:
        """
        Get current mid price for a symbol.

        Args:
            symbol: Trading symbol

        Returns:
            Mid price (bid + ask) / 2 or None if failed
        """
        try:
            if not symbol:
                logger.error("No symbol provided for price check")
                return None

            if not await self._ensure_mt5_connected():
                logger.error(f"MT5 not connected for {symbol}")
                return None

            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                error = mt5.last_error()
                logger.error(f"Failed to get tick data for {symbol}: {error}")
                return None

            # Check if bid and ask are valid
            if hasattr(tick, 'bid') and hasattr(tick, 'ask'):
                if tick.bid > 0 and tick.ask > 0:
                    return (tick.bid + tick.ask) / 2  # Return mid price
                else:
                    logger.error(f"Invalid price data for {symbol}: bid={tick.bid}, ask={tick.ask}")
                    return None
            else:
                logger.error(f"Tick data missing bid/ask for {symbol}")
                return None

        except Exception as e:
            logger.error(f"Error getting price for {symbol}: {e}")
            return None

    async def get_ask(self, symbol: str):
        """Get current ask price for a symbol"""
        try:
            if not await self._ensure_mt5_connected():
                return None

            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                return None
            return tick.ask
        except Exception as e:
            logger.error(f"Error getting ask for {symbol}: {str(e)}")
            return None

    async def get_bid(self, symbol: str):
        """Get current bid price for a symbol"""
        try:
            if not await self._ensure_mt5_connected():
                return None

            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                return None
            return tick.bid
        except Exception as e:
            logger.error(f"Error getting bid for {symbol}: {str(e)}")
            return None

    async def modify_position(self, ticket: int, take_profit: float = None, stop_loss: float = None) -> bool:
        """Modify an existing position's take profit and/or stop loss"""
        try:
            if not await self._ensure_mt5_connected():
                logger.error("MT5 not connected")
                return False

            # Get position info
            positions = mt5.positions_get(ticket=ticket)
            if not positions or len(positions) == 0:
                logger.error(f"Position {ticket} not found")
                return False
            position = positions[0]

            # Prepare request
            request = {
                "action": mt5.TRADE_ACTION_SLTP,
                "position": ticket,
                "symbol": position.symbol,
            }
            if take_profit is not None:
                request["tp"] = float(take_profit)
            if stop_loss is not None:
                request["sl"] = float(stop_loss)

            result = mt5.order_send(request)
            if result is None:
                logger.error(f"Modify position returned None")
                return False

            if result.retcode != mt5.TRADE_RETCODE_DONE:
                logger.error(f"Modify position failed: {result.retcode} - {result.comment}")
                return False

            logger.info(f"Modified position {ticket}: TP={take_profit}, SL={stop_loss}")
            return True
        except Exception as e:
            logger.error(f"Error modifying position {ticket}: {str(e)}")
            return False

    # ========================================================================
    # get_ohlcv with symbol_select() retry logic + H12 support
    # ========================================================================

    async def get_ohlcv(self, symbol, timeframe='M15', count=100, from_date=None):
        """
        Get OHLCV data for a symbol with optional start date.
        FIXED: Calls symbol_select() with retry logic before fetching data.
        FIXED: Exponential backoff for symbol selection retries.
        FIXED: Better error handling for symbol selection failures.
        UPDATED (2026-10-14): Added 'H12' for SuperTrend Oracle priming.

        Args:
            symbol: Trading symbol (e.g., 'BTCUSD', 'BTCUSD.sc')
            timeframe: Timeframe string
                       ('M1', 'M5', 'M15', 'M30', 'H1', 'H4', 'H12',
                        'D1', 'W1', 'MN1', ...)
            count: Number of bars to fetch
            from_date: Optional start date (datetime object)

        Returns:
            DataFrame with OHLCV data or None if failed
        """
        try:
            if not symbol:
                logger.error("No symbol provided for OHLCV")
                return None

            if not await self._ensure_mt5_connected():
                logger.error(f"MT5 not connected for {symbol}")
                return None

            # ===== Retry symbol selection with exponential backoff =====
            symbol_selected = False
            max_retries = 3
            base_delay = 0.5

            for attempt in range(max_retries):
                try:
                    if mt5.symbol_select(symbol, True):
                        symbol_selected = True
                        if attempt > 0:
                            logger.info(f"✅ Symbol {symbol} selected successfully on attempt {attempt + 1}")
                        break
                    else:
                        error = mt5.last_error()
                        delay = base_delay * (2 ** attempt)
                        logger.warning(
                            f"⚠️ Symbol select failed for {symbol} (attempt {attempt + 1}/{max_retries})"
                            f"{f': {error}' if error else ''}. Retrying in {delay:.1f}s..."
                        )
                        await asyncio.sleep(delay)
                except Exception as e:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"⚠️ Symbol select exception for {symbol} (attempt {attempt + 1}): {e}")
                    await asyncio.sleep(delay)

            if not symbol_selected:
                logger.error(f"❌ Failed to select symbol {symbol} after {max_retries} attempts")
                return None

            # Map timeframe string to MT5 constant
            # UPDATED: 'H12' added for SuperTrend Oracle priming.
            tf_mapping = {
                'M1':  mt5.TIMEFRAME_M1,
                'M5':  mt5.TIMEFRAME_M5,
                'M15': mt5.TIMEFRAME_M15,
                'M30': mt5.TIMEFRAME_M30,
                'H1':  mt5.TIMEFRAME_H1,
                'H4':  mt5.TIMEFRAME_H4,
                'H12': mt5.TIMEFRAME_H12,   # ← NEW
                'D1':  mt5.TIMEFRAME_D1,
                'W1':  mt5.TIMEFRAME_W1,
                'MN1': mt5.TIMEFRAME_MN1,
            }

            timeframe_val = tf_mapping.get(timeframe, mt5.TIMEFRAME_H1)

            if from_date:
                # Get data from specific date
                rates = mt5.copy_rates_from(symbol, timeframe_val, from_date, count)
            else:
                # Get data from current position
                rates = mt5.copy_rates_from_pos(symbol, timeframe_val, 0, count)

            if rates is None:
                error = mt5.last_error()
                logger.warning(f"No data returned for {symbol} {timeframe}{f': {error}' if error else ''}")
                return None

            if len(rates) == 0:
                logger.warning(f"Zero bars returned for {symbol} {timeframe}")
                return None

            # Convert to DataFrame
            df = pd.DataFrame(rates)
            df['time'] = pd.to_datetime(df['time'], unit='s')
            df.set_index('time', inplace=True)

            logger.debug(f"✅ Retrieved {len(df)} bars for {symbol} {timeframe}")
            return df

        except Exception as e:
            logger.error(f"Error getting OHLCV for {symbol}: {str(e)}")
            return None

    # ========================================================================
    # get_available_timeframes - includes H12
    # ========================================================================

    async def get_available_timeframes(self, symbol: str) -> List[str]:
        """
        Check which timeframes are available for a symbol.
        Useful for debugging when a symbol doesn't return data for certain timeframes.
        UPDATED (2026-10-14): H12 included.

        Args:
            symbol: Trading symbol

        Returns:
            List of available timeframe strings
        """
        available = []
        if not symbol:
            return available

        if not await self._ensure_mt5_connected():
            return available

        # Ensure symbol is selected with retry
        for attempt in range(3):
            if mt5.symbol_select(symbol, True):
                break
            await asyncio.sleep(0.5 * (attempt + 1))
        else:
            logger.warning(f"Failed to select symbol {symbol} for timeframe check")
            return available

        # Test each timeframe
        tf_mapping = {
            'M1':  mt5.TIMEFRAME_M1,
            'M5':  mt5.TIMEFRAME_M5,
            'M15': mt5.TIMEFRAME_M15,
            'M30': mt5.TIMEFRAME_M30,
            'H1':  mt5.TIMEFRAME_H1,
            'H4':  mt5.TIMEFRAME_H4,
            'H12': mt5.TIMEFRAME_H12,   # ← NEW
            'D1':  mt5.TIMEFRAME_D1,
            'W1':  mt5.TIMEFRAME_W1,
            'MN1': mt5.TIMEFRAME_MN1,
        }

        for tf_name, tf_const in tf_mapping.items():
            try:
                rates = mt5.copy_rates_from_pos(symbol, tf_const, 0, 10)
                if rates is not None and len(rates) > 0:
                    available.append(tf_name)
            except Exception as e:
                logger.debug(f"Timeframe {tf_name} not available for {symbol}: {e}")

        return available

    # ============ DYNAMIC FILLING MODE DETECTION ============

    def _get_best_filling_mode(self, symbol: str = None) -> int:
        """
        Determine the best filling mode for a symbol by reading the broker's
        supported filling modes from symbol_info.filling_mode (a bitmask).

        MT5 filling mode bitmask values:
          bit 0 (1) = ORDER_FILLING_FOK  supported
          bit 1 (2) = ORDER_FILLING_IOC  supported
          (neither bit set) = ORDER_FILLING_RETURN only

        Priority: RETURN → FOK → IOC (RETURN is most permissive for market orders)

        Returns:
            int: MT5 filling mode constant
        """
        try:
            if symbol:
                info = mt5.symbol_info(symbol)
                if info is not None:
                    filling_mask = info.filling_mode  # bitmask

                    # RETURN is supported when neither FOK-only nor IOC bits force exclusivity.
                    # However some brokers set filling_mode=0 meaning RETURN is the only option.
                    # Prefer RETURN first (most flexible), fall back to FOK, then IOC.
                    if filling_mask == 0:
                        # Only RETURN supported
                        logger.debug(f"{symbol}: filling_mode=0 → ORDER_FILLING_RETURN")
                        return mt5.ORDER_FILLING_RETURN

                    # Check FOK support (bit 0)
                    fok_supported = bool(filling_mask & 1)
                    # Check IOC support (bit 1)
                    ioc_supported = bool(filling_mask & 2)

                    # RETURN is implicitly supported unless broker explicitly prohibits it.
                    # Try RETURN first; if FOK-only broker (filling_mask==1), use FOK.
                    if filling_mask == 1:
                        # FOK only
                        logger.debug(f"{symbol}: filling_mode=1 → ORDER_FILLING_FOK")
                        return mt5.ORDER_FILLING_FOK
                    elif filling_mask == 2:
                        # IOC only
                        logger.debug(f"{symbol}: filling_mode=2 → ORDER_FILLING_IOC")
                        return mt5.ORDER_FILLING_IOC
                    else:
                        # Multiple modes supported; prefer RETURN
                        logger.debug(f"{symbol}: filling_mode={filling_mask} → ORDER_FILLING_RETURN")
                        return mt5.ORDER_FILLING_RETURN

            # No symbol info available — try RETURN, then FOK as fallback
            return mt5.ORDER_FILLING_RETURN

        except Exception as e:
            logger.warning(f"Error determining filling mode for {symbol}: {e}")
            return mt5.ORDER_FILLING_RETURN  # Safest default

    # ============ CRITICAL: DYNAMIC PIP VALUE METHOD ============

    async def get_pip_value(self, symbol: str) -> float:
        """
        Get the correct pip value for a symbol based on MT5 symbol info.

        For Forex:
            - 5-digit symbols (digits=5): pip = 0.0001
            - 3-digit JPY symbols (digits=3): pip = 0.01
            - 4-digit symbols (digits=4): pip = 0.0001

        For Bitcoin/Crypto:
            - BTCUSD (digits=2): pip = 1.0
            - ETHUSD (digits=2): pip = 1.0
            - Other crypto with digits=2: pip = 1.0
            - Crypto with digits=5: pip = 0.00001 (rare)

        For Metals:
            - XAUUSD (digits=2): pip = 0.01 (Gold)
            - XAGUSD (digits=3): pip = 0.001 (Silver)

        Args:
            symbol: Trading symbol (e.g., 'BTCUSD', 'EURUSD', 'XAUUSD')

        Returns:
            Pip value in price units (e.g., 1.0 for BTCUSD, 0.0001 for EURUSD)
        """
        try:
            if not symbol:
                logger.warning("No symbol provided for pip value calculation")
                return 0.0001  # Default fallback

            if not await self._ensure_mt5_connected():
                return self._get_fallback_pip_value(symbol)

            # Get symbol info from MT5
            symbol_info = await self.get_symbol_info(symbol)
            if symbol_info is None:
                # Fallback based on symbol name if MT5 info unavailable
                return self._get_fallback_pip_value(symbol)

            # Get digits and point from symbol info
            digits = symbol_info.digits
            point = symbol_info.point

            # Determine pip based on symbol type and digits
            symbol_upper = symbol.upper()

            # Bitcoin and Cryptocurrencies (BTCUSD, ETHUSD, etc.)
            # CENT accounts use digits=2; STANDARD accounts use digits=3.
            # In both cases, 1 pip = $1.00 price movement for BTC.
            if any(crypto in symbol_upper for crypto in ['BTC', 'XBT', 'ETH', 'LTC', 'BCH', 'XRP', 'ADA', 'DOT', 'LINK']):
                if digits >= 4:
                    # Very-high-precision crypto (rare)
                    return 0.00001
                else:
                    # digits 2 (CENT) or 3 (STANDARD) — both are whole-dollar pips
                    return 1.0

            # Gold and Precious Metals
            if 'XAU' in symbol_upper or 'GOLD' in symbol_upper:
                # Gold typically has 2 decimal places, pip = 0.01
                if digits == 2:
                    return 0.01
                elif digits == 3:
                    return 0.001
                else:
                    return 0.01

            if 'XAG' in symbol_upper or 'SILVER' in symbol_upper:
                # Silver typically has 3 decimal places, pip = 0.001
                if digits == 3:
                    return 0.001
                else:
                    return 0.001

            # JPY Pairs (EURJPY, USDJPY, etc.)
            if 'JPY' in symbol_upper:
                # JPY pairs typically have 3 decimal places, pip = 0.01
                if digits == 3:
                    return 0.01
                # Some brokers use 2 decimal places for JPY
                elif digits == 2:
                    return 0.01
                else:
                    return 0.01

            # Standard Forex Pairs (EURUSD, GBPUSD, etc.)
            # Most brokers use 5-digit pricing, pip = 0.0001
            if digits == 5:
                return 0.0001
            elif digits == 4:
                return 0.0001
            elif digits == 3:
                # Some exotic pairs with 3 digits
                return 0.001
            else:
                # Default: use point * 10 for standard forex
                return point * 10 if point > 0 else 0.0001

        except Exception as e:
            logger.error(f"Error calculating pip value for {symbol}: {str(e)}")
            return self._get_fallback_pip_value(symbol)

    def _get_fallback_pip_value(self, symbol: str) -> float:
        """
        Get fallback pip value based on symbol name when MT5 info is unavailable.

        Args:
            symbol: Trading symbol

        Returns:
            Estimated pip value
        """
        try:
            symbol_upper = symbol.upper()

            # Bitcoin and Cryptocurrencies
            if any(crypto in symbol_upper for crypto in ['BTC', 'XBT', 'ETH', 'LTC', 'BCH', 'XRP']):
                return 1.0

            # Gold
            if 'XAU' in symbol_upper or 'GOLD' in symbol_upper:
                return 0.01

            # Silver
            if 'XAG' in symbol_upper or 'SILVER' in symbol_upper:
                return 0.001

            # JPY Pairs
            if 'JPY' in symbol_upper:
                return 0.01

            # Standard Forex
            return 0.0001

        except Exception as e:
            logger.error(f"Error in fallback pip value for {symbol}: {e}")
            return 0.0001

    # ============ PIP VALUE CACHING ============

    _pip_value_cache = {}
    _pip_cache_time = 60  # Cache pip values for 60 seconds

    async def get_pip_value_cached(self, symbol: str) -> float:
        """
        Get cached pip value to avoid repeated MT5 calls.

        Args:
            symbol: Trading symbol

        Returns:
            Pip value in price units
        """
        current_time = time.time()

        # Check cache
        if symbol in self._pip_value_cache:
            cached_value, cache_time = self._pip_value_cache[symbol]
            if current_time - cache_time < self._pip_cache_time:
                return cached_value

        # Get fresh pip value
        pip_value = await self.get_pip_value(symbol)

        # Update cache
        self._pip_value_cache[symbol] = (pip_value, current_time)

        # Clean old cache entries
        to_remove = []
        for sym, (_, cache_time) in self._pip_value_cache.items():
            if current_time - cache_time > 3600:  # Remove after 1 hour
                to_remove.append(sym)
        for sym in to_remove:
            del self._pip_value_cache[sym]

        return pip_value

    async def calculate_pip_value(self, symbol, volume=1.0):
        """Calculate the actual pip value for a symbol in account currency - UPDATED to use get_pip_value"""
        try:
            if not symbol:
                logger.error("No symbol provided for pip value calculation")
                return 10.0 * volume  # Default

            # Get pip in price units
            pip_price = await self.get_pip_value(symbol)

            # Get symbol info for contract size
            symbol_info = await self.get_symbol_info(symbol)
            if symbol_info:
                contract_size = symbol_info.trade_contract_size
            else:
                contract_size = 100000  # Default for forex

            # Calculate pip value in account currency
            # For forex: pip value = (pip_price / exchange_rate) * contract_size * volume
            # Simplified: using standard $10 per pip for 100k lot
            if contract_size == 100000:
                # Standard forex lot
                base_pip_value = 10.0
            elif contract_size == 1000000:
                # Some brokers use 1,000,000 contract size
                base_pip_value = 100.0
            elif contract_size == 10000:
                # Mini lot
                base_pip_value = 1.0
            elif contract_size == 1000:
                # Micro lot
                base_pip_value = 0.1
            else:
                # Custom contract size
                base_pip_value = (contract_size / 100000) * 10.0

            # Adjust for Bitcoin (contract size 1)
            if 'BTC' in symbol.upper():
                base_pip_value = 1.0

            # Adjust for Gold
            if 'XAU' in symbol.upper():
                base_pip_value = 10.0

            pip_value_usd = base_pip_value * volume

            logger.debug(f"Pip value for {symbol}: ${pip_value_usd:.2f} per {volume} lots (pip_price={pip_price})")

            return pip_value_usd

        except Exception as e:
            logger.error(f"Error calculating pip value for {symbol}: {str(e)}")
            # Safe defaults
            if 'BTC' in symbol:
                return 1.0 * volume
            if 'JPY' in symbol:
                return 8.0 * volume
            return 10.0 * volume

    async def calculate_position_size(self, symbol, risk_percent=1, stop_loss_pips=50):
        """Calculate position size based on risk management with SAFETY LIMITS - FIXED"""
        try:
            if not symbol:
                logger.error("No symbol provided for position size calculation")
                return 0.01

            if not await self._ensure_mt5_connected():
                return 0.01

            balance_info = await self.get_balance()
            balance = balance_info['balance']

            if balance <= 0:
                logger.error(f"Invalid balance: {balance}")
                return 0.01  # Return minimum lot size

            risk_amount = balance * risk_percent / 100

            symbol_info = await self.get_symbol_info(symbol)
            if not symbol_info:
                logger.warning(f"No symbol info for {symbol}, using default lot size")
                return 0.01  # Return minimum lot size

            # GET PIP VALUE using the new method
            pip_value_per_lot = await self.calculate_pip_value(symbol, volume=1.0)

            # DEBUG LOGGING
            logger.info(f"POSITION SIZE CALCULATION for {symbol}:")
            logger.info(f"  Balance: ${balance:.2f}")
            logger.info(f"  Risk %: {risk_percent}% = ${risk_amount:.2f}")
            logger.info(f"  Stop Loss: {stop_loss_pips} pips")
            logger.info(f"  Pip Value per lot: ${pip_value_per_lot:.2f}")

            # FIXED DIVISION BY ZERO CHECK
            if stop_loss_pips <= 0:
                stop_loss_pips = 50  # Default value
                logger.warning(f"Stop loss pips was 0, using default: {stop_loss_pips}")

            if pip_value_per_lot <= 0:
                pip_value_per_lot = 10.0  # Default pip value
                logger.warning(f"Pip value was 0, using default: ${pip_value_per_lot:.2f}")

            risk_per_lot = stop_loss_pips * pip_value_per_lot
            logger.info(f"  Risk per lot at SL: ${risk_per_lot:.2f}")

            # Calculate position size with safety checks
            if risk_per_lot > 0:
                position_size = risk_amount / risk_per_lot
            else:
                logger.error(f"Risk per lot is 0 or negative, using minimum lot")
                return 0.01

            # Get symbol volume constraints
            min_lot = 0.01  # Default minimum
            max_lot = 1.0   # Default maximum
            step = 0.01     # Default step

            if symbol_info:
                min_lot = getattr(symbol_info, 'volume_min', 0.01)
                max_lot = getattr(symbol_info, 'volume_max', 1.0)
                step = getattr(symbol_info, 'volume_step', 0.01)

            # FIXED: Ensure position size is within valid range
            position_size = max(min_lot, min(position_size, max_lot))

            # Round to step size
            if step > 0:
                position_size = round(position_size / step) * step

            # FINAL VALIDATION - FIXED
            # Ensure position size is not less than minimum
            if position_size < min_lot:
                logger.warning(f"Position size {position_size:.6f} < minimum {min_lot}, using minimum")
                position_size = min_lot

            # Ensure position size is not too small (MT5 often rejects < 0.01)
            if position_size < 0.01:
                logger.warning(f"Position size {position_size:.6f} too small, rounding up to 0.01")
                position_size = 0.01

            # Ensure position size is not too large
            if position_size > max_lot:
                logger.warning(f"Position size {position_size:.6f} > maximum {max_lot}, capping")
                position_size = max_lot

            # Log final calculation
            logger.info(f"FINAL POSITION SIZE for {symbol}: {position_size:.6f} lots")

            # Convert to float with proper decimal places
            return float(f"{position_size:.6f}")

        except Exception as e:
            logger.error(f"Error calculating position size for {symbol}: {str(e)}")
            return 0.01  # Return minimum lot size on error

    async def validate_volume(self, symbol, volume):
        """Validate volume before placing order - NEW METHOD"""
        try:
            if not symbol:
                logger.error("No symbol provided for volume validation")
                return 0.01

            if not await self._ensure_mt5_connected():
                return 0.01

            symbol_info = await self.get_symbol_info(symbol)
            if not symbol_info:
                logger.warning(f"No symbol info for {symbol}")
                return 0.01

            min_lot = getattr(symbol_info, 'volume_min', 0.01)
            max_lot = getattr(symbol_info, 'volume_max', 1.0)
            step = getattr(symbol_info, 'volume_step', 0.01)

            # Ensure minimum lot size
            if volume < min_lot:
                logger.warning(f"Volume {volume:.6f} < minimum {min_lot}, using minimum")
                volume = min_lot

            # Ensure not too small (MT5 precision)
            if volume < 0.01:
                logger.warning(f"Volume {volume:.6f} too small, rounding up to 0.01")
                volume = 0.01

            # Ensure not too large
            if volume > max_lot:
                logger.warning(f"Volume {volume:.6f} > maximum {max_lot}, capping")
                volume = max_lot

            # Round to step size
            if step > 0:
                volume = round(volume / step) * step

            # Round to 2 decimal places for MT5
            volume = round(volume, 2)

            # Final validation
            if volume <= 0:
                volume = 0.01

            return volume

        except Exception as e:
            logger.error(f"Error validating volume: {e}")
            return 0.01

    async def get_order_info(self, order_id):
        """
        Get information about a specific order

        Args:
            order_id: Order ticket number

        Returns:
            Order information or None if not found
        """
        try:
            if not order_id:
                print("❌ No order ID provided")
                return None

            if not await self._ensure_mt5_connected():
                return None

            orders = mt5.orders_get(ticket=order_id)
            if orders is None or len(orders) == 0:
                # Check positions if not in orders
                positions = mt5.positions_get(ticket=order_id)
                if positions is not None and len(positions) > 0:
                    return positions[0]
                return None
            return orders[0]
        except Exception as e:
            print(f"❌ Error getting order info: {str(e)}")
            return None

    async def get_open_positions(self, symbol=None):
        """Get open positions with better error handling"""
        try:
            if not await self._ensure_mt5_connected():
                return []

            # Force refresh of MT5 positions
            mt5.symbols_total()

            if symbol:
                positions = mt5.positions_get(symbol=symbol)
            else:
                positions = mt5.positions_get()

            if positions is None:
                # MT5 might return None instead of empty list
                error = mt5.last_error()
                logger.warning(f"MT5 positions_get returned None. Error: {error}")
                return []

            return list(positions)  # Convert to list for consistency

        except Exception as e:
            logger.error(f"Error getting open positions: {str(e)}")
            return []

    # ============ close_position with dynamic filling modes ============

    async def close_position(self, ticket, volume=None, comment="Closed"):
        """
        Close a position by ticket number.
        FIXED: Uses dynamic filling mode with fallback (RETURN → FOK → IOC).

        Args:
            ticket: Position ticket number
            volume: Volume to close (None for full position)
            comment: Comment for the close order

        Returns:
            Order result or None if failed or skipped (non-bot position)
        """
        try:
            if not ticket:
                print("❌ No ticket provided for close position")
                return None

            if not await self._ensure_mt5_connected():
                print("❌ MT5 not connected")
                return None

            # Get position to check magic number
            positions = mt5.positions_get(ticket=ticket)
            if not positions:
                print(f"❌ Position {ticket} not found")
                return None

            position = positions[0]
            symbol = position.symbol
            magic = position.magic

            # ============ CRITICAL SAFETY CHECK ============
            # Only close positions that belong to the bot
            try:
                from trading_config import BOT_MAGIC_NUMBERS
            except ImportError:
                BOT_MAGIC_NUMBERS = {712345, 712346, 712347, 712348, 712349,
                                     712350, 712351, 712352, 712353, 712354,
                                     712355, 712356, 712357, 712358, 712359,
                                     712360, 742349, 7999999}

            if magic not in BOT_MAGIC_NUMBERS:
                print(f"🛡️ SAFETY: Skipping close of position {ticket} (magic={magic}) - not a bot position")
                logger.info(f"Skipping close of position {ticket} (magic={magic}) - not in bot magic numbers {BOT_MAGIC_NUMBERS}")
                return None

            volume_to_close = volume if volume else position.volume

            print(f"📊 Closing {volume_to_close} lots of {symbol} (bot position magic={magic})")

            # Determine close order type
            if position.type == mt5.ORDER_TYPE_BUY:
                close_type = mt5.ORDER_TYPE_SELL
                price = mt5.symbol_info_tick(symbol).bid
                print(f"🔴 Closing LONG at BID: {price}")
            else:
                close_type = mt5.ORDER_TYPE_BUY
                price = mt5.symbol_info_tick(symbol).ask
                print(f"🟢 Closing SHORT at ASK: {price}")

            # Ultra-short comment for close
            short_comment = comment[:10] if comment else "Close"

            # ============ Dynamic filling mode with fallback ============
            filling_modes = [
                mt5.ORDER_FILLING_RETURN,
                mt5.ORDER_FILLING_FOK,
                mt5.ORDER_FILLING_IOC,
            ]

            result = None
            for attempt, filling_mode in enumerate(filling_modes):
                mode_name = {
                    mt5.ORDER_FILLING_RETURN: "RETURN",
                    mt5.ORDER_FILLING_FOK: "FOK",
                    mt5.ORDER_FILLING_IOC: "IOC",
                }.get(filling_mode, str(filling_mode))

                request = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "position": ticket,
                    "symbol": symbol,
                    "volume": volume_to_close,
                    "type": close_type,
                    "price": price,
                    "deviation": 20,
                    "magic": magic,
                    "comment": short_comment,
                    "type_filling": filling_mode,
                }

                print(f"📤 Sending close request (attempt {attempt+1}, filling={mode_name})...")
                result = mt5.order_send(request)

                if result is None:
                    error = mt5.last_error()
                    print(f"❌ Close order returned None (attempt {attempt+1}). Error: {error}")
                    continue

                print(f"📨 Close result: {result.retcode} - {result.comment}")

                # If retcode is 10030 (unsupported filling mode), try next mode
                if result.retcode == 10030:
                    print(f"⚠️ Filling mode {mode_name} rejected by broker, trying next...")
                    continue

                # Success codes
                success_codes = [mt5.TRADE_RETCODE_DONE, 10009, 10008]

                if result.retcode in success_codes:
                    print(f"✅ Position closed successfully!")
                    return result
                else:
                    # Some other error - log and return
                    print(f"❌ Close failed: {result.retcode} - {result.comment}")
                    return None

            # If we've exhausted all filling modes
            if result and result.retcode == 10030:
                print(f"❌ All filling modes rejected by broker. Last error: {result.retcode} - {result.comment}")
            return None

        except Exception as e:
            print(f"❌ Error closing position: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return None

    async def close_positions_by_magic(self, magic_numbers: Set[int], symbol: str = None) -> int:
        """
        Close all positions with magic numbers in the specified set.
        Used for selective cleanup (e.g., warm reboot only closing bot positions).

        Args:
            magic_numbers: Set of magic numbers to close
            symbol: Optional symbol filter

        Returns:
            Number of positions closed
        """
        try:
            if not await self._ensure_mt5_connected():
                return 0

            if symbol:
                positions = mt5.positions_get(symbol=symbol)
            else:
                positions = mt5.positions_get()

            if not positions:
                return 0

            closed_count = 0
            for pos in positions:
                if pos.magic in magic_numbers:
                    result = await self.close_position(pos.ticket)
                    if result:
                        closed_count += 1
                    await asyncio.sleep(0.1)  # Rate limiting

            logger.info(f"Closed {closed_count} positions with magic numbers in {magic_numbers}")
            return closed_count

        except Exception as e:
            logger.error(f"Error closing positions by magic: {e}")
            return 0

    # ============ cancel_order with dynamic filling modes ============

    async def cancel_order(self, order_id):
        """
        Cancel a pending order.
        CRITICAL FIX: Only cancels orders with magic numbers that match bot's magic numbers.

        Args:
            order_id: Order ticket number

        Returns:
            True if successful, False otherwise
        """
        try:
            if not order_id:
                print("❌ No order ID provided for cancel")
                return False

            if not await self._ensure_mt5_connected():
                print("❌ MT5 not connected")
                return False

            print(f"🔧 Cancelling order {order_id}")

            # Get order information first to check magic number
            order_info = await self.get_order_info(order_id)
            if not order_info:
                print(f"❌ Order {order_id} not found")
                return False

            # Get magic number (attribute may be 'magic' or 'magic_number')
            magic = getattr(order_info, 'magic', None)
            if magic is None:
                magic = getattr(order_info, 'magic_number', 0)

            # ============ CRITICAL SAFETY CHECK ============
            # Only cancel orders that belong to the bot
            try:
                from trading_config import BOT_MAGIC_NUMBERS
            except ImportError:
                BOT_MAGIC_NUMBERS = {712345, 712346, 712347, 712348, 712349,
                                     712350, 712351, 712352, 712353, 712354,
                                     712355, 712356, 712357, 712358, 712359,
                                     712360, 742349, 7999999}

            if magic not in BOT_MAGIC_NUMBERS:
                print(f"🛡️ SAFETY: Skipping cancel of order {order_id} (magic={magic}) - not a bot order")
                logger.info(f"Skipping cancel of order {order_id} (magic={magic}) - not in bot magic numbers {BOT_MAGIC_NUMBERS}")
                return False

            # Prepare cancel request
            request = {
                "action": mt5.TRADE_ACTION_REMOVE,
                "order": order_id,
            }

            # Send cancel request
            print(f"📤 Sending cancel request...")
            result = mt5.order_send(request)

            if result is None:
                error = mt5.last_error()
                print(f"❌ Cancel order returned None. Error: {error}")
                return False

            print(f"📨 Cancel Result: {result.retcode} - {result.comment}")

            success_codes = [mt5.TRADE_RETCODE_DONE, 10009, 10008]

            if result.retcode not in success_codes:
                print(f"❌ Cancel failed: {result.retcode} - {result.comment}")
                return False

            print(f"✅ Order {order_id} cancelled successfully!")
            return True

        except Exception as e:
            print(f"❌ Error cancelling order: {str(e)}")
            import traceback
            print(f"Traceback: {traceback.format_exc()}")
            return False

    async def cancel_orders_by_magic(self, magic_numbers: Set[int], symbol: str = None) -> int:
        """
        Cancel all pending orders with magic numbers in the specified set.
        Used for selective cleanup (e.g., warm reboot only cancelling bot orders).

        Args:
            magic_numbers: Set of magic numbers to cancel
            symbol: Optional symbol filter

        Returns:
            Number of orders cancelled
        """
        try:
            if not await self._ensure_mt5_connected():
                return 0

            if symbol:
                orders = mt5.orders_get(symbol=symbol)
            else:
                orders = mt5.orders_get()

            if not orders:
                return 0

            cancelled_count = 0
            for order in orders:
                magic = getattr(order, 'magic', 0)
                if magic in magic_numbers:
                    result = await self.cancel_order(order.ticket)
                    if result:
                        cancelled_count += 1
                    await asyncio.sleep(0.05)  # Rate limiting

            logger.info(f"Cancelled {cancelled_count} orders with magic numbers in {magic_numbers}")
            return cancelled_count

        except Exception as e:
            logger.error(f"Error cancelling orders by magic: {e}")
            return 0

    async def get_pending_orders(self, symbol=None):
        """
        Get all pending orders (limit orders for grid)

        Args:
            symbol: Optional symbol filter

        Returns:
            List of pending orders
        """
        try:
            if not await self._ensure_mt5_connected():
                return []

            if symbol:
                orders = mt5.orders_get(symbol=symbol)
            else:
                orders = mt5.orders_get()

            if orders is None:
                return []

            return list(orders)
        except Exception as e:
            logger.error(f"Error getting pending orders: {str(e)}")
            return []

    # ====== ENHANCED MARKET ORDER PLACEMENT ======

    async def place_market_order(self, symbol, order_type, volume, stop_loss=None, take_profit=None, comment="", magic=12345):
        """
        Place a MARKET order at current price - FIXED VERSION
        FIXED: Changed filling mode from IOC to RETURN for broker compatibility

        Args:
            symbol: Trading symbol
            order_type: 'buy' or 'sell' (string) OR mt5.ORDER_TYPE_BUY/SELL (integer)
            volume: Lot size
            stop_loss: Stop loss price (optional)
            take_profit: Take profit price (optional)
            comment: Order comment
            magic: Magic number (default 12345 - Grid strategy)

        Returns:
            Order result or None
        """
        try:
            if not symbol:
                logger.error("❌ No symbol provided for market order")
                return None

            if not await self._ensure_mt5_connected():
                logger.error("❌ MT5 not connected")
                return None

            logger.info(f"🔧 Placing MARKET order for {symbol}")

            # Check if trading is allowed
            terminal_info = mt5.terminal_info()
            if terminal_info and not terminal_info.trade_allowed:
                logger.error("❌ TRADING DISABLED - Enable AutoTrading (Ctrl+E)")
                return None

            # Ensure symbol is selected and visible
            symbol_info = await self.get_symbol_info(symbol)
            if symbol_info is None:
                logger.error(f"❌ Symbol {symbol} not found")
                return None

            if not symbol_info.visible:
                logger.warning(f"🔄 Selecting symbol {symbol}...")
                selected = mt5.symbol_select(symbol, True)
                if not selected:
                    logger.error(f"❌ Failed to select symbol {symbol}")
                    return None

            # Get current tick data
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                logger.error(f"❌ No tick data for {symbol}")
                return None

            logger.info(f"📊 {symbol} - Bid: {tick.bid:.5f}, Ask: {tick.ask:.5f}")

            # Validate volume
            volume = await self.validate_volume(symbol, volume)

            if volume <= 0:
                logger.error(f"❌ Invalid volume after validation: {volume}")
                return None

            logger.info(f"✅ Validated volume: {volume} lots")

            # Determine MT5 order type - FIXED: Handle both string and integer inputs
            if isinstance(order_type, str):
                # String input like 'buy' or 'sell'
                if order_type.lower() == 'buy':
                    mt5_order_type = mt5.ORDER_TYPE_BUY
                    price = tick.ask  # Use ASK for BUY
                    logger.info(f"🟢 BUY Market Order: {volume} lots at {price:.5f}")
                else:
                    mt5_order_type = mt5.ORDER_TYPE_SELL
                    price = tick.bid  # Use BID for SELL
                    logger.info(f"🔴 SELL Market Order: {volume} lots at {price:.5f}")
            else:
                # Integer input (MT5 constant)
                mt5_order_type = order_type
                if order_type == mt5.ORDER_TYPE_BUY:
                    price = tick.ask
                    logger.info(f"🟢 BUY Market Order (MT5 const): {volume} lots at {price:.5f}")
                else:  # mt5.ORDER_TYPE_SELL
                    price = tick.bid
                    logger.info(f"🔴 SELL Market Order (MT5 const): {volume} lots at {price:.5f}")

            # Short comment
            short_comment = comment[:15] if comment else "Market"

            # Get the best filling mode for this broker
            filling_mode = self._get_best_filling_mode(symbol)

            # Prepare order request - FIXED FILLING MODE
            request = {
                "action": mt5.TRADE_ACTION_DEAL,
                "symbol": symbol,
                "volume": float(volume),
                "type": mt5_order_type,
                "price": float(price),
                "deviation": 20,
                "magic": magic,
                "comment": short_comment,
                "type_filling": filling_mode,  # FIXED: Now uses RETURN mode
            }

            # Add stop loss and take profit if provided
            if stop_loss:
                request["sl"] = float(stop_loss)
                logger.info(f"🛑 Stop Loss: {stop_loss:.5f}")
            if take_profit:
                request["tp"] = float(take_profit)
                logger.info(f"🎯 Take Profit: {take_profit:.5f}")

            # Send order — retry with alternative filling modes on retcode 10030
            filling_fallbacks = [
                mt5.ORDER_FILLING_RETURN,
                mt5.ORDER_FILLING_FOK,
                mt5.ORDER_FILLING_IOC,
            ]
            # Remove the already-chosen mode from fallbacks so we don't repeat it
            filling_fallbacks = [f for f in filling_fallbacks if f != filling_mode]
            attempts = [(filling_mode, "primary")] + [(f, "fallback") for f in filling_fallbacks]

            result = None
            for attempt_mode, attempt_label in attempts:
                request["type_filling"] = attempt_mode
                mode_name = {
                    mt5.ORDER_FILLING_RETURN: "RETURN",
                    mt5.ORDER_FILLING_FOK: "FOK",
                    mt5.ORDER_FILLING_IOC: "IOC",
                }.get(attempt_mode, str(attempt_mode))
                logger.info(f"📤 Sending market order ({attempt_label}, filling={mode_name})...")
                result = mt5.order_send(request)

                if result is None:
                    error = mt5.last_error()
                    logger.error(f"❌ Market order send returned None ({attempt_label}). Error: {error}")
                    continue

                logger.info(f"📨 Market Order Result:")
                logger.info(f"   - Retcode: {result.retcode}")
                logger.info(f"   - Comment: {result.comment}")
                logger.info(f"   - Order: {result.order}")
                logger.info(f"   - Volume: {result.volume}")
                logger.info(f"   - Price: {result.price}")

                if result.retcode == 10030:
                    logger.warning(f"⚠️  Filling mode {mode_name} rejected by broker, trying next...")
                    continue
                break  # Either success or a non-filling error — stop retrying

            if result is None:
                logger.error(f"❌ All filling mode attempts returned None")
                return None

            # Success codes
            success_codes = [mt5.TRADE_RETCODE_DONE, 10009, 10008]

            if result.retcode not in success_codes:
                logger.error(f"❌ Market order failed: {result.retcode} - {result.comment}")
                return None

            # SUCCESS!
            logger.info(f"✅ MARKET ORDER EXECUTED SUCCESSFULLY!")
            logger.info(f"   - Ticket: {result.order}")
            logger.info(f"   - Price: {result.price}")
            logger.info(f"   - Volume: {result.volume}")

            return result

        except Exception as e:
            logger.error(f"❌ Exception in place_market_order: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    # ====== ENHANCED LIMIT ORDER PLACEMENT ======

    async def place_limit_order(self, symbol, order_type, volume, price,
                               take_profit=None, comment="", magic=12345):
        """
        Place a LIMIT order for grid trading - ENHANCED VALIDATION VERSION
        FIXED: TP now properly enforced at minimum distance from entry
        FIXED: Changed filling mode from IOC to RETURN for broker compatibility
        ADDED: Final validation to ensure TP never equals entry price
        FIXED: Uses symbol point and digits for accurate distance calculation
        FIXED: Minimum distance enforced in points (50 points minimum)

        Args:
            symbol: Trading symbol
            order_type: mt5.ORDER_TYPE_BUY_LIMIT or mt5.ORDER_TYPE_SELL_LIMIT
            volume: Lot size
            price: Limit order price
            take_profit: Take profit price (optional)
            comment: Order comment
            magic: Magic number (default 12345 - Grid strategy)

        Returns:
            Order result or None
        """
        try:
            if not symbol:
                logger.error("❌ No symbol provided for limit order")
                return None

            if not await self._ensure_mt5_connected():
                logger.error("❌ MT5 not connected")
                return None

            logger.info(f"🔧 Placing LIMIT order for {symbol}")

            # Check if trading is allowed
            terminal_info = mt5.terminal_info()
            if terminal_info and not terminal_info.trade_allowed:
                logger.error("❌ TRADING DISABLED - Enable AutoTrading (Ctrl+E)")
                return None

            # Ensure symbol is selected and visible
            symbol_info = mt5.symbol_info(symbol)
            if symbol_info is None:
                logger.error(f"❌ Symbol {symbol} not found")
                return None

            if not symbol_info.visible:
                logger.warning(f"🔄 Selecting symbol {symbol}...")
                selected = mt5.symbol_select(symbol, True)
                if not selected:
                    logger.error(f"❌ Failed to select symbol {symbol}")
                    return None

            # Get current tick data for reference
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                logger.error(f"❌ No tick data for {symbol}")
                return None

            logger.info(f"📊 {symbol} - Bid: {tick.bid:.5f}, Ask: {tick.ask:.5f}, Limit Price: {price:.5f}")

            # Grid-specific comment handling
            short_comment = comment[:15] if comment else "GRID"
            logger.info(f"💬 Grid Comment: '{short_comment}'")

            # FIXED VOLUME VALIDATION
            volume = await self.validate_volume(symbol, volume)

            if volume <= 0:
                logger.error(f"❌ Invalid volume after validation: {volume}")
                return None

            logger.info(f"✅ Validated volume: {volume} lots")

            # ===== CRITICAL FIX: Use symbol point and digits for accurate distance =====
            point = symbol_info.point
            digits = symbol_info.digits

            # Minimum distance in points (50 points = 5 pips for 5-digit symbols)
            # This works for all symbols regardless of pip value
            min_points = 50
            min_distance_price = min_points * point

            logger.info(f"📏 Symbol details: point={point:.6f}, digits={digits}, min_distance={min_distance_price:.6f}")

            # ENHANCED VALIDATION: Ensure price is reasonable distance from market
            if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
                # Buy limit must be BELOW current ask with minimum distance
                if price >= tick.ask - min_distance_price:
                    logger.warning(f"⚠️  Buy limit too close to ask ({price:.5f} >= {tick.ask - min_distance_price:.5f}), adjusting")
                    price = tick.ask - min_distance_price
                price = round(price, digits)

            elif order_type == mt5.ORDER_TYPE_SELL_LIMIT:
                # Sell limit must be ABOVE current bid with minimum distance
                if price <= tick.bid + min_distance_price:
                    logger.warning(f"⚠️  Sell limit too close to bid ({price:.5f} <= {tick.bid + min_distance_price:.5f}), adjusting")
                    price = tick.bid + min_distance_price
                price = round(price, digits)

            logger.info(f"✅ Price after adjustment: {price:.5f}")

            # ===== CRITICAL FIX: TP validation with point-based distance =====
            if take_profit:
                take_profit = round(take_profit, digits)

                if order_type == mt5.ORDER_TYPE_BUY_LIMIT:
                    # For BUY LIMIT, TP must be ABOVE entry price by at least min_distance
                    required_tp_price = price + min_distance_price

                    if take_profit < required_tp_price:
                        logger.warning(f"⚠️  TP too close to entry ({take_profit:.5f} < {required_tp_price:.5f}), adjusting")
                        take_profit = required_tp_price

                    # FINAL GUARANTEE: Ensure after rounding, TP is strictly greater than price
                    while round(take_profit, digits) <= price:
                        take_profit += point
                        logger.warning(f"🔄 Incrementing TP to ensure > price: {take_profit:.5f}")

                else:  # SELL_LIMIT
                    # For SELL LIMIT, TP must be BELOW entry price by at least min_distance
                    required_tp_price = price - min_distance_price

                    if take_profit > required_tp_price:
                        logger.warning(f"⚠️  TP too close to entry ({take_profit:.5f} > {required_tp_price:.5f}), adjusting")
                        take_profit = required_tp_price

                    # FINAL GUARANTEE: Ensure after rounding, TP is strictly less than price
                    while round(take_profit, digits) >= price:
                        take_profit -= point
                        logger.warning(f"🔄 Decrementing TP to ensure < price: {take_profit:.5f}")

                take_profit = round(take_profit, digits)
                logger.info(f"🎯 Take Profit after adjustment: {take_profit:.5f}")

                # FINAL VALIDATION
                if order_type == mt5.ORDER_TYPE_BUY_LIMIT and take_profit <= price:
                    logger.error(f"❌ CRITICAL: BUY TP still <= price after all adjustments!")
                    # Force a larger adjustment
                    take_profit = price + (min_points * 2 * point)
                    take_profit = round(take_profit, digits)
                    logger.warning(f"🆘 Force-adjusted BUY TP to: {take_profit:.5f}")

                elif order_type == mt5.ORDER_TYPE_SELL_LIMIT and take_profit >= price:
                    logger.error(f"❌ CRITICAL: SELL TP still >= price after all adjustments!")
                    # Force a larger adjustment
                    take_profit = price - (min_points * 2 * point)
                    take_profit = round(take_profit, digits)
                    logger.warning(f"🆘 Force-adjusted SELL TP to: {take_profit:.5f}")

            logger.info(f"✅ FINAL ORDER PARAMETERS:")
            logger.info(f"   Price: {price:.5f}")
            if take_profit:
                logger.info(f"   Take Profit: {take_profit:.5f}")
            logger.info(f"   Volume: {volume} lots")
            logger.info(f"   Order Type: {'BUY LIMIT' if order_type == mt5.ORDER_TYPE_BUY_LIMIT else 'SELL LIMIT'}")
            logger.info(f"   Magic: {magic}")

            # Get the best filling mode for this broker
            filling_mode = self._get_best_filling_mode(symbol)

            # Prepare LIMIT order request - FIXED FILLING MODE
            request = {
                "action": mt5.TRADE_ACTION_PENDING,
                "symbol": symbol,
                "volume": float(volume),
                "type": order_type,
                "price": float(price),
                "deviation": 20,
                "magic": magic,
                "comment": short_comment,
                "type_filling": filling_mode,  # FIXED: Changed from IOC to RETURN
                "type_time": mt5.ORDER_TIME_GTC,  # Good Till Cancelled
            }

            # Add take profit if provided and valid
            if take_profit and take_profit > 0:
                request["tp"] = float(take_profit)
                logger.info(f"🎯 Grid Take Profit: {take_profit:.5f}")

            # Send LIMIT order
            logger.info(f"📤 Sending LIMIT order...")
            result = mt5.order_send(request)

            if result is None:
                error = mt5.last_error()
                logger.error(f"❌ LIMIT order send returned None. Error: {error}")
                return result

            logger.info(f"📨 LIMIT Order Result:")
            logger.info(f"   - Retcode: {result.retcode}")
            logger.info(f"   - Comment: {result.comment}")
            logger.info(f"   - Order: {result.order}")
            logger.info(f"   - Volume: {result.volume}")
            logger.info(f"   - Price: {result.price}")
            if hasattr(result, 'tp'):
                logger.info(f"   - Take Profit: {result.tp}")

            # SUCCESS CODES for pending orders
            success_codes = [
                mt5.TRADE_RETCODE_DONE,  # 10009 - Order placed
                10009,  # Order placed
                10008,  # Order placed
                10027,  # Requote
            ]

            # Handle specific error codes
            if result.retcode == 10016:  # Invalid stops
                logger.warning(f"⚠️  Invalid stops error - trying without take profit")
                # Try again without take profit
                if 'tp' in request:
                    del request['tp']
                    logger.warning(f"🔄 Retrying without take profit...")
                    result = mt5.order_send(request)
                    if result is None:
                        logger.error(f"❌ Retry failed: No result")
                        return result

                    logger.warning(f"📨 Retry Result: {result.retcode} - {result.comment}")

            if result.retcode not in success_codes:
                logger.error(f"❌ LIMIT order failed for {symbol}: retcode={result.retcode} ({result.comment})")
                logger.error(f"📋 Full result: {result}")
                return None

            # SUCCESS!
            logger.info(f"✅ LIMIT ORDER PLACED SUCCESSFULLY!")
            logger.info(f"   - Ticket: {result.order}")
            logger.info(f"   - Price: {result.price}")
            logger.info(f"   - Volume: {result.volume}")
            if hasattr(result, 'tp'):
                logger.info(f"   - Take Profit: {result.tp}")

            return result

        except Exception as e:
            logger.error(f"❌ Exception in place_limit_order: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    # ====== ENHANCED MARKET ORDER PLACEMENT (LEGACY METHOD) ======

    async def place_order(self, symbol, order_type, volume, price=None,
                         stop_loss=None, take_profit=None, comment="", magic=12345):
        """
        Place an order in MT5 with FIXED VOLUME VALIDATION and dynamic filling mode.
        FIXED: Uses dynamic filling mode with fallback.
        """
        try:
            if not symbol:
                logger.error("❌ No symbol provided for order")
                return None

            if not await self._ensure_mt5_connected():
                logger.error("❌ MT5 not connected")
                return None

            logger.info(f"🔧 Starting order for {symbol}")

            # Check if trading is allowed
            terminal_info = mt5.terminal_info()
            if terminal_info and not terminal_info.trade_allowed:
                logger.error("❌ TRADING DISABLED - Enable AutoTrading (Ctrl+E)")
                return None

            # Ensure symbol is selected and visible
            symbol_info = await self.get_symbol_info(symbol)
            if symbol_info is None:
                logger.error(f"❌ Symbol {symbol} not found")
                return None

            if not symbol_info.visible:
                logger.warning(f"🔄 Selecting symbol {symbol}...")
                selected = mt5.symbol_select(symbol, True)
                if not selected:
                    logger.error(f"❌ Failed to select symbol {symbol}")
                    return None

            # Get current tick data
            tick = mt5.symbol_info_tick(symbol)
            if tick is None:
                logger.error(f"❌ No tick data for {symbol}")
                return None

            logger.info(f"📊 {symbol} - Bid: {tick.bid:.5f}, Ask: {tick.ask:.5f}")

            # ULTRA-STRICT COMMENT FIX: Force very short comments
            if comment and len(comment) > 15:
                # Extract only the essential part
                if "bearish" in comment.lower():
                    short_comment = "7/7 Bear"
                elif "bullish" in comment.lower():
                    short_comment = "7/7 Bull"
                elif "initial" in comment.lower():
                    short_comment = "First DCA"
                elif "perfect" in comment.lower():
                    short_comment = "Perfect 7/7"
                else:
                    short_comment = "DCA Trade"
            else:
                short_comment = comment if comment else "DCA"

            # Ensure absolute maximum of 15 characters
            short_comment = short_comment[:15]
            logger.info(f"💬 Comment: '{short_comment}' (Length: {len(short_comment)})")

            # FIXED VOLUME VALIDATION
            volume = await self.validate_volume(symbol, volume)

            if volume <= 0:
                logger.error(f"❌ Invalid volume after validation: {volume}")
                return None

            logger.info(f"✅ Validated volume: {volume} lots")

            # ============ Dynamic filling mode with fallback ============
            filling_modes = [
                mt5.ORDER_FILLING_RETURN,
                mt5.ORDER_FILLING_FOK,
                mt5.ORDER_FILLING_IOC,
            ]

            result = None
            for attempt, filling_mode in enumerate(filling_modes):
                mode_name = {
                    mt5.ORDER_FILLING_RETURN: "RETURN",
                    mt5.ORDER_FILLING_FOK: "FOK",
                    mt5.ORDER_FILLING_IOC: "IOC",
                }.get(filling_mode, str(filling_mode))

                # Prepare order request with ULTRA-SHORT comment
                request = {
                    "action": mt5.TRADE_ACTION_DEAL,
                    "symbol": symbol,
                    "volume": float(volume),
                    "deviation": 20,
                    "magic": magic,
                    "comment": short_comment,
                    "type_filling": filling_mode,
                }

                # Set order type and price
                if order_type == mt5.ORDER_TYPE_BUY:
                    request["type"] = mt5.ORDER_TYPE_BUY
                    request["price"] = tick.ask  # Use ASK for BUY
                    logger.info(f"🟢 BUY Order: {volume} lots at {tick.ask:.5f}")
                else:  # ORDER_TYPE_SELL
                    request["type"] = mt5.ORDER_TYPE_SELL
                    request["price"] = tick.bid  # Use BID for SELL
                    logger.info(f"🔴 SELL Order: {volume} lots at {tick.bid:.5f}")

                # Add stop loss and take profit if provided
                if stop_loss:
                    request["sl"] = float(stop_loss)
                    logger.info(f"🛑 Stop Loss: {stop_loss:.5f}")
                if take_profit:
                    request["tp"] = float(take_profit)
                    logger.info(f"🎯 Take Profit: {take_profit:.5f}")

                # Send order
                logger.info(f"📤 Sending order (attempt {attempt+1}, filling={mode_name})...")
                result = mt5.order_send(request)

                if result is None:
                    error = mt5.last_error()
                    logger.error(f"❌ Order send returned None (attempt {attempt+1}). Error: {error}")
                    continue

                logger.info(f"📨 Order Result (attempt {attempt+1}):")
                logger.info(f"   - Retcode: {result.retcode}")
                logger.info(f"   - Comment: {result.comment}")
                logger.info(f"   - Order: {result.order}")
                logger.info(f"   - Volume: {result.volume}")

                # If retcode is 10030 (unsupported filling mode), try next mode
                if result.retcode == 10030:
                    logger.warning(f"⚠️ Filling mode {mode_name} rejected by broker, trying next...")
                    continue

                # Success codes
                success_codes = [mt5.TRADE_RETCODE_DONE, 10009, 10008]

                if result.retcode in success_codes:
                    logger.info(f"✅ ORDER EXECUTED SUCCESSFULLY!")
                    logger.info(f"   - Ticket: {result.order}")
                    logger.info(f"   - Price: {result.price}")
                    logger.info(f"   - Volume: {result.volume}")
                    return result
                else:
                    logger.error(f"❌ Order failed: {result.retcode} - {result.comment}")
                    return None

            # If we've exhausted all filling modes
            if result and result.retcode == 10030:
                logger.error(f"❌ All filling modes rejected by broker. Last error: {result.retcode} - {result.comment}")
            return None

        except Exception as e:
            logger.error(f"❌ Exception in place_order: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return None

    # ====== ORDER LEVEL CALCULATION ======

    async def calculate_order_levels(self, symbol, entry_price, side, stop_loss_pips, take_profit_pips):
        """Calculate stop loss and take profit levels for your broker"""
        try:
            symbol_info = await self.get_symbol_info(symbol)
            if not symbol_info:
                return None, None

            # Get pip value in price units
            pip_price = await self.get_pip_value(symbol)

            logging.info(f"Calculating levels for {symbol}: entry={entry_price:.5f}, pip_price={pip_price}")
            logging.info(f"  SL pips: {stop_loss_pips}, TP pips: {take_profit_pips}")

            # Calculate TP and SL in price
            if side == 'buy':
                stop_loss = entry_price - (stop_loss_pips * pip_price)
                take_profit = entry_price + (take_profit_pips * pip_price)
            else:  # sell
                stop_loss = entry_price + (stop_loss_pips * pip_price)
                take_profit = entry_price - (take_profit_pips * pip_price)

            # Round to correct digits
            stop_loss = round(stop_loss, symbol_info.digits)
            take_profit = round(take_profit, symbol_info.digits)

            logging.info(f"  Calculated levels: SL={stop_loss:.5f}, TP={take_profit:.5f}")
            logging.info(f"  Price difference: SL diff={abs(entry_price-stop_loss):.5f}, TP diff={abs(entry_price-take_profit):.5f}")

            return stop_loss, take_profit

        except Exception as e:
            logging.error(f"Error calculating order levels for {symbol}: {str(e)}")
            return None, None

    # ====== ATR CALCULATION METHODS ======

    async def calculate_atr(self, symbol, timeframe='H1', period=14, count=100):
        """Calculate Average True Range (ATR) for ATR grid strategy"""
        try:
            if not symbol:
                logging.warning("No symbol provided for ATR calculation")
                return 0.0

            # Get OHLCV data
            df = await self.get_ohlcv(symbol, timeframe, count * 2)
            if df is None or len(df) < period:
                logging.warning(f"Insufficient data for ATR calculation on {symbol}")
                return 0.0

            # Calculate True Range
            high = df['high']
            low = df['low']
            close = df['close']

            # True Range formula
            tr1 = high - low
            tr2 = abs(high - close.shift())
            tr3 = abs(low - close.shift())

            true_range = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

            # Calculate ATR
            atr = true_range.rolling(window=period).mean().iloc[-1]

            logging.debug(f"ATR for {symbol} ({timeframe}, {period}): {atr:.5f}")

            return atr
        except Exception as e:
            logging.error(f"Error calculating ATR for {symbol}: {str(e)}")
            return 0.0

    async def calculate_atr_pips(self, symbol, timeframe='H1', period=14):
        """Calculate ATR in pips for ATR grid strategy"""
        try:
            if not symbol:
                logging.warning("No symbol provided for ATR pips calculation")
                return 10.0

            atr = await self.calculate_atr(symbol, timeframe, period)
            pip_price = await self.get_pip_value(symbol)

            atr_pips = atr / pip_price if pip_price > 0 else 0

            return atr_pips
        except Exception as e:
            logging.error(f"Error calculating ATR pips: {str(e)}")
            return 10.0  # Default fallback

    # ====== GRID SPECIFIC METHODS ======

    async def calculate_grid_pip_value(self, symbol):
        """
        Calculate pip value specifically for grid trading calculations

        Args:
            symbol: Trading symbol

        Returns:
            Pip value in price units
        """
        try:
            if not symbol:
                logging.error("No symbol provided for grid pip value")
                return 0.0001

            # Use the dynamic pip value method
            return await self.get_pip_value(symbol)

        except Exception as e:
            logging.error(f"Error calculating grid pip value: {str(e)}")
            return 0.0001

    async def calculate_grid_position_size(self, symbol, risk_percent=2.0, grid_spacing_pips=10):
        """
        Calculate position size for grid trading

        Args:
            symbol: Trading symbol
            risk_percent: Risk percentage per grid level
            grid_spacing_pips: Distance between grid levels in pips

        Returns:
            Position size in lots
        """
        try:
            if not symbol:
                logging.error("No symbol provided for grid position size")
                return 0.01

            # Get account balance
            balance_info = await self.get_balance()
            balance = balance_info['balance']

            if balance <= 0:
                logging.error(f"Invalid balance: {balance}")
                return 0.01

            # Calculate risk amount per grid level
            risk_amount = balance * (risk_percent / 100)

            # Get pip value
            pip_value_per_lot = await self.calculate_pip_value(symbol, volume=1.0)

            # Calculate position size based on grid spacing
            # Risk is spread across the grid spacing
            position_size = risk_amount / (grid_spacing_pips * pip_value_per_lot)

            # Get symbol info for limits
            symbol_info = await self.get_symbol_info(symbol)
            if symbol_info:
                min_lot = symbol_info.volume_min if hasattr(symbol_info, 'volume_min') else 0.01
                max_lot = symbol_info.volume_max if hasattr(symbol_info, 'volume_max') else 1.0

                position_size = max(min_lot, min(position_size, max_lot))

                # Round to step size
                step = symbol_info.volume_step if hasattr(symbol_info, 'volume_step') else 0.01
                if step > 0:
                    position_size = round(position_size / step) * step

            # Final safety check
            position_size = min(position_size, 1.0)  # Max 1 lot for grid trading

            logging.info(f"Grid position size for {symbol}: {position_size:.2f} lots")
            logging.info(f"  Risk: ${risk_amount:.2f}, Grid Spacing: {grid_spacing_pips} pips")

            return position_size

        except Exception as e:
            logging.error(f"Error calculating grid position size: {str(e)}")
            return 0.01

    # ====== POSITION MANAGEMENT METHODS ======

    async def get_positions_with_orders(self, symbol=None):
        """
        Get both positions and pending orders for a symbol

        Args:
            symbol: Optional symbol filter

        Returns:
            Dict with positions and orders
        """
        try:
            positions = await self.get_open_positions(symbol)
            orders = await self.get_pending_orders(symbol)

            return {
                'positions': positions,
                'orders': orders,
                'total_count': len(positions) + len(orders)
            }
        except Exception as e:
            logging.error(f"Error getting positions with orders: {str(e)}")
            return {'positions': [], 'orders': [], 'total_count': 0}

    async def close_position_by_ticket(self, ticket, volume=None):
        """
        Close a position by ticket number

        Args:
            ticket: Position ticket number
            volume: Volume to close (None for full position)

        Returns:
            Close result or None if failed
        """
        try:
            return await self.close_position(ticket, volume)
        except Exception as e:
            logging.error(f"Error closing position by ticket: {str(e)}")
            return None

    async def close_all_positions(self, symbol=None, comment="Closed all"):
        """
        Close all open positions

        Args:
            symbol: Optional symbol filter
            comment: Comment for close orders

        Returns:
            List of close results
        """
        try:
            if not await self._ensure_mt5_connected():
                return []

            positions = await self.get_open_positions(symbol)
            results = []

            for position in positions:
                # Check magic number before closing
                if position.magic not in BOT_MAGIC_NUMBERS:
                    logger.info(f"Skipping non-bot position {position.ticket} (magic={position.magic})")
                    continue

                result = await self.close_position(position.ticket, comment=comment)
                if result:
                    results.append(result)
                    await asyncio.sleep(0.1)  # Rate limiting

            logging.info(f"Closed {len(results)}/{len(positions)} positions")
            return results

        except Exception as e:
            logging.error(f"Error closing all positions: {str(e)}")
            return []

    async def cancel_all_pending_orders(self, symbol=None):
        """
        Cancel all pending orders

        Args:
            symbol: Optional symbol filter

        Returns:
            List of cancel results
        """
        try:
            if not await self._ensure_mt5_connected():
                return []

            orders = await self.get_pending_orders(symbol)
            results = []

            for order in orders:
                # Check magic number before cancelling
                magic = getattr(order, 'magic', 0)
                if magic not in BOT_MAGIC_NUMBERS:
                    logger.info(f"Skipping non-bot order {order.ticket} (magic={magic})")
                    continue

                result = await self.cancel_order(order.ticket)
                if result:
                    results.append(result)
                    await asyncio.sleep(0.1)  # Rate limiting

            logging.info(f"Cancelled {len(results)}/{len(orders)} pending orders")
            return results

        except Exception as e:
            logging.error(f"Error cancelling all pending orders: {str(e)}")
            return []

    async def get_position_profit_pips(self, position):
        """
        Calculate profit in pips for a position

        Args:
            position: Position object from MT5

        Returns:
            Profit in pips
        """
        try:
            # Get current price
            current_price = await self.get_current_price(position.symbol)
            if not current_price:
                return 0.0

            # Get pip value
            pip_price = await self.get_pip_value(position.symbol)

            # Calculate profit in pips
            if position.type == mt5.ORDER_TYPE_BUY:
                profit_pips = (current_price - position.price_open) / pip_price
            else:  # SELL
                profit_pips = (position.price_open - current_price) / pip_price

            return profit_pips

        except Exception as e:
            logging.error(f"Error calculating position profit: {str(e)}")
            return 0.0

    async def get_position_profit_percent(self, position):
        """
        Calculate profit percentage for a position

        Args:
            position: Position object from MT5

        Returns:
            Profit percentage
        """
        try:
            profit_pips = await self.get_position_profit_pips(position)

            # Get pip value in account currency
            pip_value = await self.calculate_pip_value(position.symbol, position.volume)

            # Calculate profit percentage relative to position size
            position_value = position.volume * 100000  # Approximate
            if position_value > 0:
                profit_percent = (profit_pips * pip_value / position_value) * 100
                return profit_percent

            return 0.0

        except Exception as e:
            logging.error(f"Error calculating position profit percent: {str(e)}")
            return 0.0

    async def check_order_filled(self, order_id):
        """
        Check if a pending order has been filled

        Args:
            order_id: Order ticket number

        Returns:
            True if filled, False if still pending, None if not found
        """
        try:
            if not order_id:
                return None

            if not await self._ensure_mt5_connected():
                return None

            # Check if order still exists
            order = await self.get_order_info(order_id)
            if order is None:
                # Order might be filled and moved to positions
                positions = mt5.positions_get(ticket=order_id)
                if positions and len(positions) > 0:
                    return True  # Filled and now a position
                return None  # Not found

            # Order still exists, check its state
            if hasattr(order, 'state'):
                if order.state == mt5.ORDER_STATE_FILLED:
                    return True
                elif order.state == mt5.ORDER_STATE_PLACED:
                    return False
                elif order.state == mt5.ORDER_STATE_CANCELED:
                    return None

            return False

        except Exception as e:
            logging.error(f"Error checking order filled: {str(e)}")
            return None

    @property
    def dry_run_mode(self):
        """Check if we're in dry run mode"""
        return not self._connected


# ====== HELPER FUNCTIONS ======

def get_mt5_timeframe(timeframe_str):
    """Convert timeframe string to MT5 constant (includes H12)."""
    tf_mapping = {
        'M1':  mt5.TIMEFRAME_M1,
        'M5':  mt5.TIMEFRAME_M5,
        'M15': mt5.TIMEFRAME_M15,
        'M30': mt5.TIMEFRAME_M30,
        'H1':  mt5.TIMEFRAME_H1,
        'H4':  mt5.TIMEFRAME_H4,
        'H12': mt5.TIMEFRAME_H12,   # ← NEW
        'D1':  mt5.TIMEFRAME_D1,
        'W1':  mt5.TIMEFRAME_W1,
        'MN1': mt5.TIMEFRAME_MN1,
    }
    return tf_mapping.get(timeframe_str, mt5.TIMEFRAME_H1)


def format_price(price, digits):
    """Format price with correct number of digits"""
    return round(price, digits)


def calculate_pips_difference(price1, price2, symbol):
    """Calculate pips difference between two prices"""
    if not symbol or price1 is None or price2 is None:
        return 0.0

    # Use the pip value from the symbol
    if 'JPY' in symbol:
        pip_value = 0.01
    elif 'BTC' in symbol:
        pip_value = 1.0
    else:
        pip_value = 0.0001

    return abs(price1 - price2) / pip_value


# ====== TEST FUNCTION ======

async def test_mt5_connection():
    """Test MT5 connection and basic functionality"""
    try:
        # Initialize connection
        mt5_conn = MT5Connection()

        # Check if MT5 is initialized
        if not mt5.initialize():
            print("❌ MT5 initialization failed")
            return False

        print("✅ MT5 initialized successfully")

        # Test getting account info
        account_info = await mt5_conn.get_account_info()
        if account_info:
            print(f"✅ Account info retrieved: {account_info.login}, Balance: {account_info.balance}")
        else:
            print("⚠️  Could not get account info")

        # Test getting symbol info
        symbol = "BTCUSD"
        symbol_info = await mt5_conn.get_symbol_info(symbol)
        if symbol_info:
            print(f"✅ Symbol info for {symbol}: Digits={symbol_info.digits}, Point={symbol_info.point}")
        else:
            print(f"❌ Could not get symbol info for {symbol}")

        # Test getting tick data
        tick = await mt5_conn.get_tick(symbol)
        if tick:
            print(f"✅ Tick data for {symbol}: Bid={tick.bid}, Ask={tick.ask}")
        else:
            print(f"❌ Could not get tick data for {symbol}")

        # Test getting current price
        price = await mt5_conn.get_current_price(symbol)
        if price:
            print(f"✅ Current price for {symbol}: {price:.5f}")
        else:
            print(f"❌ Could not get current price for {symbol}")

        # Test getting pip value
        pip_value = await mt5_conn.get_pip_value(symbol)
        print(f"✅ Pip value for {symbol}: {pip_value}")

        # Test cached pip value
        pip_value_cached = await mt5_conn.get_pip_value_cached(symbol)
        print(f"✅ Cached pip value for {symbol}: {pip_value_cached}")

        # Test calculating pip value in USD
        pip_usd = await mt5_conn.calculate_pip_value(symbol, volume=1.0)
        print(f"✅ Pip value in USD for {symbol}: ${pip_usd:.2f}")

        # Test H12 fetch (newly supported for SuperTrend Oracle priming)
        df_h12 = await mt5_conn.get_ohlcv(symbol, 'H12', 50)
        if df_h12 is not None and not df_h12.empty:
            print(f"✅ H12 fetch OK for {symbol}: {len(df_h12)} bars")
        else:
            print(f"⚠️  H12 fetch returned no data for {symbol} (broker may not support it)")

        # Shutdown
        await mt5_conn.shutdown()
        print("✅ MT5 shutdown successfully")

        return True

    except Exception as e:
        print(f"❌ Test failed: {str(e)}")
        import traceback
        print(f"Traceback: {traceback.format_exc()}")
        return False


if __name__ == "__main__":
    # Run test
    asyncio.run(test_mt5_connection())