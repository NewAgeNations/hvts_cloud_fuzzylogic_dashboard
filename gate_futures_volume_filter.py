#!/usr/bin/env python3
"""
gate_futures_volume_filter.py

Utility to fetch all Gate.io futures symbols and filter those with daily trading volume
above a specified threshold (default: 1,000,000 USDT).

ENHANCED: Now supports Pivot Zone filtering to constrain analysis within specific
ATH-based zones. You can specify one or more zones to filter symbols.

ENHANCED: Now supports GMMA Daily timeframe signal filtering to identify symbols
with bullish/bearish daily trends before including them.

ENHANCED: Added get_filtered_symbols_for_bot() convenience function for integration
with the HVTS Bitcoin DCA bot's dynamic symbol filtering.

Updated to work with the HVTS Bitcoin DCA bot's symbol list and provide better
output formatting with pivot zone information.

Usage:
    python gate_futures_volume_filter.py [--min-volume 1000000] [--format list|json] 
                                         [--update-symbols] [--top 50] [--compare]
                                         [--AboveBuy] [--StrongSupport] [--ReversalZone]
                                         [--AccumulationZone] [--ExtremeDiscount]
                                         [--GMMA] [--GMMA-timeframe 1d]

Examples:
    # List all symbols with volume > 1M USDT
    python gate_futures_volume_filter.py --min-volume 1000000
    
    # Filter to Extreme Discount zone only
    python gate_futures_volume_filter.py --min-volume 2000000 --ExtremeDiscount
    
    # Filter to Accumulation AND Extreme zones
    python gate_futures_volume_filter.py --AccumulationZone --ExtremeDiscount
    
    # Filter to symbols with bullish daily GMMA signal
    python gate_futures_volume_filter.py --GMMA --GMMA-signal bullish
    
    # Output as JSON with zone info
    python gate_futures_volume_filter.py --min-volume 2000000 --format json --ExtremeDiscount
    
    # Update the bot's symbol list with top N symbols by volume in specific zones
    python gate_futures_volume_filter.py --update-symbols --top 50 --AccumulationZone --ExtremeDiscount
"""

import argparse
import json
import sys
import time
import os
import asyncio
import re
from typing import List, Dict, Any, Optional, Set, Tuple
from pathlib import Path
from datetime import datetime, timedelta

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

try:
    import ccxt
except ImportError:
    print("❌ ccxt not installed. Please install: pip install ccxt")
    sys.exit(1)

try:
    import requests
except ImportError:
    print("❌ requests not installed. Please install: pip install requests")
    sys.exit(1)

# Import pivot zone helper
try:
    from pivot_analysis import get_zone_for_symbol, ZONE_ABOVE_BUY, ZONE_STRONG_SUP, \
        ZONE_REVERSAL, ZONE_ACCUMULATION, ZONE_EXTREME
except ImportError:
    print("⚠️ pivot_analysis.py not found. Zone filtering will be disabled.")
    # Define fallback constants
    ZONE_ABOVE_BUY = "Above Buy Zone"
    ZONE_STRONG_SUP = "Strong Support"
    ZONE_REVERSAL = "Reversal Zone"
    ZONE_ACCUMULATION = "Accumulation Zone"
    ZONE_EXTREME = "Extreme Discount"
    
    async def get_zone_for_symbol(symbol: str, price: float) -> str:
        return "Unknown (pivot_analysis not available)"

# Import GMMA helper
try:
    from exchange import fetch_ohlcv as gate_fetch_ohlcv, \
        get_exchange_connection as get_gate_connection
except ImportError:
    print("⚠️ exchange.py not found. GMMA filtering will be disabled.")
    gate_fetch_ohlcv = None
    get_gate_connection = None

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
def _env_min_volume(default: float = 1_000_000.0) -> float:
    """Read the volume threshold (24h USDT) from CRYPTO_MIN_VOLUME_USDT in .env."""
    raw = os.getenv("CRYPTO_MIN_VOLUME_USDT", "").strip().replace("_", "").replace(",", "")
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


DEFAULT_MIN_VOLUME = _env_min_volume()  # USDT, configurable via CRYPTO_MIN_VOLUME_USDT
GATE_API_URL = "https://api.gateio.ws/api/v4/futures/usdt/tickers"
BOT_SYMBOLS_FILE = "HVTS-BitcoinDCA.py"
SYMBOL_SECTION_START = "# Tier 1 — Ultra High Volume (Top 10 by volume) - UNCHANGED"
SYMBOL_SECTION_END = "# Combined list for iteration"

# Zone name constants (mirrors pivot_analysis.py)
ZONE_NAMES = {
    'above_buy': ZONE_ABOVE_BUY,
    'strong_support': ZONE_STRONG_SUP,
    'reversal': ZONE_REVERSAL,
    'accumulation': ZONE_ACCUMULATION,
    'extreme': ZONE_EXTREME,
}

# GMMA timeframe mapping
GMMA_TIMEFRAMES = {
    '1h': '1h',
    '4h': '4h',
    '1d': '1d',
    'daily': '1d',
    '1w': '1w',
    'weekly': '1w',
}

# GMMA signal types
GMMA_SIGNAL_BULLISH = "bullish"
GMMA_SIGNAL_BEARISH = "bearish"
GMMA_SIGNAL_STRONG_BULLISH = "strong_bullish"
GMMA_SIGNAL_STRONG_BEARISH = "strong_bearish"
GMMA_SIGNAL_NEUTRAL = "neutral"

# -----------------------------------------------------------------------------
# GMMA Signal Detection
# -----------------------------------------------------------------------------

class GMMASignalDetector:
    """Detects GMMA signals for symbols using the exchange module."""
    
    def __init__(self, timeframe: str = '1d', min_bars: int = 70):
        self.timeframe = GMMA_TIMEFRAMES.get(timeframe, '1d')
        self.min_bars = min_bars
        self._cache: Dict[str, Tuple[str, float]] = {}  # symbol -> (signal, timestamp)
        self._cache_ttl = 3600  # 1 hour cache
    
    def _is_cache_valid(self, symbol: str) -> bool:
        """Check if cached signal is still valid."""
        if symbol not in self._cache:
            return False
        _, timestamp = self._cache[symbol]
        return (time.time() - timestamp) < self._cache_ttl
    
    async def get_signal(self, symbol: str) -> Optional[str]:
        """
        Get GMMA signal for a symbol on the configured timeframe.
        
        Returns:
            'bullish', 'bearish', 'neutral', or None if error
        """
        # Check cache first
        if self._is_cache_valid(symbol):
            signal, _ = self._cache[symbol]
            return signal
        
        if gate_fetch_ohlcv is None:
            return None
        
        try:
            # Fetch OHLCV data
            ohlcv = await gate_fetch_ohlcv(symbol, self.timeframe, 200)
            
            if not ohlcv or len(ohlcv) < self.min_bars:
                return None
            
            # Calculate GMMA signal
            signal = self._calculate_gmma_signal(ohlcv)
            
            # Cache the result
            self._cache[symbol] = (signal, time.time())
            
            return signal
            
        except Exception as e:
            print(f"   ⚠️ Error getting GMMA signal for {symbol}: {e}", file=sys.stderr)
            return None
    
    def _calculate_gmma_signal(self, ohlcv: List) -> str:
        """
        Calculate GMMA signal from OHLCV data.
        
        Uses the same GMMA logic as the bot:
        - Short EMAs: [4, 7, 10, 13, 16, 20]
        - Long EMAs: [30, 36, 42, 48, 54, 60]
        """
        try:
            import pandas as pd
            import numpy as np
            
            # Convert to DataFrame
            df = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
            close = df['close']
            
            # GMMA periods
            short_periods = [4, 7, 10, 13, 16, 20]
            long_periods = [30, 36, 42, 48, 54, 60]
            
            # Calculate EMAs
            short_emas = [close.ewm(span=p, adjust=False).mean() for p in short_periods]
            long_emas = [close.ewm(span=p, adjust=False).mean() for p in long_periods]
            
            # Get latest values
            short_avg = np.mean([ema.iloc[-1] for ema in short_emas])
            long_avg = np.mean([ema.iloc[-1] for ema in long_emas])
            price = close.iloc[-1]
            
            # Calculate normalized spread
            spread = short_avg - long_avg
            norm_spread = abs(spread) / price if price > 0 else 0
            
            # Determine signal
            if short_avg > long_avg:
                if norm_spread > 0.005:
                    signal = GMMA_SIGNAL_STRONG_BULLISH
                else:
                    signal = GMMA_SIGNAL_BULLISH
            elif short_avg < long_avg:
                if norm_spread > 0.005:
                    signal = GMMA_SIGNAL_STRONG_BEARISH
                else:
                    signal = GMMA_SIGNAL_BEARISH
            else:
                signal = GMMA_SIGNAL_NEUTRAL
            
            return signal
            
        except Exception as e:
            print(f"   ⚠️ Error calculating GMMA signal: {e}", file=sys.stderr)
            return GMMA_SIGNAL_NEUTRAL
    
    async def get_bullish_symbols(self, symbols: List[str]) -> Set[str]:
        """
        Get set of symbols with bullish GMMA signals.
        
        Args:
            symbols: List of symbols to check
            
        Returns:
            Set of symbols with bullish signals
        """
        if not symbols:
            return set()
        
        print(f"\n📈 Checking GMMA signals for {len(symbols)} symbols ({self.timeframe} timeframe)...", file=sys.stderr)
        
        bullish_symbols = set()
        checked = 0
        
        # Process in batches to avoid rate limits
        batch_size = 20
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            
            # Get signals for batch
            tasks = [self.get_signal(sym) for sym in batch]
            results = await asyncio.gather(*tasks)
            
            for symbol, signal in zip(batch, results):
                checked += 1
                if signal in (GMMA_SIGNAL_BULLISH, GMMA_SIGNAL_STRONG_BULLISH):
                    bullish_symbols.add(symbol)
                elif signal == GMMA_SIGNAL_NEUTRAL:
                    # Neutral is acceptable for filtering
                    pass
                # Bearish symbols are excluded
            
            print(f"   Progress: {checked}/{len(symbols)} symbols checked", file=sys.stderr)
            
            # Rate limit
            if i + batch_size < len(symbols):
                await asyncio.sleep(0.5)
        
        print(f"   ✅ Found {len(bullish_symbols)} bullish symbols", file=sys.stderr)
        return bullish_symbols

# -----------------------------------------------------------------------------
# Pivot Zone Flag Parsing
# -----------------------------------------------------------------------------

def parse_zone_flags(args) -> Set[str]:
    """
    Parse CLI arguments and return a set of allowed zone names.
    
    Args:
        args: Parsed argparse arguments
        
    Returns:
        Set of zone name strings
    """
    zones = set()
    if hasattr(args, 'AboveBuy') and args.AboveBuy:
        zones.add(ZONE_ABOVE_BUY)
    if hasattr(args, 'StrongSupport') and args.StrongSupport:
        zones.add(ZONE_STRONG_SUP)
    if hasattr(args, 'ReversalZone') and args.ReversalZone:
        zones.add(ZONE_REVERSAL)
    if hasattr(args, 'AccumulationZone') and args.AccumulationZone:
        zones.add(ZONE_ACCUMULATION)
    if hasattr(args, 'ExtremeDiscount') and args.ExtremeDiscount:
        zones.add(ZONE_EXTREME)
    return zones

def get_zone_flag_help() -> str:
    """Return help text for pivot zone flags."""
    return """
  PIVOT ZONE FILTERING FLAGS:
    --AboveBuy                Include symbols in Above Buy Zone (0-25% discount)
    --StrongSupport           Include symbols in Strong Support (25-50% discount)
    --ReversalZone            Include symbols in Reversal Zone (50-75% discount)
    --AccumulationZone        Include symbols in Accumulation Zone (75-90% discount)
    --ExtremeDiscount         Include symbols in Extreme Discount (90-100% discount)
    
    If no zone flags are specified, all symbols are included (no zone filtering).
    Multiple zone flags can be combined (e.g., --AccumulationZone --ExtremeDiscount)
    """

def get_gmma_help() -> str:
    """Return help text for GMMA flags."""
    return """
  GMMA SIGNAL FILTERING FLAGS:
    --GMMA                    Enable GMMA signal filtering
    --GMMA-timeframe {1h,4h,1d,daily,1w,weekly}
                              Timeframe for GMMA signal (default: 1d)
    --GMMA-signal {bullish,bearish,neutral}
                              Signal type to filter (default: bullish)
                              - bullish: includes bullish and strong_bullish
                              - bearish: includes bearish and strong_bearish
                              - neutral: includes neutral only
    
    Example: --GMMA --GMMA-timeframe 1d --GMMA-signal bullish
    """

# -----------------------------------------------------------------------------
# Core Fetching Functions
# -----------------------------------------------------------------------------

def fetch_tickers_direct(min_volume: float) -> List[Dict]:
    """
    Fetch futures tickers using ccxt with batch processing.
    Returns list of dicts with symbol, volume, price, change.
    """
    try:
        exchange = ccxt.gateio({
            'options': {
                'defaultType': 'swap',  # 'swap' = perpetual futures
            },
            'enableRateLimit': True,
            'timeout': 30000,  # 30 seconds
        })

        print(f"🔄 Fetching Gate.io futures markets...", file=sys.stderr)
        
        # Load markets first
        exchange.load_markets()
        
        # Get all futures symbols
        all_futures_symbols = []
        for symbol, market in exchange.markets.items():
            # Check if this is a futures market
            if market.get('swap') or market.get('future') or market.get('perpetual'):
                all_futures_symbols.append(symbol)
        
        print(f"📊 Found {len(all_futures_symbols)} futures symbols", file=sys.stderr)
        
        if not all_futures_symbols:
            print("⚠️  No futures symbols found. Trying alternative method...", file=sys.stderr)
            # Try alternative: fetch tickers with explicit symbols list
            all_symbols = list(exchange.markets.keys())
            # Filter for USDT pairs that look like futures
            futures_candidates = [s for s in all_symbols if '_USDT' in s or 'USDT' in s]
            
            if futures_candidates:
                test_symbols = futures_candidates[:10]
                try:
                    test_tickers = exchange.fetch_tickers(test_symbols)
                    if test_tickers:
                        print(f"✅ Successfully fetched tickers for {len(test_tickers)} symbols", file=sys.stderr)
                        all_futures_symbols = futures_candidates
                except Exception as e:
                    print(f"⚠️  Error fetching test tickers: {e}", file=sys.stderr)
        
        # Fetch tickers in batches to avoid rate limits
        print(f"🔄 Fetching tickers for {len(all_futures_symbols)} symbols...", file=sys.stderr)
        
        high_volume_symbols = []
        batch_size = 50
        total_symbols = len(all_futures_symbols)
        
        for i in range(0, total_symbols, batch_size):
            batch = all_futures_symbols[i:i+batch_size]
            batch_num = i//batch_size + 1
            total_batches = (total_symbols + batch_size - 1)//batch_size
            print(f"   Fetching batch {batch_num}/{total_batches}...", file=sys.stderr)
            
            try:
                tickers = exchange.fetch_tickers(batch)
                
                for symbol, ticker in tickers.items():
                    # quoteVolume is the 24h trading volume in quote currency (USDT)
                    volume = ticker.get('quoteVolume', 0.0)
                    
                    # Some exchanges use 'info' field for additional data
                    if volume is None or volume == 0:
                        # Try alternative fields
                        base_vol = ticker.get('baseVolume', 0.0)
                        last = ticker.get('last', 0)
                        volume = base_vol * last if base_vol and last else 0
                    
                    if volume >= min_volume:
                        # Convert from Gate.io format to readable format
                        readable_symbol = symbol.replace('_', '/')
                        # Remove :USDT suffix if present
                        if ':USDT' in readable_symbol:
                            readable_symbol = readable_symbol.replace(':USDT', '')
                        high_volume_symbols.append({
                            'symbol': readable_symbol,
                            'volume_usdt': volume,
                            'last_price': ticker.get('last', 0),
                            'change_24h': ticker.get('percentage', 0)
                        })
                
                # Respect rate limits
                if i + batch_size < total_symbols:
                    time.sleep(0.5)
                    
            except Exception as e:
                print(f"⚠️  Error fetching batch: {e}", file=sys.stderr)
                continue
        
        # Sort by volume (highest first)
        high_volume_symbols.sort(key=lambda x: x['volume_usdt'], reverse=True)
        
        print(f"✅ Found {len(high_volume_symbols)} symbols with volume > ${min_volume:,.0f}", file=sys.stderr)
        return high_volume_symbols

    except Exception as e:
        print(f"Error fetching futures data from Gate.io: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return []


def _ticker_quote_volume(ticker: Dict[str, Any]) -> float:
    """
    24h traded volume in USDT for one Gate.io futures ticker.

    Gate's /futures/usdt/tickers payload exposes volume_24h_quote (quote
    currency = USDT) and volume_24h_settle (settle currency = USDT for
    linear contracts). 'volume_usdt' is kept as a last-resort fallback for
    older payloads.
    """
    for key in ("volume_24h_quote", "volume_24h_settle", "volume_usdt"):
        raw = ticker.get(key)
        if raw in (None, ""):
            continue
        try:
            val = float(raw)
        except (TypeError, ValueError):
            continue
        if val > 0:
            return val
    return 0.0


def fetch_tickers_alternative(min_volume: float, quiet: bool = False) -> List[Dict]:
    """
    Alternative method using public API endpoint directly.
    This bypasses ccxt for more reliable futures data (one HTTP call).

    Args:
        min_volume: minimum 24h USDT volume (0 returns every contract).
        quiet: suppress progress/error prints (used when called from the
               full-screen dashboard so stderr does not corrupt the display).
    """
    def _log(msg: str) -> None:
        if not quiet:
            print(msg, file=sys.stderr)

    try:
        _log("🔄 Fetching futures data from Gate.io API...")

        response = requests.get(GATE_API_URL, timeout=30)
        response.raise_for_status()

        data = response.json()

        high_volume_symbols = []

        for ticker in data:
            symbol = ticker.get('contract')
            if not symbol:
                continue
            volume = _ticker_quote_volume(ticker)

            if volume >= min_volume:
                # Convert to readable format
                readable_symbol = symbol.replace('_', '/')
                try:
                    last_price = float(ticker.get('last', 0) or 0)
                except (TypeError, ValueError):
                    last_price = 0.0
                try:
                    change = float(ticker.get('change_percentage', 0) or 0)
                except (TypeError, ValueError):
                    change = 0.0
                high_volume_symbols.append({
                    'symbol': readable_symbol,
                    'volume_usdt': volume,
                    'last_price': last_price,
                    'change_24h': change
                })

        # Sort by volume (highest first)
        high_volume_symbols.sort(key=lambda x: x['volume_usdt'], reverse=True)

        _log(f"✅ Found {len(high_volume_symbols)} symbols with volume > ${min_volume:,.0f}")
        return high_volume_symbols

    except Exception as e:
        _log(f"Error with alternative method: {e}")
        return []


GATE_CANDLES_URL = "https://api.gateio.ws/api/v4/futures/usdt/candlesticks"
_CANDLE_INTERVALS = {"1d": "1d", "d1": "1d", "daily": "1d", "1h": "1h", "h1": "1h",
                     "4h": "4h", "h4": "4h", "1w": "7d", "weekly": "7d"}
_candle_session = None


def fetch_candles_public(symbol: str, timeframe: str = "1d", limit: int = 120,
                         timeout: float = 10.0, attempts: int = 3) -> Optional[List[List[float]]]:
    """
    Fetch candles from Gate.io's PUBLIC futures endpoint — no API keys, no ccxt,
    no load_markets(). Returns ccxt-style rows [ts_ms, open, high, low, close, volume]
    sorted oldest→newest, or None on failure. Blocking: call via asyncio.to_thread.
    """
    global _candle_session
    if _candle_session is None:
        _candle_session = requests.Session()
    interval = _CANDLE_INTERVALS.get(str(timeframe).lower(), "1d")
    params = {"contract": symbol.replace("/", "_").replace(":USDT", ""),
              "interval": interval, "limit": int(limit)}
    for attempt in range(1, attempts + 1):
        try:
            resp = _candle_session.get(GATE_CANDLES_URL, params=params, timeout=timeout)
            if resp.status_code == 429:               # rate limited → back off and retry
                time.sleep(1.0 * attempt)
                continue
            resp.raise_for_status()
            rows = []
            for c in resp.json():
                rows.append([int(c["t"]) * 1000, float(c["o"]), float(c["h"]),
                             float(c["l"]), float(c["c"]), float(c.get("v") or 0)])
            rows.sort(key=lambda r: r[0])
            return rows
        except Exception:
            if attempt < attempts:
                time.sleep(0.5 * attempt)
    return None


async def apply_zone_filter(
    symbols: List[Dict],
    allowed_zones: Set[str],
    max_concurrent: int = 5
) -> List[Dict]:
    """
    Apply pivot zone filtering to a list of symbols.
    
    Args:
        symbols: List of symbol dicts with 'symbol' and 'last_price'
        allowed_zones: Set of allowed zone names
        max_concurrent: Maximum concurrent zone lookups
        
    Returns:
        Filtered list with 'zone' field added to each item
    """
    if not allowed_zones:
        # No filtering - just add zone as N/A
        for item in symbols:
            item['zone'] = "N/A (no filter)"
        return symbols
    
    print(f"\n🔍 Filtering by allowed zones: {', '.join(sorted(allowed_zones))}", file=sys.stderr)
    
    # Use semaphore to limit concurrent lookups
    semaphore = asyncio.Semaphore(max_concurrent)
    
    async def get_zone_with_semaphore(item: Dict) -> Dict:
        async with semaphore:
            symbol = item['symbol']
            price = item['last_price']
            try:
                zone = await get_zone_for_symbol(symbol, price)
                item['zone'] = zone
                return item
            except Exception as e:
                print(f"   ⚠️ Error getting zone for {symbol}: {e}", file=sys.stderr)
                item['zone'] = "Error"
                return item
    
    # Process all symbols with progress indicator
    total = len(symbols)
    print(f"   Analysing {total} symbols for zone classification...", file=sys.stderr)
    
    tasks = [get_zone_with_semaphore(item) for item in symbols]
    results = []
    
    for i, task in enumerate(asyncio.as_completed(tasks), 1):
        result = await task
        results.append(result)
        if i % 10 == 0 or i == total:
            print(f"   Progress: {i}/{total} symbols analysed", file=sys.stderr)
    
    # Filter by allowed zones
    filtered = [item for item in results if item.get('zone') in allowed_zones]
    
    print(f"   ✅ {len(filtered)} symbols remain after zone filter", file=sys.stderr)
    
    # Show zone breakdown
    zone_counts = {}
    for item in filtered:
        zone = item.get('zone', 'Unknown')
        zone_counts[zone] = zone_counts.get(zone, 0) + 1
    
    if zone_counts:
        print(f"   Zone breakdown:", file=sys.stderr)
        for zone, count in sorted(zone_counts.items()):
            print(f"      {zone}: {count}", file=sys.stderr)
    
    return filtered


async def apply_gmma_filter(
    symbols: List[Dict],
    signal_type: str = 'bullish',
    timeframe: str = '1d'
) -> List[Dict]:
    """
    Apply GMMA signal filtering to a list of symbols.
    
    Args:
        symbols: List of symbol dicts with 'symbol' field
        signal_type: 'bullish', 'bearish', or 'neutral'
        timeframe: GMMA timeframe (1h, 4h, 1d, 1w)
        
    Returns:
        Filtered list with 'gmma_signal' field added
    """
    if not symbols:
        return []
    
    print(f"\n📊 Filtering by GMMA {signal_type} signals ({timeframe} timeframe)...", file=sys.stderr)
    
    detector = GMMASignalDetector(timeframe=timeframe)
    
    # Get signals for all symbols
    symbol_list = [item['symbol'] for item in symbols]
    bullish_set = set()
    
    # Determine which signals to keep
    keep_signals = set()
    if signal_type == 'bullish':
        keep_signals = {GMMA_SIGNAL_BULLISH, GMMA_SIGNAL_STRONG_BULLISH}
    elif signal_type == 'bearish':
        keep_signals = {GMMA_SIGNAL_BEARISH, GMMA_SIGNAL_STRONG_BEARISH}
    elif signal_type == 'neutral':
        keep_signals = {GMMA_SIGNAL_NEUTRAL}
    else:
        keep_signals = {GMMA_SIGNAL_BULLISH, GMMA_SIGNAL_STRONG_BULLISH}
    
    # Process in batches
    batch_size = 20
    filtered_symbols = []
    signal_counts = {
        'bullish': 0,
        'strong_bullish': 0,
        'bearish': 0,
        'strong_bearish': 0,
        'neutral': 0,
        'error': 0
    }
    
    for i in range(0, len(symbol_list), batch_size):
        batch = symbol_list[i:i+batch_size]
        
        # Get signals
        tasks = [detector.get_signal(sym) for sym in batch]
        results = await asyncio.gather(*tasks)
        
        for symbol, signal in zip(batch, results):
            # Add signal to item
            item = next((s for s in symbols if s['symbol'] == symbol), None)
            if item:
                item['gmma_signal'] = signal if signal else 'error'
            
            # Count signals
            if signal in signal_counts:
                signal_counts[signal] += 1
            else:
                signal_counts['error'] += 1
            
            # Keep if signal matches
            if signal and signal in keep_signals:
                filtered_symbols.append(symbol)
        
        print(f"   Progress: {len(filtered_symbols)}/{len(symbols)} symbols matched", file=sys.stderr)
        
        # Rate limit
        if i + batch_size < len(symbol_list):
            await asyncio.sleep(0.5)
    
    # Show signal breakdown
    print(f"\n📊 GMMA Signal Breakdown ({len(symbols)} symbols):", file=sys.stderr)
    print(f"   🟢 Bullish: {signal_counts.get('bullish', 0) + signal_counts.get('strong_bullish', 0)}", file=sys.stderr)
    print(f"   🔴 Bearish: {signal_counts.get('bearish', 0) + signal_counts.get('strong_bearish', 0)}", file=sys.stderr)
    print(f"   ⚪ Neutral: {signal_counts.get('neutral', 0)}", file=sys.stderr)
    print(f"   ❌ Error: {signal_counts.get('error', 0)}", file=sys.stderr)
    
    # Filter symbols list
    filtered_items = [s for s in symbols if s['symbol'] in filtered_symbols]
    
    print(f"   ✅ {len(filtered_items)} symbols remain after GMMA filter", file=sys.stderr)
    
    return filtered_items


async def fetch_and_filter_symbols(
    min_volume: float,
    allowed_zones: Set[str],
    output_format: str = "list",
    use_alternative: bool = False,
    max_concurrent: int = 5,
    enable_gmma: bool = False,
    gmma_timeframe: str = "1d",
    gmma_signal: str = "bullish"
) -> List[Dict]:
    """
    Main function to fetch symbols and apply zone filtering.
    
    Args:
        min_volume: Minimum daily volume in USDT
        allowed_zones: Set of allowed zone names
        output_format: 'list' or 'json'
        use_alternative: Use alternative API method
        max_concurrent: Maximum concurrent zone lookups
        enable_gmma: Enable GMMA signal filtering
        gmma_timeframe: GMMA timeframe (1h, 4h, 1d, 1w)
        gmma_signal: Signal type (bullish, bearish, neutral)
        
    Returns:
        List of symbol dicts with zone and GMMA information
    """
    # Fetch symbols
    if use_alternative:
        symbols = fetch_tickers_alternative(min_volume)
    else:
        symbols = fetch_tickers_direct(min_volume)
    
    if not symbols:
        return []
    
    # Apply zone filter
    filtered = await apply_zone_filter(symbols, allowed_zones, max_concurrent)
    
    if not filtered:
        return []
    
    # Apply GMMA filter if enabled
    if enable_gmma:
        if gate_fetch_ohlcv is None:
            print("⚠️ GMMA filtering requires exchange.py. Skipping...", file=sys.stderr)
        else:
            filtered = await apply_gmma_filter(filtered, gmma_signal, gmma_timeframe)
    
    return filtered


# ============================================================================
# BOT INTEGRATION - CONVENIENCE FUNCTION
# ============================================================================

async def get_filtered_symbols_for_bot(
    min_volume: float = 1_000_000,
    zones: Set[str] = None,
    gmma_timeframe: str = '1d',
    gmma_signal: str = 'bullish',
    use_alternative: bool = False,
    max_concurrent: int = 5
) -> List[str]:
    """
    Convenience function for the HVTS Bitcoin DCA bot to get filtered symbols.
    Returns just the symbol names as a list.
    
    This function applies the triple-filter:
    1. Daily volume > min_volume (default: 1,000,000 USDT)
    2. GMMA signal matches gmma_signal on gmma_timeframe (default: bullish on 1d)
    3. Price within specified zones (default: Accumulation and Extreme Discount)
    
    Args:
        min_volume: Minimum daily volume in USDT (default: 1,000,000)
        zones: Set of allowed zone names (default: {Accumulation, Extreme})
        gmma_timeframe: GMMA timeframe (default: '1d')
        gmma_signal: GMMA signal type (default: 'bullish')
        use_alternative: Use alternative API method (default: False)
        max_concurrent: Maximum concurrent zone lookups (default: 5)
        
    Returns:
        List of symbol strings that pass all filters
        
    Example:
        symbols = await get_filtered_symbols_for_bot()
        # Returns ['BTC/USDT', 'ETH/USDT', ...] that meet all criteria
    """
    if zones is None:
        zones = {ZONE_ACCUMULATION, ZONE_EXTREME}
    
    print(f"\n🔍 Bot triple-filter running:")
    print(f"   📊 Min Volume: ${min_volume:,.0f} USDT")
    print(f"   📈 GMMA Signal: {gmma_signal} on {gmma_timeframe}")
    print(f"   📍 Zones: {', '.join(sorted(zones))}")
    
    filtered = await fetch_and_filter_symbols(
        min_volume=min_volume,
        allowed_zones=zones,
        enable_gmma=True,
        gmma_timeframe=gmma_timeframe,
        gmma_signal=gmma_signal,
        use_alternative=use_alternative,
        max_concurrent=max_concurrent
    )
    
    symbols = [item['symbol'] for item in filtered]
    print(f"✅ Bot triple-filter returned {len(symbols)} symbols")
    return symbols


# -----------------------------------------------------------------------------
# Output Functions
# -----------------------------------------------------------------------------

def output_results(
    symbols: List[Dict],
    min_volume: float,
    output_format: str = "list",
    allowed_zones: Set[str] = None,
    enable_gmma: bool = False,
    gmma_timeframe: str = "1d",
    gmma_signal: str = "bullish"
):
    """
    Print results in the requested format.
    
    Args:
        symbols: List of symbol dicts with zone information
        min_volume: Minimum volume threshold used
        output_format: 'list' or 'json'
        allowed_zones: Set of allowed zones (for display)
        enable_gmma: Whether GMMA filtering was applied
        gmma_timeframe: GMMA timeframe used
        gmma_signal: GMMA signal type used
    """
    if output_format == 'json':
        output = {
            'min_volume_usdt': min_volume,
            'zone_filter': sorted(allowed_zones) if allowed_zones else None,
            'gmma_filter': {
                'enabled': enable_gmma,
                'timeframe': gmma_timeframe if enable_gmma else None,
                'signal': gmma_signal if enable_gmma else None
            } if enable_gmma else None,
            'symbol_count': len(symbols),
            'symbols': [s['symbol'] for s in symbols],
            'details': symbols[:50]  # Show top 50 details
        }
        print(json.dumps(output, indent=2))
    else:
        # Plain list output with stats
        print(f"\n📊 Symbols with daily volume > ${min_volume:,.0f} USDT", file=sys.stderr)
        
        if allowed_zones:
            print(f"   Zone filter: {', '.join(sorted(allowed_zones))}", file=sys.stderr)
        
        if enable_gmma:
            print(f"   GMMA filter: {gmma_signal} ({gmma_timeframe} timeframe)", file=sys.stderr)
        
        print(f"   Total: {len(symbols)} symbols\n", file=sys.stderr)
        
        if symbols:
            # Print header with GMMA column if enabled
            if enable_gmma:
                print(f"  {'Symbol':<14} {'Volume (USDT)':<16} {'24h Change':<12} {'Zone':<20} {'GMMA':<12}")
                print(f"  {'-'*14} {'-'*16} {'-'*12} {'-'*20} {'-'*12}")
            else:
                print(f"  {'Symbol':<14} {'Volume (USDT)':<16} {'24h Change':<12} {'Zone':<20}")
                print(f"  {'-'*14} {'-'*16} {'-'*12} {'-'*20}")
            
            for item in symbols:
                vol = item['volume_usdt']
                vol_m = vol / 1_000_000
                change = item.get('change_24h', 0)
                change_str = f"{change:+.2f}%" if change else "N/A"
                zone = item.get('zone', 'Unknown')
                # Truncate zone name if too long
                if len(zone) > 20:
                    zone = zone[:17] + "..."
                
                if enable_gmma:
                    signal = item.get('gmma_signal', 'N/A')
                    signal_icon = {
                        'strong_bullish': '🟢🔵',
                        'bullish': '🟢',
                        'neutral': '⚪',
                        'bearish': '🔴',
                        'strong_bearish': '🔴🔵',
                    }.get(signal, '❓')
                    print(f"  {item['symbol']:<14} ${vol:>13,.0f}  {change_str:<12} {zone:<20} {signal_icon:<12}")
                else:
                    print(f"  {item['symbol']:<14} ${vol:>13,.0f}  {change_str:<12} {zone:<20}")
        else:
            print("   No symbols match the criteria.", file=sys.stderr)

# -----------------------------------------------------------------------------
# Symbol List Update Functions
# -----------------------------------------------------------------------------

def update_symbol_list(symbols: List[str], top_n: int = 50, output_file: str = BOT_SYMBOLS_FILE):
    """
    Update the bot's symbol list with the top N symbols by volume.
    This is a helper to generate a new symbol list for the bot.
    
    Args:
        symbols: List of symbol strings
        top_n: Number of top symbols to include
        output_file: Path to the bot's Python file
    """
    if not symbols:
        print("❌ No symbols to update")
        return
    
    top_symbols = symbols[:top_n]
    
    print(f"\n📝 Top {len(top_symbols)} symbols by volume:")
    for i, sym in enumerate(top_symbols, 1):
        print(f"  {i:3d}. {sym}")
    
    # Format for Python list
    formatted = "[\n    '" + "',\n    '".join(top_symbols) + "',\n]"
    
    print("\n" + "="*70)
    print("📋 COPY THIS INTO YOUR BOT'S SYMBOL LIST:")
    print("="*70)
    print(formatted)
    print("="*70)
    
    # Optionally save to file
    output_path = Path("symbols_top_{}.txt".format(top_n))
    output_path.write_text(formatted)
    print(f"\n✅ Saved to: {output_path}")
    
    return top_symbols


def compare_with_bot_symbols(fetched_symbols: List[str], bot_symbols_file: str = BOT_SYMBOLS_FILE):
    """
    Compare fetched symbols with the bot's current symbol list.
    Shows which symbols are missing or extra.
    
    Args:
        fetched_symbols: List of fetched symbol strings
        bot_symbols_file: Path to the bot's Python file
    """
    # Parse the bot's symbol list from the file
    bot_symbols = []
    try:
        with open(bot_symbols_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find the symbol lists in the file
        pattern = r"TIER[123]_SYMBOLS = \[(.*?)\]"
        matches = re.findall(pattern, content, re.DOTALL)
        
        for match in matches:
            # Extract symbols from the list
            symbols = re.findall(r"'([^']+)'", match)
            bot_symbols.extend(symbols)
        
        print(f"\n📊 Bot currently has {len(bot_symbols)} symbols")
    except Exception as e:
        print(f"⚠️ Could not read bot symbols file: {e}")
        return
    
    # Convert to sets for comparison
    fetched_set = set(fetched_symbols)
    bot_set = set(bot_symbols)
    
    missing_in_bot = fetched_set - bot_set
    extra_in_bot = bot_set - fetched_set
    
    print(f"\n📈 Fetched symbols: {len(fetched_set)}")
    print(f"📊 Bot symbols: {len(bot_set)}")
    print(f"🔄 Intersection: {len(fetched_set & bot_set)}")
    
    if missing_in_bot:
        print(f"\n❌ Symbols in fetched but NOT in bot ({len(missing_in_bot)}):")
        for sym in sorted(missing_in_bot)[:30]:
            print(f"  - {sym}")
        if len(missing_in_bot) > 30:
            print(f"  ... and {len(missing_in_bot) - 30} more")
    
    if extra_in_bot:
        print(f"\n⚠️ Symbols in bot but NOT in fetched ({len(extra_in_bot)}):")
        for sym in sorted(extra_in_bot)[:30]:
            print(f"  - {sym}")
        if len(extra_in_bot) > 30:
            print(f"  ... and {len(extra_in_bot) - 30} more")


# -----------------------------------------------------------------------------
# Main Entry Point
# -----------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Filter Gate.io futures symbols by daily trading volume, pivot zone, and GMMA signals.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=get_zone_flag_help() + "\n" + get_gmma_help()
    )
    
    # Volume and output options
    parser.add_argument(
        "--min-volume",
        type=float,
        default=DEFAULT_MIN_VOLUME,
        help=f"Minimum daily volume in USDT (default: {DEFAULT_MIN_VOLUME:,})"
    )
    parser.add_argument(
        "--format",
        choices=["list", "json"],
        default="list",
        help="Output format: plain list or JSON"
    )
    parser.add_argument(
        "--alternative",
        action="store_true",
        help="Use direct API call instead of ccxt (more reliable for futures)"
    )
    parser.add_argument(
        "--concurrent",
        type=int,
        default=5,
        help="Maximum concurrent zone lookups (default: 5)"
    )
    
    # Symbol list management
    parser.add_argument(
        "--update-symbols",
        action="store_true",
        help="Generate a new symbol list for the bot"
    )
    parser.add_argument(
        "--top",
        type=int,
        default=50,
        help="Number of top symbols to include when using --update-symbols (default: 50)"
    )
    parser.add_argument(
        "--compare",
        action="store_true",
        help="Compare fetched symbols with the bot's current symbol list"
    )
    
    # Pivot zone filtering flags
    parser.add_argument(
        "--AboveBuy",
        action="store_true",
        help="Include symbols in Above Buy Zone (0-25% discount from ATH)"
    )
    parser.add_argument(
        "--StrongSupport",
        action="store_true",
        help="Include symbols in Strong Support (25-50% discount from ATH)"
    )
    parser.add_argument(
        "--ReversalZone",
        action="store_true",
        help="Include symbols in Reversal Zone (50-75% discount from ATH)"
    )
    parser.add_argument(
        "--AccumulationZone",
        action="store_true",
        help="Include symbols in Accumulation Zone (75-90% discount from ATH)"
    )
    parser.add_argument(
        "--ExtremeDiscount",
        action="store_true",
        help="Include symbols in Extreme Discount (90-100% discount from ATH)"
    )
    
    # GMMA filtering flags
    parser.add_argument(
        "--GMMA",
        action="store_true",
        help="Enable GMMA signal filtering"
    )
    parser.add_argument(
        "--GMMA-timeframe",
        choices=["1h", "4h", "1d", "daily", "1w", "weekly"],
        default="1d",
        help="Timeframe for GMMA signal (default: 1d)"
    )
    parser.add_argument(
        "--GMMA-signal",
        choices=["bullish", "bearish", "neutral"],
        default="bullish",
        help="Signal type to filter (default: bullish)"
    )
    
    args = parser.parse_args()

    # Parse zone flags
    allowed_zones = parse_zone_flags(args)
    
    # Run async fetch
    print("\n" + "="*70, file=sys.stderr)
    print("  GATE.IO FUTURES VOLUME FILTER WITH PIVOT ZONE & GMMA SUPPORT", file=sys.stderr)
    print("="*70, file=sys.stderr)
    
    if allowed_zones:
        print(f"📍 Zone filter: {', '.join(sorted(allowed_zones))}", file=sys.stderr)
    else:
        print(f"📍 Zone filter: None (all zones included)", file=sys.stderr)
    
    if args.GMMA:
        print(f"📈 GMMA filter: {args.GMMA_signal} ({args.GMMA_timeframe} timeframe)", file=sys.stderr)
    
    print(f"📊 Minimum volume: ${args.min_volume:,.0f} USDT", file=sys.stderr)
    print("="*70 + "\n", file=sys.stderr)
    
    # Fetch and filter
    symbols = asyncio.run(fetch_and_filter_symbols(
        min_volume=args.min_volume,
        allowed_zones=allowed_zones,
        output_format=args.format,
        use_alternative=args.alternative,
        max_concurrent=args.concurrent,
        enable_gmma=args.GMMA,
        gmma_timeframe=args.GMMA_timeframe,
        gmma_signal=args.GMMA_signal
    ))
    
    if not symbols:
        print("❌ No symbols found matching criteria.", file=sys.stderr)
        sys.exit(1)
    
    # Output results
    output_results(
        symbols,
        args.min_volume,
        args.format,
        allowed_zones,
        args.GMMA,
        args.GMMA_timeframe,
        args.GMMA_signal
    )
    
    # Handle additional actions
    if args.update_symbols:
        symbol_names = [s['symbol'] for s in symbols]
        update_symbol_list(symbol_names, args.top)
    
    if args.compare:
        symbol_names = [s['symbol'] for s in symbols]
        compare_with_bot_symbols(symbol_names)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️ Operation cancelled by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)