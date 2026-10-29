# basic_smc.py - Binary State Machine with Database Persistence (Multi-timeframe)
"""
Basic Smart Money Concept (BMC) – Binary State Machine with Database Persistence
Detects Break of Structure (BOS) and maintains a persistent binary state.

NOW SUPPORTS MULTIPLE TIMEFRAMES (H1, H4, and D1)

UPDATED (2026-09-04):
    • CHANGED: Timeframes from M30/H1/H4 to H1/H4/D1 (Daily)
    • CHANGED: DEFAULT_BARS optimized for higher timeframes
    • CHANGED: DEFAULT_LOOKBACK adjusted for Daily timeframe
    • CHANGED: DEFAULT_SWING_LOOKBACK adjusted for Daily timeframe
    • FIXED: All functions now properly handle D1 timeframe
    • IMPROVED: Bootstrap bias with better trend detection for Daily
    • IMPROVED: Better error handling for all timeframes
    • ADDED: D1 to SUPPORTED_TIMEFRAMES

PREVIOUS UPDATES (2026-08-26):
    • ADDED: Multi-timeframe support (M30 and H4)
    • ADDED: timeframe parameter to all functions
    • ADDED: bars parameter for configurable data fetch
    • UPDATED: Database table now uses (symbol, timeframe) as composite key
    • UPDATED: All database functions include timeframe in queries
    • UPDATED: get_basic_smc_signal_details now accepts timeframe parameter
    • ADDED: DEFAULT_TIMEFRAME = 'H4' for backward compatibility
    • ADDED: SUPPORTED_TIMEFRAMES = ['M30', 'H4']
    • ADDED: DEFAULT_BARS = {'M30': 200, 'H4': 100}
    • ADDED: DEFAULT_SWING_LOOKBACK = {'M30': 3, 'H4': 3}
    • ADDED: Bootstrap method now works with both timeframes

PREVIOUS UPDATES (2026-08-15):
    • CHANGED: Timeframe from M30 to H4 for longer-term trend analysis
    • CHANGED: Default lookback from 100 to 60 bars (10 days of H4 data)
    • CHANGED: Default swing lookback from 2 to 3 for H4 timeframe
    • UPDATED: fetch_bars parameter to match H4 requirements
    • RETAINED: All existing binary state persistence and logic

PREVIOUS UPDATES (2026-08-01):
    • FIXED: Excessive logging spam – detection logs changed from INFO to DEBUG
    • ADDED: Signal cooldown mechanism to prevent repeated identical signal logging
    • FIXED: get_basic_smc_binary_signal is now async – no more event loop errors
    • ADDED: Binary state machine – only 'bullish' or 'bearish' (never 'neutral')
    • ADDED: Database persistence for binary state across bot restarts
    • ADDED: Full state details retrieval (confidence, strength, swing levels)
    • ADDED: State management functions (clear, summary, get details)
    • MODIFIED: analyze_basic_smc returns stored state when no breakout detected
    • MODIFIED: Swing detection as fallback method
    • RETAINED: Rolling breakout logic for new signal detection
    • RETAINED: Confidence scoring and strength metrics
    • RETAINED: Full backward compatibility with existing code
"""

import pandas as pd
import numpy as np
import logging
import sqlite3
import time
import os
from typing import Dict, Optional, List, Tuple, Any
from datetime import datetime

logger = logging.getLogger(__name__)

# ============================================================================
# DATABASE CONFIGURATION
# ============================================================================

SMC_STATE_DB = os.getenv('DB_PATH', 'forex_trading.db')

# ============================================================================
# TIMEFRAME CONFIGURATION - UPDATED: H1, H4, D1 (Daily)
# ============================================================================

SUPPORTED_TIMEFRAMES = ['M15', 'M30', 'H1', 'H4', 'D1']  # M15/M30 added for the SmartMoney AI bias dashboard
DEFAULT_TIMEFRAME = 'H4'

DEFAULT_BARS = {
    'H1': 200,    # ~8 days of H1 data
    'H4': 150,    # ~25 days of H4 data
    'D1': 100,    # ~100 days of Daily data
}

DEFAULT_LOOKBACK = {
    'H1': 10,     # Rolling window for breakout detection (H1)
    'H4': 10,     # Rolling window for breakout detection (H4)
    'D1': 10,     # Rolling window for breakout detection (D1)
}

DEFAULT_SWING_LOOKBACK = {
    'H1': 3,      # Swing detection lookback (H1)
    'H4': 3,      # Swing detection lookback (H4)
    'D1': 3,      # Swing detection lookback (D1)
}

# ============================================================================
# DATABASE INITIALIZATION
# ============================================================================

def _init_db() -> None:
    """
    Initialize the database and create the basic_smc_state table if it doesn't exist.
    Called at module import to ensure the table exists.
    Now uses (symbol, timeframe) as composite primary key.
    """
    try:
        conn = sqlite3.connect(SMC_STATE_DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS basic_smc_state (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at REAL NOT NULL,
                confidence REAL DEFAULT 0.0,
                strength REAL DEFAULT 0.0,
                swing_high REAL,
                swing_low REAL,
                method_used TEXT DEFAULT 'breakout',
                lookback_used INTEGER DEFAULT 10,
                PRIMARY KEY (symbol, timeframe)
            )
        ''')
        
        # Create indexes
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_basic_smc_state_symbol 
            ON basic_smc_state(symbol)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_basic_smc_state_timeframe 
            ON basic_smc_state(timeframe)
        ''')
        
        cursor.execute('''
            CREATE INDEX IF NOT EXISTS idx_basic_smc_state_symbol_timeframe 
            ON basic_smc_state(symbol, timeframe)
        ''')
        
        conn.commit()
        conn.close()
        logger.debug("Basic SMC database table initialized (multi-timeframe)")
        
    except Exception as e:
        logger.error(f"Error initializing basic_smc database: {e}")


# ============================================================================
# DATABASE HELPERS
# ============================================================================

def _get_stored_state(symbol: str, timeframe: str = DEFAULT_TIMEFRAME) -> str:
    """
    Retrieve the stored binary state for a symbol and timeframe from the database.
    
    Args:
        symbol: Trading symbol (e.g., 'BTCUSD') - base symbol for persistence
        timeframe: Timeframe ('H1', 'H4', or 'D1')
        
    Returns:
        'bullish', 'bearish', or 'neutral' if no state exists
    """
    try:
        conn = sqlite3.connect(SMC_STATE_DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        # ALWAYS ensure table exists - this is the critical fix
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS basic_smc_state (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at REAL NOT NULL,
                confidence REAL DEFAULT 0.0,
                strength REAL DEFAULT 0.0,
                swing_high REAL,
                swing_low REAL,
                method_used TEXT DEFAULT 'breakout',
                lookback_used INTEGER DEFAULT 10,
                PRIMARY KEY (symbol, timeframe)
            )
        ''')
        conn.commit()
        
        cursor.execute(
            'SELECT state FROM basic_smc_state WHERE symbol = ? AND timeframe = ?',
            (symbol, timeframe)
        )
        row = cursor.fetchone()
        conn.close()
        
        if row:
            logger.debug(f"Basic SMC: Retrieved stored state for {symbol} ({timeframe}): {row[0]}")
            return row[0]
        else:
            logger.debug(f"Basic SMC: No stored state found for {symbol} ({timeframe})")
            return 'neutral'
            
    except Exception as e:
        logger.error(f"Error reading basic_smc state for {symbol} ({timeframe}): {e}")
        return 'neutral'


def _set_stored_state(
    symbol: str, 
    state: str, 
    confidence: float = 0.0,
    strength: float = 0.0,
    swing_high: Optional[float] = None,
    swing_low: Optional[float] = None,
    method_used: str = 'breakout',
    lookback_used: int = 10,
    timeframe: str = DEFAULT_TIMEFRAME
) -> None:
    """
    Store the binary state for a symbol and timeframe in the database.
    
    Args:
        symbol: Trading symbol (base symbol for persistence)
        timeframe: Timeframe ('H1', 'H4', or 'D1')
        state: 'bullish' or 'bearish'
        confidence: Confidence score (0-100)
        strength: Strength metric (0-1)
        swing_high: Most recent swing high price
        swing_low: Most recent swing low price
        method_used: Detection method used
        lookback_used: Lookback period used
    """
    try:
        conn = sqlite3.connect(SMC_STATE_DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        # Ensure table exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS basic_smc_state (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at REAL NOT NULL,
                confidence REAL DEFAULT 0.0,
                strength REAL DEFAULT 0.0,
                swing_high REAL,
                swing_low REAL,
                method_used TEXT DEFAULT 'breakout',
                lookback_used INTEGER DEFAULT 10,
                PRIMARY KEY (symbol, timeframe)
            )
        ''')
        
        cursor.execute('''
            INSERT OR REPLACE INTO basic_smc_state 
            (symbol, timeframe, state, updated_at, confidence, strength, 
             swing_high, swing_low, method_used, lookback_used)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            symbol, 
            timeframe,
            state, 
            time.time(), 
            confidence, 
            strength, 
            swing_high, 
            swing_low, 
            method_used, 
            lookback_used
        ))
        
        conn.commit()
        conn.close()
        logger.debug(f"Basic SMC: Stored state for {symbol} ({timeframe}): {state} (conf: {confidence:.1f}%)")
        
    except Exception as e:
        logger.error(f"Error storing basic_smc state for {symbol} ({timeframe}): {e}")


def _get_stored_state_details(symbol: str, timeframe: str = DEFAULT_TIMEFRAME) -> Dict[str, Any]:
    """
    Retrieve the full stored state details for a symbol and timeframe.
    
    Args:
        symbol: Trading symbol (base symbol for persistence)
        timeframe: Timeframe ('H1', 'H4', or 'D1')
        
    Returns:
        Dict with all stored state fields, or empty dict if no state exists
    """
    try:
        conn = sqlite3.connect(SMC_STATE_DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        # ALWAYS ensure table exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS basic_smc_state (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at REAL NOT NULL,
                confidence REAL DEFAULT 0.0,
                strength REAL DEFAULT 0.0,
                swing_high REAL,
                swing_low REAL,
                method_used TEXT DEFAULT 'breakout',
                lookback_used INTEGER DEFAULT 10,
                PRIMARY KEY (symbol, timeframe)
            )
        ''')
        conn.commit()
        
        cursor.execute('''
            SELECT symbol, timeframe, state, updated_at, confidence, strength, 
                   swing_high, swing_low, method_used, lookback_used
            FROM basic_smc_state 
            WHERE symbol = ? AND timeframe = ?
        ''', (symbol, timeframe))
        
        row = cursor.fetchone()
        conn.close()
        
        if row:
            return {
                'symbol': row[0],
                'timeframe': row[1],
                'state': row[2],
                'updated_at': row[3],
                'updated_at_str': datetime.fromtimestamp(row[3]).strftime('%Y-%m-%d %H:%M:%S') if row[3] else None,
                'confidence': row[4],
                'strength': row[5],
                'swing_high': row[6],
                'swing_low': row[7],
                'method_used': row[8],
                'lookback_used': row[9]
            }
        return {}
        
    except Exception as e:
        logger.error(f"Error reading full basic_smc state for {symbol} ({timeframe}): {e}")
        return {}


def _get_all_stored_states(symbol: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    """
    Get all stored states, optionally filtered by symbol.
    
    Args:
        symbol: Optional symbol filter
        
    Returns:
        Dict mapping "symbol_timeframe" to state details
    """
    try:
        conn = sqlite3.connect(SMC_STATE_DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        # Ensure table exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS basic_smc_state (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at REAL NOT NULL,
                confidence REAL DEFAULT 0.0,
                strength REAL DEFAULT 0.0,
                swing_high REAL,
                swing_low REAL,
                method_used TEXT DEFAULT 'breakout',
                lookback_used INTEGER DEFAULT 10,
                PRIMARY KEY (symbol, timeframe)
            )
        ''')
        
        if symbol:
            cursor.execute('''
                SELECT symbol, timeframe, state, updated_at, confidence, strength, 
                       swing_high, swing_low, method_used, lookback_used
                FROM basic_smc_state 
                WHERE symbol = ?
                ORDER BY timeframe
            ''', (symbol,))
        else:
            cursor.execute('''
                SELECT symbol, timeframe, state, updated_at, confidence, strength, 
                       swing_high, swing_low, method_used, lookback_used
                FROM basic_smc_state 
                ORDER BY symbol, timeframe
            ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        result = {}
        for row in rows:
            key = f"{row[0]}_{row[1]}"
            result[key] = {
                'symbol': row[0],
                'timeframe': row[1],
                'state': row[2],
                'updated_at': row[3],
                'updated_at_str': datetime.fromtimestamp(row[3]).strftime('%Y-%m-%d %H:%M:%S') if row[3] else None,
                'confidence': row[4],
                'strength': row[5],
                'swing_high': row[6],
                'swing_low': row[7],
                'method_used': row[8],
                'lookback_used': row[9]
            }
        return result
        
    except Exception as e:
        logger.error(f"Error getting all stored states: {e}")
        return {}


# ============================================================================
# SIGNAL COOLDOWN CACHE
# ============================================================================

# Cache to prevent repeated identical signal logging
_signal_log_cache: Dict[str, Tuple[str, float]] = {}  # symbol_timeframe -> (signal, timestamp)
_SIGNAL_LOG_COOLDOWN = 60  # seconds between logging the same signal

def _should_log_signal(symbol: str, signal: str, timeframe: str = DEFAULT_TIMEFRAME) -> bool:
    """
    Check if we should log this signal (cooldown prevents spam).
    
    Args:
        symbol: Trading symbol
        signal: Signal string ('bullish', 'bearish', 'neutral')
        timeframe: Timeframe ('H1', 'H4', or 'D1')
        
    Returns:
        True if the signal should be logged, False if it's a duplicate within cooldown
    """
    global _signal_log_cache
    now = time.time()
    key = f"{symbol}_{timeframe}"
    
    if key in _signal_log_cache:
        cached_signal, last_time = _signal_log_cache[key]
        if cached_signal == signal and (now - last_time) < _SIGNAL_LOG_COOLDOWN:
            return False
    
    _signal_log_cache[key] = (signal, now)
    
    # Clean old cache entries
    to_remove = []
    for k, (_, ts) in _signal_log_cache.items():
        if now - ts > 300:  # Remove after 5 minutes
            to_remove.append(k)
    for k in to_remove:
        del _signal_log_cache[k]
    
    return True


# ============================================================================
# SWING POINT DETECTION
# ============================================================================

def detect_swing_points(
    high: np.ndarray, 
    low: np.ndarray, 
    lookback: int = 3
) -> Tuple[List[float], List[float]]:
    """
    Identify swing highs and lows using a configurable lookback window.
    
    Args:
        high: Array of high prices
        low: Array of low prices
        lookback: Number of bars on each side for swing detection (default: 3)

    Returns:
        swing_highs (List[float]): prices of identified swing highs
        swing_lows  (List[float]): prices of identified swing lows
    """
    swing_highs = []
    swing_lows = []
    n = len(high)

    if n < 2 * lookback + 1:
        return swing_highs, swing_lows

    for i in range(lookback, n - lookback):
        # Swing High: price > all neighbours within lookback window
        is_swing_high = True
        for j in range(1, lookback + 1):
            if high[i] <= high[i - j] or high[i] <= high[i + j]:
                is_swing_high = False
                break
        if is_swing_high:
            swing_highs.append(high[i])

        # Swing Low: price < all neighbours within lookback window
        is_swing_low = True
        for j in range(1, lookback + 1):
            if low[i] >= low[i - j] or low[i] >= low[i + j]:
                is_swing_low = False
                break
        if is_swing_low:
            swing_lows.append(low[i])

    return swing_highs, swing_lows


# ============================================================================
# BREAKOUT DETECTION
# ============================================================================

def detect_breakout_levels(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    lookback: int = 10
) -> Dict[str, Any]:
    """
    Detect breakouts using rolling highest high and lowest low.

    This is the PRIMARY detection method for basic_smc.py.
    It works reliably in both trending and ranging markets.

    Args:
        high: Array of high prices
        low: Array of low prices
        close: Array of close prices
        lookback: Number of bars for rolling window (default: 10)

    Returns:
        Dict with:
            - signal: 'bullish', 'bearish', or 'neutral'
            - swing_high: Highest high in lookback window (or None)
            - swing_low: Lowest low in lookback window (or None)
            - confidence: Breakout confidence score (0-100)
            - strength: Breakout strength (0-1)
    """
    n = len(close)

    if n < lookback + 2:
        return {
            'signal': 'neutral',
            'swing_high': None,
            'swing_low': None,
            'confidence': 0.0,
            'strength': 0.0
        }

    # Use lookback bars excluding the current bar (to avoid lookahead bias)
    start_idx = n - lookback - 1
    end_idx = n - 1  # exclude current bar

    recent_high = np.max(high[start_idx:end_idx]) if end_idx > start_idx else high[n-2]
    recent_low = np.min(low[start_idx:end_idx]) if end_idx > start_idx else low[n-2]

    current_close = close[-1]
    current_high = high[-1]
    current_low = low[-1]

    # Calculate breakout strength
    price_range = recent_high - recent_low
    if price_range <= 0:
        return {
            'signal': 'neutral',
            'swing_high': float(recent_high),
            'swing_low': float(recent_low),
            'confidence': 0.0,
            'strength': 0.0
        }

    # Determine signal
    signal = 'neutral'
    confidence = 0.0
    strength = 0.0

    # Bullish breakout: close above recent high
    if current_close > recent_high:
        signal = 'bullish'
        # Confidence: how far above the breakout level
        breakout_distance = (current_close - recent_high) / price_range
        confidence = min(100.0, 50.0 + breakout_distance * 50.0)
        strength = min(1.0, breakout_distance / 2.0)

        logger.debug(
            f"Basic SMC: BULLISH breakout - Price {current_close:.2f} > "
            f"Resistance {recent_high:.2f} (conf: {confidence:.1f}%)"
        )

    # Bearish breakout: close below recent low
    elif current_close < recent_low:
        signal = 'bearish'
        breakout_distance = (recent_low - current_close) / price_range
        confidence = min(100.0, 50.0 + breakout_distance * 50.0)
        strength = min(1.0, breakout_distance / 2.0)

        logger.debug(
            f"Basic SMC: BEARISH breakout - Price {current_close:.2f} < "
            f"Support {recent_low:.2f} (conf: {confidence:.1f}%)"
        )

    else:
        # Neutral - no breakout
        # But still calculate how close we are to a breakout
        distance_to_high = (recent_high - current_close) / price_range if price_range > 0 else 1.0
        distance_to_low = (current_close - recent_low) / price_range if price_range > 0 else 1.0
        proximity = min(distance_to_high, distance_to_low)

        if proximity < 0.1:  # Within 10% of a breakout level
            logger.debug(
                f"Basic SMC: NEAR BREAKOUT - Price {current_close:.2f} within "
                f"{proximity*100:.1f}% of level (high={recent_high:.2f}, low={recent_low:.2f})"
            )

    return {
        'signal': signal,
        'swing_high': float(recent_high),
        'swing_low': float(recent_low),
        'confidence': confidence,
        'strength': strength
    }


# ============================================================================
# COLD-START BOOTSTRAP (fixes permanent method:none on fresh installs)
# ============================================================================

def _compute_bootstrap_bias(
    high: np.ndarray,
    low: np.ndarray,
    close: np.ndarray,
    swing_lookback: int = 3
) -> Dict[str, Any]:
    """
    Determine an initial directional bias from historical structure when no
    stored state exists yet (cold start) AND no live breakout/swing event has
    fired this cycle.

    WHY THIS EXISTS:
    Without it, analyze_basic_smc() can sit at signal='neutral',
    method_used='none' indefinitely on a fresh install / new symbol, because
    detect_breakout_levels() and detect_swing_points() only fire on a NEW
    extreme relative to a short lookback window — they never look at the
    broader trend already present in the fetched history to make an initial
    call. On any timeframe that can mean days of "method: none" with zero signal.

    METHOD (deliberately conservative — requires agreement between two
    independent reads of the same data, else stays neutral):
      1. Trend slope: linear regression over all fetched closes.
      2. Structure position: where the current close sits within the most
         recent swing-high/swing-low range (near the top vs near the bottom).
    Only seeds a state if both agree on direction; otherwise remains neutral
    and lets the normal breakout/swing detection take over on a later cycle.

    Returns a dict shaped like detect_breakout_levels()'s output.
    """
    n = len(close)
    result: Dict[str, Any] = {
        'signal': 'neutral',
        'swing_high': None,
        'swing_low': None,
        'confidence': 0.0,
        'strength': 0.0
    }

    if n < 10:
        return result

    # --- Component 1: trend slope over the fetched window ---
    x = np.arange(n)
    slope, _intercept = np.polyfit(x, close, 1)
    price_scale = np.mean(close) if np.mean(close) != 0 else 1.0
    normalized_slope = (slope * n) / price_scale  # total drift over window, as a fraction of price

    # --- Component 2: position within most recent swing structure ---
    swing_highs, swing_lows = detect_swing_points(high, low, swing_lookback)
    recent_swing_high = swing_highs[-1] if swing_highs else float(np.max(high))
    recent_swing_low = swing_lows[-1] if swing_lows else float(np.min(low))
    current_price = close[-1]

    structure_range = recent_swing_high - recent_swing_low
    position_in_range = (
        (current_price - recent_swing_low) / structure_range
        if structure_range > 0 else 0.5
    )  # 0 = at recent low, 1 = at recent high

    # More conservative thresholds for higher timeframes
    trend_threshold = 0.015  # Slightly higher for Daily
    position_threshold = 0.55  # Slightly higher for Daily

    trend_bias = 1 if normalized_slope > trend_threshold else (-1 if normalized_slope < -trend_threshold else 0)
    position_bias = 1 if position_in_range > position_threshold else (-1 if position_in_range < (1 - position_threshold) else 0)

    if trend_bias == 0 or position_bias == 0 or trend_bias != position_bias:
        # Trend and structure disagree (or are too flat) — don't seed a guess
        return result

    signal = 'bullish' if trend_bias > 0 else 'bearish'
    slope_component = min(1.0, abs(normalized_slope) * 5.0)
    position_component = abs(position_in_range - 0.5) * 2.0  # 0..1
    confidence = 50.0 + (slope_component * 0.5 + position_component * 0.5) * 40.0
    confidence = min(90.0, max(50.0, confidence))  # cap below live breakout confidence
    strength = min(1.0, (slope_component + position_component) / 2.0)

    result.update({
        'signal': signal,
        'swing_high': float(recent_swing_high),
        'swing_low': float(recent_swing_low),
        'confidence': confidence,
        'strength': strength
    })
    return result


# ============================================================================
# MAIN ANALYSIS FUNCTION (SYNCHRONOUS) - Multi-timeframe
# ============================================================================

def analyze_basic_smc(
    df: pd.DataFrame,
    lookback: int = 10,
    swing_lookback: int = 3,
    use_breakout_method: bool = True,
    symbol: str = 'BTCUSD',
    timeframe: str = DEFAULT_TIMEFRAME
) -> Dict[str, Optional[float]]:
    """
    Perform Basic SMC Break of Structure analysis with binary state persistence.

    Returns binary signal ('bullish' or 'bearish') when state exists.
    Only returns 'neutral' on first run or when no state exists.

    Args:
        df: DataFrame with columns 'high', 'low', 'close'.
        lookback: Rolling window for breakout detection (default: 10)
        swing_lookback: Lookback for swing detection fallback (default: 3)
        use_breakout_method: If True, use breakout detection first (default: True)
        symbol: Trading symbol for state persistence (default: 'BTCUSD')
        timeframe: Timeframe ('H1', 'H4', or 'D1') (default: 'H4')

    Returns:
        dict: {
            'signal': str,           # 'bullish', 'bearish', or 'neutral'
            'swing_high': float,     # most recent swing high price or None
            'swing_low': float,      # most recent swing low price or None
            'confidence': float,     # breakout confidence (0-100)
            'strength': float,       # breakout strength (0-1)
            'method_used': str,      # 'breakout', 'swing', 'bootstrap', or 'stored'
            'lookback_used': int     # lookback used for detection
        }
    """
    # Validate timeframe - UPDATED: H1, H4, D1
    if timeframe not in SUPPORTED_TIMEFRAMES:
        logger.warning(f"Unsupported timeframe: {timeframe}. Using {DEFAULT_TIMEFRAME}")
        timeframe = DEFAULT_TIMEFRAME
    
    result = {
        'signal': 'neutral',
        'swing_high': None,
        'swing_low': None,
        'confidence': 0.0,
        'strength': 0.0,
        'method_used': 'none',
        'lookback_used': lookback
    }

    # Validate input
    min_bars = max(lookback, swing_lookback * 2 + 5)
    if df is None or len(df) < min_bars:
        logger.warning(
            f"Basic SMC ({timeframe}): Insufficient data. "
            f"Need {min_bars} bars, got {len(df) if df is not None else 0}"
        )
        # Try to return stored state even with insufficient data
        stored_state = _get_stored_state(symbol, timeframe)
        if stored_state != 'neutral':
            stored_details = _get_stored_state_details(symbol, timeframe)
            result.update({
                'signal': stored_state,
                'swing_high': stored_details.get('swing_high'),
                'swing_low': stored_details.get('swing_low'),
                'confidence': stored_details.get('confidence', 0.0),
                'strength': stored_details.get('strength', 0.0),
                'method_used': 'stored',
                'lookback_used': stored_details.get('lookback_used', lookback)
            })
            logger.debug(f"Basic SMC ({timeframe}): Insufficient data, using stored state '{stored_state}' for {symbol}")
        return result

    try:
        high = df['high'].values
        low = df['low'].values
        close = df['close'].values

        # === METHOD 1: Breakout detection (primary - works in trends) ===
        if use_breakout_method and len(close) >= lookback + 2:
            breakout = detect_breakout_levels(high, low, close, lookback)

            if breakout['signal'] != 'neutral':
                # New breakout detected → update state and return
                _set_stored_state(
                    symbol=symbol,
                    state=breakout['signal'],
                    confidence=breakout['confidence'],
                    strength=breakout['strength'],
                    swing_high=breakout['swing_high'],
                    swing_low=breakout['swing_low'],
                    method_used='breakout',
                    lookback_used=lookback,
                    timeframe=timeframe
                )
                
                result.update({
                    'signal': breakout['signal'],
                    'swing_high': breakout['swing_high'],
                    'swing_low': breakout['swing_low'],
                    'confidence': breakout['confidence'],
                    'strength': breakout['strength'],
                    'method_used': 'breakout',
                    'lookback_used': lookback
                })
                
                # Only log if cooldown allows (prevents spam)
                if _should_log_signal(symbol, breakout['signal'], timeframe):
                    logger.info(f"✅ Basic SMC ({timeframe}): New {breakout['signal'].upper()} breakout detected for {symbol}")
                else:
                    logger.debug(f"Basic SMC ({timeframe}): {breakout['signal'].upper()} breakout (cooldown) for {symbol}")
                
                return result

        # === METHOD 2: Swing detection (fallback - works in ranging markets) ===
        swing_highs, swing_lows = detect_swing_points(high, low, swing_lookback)

        recent_swing_high = swing_highs[-1] if swing_highs else None
        recent_swing_low = swing_lows[-1] if swing_lows else None
        current_price = close[-1]

        # Check for bullish break above recent swing high
        if recent_swing_high is not None and current_price > recent_swing_high:
            _set_stored_state(
                symbol=symbol,
                state='bullish',
                confidence=50.0,
                strength=0.5,
                swing_high=recent_swing_high,
                swing_low=recent_swing_low,
                method_used='swing',
                lookback_used=swing_lookback,
                timeframe=timeframe
            )
            
            result.update({
                'signal': 'bullish',
                'swing_high': recent_swing_high,
                'swing_low': recent_swing_low,
                'confidence': 50.0,
                'strength': 0.5,
                'method_used': 'swing',
                'lookback_used': swing_lookback
            })
            
            if _should_log_signal(symbol, 'bullish', timeframe):
                logger.info(f"✅ Basic SMC ({timeframe})(swing): BULLISH detected for {symbol}")
            else:
                logger.debug(f"Basic SMC ({timeframe})(swing): BULLISH (cooldown) for {symbol}")
            
            return result

        # Check for bearish break below recent swing low
        elif recent_swing_low is not None and current_price < recent_swing_low:
            _set_stored_state(
                symbol=symbol,
                state='bearish',
                confidence=50.0,
                strength=0.5,
                swing_high=recent_swing_high,
                swing_low=recent_swing_low,
                method_used='swing',
                lookback_used=swing_lookback,
                timeframe=timeframe
            )
            
            result.update({
                'signal': 'bearish',
                'swing_high': recent_swing_high,
                'swing_low': recent_swing_low,
                'confidence': 50.0,
                'strength': 0.5,
                'method_used': 'swing',
                'lookback_used': swing_lookback
            })
            
            if _should_log_signal(symbol, 'bearish', timeframe):
                logger.info(f"✅ Basic SMC ({timeframe})(swing): BEARISH detected for {symbol}")
            else:
                logger.debug(f"Basic SMC ({timeframe})(swing): BEARISH (cooldown) for {symbol}")
            
            return result

        # === No new breakout detected → use stored state ===
        stored_state = _get_stored_state(symbol, timeframe)
        if stored_state != 'neutral':
            # Get full stored details for better result
            stored_details = _get_stored_state_details(symbol, timeframe)
            result.update({
                'signal': stored_state,
                'swing_high': stored_details.get('swing_high'),
                'swing_low': stored_details.get('swing_low'),
                'confidence': stored_details.get('confidence', 0.0),
                'strength': stored_details.get('strength', 0.0),
                'method_used': 'stored',
                'lookback_used': stored_details.get('lookback_used', lookback)
            })
            logger.debug(
                f"Basic SMC ({timeframe}): No new breakout for {symbol}, using stored state '{stored_state}' "
                f"(conf: {stored_details.get('confidence', 0):.1f}%)"
            )
        else:
            # No state exists (first run / cold start) → try to bootstrap an
            # initial bias from the historical structure already in `df`
            # instead of sitting on 'neutral'/'none' indefinitely.
            bootstrap = _compute_bootstrap_bias(high, low, close, swing_lookback)

            if bootstrap['signal'] != 'neutral':
                _set_stored_state(
                    symbol=symbol,
                    state=bootstrap['signal'],
                    confidence=bootstrap['confidence'],
                    strength=bootstrap['strength'],
                    swing_high=bootstrap['swing_high'],
                    swing_low=bootstrap['swing_low'],
                    method_used='bootstrap',
                    lookback_used=lookback,
                    timeframe=timeframe
                )
                result.update({
                    'signal': bootstrap['signal'],
                    'swing_high': bootstrap['swing_high'],
                    'swing_low': bootstrap['swing_low'],
                    'confidence': bootstrap['confidence'],
                    'strength': bootstrap['strength'],
                    'method_used': 'bootstrap',
                    'lookback_used': lookback
                })
                logger.info(
                    f"🌱 Basic SMC ({timeframe}): Bootstrapped initial {bootstrap['signal'].upper()} state for "
                    f"{symbol} from trend/structure (conf: {bootstrap['confidence']:.1f}%) — no more waiting on a live breakout."
                )
            else:
                logger.info(
                    f"Basic SMC ({timeframe}): No stored state for {symbol}, no breakout, and trend/structure "
                    f"disagree — returning neutral. (First run)"
                )

        return result

    except Exception as e:
        logger.error(f"Basic SMC ({timeframe}) analysis error for {symbol}: {e}")
        # Try to return stored state on error
        stored_state = _get_stored_state(symbol, timeframe)
        if stored_state != 'neutral':
            stored_details = _get_stored_state_details(symbol, timeframe)
            result.update({
                'signal': stored_state,
                'swing_high': stored_details.get('swing_high'),
                'swing_low': stored_details.get('swing_low'),
                'confidence': stored_details.get('confidence', 0.0),
                'strength': stored_details.get('strength', 0.0),
                'method_used': 'stored',
                'lookback_used': stored_details.get('lookback_used', lookback)
            })
            logger.warning(f"Basic SMC ({timeframe}): Error in analysis, using stored state '{stored_state}' for {symbol}")
        else:
            logger.error(f"Basic SMC ({timeframe}): Analysis error and no stored state for {symbol}")
        return result


# ============================================================================
# COMPATIBILITY FUNCTIONS
# ============================================================================

def analyze_smc(
    df: pd.DataFrame,
    lookback: int = 10,
    swing_lookback: int = 3,
    symbol: str = 'BTCUSD',
    timeframe: str = DEFAULT_TIMEFRAME
) -> Dict[str, Optional[float]]:
    """
    Alias for analyze_basic_smc with binary state persistence and multi-timeframe support.
    
    Args:
        df: DataFrame with columns 'high', 'low', 'close'
        lookback: Rolling window for breakout detection
        swing_lookback: Lookback for swing detection fallback
        symbol: Trading symbol for state persistence
        timeframe: Timeframe ('H1', 'H4', or 'D1')

    Returns:
        Same as analyze_basic_smc
    """
    return analyze_basic_smc(df, lookback, swing_lookback, True, symbol, timeframe)


# ============================================================================
# ASYNC BINARY SIGNAL RETRIEVAL - Multi-timeframe
# ============================================================================

async def get_basic_smc_binary_signal(
    symbol: str, 
    mt5_conn,
    timeframe: str = DEFAULT_TIMEFRAME,
    bars: int = None
) -> str:
    """
    Async function to get the current binary SMC signal.
    Returns 'bullish', 'bearish', or 'neutral' (if no state exists).
    
    Args:
        symbol: Trading symbol (base symbol for persistence, also used for data fetch)
        mt5_conn: MT5 connection for fetching data
        timeframe: Timeframe ('H1', 'H4', or 'D1') (default: 'H4')
        bars: Number of bars to fetch (auto-calculated if None)
        
    Returns:
        'bullish', 'bearish', or 'neutral' (if no state exists)
    """
    # Validate timeframe - UPDATED: H1, H4, D1
    if timeframe not in SUPPORTED_TIMEFRAMES:
        logger.warning(f"Unsupported timeframe: {timeframe}. Using {DEFAULT_TIMEFRAME}")
        timeframe = DEFAULT_TIMEFRAME
    
    # Set default bars if not provided
    if bars is None:
        bars = DEFAULT_BARS.get(timeframe, 100)
    
    try:
        # Ensure database exists
        _init_db()
        
        # Fetch fresh data asynchronously
        if hasattr(mt5_conn, 'is_connected') and not mt5_conn.is_connected():
            logger.debug(f"MT5 not connected for {symbol}, using stored SMC state ({timeframe})")
            return _get_stored_state(symbol, timeframe)
        
        df = await mt5_conn.get_ohlcv(symbol, timeframe, bars)
        if df is not None and not df.empty:
            result = analyze_basic_smc(df, symbol=symbol, timeframe=timeframe)
            return result.get('signal', 'neutral')
        else:
            logger.debug(f"No {timeframe} data for {symbol}, using stored SMC state")
            return _get_stored_state(symbol, timeframe)
            
    except Exception as e:
        logger.warning(f"Error fetching data for basic_smc {symbol} ({timeframe}): {e}")
        # Fallback to stored state
        return _get_stored_state(symbol, timeframe)


async def get_basic_smc_signal_details(
    symbol: str,
    mt5_conn,
    data_symbol: Optional[str] = None,
    lookback: int = 10,
    swing_lookback: int = 3,
    timeframe: str = DEFAULT_TIMEFRAME,
    bars: int = None
) -> Dict[str, Any]:
    """
    Async function to get full SMC signal details including confidence and method.
    Returns a dict with keys: signal, confidence, strength, swing_high, swing_low,
    method_used, lookback_used, timeframe.
    
    This is the PRIMARY interface for the HVTS-SmartMoney Alert signal.
    
    Args:
        symbol: Trading symbol (BASE symbol) used for database key/persistence
        mt5_conn: MT5 connection for fetching data
        data_symbol: MT5 symbol for data fetching (e.g., 'BTCUSD.sc'). 
                     If None, uses the symbol argument.
        lookback: Rolling window for breakout detection (default: 10)
        swing_lookback: Lookback for swing detection (default: 3)
        timeframe: Timeframe ('H1', 'H4', or 'D1') (default: 'H4')
        bars: Number of bars to fetch (auto-calculated if None)
        
    Returns:
        Dict with signal details:
            - signal: 'bullish', 'bearish', or 'neutral'
            - confidence: float (0-100)
            - strength: float (0-1)
            - swing_high: float or None
            - swing_low: float or None
            - method_used: 'breakout', 'swing', 'bootstrap', 'stored', or 'unknown'
            - lookback_used: int
            - timeframe: str
    """
    # Validate timeframe - UPDATED: H1, H4, D1
    if timeframe not in SUPPORTED_TIMEFRAMES:
        logger.warning(f"Unsupported timeframe: {timeframe}. Using {DEFAULT_TIMEFRAME}")
        timeframe = DEFAULT_TIMEFRAME
    
    # Set default bars if not provided
    if bars is None:
        bars = DEFAULT_BARS.get(timeframe, 100)
    
    # Ensure database exists
    _init_db()
    
    # Use the data_symbol for MT5 fetch, or fall back to symbol
    fetch_symbol = data_symbol if data_symbol is not None else symbol
    
    logger.debug(f"🔍 Getting Basic SMC Alert for {symbol} ({timeframe}) (data: {fetch_symbol})")
    
    try:
        # Check connection; fallback to stored state if unavailable.
        if not mt5_conn:
            logger.debug(f"No mt5_conn provided for {symbol} ({timeframe}), using stored SMC state")
            stored = _get_stored_state_details(symbol, timeframe)
            if stored:
                return {
                    'signal': stored.get('state', 'neutral'),
                    'confidence': stored.get('confidence', 0.0),
                    'strength': stored.get('strength', 0.0),
                    'swing_high': stored.get('swing_high'),
                    'swing_low': stored.get('swing_low'),
                    'method_used': stored.get('method_used', 'stored'),
                    'lookback_used': stored.get('lookback_used', lookback),
                    'timeframe': timeframe
                }
            return {
                'signal': 'neutral',
                'confidence': 0.0,
                'strength': 0.0,
                'swing_high': None,
                'swing_low': None,
                'method_used': 'unknown',
                'lookback_used': lookback,
                'timeframe': timeframe
            }
        if hasattr(mt5_conn, 'is_connected') and not mt5_conn.is_connected():
            logger.debug(f"MT5 not connected for {symbol} ({timeframe}), using stored SMC state")
            stored = _get_stored_state_details(symbol, timeframe)
            if stored:
                return {
                    'signal': stored.get('state', 'neutral'),
                    'confidence': stored.get('confidence', 0.0),
                    'strength': stored.get('strength', 0.0),
                    'swing_high': stored.get('swing_high'),
                    'swing_low': stored.get('swing_low'),
                    'method_used': stored.get('method_used', 'stored'),
                    'lookback_used': stored.get('lookback_used', lookback),
                    'timeframe': timeframe
                }
            return {
                'signal': 'neutral',
                'confidence': 0.0,
                'strength': 0.0,
                'swing_high': None,
                'swing_low': None,
                'method_used': 'unknown',
                'lookback_used': lookback,
                'timeframe': timeframe
            }
        
        # Fetch data using data_symbol (MT5 symbol)
        logger.debug(f"📊 Fetching {timeframe} data for {fetch_symbol} ({bars} bars)")
        df = await mt5_conn.get_ohlcv(fetch_symbol, timeframe, bars)
        
        if df is not None and not df.empty:
            logger.info(f"✅ Got {len(df)} {timeframe} bars for {fetch_symbol}")
            
            # Pass the BASE symbol for persistence
            result = analyze_basic_smc(
                df,
                lookback=lookback,
                swing_lookback=swing_lookback,
                symbol=symbol,  # Use base symbol for DB key
                timeframe=timeframe
            )
            
            logger.debug(f"📊 Analysis result: signal={result.get('signal')}, method={result.get('method_used')}, confidence={result.get('confidence', 0):.1f}%")
            
            return {
                'signal': result.get('signal', 'neutral'),
                'confidence': result.get('confidence', 0.0),
                'strength': result.get('strength', 0.0),
                'swing_high': result.get('swing_high'),
                'swing_low': result.get('swing_low'),
                'method_used': result.get('method_used', 'unknown'),
                'lookback_used': result.get('lookback_used', lookback),
                'timeframe': timeframe
            }
        else:
            # No data, fallback to stored
            logger.warning(f"⚠️ No {timeframe} data for {fetch_symbol}, using stored SMC state for {symbol}")
            stored = _get_stored_state_details(symbol, timeframe)
            if stored:
                return {
                    'signal': stored.get('state', 'neutral'),
                    'confidence': stored.get('confidence', 0.0),
                    'strength': stored.get('strength', 0.0),
                    'swing_high': stored.get('swing_high'),
                    'swing_low': stored.get('swing_low'),
                    'method_used': stored.get('method_used', 'stored'),
                    'lookback_used': stored.get('lookback_used', lookback),
                    'timeframe': timeframe
                }
            return {
                'signal': 'neutral',
                'confidence': 0.0,
                'strength': 0.0,
                'swing_high': None,
                'swing_low': None,
                'method_used': 'unknown',
                'lookback_used': lookback,
                'timeframe': timeframe
            }
    except Exception as e:
        logger.error(f"Error in get_basic_smc_signal_details for {symbol} ({timeframe}) (data: {fetch_symbol}): {e}")
        # Fallback to stored state on error
        stored = _get_stored_state_details(symbol, timeframe)
        if stored:
            return {
                'signal': stored.get('state', 'neutral'),
                'confidence': stored.get('confidence', 0.0),
                'strength': stored.get('strength', 0.0),
                'swing_high': stored.get('swing_high'),
                'swing_low': stored.get('swing_low'),
                'method_used': stored.get('method_used', 'stored'),
                'lookback_used': stored.get('lookback_used', lookback),
                'timeframe': timeframe
            }
        return {
            'signal': 'neutral',
            'confidence': 0.0,
            'strength': 0.0,
            'swing_high': None,
            'swing_low': None,
            'method_used': 'error',
            'lookback_used': lookback,
            'timeframe': timeframe
        }


# ============================================================================
# SYNCHRONOUS STATE MANAGEMENT FUNCTIONS
# ============================================================================

def get_stored_smc_state(symbol: str, timeframe: str = DEFAULT_TIMEFRAME) -> Dict[str, Any]:
    """
    Get the full stored SMC state without fetching new data.
    
    Args:
        symbol: Trading symbol (base symbol for persistence)
        timeframe: Timeframe ('H1', 'H4', or 'D1')
        
    Returns:
        Dict with stored state details, or empty dict if no state exists
    """
    return _get_stored_state_details(symbol, timeframe)


def get_all_stored_smc_states(symbol: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    """
    Get all stored SMC states, optionally filtered by symbol.
    
    Args:
        symbol: Optional symbol filter
        
    Returns:
        Dict mapping "symbol_timeframe" to state details
    """
    return _get_all_stored_states(symbol)


def clear_smc_state(
    symbol: Optional[str] = None, 
    timeframe: Optional[str] = None
) -> int:
    """
    Clear stored SMC state for a symbol and/or timeframe.
    
    Args:
        symbol: Trading symbol (if None, clears all symbols)
        timeframe: Timeframe (if None, clears all timeframes for symbol)
        
    Returns:
        Number of states cleared
    """
    try:
        conn = sqlite3.connect(SMC_STATE_DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        # Ensure table exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS basic_smc_state (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at REAL NOT NULL,
                confidence REAL DEFAULT 0.0,
                strength REAL DEFAULT 0.0,
                swing_high REAL,
                swing_low REAL,
                method_used TEXT DEFAULT 'breakout',
                lookback_used INTEGER DEFAULT 10,
                PRIMARY KEY (symbol, timeframe)
            )
        ''')
        
        if symbol and timeframe:
            cursor.execute(
                'DELETE FROM basic_smc_state WHERE symbol = ? AND timeframe = ?',
                (symbol, timeframe)
            )
        elif symbol:
            cursor.execute(
                'DELETE FROM basic_smc_state WHERE symbol = ?',
                (symbol,)
            )
        elif timeframe:
            cursor.execute(
                'DELETE FROM basic_smc_state WHERE timeframe = ?',
                (timeframe,)
            )
        else:
            cursor.execute('DELETE FROM basic_smc_state')
        
        count = cursor.rowcount
        conn.commit()
        conn.close()
        
        timeframe_str = f" ({timeframe})" if timeframe else ""
        symbol_str = f" for {symbol}" if symbol else " for all symbols"
        logger.info(f"Basic SMC: Cleared {count} state(s){symbol_str}{timeframe_str}")
        return count
        
    except Exception as e:
        logger.error(f"Error clearing basic_smc state: {e}")
        return 0


def get_smc_state_summary(timeframe: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    """
    Get a summary of all stored SMC states, optionally filtered by timeframe.
    
    Args:
        timeframe: Optional timeframe filter ('H1', 'H4', or 'D1')
        
    Returns:
        Dict mapping symbol to state details
    """
    try:
        conn = sqlite3.connect(SMC_STATE_DB, timeout=30)
        conn.execute("PRAGMA journal_mode=WAL")
        cursor = conn.cursor()
        
        # Ensure table exists
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS basic_smc_state (
                symbol TEXT NOT NULL,
                timeframe TEXT NOT NULL,
                state TEXT NOT NULL,
                updated_at REAL NOT NULL,
                confidence REAL DEFAULT 0.0,
                strength REAL DEFAULT 0.0,
                swing_high REAL,
                swing_low REAL,
                method_used TEXT DEFAULT 'breakout',
                lookback_used INTEGER DEFAULT 10,
                PRIMARY KEY (symbol, timeframe)
            )
        ''')
        
        if timeframe:
            cursor.execute('''
                SELECT symbol, timeframe, state, updated_at, confidence, strength, method_used
                FROM basic_smc_state
                WHERE timeframe = ?
                ORDER BY symbol
            ''', (timeframe,))
        else:
            cursor.execute('''
                SELECT symbol, timeframe, state, updated_at, confidence, strength, method_used
                FROM basic_smc_state
                ORDER BY symbol, timeframe
            ''')
        
        rows = cursor.fetchall()
        conn.close()
        
        summary = {}
        for row in rows:
            key = f"{row[0]}_{row[1]}"
            summary[key] = {
                'symbol': row[0],
                'timeframe': row[1],
                'state': row[2],
                'updated_at': datetime.fromtimestamp(row[3]).strftime('%Y-%m-%d %H:%M:%S') if row[3] else None,
                'confidence': row[4],
                'strength': row[5],
                'method_used': row[6]
            }
        
        return summary
        
    except Exception as e:
        logger.error(f"Error getting SMC state summary: {e}")
        return {}


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def get_basic_smc_signal(
    df: pd.DataFrame,
    lookback: int = 10,
    symbol: str = 'BTCUSD',
    timeframe: str = DEFAULT_TIMEFRAME
) -> str:
    """
    Simplified synchronous helper to get just the signal string.
    
    Args:
        df: DataFrame with OHLCV data
        lookback: Rolling window for breakout detection
        symbol: Trading symbol for state persistence
        timeframe: Timeframe ('H1', 'H4', or 'D1')
        
    Returns:
        'bullish', 'bearish', or 'neutral' (if no state exists)
    """
    result = analyze_basic_smc(df, lookback, symbol=symbol, timeframe=timeframe)
    return result.get('signal', 'neutral')


def get_breakout_strength(
    df: pd.DataFrame,
    lookback: int = 10,
    symbol: str = 'BTCUSD',
    timeframe: str = DEFAULT_TIMEFRAME
) -> Dict[str, Any]:
    """
    Get detailed breakout strength metrics.
    
    Args:
        df: DataFrame with OHLCV data
        lookback: Rolling window for breakout detection
        symbol: Trading symbol for state persistence
        timeframe: Timeframe ('H1', 'H4', or 'D1')
        
    Returns:
        Dict with breakout metrics including:
            - signal: 'bullish', 'bearish', or 'neutral'
            - confidence: 0-100
            - strength: 0-1
            - distance_to_high: distance to resistance as % of range
            - distance_to_low: distance to support as % of range
            - range_pips: price range in pips
    """
    result = {
        'signal': 'neutral',
        'confidence': 0.0,
        'strength': 0.0,
        'distance_to_high': 0.0,
        'distance_to_low': 0.0,
        'range_pips': 0.0
    }

    try:
        if df is None or len(df) < lookback + 2:
            # Try to return stored state
            stored_state = _get_stored_state(symbol, timeframe)
            if stored_state != 'neutral':
                stored_details = _get_stored_state_details(symbol, timeframe)
                result['signal'] = stored_state
                result['confidence'] = stored_details.get('confidence', 0.0)
                result['strength'] = stored_details.get('strength', 0.0)
            return result

        high = df['high'].values
        low = df['low'].values
        close = df['close'].values

        start_idx = len(close) - lookback - 1
        end_idx = len(close) - 1

        recent_high = np.max(high[start_idx:end_idx])
        recent_low = np.min(low[start_idx:end_idx])
        current_close = close[-1]

        price_range = recent_high - recent_low
        if price_range <= 0:
            return result

        result['range_pips'] = float(price_range)
        result['distance_to_high'] = float((recent_high - current_close) / price_range)
        result['distance_to_low'] = float((current_close - recent_low) / price_range)

        if current_close > recent_high:
            result['signal'] = 'bullish'
            result['confidence'] = min(100.0, 50.0 + ((current_close - recent_high) / price_range) * 50.0)
            result['strength'] = min(1.0, (current_close - recent_high) / price_range / 2.0)

        elif current_close < recent_low:
            result['signal'] = 'bearish'
            result['confidence'] = min(100.0, 50.0 + ((recent_low - current_close) / price_range) * 50.0)
            result['strength'] = min(1.0, (recent_low - current_close) / price_range / 2.0)

        return result

    except Exception as e:
        logger.error(f"Error in get_breakout_strength for {symbol} ({timeframe}): {e}")
        # Try to return stored state on error
        stored_state = _get_stored_state(symbol, timeframe)
        if stored_state != 'neutral':
            stored_details = _get_stored_state_details(symbol, timeframe)
            result['signal'] = stored_state
            result['confidence'] = stored_details.get('confidence', 0.0)
            result['strength'] = stored_details.get('strength', 0.0)
        return result


# ============================================================================
# EXPORTS
# ============================================================================

__all__ = [
    # Main analysis functions
    'analyze_basic_smc',
    'analyze_smc',
    'get_basic_smc_signal',
    'get_basic_smc_binary_signal',      # async - use await
    'get_basic_smc_signal_details',      # async - use await
    
    # State management
    'get_stored_smc_state',
    'get_all_stored_smc_states',
    'clear_smc_state',
    'get_smc_state_summary',
    
    # Detailed metrics
    'get_breakout_strength',
    
    # Low-level functions (for advanced use)
    'detect_swing_points',
    'detect_breakout_levels',
    
    # Constants
    'SUPPORTED_TIMEFRAMES',
    'DEFAULT_TIMEFRAME',
    'DEFAULT_BARS',
    'DEFAULT_LOOKBACK',
    'DEFAULT_SWING_LOOKBACK',
]


# ============================================================================
# DATABASE INITIALIZATION ON IMPORT
# ============================================================================

# Initialize the database when the module is imported
_init_db()


# ============================================================================
# TEST FUNCTION
# ============================================================================

def test_basic_smc_sync():
    """
    Synchronous test for basic_smc.py binary state functionality.
    Tests H1, H4, and D1 timeframes.
    """
    print("=" * 70)
    print("🔬 BASIC SMC BINARY STATE TEST (SYNC) - MULTI-TIMEFRAME")
    print("=" * 70)
    
    # Create test data
    np.random.seed(42)
    n_bars = 200
    
    def create_test_data(n, base_price, swing_high_idx, swing_low_idx, break_price):
        trend = np.linspace(base_price, base_price + 5000, n) + np.random.randn(n) * 100
        high = trend + np.abs(np.random.randn(n) * 150)
        low = trend - np.abs(np.random.randn(n) * 150)
        close = trend + np.random.randn(n) * 75
        
        # Create swing points
        high[swing_high_idx] = base_price + 3000
        low[swing_low_idx] = base_price - 3000
        
        # Force a break at the end
        close[-1] = break_price
        high[-1] = break_price + 20
        
        return pd.DataFrame({
            'high': high,
            'low': low,
            'close': close,
            'open': close - np.random.randn(n) * 50
        })
    
    # Test all timeframes - UPDATED: H1, H4, D1
    timeframes = ['H1', 'H4', 'D1']
    symbols = ['BTCUSD_TEST', 'EURUSD_TEST']
    
    for timeframe in timeframes:
        print(f"\n{'='*60}")
        print(f"📊 TESTING TIMEFRAME: {timeframe}")
        print(f"{'='*60}")
        
        # Create appropriate test data
        if timeframe == 'H1':
            n = 200
        elif timeframe == 'H4':
            n = 150
        else:  # D1
            n = 100
            
        df = create_test_data(n, 60000, 60, 100, 62800)
        
        for symbol in symbols:
            print(f"\n📊 Testing {symbol} ({timeframe}):")
            print("-" * 40)
            
            # Clear existing state
            clear_smc_state(symbol, timeframe)
            
            # Test 1: Uptrend (should return BULLISH)
            result = analyze_basic_smc(df, symbol=symbol, timeframe=timeframe)
            print(f"   Signal: {result['signal']}")
            print(f"   Method: {result.get('method_used', 'unknown')}")
            print(f"   Confidence: {result.get('confidence', 0):.1f}%")
            
            # Test 2: Ranging market (should return stored state)
            range_price = np.ones(n) * 62800 + np.random.randn(n) * 150
            df_range = pd.DataFrame({
                'high': range_price + np.abs(np.random.randn(n) * 150),
                'low': range_price - np.abs(np.random.randn(n) * 150),
                'close': range_price + np.random.randn(n) * 75,
                'open': range_price - np.random.randn(n) * 50
            })
            
            result_stored = analyze_basic_smc(df_range, symbol=symbol, timeframe=timeframe)
            print(f"\n   Stored Signal: {result_stored['signal']} (should be BULLISH)")
            print(f"   Method: {result_stored.get('method_used', 'unknown')}")
            
            # Test 3: Retrieve stored state directly
            stored = get_stored_smc_state(symbol, timeframe)
            print(f"\n   Stored State: {stored.get('state', 'None')}")
            print(f"   Updated: {stored.get('updated_at_str', 'Never')}")
            
            # Clean up
            clear_smc_state(symbol, timeframe)
            print("   🧹 Cleaned up")
    
    print("\n" + "=" * 70)
    print("✅ Basic SMC multi-timeframe test complete")
    print("=" * 70)


async def test_basic_smc_async():
    """
    Async test for basic_smc.py binary state functionality.
    Tests the async get_basic_smc_signal_details function with all timeframes.
    """
    print("=" * 70)
    print("🔬 BASIC SMC BINARY STATE TEST (ASYNC) - MULTI-TIMEFRAME")
    print("=" * 70)
    
    # Create mock MT5 connection
    class MockMT5Connection:
        async def get_ohlcv(self, symbol, timeframe, count):
            print(f"   Fetching {timeframe} data for {symbol} ({count} bars)")
            np.random.seed(42)
            
            n = count
            trend = np.linspace(60000, 65000, n) + np.random.randn(n) * 100
            df = pd.DataFrame({
                'high': trend + np.abs(np.random.randn(n) * 150),
                'low': trend - np.abs(np.random.randn(n) * 150),
                'close': trend + np.random.randn(n) * 75,
                'open': trend - np.random.randn(n) * 50
            })
            # Force a bullish break at the end
            df.loc[df.index[-1], 'close'] = 65200
            df.loc[df.index[-1], 'high'] = 65300
            return df
        
        def is_connected(self):
            return True
    
    mt5 = MockMT5Connection()
    
    test_symbols = ['BTCUSD_TEST', 'EURUSD_TEST']
    timeframes = ['H1', 'H4', 'D1']  # UPDATED
    
    for timeframe in timeframes:
        print(f"\n{'='*60}")
        print(f"📊 TESTING ASYNC WITH TIMEFRAME: {timeframe}")
        print(f"{'='*60}")
        
        # Get default bars for timeframe
        bars = DEFAULT_BARS.get(timeframe, 100)
        
        for symbol in test_symbols:
            print(f"\n📊 Testing {symbol} ({timeframe}):")
            print("-" * 40)
            
            # Clear existing state
            clear_smc_state(symbol, timeframe)
            
            # Get full signal details
            details = await get_basic_smc_signal_details(
                symbol=symbol,
                mt5_conn=mt5,
                data_symbol=f"{symbol}.sc",
                lookback=10,
                swing_lookback=3,
                timeframe=timeframe,
                bars=bars
            )
            
            print(f"   Signal: {details.get('signal')}")
            print(f"   Confidence: {details.get('confidence', 0):.1f}%")
            print(f"   Strength: {details.get('strength', 0):.2f}")
            print(f"   Method: {details.get('method_used', 'unknown')}")
            print(f"   Timeframe: {details.get('timeframe', 'N/A')}")
            
            # Clean up
            clear_smc_state(symbol, timeframe)
            print("   🧹 Cleaned up")
    
    print("\n" + "=" * 70)
    print("✅ Basic SMC async multi-timeframe test complete")
    print("=" * 70)


if __name__ == "__main__":
    # Configure logging for test
    logging.basicConfig(level=logging.INFO)
    
    # Run synchronous test
    test_basic_smc_sync()
    
    # Run async test
    import asyncio
    asyncio.run(test_basic_smc_async())