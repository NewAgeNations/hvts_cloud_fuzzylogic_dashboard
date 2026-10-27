"""
SuperTrendOracleIndicator_M5
A five‑indicator oracle based on:
  - Fibonacci on 30‑minute bars  (smooth np.interp, not step function)
  - Fibonacci on 1‑hour bars     (smooth np.interp, not step function)
  - Polynomial Regression (Degree 3) on 30‑minute bars
  - Polynomial Regression (Degree 3) on 1‑hour bars
  - Polynomial Regression (Degree 3) on 12‑hour bars
All combined using improved fuzzy logic with real ADX weighting.

SIGNAL QUALITY IMPROVEMENTS (v3):
  1. Smooth Fibonacci: np.interp across all 7 canonical Fib levels gives
     continuous output instead of the original 6-step ladder.
  2. Real ADX: Wilder's smoothed Directional Movement Index replaces the
     synthetic 'strength * 50' placeholder. ADX is computed independently on
     the 30m, 1h and 12h bar caches and exposed on every M1Signal.adx field.
  3. Better fuzzy logic:
     - Sigmoid steepness reduced 10 → 5 for genuinely graded membership.
     - ADX-based dynamic weighting: trending markets upweight Poly,
       ranging markets upweight Fib.
     - ADX confidence multiplier dampens signals in flat/choppy conditions.
  4. Hysteresis dead-band (±0.05) in _determine_signal prevents rapid
     BUY↔NEUTRAL oscillation near threshold boundaries.
  5. Cache trimming for data_30m / data_1h / data_12h (was unbounded, now capped at
     2× the longest lookback for each timeframe).

OPTIMIZATION FOR SIGNAL GENERATION FREQUENCY (v3.1):
  1. SIGNAL GENERATION OPTIMIZATION: Signals are now generated ONLY when a
     higher‑timeframe bar is completed (30min, 1h, or 12h). This reduces
     thousands of signals per update to a few per minute.
  2. Last processed timestamp tracking ensures no duplicate bar processing.
  3. Bar completion detection uses proper floor rounding and timestamp comparison.
  4. Added signal_emitted flag to prevent duplicate signals on the same bar.

═══════════════════════════════════════════════════════════════════════════════
v3.2 — WARM-UP FIX (2026-10-14)
═══════════════════════════════════════════════════════════════════════════════

PROBLEM (diagnosed 2026-10-14):
    The oracle was mathematically incapable of emitting a non-neutral reading
    until 3+ of its 5 legs were fully warmed up. Three of five legs
    (poly_30m, poly_1h, poly_12h) require 30h / 40h / 10 days of continuous
    runtime respectively — impossible to satisfy on a fresh install, and
    reset on every restart. With 3 of 5 legs pinned at the 0.5 sentinel, the
    maximum achievable |net_score| was ~0.373 — BELOW the 0.4 threshold.

FIXES APPLIED IN v3.2:

  1. RENORMALIZE OVER WARM LEGS (the primary fix)
     In _calculate_combined_score(), any leg whose backing cache is shorter
     than its lookback contributes ZERO weight and is excluded from the
     weighted average. The total_weight is renormalized over the surviving
     legs. This means a 2-leg-warm oracle produces a genuine 2-leg consensus
     with usable magnitude, instead of a 5-leg consensus diluted by three
     0.5 sentinels.

  2. PER-LEG "READY" TRACKING
     A new helper _leg_is_warm() reports whether each leg has enough history.
     _calculate_combined_score() consults it per-leg and returns the set of
     warm-leg names in its components dict under key '__warm__' so callers
     can see which legs actually contributed.

  3. is_ready() SEMANTICS PRESERVED, BUT NEW is_warm_count()
     is_ready() still means "every leg has enough history" (the strict
     definition used by the reader for the fully-warmed path). A new
     warm_leg_count() returns how many legs are warm, so the reader can
     gate on "at least N legs" if desired.

  4. PARTIAL-WARMUP SIGNAL THRESHOLD
     When fewer than 5 legs are warm, the effective threshold is scaled
     proportionally so a 2-leg-warm oracle isn't required to clear the same
     0.4 that a 5-leg-warm oracle would. The scaling is:
         effective_threshold = base_threshold * sqrt(5 / warm_leg_count)
     This is applied ONLY inside _determine_signal(); the raw net_score and
     the components dict are unchanged, so the reader's own threshold logic
     still works on the raw score.

  5. CACHE TRIM PRESERVED
     Cache-trim logic unchanged. With renormalized weighting, the trim
     no longer causes the previously-observed "warm leg gets evicted and
     score collapses" failure mode.

  6. NO CHANGE to public interfaces
     OracleManager, SignalType, M1Signal, and all method signatures remain
     source-compatible.

═══════════════════════════════════════════════════════════════════════════════
v3.3 — DIRECT HIGHER-TIMEFRAME PRIMING (2026-10-14)
═══════════════════════════════════════════════════════════════════════════════

PROBLEM:
    Even with v3.2's renormalization, a fresh oracle has to accumulate 12h
    bars one at a time — one bar every 12 wall-clock hours, and only if the
    process is alive at the boundary. Reaching poly_12h's required 20 bars
    takes ~10 days of uninterrupted runtime. Every restart resets the clock.
    The bottleneck is the FETCH layer (Gate.io caps fetch_ohlcv at ~1500
    bars/request), not the indicators themselves.

FIX APPLIED IN v3.3:

  NEW PUBLIC METHOD: prime_from_htf(data_30m, data_1h, data_12h)

    Injects pre-fetched higher-timeframe OHLC DataFrames directly into the
    oracle's resample caches, bypassing the M1 accumulation path entirely.
    After priming, _calculate_fibonacci(), _calculate_poly_bullishness(),
    and _calculate_adx() are recomputed immediately on the injected frames,
    so the oracle is fully warm on the very first cycle.

    The incremental M1 path (update() / process_bar()) is unchanged and
    continues to run normally on top of the primed cache: new M1 bars
    extend the current incomplete HTF bar, and completion at the next
    boundary is handled by the existing _complete_*_bar() methods.

    Effects:
      • Warm-up drops from ~9 days to ~1 cycle (3 API calls).
      • Every restart can re-prime in seconds.
      • No behavioral change to the fully-warmed steady-state oracle.
      • No change to thresholds, sigmoid scale, or ADX math.
      • No change to the incremental path — M1 updates still extend bars.

    Signature:
        prime_from_htf(
            data_30m: Optional[pd.DataFrame] = None,
            data_1h:  Optional[pd.DataFrame] = None,
            data_12h: Optional[pd.DataFrame] = None,
        ) -> Dict[str, Any]

    Each frame must have columns [open, high, low, close] (volume optional)
    and a DatetimeIndex aligned to the timeframe's natural bar boundary
    (e.g. 30m frames on :00 and :30, 1h frames on :00, 12h frames on 00:00
    and 12:00). Index will be sorted and de-duplicated internally. Frames
    with fewer rows than the corresponding lookback are accepted but leave
    that leg unwarmed.

    The method is idempotent: calling it with larger frames overwrites the
    caches and recomputes the _latest_* values. Calling it with smaller
    frames does not shrink a previously primed cache (it merges and de-
    duplicates by timestamp, keeping the union).

    Returned dict reports per-leg readiness after priming:
        {
            'warm_legs': [...],
            'warm_count': int,
            'caches': {'data_30m': n, 'data_1h': n, 'data_12h': n},
            'ready': bool,
            'latest': {
                'fib_30m': float, 'fib_1h': float,
                'poly_30m': float, 'poly_1h': float, 'poly_12h': float,
                'adx_30m': float, 'adx_1h': float, 'adx_12h': float,
            },
        }

  NEW HELPER: _recompute_latest_indicators_from_caches()

    Pure function that reads the current cache state and re-populates every
    self._latest_* attribute. Called by prime_from_htf() after injection.
    Also useful for any future cache-persistence layer that wants to
    restore _latest_* values after loading caches from disk.

  NEW HELPER: _merge_htf_frame(existing, incoming) -> pd.DataFrame

    Merges two HTF OHLC frames:
      • normalizes index to DatetimeIndex
      • sorts
      • drops duplicate indices, keeping the NEWER row's values
      • trims to 2× the longest relevant lookback
    Used by prime_from_htf() so repeated priming calls accumulate safely.

WHAT v3.3 DOES NOT DO:
    • It does NOT change the sigmoid scale, thresholds, or ADX formula.
    • It does NOT change the incremental M1 update path.
    • It does NOT change is_ready() / warm_leg_count() semantics.
    • It does NOT change the M1Signal / M1IndicatorValues structures.
    • It does NOT depend on oracle_signal_reader.py to function — the new
      method is fully self-contained; the reader simply calls it.

All external interfaces (OracleManager, SignalType, M1Signal, etc.) remain
unchanged so that dependent modules (main_bot.py, grid_strategy.py, …) work
seamlessly. Existing parameter names and defaults are fully preserved.

Input data must still be 1‑minute bars (M1) for the INCREMENTAL path; the
oracle internally resamples to 30‑minute, 1‑hour and 12‑hour bars for
indicator calculation. The PRIMING path accepts HTF frames directly and
does not require any M1 input.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple, Union, Any, Set
from dataclasses import dataclass, field
from enum import Enum
import warnings
import logging
import time
from datetime import datetime
import sqlite3

warnings.filterwarnings('ignore')

# Try to import ta library (kept for compatibility, not actually used)
try:
    import ta
except ImportError:
    pass

# Configure logging
logger = logging.getLogger(__name__)


# ============================================================================
# Core Constants - Input timeframe is still M1
# ============================================================================

class Timeframe(Enum):
    """Strict M1-only timeframe definition for input data"""
    M1 = "1min"
    MINUTES_1 = "1T"
    
    @classmethod
    def validate(cls, freq: str) -> bool:
        """Validate if the frequency is M1"""
        valid_freqs = ['1min', '1T', '1Min', '1MIN', '1t', '1minute']
        return freq.lower() in valid_freqs


# ============================================================================
# Core Data Structures (unchanged for backward compatibility)
# ============================================================================

class SignalType(Enum):
    """Enumeration of possible signal types"""
    STRONG_BUY = "Strong Buy"
    STRONG_SELL = "Strong Sell"
    NEUTRAL = "Neutral"
    BUY = "Buy"
    SELL = "Sell"
    
    def __str__(self):
        return self.value
    
    @classmethod
    def to_direction(cls, signal_type: 'SignalType') -> str:
        if signal_type in [cls.STRONG_BUY, cls.BUY]:
            return 'bullish'
        elif signal_type in [cls.STRONG_SELL, cls.SELL]:
            return 'bearish'
        else:
            return 'neutral'
    
    @classmethod
    def get_strength(cls, signal_type: 'SignalType') -> str:
        if signal_type == cls.STRONG_BUY:
            return 'strong'
        elif signal_type == cls.STRONG_SELL:
            return 'strong'
        elif signal_type == cls.BUY:
            return 'moderate'
        elif signal_type == cls.SELL:
            return 'moderate'
        else:
            return 'weak'


@dataclass
class M1Signal:
    """Container for M1 trading signals with metadata"""
    type: SignalType
    timestamp: pd.Timestamp
    price: float
    score: int
    adx: float
    confidence: float
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        if self.type in [SignalType.STRONG_BUY, SignalType.STRONG_SELL,
                         SignalType.BUY, SignalType.SELL]:
            self.confidence = min(100, max(0, self.confidence))
        else:
            self.confidence = 0
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'type': self.type.value,
            'timestamp': self.timestamp.isoformat() if hasattr(self.timestamp, 'isoformat') else str(self.timestamp),
            'price': float(self.price),
            'score': int(self.score),
            'adx': float(self.adx),
            'confidence': float(self.confidence),
            'metadata': self.metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'M1Signal':
        return cls(
            type=SignalType(data['type']),
            timestamp=pd.Timestamp(data['timestamp']),
            price=data['price'],
            score=data['score'],
            adx=data['adx'],
            confidence=data['confidence'],
            metadata=data.get('metadata', {})
        )


@dataclass
class M1IndicatorValues:
    """Container for all indicator values at a point in time"""
    timestamp: pd.Timestamp
    close: float
    high: float
    low: float
    # Legacy fields - kept for backward compatibility but set to defaults
    ema: float = 0.0
    macd: float = 0.0
    macd_signal: float = 0.0
    rsi: float = 50.0
    adx: float = 0.0
    psar: float = 0.0
    ichimoku_a: float = 0.0
    ichimoku_b: float = 0.0
    score: int = 0
    volume: Optional[float] = None
    atr: Optional[float] = None
    net_score: Optional[float] = None  # Combined net score
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for easy serialization"""
        return {
            'timestamp': self.timestamp.isoformat() if hasattr(self.timestamp, 'isoformat') else str(self.timestamp),
            'close': float(self.close),
            'high': float(self.high),
            'low': float(self.low),
            'ema': float(self.ema),
            'macd': float(self.macd),
            'macd_signal': float(self.macd_signal),
            'rsi': float(self.rsi),
            'adx': float(self.adx),
            'psar': float(self.psar),
            'ichimoku_a': float(self.ichimoku_a),
            'ichimoku_b': float(self.ichimoku_b),
            'score': int(self.score),
            'volume': float(self.volume) if self.volume is not None else None,
            'atr': float(self.atr) if self.atr is not None else None,
            'net_score': float(self.net_score) if self.net_score is not None else None
        }


# ============================================================================
# M1 Timeframe Validator (unchanged, ensures input is M1)
# ============================================================================

class M1TimeframeValidator:
    """Strict validator for M1 timeframe data."""
    
    BARS_PER_DAY = 1440
    BARS_PER_HOUR = 60
    BARS_PER_WEEK = 10080
    
    @classmethod
    def validate_timeframe(cls, df: pd.DataFrame) -> Tuple[bool, str]:
        if not isinstance(df.index, pd.DatetimeIndex):
            return False, "Index must be DatetimeIndex"
        if len(df) < 2:
            return True, "Insufficient data for frequency detection"
        try:
            freq = pd.infer_freq(df.index)
            if freq:
                if Timeframe.validate(freq):
                    return True, f"Valid M1 timeframe detected: {freq}"
                else:
                    return False, f"Invalid timeframe: {freq}. Must be M1 (1-minute)"
        except:
            pass
        time_diffs = df.index.to_series().diff().dropna().value_counts()
        if len(time_diffs) > 0:
            most_common_diff = time_diffs.index[0]
            if pd.Timedelta(seconds=55) <= most_common_diff <= pd.Timedelta(seconds=65):
                return True, f"Valid M1 data detected (avg spacing: {most_common_diff})"
            else:
                return False, f"Invalid bar spacing: {most_common_diff}. Must be ~1 minute"
        return False, "Could not validate timeframe"
    
    @classmethod
    def resample_to_m1(cls, df: pd.DataFrame) -> pd.DataFrame:
        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }
        agg_dict = {col: ohlc_dict[col] for col in ohlc_dict.keys() if col in df.columns}
        df_m1 = df.resample('1min').agg(agg_dict)
        df_m1 = df_m1.dropna(subset=['open', 'high', 'low', 'close'])
        return df_m1
    
    @classmethod
    def ensure_m1(cls, df: pd.DataFrame, auto_resample: bool = True) -> pd.DataFrame:
        is_valid, message = cls.validate_timeframe(df)
        if is_valid:
            return df
        if auto_resample:
            warnings.warn(f"Resampling data to M1: {message}")
            return cls.resample_to_m1(df)
        else:
            raise ValueError(f"Data must be M1 timeframe. {message}")


# ============================================================================
# Core M1 Indicator Engine (FIVE INDICATORS: 2 Fibonacci + 3 Poly)
# ============================================================================

class SuperTrendOracle_M2:
    """
    Professional M1 timeframe indicator suite.
    NOW BASED ON FIVE INDICATORS:
        - Fibonacci on 30-minute bars
        - Fibonacci on 1-hour bars
        - Polynomial Regression (Degree 3) on 30-minute bars
        - Polynomial Regression (Degree 3) on 1-hour bars
        - Polynomial Regression (Degree 3) on 12-hour bars
    Combined with fuzzy logic.

    Input data is still M1 (1-minute) for the INCREMENTAL path. The PRIMING
    path accepts higher-timeframe frames directly via prime_from_htf() and
    does not require any M1 input.

    OPTIMIZED SIGNAL GENERATION: Signals are generated ONLY when a higher‑timeframe
    bar is completed (30min, 1h, or 12h). This reduces thousands of signals per
    update to a few per minute.

    ═══════════════════════════════════════════════════════════════════════
    v3.2 WARM-UP FIX:
      Any leg whose cache is shorter than its lookback contributes ZERO
      weight and is excluded from the combined score. Total weight is
      renormalized over the surviving (warm) legs, so a partially-warmed
      oracle produces a genuine N-leg consensus instead of a 5-leg
      average diluted by 0.5 sentinels.

    v3.3 DIRECT HTF PRIMING:
      prime_from_htf(data_30m, data_1h, data_12h) injects pre-fetched
      higher-timeframe bars into the resample caches, bypassing the
      slow M1-accumulation path entirely. The oracle becomes fully warm
      in ~1 cycle instead of ~9 days.
    ═══════════════════════════════════════════════════════════════════════

    Parameters:
    ----------
    fib_lookback_30m : int, default=30
        Number of 30-minute bars for first Fibonacci range (30 * 30min = 15 hours)
    fib_lookback_1h : int, default=20
        Number of 1-hour bars for second Fibonacci range (20 * 60min = 20 hours)
    poly_lookback_30m : int, default=60
        Number of 30-minute bars for polynomial regression (60 * 30min = 30 hours)
    poly_lookback_1h : int, default=40
        Number of 1-hour bars for polynomial regression (40 * 60min = 40 hours)
    poly_lookback_12h : int, default=20
        Number of 12-hour bars for polynomial regression (20 * 12h = 240 hours / 10 days)
    buy_threshold : float, default=0.4
        Net score threshold for BUY signals (0.0-1.0 scale)
    strong_buy_threshold : float, default=0.6
        Net score threshold for STRONG_BUY
    sell_threshold : float, default=-0.4
        Net score threshold for SELL (-1.0-0.0 scale)
    strong_sell_threshold : float, default=-0.6
        Net score threshold for STRONG_SELL
    strength_threshold : float, default=0.3
        Minimum net score magnitude for strong signals
    auto_resample : bool, default=True
        Automatically resample non-M1 data to M1
    name : str, default="SuperTrendOracle_M2"
        Instance name
    """
    
    # New default parameters with 12-hour polynomial lookback
    DEFAULT_PARAMS = {
        'fib_lookback_30m': 30,          # 15 hours
        'fib_lookback_1h': 20,           # 20 hours
        'poly_lookback_30m': 60,          # 30 hours
        'poly_lookback_1h': 40,           # 40 hours
        'poly_lookback_12h': 20,          # 20 * 12h = 240 hours (10 days)
        'buy_threshold': 0.4,
        'strong_buy_threshold': 0.6,
        'sell_threshold': -0.4,
        'strong_sell_threshold': -0.6,
        'strength_threshold': 0.3,
        'auto_resample': True,
    }
    
    # M1-specific time constants (unchanged)
    M1_BAR_MINUTES = 1
    M1_BARS_PER_HOUR = 60
    M1_BARS_PER_DAY = 1440
    M1_BARS_PER_12H = 720  # 12 * 60
    
    def __init__(self, **kwargs):
        self.params = self.DEFAULT_PARAMS.copy()
        self.params.update(kwargs)
        self._validate_params()
        
        self.name = kwargs.get('name', "SuperTrendOracle_M2")
        self._last_values: Optional[M1IndicatorValues] = None
        self._last_signal: Optional[M1Signal] = None
        self._indicators_ready = False
        
        # Minimum history required: enough M1 bars to form required bars for all three timeframes
        min_30m_bars = max(self.params['fib_lookback_30m'], self.params['poly_lookback_30m'])
        min_1h_bars = max(self.params['fib_lookback_1h'], self.params['poly_lookback_1h'])
        min_12h_bars = self.params['poly_lookback_12h']
        # 12 hours = 720 minutes, 1 hour = 60 minutes, 30 minutes = 30 minutes
        self._min_history_required = max(
            min_30m_bars * 30,
            min_1h_bars * 60,
            min_12h_bars * 720
        ) + 60  # extra margin
        
        # Performance metrics
        self.metrics = {
            'total_calculations': 0,
            'signals_generated': 0,
            'last_update': None,
            'bars_processed': 0,
            'timeframe_validations': 0,
            'timeframe_violations': 0,
            'last_30m_bar_time': None,
            'last_1h_bar_time': None,
            'last_12h_bar_time': None,
            'prime_calls': 0,           # v3.3
            'prime_bars_injected': 0,   # v3.3
        }
        
        # Cache for raw M1 data and resampled data
        self._cache = {
            'data': pd.DataFrame(),      # raw M1 data
            'data_30m': pd.DataFrame(),  # resampled 30m OHLC
            'data_1h': pd.DataFrame(),   # resampled 1h OHLC
            'data_12h': pd.DataFrame(),  # resampled 12h OHLC
            'signals': []
        }
        
        # Last computed indicator state (includes combined info)
        self._last_indicator_state: Optional[Dict] = None
        self._last_combined_state: Optional[Dict] = None
        
        # Timestamp tracking
        self._last_timestamp: Optional[pd.Timestamp] = None
        self._expected_next_timestamp: Optional[pd.Timestamp] = None
        self._last_processed_timestamp: Optional[pd.Timestamp] = None  # For new bar detection
        
        # Last bar timestamps for which we have indicators
        self._last_30m_bar_computed: Optional[pd.Timestamp] = None
        self._last_1h_bar_computed: Optional[pd.Timestamp] = None
        self._last_12h_bar_computed: Optional[pd.Timestamp] = None
        
        # Track the current incomplete 30m, 1h and 12h bars
        self._current_30m_bar_start: Optional[pd.Timestamp] = None
        self._current_30m_bar_data: List[Dict] = []  # Store incomplete bar data
        self._current_1h_bar_start: Optional[pd.Timestamp] = None
        self._current_1h_bar_data: List[Dict] = []   # Store incomplete bar data
        self._current_12h_bar_start: Optional[pd.Timestamp] = None
        self._current_12h_bar_data: List[Dict] = []  # Store incomplete bar data
        
        # ============ OPTIMIZATION: Track whether signal was emitted for current higher timeframe bars ============
        self._signal_emitted_for_30m_bar: Optional[pd.Timestamp] = None
        self._signal_emitted_for_1h_bar: Optional[pd.Timestamp] = None
        self._signal_emitted_for_12h_bar: Optional[pd.Timestamp] = None
        
        # Internal storage for latest indicator values
        self._latest_fib_30m = 0.5
        self._latest_fib_1h = 0.5
        self._latest_poly_30m = 0.5
        self._latest_poly_1h = 0.5
        self._latest_poly_12h = 0.5
        # Real ADX values (computed from 30m / 1h / 12h bars via Wilder's smoothing)
        self._latest_adx_30m = 20.0   # neutral default (≈ borderline trend)
        self._latest_adx_1h = 20.0
        self._latest_adx_12h = 20.0

        # ============ v3.2: Warm-up tracking ============
        # Set of leg names that contributed to the last combined score.
        # Populated by _calculate_combined_score(); read by callers to know
        # which legs were actually warm at the time of the score.
        self._last_warm_legs: Set[str] = set()
    
    def _validate_params(self):
        """Validate new parameters"""
        for key, value in self.params.items():
            if key in ['fib_lookback_30m', 'fib_lookback_1h', 
                       'poly_lookback_30m', 'poly_lookback_1h', 'poly_lookback_12h']:
                if not isinstance(value, int) or value < 1:
                    raise ValueError(f"{key} must be positive integer, got {value}")
            elif key in ['buy_threshold', 'strong_buy_threshold']:
                if not isinstance(value, (int, float)) or value < 0 or value > 1:
                    raise ValueError(f"{key} must be between 0 and 1, got {value}")
            elif key in ['sell_threshold', 'strong_sell_threshold']:
                if not isinstance(value, (int, float)) or value < -1 or value > 0:
                    raise ValueError(f"{key} must be between -1 and 0, got {value}")
            elif key == 'strength_threshold':
                if not isinstance(value, (int, float)) or value < 0 or value > 1:
                    raise ValueError(f"{key} must be between 0 and 1, got {value}")
            elif key == 'auto_resample':
                if not isinstance(value, bool):
                    raise ValueError(f"{key} must be boolean, got {value}")
    
    def _validate_m1_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Strictly validate and enforce M1 timeframe"""
        self.metrics['timeframe_validations'] += 1
        if df.empty:
            return df
        df_m1 = M1TimeframeValidator.ensure_m1(df, auto_resample=self.params['auto_resample'])
        if len(df_m1) > 1:
            time_diffs = df_m1.index.to_series().diff()
            large_gaps = time_diffs[time_diffs > pd.Timedelta(minutes=1.5)]
            if not large_gaps.empty:
                warnings.warn(f"Found {len(large_gaps)} gaps in M1 data.")
                self.metrics['timeframe_violations'] += len(large_gaps)
        if len(df_m1) > 0:
            seconds = df_m1.index.second
            invalid_starts = seconds[seconds != 0]
            if not invalid_starts.empty:
                warnings.warn(f"Found {len(invalid_starts)} bars not aligned to 1-minute marks.")
        return df_m1
    
    def _check_bar_continuity(self, timestamp: pd.Timestamp) -> bool:
        """Check if bars are arriving in correct M1 sequence"""
        if self._last_timestamp is None:
            self._last_timestamp = timestamp
            self._expected_next_timestamp = timestamp + pd.Timedelta(minutes=1)
            return True
        expected = self._last_timestamp + pd.Timedelta(minutes=1)
        if timestamp == expected:
            self._last_timestamp = timestamp
            self._expected_next_timestamp = timestamp + pd.Timedelta(minutes=1)
            return True
        elif timestamp > expected:
            gaps = (timestamp - expected).total_seconds() / 60
            warnings.warn(f"Gap in M1 data: missing ~{gaps:.0f} bars. Expected {expected}, got {timestamp}")
            self._last_timestamp = timestamp
            self._expected_next_timestamp = timestamp + pd.Timedelta(minutes=1)
            self.metrics['timeframe_violations'] += 1
            return False
        else:
            warnings.warn(f"Out-of-order M1 bar: {timestamp} (last was {self._last_timestamp})")
            self.metrics['timeframe_violations'] += 1
            return False
    
    def prepare_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Prepare and validate input M1 data"""
        required_cols = ['open', 'high', 'low', 'close']
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            column_mapping = {
                'Open': 'open', 'HIGH': 'high', 'Low': 'low', 'Close': 'close', 'Volume': 'volume'
            }
            df = df.rename(columns=column_mapping)
            missing = [col for col in required_cols if col not in df.columns]
            if missing:
                raise ValueError(f"Missing required columns after mapping: {missing}")
        if not isinstance(df.index, pd.DatetimeIndex):
            df = df.copy()
            df.index = pd.to_datetime(df.index)
        df = df.sort_index()
        df = self._validate_m1_data(df)
        return df
    
    # ========== NEW INDICATOR CALCULATIONS ==========
    
    def _resample_to_30m(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resample M1 data to 30-minute OHLC"""
        if df.empty:
            return pd.DataFrame()
        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }
        agg_dict = {col: ohlc_dict[col] for col in ohlc_dict.keys() if col in df.columns}
        df_30m = df.resample('30min').agg(agg_dict)
        df_30m = df_30m.dropna(subset=['open', 'high', 'low', 'close'])
        return df_30m
    
    def _resample_to_1h(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resample M1 data to 1-hour OHLC"""
        if df.empty:
            return pd.DataFrame()
        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }
        agg_dict = {col: ohlc_dict[col] for col in ohlc_dict.keys() if col in df.columns}
        df_1h = df.resample('1h').agg(agg_dict)
        df_1h = df_1h.dropna(subset=['open', 'high', 'low', 'close'])
        return df_1h
    
    def _resample_to_12h(self, df: pd.DataFrame) -> pd.DataFrame:
        """Resample M1 data to 12-hour OHLC"""
        if df.empty:
            return pd.DataFrame()
        ohlc_dict = {
            'open': 'first',
            'high': 'max',
            'low': 'min',
            'close': 'last',
            'volume': 'sum'
        }
        agg_dict = {col: ohlc_dict[col] for col in ohlc_dict.keys() if col in df.columns}
        df_12h = df.resample('12h').agg(agg_dict)
        df_12h = df_12h.dropna(subset=['open', 'high', 'low', 'close'])
        return df_12h
    
    def _update_incomplete_bars(self, row: pd.Series, timestamp: pd.Timestamp):
        """Update incomplete 30m, 1h and 12h bars with new M1 data"""
        # Update 30m incomplete bar
        bar_30m_start = timestamp.floor('30min')
        if self._current_30m_bar_start != bar_30m_start:
            # New 30m bar starting - complete the previous one
            if self._current_30m_bar_data:
                self._complete_30m_bar()
            self._current_30m_bar_start = bar_30m_start
            self._current_30m_bar_data = [row.to_dict()]
        else:
            self._current_30m_bar_data.append(row.to_dict())
        
        # Update 1h incomplete bar
        bar_1h_start = timestamp.floor('1h')
        if self._current_1h_bar_start != bar_1h_start:
            # New 1h bar starting - complete the previous one
            if self._current_1h_bar_data:
                self._complete_1h_bar()
            self._current_1h_bar_start = bar_1h_start
            self._current_1h_bar_data = [row.to_dict()]
        else:
            self._current_1h_bar_data.append(row.to_dict())
        
        # Update 12h incomplete bar
        bar_12h_start = timestamp.floor('12h')
        if self._current_12h_bar_start != bar_12h_start:
            # New 12h bar starting - complete the previous one
            if self._current_12h_bar_data:
                self._complete_12h_bar()
            self._current_12h_bar_start = bar_12h_start
            self._current_12h_bar_data = [row.to_dict()]
        else:
            self._current_12h_bar_data.append(row.to_dict())
    
    def _complete_30m_bar(self):
        """Complete the current 30m bar and compute indicators"""
        if not self._current_30m_bar_data:
            return
        
        # Create OHLC for the completed bar
        df_bar = pd.DataFrame(self._current_30m_bar_data)
        bar_ohlc = {
            'open': df_bar['open'].iloc[0],
            'high': df_bar['high'].max(),
            'low': df_bar['low'].min(),
            'close': df_bar['close'].iloc[-1],
            'volume': df_bar['volume'].sum() if 'volume' in df_bar.columns else 0
        }
        
        # Add to 30m dataframe
        new_bar = pd.DataFrame([bar_ohlc], index=[self._current_30m_bar_start])
        if self._cache['data_30m'].empty:
            self._cache['data_30m'] = new_bar
        else:
            self._cache['data_30m'] = pd.concat([self._cache['data_30m'], new_bar])
        
        # Compute indicators using completed bar
        self._latest_fib_30m = self._calculate_fibonacci(
            self._cache['data_30m'], self.params['fib_lookback_30m'])
        self._latest_poly_30m = self._calculate_poly_bullishness(
            self._cache['data_30m'], self.params['poly_lookback_30m'])
        self._latest_adx_30m = self._calculate_adx(self._cache['data_30m'])
        
        self._last_30m_bar_computed = self._current_30m_bar_start
        self.metrics['last_30m_bar_time'] = self._current_30m_bar_start
        
        logger.debug(f"New 30m bar {self._current_30m_bar_start}: "
                    f"fib={self._latest_fib_30m:.2f}, poly={self._latest_poly_30m:.2f}, "
                    f"adx={self._latest_adx_30m:.1f}")
        
        # Trim cache to avoid unbounded memory growth (keep 2× the longest lookback)
        max_30m_bars = max(self.params['fib_lookback_30m'], self.params['poly_lookback_30m']) * 2
        if len(self._cache['data_30m']) > max_30m_bars:
            self._cache['data_30m'] = self._cache['data_30m'].iloc[-max_30m_bars:]
        
        # Reset signal emission flag for this bar
        self._signal_emitted_for_30m_bar = None
        
        # Clear incomplete bar data
        self._current_30m_bar_data = []
    
    def _complete_1h_bar(self):
        """Complete the current 1h bar and compute indicators"""
        if not self._current_1h_bar_data:
            return
        
        # Create OHLC for the completed bar
        df_bar = pd.DataFrame(self._current_1h_bar_data)
        bar_ohlc = {
            'open': df_bar['open'].iloc[0],
            'high': df_bar['high'].max(),
            'low': df_bar['low'].min(),
            'close': df_bar['close'].iloc[-1],
            'volume': df_bar['volume'].sum() if 'volume' in df_bar.columns else 0
        }
        
        # Add to 1h dataframe
        new_bar = pd.DataFrame([bar_ohlc], index=[self._current_1h_bar_start])
        if self._cache['data_1h'].empty:
            self._cache['data_1h'] = new_bar
        else:
            self._cache['data_1h'] = pd.concat([self._cache['data_1h'], new_bar])
        
        # Compute indicators using completed bar
        self._latest_fib_1h = self._calculate_fibonacci(
            self._cache['data_1h'], self.params['fib_lookback_1h'])
        self._latest_poly_1h = self._calculate_poly_bullishness(
            self._cache['data_1h'], self.params['poly_lookback_1h'])
        self._latest_adx_1h = self._calculate_adx(self._cache['data_1h'])
        
        self._last_1h_bar_computed = self._current_1h_bar_start
        self.metrics['last_1h_bar_time'] = self._current_1h_bar_start
        
        logger.debug(f"New 1h bar {self._current_1h_bar_start}: "
                    f"fib={self._latest_fib_1h:.2f}, poly={self._latest_poly_1h:.2f}, "
                    f"adx={self._latest_adx_1h:.1f}")
        
        # Trim cache to avoid unbounded memory growth (keep 2× the longest lookback)
        max_1h_bars = max(self.params['fib_lookback_1h'], self.params['poly_lookback_1h']) * 2
        if len(self._cache['data_1h']) > max_1h_bars:
            self._cache['data_1h'] = self._cache['data_1h'].iloc[-max_1h_bars:]
        
        # Reset signal emission flag for this bar
        self._signal_emitted_for_1h_bar = None
        
        # Clear incomplete bar data
        self._current_1h_bar_data = []
    
    def _complete_12h_bar(self):
        """Complete the current 12h bar and compute indicators"""
        if not self._current_12h_bar_data:
            return
        
        # Create OHLC for the completed bar
        df_bar = pd.DataFrame(self._current_12h_bar_data)
        bar_ohlc = {
            'open': df_bar['open'].iloc[0],
            'high': df_bar['high'].max(),
            'low': df_bar['low'].min(),
            'close': df_bar['close'].iloc[-1],
            'volume': df_bar['volume'].sum() if 'volume' in df_bar.columns else 0
        }
        
        # Add to 12h dataframe
        new_bar = pd.DataFrame([bar_ohlc], index=[self._current_12h_bar_start])
        if self._cache['data_12h'].empty:
            self._cache['data_12h'] = new_bar
        else:
            self._cache['data_12h'] = pd.concat([self._cache['data_12h'], new_bar])
        
        # Compute indicators using completed bar
        self._latest_poly_12h = self._calculate_poly_bullishness(
            self._cache['data_12h'], self.params['poly_lookback_12h'])
        self._latest_adx_12h = self._calculate_adx(self._cache['data_12h'])
        
        self._last_12h_bar_computed = self._current_12h_bar_start
        self.metrics['last_12h_bar_time'] = self._current_12h_bar_start
        
        logger.debug(f"New 12h bar {self._current_12h_bar_start}: "
                    f"poly={self._latest_poly_12h:.2f}, adx={self._latest_adx_12h:.1f}")
        
        # Trim cache to avoid unbounded memory growth (keep 2× the longest lookback)
        max_12h_bars = self.params['poly_lookback_12h'] * 2
        if len(self._cache['data_12h']) > max_12h_bars:
            self._cache['data_12h'] = self._cache['data_12h'].iloc[-max_12h_bars:]
        
        # Reset signal emission flag for this bar
        self._signal_emitted_for_12h_bar = None
        
        # Clear incomplete bar data
        self._current_12h_bar_data = []
    
    # ========== FIBONACCI INDICATOR (smooth interpolation) ==========
    def _calculate_fibonacci(self, df: pd.DataFrame, lookback: int) -> float:
        """
        Calculate Fibonacci indicator value (bullishness in [0,1]).
        Uses linear interpolation between the 7 canonical Fibonacci price levels
        so the output is a continuous signal rather than a coarse 6-step ladder.
        """
        if len(df) < lookback:
            return 0.5
        recent = df.iloc[-lookback:]
        high = recent['high'].max()
        low = recent['low'].min()
        current_close = recent['close'].iloc[-1]
        if high == low:
            return 0.5
        r = (current_close - low) / (high - low)  # position in [0, 1]

        # Canonical Fibonacci retracement levels → target bullishness scores.
        # Using linear interpolation gives a smooth, continuous output.
        fib_levels  = [0.000, 0.236, 0.382, 0.500, 0.618, 0.764, 1.000]
        bull_scores = [0.000, 0.100, 0.300, 0.500, 0.700, 0.900, 1.000]
        return float(np.interp(r, fib_levels, bull_scores))
    
    # ========== POLYNOMIAL INDICATOR ==========
    def _calculate_poly_bullishness(self, df: pd.DataFrame, lookback: int) -> float:
        """
        Compute a bullishness score (0..1) from polynomial regression of degree 3.
        Uses slope at the last bar normalized by the average bar range.
        """
        if len(df) < lookback:
            return 0.5
        closes = df['close'].values[-lookback:]
        highs = df['high'].values[-lookback:]
        lows = df['low'].values[-lookback:]
        x = np.arange(len(closes))
        # Fit polynomial degree 3
        coeffs = np.polyfit(x, closes, 3)
        poly = np.poly1d(coeffs)
        deriv = np.polyder(poly)
        slope = deriv(len(closes) - 1)          # price change per bar
        avg_range = np.mean(highs - lows)
        if avg_range == 0:
            return 0.5
        norm_slope = slope / avg_range
        # Map to [0,1] using tanh (0.5 = neutral)
        bullish = 0.5 + 0.5 * np.tanh(norm_slope * 3)
        return bullish
    
    # ========== REAL ADX INDICATOR ==========
    def _calculate_adx(self, df: pd.DataFrame, period: int = 14) -> float:
        """
        Compute Wilder's Average Directional Index (ADX) on the given OHLC frame.

        Returns ADX in [0, 100]:
            < 20  → weak / ranging market
            20-25 → borderline
            > 25  → trending
            > 40  → strongly trending

        Falls back to 20.0 (neutral) when insufficient history.
        """
        if len(df) < period + 2:
            return 20.0

        high  = df['high'].values
        low   = df['low'].values
        close = df['close'].values
        n = len(close)

        tr      = np.zeros(n)
        dm_plus = np.zeros(n)
        dm_minus= np.zeros(n)

        for i in range(1, n):
            tr[i] = max(high[i] - low[i],
                        abs(high[i] - close[i - 1]),
                        abs(low[i]  - close[i - 1]))
            up_move   = high[i] - high[i - 1]
            down_move = low[i - 1] - low[i]
            dm_plus[i]  = up_move   if (up_move   > down_move and up_move   > 0) else 0.0
            dm_minus[i] = down_move if (down_move > up_move   and down_move > 0) else 0.0

        def _wilder_smooth(arr: np.ndarray, p: int) -> np.ndarray:
            """
            Wilder's exponential smoothing.
            Seeded with the MEAN of the first p values (not the sum) so that
            all derived values stay in the same numeric range as the input.
            Recurrence: S_i = S_{i-1} * (p-1)/p + X_i / p
            """
            out = np.zeros(len(arr))
            if p >= len(arr):
                return out
            out[p] = arr[1:p + 1].mean()   # mean seed → keeps scale correct
            for i in range(p + 1, len(arr)):
                out[i] = out[i - 1] * (p - 1) / p + arr[i] / p
            return out

        atr_s = _wilder_smooth(tr,       period)
        dp_s  = _wilder_smooth(dm_plus,  period)
        dm_s  = _wilder_smooth(dm_minus, period)

        with np.errstate(invalid='ignore', divide='ignore'):
            di_plus  = np.where(atr_s > 0, 100.0 * dp_s / atr_s, 0.0)
            di_minus = np.where(atr_s > 0, 100.0 * dm_s / atr_s, 0.0)
            di_sum   = di_plus + di_minus
            dx       = np.where(di_sum > 0,
                                100.0 * np.abs(di_plus - di_minus) / di_sum,
                                0.0)

        adx_arr = _wilder_smooth(dx, period)

        # Return the most recent non-zero value
        valid = adx_arr[adx_arr > 0]
        return float(valid[-1]) if len(valid) > 0 else 20.0

    # ================================================================
    # v3.2: Per-leg warm-up introspection
    # ================================================================
    def _leg_is_warm(self, leg_name: str) -> bool:
        """
        Return True iff the named leg has enough cache history to
        contribute a real (non-sentinel) value to the combined score.

        A leg is warm iff:
            len(cache_for_its_timeframe) >= its lookback
        """
        if leg_name == 'fib_30m':
            return len(self._cache['data_30m']) >= self.params['fib_lookback_30m']
        if leg_name == 'poly_30m':
            return len(self._cache['data_30m']) >= self.params['poly_lookback_30m']
        if leg_name == 'fib_1h':
            return len(self._cache['data_1h'])  >= self.params['fib_lookback_1h']
        if leg_name == 'poly_1h':
            return len(self._cache['data_1h'])  >= self.params['poly_lookback_1h']
        if leg_name == 'poly_12h':
            return len(self._cache['data_12h']) >= self.params['poly_lookback_12h']
        return False

    def get_warm_legs(self) -> Set[str]:
        """Return the set of leg names currently warm."""
        return {
            name for name in
            ('fib_30m', 'poly_30m', 'fib_1h', 'poly_1h', 'poly_12h')
            if self._leg_is_warm(name)
        }

    def warm_leg_count(self) -> int:
        """Number of legs currently warm (0..5)."""
        return len(self.get_warm_legs())

    def get_warmup_report(self) -> Dict[str, Any]:
        """
        Diagnostic dict for external callers (mirrors the reader's
        get_warmup_status() but from the oracle side).
        """
        return {
            'warm_count': self.warm_leg_count(),
            'total_legs': 5,
            'warm_legs': sorted(self.get_warm_legs()),
            'caches': {
                'data_30m': len(self._cache['data_30m']),
                'data_1h':  len(self._cache['data_1h']),
                'data_12h': len(self._cache['data_12h']),
            },
            'lookbacks': {
                'fib_30m':  self.params['fib_lookback_30m'],
                'poly_30m': self.params['poly_lookback_30m'],
                'fib_1h':   self.params['fib_lookback_1h'],
                'poly_1h':  self.params['poly_lookback_1h'],
                'poly_12h': self.params['poly_lookback_12h'],
            },
        }

    # ================================================================
    # v3.3: Direct higher-timeframe priming
    # ================================================================
    def _merge_htf_frame(
        self,
        existing: pd.DataFrame,
        incoming: pd.DataFrame,
        max_bars: int,
    ) -> pd.DataFrame:
        """
        Merge two higher-timeframe OHLC frames safely.

          • Normalizes incoming to a sorted DatetimeIndex.
          • Concatenates with existing.
          • Drops duplicate indices, keeping the NEWER (incoming) row.
          • Trims to `max_bars` most-recent rows.

        Used by prime_from_htf() so repeated priming calls accumulate
        rather than replace, and so the cache never grows unbounded.
        """
        if incoming is None or incoming.empty:
            out = existing if existing is not None else pd.DataFrame()
        else:
            df_in = incoming.copy()
            if not isinstance(df_in.index, pd.DatetimeIndex):
                df_in.index = pd.to_datetime(df_in.index)
            df_in = df_in.sort_index()

            # Required columns — drop rows missing OHLC
            for col in ('open', 'high', 'low', 'close'):
                if col not in df_in.columns:
                    raise ValueError(
                        f"_merge_htf_frame: incoming frame is missing "
                        f"required column '{col}'"
                    )
            df_in = df_in.dropna(subset=['open', 'high', 'low', 'close'])

            if existing is None or existing.empty:
                out = df_in
            else:
                df_ex = existing.copy()
                if not isinstance(df_ex.index, pd.DatetimeIndex):
                    df_ex.index = pd.to_datetime(df_ex.index)
                df_ex = df_ex.sort_index()

                # Concat, then keep LAST occurrence for any duplicate index
                # (i.e. the incoming row wins — it is presumed to be the
                # most recent/complete version of that bar).
                combined = pd.concat([df_ex, df_in])
                combined = combined[~combined.index.duplicated(keep='last')]
                out = combined.sort_index()

        # Trim to max_bars most-recent rows
        if out is not None and not out.empty and len(out) > max_bars:
            out = out.iloc[-max_bars:]
        return out if out is not None else pd.DataFrame()

    def _recompute_latest_indicators_from_caches(self) -> None:
        """
        Recompute every self._latest_* attribute from the current resample
        caches. Called by prime_from_htf() after injection.

        This is deliberately side-effect-free beyond mutating the
        self._latest_* attributes — it does not touch timestamps, flags,
        or signal state.
        """
        # 30m-derived
        self._latest_fib_30m = self._calculate_fibonacci(
            self._cache['data_30m'], self.params['fib_lookback_30m'])
        self._latest_poly_30m = self._calculate_poly_bullishness(
            self._cache['data_30m'], self.params['poly_lookback_30m'])
        self._latest_adx_30m = self._calculate_adx(self._cache['data_30m'])

        # 1h-derived
        self._latest_fib_1h = self._calculate_fibonacci(
            self._cache['data_1h'], self.params['fib_lookback_1h'])
        self._latest_poly_1h = self._calculate_poly_bullishness(
            self._cache['data_1h'], self.params['poly_lookback_1h'])
        self._latest_adx_1h = self._calculate_adx(self._cache['data_1h'])

        # 12h-derived
        self._latest_poly_12h = self._calculate_poly_bullishness(
            self._cache['data_12h'], self.params['poly_lookback_12h'])
        self._latest_adx_12h = self._calculate_adx(self._cache['data_12h'])

    def prime_from_htf(
        self,
        data_30m: Optional[pd.DataFrame] = None,
        data_1h:  Optional[pd.DataFrame] = None,
        data_12h: Optional[pd.DataFrame] = None,
    ) -> Dict[str, Any]:
        """
        Inject pre-fetched higher-timeframe OHLC frames directly into the
        oracle's resample caches, bypassing the M1 accumulation path.

        This is the FAST warm-up path: instead of waiting ~9 days for 12h
        bars to accumulate one at a time, the caller fetches them directly
        (a handful of API calls) and primes the oracle in ~1 cycle.

        Args:
            data_30m : DataFrame with [open, high, low, close] (volume
                       optional) on a 30-minute DatetimeIndex.
            data_1h  : DataFrame with [open, high, low, close] (volume
                       optional) on an hourly DatetimeIndex.
            data_12h : DataFrame with [open, high, low, close] (volume
                       optional) on a 12-hour DatetimeIndex.

            All three are optional — supplying any subset primes the
            corresponding legs. Supplying None leaves the existing cache
            for that timeframe untouched.

        Returns:
            dict with the post-priming warm-up state:
                {
                    'warm_legs': [...],
                    'warm_count': int,
                    'caches': {'data_30m': n, 'data_1h': n, 'data_12h': n},
                    'ready': bool,
                    'latest': {
                        'fib_30m': float, 'fib_1h': float,
                        'poly_30m': float, 'poly_1h': float,
                        'poly_12h': float,
                        'adx_30m': float, 'adx_1h': float, 'adx_12h': float,
                    },
                }

        Notes:
            • Idempotent: calling multiple times merges with existing cache,
              keeping the newer row for duplicate timestamps.
            • Does NOT touch the incremental M1 state (last_processed_timestamp,
              current_*_bar_data, etc.). If the caller subsequently feeds M1
              bars via update(), the M1 path will extend the most recent HTF
              bar from the primed cache rather than starting a new one — as
              long as the M1 bar's floor() matches the last HTF bar's index.
            • Does NOT emit a signal by itself. After priming, the caller
              should either call process_bar() with a fresh M1 bar, or call
              _calculate_combined_score() directly to inspect the combined
              state.
        """
        injected_counts: Dict[str, int] = {
            'data_30m': 0, 'data_1h': 0, 'data_12h': 0
        }

        # ── 30m ──
        if data_30m is not None and not data_30m.empty:
            max_30m = max(
                self.params['fib_lookback_30m'],
                self.params['poly_lookback_30m'],
            ) * 2
            before = len(self._cache['data_30m'])
            self._cache['data_30m'] = self._merge_htf_frame(
                self._cache['data_30m'], data_30m, max_30m
            )
            injected_counts['data_30m'] = len(self._cache['data_30m']) - before
            if not self._cache['data_30m'].empty:
                self._last_30m_bar_computed = self._cache['data_30m'].index[-1]

        # ── 1h ──
        if data_1h is not None and not data_1h.empty:
            max_1h = max(
                self.params['fib_lookback_1h'],
                self.params['poly_lookback_1h'],
            ) * 2
            before = len(self._cache['data_1h'])
            self._cache['data_1h'] = self._merge_htf_frame(
                self._cache['data_1h'], data_1h, max_1h
            )
            injected_counts['data_1h'] = len(self._cache['data_1h']) - before
            if not self._cache['data_1h'].empty:
                self._last_1h_bar_computed = self._cache['data_1h'].index[-1]

        # ── 12h ──
        if data_12h is not None and not data_12h.empty:
            max_12h = self.params['poly_lookback_12h'] * 2
            before = len(self._cache['data_12h'])
            self._cache['data_12h'] = self._merge_htf_frame(
                self._cache['data_12h'], data_12h, max_12h
            )
            injected_counts['data_12h'] = len(self._cache['data_12h']) - before
            if not self._cache['data_12h'].empty:
                self._last_12h_bar_computed = self._cache['data_12h'].index[-1]

        # ── Recompute every _latest_* value from the new caches ──
        self._recompute_latest_indicators_from_caches()

        # ── Metrics ──
        self.metrics['prime_calls'] += 1
        self.metrics['prime_bars_injected'] += sum(injected_counts.values())

        report = self.get_warmup_report()
        report['injected'] = injected_counts
        report['latest'] = {
            'fib_30m':  float(self._latest_fib_30m),
            'fib_1h':   float(self._latest_fib_1h),
            'poly_30m': float(self._latest_poly_30m),
            'poly_1h':  float(self._latest_poly_1h),
            'poly_12h': float(self._latest_poly_12h),
            'adx_30m':  float(self._latest_adx_30m),
            'adx_1h':   float(self._latest_adx_1h),
            'adx_12h':  float(self._latest_adx_12h),
        }

        logger.info(
            f"⚡ Oracle primed from HTF | injected: "
            f"30m=+{injected_counts['data_30m']} "
            f"1h=+{injected_counts['data_1h']} "
            f"12h=+{injected_counts['data_12h']} | "
            f"caches: 30m={report['caches']['data_30m']} "
            f"1h={report['caches']['data_1h']} "
            f"12h={report['caches']['data_12h']} | "
            f"warm_legs={report['warm_count']}/5 "
            f"({', '.join(report['warm_legs']) or 'none'})"
        )

        return report

    # ================================================================
    # v3.2: Combined score with warm-leg renormalization
    # ================================================================
    def _calculate_combined_score(self) -> Tuple[float, float, Dict]:
        """
        Combine the five indicators using improved fuzzy logic with
        warm-leg renormalization (v3.2).

        DESIGN:
          1. Sigmoid steepness of 5 (unchanged from v3) gives genuinely
             graded membership across [0.2, 0.8].
          2. ADX-based dynamic weighting between Fib (ranging) and Poly
             (trending) is computed as in v3 — but ONLY over the legs
             that are actually warm.
          3. Any leg whose backing cache is shorter than its lookback
             contributes ZERO weight and is EXCLUDED. The total weight is
             renormalized over the surviving legs.

        EFFECT:
          A 2-leg-warm oracle is a genuine 2-leg consensus, not a 5-leg
          consensus diluted by three 0.5 sentinels. This is what allows
          the oracle to produce usable |net_score| values during warm-up.

        The components dict includes '__warm__' (a sorted list of warm
        leg names) so callers can see which legs contributed.
        """
        # ── Raw leg values (each returns 0.5 if not warmed) ──
        fib30   = self._latest_fib_30m
        fib1h   = self._latest_fib_1h
        poly30  = self._latest_poly_30m
        poly1h  = self._latest_poly_1h
        poly12h = self._latest_poly_12h
        adx30   = self._latest_adx_30m
        adx1h   = self._latest_adx_1h
        adx12h  = self._latest_adx_12h

        # ── v3.2: per-leg warm flags ──
        warm_fib_30m  = self._leg_is_warm('fib_30m')
        warm_poly_30m = self._leg_is_warm('poly_30m')
        warm_fib_1h   = self._leg_is_warm('fib_1h')
        warm_poly_1h  = self._leg_is_warm('poly_1h')
        warm_poly_12h = self._leg_is_warm('poly_12h')

        warm_legs: List[str] = [
            name for name, flag in (
                ('fib_30m',  warm_fib_30m),
                ('poly_30m', warm_poly_30m),
                ('fib_1h',   warm_fib_1h),
                ('poly_1h',  warm_poly_1h),
                ('poly_12h', warm_poly_12h),
            ) if flag
        ]
        self._last_warm_legs = set(warm_legs)

        components = {
            'fib_30m':  fib30,
            'fib_1h':   fib1h,
            'poly_30m': poly30,
            'poly_1h':  poly1h,
            'poly_12h': poly12h,
            'adx_30m':  adx30,
            'adx_1h':   adx1h,
            'adx_12h':  adx12h,
            '__warm__': sorted(warm_legs),
        }

        # ── Degenerate: no legs warm → neutral, zero strength ──
        if not warm_legs:
            return 0.0, 0.0, components

        # ── Step 1: genuine fuzzy sigmoid (scale=5) ──
        # At v=0.7: mu_bull ≈ 0.73
        # At v=0.3: mu_bull ≈ 0.27
        scale = 5

        def _mu_bull(v: float) -> float:
            return 1.0 / (1.0 + np.exp(-scale * (v - 0.5)))

        # ── Step 2: ADX-driven regime weights (only over warm legs) ──
        # adx_regime in [0,1]: 0 = ranging, 1 = strongly trending
        # (inflection at ADX=25)
        adx_avg    = (adx30 + adx1h + adx12h) / 3.0
        adx_regime = float(np.clip(
            1.0 / (1.0 + np.exp(-0.20 * (adx_avg - 25))), 0.0, 1.0
        ))

        # Base weights per leg family (before per-leg zeroing)
        w_fib_base  = 1.0 - 0.30 * adx_regime   # [0.70 → 1.00]
        w_poly_base = 0.70 + 0.30 * adx_regime  # [0.70 → 1.00]

        # v3.2: apply warm-leg mask, then renormalize over survivors
        leg_weights: Dict[str, float] = {
            'fib_30m':  w_fib_base  if warm_fib_30m  else 0.0,
            'fib_1h':   w_fib_base  if warm_fib_1h   else 0.0,
            'poly_30m': w_poly_base if warm_poly_30m else 0.0,
            'poly_1h':  w_poly_base if warm_poly_1h  else 0.0,
            'poly_12h': w_poly_base if warm_poly_12h else 0.0,
        }
        total_weight = sum(leg_weights.values())
        if total_weight <= 0.0:
            return 0.0, 0.0, components

        # ── Step 3: weighted bull / bear averages over warm legs only ──
        leg_values: Dict[str, float] = {
            'fib_30m':  fib30,
            'fib_1h':   fib1h,
            'poly_30m': poly30,
            'poly_1h':  poly1h,
            'poly_12h': poly12h,
        }

        avg_bull = 0.0
        avg_bear = 0.0
        for name, w in leg_weights.items():
            if w <= 0.0:
                continue
            v  = leg_values[name]
            mu = _mu_bull(v)
            avg_bull += w * mu
            avg_bear += w * (1.0 - mu)
        avg_bull /= total_weight
        avg_bear /= total_weight

        net_score    = avg_bull - avg_bear   # [-1, 1]
        raw_strength = abs(net_score)

        # ── Step 4: ADX confidence multiplier ──
        adx_multiplier = float(np.clip(adx_avg / 25.0, 0.40, 1.50))
        strength = min(1.0, raw_strength * adx_multiplier)

        # ── Record which legs contributed, on the components dict ──
        components['__weights__'] = {
            k: round(v / total_weight, 4) for k, v in leg_weights.items()
            if v > 0.0
        }

        return net_score, strength, components
    
    def _determine_signal(self, net_score: float, strength: float) -> Tuple[SignalType, float]:
        """
        Determine signal type and confidence.

        HYSTERESIS (±0.05) prevents rapid BUY↔NEUTRAL oscillation near
        threshold boundaries: once a directional state is active, it takes
        a slightly larger reverse move to exit it.

        v3.2 PARTIAL-WARMUP THRESHOLD SCALING:
          When fewer than 5 legs are warm, the base buy/sell threshold is
          scaled up by sqrt(5 / warm_leg_count) — i.e. the threshold is
          RAISED proportionally to the missing confidence, so a 2-leg-warm
          oracle can't fire on noise. This preserves the "signal = strong
          consensus" contract while still allowing a partially-warmed
          oracle to eventually emit a real signal.

          Formula:  effective_threshold = base * sqrt(5 / warm_count)
            5 legs warm → base × 1.00  (unchanged)
            4 legs warm → base × 1.12
            3 legs warm → base × 1.29
            2 legs warm → base × 1.58
            1 leg  warm → base × 2.24

          If warm_count == 0 → no signal is possible; return NEUTRAL.
        """
        warm_count = self.warm_leg_count()
        if warm_count <= 0:
            return SignalType.NEUTRAL, 0.0

        # Scaling factor: 1.0 at 5 legs, grows as legs are missing.
        scale_factor = float(np.sqrt(5.0 / float(warm_count)))

        confidence = strength * 100.0
        HYSTERESIS = 0.05

        # Loosen thresholds in the direction of the last signal (sticky state)
        last_type  = self._last_signal.type if self._last_signal else SignalType.NEUTRAL
        buy_thresh        = self.params['buy_threshold']        * scale_factor
        strong_buy_thresh = self.params['strong_buy_threshold'] * scale_factor
        sell_thresh       = self.params['sell_threshold']       * scale_factor
        strong_sell_thresh= self.params['strong_sell_threshold']* scale_factor

        if last_type in (SignalType.BUY, SignalType.STRONG_BUY):
            buy_thresh        -= HYSTERESIS
            strong_buy_thresh -= HYSTERESIS
        elif last_type in (SignalType.SELL, SignalType.STRONG_SELL):
            sell_thresh        += HYSTERESIS
            strong_sell_thresh += HYSTERESIS

        if net_score >= strong_buy_thresh and strength >= self.params['strength_threshold']:
            return SignalType.STRONG_BUY, confidence
        elif net_score >= buy_thresh:
            return SignalType.BUY, confidence
        elif net_score <= strong_sell_thresh and strength >= self.params['strength_threshold']:
            return SignalType.STRONG_SELL, confidence
        elif net_score <= sell_thresh:
            return SignalType.SELL, confidence
        else:
            return SignalType.NEUTRAL, 0.0
    
    def calculate_score(self, row: pd.Series) -> int:
        """Convert net_score to integer -5..5"""
        net_score, _, _ = self._calculate_combined_score()
        return max(-5, min(5, int(round(net_score * 5))))
    
    def determine_signal(self, score: int, adx: float) -> SignalType:
        """Legacy: use combined state if available"""
        if self._last_combined_state is not None:
            net_score = self._last_combined_state.get('net_score', score/5.0)
            strength = self._last_combined_state.get('strength', abs(net_score))
            sig_type, _ = self._determine_signal(net_score, strength)
            return sig_type
        # fallback
        if score >= 4:
            return SignalType.STRONG_BUY
        elif score <= -4:
            return SignalType.STRONG_SELL
        elif score >= 2:
            return SignalType.BUY
        elif score <= -2:
            return SignalType.SELL
        return SignalType.NEUTRAL
    
    def calculate_confidence(self, score: int, adx: float) -> float:
        """Legacy: use combined state if available"""
        if self._last_combined_state is not None:
            net_score = self._last_combined_state.get('net_score', score/5.0)
            strength = self._last_combined_state.get('strength', abs(net_score))
            _, confidence = self._determine_signal(net_score, strength)
            return confidence
        return 0.0
    
    # ============ OPTIMIZED: process_bar with controlled signal emission ============
    def process_bar(self, timestamp: pd.Timestamp, row: pd.Series) -> Optional[M1Signal]:
        """
        Process a single M1 bar and generate signal if conditions are met.
        OPTIMIZED: Signals are generated ONLY when a higher‑timeframe bar is completed.
        This reduces thousands of signals per update to a few per minute.
        """
        self._check_bar_continuity(timestamp)
        self.metrics['bars_processed'] += 1
        
        # Update incomplete bars - this may trigger completion of higher timeframe bars
        self._update_incomplete_bars(row, timestamp)
        
        # Compute combined score using latest indicators
        net_score, strength, components = self._calculate_combined_score()
        int_score = max(-5, min(5, int(round(net_score * 5))))
        
        self._last_combined_state = {
            'net_score': net_score,
            'strength': strength,
            'components': components,
            'warm_legs': sorted(self._last_warm_legs),
            'timestamp': timestamp
        }
        
        # Real ADX: average of 30m, 1h and 12h computed values
        real_adx = (self._latest_adx_30m + self._latest_adx_1h + self._latest_adx_12h) / 3.0

        # Update last indicator state (for get_derived_signal)
        self._last_indicator_state = {
            'timestamp': timestamp,
            'score': int_score,
            'net_score': net_score,
            'adx': real_adx,
            'close': float(row['close']),
            'rsi': 50.0,
            'ema': 0.0,
            'macd': 0.0,
            'macd_signal': 0.0,
            'psar': 0.0,
            'ichimoku_a': 0.0,
            'ichimoku_b': 0.0,
            'plus_di': 0.0,
            'minus_di': 0.0,
            'atr': 0.0,
            'fib_components': components
        }
        
        # ============ OPTIMIZATION: Only generate signal when a higher timeframe bar is completed ============
        # Check if any higher timeframe bar was just completed
        signal_should_emit = False
        signal_reason = None
        
        # Check for new 30m bar completion
        if self._last_30m_bar_computed and self._signal_emitted_for_30m_bar != self._last_30m_bar_computed:
            signal_should_emit = True
            signal_reason = "30m_bar_completed"
            self._signal_emitted_for_30m_bar = self._last_30m_bar_computed
        
        # Check for new 1h bar completion
        elif self._last_1h_bar_computed and self._signal_emitted_for_1h_bar != self._last_1h_bar_computed:
            signal_should_emit = True
            signal_reason = "1h_bar_completed"
            self._signal_emitted_for_1h_bar = self._last_1h_bar_computed
        
        # Check for new 12h bar completion
        elif self._last_12h_bar_computed and self._signal_emitted_for_12h_bar != self._last_12h_bar_computed:
            signal_should_emit = True
            signal_reason = "12h_bar_completed"
            self._signal_emitted_for_12h_bar = self._last_12h_bar_computed
        
        # Determine signal type using hysteresis-aware thresholds
        # (v3.2: partial-warmup scaling is applied inside _determine_signal)
        signal_type, confidence = self._determine_signal(net_score, strength)
        
        # Only emit signal if it's non-neutral AND we have a higher timeframe bar completion
        if signal_should_emit and signal_type in [SignalType.STRONG_BUY, SignalType.STRONG_SELL, SignalType.BUY, SignalType.SELL]:
            metadata = {
                'components': components,
                'net_score': float(net_score),
                'strength': float(strength),
                'fib_30m': float(self._latest_fib_30m),
                'fib_1h': float(self._latest_fib_1h),
                'poly_30m': float(self._latest_poly_30m),
                'poly_1h': float(self._latest_poly_1h),
                'poly_12h': float(self._latest_poly_12h),
                'adx_30m': float(self._latest_adx_30m),
                'adx_1h': float(self._latest_adx_1h),
                'adx_12h': float(self._latest_adx_12h),
                'adx_avg': float(real_adx),
                'warm_legs': sorted(self._last_warm_legs),
                'warm_count': self.warm_leg_count(),
                'bar_number': self.metrics['bars_processed'],
                'timeframe': 'M1 (30m+1h Fibonacci + 30m+1h+12h Poly)',
                'score_components': components,
                'signal_reason': signal_reason,
                'signal_timestamp': timestamp.isoformat()
            }
            signal = M1Signal(
                type=signal_type,
                timestamp=timestamp,
                price=float(row['close']),
                score=int_score,
                adx=float(real_adx),
                confidence=float(confidence),
                metadata=metadata
            )
            self._last_signal = signal
            self.metrics['signals_generated'] += 1
            
            logger.debug(f"📊 Signal emitted at {timestamp} - {signal_type.value} (reason: {signal_reason}, net_score: {net_score:.3f}, warm_legs: {len(self._last_warm_legs)}/5)")
            return signal
        
        return None
    
    def update(self, df: pd.DataFrame) -> List[M1Signal]:
        """
        Update with new M1 data. Processes each bar sequentially to correctly handle
        higher timeframe bar completions and avoid look-ahead bias.
        
        OPTIMIZED: Only processes new bars that have not been processed before.
        """
        signals = []
        df = self.prepare_data(df)
        if df.empty:
            return signals
        
        # Filter only new bars that haven't been processed
        if self._last_processed_timestamp is not None:
            new_df = df[df.index > self._last_processed_timestamp]
            if new_df.empty:
                logger.debug(f"No new bars since {self._last_processed_timestamp}")
                return signals
            df = new_df
            logger.debug(f"Processing {len(df)} new bars since {self._last_processed_timestamp}")
        
        # Process each M1 bar in chronological order
        for timestamp, row in df.iterrows():
            signal = self.process_bar(timestamp, row)
            if signal:
                signals.append(signal)
            
            # Update last values for compatibility
            self._last_values = M1IndicatorValues(
                timestamp=timestamp,
                close=float(row['close']),
                high=float(row['high']),
                low=float(row['low']),
                score=self.calculate_score(row),
                volume=float(row.get('volume', 0)) if 'volume' in row else None,
                net_score=self._last_combined_state['net_score'] if self._last_combined_state else None,
                adx=float((self._latest_adx_30m + self._latest_adx_1h + self._latest_adx_12h) / 3.0),
                rsi=50.0
            )
            self.metrics['total_calculations'] += 1
            
            # Add to cache (limited size)
            self._cache['data'] = pd.concat([self._cache['data'], row.to_frame().T])
        
        # Update last processed timestamp
        if not df.empty:
            self._last_processed_timestamp = df.index[-1]
        
        # Keep cache manageable
        if len(self._cache['data']) > 10000:
            self._cache['data'] = self._cache['data'].iloc[-10000:]
        
        self._cache['signals'].extend(signals)
        self._cache['signals'] = self._cache['signals'][-100:]
        
        self.metrics['last_update'] = pd.Timestamp.now()
        
        if signals:
            logger.info(f"Generated {len(signals)} signal(s) from {len(df)} new bars")
        
        return signals
    
    def backtest(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Run the indicator on historical M1 data and return full results.
        Processes bars sequentially to eliminate look-ahead bias.
        Now includes polynomial indicators for 12h.
        """
        df = self.prepare_data(df)
        
        # Reset state for clean backtest
        self.reset()
        
        # Prepare results dataframe
        results = df.copy()
        results['fib_30m'] = 0.5
        results['fib_1h'] = 0.5
        results['poly_30m'] = 0.5
        results['poly_1h'] = 0.5
        results['poly_12h'] = 0.5
        results['net_score'] = 0.0
        results['strength'] = 0.0
        results['signal'] = 'Neutral'
        results['signal_type'] = SignalType.NEUTRAL
        results['confidence'] = 0.0
        results['score'] = 0
        
        # Store indicator values at each step
        fib_30m_values = []
        fib_1h_values = []
        poly_30m_values = []
        poly_1h_values = []
        poly_12h_values = []
        net_scores = []
        strengths = []
        signals = []
        confidences = []
        scores = []
        
        # Process each bar sequentially
        for timestamp, row in df.iterrows():
            # Process the bar (this will update higher timeframe indicators only when bars complete)
            self.process_bar(timestamp, row)
            
            # Store current indicator values
            fib_30m_values.append(self._latest_fib_30m)
            fib_1h_values.append(self._latest_fib_1h)
            poly_30m_values.append(self._latest_poly_30m)
            poly_1h_values.append(self._latest_poly_1h)
            poly_12h_values.append(self._latest_poly_12h)
            
            # Get combined state
            if self._last_combined_state:
                net_scores.append(self._last_combined_state['net_score'])
                strengths.append(self._last_combined_state['strength'])
            else:
                net_scores.append(0.0)
                strengths.append(0.0)
            
            # Determine signal
            net_score = net_scores[-1]
            strength = strengths[-1]
            sig_type, conf = self._determine_signal(net_score, strength)
            signals.append(sig_type.value)
            confidences.append(conf)
            scores.append(max(-5, min(5, int(round(net_score * 5)))))
        
        # Assign results
        results['fib_30m'] = fib_30m_values
        results['fib_1h'] = fib_1h_values
        results['poly_30m'] = poly_30m_values
        results['poly_1h'] = poly_1h_values
        results['poly_12h'] = poly_12h_values
        results['net_score'] = net_scores
        results['strength'] = strengths
        results['signal'] = signals
        results['signal_type'] = [SignalType(s) for s in signals]
        results['confidence'] = confidences
        results['score'] = scores
        results['timeframe'] = 'M1 (30m+1h Fibonacci + 30m+1h+12h Poly)'
        
        return results
    
    def get_time_remaining(self, current_time: Optional[pd.Timestamp] = None) -> pd.Timedelta:
        """Time until next M1 bar"""
        if current_time is None:
            current_time = pd.Timestamp.now()
        total_seconds = current_time.minute * 60 + current_time.second
        next_total_seconds = ((total_seconds // 60) + 1) * 60
        if next_total_seconds >= 3600:
            next_total_seconds = 0
            next_time = current_time.replace(minute=0, second=0, microsecond=0) + pd.Timedelta(hours=1)
        else:
            next_minute = next_total_seconds // 60
            next_second = next_total_seconds % 60
            next_time = current_time.replace(minute=next_minute, second=next_second, microsecond=0)
        return next_time - current_time
    
    def get_last_signal(self) -> Optional[M1Signal]:
        """Get the last generated signal (includes derived fallback)"""
        if self._last_signal is not None:
            return self._last_signal
        return self.get_derived_signal()
    
    def get_derived_signal(self) -> Optional[M1Signal]:
        """Derive a signal from the latest combined state even if no formal signal fired."""
        if self._last_combined_state is None:
            return None
        net_score = self._last_combined_state['net_score']
        strength = self._last_combined_state['strength']
        if net_score > 0:
            sig_type = SignalType.BUY
            confidence = min(60.0, strength * 60.0)
        elif net_score < 0:
            sig_type = SignalType.SELL
            confidence = min(60.0, strength * 60.0)
        else:
            sig_type = SignalType.NEUTRAL
            confidence = 0.0
        return M1Signal(
            type=sig_type,
            timestamp=self._last_combined_state['timestamp'],
            price=self._last_indicator_state['close'] if self._last_indicator_state else 0.0,
            score=int(round(net_score * 5)),
            adx=float((self._latest_adx_30m + self._latest_adx_1h + self._latest_adx_12h) / 3.0),
            confidence=confidence,
            metadata={'derived': True, 'net_score': net_score, 'strength': strength}
        )
    
    def get_last_values(self) -> Optional[M1IndicatorValues]:
        return self._last_values
    
    def get_recent_signals(self, n: int = 10) -> List[M1Signal]:
        return self._cache['signals'][-n:]
    
    def get_metrics(self) -> Dict:
        return self.metrics.copy()
    
    def get_combined_state(self) -> Optional[Dict]:
        return self._last_combined_state
    
    # Keep old name for backward compatibility
    get_fuzzy_state = get_combined_state
    
    def is_ready(self) -> bool:
        """
        Check if enough M1 data to form required 30m, 1h and 12h bars.

        STRICT definition: ALL five legs must be warm. This is the flag
        oracle_signal_reader.py v1.1 uses to gate whether a reading is
        trusted for the "fully warmed" path.
        """
        return (
            len(self._cache['data_30m']) >= max(self.params['fib_lookback_30m'], self.params['poly_lookback_30m']) and
            len(self._cache['data_1h'])  >= max(self.params['fib_lookback_1h'],  self.params['poly_lookback_1h'])  and
            len(self._cache['data_12h']) >= self.params['poly_lookback_12h']
        )
    
    def get_min_history_required(self) -> int:
        return self._min_history_required
    
    def get_m1_stats(self) -> Dict:
        return {
            'bars_processed': self.metrics['bars_processed'],
            'timeframe_validations': self.metrics['timeframe_validations'],
            'timeframe_violations': self.metrics['timeframe_violations'],
            'min_history_required_m1_bars': self._min_history_required,
            'min_history_required_time': f"{self._min_history_required} minutes",
            'bars_per_day': self.M1_BARS_PER_DAY,
            'bars_per_hour': self.M1_BARS_PER_HOUR,
            'bars_per_12h': self.M1_BARS_PER_12H,
            'last_30m_bar_time': self.metrics.get('last_30m_bar_time'),
            'last_1h_bar_time': self.metrics.get('last_1h_bar_time'),
            'last_12h_bar_time': self.metrics.get('last_12h_bar_time'),
            'warm_legs': sorted(self._last_warm_legs),
            'warm_count': self.warm_leg_count(),
            'prime_calls': self.metrics.get('prime_calls', 0),
            'prime_bars_injected': self.metrics.get('prime_bars_injected', 0),
        }
    
    def reset(self):
        self._last_values = None
        self._last_signal = None
        self._last_indicator_state = None
        self._last_combined_state = None
        self._indicators_ready = False
        self._last_timestamp = None
        self._expected_next_timestamp = None
        self._last_processed_timestamp = None
        self._last_30m_bar_computed = None
        self._last_1h_bar_computed = None
        self._last_12h_bar_computed = None
        self._current_30m_bar_start = None
        self._current_30m_bar_data = []
        self._current_1h_bar_start = None
        self._current_1h_bar_data = []
        self._current_12h_bar_start = None
        self._current_12h_bar_data = []
        self._signal_emitted_for_30m_bar = None
        self._signal_emitted_for_1h_bar = None
        self._signal_emitted_for_12h_bar = None
        self._cache = {'data': pd.DataFrame(), 'data_30m': pd.DataFrame(), 
                       'data_1h': pd.DataFrame(), 'data_12h': pd.DataFrame(), 'signals': []}
        self._latest_fib_30m = 0.5
        self._latest_fib_1h = 0.5
        self._latest_poly_30m = 0.5
        self._latest_poly_1h = 0.5
        self._latest_poly_12h = 0.5
        self._latest_adx_30m = 20.0
        self._latest_adx_1h = 20.0
        self._latest_adx_12h = 20.0
        self._last_warm_legs = set()
        self.metrics = {
            'total_calculations': 0,
            'signals_generated': 0,
            'last_update': None,
            'bars_processed': 0,
            'timeframe_validations': 0,
            'timeframe_violations': 0,
            'last_30m_bar_time': None,
            'last_1h_bar_time': None,
            'last_12h_bar_time': None,
            'prime_calls': 0,
            'prime_bars_injected': 0,
        }


# ============================================================================
# Oracle Manager (updated with signal frequency optimization)
# ============================================================================

class OracleManager:
    """
    Manager class for SuperTrendOracle_M2 instances.
    Handles multiple symbols, fetches data from MT5, and provides signals.
    OPTIMIZED: Only updates when new data is available.
    """
    
    def __init__(self, mt5_conn=None, config: Dict = None):
        self.mt5_conn = mt5_conn
        self.config = config or {}
        self.oracles: Dict[str, SuperTrendOracle_M2] = {}
        self.symbol_data_cache: Dict[str, pd.DataFrame] = {}
        self.last_update: Dict[str, float] = {}
        self.last_update_timestamp: Dict[str, pd.Timestamp] = {}  # Track last timestamp processed
        self.db_path = self.config.get('db_path', 'forex_trading.db')
        
        # Oracle parameters from config (now with new params)
        self.oracle_params = {
            'fib_lookback_30m': self.config.get('fib_lookback_30m', 30),
            'fib_lookback_1h': self.config.get('fib_lookback_1h', 20),
            'poly_lookback_30m': self.config.get('poly_lookback_30m', 60),
            'poly_lookback_1h': self.config.get('poly_lookback_1h', 40),
            'poly_lookback_12h': self.config.get('poly_lookback_12h', 20),
            'buy_threshold': self.config.get('buy_threshold', 0.4),
            'strong_buy_threshold': self.config.get('strong_buy_threshold', 0.6),
            'sell_threshold': self.config.get('sell_threshold', -0.4),
            'strong_sell_threshold': self.config.get('strong_sell_threshold', -0.6),
            'strength_threshold': self.config.get('strength_threshold', 0.3),
            'auto_resample': self.config.get('auto_resample', True),
        }
        
        self.signal_history: Dict[str, List[M1Signal]] = {}
        logger.info(f"✅ Oracle Manager initialized with M1-only configuration (30m+1h Fibonacci + 30m+1h+12h Poly)")
    
    def _get_min_history_required(self) -> int:
        return max(
            max(self.oracle_params['fib_lookback_30m'], self.oracle_params['poly_lookback_30m']) * 30,
            max(self.oracle_params['fib_lookback_1h'], self.oracle_params['poly_lookback_1h']) * 60,
            self.oracle_params['poly_lookback_12h'] * 720
        )
    
    def set_mt5_connection(self, mt5_conn):
        self.mt5_conn = mt5_conn
    
    async def fetch_m1_data(self, symbol: str, bars: int = None) -> Optional[pd.DataFrame]:
        """Fetch M1 data from MT5, only requesting new bars since last update."""
        if not self.mt5_conn:
            logger.error(f"❌ No MT5 connection for {symbol}")
            return None
        
        # Determine how many bars to fetch
        if bars is None:
            bars = self._get_min_history_required() + 60
        
        try:
            df = await self.mt5_conn.get_ohlcv(symbol, 'M1', bars)
            if df is None or df.empty:
                logger.warning(f"⚠️ No M1 data received for {symbol}")
                return None
            
            # Rename columns to standard names
            column_mapping = {
                'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume',
                'tick_volume': 'volume'
            }
            df = df.rename(columns=column_mapping)
            
            required = ['open', 'high', 'low', 'close']
            missing = [col for col in required if col not in df.columns]
            if missing:
                logger.error(f"❌ Missing required columns for {symbol}: {missing}")
                return None
            
            # Filter only new bars if we have a last update timestamp
            last_ts = self.last_update_timestamp.get(symbol)
            if last_ts is not None:
                new_df = df[df.index > last_ts]
                if new_df.empty:
                    logger.debug(f"No new M1 data for {symbol} since {last_ts}")
                    return None
                df = new_df
            
            logger.debug(f"📊 Fetched {len(df)} new M1 bars for {symbol}")
            return df
            
        except Exception as e:
            logger.error(f"❌ Error fetching M1 data for {symbol}: {e}")
            return None
    
    def get_oracle(self, symbol: str) -> SuperTrendOracle_M2:
        if symbol not in self.oracles:
            oracle_params = self.oracle_params.copy()
            oracle_params['name'] = f"Oracle_{symbol}"
            self.oracles[symbol] = SuperTrendOracle_M2(**oracle_params)
            logger.info(f"✅ Created new M1 oracle for {symbol} (30m+1h Fibonacci + 30m+1h+12h Poly)")
        return self.oracles[symbol]
    
    async def update_symbol(self, symbol: str) -> List[M1Signal]:
        """Update a symbol with new data and return generated signals."""
        df = await self.fetch_m1_data(symbol)
        if df is None or df.empty:
            return []
        
        oracle = self.get_oracle(symbol)
        signals = oracle.update(df)
        
        # Update cache and timestamps
        self.symbol_data_cache[symbol] = df
        self.last_update[symbol] = time.time()
        if not df.empty:
            self.last_update_timestamp[symbol] = df.index[-1]
        
        # Store signal history
        if symbol not in self.signal_history:
            self.signal_history[symbol] = []
        for signal in signals:
            self.signal_history[symbol].append(signal)
            self._save_signal_to_db(symbol, signal)
        
        # Keep history limited
        if len(self.signal_history[symbol]) > 100:
            self.signal_history[symbol] = self.signal_history[symbol][-100:]
        
        if signals:
            logger.info(f"📊 {symbol}: Generated {len(signals)} signals (30m+1h Fibonacci + 30m+1h+12h Poly)")
        else:
            logger.debug(f"📊 {symbol}: No new signals (processed {len(df)} bars)")
        
        return signals
    
    async def get_signal(self, symbol: str) -> Optional[M1Signal]:
        """Get the latest signal for a symbol, updating if necessary."""
        last_update = self.last_update.get(symbol, 0)
        # Update every 30 seconds if no new bars, but don't force if no new data
        if time.time() - last_update > 30:
            await self.update_symbol(symbol)
        
        oracle = self.get_oracle(symbol)
        signal = oracle.get_last_signal()
        if signal is None:
            logger.debug(f"⚠️ Oracle has no signal yet for {symbol}")
        return signal
    
    async def get_current_direction(self, symbol: str) -> str:
        signal = await self.get_signal(symbol)
        if signal:
            return SignalType.to_direction(signal.type)
        return 'neutral'
    
    async def get_current_strength(self, symbol: str) -> str:
        signal = await self.get_signal(symbol)
        if signal:
            return SignalType.get_strength(signal.type)
        return 'weak'
    
    async def get_confidence(self, symbol: str) -> float:
        signal = await self.get_signal(symbol)
        if signal:
            return signal.confidence
        return 0.0
    
    async def get_combined_metrics(self, symbol: str) -> Optional[Dict]:
        oracle = self.get_oracle(symbol)
        return oracle.get_combined_state()
    
    # Keep old name for backward compatibility
    get_fuzzy_metrics = get_combined_metrics
    
    def _save_signal_to_db(self, symbol: str, signal: M1Signal):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS m1_signal_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    symbol TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    signal_type TEXT NOT NULL,
                    price REAL NOT NULL,
                    score INTEGER NOT NULL,
                    adx REAL NOT NULL,
                    confidence REAL NOT NULL,
                    net_score REAL,
                    metadata TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )
            """)
            try:
                cursor.execute("ALTER TABLE m1_signal_history ADD COLUMN net_score REAL")
            except:
                pass
            net_score = signal.metadata.get('net_score') if isinstance(signal.metadata, dict) else None
            cursor.execute("""
                INSERT OR REPLACE INTO m1_signal_history 
                (symbol, timestamp, signal_type, price, score, adx, confidence, net_score, metadata)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                symbol,
                signal.timestamp.isoformat(),
                signal.type.value,
                signal.price,
                signal.score,
                signal.adx,
                signal.confidence,
                net_score,
                str(signal.metadata)
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"❌ Error saving signal to DB: {e}")
    
    def get_recent_signals(self, symbol: str, n: int = 10) -> List[M1Signal]:
        return self.signal_history.get(symbol, [])[-n:]
    
    def get_oracle_metrics(self, symbol: str) -> Dict:
        oracle = self.oracles.get(symbol)
        if oracle:
            return oracle.get_metrics()
        return {}
    
    def get_all_metrics(self) -> Dict[str, Dict]:
        return {sym: oracle.get_metrics() for sym, oracle in self.oracles.items()}
    
    def is_ready(self, symbol: str) -> bool:
        oracle = self.oracles.get(symbol)
        if oracle:
            return oracle.is_ready()
        return False
    
    def reset_symbol(self, symbol: str):
        if symbol in self.oracles:
            self.oracles[symbol].reset()
            self.last_update_timestamp.pop(symbol, None)
            logger.info(f"🔄 Reset M1 oracle for {symbol}")
    
    def reset_all(self):
        for symbol, oracle in self.oracles.items():
            oracle.reset()
            self.last_update_timestamp.pop(symbol, None)
        logger.info("🔄 Reset all M1 oracles")


# ============================================================================
# M1-Specific Integration Adapters (unchanged)
# ============================================================================

class M1TradingAdapter:
    """Base adapter class for M1-only trading integration."""
    
    def __init__(self, oracle: SuperTrendOracle_M2):
        self.oracle = oracle
        self._callbacks = []
        self._last_m1_bar: Optional[pd.Timestamp] = None
        
    def on_data(self, data: pd.DataFrame):
        data = M1TimeframeValidator.ensure_m1(data, auto_resample=False)
        signals = self.oracle.update(data)
        for signal in signals:
            self.on_signal(signal)
        if not data.empty:
            self._last_m1_bar = data.index[-1]
    
    def on_signal(self, signal: M1Signal):
        for callback in self._callbacks:
            callback(signal)
    
    def register_callback(self, callback):
        self._callbacks.append(callback)
    
    def unregister_callback(self, callback):
        if callback in self._callbacks:
            self._callbacks.remove(callback)
    
    def wait_for_next_m1_bar(self) -> pd.Timedelta:
        return self.oracle.get_time_remaining()


class M1LiveTradingAdapter(M1TradingAdapter):
    """Adapter for live M1 trading integration."""
    
    def __init__(self, oracle: SuperTrendOracle_M2, symbol: str, exchange=None, broker=None):
        super().__init__(oracle)
        self.symbol = symbol
        self.exchange = exchange
        self.broker = broker
        self.positions = {}
        self.m1_trades = []
    
    def on_signal(self, signal: M1Signal):
        super().on_signal(signal)
        if signal.type in [SignalType.STRONG_BUY, SignalType.BUY]:
            self.execute_m1_buy(signal)
        elif signal.type in [SignalType.STRONG_SELL, SignalType.SELL]:
            self.execute_m1_sell(signal)
    
    def execute_m1_buy(self, signal: M1Signal):
        components_info = ""
        if 'components' in signal.metadata:
            comps = signal.metadata['components']
            fib30 = comps.get('fib_30m', 0)
            fib1h = comps.get('fib_1h', 0)
            poly30 = comps.get('poly_30m', 0)
            poly1h = comps.get('poly_1h', 0)
            poly12h = comps.get('poly_12h', 0)
            components_info = f" (fib30={fib30:.2f}, fib1h={fib1h:.2f}, poly30={poly30:.2f}, poly1h={poly1h:.2f}, poly12h={poly12h:.2f})"
        trade = {
            'timestamp': signal.timestamp,
            'symbol': self.symbol,
            'type': 'BUY',
            'price': signal.price,
            'confidence': signal.confidence,
            'score': signal.score,
            'net_score': signal.metadata.get('net_score'),
            'timeframe': 'M1',
            'signal_strength': SignalType.get_strength(signal.type)
        }
        self.m1_trades.append(trade)
        logger.info(f"🟢 M1 BUY signal for {self.symbol} @ {signal.price:.2f} (conf: {signal.confidence:.1f}%{components_info})")
    
    def execute_m1_sell(self, signal: M1Signal):
        components_info = ""
        if 'components' in signal.metadata:
            comps = signal.metadata['components']
            fib30 = comps.get('fib_30m', 0)
            fib1h = comps.get('fib_1h', 0)
            poly30 = comps.get('poly_30m', 0)
            poly1h = comps.get('poly_1h', 0)
            poly12h = comps.get('poly_12h', 0)
            components_info = f" (fib30={fib30:.2f}, fib1h={fib1h:.2f}, poly30={poly30:.2f}, poly1h={poly1h:.2f}, poly12h={poly12h:.2f})"
        trade = {
            'timestamp': signal.timestamp,
            'symbol': self.symbol,
            'type': 'SELL',
            'price': signal.price,
            'confidence': signal.confidence,
            'score': signal.score,
            'net_score': signal.metadata.get('net_score'),
            'timeframe': 'M1',
            'signal_strength': SignalType.get_strength(signal.type)
        }
        self.m1_trades.append(trade)
        logger.info(f"🔴 M1 SELL signal for {self.symbol} @ {signal.price:.2f} (conf: {signal.confidence:.1f}%{components_info})")


class M1BacktestAdapter(M1TradingAdapter):
    """Adapter for M1 backtesting integration."""
    
    def __init__(self, oracle: SuperTrendOracle_M2):
        super().__init__(oracle)
        self.signals_history = []
        self.m1_trades = []
        self.m1_equity_curve = []
        self.m1_stats = {
            'total_m1_bars': 0,
            'm1_buy_signals': 0,
            'm1_sell_signals': 0,
            'avg_m1_confidence': 0,
            'avg_net_score': 0,
            'avg_bullishness': 0
        }
        self.components_history = []
    
    def on_signal(self, signal: M1Signal):
        super().on_signal(signal)
        signal_data = {
            'timestamp': signal.timestamp,
            'signal': signal.type.value,
            'price': signal.price,
            'score': signal.score,
            'adx': signal.adx,
            'confidence': signal.confidence,
            'bar_time': signal.timestamp.strftime('%H:%M'),
            'timeframe': 'M1',
            'strength': SignalType.get_strength(signal.type),
            'net_score': signal.metadata.get('net_score'),
            'components': signal.metadata.get('components')
        }
        self.signals_history.append(signal_data)
        if signal.type in [SignalType.STRONG_BUY, SignalType.BUY]:
            self.m1_stats['m1_buy_signals'] += 1
        elif signal.type in [SignalType.STRONG_SELL, SignalType.SELL]:
            self.m1_stats['m1_sell_signals'] += 1
        if 'components' in signal.metadata:
            self.components_history.append(signal.metadata['components'])
    
    def on_data(self, data: pd.DataFrame):
        super().on_data(data)
        self.m1_stats['total_m1_bars'] += len(data)
    
    def get_m1_summary(self) -> Dict:
        if self.signals_history:
            df = pd.DataFrame(self.signals_history)
            self.m1_stats['avg_m1_confidence'] = df['confidence'].mean()
            if 'net_score' in df.columns:
                self.m1_stats['avg_net_score'] = df['net_score'].mean()
            if self.components_history:
                bullish_avgs = []
                for comp in self.components_history:
                    if comp:
                        avg = np.mean(list(comp.values()))
                        bullish_avgs.append(avg)
                if bullish_avgs:
                    self.m1_stats['avg_bullishness'] = np.mean(bullish_avgs)
        return {
            **self.m1_stats,
            'm1_signal_frequency': (self.m1_stats['m1_buy_signals'] + self.m1_stats['m1_sell_signals']) / max(1, self.m1_stats['total_m1_bars']),
            'total_m1_signals': len(self.signals_history),
            'five_indicator_enabled': len(self.components_history) > 0
        }


# ============================================================================
# M1 Utility Functions (unchanged)
# ============================================================================

def create_m1_oracle(config: Dict = None) -> SuperTrendOracle_M2:
    if config is None:
        config = {}
    params = SuperTrendOracle_M2.DEFAULT_PARAMS.copy()
    params.update(config)
    return SuperTrendOracle_M2(**params)


def create_oracle_manager(mt5_conn=None, config: Dict = None) -> OracleManager:
    return OracleManager(mt5_conn, config)


def load_m1_data_from_csv(filepath: str) -> pd.DataFrame:
    df = pd.read_csv(filepath, index_col=0, parse_dates=True)
    column_mapping = {
        'Open': 'open', 'High': 'high', 'Low': 'low', 'Close': 'close',
        'Volume': 'volume', 'OPEN': 'open', 'HIGH': 'high', 'LOW': 'low', 'CLOSE': 'close'
    }
    df = df.rename(columns=column_mapping)
    df = M1TimeframeValidator.ensure_m1(df, auto_resample=True)
    return df


def get_next_m1_time(current_time: Optional[pd.Timestamp] = None) -> pd.Timestamp:
    if current_time is None:
        current_time = pd.Timestamp.now()
    total_seconds = current_time.minute * 60 + current_time.second
    next_total_seconds = ((total_seconds // 60) + 1) * 60
    if next_total_seconds >= 3600:
        next_total_seconds = 0
        next_time = current_time.replace(minute=0, second=0, microsecond=0) + pd.Timedelta(hours=1)
    else:
        next_minute = next_total_seconds // 60
        next_second = next_total_seconds % 60
        next_time = current_time.replace(minute=next_minute, second=next_second, microsecond=0)
    return next_time


def init_m1_database(db_path: str = 'forex_trading.db'):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS m1_signal_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symbol TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                price REAL NOT NULL,
                score INTEGER NOT NULL,
                adx REAL NOT NULL,
                confidence REAL NOT NULL,
                net_score REAL,
                metadata TEXT,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_m1_signal_history_symbol_timestamp 
            ON m1_signal_history(symbol, timestamp DESC)
        """)
        conn.commit()
        conn.close()
        logger.info("✅ M1 database tables initialized (30m+1h Fibonacci + 30m+1h+12h Poly oracle)")
    except Exception as e:
        logger.error(f"❌ Error initializing M1 database: {e}")


# ============================================================================
# Example Usage
# ============================================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    print("=" * 80)
    print("SUPERTREND ORACLE M1 (30m+1h Fibonacci + 30m+1h+12h Poly with fuzzy combination)")
    print("v3.3 — warm-leg renormalization + partial-warmup scaling + direct HTF priming")
    print("=" * 80)
    
    # Generate M1 data (for demo)
    m1_dates = pd.date_range(start='2024-01-01 00:00:00', periods=5000, freq='1min')
    np.random.seed(42)
    price = 50000 + np.cumsum(np.random.randn(5000) * 10)
    m1_data = pd.DataFrame({
        'open': price,
        'high': price + np.abs(np.random.randn(5000) * 20),
        'low': price - np.abs(np.random.randn(5000) * 20),
        'close': price + np.random.randn(5000) * 5,
        'volume': np.random.randint(100, 1000, 5000)
    }, index=m1_dates)
    
    print(f"Created {len(m1_data)} M1 bars")
    
    # Create oracle with new parameters
    oracle = SuperTrendOracle_M2(
        name="FiveIndicatorOracle",
        fib_lookback_30m=30,
        fib_lookback_1h=20,
        poly_lookback_30m=60,
        poly_lookback_1h=40,
        poly_lookback_12h=20,
        buy_threshold=0.4,
        strong_buy_threshold=0.6
    )
    
    # ================================================================
    # v3.3 DEMO: Direct HTF priming
    # ================================================================
    print("\n--- v3.3 Direct HTF Priming Demo ---")
    # Build the three HTF frames directly from the M1 sample
    # (in production these come from 3 separate fetch_ohlcv calls)
    df_30m = m1_data.resample('30min').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum',
    }).dropna(subset=['open', 'high', 'low', 'close'])
    df_1h = m1_data.resample('1h').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum',
    }).dropna(subset=['open', 'high', 'low', 'close'])
    df_12h = m1_data.resample('12h').agg({
        'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum',
    }).dropna(subset=['open', 'high', 'low', 'close'])

    print(f"  HTF frames: 30m={len(df_30m)} 1h={len(df_1h)} 12h={len(df_12h)}")

    report = oracle.prime_from_htf(data_30m=df_30m, data_1h=df_1h, data_12h=df_12h)
    print(f"  After priming: warm_count={report['warm_count']}/5 ({report['warm_legs']})")
    print(f"  Caches       : {report['caches']}")
    print(f"  Injected     : {report['injected']}")
    print(f"  Latest values: {report['latest']}")
    print(f"  is_ready()   : {oracle.is_ready()}")

    # Now feed a fresh M1 bar — the incremental path continues seamlessly
    combined = oracle._calculate_combined_score()
    print(f"\n  Combined score after prime: net_score={combined[0]:+.3f} strength={combined[1]:.3f}")
    print(f"  Warm legs: {combined[2].get('__warm__')}")
    print(f"  Weights  : {combined[2].get('__weights__')}")

    # ================================================================
    # Batch update test (incremental M1 path)
    # ================================================================
    print("\n--- Testing Batch Update with Optimized Signal Generation ---")
    batch_size = 100
    total_signals = 0
    for i in range(0, len(m1_data), batch_size):
        batch = m1_data.iloc[i:i+batch_size]
        signals = oracle.update(batch)
        if signals:
            total_signals += len(signals)
            print(f"Batch {i//batch_size + 1}: Generated {len(signals)} signals (total: {total_signals})")
            for s in signals[:2]:
                components = s.metadata.get('components', {})
                signal_reason = s.metadata.get('signal_reason', 'unknown')
                warm = s.metadata.get('warm_legs', [])
                print(f"  {s.timestamp}: {s.type} @ {s.price:.2f} (conf: {s.confidence:.1f}%, reason: {signal_reason}, warm={len(warm)}/5)")
    
    print(f"\n✅ Total signals generated: {total_signals} from {len(m1_data)} bars")

    # Warm-up report
    print("\n--- Final Warm-up Report ---")
    rep = oracle.get_warmup_report()
    print(f"  warm_count : {rep['warm_count']}/5")
    print(f"  warm_legs  : {rep['warm_legs']}")
    print(f"  caches     : {rep['caches']}")

    # Metrics with prime counters
    print("\n--- Oracle Metrics (v3.3) ---")
    m = oracle.get_metrics()
    print(f"  prime_calls         : {m.get('prime_calls')}")
    print(f"  prime_bars_injected : {m.get('prime_bars_injected')}")
    print(f"  bars_processed      : {m.get('bars_processed')}")
    print(f"  signals_generated   : {m.get('signals_generated')}")

    print("\n✅ v3.3 direct HTF priming active:")
    print("   • prime_from_htf() injects 30m/1h/12h frames directly")
    print("   • Full warm-up in ~1 cycle instead of ~9 days")
    print("   • Incremental M1 path (update/process_bar) unchanged")
    print("   • v3.2 warm-leg renormalization + partial-warmup scaling preserved")
    print("   • is_ready() / warm_leg_count() / get_warmup_report() unchanged")