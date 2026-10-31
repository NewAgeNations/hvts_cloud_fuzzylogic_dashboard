#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==============================================================================
HVTS SmartMoney AI — Dual-Market Dashboard (SMC D1 + Fractals D1) — v4.1
==============================================================================

v4.1 CHANGES (crypto "no signals" fix)
──────────────────────────────────────
    • FIXED: _ohlcv_list_to_df is now bulletproof — filters malformed rows,
             coerces numerics, drops NaN bars. (Diagnostic Cause A)
    • FIXED: _resolve_frames_into_dash no longer silently skips empty frames.
             Failed fetches now surface as a visible ⚠ ERROR on the row
             instead of staying at `· …` forever. (Cause D)
    • FIXED: Stale Gate.io circuit breakers are cleared once at startup so a
             bad previous run doesn't lock crypto out for 6h. (Cause E)
    • FIXED: compute_smc / apply_smc_result propagate explicit "neutral"
             state so the panel can distinguish "never fetched" from
             "fetched but no breakout". (Cause C / F)
    • IMPROVED: smc_cell / fractal_cell differentiate `· …` (no data yet)
             from `— NEUTRAL` (indicator ran, no bias).
    • IMPROVED: Per-symbol fetch logging (WARNING level) — you can see
             exactly which crypto symbol failed and why.
    • IMPROVED: Startup summary is coloured green ONLY when every symbol
             resolved; yellow otherwise.

v4.0 baseline
─────────────
    • SPLIT:     FOREX (MT5) and CRYPTO (Gate.io) into two independent feeds.
    • SPLIT:     Signals panel now shows two stacked tables.
    • ADDED:     GateState dataclass — Gate balance + connectivity.
    • ADDED:     PER-MARKET demo fallback: if MT5 is live but Gate is not
                 configured (or vice versa), only the missing market falls
                 back to synthetic data.
    • ADDED:     render_signals_panel() now takes a `symbols` list and a
                 `title` parameter.
    • UPDATED:   Header shows both MT5 and Gate connection status.
    • UPDATED:   Account panel shows both MT5 equity and Gate USDT balance.
    • UPDATED:   Telegram digest has two clearly separated sections.
    • KEPT:      SMC D1 + Fractals D1 indicators, licence system, all
                 safety guards in exchange.py and basic_smc.py.

USAGE
─────
    python hvts_dashboard.py
    python hvts_dashboard.py --demo
    python hvts_dashboard.py --license status|activate|deactivate|info
    python hvts_dashboard.py --offline        # ADMIN ONLY
==============================================================================
"""

from __future__ import annotations

import argparse
import asyncio
import html as _html
import json
import logging
import os
import sys
import time
import traceback
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# ── Load .env BEFORE reading any env var used for configuration ──────────
try:
    from dotenv import load_dotenv
    load_dotenv(override=True)
except ImportError:
    pass

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Rich
# ---------------------------------------------------------------------------
try:
    from rich.align import Align
    from rich.box import HEAVY, ROUNDED
    from rich.console import Console, Group
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.prompt import Prompt
    from rich.table import Table
    from rich.text import Text
except ImportError:
    print("❌ The 'rich' library is required.  →  pip install rich")
    sys.exit(1)

logger = logging.getLogger(__name__)


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION — symbols from .env
# ═══════════════════════════════════════════════════════════════════════════

def _parse_symbols_env(key: str, default: str) -> List[str]:
    """Parse a comma-separated symbol list from an env var."""
    raw = os.getenv(key, default)
    return [s.strip() for s in raw.split(",") if s.strip()]


def _env_float(key: str, default: float) -> float:
    raw = os.getenv(key, "").strip().replace("_", "").replace(",", "")
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(key: str, default: int) -> int:
    return int(_env_float(key, float(default)))


def _env_bool(key: str, default: bool) -> bool:
    raw = os.getenv(key, "").strip().lower()
    if not raw:
        return default
    return raw in ("1", "true", "yes", "on")


FOREX_SYMBOLS: List[str] = _parse_symbols_env(
    "FOREX_SYMBOLS",
    "EURUSD,GBPUSD,USDCAD,USDCHF,EURGBP,BTCUSD",
)
CRYPTO_SYMBOLS: List[str] = _parse_symbols_env(
    "CRYPTO_SYMBOLS",
    "BTC/USDT,ETH/USDT,SOL/USDT",
)
ALL_SYMBOLS: List[str] = FOREX_SYMBOLS + CRYPTO_SYMBOLS


MT5_ACCOUNT  = os.getenv("MT5_ACCOUNT",  "960131530")
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "@h1hZqTb")
MT5_SERVER   = os.getenv("MT5_SERVER",   "HantecMarketsMU-MT5")

REFRESH_INTERVAL       = 2.0
ACCOUNT_INTERVAL       = 10.0    # MT5 account refresh
GATE_BALANCE_INTERVAL  = 30.0    # Gate USDT balance refresh

# ── SMC D1 feed (both markets) ──────────────────────────────────────────
SMC_TIMEFRAME      = "D1"
SMC_FETCH_BARS     = 120
SMC_LOOKBACK       = 10
SMC_SWING_LOOKBACK = 3

# ── Fractals D1 feed (both markets) ─────────────────────────────────────
FRACTAL_TIMEFRAME  = "D1"
FRACTAL_FETCH_BARS = 120
FRACTAL_LEFT_BARS  = 2
FRACTAL_RIGHT_BARS = 2

# Both indicators consume the SAME D1 bars → request once per symbol
D1_FETCH_BARS = max(SMC_FETCH_BARS, FRACTAL_FETCH_BARS)

SIGNAL_FETCH_INTERVAL = 60.0

# ── Telegram ────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    "7426274394:AAHAebYveqkmi-S54yavvXy-WRXfdIG7m7s",
)
TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    "-1001897246404",
)
TELEGRAM_INTERVAL   = max(60, _env_int("TELEGRAM_INTERVAL_SECONDS", 60 * 60))   # default: hourly
TELEGRAM_PARSE_MODE = "HTML"
TELEGRAM_MAX_CHARS  = 3900      # Telegram hard limit is 4096 per message

# ── Dynamic crypto top-N (Gate.io futures, SMC D1 + Fractals D1 consensus) ──
#   CRYPTO_DYNAMIC            1 = scan every Gate futures contract above the
#                             volume threshold; 0 = legacy static CRYPTO_SYMBOLS
#   CRYPTO_MIN_VOLUME_USDT    24h futures volume floor (USDT)
#   CRYPTO_TOP_N              rows shown in the crypto window
#   CRYPTO_SCAN_INTERVAL      seconds between full universe scans
#   CRYPTO_SCAN_CONCURRENCY   parallel D1 fetches during a scan
#   SHOWCASE_RETENTION_HOURS  0 = a showcased symbol stays on the Telegram list
#                             forever; N = drop it N hours after it last
#                             appeared in the top-N window
CRYPTO_DYNAMIC           = _env_bool("CRYPTO_DYNAMIC", True)
CRYPTO_MIN_VOLUME_USDT   = _env_float("CRYPTO_MIN_VOLUME_USDT", 1_000_000.0)
CRYPTO_TOP_N             = max(1, _env_int("CRYPTO_TOP_N", 10))
CRYPTO_SCAN_INTERVAL     = max(60.0, _env_float("CRYPTO_SCAN_INTERVAL", 600.0))
CRYPTO_SCAN_CONCURRENCY  = max(1, _env_int("CRYPTO_SCAN_CONCURRENCY", 6))
CRYPTO_SCAN_BATCH        = 25
CRYPTO_FETCH_TIMEOUT     = 30.0       # hard cap per symbol so one stuck request never freezes a batch
# public  = Gate's keyless candlestick endpoint (default, immune to API-key problems)
# exchange = exchange.py's authenticated ccxt connection
CRYPTO_CANDLE_SOURCE     = os.getenv("CRYPTO_CANDLE_SOURCE", "public").strip().lower()
SHOWCASE_RETENTION_HOURS = max(0.0, _env_float("SHOWCASE_RETENTION_HOURS", 0.0))


# ═══════════════════════════════════════════════════════════════════════════
# OPTIONAL IMPORTS (MT5 / SMC / Fractals / Gate / License)
# ═══════════════════════════════════════════════════════════════════════════

# --- MT5 ---
try:
    from mt5_connection import MT5Connection
    MT5_AVAILABLE = True
    _MT5_IMPORT_ERR: Optional[str] = None
except Exception as exc:
    MT5_AVAILABLE = False
    MT5Connection = None                                    # type: ignore[assignment]
    _MT5_IMPORT_ERR = str(exc)


# --- SMC (basic_smc) — needs DB_PATH set before import ---
def _app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


SMC_DB_LIVE = Path(os.getenv("HVTS_SMC_DB", str(_app_dir() / "hvts_dashboard_smc_d1.db")))
SMC_DB_DEMO = SMC_DB_LIVE.with_name(f"{SMC_DB_LIVE.stem}_demo{SMC_DB_LIVE.suffix}")
SHOWCASE_FILE = Path(os.getenv("HVTS_SHOWCASE_FILE", str(_app_dir() / "hvts_crypto_showcase.json")))

_prev_db_path = os.environ.get("DB_PATH")
os.environ["DB_PATH"] = str(SMC_DB_LIVE)
try:
    import basic_smc as _smc
    SMC_AVAILABLE = True
    _SMC_IMPORT_ERR: Optional[str] = None
except Exception as exc:
    _smc = None                                             # type: ignore[assignment]
    SMC_AVAILABLE = False
    _SMC_IMPORT_ERR = str(exc)
finally:
    if _prev_db_path is None:
        os.environ.pop("DB_PATH", None)
    else:
        os.environ["DB_PATH"] = _prev_db_path


# --- Fractals ---
try:
    from fractals import Fractals
    FRACTALS_AVAILABLE = True
    _FRACTALS_IMPORT_ERR: Optional[str] = None
except Exception as exc:
    Fractals = None                                         # type: ignore[assignment]
    FRACTALS_AVAILABLE = False
    _FRACTALS_IMPORT_ERR = str(exc)


# --- Gate.io (exchange.py) ---
try:
    from exchange import (
        fetch_ohlcv as gate_fetch_ohlcv,
        get_balance as gate_get_balance,
        get_exchange_connection as _gate_get_conn,
    )
    GATE_AVAILABLE = True
    _GATE_IMPORT_ERR: Optional[str] = None
except Exception as exc:
    gate_fetch_ohlcv = None                                 # type: ignore[assignment]
    gate_get_balance = None                                 # type: ignore[assignment]
    _gate_get_conn = None                                   # type: ignore[assignment]
    GATE_AVAILABLE = False
    _GATE_IMPORT_ERR = str(exc)


# --- Gate.io futures volume filter (24h USDT volume universe) ---
try:
    import gate_futures_volume_filter as _gvf
    VOLUME_FILTER_AVAILABLE = True
    _VOLUME_FILTER_IMPORT_ERR: Optional[str] = None
except (Exception, SystemExit) as exc:     # the module sys.exit()s if ccxt/requests are missing
    _gvf = None                                             # type: ignore[assignment]
    VOLUME_FILTER_AVAILABLE = False
    _VOLUME_FILTER_IMPORT_ERR = str(exc)


# --- License ---
try:
    from license_manager import (
        LicenseManager,
        SECRET_KEY,
        LICENSE_FILE,
    )
    LICENSE_AVAILABLE = True
    _LICENSE_IMPORT_ERR: Optional[str] = None
except Exception as exc:
    LICENSE_AVAILABLE = False
    LicenseManager = None                                   # type: ignore[assignment]
    SECRET_KEY = b""                                        # type: ignore[assignment]
    LICENSE_FILE = "license.key"                            # type: ignore[assignment]
    _LICENSE_IMPORT_ERR = str(exc)


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class AccountState:
    """MT5 account snapshot."""
    connected:   bool  = False
    mode:        str   = "OFFLINE"
    login:       str   = ""
    server:      str   = ""
    balance:     float = 0.0
    equity:      float = 0.0
    margin:      float = 0.0
    free_margin: float = 0.0
    currency:    str   = "USD"
    error:       str   = ""


@dataclass
class GateState:
    """Gate.io Futures snapshot."""
    connected:    bool  = False
    usdt_free:    float = 0.0
    usdt_total:   float = 0.0
    last_update:  Optional[datetime] = None
    error:        str = ""


@dataclass
class LicenseState:
    valid:       bool  = False
    email:       str   = ""
    expiry:      str   = ""
    days_left:   int   = -1
    features:    List[str] = field(default_factory=list)
    machine_id:  str   = ""
    error:       str   = ""


@dataclass
class SymbolState:
    symbol:       str
    market:       str = ""                # "FOREX" | "CRYPTO"
    last_price:   float = 0.0
    last_update:  Optional[datetime] = None
    error:        str = ""


@dataclass
class SmcState:
    state:       str                = ""
    confidence:  float              = 0.0
    strength:    float              = 0.0
    method:      str                = ""
    swing_high:  Optional[float]    = None
    swing_low:   Optional[float]    = None
    updated:     Optional[datetime] = None


@dataclass
class FractalState:
    signal:      str                = ""
    state:       str                = ""
    swing_high:  Optional[float]    = None
    swing_low:   Optional[float]    = None
    updated:     Optional[datetime] = None


@dataclass
class DashboardState:
    account:              AccountState           = field(default_factory=AccountState)
    gate:                 GateState              = field(default_factory=GateState)
    license:              LicenseState           = field(default_factory=LicenseState)
    symbols:              Dict[str, SymbolState] = field(default_factory=dict)
    smc:                  Dict[str, SmcState]    = field(default_factory=dict)
    fractal:              Dict[str, FractalState]= field(default_factory=dict)
    focus_symbol:         str = (ALL_SYMBOLS[0] if ALL_SYMBOLS else "BTCUSD")
    start_time:           datetime = field(default_factory=datetime.now)
    cycle:                int = 0
    mt5_status:           str = "OFFLINE"

    telegram_last_send:   Optional[datetime] = None
    telegram_next_send:   Optional[datetime] = None
    telegram_last_status: str = ""
    telegram_sent_count:  int = 0
    telegram_fail_count:  int = 0

    # ── dynamic crypto top-N + showcase registry ───────────────────────
    crypto_dynamic:       bool = False
    crypto_volume:        Dict[str, float]    = field(default_factory=dict)   # 24h USDT vol, all contracts
    crypto_change:        Dict[str, float]    = field(default_factory=dict)   # 24h % change
    crypto_top:           List[str]           = field(default_factory=list)   # current top-N (panel order)
    crypto_dual:          Dict[str, Tuple[str, datetime]] = field(default_factory=dict)  # sym -> (state, since)
    showcase:             Dict[str, Dict[str, str]] = field(default_factory=dict)        # sym -> {first,last}
    crypto_scan_last:     Optional[datetime] = None
    crypto_scan_status:   str = "not started"
    crypto_scan_done:     int = 0
    crypto_scan_total:    int = 0
    crypto_universe_n:    int = 0
    crypto_qualified_n:   int = 0


# ═══════════════════════════════════════════════════════════════════════════
# VISUAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def fmt_money(v: float) -> str:
    return f"${v:,.2f}"


def fmt_price(p: float) -> str:
    if p >= 1000:  return f"{p:,.2f}"
    if p >= 10:    return f"{p:.4f}"
    if p >= 1:     return f"{p:.5f}"
    return f"{p:.6f}"


def smc_cell(smc: Optional[SmcState]) -> Text:
    """
    v4.1: distinguish three states:
      • SMC_AVAILABLE False       → n/a
      • smc is None               → never attempted (· …)
      • smc.state == bullish/bearish → signal
      • smc.state == neutral      → indicator ran, no bias (— NEUTRAL)
    """
    if not SMC_AVAILABLE:
        return Text("n/a", style="grey50")
    if smc is None:
        return Text("·  …", style="grey50")
    if smc.state == "bullish":
        return Text("▲ BULLISH", style="bold bright_green")
    if smc.state == "bearish":
        return Text("▼ BEARISH", style="bold bright_red")
    return Text("— NEUTRAL", style="grey50")


def fractal_cell(fr: Optional[FractalState]) -> Text:
    """
    v4.1: distinguish three states:
      • FRACTALS_AVAILABLE False  → n/a
      • fr is None                → never attempted (· …)
      • fr.state bullish/bearish  → signal
      • fr.state == ""            → indicator ran, no breakout yet (— NO BREAK)
    """
    if not FRACTALS_AVAILABLE:
        return Text("n/a", style="grey50")
    if fr is None:
        return Text("·  …", style="grey50")
    if fr.state == "bullish":
        return Text("▲ BULLISH", style="bold bright_green")
    if fr.state == "bearish":
        return Text("▼ BEARISH", style="bold bright_red")
    return Text("— NO BREAK", style="grey50")


def _age_str(ts: Optional[datetime]) -> str:
    if ts is None:
        return "—"
    delta = (datetime.now() - ts).total_seconds()
    if delta < 60:    return f"{int(delta)}s"
    if delta < 3600:  return f"{int(delta // 60)}m"
    return f"{int(delta // 3600)}h {int((delta % 3600) // 60)}m"


def _interval_label(seconds: float) -> str:
    if seconds % 3600 == 0:
        return f"{int(seconds // 3600)}h"
    if seconds % 60 == 0:
        return f"{int(seconds // 60)}m"
    return f"{int(seconds)}s"


def _until_str(ts: Optional[datetime]) -> str:
    if ts is None:
        return "—"
    delta = (ts - datetime.now()).total_seconds()
    if delta <= 0:    return "now"
    if delta < 60:    return f"{int(delta)}s"
    if delta < 3600:  return f"{int(delta // 60)}m"
    return f"{int(delta // 3600)}h {int((delta % 3600) // 60)}m"


# ═══════════════════════════════════════════════════════════════════════════
# LICENSE INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════

def _license_banner(console: Console) -> None:
    title = Text()
    title.append("⚡ ", style="bold bright_yellow")
    title.append("HVTS", style="bold bright_cyan")
    title.append(" SmartMoney ", style="bold bright_white")
    title.append("AI", style="bold bright_magenta")
    title.append("   ▸  ", style="bright_cyan")
    title.append("SMC D1 + Fractals D1   │   FOREX + CRYPTO", style="italic cyan")
    console.print()
    console.print(Panel(
        Align.center(title),
        border_style="bright_cyan",
        box=HEAVY,
        padding=(0, 2),
    ))


def load_license_state(manager: LicenseManager) -> LicenseState:
    state = LicenseState(machine_id=manager.get_machine_id())
    if not LICENSE_AVAILABLE or manager is None:
        state.error = _LICENSE_IMPORT_ERR or "license_manager unavailable"
        return state
    try:
        valid, payload = manager.validate_license()
    except Exception as exc:
        state.error = f"validate: {exc}"[:120]
        return state
    state.valid = bool(valid)
    if valid:
        state.email    = payload.get("email", "") or ""
        state.features = list(payload.get("features", []) or [])
        if "expiry" in payload:
            try:
                exp_dt = datetime.fromisoformat(payload["expiry"])
                state.expiry    = exp_dt.strftime("%Y-%m-%d")
                state.days_left = max(0, (exp_dt - datetime.utcnow()).days)
            except Exception:
                state.expiry = payload.get("expiry", "")
        else:
            state.expiry    = "Never"
            state.days_left = -1
    else:
        state.error = payload.get("error", "No valid license")
    return state


def render_activation_screen(console: Console, manager: LicenseManager) -> Optional[str]:
    info = manager.get_machine_info()
    _license_banner(console)

    info_tbl = Table(box=ROUNDED, border_style="cyan", show_header=False, padding=(0, 1))
    info_tbl.add_column("k", style="grey58", no_wrap=True)
    info_tbl.add_column("v", style="bright_white")
    info_tbl.add_row("Hostname",     info["hostname"])
    info_tbl.add_row("System",       f"{info['system']} {info['release']}")
    info_tbl.add_row("Architecture", info["machine"])
    info_tbl.add_row("Processor",    info["processor"] or "—")
    info_tbl.add_row("Machine ID",   Text(info["machine_id"], style="bold bright_yellow"))

    console.print(Panel(
        Group(
            Text.from_markup("[bold bright_red]⚠  LICENSE REQUIRED[/]"),
            Text(""),
            Text.from_markup(
                "[white]This build of [/][bold cyan]HVTS SmartMoney AI[/]"
                "[white] is not activated on this machine.[/]"
            ),
            Text(""),
            Text.from_markup(
                "[grey58]Send the Machine ID below to the vendor to receive "
                "your license key.[/]"
            ),
            Text(""),
            info_tbl,
            Text(""),
            Text.from_markup(
                "[grey58]Paste the license key below (or press [/]"
                "[bold]Enter[/][grey58] to quit).[/]"
            ),
        ),
        title="[bold bright_cyan]◈ ACTIVATION[/]",
        border_style="bright_cyan",
        box=HEAVY,
        padding=(1, 2),
    ))

    try:
        key = Prompt.ask(
            "[bold bright_cyan]License key[/]",
            default="",
            console=console,
            show_default=False,
        ).strip()
    except (KeyboardInterrupt, EOFError):
        return None

    if not key:
        return None

    ok, payload = manager.validate_license(key)
    if not ok:
        console.print(Panel(
            Text.from_markup(
                f"[bold bright_red]❌ Activation failed:[/] "
                f"[white]{payload.get('error', 'unknown')}[/]"
            ),
            border_style="bright_red", box=ROUNDED,
        ))
        return None

    if not manager.save_license(key):
        console.print(Panel(
            Text.from_markup(f"[bold bright_red]❌ Could not save {LICENSE_FILE}[/]"),
            border_style="bright_red", box=ROUNDED,
        ))
        return None

    console.print(Panel(
        Text.from_markup(
            "[bold bright_green]✅ LICENSE ACTIVATED[/]\n\n"
            f"[white]Licensed to:[/] [bright_white]{payload.get('email', '—')}[/]\n"
            f"[white]Expires:[/]     [bright_white]{payload.get('expiry', 'Never')}[/]\n"
            f"[white]Features:[/]    [bright_white]{', '.join(payload.get('features', ['all']))}[/]"
        ),
        border_style="bright_green", box=ROUNDED,
    ))
    return key


def ensure_licensed(console: Console, bypass: bool = False) -> Optional[LicenseState]:
    if bypass:
        console.log("[bold yellow]🔓 OFFLINE MODE — licence check bypassed (ADMIN ONLY)[/]")
        return LicenseState(
            valid=True, email="admin@offline", expiry="Never",
            features=["all"], machine_id="offline",
        )

    if not LICENSE_AVAILABLE:
        console.print(Panel(
            Text.from_markup(
                "[bold bright_red]❌ license_manager.py could not be imported.[/]\n\n"
                f"[white]{_LICENSE_IMPORT_ERR or 'unknown import error'}[/]"
            ),
            title="[bold bright_red]LICENSE SYSTEM ERROR[/]",
            border_style="bright_red", box=HEAVY,
        ))
        return None

    manager = LicenseManager(SECRET_KEY)
    state = load_license_state(manager)

    if state.valid:
        if 0 <= state.days_left <= 7:
            console.log(f"[bold yellow]⚠ License expires in {state.days_left} day(s)[/]")
        return state

    console.log(f"[yellow]⚠ License not valid: {state.error}[/]")
    token = render_activation_screen(console, manager)
    if token is None:
        console.print(Panel(
            Text.from_markup(
                "[bold bright_red]✖ Exiting — no valid license.[/]\n\n"
                "[grey58]Run again with a valid key, or contact support.[/]"
            ),
            border_style="bright_red", box=ROUNDED,
        ))
        return None

    return load_license_state(manager)


def render_license_panel(dash: DashboardState) -> Panel:
    lic = dash.license
    tbl = Table(box=None, show_header=False, padding=(0, 1), expand=True)
    tbl.add_column("k", style="grey58", no_wrap=True)
    tbl.add_column("v", justify="right", no_wrap=True)

    if lic.valid:
        tbl.add_row("Status", Text("● ACTIVE", style="bold bright_green"))
    else:
        tbl.add_row("Status", Text("○ INACTIVE", style="bold bright_red"))

    tbl.add_row("", "")
    tbl.add_row("Email", Text(lic.email or "—", style="bright_white"))
    if lic.valid:
        if lic.expiry == "Never":
            tbl.add_row("Expiry", Text("Permanent", style="bold bright_green"))
        elif lic.days_left >= 0:
            col = ("bright_green" if lic.days_left > 30
                   else "yellow" if lic.days_left > 7
                   else "bright_red")
            tbl.add_row("Expiry",
                        Text(f"{lic.expiry} ({lic.days_left}d)", style=f"bold {col}"))
        else:
            tbl.add_row("Expiry", Text(lic.expiry or "—", style="bright_white"))
        tbl.add_row("Features", Text(", ".join(lic.features) or "all", style="bright_cyan"))
    tbl.add_row("Machine", Text(lic.machine_id[:16] if lic.machine_id else "—", style="grey58"))

    return Panel(
        tbl,
        title="[bold bright_cyan]🔑 LICENSE[/]",
        border_style="cyan",
        box=ROUNDED,
        padding=(0, 1),
    )


# ═══════════════════════════════════════════════════════════════════════════
# SMC D1 ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def smc_select_db(path: Path) -> None:
    if not SMC_AVAILABLE:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        _smc.SMC_STATE_DB = str(path)
        _smc._init_db()
    except Exception:
        pass


def _forced_binary_bias(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """Force a binary bias when analyze_basic_smc has no stored state and no breakout."""
    try:
        high  = df["high"].to_numpy(dtype=float)
        low   = df["low"].to_numpy(dtype=float)
        close = df["close"].to_numpy(dtype=float)
        n = len(close)
        if n < 5:
            return None

        swing_highs, swing_lows = _smc.detect_swing_points(high, low, SMC_SWING_LOOKBACK)
        win = min(SMC_LOOKBACK, n - 1)
        hi = swing_highs[-1] if swing_highs else float(np.max(high[-win - 1:-1]))
        lo = swing_lows[-1]  if swing_lows  else float(np.min(low[-win - 1:-1]))

        mid = (hi + lo) / 2.0
        px  = float(close[-1])
        if px > mid:
            sig = "bullish"
        elif px < mid:
            sig = "bearish"
        else:
            slope = float(np.polyfit(np.arange(n), close, 1)[0])
            sig = "bullish" if slope >= 0 else "bearish"
        return {"signal": sig, "swing_high": float(hi), "swing_low": float(lo)}
    except Exception as exc:
        logger.warning(f"_forced_binary_bias failed: {exc}")
        return None


def compute_smc(sym: str, df: Optional[pd.DataFrame]) -> Optional[Dict[str, Any]]:
    """
    v4.1: always returns a result dict (possibly with signal='neutral')
    when SMC is available and df has enough data — so the dashboard can
    distinguish 'never fetched' from 'fetched but neutral'.
    """
    if not SMC_AVAILABLE:
        return None

    try:
        res = _smc.analyze_basic_smc(
            df if df is not None else pd.DataFrame(),
            lookback=SMC_LOOKBACK,
            swing_lookback=SMC_SWING_LOOKBACK,
            symbol=sym,
            timeframe=SMC_TIMEFRAME,
        )
    except Exception as exc:
        logger.warning(f"SMC analyze_basic_smc threw for {sym}: {exc}")
        return None

    if res.get("signal") in ("bullish", "bearish"):
        return res

    # Neutral — try forced bias from the raw candles
    forced = _forced_binary_bias(df) if df is not None and not df.empty else None
    if forced is None:
        # Genuinely can't force a bias — return the neutral result so the
        # UI can display "— NEUTRAL" instead of "· …".
        logger.debug(
            f"SMC {sym}: no forced bias "
            f"(df_len={0 if df is None else len(df)}, "
            f"signal={res.get('signal')!r})"
        )
        return res

    try:
        _smc._set_stored_state(
            symbol=sym,
            state=forced["signal"],
            confidence=50.0,
            strength=0.0,
            swing_high=forced["swing_high"],
            swing_low=forced["swing_low"],
            method_used="forced",
            lookback_used=SMC_LOOKBACK,
            timeframe=SMC_TIMEFRAME,
        )
    except Exception as exc:
        logger.warning(f"SMC _set_stored_state failed for {sym}: {exc}")

    res.update({
        "signal":      forced["signal"],
        "swing_high":  forced["swing_high"],
        "swing_low":   forced["swing_low"],
        "confidence":  50.0,
        "strength":    0.0,
        "method_used": "forced",
    })
    return res


def apply_smc_result(dash: DashboardState, sym: str, res: Optional[Dict[str, Any]]) -> None:
    """
    v4.1: accept neutral results too, so dash.smc[sym] distinguishes
    'never attempted' (None) from 'attempted, neutral' (state='neutral').
    """
    if not res:
        return
    sig = res.get("signal", "")
    if sig not in ("bullish", "bearish", "neutral", ""):
        return

    state = dash.smc.setdefault(sym, SmcState())
    state.state      = sig or "neutral"
    state.confidence = float(res.get("confidence") or 0.0)
    state.strength   = float(res.get("strength") or 0.0)
    state.method     = str(res.get("method_used") or "")
    state.swing_high = res.get("swing_high")
    state.swing_low  = res.get("swing_low")
    state.updated    = datetime.now()


# ═══════════════════════════════════════════════════════════════════════════
# FRACTALS D1 ENGINE
# ═══════════════════════════════════════════════════════════════════════════

def compute_fractals(sym: str, df: Optional[pd.DataFrame]) -> Optional[FractalState]:
    if not FRACTALS_AVAILABLE:
        return None
    if df is None or len(df) < (FRACTAL_LEFT_BARS + FRACTAL_RIGHT_BARS + 2):
        return None
    try:
        f = Fractals(
            high=df["high"], low=df["low"],
            left_bars=FRACTAL_LEFT_BARS, right_bars=FRACTAL_RIGHT_BARS,
        )
        latest = f.get_latest_signal(close=df["close"])
        up_levels, down_levels = f.get_fractal_levels()
        up_clean   = up_levels.dropna()
        down_clean = down_levels.dropna()
        return FractalState(
            signal=latest or "",
            state={"buy": "bullish", "sell": "bearish"}.get(latest or "", ""),
            swing_high=float(up_clean.iloc[-1])   if len(up_clean)   else None,
            swing_low=float(down_clean.iloc[-1]) if len(down_clean) else None,
            updated=datetime.now(),
        )
    except Exception as exc:
        logger.warning(f"Fractals compute error for {sym}: {exc}")
        return None


# ═══════════════════════════════════════════════════════════════════════════
# OHLCV NORMALISATION
# ═══════════════════════════════════════════════════════════════════════════

def _normalize_ohlcv(df: Optional[pd.DataFrame]) -> pd.DataFrame:
    """Normalise an MT5-style DataFrame (columns may be mixed-case)."""
    if df is None or df.empty:
        return pd.DataFrame()
    df = df.copy()
    df.columns = [str(c).lower() for c in df.columns]
    if "tick_volume" in df.columns and "volume" not in df.columns:
        df = df.rename(columns={"tick_volume": "volume"})
    if not isinstance(df.index, pd.DatetimeIndex):
        df.index = pd.to_datetime(df.index)
    df = df.sort_index()
    df = df[~df.index.duplicated(keep="last")]
    return df


def _ohlcv_list_to_df(ohlcv: Optional[List]) -> pd.DataFrame:
    """
    v4.1: bulletproof conversion from a CCXT OHLCV list to a normalised
    DataFrame. Handles:
      • ohlcv is None / [] / list of empty rows
      • rows with wrong shape (fewer than 6 fields)
      • non-numeric values
      • NaT timestamps
    Always returns either a clean DataFrame or an empty one — never raises.
    """
    if not ohlcv:
        return pd.DataFrame()
    try:
        rows = [r for r in ohlcv if r and len(r) >= 6]
        if not rows:
            logger.warning(f"_ohlcv_list_to_df: no usable rows out of {len(ohlcv)}")
            return pd.DataFrame()
        df = pd.DataFrame(rows, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms", errors="coerce")
        df = df.dropna(subset=["timestamp"]).set_index("timestamp").sort_index()
        for c in ("open", "high", "low", "close", "volume"):
            df[c] = pd.to_numeric(df[c], errors="coerce")
        df = df.dropna(subset=["high", "low", "close"])
        if df.empty:
            logger.warning(f"_ohlcv_list_to_df: all rows dropped after numeric coercion")
            return pd.DataFrame()
        return _normalize_ohlcv(df)
    except Exception as exc:
        logger.warning(f"_ohlcv_list_to_df failed: {exc}")
        return pd.DataFrame()


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

def _tg_escape(s: str) -> str:
    return _html.escape(str(s), quote=False)


def _fmt_smc_compact(smc: Optional[SmcState]) -> str:
    if smc is None or smc.state not in ("bullish", "bearish"):
        return "— SMC"
    arrow = "▲" if smc.state == "bullish" else "▼"
    return f"{arrow} SMC D1 {smc.confidence:.0f}%"


def _fmt_fractal_compact(fr: Optional[FractalState]) -> str:
    if fr is None or fr.state not in ("bullish", "bearish"):
        return "— FRACTALS"
    arrow = "▲" if fr.state == "bullish" else "▼"
    return f"{arrow} FRACTALS D1"


def _symbol_agreement_emoji(smc: Optional[SmcState], fr: Optional[FractalState]) -> str:
    smc_b = smc is not None and smc.state == "bullish"
    smc_s = smc is not None and smc.state == "bearish"
    fr_b  = fr  is not None and fr.state  == "bullish"
    fr_s  = fr  is not None and fr.state  == "bearish"
    if smc_b and fr_b: return "🟢"
    if smc_s and fr_s: return "🔴"
    if smc_b and fr_s: return "🟡"
    if smc_s and fr_b: return "🟡"
    if smc_b or smc_s or fr_b or fr_s: return "⚪"
    return "⚫"


def _format_market_lines(dash: DashboardState, symbols: List[str]) -> List[str]:
    lines: List[str] = []
    if not symbols:
        lines.append("  <i>(no symbols configured)</i>")
        return lines
    for sym in symbols:
        smc    = dash.smc.get(sym)
        fr     = dash.fractal.get(sym)
        emoji  = _symbol_agreement_emoji(smc, fr)
        smc_p  = _fmt_smc_compact(smc)
        fr_p   = _fmt_fractal_compact(fr)
        lines.append(
            f"{emoji} <b>{_tg_escape(sym)}</b>   "
            f"{_tg_escape(smc_p)}   |   {_tg_escape(fr_p)}"
        )
    return lines


def _fmt_vol(v: float) -> str:
    if v >= 1e9: return f"${v / 1e9:.2f}B"
    if v >= 1e6: return f"${v / 1e6:.1f}M"
    if v >= 1e3: return f"${v / 1e3:.0f}K"
    return f"${v:.0f}"


def _crypto_line(dash: DashboardState, sym: str, tag: str = "") -> str:
    smc   = dash.smc.get(sym)
    fr    = dash.fractal.get(sym)
    vol   = dash.crypto_volume.get(sym)
    emoji = _symbol_agreement_emoji(smc, fr)
    if vol is None:
        vol_txt = "vol n/a"
    elif vol < CRYPTO_MIN_VOLUME_USDT:
        vol_txt = f"{_fmt_vol(vol)} ⚠below min"
    else:
        vol_txt = _fmt_vol(vol)
    line = (
        f"{emoji} <b>{_tg_escape(sym)}</b>   "
        f"{_tg_escape(_fmt_smc_compact(smc))}   |   {_tg_escape(_fmt_fractal_compact(fr))}   "
        f"· {_tg_escape(vol_txt)}"
    )
    if tag:
        line += f"  {tag}"
    return line


def _format_crypto_dynamic_lines(dash: DashboardState) -> List[str]:
    """Live top-N first, then EVERY other showcased symbol (mixed signals included)."""
    lines: List[str] = [
        f"<i>Futures vol ≥ {_tg_escape(_fmt_vol(CRYPTO_MIN_VOLUME_USDT))} · "
        f"universe {dash.crypto_universe_n} · qualifying {dash.crypto_qualified_n}</i>",
        "",
        f"🔥 <b>LIVE TOP {CRYPTO_TOP_N} — dual consensus now</b>",
    ]
    if dash.crypto_top:
        for sym in dash.crypto_top:
            lines.append(_crypto_line(dash, sym))
    else:
        lines.append("  <i>(no symbol currently has dual SMC + Fractals consensus)</i>")

    others = [s for s in dash.showcase if s not in dash.crypto_top]
    others.sort(key=lambda s: dash.showcase[s].get("last", ""), reverse=True)
    lines.extend(["", f"📌 <b>SHOWCASED EARLIER — still tracked ({len(others)})</b>"])
    if others:
        for sym in others:
            lines.append(_crypto_line(dash, sym))
    else:
        lines.append("  <i>(none yet)</i>")
    return lines


def split_telegram_text(text: str, limit: int = TELEGRAM_MAX_CHARS) -> List[str]:
    """Split on line boundaries so no HTML tag is ever cut in half."""
    chunks: List[str] = []
    cur: List[str] = []
    size = 0
    for line in text.split("\n"):
        add = len(line) + 1
        if cur and size + add > limit:
            chunks.append("\n".join(cur))
            cur, size = [], 0
        cur.append(line)
        size += add
    if cur:
        chunks.append("\n".join(cur))
    return chunks


def build_telegram_message(dash: DashboardState) -> str:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = [
        "⚡ <b>HVTS SmartMoney AI</b>",
        f"📅 {_tg_escape(now_str)}",
        "",
        "<b>📊 FOREX — MT5 · SMC D1 + Fractals D1</b>",
        "─────────────────────────────",
    ]
    lines.extend(_format_market_lines(dash, FOREX_SYMBOLS))
    lines.extend([
        "",
        "<b>📊 CRYPTO — GATE.IO FUTURES · SMC D1 + Fractals D1</b>",
        "─────────────────────────────",
    ])
    if dash.crypto_dynamic:
        lines.extend(_format_crypto_dynamic_lines(dash))
    else:
        lines.extend(_format_market_lines(dash, CRYPTO_SYMBOLS))
    lines.extend([
        "─────────────────────────────",
        "SMC: Smart Money Concept (BOS binary state) on D1",
        "FRACTALS: Williams 2-2 breakout on D1",
        "",
        "🟢 both bullish   🔴 both bearish",
        "🟡 conflict   ⚪ single signal   ⚫ no signal",
    ])
    return "\n".join(lines)


async def _send_telegram_message(text: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = urllib.parse.urlencode({
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": TELEGRAM_PARSE_MODE,
        "disable_web_page_preview": "true",
    }).encode("utf-8")

    def _do() -> bool:
        try:
            req = urllib.request.Request(url, data=payload)
            with urllib.request.urlopen(req, timeout=20) as resp:
                if resp.status == 200:
                    return True
                body = resp.read().decode("utf-8", errors="ignore")[:200]
                logger.warning(f"Telegram HTTP {resp.status}: {body}")
                return False
        except Exception as exc:
            logger.warning(f"Telegram send failed: {exc}")
            return False

    return await asyncio.to_thread(_do)


async def telegram_loop(dash: DashboardState, stop_event: asyncio.Event) -> None:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning(
            "Telegram not configured — set TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID"
        )
        dash.telegram_last_status = "not configured"
        return

    # Dynamic crypto: give the first universe scan up to 5 min so the first
    # digest is not empty.
    if dash.crypto_dynamic:
        waited = 0
        while dash.crypto_scan_last is None and waited < 300 and not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
            waited += 5

    last_send = 0.0
    while not stop_event.is_set():
        now = time.time()
        if now - last_send >= TELEGRAM_INTERVAL:
            try:
                msg = build_telegram_message(dash)
                ok = True
                for chunk in split_telegram_text(msg):
                    if not await _send_telegram_message(chunk):
                        ok = False
                        break
                    await asyncio.sleep(0.4)          # stay under Telegram flood limits
                if ok:
                    last_send = now
                    dash.telegram_last_send   = datetime.now()
                    dash.telegram_next_send   = datetime.now() + timedelta(seconds=TELEGRAM_INTERVAL)
                    dash.telegram_last_status = "ok"
                    dash.telegram_sent_count += 1
                    logger.info("📨 Telegram signal update sent")
                else:
                    dash.telegram_last_status = "failed"
                    dash.telegram_fail_count += 1
                    logger.warning("Telegram send failed; will retry next cycle")
            except Exception as exc:
                dash.telegram_last_status = f"error: {str(exc)[:60]}"
                dash.telegram_fail_count += 1
                logger.warning(f"Telegram loop error: {exc}")

        try:
            await asyncio.wait_for(stop_event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            pass


# ═══════════════════════════════════════════════════════════════════════════
# DYNAMIC CRYPTO TOP-N  (Gate.io futures · SMC D1 + Fractals D1 consensus)
# ═══════════════════════════════════════════════════════════════════════════
#
# A symbol QUALIFIES for the crypto window when:
#     24h futures volume >= CRYPTO_MIN_VOLUME_USDT
#   AND ( SMC D1 bullish AND Fractals D1 bullish
#      OR SMC D1 bearish AND Fractals D1 bearish )
#
# The window shows the freshest CRYPTO_TOP_N qualifying symbols (newest dual
# consensus first, volume as tie-break). Every symbol that has ever been shown
# in that window is added to the SHOWCASE registry (persisted to disk) and
# keeps being scanned + reported in the hourly Telegram digest — even after
# its signals turn mixed or it drops out of the window.

def _dual_state(smc: Optional["SmcState"], fr: Optional["FractalState"]) -> str:
    """'bullish' / 'bearish' when SMC D1 and Fractals D1 agree, else ''."""
    if smc is None or fr is None:
        return ""
    if smc.state == "bullish" and fr.state == "bullish":
        return "bullish"
    if smc.state == "bearish" and fr.state == "bearish":
        return "bearish"
    return ""


def load_showcase(dash: DashboardState) -> None:
    """Restore the showcase registry and dual-since timestamps from disk."""
    try:
        if not SHOWCASE_FILE.exists():
            return
        data = json.loads(SHOWCASE_FILE.read_text(encoding="utf-8"))
        for sym, ent in (data.get("showcase") or {}).items():
            if isinstance(ent, dict) and ent.get("first"):
                dash.showcase[sym] = {
                    "first": str(ent["first"]),
                    "last":  str(ent.get("last") or ent["first"]),
                }
        for sym, ent in (data.get("dual") or {}).items():
            try:
                dash.crypto_dual[sym] = (str(ent[0]), datetime.fromisoformat(ent[1]))
            except Exception:
                continue
        prune_showcase(dash)
        logger.info(f"Showcase registry restored: {len(dash.showcase)} symbol(s)")
    except Exception as exc:
        logger.warning(f"Could not load showcase file {SHOWCASE_FILE}: {exc}")


def save_showcase(dash: DashboardState) -> None:
    """Atomic write so a crash mid-save never corrupts the registry."""
    try:
        payload = {
            "showcase": dash.showcase,
            "dual": {s: [st, ts.isoformat(timespec="seconds")]
                     for s, (st, ts) in dash.crypto_dual.items()
                     if s in dash.showcase or s in dash.crypto_top},
        }
        SHOWCASE_FILE.parent.mkdir(parents=True, exist_ok=True)
        tmp = SHOWCASE_FILE.with_suffix(SHOWCASE_FILE.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        os.replace(tmp, SHOWCASE_FILE)
    except Exception as exc:
        logger.warning(f"Could not save showcase file {SHOWCASE_FILE}: {exc}")


def prune_showcase(dash: DashboardState) -> None:
    """Drop symbols not seen in the top-N for SHOWCASE_RETENTION_HOURS (0 = keep forever)."""
    if SHOWCASE_RETENTION_HOURS <= 0:
        return
    cutoff = datetime.now() - timedelta(hours=SHOWCASE_RETENTION_HOURS)
    for sym in list(dash.showcase):
        if sym in dash.crypto_top:
            continue
        try:
            last = datetime.fromisoformat(dash.showcase[sym]["last"])
        except Exception:
            continue
        if last < cutoff:
            dash.showcase.pop(sym, None)


def update_crypto_top(
    dash: DashboardState,
    stamp: Optional[datetime] = None,
    register: bool = True,
) -> None:
    """
    Re-rank the top-N from current signals/volumes.

    stamp:    time used for "consensus started" — a scan passes ONE timestamp for
              every symbol so scan order never masquerades as freshness
              (ties then fall back to higher volume).
    register: add the current top-N to the showcase registry. Only done once a
              scan has finished, so half-scanned intermediate rankings are
              displayed but never recorded.
    """
    now = stamp or datetime.now()
    tracked = set(dash.crypto_volume) | set(dash.showcase)

    qualified: List[str] = []
    for sym in tracked:
        state = _dual_state(dash.smc.get(sym), dash.fractal.get(sym))
        prev  = dash.crypto_dual.get(sym)
        if state:
            if prev is None or prev[0] != state:
                dash.crypto_dual[sym] = (state, now)       # consensus (re)started now
            if dash.crypto_volume.get(sym, 0.0) >= CRYPTO_MIN_VOLUME_USDT:
                qualified.append(sym)
        else:
            dash.crypto_dual.pop(sym, None)

    # freshest consensus first; higher volume wins ties
    qualified.sort(
        key=lambda s: (dash.crypto_dual[s][1], dash.crypto_volume.get(s, 0.0)),
        reverse=True,
    )
    dash.crypto_top = qualified[:CRYPTO_TOP_N]
    dash.crypto_qualified_n = len(qualified)

    if register:
        seen = datetime.now().isoformat(timespec="seconds")
        for sym in dash.crypto_top:
            ent = dash.showcase.get(sym)
            if ent is None:
                dash.showcase[sym] = {"first": seen, "last": seen}
            else:
                ent["last"] = seen
        prune_showcase(dash)


async def scan_crypto_universe(dash: DashboardState, stop_event: asyncio.Event) -> None:
    """
    One full pass:
      1. pull 24h volumes for every Gate.io USDT futures contract
      2. universe = contracts >= CRYPTO_MIN_VOLUME_USDT  (+ every showcased symbol)
      3. fetch D1 bars, compute SMC D1 + Fractals D1, re-rank after each batch
    """
    t0 = time.time()
    scan_stamp = datetime.now()
    dash.crypto_scan_status = "fetching volumes…"

    try:
        tickers = await asyncio.to_thread(_gvf.fetch_tickers_alternative, 0.0, True)
    except Exception as exc:
        tickers = []
        logger.warning(f"[CRYPTO] ticker fetch raised: {exc}")

    if not tickers:
        dash.crypto_scan_status = "volume fetch failed — keeping previous universe"
        dash.crypto_scan_last = dash.crypto_scan_last or datetime.now()
        return

    dash.crypto_volume = {t["symbol"]: float(t["volume_usdt"]) for t in tickers}
    dash.crypto_change = {t["symbol"]: float(t.get("change_24h") or 0.0) for t in tickers}

    universe = [t["symbol"] for t in tickers if t["volume_usdt"] >= CRYPTO_MIN_VOLUME_USDT]
    dash.crypto_universe_n = len(universe)
    to_scan = list(dict.fromkeys(universe + list(dash.showcase)))      # keep volume order
    dash.crypto_scan_total = len(to_scan)
    dash.crypto_scan_done  = 0

    sem = asyncio.Semaphore(CRYPTO_SCAN_CONCURRENCY)

    async def _bars(sym: str) -> Tuple[str, pd.DataFrame]:
        async with sem:
            try:
                if CRYPTO_CANDLE_SOURCE == "exchange":
                    coro = gate_fetch_ohlcv(sym, FRACTAL_TIMEFRAME, D1_FETCH_BARS)
                else:
                    coro = asyncio.to_thread(
                        _gvf.fetch_candles_public, sym, "1d", D1_FETCH_BARS
                    )
                ohlcv = await asyncio.wait_for(coro, timeout=CRYPTO_FETCH_TIMEOUT)
                return sym, _ohlcv_list_to_df(ohlcv)
            except Exception as exc:                # includes asyncio.TimeoutError
                logger.debug(f"[CRYPTO] {sym}: candle fetch failed: {exc!r}")
                return sym, pd.DataFrame()

    for i in range(0, len(to_scan), CRYPTO_SCAN_BATCH):
        if stop_event.is_set():
            return
        batch = to_scan[i:i + CRYPTO_SCAN_BATCH]
        dash.crypto_scan_status = f"scanning {i}/{len(to_scan)} done"
        frames = await asyncio.gather(*[_bars(sym) for sym in batch])
        await asyncio.to_thread(_resolve_frames_into_dash, list(frames), dash, "CRYPTO")
        dash.crypto_scan_done = min(i + len(batch), len(to_scan))
        update_crypto_top(dash, stamp=scan_stamp, register=False)   # panel fills progressively

    update_crypto_top(dash, stamp=scan_stamp, register=True)        # scan complete → record showcase
    save_showcase(dash)
    dash.crypto_scan_last = datetime.now()
    dash.crypto_scan_status = (
        f"ok · {len(to_scan)} scanned in {time.time() - t0:.0f}s · "
        f"{dash.crypto_qualified_n} qualifying"
    )
    logger.info(f"[CRYPTO] scan complete: {dash.crypto_scan_status}")


async def crypto_scan_loop(dash: DashboardState, stop_event: asyncio.Event) -> None:
    while not stop_event.is_set():
        try:
            await scan_crypto_universe(dash, stop_event)
        except Exception as exc:
            dash.crypto_scan_status = f"error: {str(exc)[:60]}"
            dash.crypto_scan_last = dash.crypto_scan_last or datetime.now()
            logger.warning(f"[CRYPTO] scan error: {exc}")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=CRYPTO_SCAN_INTERVAL)
        except asyncio.TimeoutError:
            pass


# ═══════════════════════════════════════════════════════════════════════════
# DEMO DATA GENERATOR (synthetic D1 for both markets)
# ═══════════════════════════════════════════════════════════════════════════

BASE_PRICES = {
    # Forex (MT5 CFD prices)
    "BTCUSD": 67250.0, "ETHUSD": 3540.0, "XAUUSD": 2360.0,
    "CHFJPY": 171.20,  "EURCAD": 1.4740, "EURCHF": 0.9830,
    "EURGBP": 0.8520,  "EURJPY": 165.40, "EURUSD": 1.0835,
    "GBPUSD": 1.2715,  "USDCAD": 1.3610, "USDCHF": 0.8965,
    "USDJPY": 150.30,  "USOIL+": 78.45,
    # Crypto (Gate.io spot/futures)
    "BTC/USDT": 67250.0, "ETH/USDT": 3540.0, "SOL/USDT": 145.0,
    "BNB/USDT": 590.0,   "XRP/USDT": 0.52,   "ADA/USDT": 0.44,
}


def gen_demo_d1(symbol: str) -> pd.DataFrame:
    """Synthesize a realistic D1 OHLC series for the given symbol."""
    bars = D1_FETCH_BARS
    vol = 0.014
    rng = np.random.default_rng((abs(hash(symbol)) + ord('D')) % (2**32))
    base = BASE_PRICES.get(symbol, 100.0)
    close = np.exp(np.cumsum(rng.normal(0.0, vol, bars)))
    close = close * (base / close[-1])
    open_ = np.roll(close, 1); open_[0] = close[0]
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, vol / 3, bars)))
    low  = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, vol / 3, bars)))
    idx  = pd.date_range(end=pd.Timestamp.now().floor("1D"), periods=bars, freq="1D")
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close,
         "volume": rng.integers(500, 20000, bars)},
        index=idx,
    )


class DemoDriver:
    """Synthetic D1 feed shared by both indicators across both markets."""

    def __init__(self):
        self.prices: Dict[str, float] = {}
        self.d1_bars: Dict[str, pd.DataFrame] = {}
        self.rng = np.random.default_rng(42)

    def seed(self, symbol: str) -> pd.DataFrame:
        d1 = gen_demo_d1(symbol)
        self.d1_bars[symbol] = d1
        self.prices[symbol] = float(d1["close"].iloc[-1])
        return d1

    def next_tick(self, symbol: str) -> float:
        p = self.prices.get(symbol, BASE_PRICES.get(symbol, 100.0))
        p = max(0.01, p + float(self.rng.normal(0, p * 0.0006)))
        self.prices[symbol] = p
        return p


# ═══════════════════════════════════════════════════════════════════════════
# SIGNAL UPDATE — FOREX (MT5)
# ═══════════════════════════════════════════════════════════════════════════

def _resolve_frames_into_dash(
    frames: List[Tuple[str, pd.DataFrame]],
    dash: DashboardState,
    market: str,
    set_price: bool = True,
) -> Tuple[int, int]:
    """
    Compute SMC + Fractals for a batch of (symbol, df) frames and write results
    into `dash`. If `set_price` is True, also writes last_price / last_update.

    v4.1: Empty frames no longer silently skip the symbol. They now set a
    visible error on the SymbolState (only if the symbol has no prior good
    data) so the dashboard shows ⚠ ERROR instead of staying at `· …`.
    """
    n_smc = 0
    n_fr  = 0
    for sym, df in frames:
        try:
            if df is None or df.empty:
                st = dash.symbols.get(sym)
                if st is None:
                    st = SymbolState(symbol=sym, market=market)
                    dash.symbols[sym] = st
                # Only flag as error if we've never had good data for this symbol
                if st.last_price == 0.0 and not st.error:
                    st.error = "no data from exchange"
                    st.last_update = datetime.now()
                    logger.warning(
                        f"[{market}] {sym}: fetch returned empty — "
                        f"symbol will show ⚠ ERROR (Cause A/D/E in diagnostics)"
                    )
                continue

            # SMC
            if SMC_AVAILABLE:
                try:
                    res = compute_smc(sym, df)
                    if res is not None:
                        apply_smc_result(dash, sym, res)
                        if res.get("signal") in ("bullish", "bearish"):
                            n_smc += 1
                except Exception as exc:
                    logger.warning(f"SMC error for {sym} ({market}): {exc}")

            # Fractals
            if FRACTALS_AVAILABLE:
                try:
                    fr = compute_fractals(sym, df)
                    if fr is not None:
                        dash.fractal[sym] = fr
                        if fr.state in ("bullish", "bearish"):
                            n_fr += 1
                except Exception as exc:
                    logger.warning(f"Fractals error for {sym} ({market}): {exc}")

            # Price / age
            if set_price:
                st = dash.symbols.setdefault(sym, SymbolState(symbol=sym, market=market))
                try:
                    st.last_price  = float(df["close"].iloc[-1])
                    st.last_update = datetime.now()
                    st.error       = ""
                except Exception:
                    pass
        except Exception as exc:
            logger.warning(f"Frame resolve error for {sym} ({market}): {exc}")
    return n_smc, n_fr


async def update_forex_signals_live(
    conn: MT5Connection, dash: DashboardState
) -> Tuple[int, int]:
    """Fetch D1 per forex symbol from MT5 and resolve both indicators."""
    if not FOREX_SYMBOLS:
        return 0, 0
    if not (SMC_AVAILABLE or FRACTALS_AVAILABLE):
        return 0, 0

    async def _bars(sym: str):
        try:
            df = await conn.get_ohlcv(sym, FRACTAL_TIMEFRAME, D1_FETCH_BARS)
            return sym, _normalize_ohlcv(df)
        except Exception as exc:
            logger.debug(f"MT5 get_ohlcv error for {sym}: {exc}")
            return sym, pd.DataFrame()

    frames = await asyncio.gather(*[_bars(s) for s in FOREX_SYMBOLS])
    return await asyncio.to_thread(_resolve_frames_into_dash, frames, dash, "FOREX")


async def update_forex_signals_demo(
    driver: DemoDriver, dash: DashboardState
) -> Tuple[int, int]:
    if not FOREX_SYMBOLS:
        return 0, 0
    frames = [(sym, driver.d1_bars.get(sym, pd.DataFrame())) for sym in FOREX_SYMBOLS]
    return await asyncio.to_thread(_resolve_frames_into_dash, frames, dash, "FOREX")


# ═══════════════════════════════════════════════════════════════════════════
# SIGNAL UPDATE — CRYPTO (Gate.io)
# ═══════════════════════════════════════════════════════════════════════════

async def update_crypto_signals_live(dash: DashboardState) -> Tuple[int, int]:
    """
    Fetch D1 per crypto symbol from Gate.io and resolve both indicators.

    v4.1: per-symbol fetch failures are logged at WARNING level and reflected
    in dash.symbols[sym].error, so the panel shows ⚠ ERROR instead of `· …`.
    """
    if not CRYPTO_SYMBOLS:
        return 0, 0
    if not GATE_AVAILABLE:
        return 0, 0
    if not (SMC_AVAILABLE or FRACTALS_AVAILABLE):
        return 0, 0

    async def _bars(sym: str) -> Tuple[str, pd.DataFrame]:
        try:
            ohlcv = await gate_fetch_ohlcv(sym, FRACTAL_TIMEFRAME, D1_FETCH_BARS)
            df = _ohlcv_list_to_df(ohlcv)
            if df.empty:
                # Pre-populate error state so resolve() sees it
                st = dash.symbols.get(sym)
                if st is None:
                    st = SymbolState(symbol=sym, market="CRYPTO")
                    dash.symbols[sym] = st
                if st.last_price == 0.0:
                    n_bars = len(ohlcv) if ohlcv else 0
                    st.error = f"empty ohlcv ({n_bars} rows)"[:40]
                    st.last_update = datetime.now()
                logger.warning(
                    f"[CRYPTO] {sym}: gate_fetch_ohlcv returned "
                    f"{len(ohlcv) if ohlcv else 0} rows → df empty "
                    f"(Cause A: conversion failure OR Cause E: circuit open)"
                )
            return sym, df
        except Exception as exc:
            st = dash.symbols.get(sym)
            if st is None:
                st = SymbolState(symbol=sym, market="CRYPTO")
                dash.symbols[sym] = st
            if st.last_price == 0.0:
                st.error = str(exc)[:40]
                st.last_update = datetime.now()
            logger.warning(f"[CRYPTO] {sym}: fetch_ohlcv raised: {exc}")
            return sym, pd.DataFrame()

    frames = await asyncio.gather(*[_bars(s) for s in CRYPTO_SYMBOLS])
    return await asyncio.to_thread(_resolve_frames_into_dash, frames, dash, "CRYPTO")


async def update_crypto_signals_demo(
    driver: DemoDriver, dash: DashboardState
) -> Tuple[int, int]:
    if not CRYPTO_SYMBOLS:
        return 0, 0
    frames = [(sym, driver.d1_bars.get(sym, pd.DataFrame())) for sym in CRYPTO_SYMBOLS]
    return await asyncio.to_thread(_resolve_frames_into_dash, frames, dash, "CRYPTO")


# ═══════════════════════════════════════════════════════════════════════════
# ACCOUNT / GATE / DEMO UPDATES
# ═══════════════════════════════════════════════════════════════════════════

async def update_account(conn: MT5Connection, dash: DashboardState) -> None:
    try:
        bal = await conn.get_balance()
        dash.account.balance     = float(bal.get("balance", 0.0))
        dash.account.equity      = float(bal.get("equity", 0.0))
        dash.account.margin      = float(bal.get("margin", 0.0))
        dash.account.free_margin = float(bal.get("free_margin", 0.0))
        dash.account.currency    = str(bal.get("currency", "USD"))
        dash.account.connected   = True
        dash.account.mode        = "LIVE"
        dash.account.error       = ""

        info = await conn.get_account_info()
        if info is not None:
            dash.account.login  = str(getattr(info, "login", MT5_ACCOUNT))
            dash.account.server = str(getattr(info, "server", MT5_SERVER))
        else:
            dash.account.login  = MT5_ACCOUNT
            dash.account.server = MT5_SERVER
    except Exception as exc:
        dash.account.error = str(exc)[:80]
        dash.account.connected = False


async def update_gate_balance(dash: DashboardState) -> None:
    """Fetch Gate.io USDT balance (uses exchange.py's 30s cache)."""
    if not GATE_AVAILABLE or gate_get_balance is None:
        dash.gate.connected = False
        dash.gate.error     = "exchange.py not importable"
        dash.gate.last_update = datetime.now()
        return
    try:
        bal = await gate_get_balance(use_cache=True)
        if bal:
            dash.gate.usdt_free  = float(bal.get("USDT", 0.0))
            dash.gate.usdt_total = float(bal.get("total", 0.0))
            dash.gate.connected  = True
            dash.gate.error      = ""
        else:
            dash.gate.error = "no balance (check API keys)"
            dash.gate.connected = False
        dash.gate.last_update = datetime.now()
    except Exception as exc:
        dash.gate.error = str(exc)[:80]
        dash.gate.connected = False
        dash.gate.last_update = datetime.now()


async def update_demo_prices(
    driver: DemoDriver, dash: DashboardState, symbols: List[str]
) -> None:
    """Advance the synthetic D1 feed for the specified symbols only."""
    if not symbols:
        return
    now = datetime.now()
    for sym in symbols:
        st = dash.symbols.setdefault(sym, SymbolState(symbol=sym))
        px = driver.next_tick(sym)
        st.last_price  = px
        st.last_update = now
        st.error       = ""

        df = driver.d1_bars.get(sym)
        if df is not None and not df.empty:
            try:
                df.iloc[-1, df.columns.get_loc("close")] = px
                df.iloc[-1, df.columns.get_loc("high")]  = max(float(df["high"].iloc[-1]), px)
                df.iloc[-1, df.columns.get_loc("low")]   = min(float(df["low"].iloc[-1]),  px)
            except Exception:
                pass


def _seed_demo_account(dash: DashboardState) -> None:
    dash.account.connected   = True
    dash.account.mode        = "DEMO"
    dash.account.login       = MT5_ACCOUNT
    dash.account.server      = MT5_SERVER
    dash.account.balance     = 50_000.0
    dash.account.equity      = dash.account.balance
    dash.account.margin      = 320.0
    dash.account.free_margin = dash.account.equity - dash.account.margin


def bootstrap_demo(dash: DashboardState, console: Console) -> DemoDriver:
    driver = DemoDriver()
    console.log(
        f"[yellow]⚠ Demo driver seeded for {len(ALL_SYMBOLS)} symbols "
        f"(forex: {len(FOREX_SYMBOLS)}, crypto: {len(CRYPTO_SYMBOLS)})[/]"
    )
    for sym in ALL_SYMBOLS:
        df = driver.seed(sym)
        market = "FOREX" if sym in FOREX_SYMBOLS else "CRYPTO"
        st = SymbolState(symbol=sym, market=market)
        st.last_price  = float(df["close"].iloc[-1])
        st.last_update = datetime.now()
        dash.symbols[sym] = st
    _seed_demo_account(dash)
    return driver


# ═══════════════════════════════════════════════════════════════════════════
# RENDER
# ═══════════════════════════════════════════════════════════════════════════

def render_header(dash: DashboardState) -> Panel:
    title = Text()
    title.append("⚡ ", style="bold bright_yellow")
    title.append("HVTS", style="bold bright_cyan")
    title.append(" SmartMoney ", style="bold bright_white")
    title.append("AI", style="bold bright_magenta")
    title.append("   ▸  ", style="bright_cyan")
    title.append("SMC D1 + Fractals D1   │   FOREX + CRYPTO", style="italic cyan")

    sub = Text()
    if dash.account.connected:
        sub.append("MT5:", style="grey58")
        sub.append(
            "● LIVE" if dash.account.mode == "LIVE" else "● DEMO",
            style=("bold bright_green" if dash.account.mode == "LIVE"
                   else "bold bright_yellow"),
        )
    else:
        sub.append("MT5:", style="grey58")
        sub.append("○ DOWN", style="bold bright_red")
    sub.append("   │   ", style="grey50")

    sub.append("Gate:", style="grey58")
    if not GATE_AVAILABLE:
        sub.append("○ N/A", style="bold grey50")
    elif dash.gate.error:
        sub.append("✕ ERR", style="bold bright_red")
    elif dash.gate.connected:
        sub.append("● LIVE", style="bold bright_green")
    else:
        sub.append("● PENDING", style="bold bright_yellow")
    sub.append("   │   ", style="grey50")

    if dash.crypto_dynamic:
        sub.append(f"F:{len(FOREX_SYMBOLS)} C:{dash.crypto_universe_n}", style="grey58")
    else:
        sub.append(f"F:{len(FOREX_SYMBOLS)} C:{len(CRYPTO_SYMBOLS)}", style="grey58")
    sub.append(f"   │   Cycle {dash.cycle}", style="grey58")

    if dash.license.valid:
        sub.append("   │   ", style="grey50")
        sub.append("🔑 ", style="bright_green")
        sub.append(dash.license.email or "licensed", style="bright_green")

    return Panel(
        Align.center(Group(Align.center(title), Align.center(sub))),
        border_style="bright_cyan",
        box=HEAVY,
        padding=(0, 1),
    )


def render_signals_panel(
    dash: DashboardState,
    symbols: List[str],
    title: str,
) -> Panel:
    """Render a signals table for a subset of symbols."""
    tbl = Table(
        box=ROUNDED,
        border_style="cyan",
        header_style="bold bright_cyan on grey11",
        show_edge=True,
        padding=(0, 1),
        expand=True,
    )
    tbl.add_column("SYMBOL",      style="bold bright_white", no_wrap=True)
    tbl.add_column("SMC D1",      justify="center", no_wrap=True)
    tbl.add_column("FRACTALS D1", justify="center", no_wrap=True)
    tbl.add_column("PRICE",       justify="right",  no_wrap=True)
    tbl.add_column("UPDATED",     justify="right",  no_wrap=True)

    if not symbols:
        tbl.add_row(
            Text("—", style="grey50"),
            Text("—", style="grey50"),
            Text("—", style="grey50"),
            Text("no symbols configured", style="italic grey50"),
            Text("—", style="grey50"),
        )
    else:
        for sym in symbols:
            st = dash.symbols.get(sym)
            if st is None:
                tbl.add_row(
                    Text(sym, style="bold bright_white"),
                    Text("· …", style="grey50"),
                    Text("· …", style="grey50"),
                    Text("—", style="grey58"),
                    Text("—", style="grey58"),
                )
                continue
            if st.error:
                tbl.add_row(
                    Text(sym, style="bold bright_white"),
                    Text("⚠ ERROR", style="bold bright_red"),
                    Text("⚠ ERROR", style="bold bright_red"),
                    Text("—", style="grey58"),
                    Text(st.error[:24], style="italic red"),
                )
                continue

            price_txt = fmt_price(st.last_price) if st.last_price else "—"
            tbl.add_row(
                Text(sym, style="bold bright_white"),
                smc_cell(dash.smc.get(sym)),
                fractal_cell(dash.fractal.get(sym)),
                Text(price_txt, style="bright_white"),
                Text(_age_str(st.last_update), style="grey58"),
            )

    return Panel(
        tbl,
        title=title,
        border_style="cyan",
        box=ROUNDED,
        padding=(0, 0),
    )


def render_forex_signals_panel(dash: DashboardState) -> Panel:
    return render_signals_panel(
        dash, FOREX_SYMBOLS,
        "[bold bright_cyan]◈ FOREX SIGNALS — MT5 · SMC D1 + Fractals D1[/]",
    )


def render_crypto_top_panel(dash: DashboardState) -> Panel:
    """Dynamic window: freshest CRYPTO_TOP_N dual-consensus symbols above the volume floor."""
    tbl = Table(
        box=ROUNDED, border_style="cyan",
        header_style="bold bright_cyan on grey11",
        show_edge=True, padding=(0, 1), expand=True,
    )
    tbl.add_column("#",           justify="right", style="grey58", no_wrap=True)
    tbl.add_column("SYMBOL",      style="bold bright_white", no_wrap=True)
    tbl.add_column("CONSENSUS",   justify="center", no_wrap=True)
    tbl.add_column("SMC D1",      justify="center", no_wrap=True)
    tbl.add_column("FRACTALS D1", justify="center", no_wrap=True)
    tbl.add_column("24H VOL",     justify="right",  no_wrap=True)
    tbl.add_column("24H %",       justify="right",  no_wrap=True)
    tbl.add_column("PRICE",       justify="right",  no_wrap=True)
    tbl.add_column("SINCE",       justify="right",  no_wrap=True)

    if not dash.crypto_top:
        if dash.crypto_scan_last is None:
            msg = f"scanning Gate.io futures… {dash.crypto_scan_status}"
        else:
            msg = "no symbol currently has dual SMC D1 + Fractals D1 consensus above the volume floor"
        tbl.add_row("", Text(msg, style="italic grey50"), "", "", "", "", "", "", "")
    else:
        for i, sym in enumerate(dash.crypto_top, 1):
            state, since = dash.crypto_dual.get(sym, ("", None))
            st  = dash.symbols.get(sym)
            chg = dash.crypto_change.get(sym, 0.0)
            tbl.add_row(
                str(i),
                sym,
                (Text("▲▲ BULL", style="bold bright_green") if state == "bullish"
                 else Text("▼▼ BEAR", style="bold bright_red")),
                smc_cell(dash.smc.get(sym)),
                fractal_cell(dash.fractal.get(sym)),
                Text(_fmt_vol(dash.crypto_volume.get(sym, 0.0)), style="bright_white"),
                Text(f"{chg:+.1f}%", style="bright_green" if chg >= 0 else "bright_red"),
                Text(fmt_price(st.last_price) if st and st.last_price else "—", style="bright_white"),
                Text(_age_str(since), style="grey58"),
            )

    scan_age = _age_str(dash.crypto_scan_last) + " ago" if dash.crypto_scan_last else "—"
    subtitle = (
        f"[grey58]vol ≥ {_fmt_vol(CRYPTO_MIN_VOLUME_USDT)} · universe {dash.crypto_universe_n} · "
        f"qualifying {dash.crypto_qualified_n} · showcased {len(dash.showcase)} · "
        f"last scan {scan_age} · {dash.crypto_scan_status}[/]"
    )
    return Panel(
        tbl,
        title=f"[bold bright_cyan]◈ CRYPTO TOP {CRYPTO_TOP_N} — GATE.IO FUTURES · SMC D1 + Fractals D1 CONSENSUS[/]",
        subtitle=subtitle,
        border_style="cyan", box=ROUNDED, padding=(0, 0),
    )


def render_crypto_signals_panel(dash: DashboardState) -> Panel:
    if dash.crypto_dynamic:
        return render_crypto_top_panel(dash)
    return render_signals_panel(
        dash, CRYPTO_SYMBOLS,
        "[bold bright_cyan]◈ CRYPTO SIGNALS — GATE.IO · SMC D1 + Fractals D1[/]",
    )


def render_account_panel(dash: DashboardState) -> Panel:
    acc = dash.account
    gate = dash.gate
    tbl = Table(box=None, show_header=False, padding=(0, 1), expand=True)
    tbl.add_column("k", style="grey58", no_wrap=True)
    tbl.add_column("v", justify="right", no_wrap=True)

    if acc.connected:
        tbl.add_row("MT5",
                    Text("● LIVE" if acc.mode == "LIVE" else "● DEMO",
                         style=("bold bright_green" if acc.mode == "LIVE"
                                else "bold bright_yellow")))
    else:
        tbl.add_row("MT5", Text(f"○ {acc.mode}", style="bold bright_yellow"))
    tbl.add_row("Login",  Text(acc.login  or "—", style="bright_white"))
    tbl.add_row("Server", Text(acc.server or "—", style="grey58"))
    tbl.add_row("", "")
    tbl.add_row("Balance", Text(fmt_money(acc.balance),     style="bold bright_white"))
    tbl.add_row("Equity",  Text(fmt_money(acc.equity),      style="bold bright_white"))
    tbl.add_row("Margin",  Text(fmt_money(acc.margin),      style="bright_yellow"))
    tbl.add_row("Free",    Text(fmt_money(acc.free_margin), style="bright_cyan"))

    tbl.add_row("", "")
    if not GATE_AVAILABLE:
        tbl.add_row("Gate",
                    Text("○ N/A (exchange.py missing)", style="bold grey50"))
    elif gate.error:
        tbl.add_row("Gate", Text(f"✕ {gate.error[:28]}", style="bold bright_red"))
    elif gate.connected:
        tbl.add_row("Gate", Text("● LIVE", style="bold bright_green"))
        tbl.add_row("USDT", Text(fmt_money(gate.usdt_free),
                                 style="bold bright_white"))
    else:
        tbl.add_row("Gate", Text("● PENDING", style="bold bright_yellow"))

    tbl.add_row("", "")
    up = datetime.now() - dash.start_time
    h, rem = divmod(int(up.total_seconds()), 3600)
    m, s = divmod(rem, 60)
    tbl.add_row("Uptime", Text(f"{h:02d}:{m:02d}:{s:02d}", style="grey58"))

    return Panel(
        tbl,
        title="[bold bright_cyan]💼 ACCOUNTS[/]",
        border_style="cyan",
        box=ROUNDED,
        padding=(0, 1),
    )


def _smc_detail_lines(smc: Optional[SmcState]) -> Text:
    t = Text()
    t.append("  ")
    t.append_text(smc_cell(smc))
    if smc and smc.state in ("bullish", "bearish"):
        t.append("   Conf ", style="grey58")
        t.append(f"{smc.confidence:.0f}%", style="bold bright_white")
        t.append("   Method ", style="grey58")
        t.append(smc.method or "—", style="bright_white")
        t.append("\n  ", style="")
        if smc.swing_high is not None:
            t.append("Swing H ", style="grey58")
            t.append(fmt_price(smc.swing_high), style="bright_white")
        if smc.swing_low is not None:
            t.append("   Swing L ", style="grey58")
            t.append(fmt_price(smc.swing_low), style="bright_white")
    return t


def _fractal_detail_lines(fr: Optional[FractalState]) -> Text:
    t = Text()
    t.append("  ")
    t.append_text(fractal_cell(fr))
    if fr and fr.state in ("bullish", "bearish"):
        t.append("\n  ", style="")
        if fr.swing_high is not None:
            t.append("Fractal H ", style="grey58")
            t.append(fmt_price(fr.swing_high), style="bright_white")
        if fr.swing_low is not None:
            t.append("   Fractal L ", style="grey58")
            t.append(fmt_price(fr.swing_low), style="bright_white")
    return t


def render_detail_panel(dash: DashboardState) -> Panel:
    sym = dash.focus_symbol
    st  = dash.symbols.get(sym)
    if st is None:
        return Panel(
            Align.center(Text("— focus unavailable —", style="grey58")),
            title="[bold bright_cyan]◈ FOCUS[/]",
            border_style="cyan", box=ROUNDED,
        )

    smc = dash.smc.get(sym)
    fr  = dash.fractal.get(sym)
    market_tag = f"[{st.market}]" if st.market else ""

    body = Group(
        Text.from_markup(
            f"[bold bright_cyan]FOCUS[/] · [bold bright_white]{sym}[/] "
            f"[grey58]{market_tag}[/]"
            f"  ·  last [bright_white]{fmt_price(st.last_price) if st.last_price else '—'}[/]"
        ),
        Text(""),
        Text.from_markup("[grey58]SMC D1[/]"),
        _smc_detail_lines(smc),
        Text(""),
        Text.from_markup("[grey58]FRACTALS D1[/]"),
        _fractal_detail_lines(fr),
    )
    return Panel(body, border_style="bright_cyan", box=ROUNDED, padding=(0, 1))


def render_status_panel(dash: DashboardState) -> Panel:
    tbl = Table(box=None, show_header=False, padding=(0, 1), expand=True)
    tbl.add_column("k", style="grey58", no_wrap=True)
    tbl.add_column("v", style="bright_white", no_wrap=False)

    configured = bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
    tbl.add_row(
        "Telegram",
        Text("● CONFIGURED", style="bold bright_green") if configured
        else Text("○ NOT CONFIGURED", style="bold bright_yellow"),
    )
    tbl.add_row("Interval", Text(_interval_label(TELEGRAM_INTERVAL), style="bright_white"))
    if configured:
        tbl.add_row(
            "Last send",
            Text(
                f"{dash.telegram_last_send.strftime('%Y-%m-%d %H:%M:%S')}  ({_age_str(dash.telegram_last_send)} ago)"
                if dash.telegram_last_send else "pending initial send…",
                style="bright_white",
            ),
        )
        tbl.add_row(
            "Next send",
            Text(
                f"{dash.telegram_next_send.strftime('%Y-%m-%d %H:%M:%S')}  (in {_until_str(dash.telegram_next_send)})"
                if dash.telegram_next_send else "—",
                style="bright_white",
            ),
        )
        tbl.add_row(
            "Delivery",
            Text(f"{dash.telegram_sent_count} sent / {dash.telegram_fail_count} failed",
                 style="bright_white"),
        )
        if dash.telegram_last_status:
            col = "bright_green" if dash.telegram_last_status == "ok" else "bright_red"
            tbl.add_row("Last status", Text(dash.telegram_last_status, style=col))

    if dash.crypto_dynamic:
        tbl.add_row(
            "Crypto scan",
            Text(
                f"{dash.crypto_scan_status}  │  showcased {len(dash.showcase)}  │  "
                f"top-{CRYPTO_TOP_N} {len(dash.crypto_top)}",
                style="bright_white",
            ),
        )

    return Panel(
        tbl,
        title="[bold bright_cyan]◈ SYSTEM STATUS[/]",
        border_style="cyan", box=ROUNDED, padding=(0, 1),
    )


def build_dashboard(dash: DashboardState) -> Layout:
    root = Layout(name="root")

    root.split_column(
        Layout(name="header", size=4),
        Layout(name="middle", ratio=3),
        Layout(name="detail", size=12),
        Layout(name="status", size=9),
    )

    root["middle"].split_row(
        Layout(name="signals", ratio=3),
        Layout(name="account", ratio=1),
    )
    root["signals"].split_column(
        Layout(render_forex_signals_panel(dash),  name="forex"),
        Layout(render_crypto_signals_panel(dash), name="crypto"),
    )

    account_stack = Layout()
    account_stack.split_column(
        Layout(render_account_panel(dash), name="acct"),
        Layout(render_license_panel(dash), name="lic", size=11),
    )
    root["account"].update(account_stack)

    root["header"].update(render_header(dash))
    root["detail"].update(render_detail_panel(dash))
    root["status"].update(render_status_panel(dash))
    return root


def pick_focus(dash: DashboardState) -> str:
    """Prefer SMC+Fractals agreement, then highest SMC confidence, then current focus."""
    best_sym: Optional[str] = None
    best_score = -1.0
    fallback_sym: Optional[str] = None
    fallback_score = -1.0

    candidates = FOREX_SYMBOLS + (dash.crypto_top if dash.crypto_dynamic else CRYPTO_SYMBOLS)
    for sym in candidates:
        smc = dash.smc.get(sym)
        fr  = dash.fractal.get(sym)
        smc_b = smc is not None and smc.state in ("bullish", "bearish")
        fr_b  = fr  is not None and fr.state  in ("bullish", "bearish")
        score = (smc.confidence / 100.0) if smc_b else 0.0

        if smc_b and fr_b and smc.state == fr.state:
            if score > best_score:
                best_score = score
                best_sym = sym
        elif smc_b and score > fallback_score:
            fallback_score = score
            fallback_sym = sym

    return best_sym or fallback_sym or dash.focus_symbol


# ═══════════════════════════════════════════════════════════════════════════
# LICENSE CLI SUBCOMMAND
# ═══════════════════════════════════════════════════════════════════════════

def run_license_cli(action: str, console: Console) -> int:
    if not LICENSE_AVAILABLE:
        console.print(Panel(
            Text.from_markup(
                "[bold bright_red]❌ license_manager.py not importable.[/]\n"
                f"[white]{_LICENSE_IMPORT_ERR}[/]"
            ),
            border_style="bright_red", box=ROUNDED,
        ))
        return 1

    manager = LicenseManager(SECRET_KEY)

    if action == "info":
        info = manager.get_machine_info()
        tbl = Table(box=ROUNDED, border_style="cyan", show_header=False, padding=(0, 1))
        tbl.add_column("k", style="grey58", no_wrap=True)
        tbl.add_column("v", style="bright_white")
        tbl.add_row("Hostname",     info["hostname"])
        tbl.add_row("System",       f"{info['system']} {info['release']}")
        tbl.add_row("Architecture", info["machine"])
        tbl.add_row("Processor",    info["processor"] or "—")
        tbl.add_row("Machine ID",   Text(info["machine_id"], style="bold bright_yellow"))
        console.print(Panel(
            Group(
                Text.from_markup("[bold bright_cyan]MACHINE INFORMATION[/]"),
                Text(""),
                tbl,
                Text(""),
                Text.from_markup(
                    "[grey58]Send the Machine ID to the vendor to obtain a license key.[/]"
                ),
            ),
            border_style="bright_cyan", box=HEAVY, padding=(1, 2),
        ))
        return 0

    if action == "status":
        state = load_license_state(manager)
        if state.valid:
            console.print(Panel(
                Text.from_markup(
                    "[bold bright_green]✅ LICENSE ACTIVE[/]\n\n"
                    f"[white]Licensed to:[/] [bright_white]{state.email}[/]\n"
                    f"[white]Expires:[/]     [bright_white]"
                    f"{'Permanent' if state.expiry == 'Never' else state.expiry}"
                    f"{f' ({state.days_left} days left)' if state.days_left >= 0 else ''}[/]\n"
                    f"[white]Features:[/]    [bright_white]{', '.join(state.features) or 'all'}[/]\n"
                    f"[white]Machine:[/]     [grey58]{state.machine_id}[/]"
                ),
                title="[bold bright_cyan]◈ LICENSE STATUS[/]",
                border_style="bright_green", box=ROUNDED, padding=(1, 2),
            ))
        else:
            console.print(Panel(
                Text.from_markup(
                    f"[bold bright_red]❌ LICENSE INACTIVE[/]\n\n"
                    f"[white]Reason:[/] [bright_red]{state.error}[/]\n\n"
                    "[grey58]Run:  python hvts_dashboard.py --license activate[/]"
                ),
                title="[bold bright_cyan]◈ LICENSE STATUS[/]",
                border_style="bright_red", box=ROUNDED, padding=(1, 2),
            ))
        return 0

    if action == "activate":
        token = render_activation_screen(console, manager)
        return 0 if token else 1

    if action == "deactivate":
        try:
            confirm = Prompt.ask(
                "[bold bright_yellow]Remove license.key? (y/N)[/]",
                default="N", console=console,
            ).strip().lower()
        except (KeyboardInterrupt, EOFError):
            return 1
        if confirm == "y":
            if manager.remove_license():
                console.print("[bright_green]✅ License deactivated.[/]")
                return 0
            console.print("[bright_red]❌ Failed to deactivate license.[/]")
            return 1
        console.print("[grey58]Cancelled.[/]")
        return 0

    console.print(f"[red]Unknown license action: {action}[/]")
    return 1


# ═══════════════════════════════════════════════════════════════════════════
# CIRCUIT BREAKER RESET (v4.1 — fixes Cause E)
# ═══════════════════════════════════════════════════════════════════════════

def _clear_gate_circuit_breakers(console: Console) -> None:
    """
    v4.1: Clear stale Gate.io circuit breakers at dashboard startup.

    exchange.py's GateConnection._circuit_breakers is an in-memory dict that
    trips after 3 consecutive failures and locks a symbol out for 6 HOURS.
    If a previous session had bad API keys or network glitches, the crypto
    section can stay at `· …` for hours even after the underlying issue is
    fixed. Clearing here gives the dashboard a clean slate on every start.

    Safe because:
      • We only touch the breaker dict — no exchange calls are made here.
      • Trading engines run in a separate process (or their own circuit
        breakers would be unaffected by this dashboard's reset).
    """
    if not GATE_AVAILABLE or _gate_get_conn is None:
        return
    try:
        conn = _gate_get_conn()
        breakers = getattr(conn, "_circuit_breakers", None)
        if isinstance(breakers, dict) and breakers:
            count = len(breakers)
            breakers.clear()
            console.log(
                f"[yellow]⚠ Cleared {count} stale Gate circuit breaker(s) "
                f"at startup — crypto fetches may retry immediately[/]"
            )
    except Exception as exc:
        logger.debug(f"Circuit-breaker reset skipped: {exc}")


# ═══════════════════════════════════════════════════════════════════════════
# MAIN DASHBOARD LOOP
# ═══════════════════════════════════════════════════════════════════════════

async def run_dashboard(force_demo: bool = False, offline: bool = False) -> None:
    console = Console()

    # ── 1. License gate ────────────────────────────────────────────────
    lic_state = ensure_licensed(console, bypass=offline)
    if lic_state is None:
        return

    dash = DashboardState()
    dash.license = lic_state

    # Dynamic crypto top-N needs live Gate.io + the volume filter module.
    dynamic = bool(
        CRYPTO_DYNAMIC and GATE_AVAILABLE and VOLUME_FILTER_AVAILABLE and not force_demo
    )
    if CRYPTO_DYNAMIC and not dynamic:
        why = (
            "demo mode" if force_demo
            else f"exchange.py unavailable ({_GATE_IMPORT_ERR})" if not GATE_AVAILABLE
            else f"gate_futures_volume_filter unavailable ({_VOLUME_FILTER_IMPORT_ERR})"
        )
        console.log(f"[yellow]⚠ Dynamic crypto top-{CRYPTO_TOP_N} disabled — {why}; "
                    f"using static CRYPTO_SYMBOLS[/]")
    dash.crypto_dynamic = dynamic
    if dynamic:
        load_showcase(dash)

    # ── 2. Pre-create SymbolState entries so the first render shows the roster
    for sym in FOREX_SYMBOLS:
        dash.symbols[sym] = SymbolState(symbol=sym, market="FOREX")
    for sym in CRYPTO_SYMBOLS:
        dash.symbols[sym] = SymbolState(symbol=sym, market="CRYPTO")

    # ── 2.5. v4.1: clear stale Gate circuit breakers BEFORE any crypto fetch
    if not force_demo and GATE_AVAILABLE and (CRYPTO_SYMBOLS or dynamic):
        _clear_gate_circuit_breakers(console)

    # ── 3. Try MT5 (forex) ────────────────────────────────────────────
    driver: Optional[DemoDriver] = None
    conn: Optional[MT5Connection] = None
    live_forex = False

    if not force_demo and MT5_AVAILABLE and FOREX_SYMBOLS:
        try:
            conn = MT5Connection()
            ok = await conn.initialize(
                account=MT5_ACCOUNT, password=MT5_PASSWORD, server=MT5_SERVER,
            )
            if ok:
                console.log(f"[bright_green]✅ MT5 connected — {MT5_ACCOUNT}@{MT5_SERVER}[/]")
                dash.mt5_status = "LIVE"
                await update_account(conn, dash)
                live_forex = True
            else:
                console.log("[yellow]⚠ MT5 initialize() returned False — forex will use DEMO[/]")
                dash.mt5_status = "MT5 init failed"
        except Exception as exc:
            console.log(f"[yellow]⚠ MT5 setup failed: {exc}[/]")
            dash.mt5_status = f"MT5 error: {exc}"[:40]
    elif not MT5_AVAILABLE:
        console.log(f"[yellow]⚠ MT5 module unavailable: {_MT5_IMPORT_ERR}[/]")
        dash.mt5_status = "MT5 module missing"
    elif not FOREX_SYMBOLS:
        console.log("[grey58]ℹ No FOREX_SYMBOLS configured — forex section disabled[/]")

    # ── 4. Gate (crypto) availability ─────────────────────────────────
    live_crypto = GATE_AVAILABLE and not force_demo and (bool(CRYPTO_SYMBOLS) or dynamic)
    if not GATE_AVAILABLE:
        console.log(f"[yellow]⚠ exchange.py unavailable: {_GATE_IMPORT_ERR}[/]")
    elif not CRYPTO_SYMBOLS:
        console.log("[grey58]ℹ No CRYPTO_SYMBOLS configured — crypto section disabled[/]")

    # ── 5. Demo driver (fallback only) ────────────────────────────────
    if (not live_forex and FOREX_SYMBOLS) or (not live_crypto and CRYPTO_SYMBOLS):
        driver = bootstrap_demo(dash, console)

    # ── 6. Pick SMC DB path (live or demo) ────────────────────────────
    if live_forex or live_crypto:
        smc_select_db(SMC_DB_LIVE)
    else:
        smc_select_db(SMC_DB_DEMO)

    # ── 7. Initial signal pass — run forex and crypto in parallel ─────
    forex_task = (
        update_forex_signals_live(conn, dash) if live_forex
        else update_forex_signals_demo(driver, dash) if (driver and FOREX_SYMBOLS)
        else asyncio.sleep(0, result=(0, 0))
    )
    crypto_task = (
        asyncio.sleep(0, result=(0, 0)) if dynamic          # handled by crypto_scan_loop
        else update_crypto_signals_live(dash) if live_crypto
        else update_crypto_signals_demo(driver, dash) if (driver and CRYPTO_SYMBOLS)
        else asyncio.sleep(0, result=(0, 0))
    )
    (n_smc_f, n_fr_f), (n_smc_c, n_fr_c) = await asyncio.gather(forex_task, crypto_task)

    # ── 7.5. v4.1: colored startup summary (green only when ALL resolve)
    forex_ok  = (n_smc_f == len(FOREX_SYMBOLS))  if FOREX_SYMBOLS  else True
    crypto_ok = True if dynamic else ((n_smc_c == len(CRYPTO_SYMBOLS)) if CRYPTO_SYMBOLS else True)
    summary_color = "bright_green" if (forex_ok and crypto_ok) else "yellow"
    console.log(
        f"[{summary_color}]✅ Signals resolved — "
        f"FOREX SMC {n_smc_f}/{len(FOREX_SYMBOLS)} Fractals {n_fr_f}/{len(FOREX_SYMBOLS)} "
        f"({'LIVE' if live_forex else 'DEMO'})   │   "
        + (
            f"CRYPTO dynamic top-{CRYPTO_TOP_N} (vol ≥ ${CRYPTO_MIN_VOLUME_USDT:,.0f}) — scanning in background[/]"
            if dynamic else
            f"CRYPTO SMC {n_smc_c}/{len(CRYPTO_SYMBOLS)} Fractals {n_fr_c}/{len(CRYPTO_SYMBOLS)} "
            f"({'LIVE' if live_crypto else 'DEMO'})[/]"
        )
    )

    # v4.1: explicitly warn if any crypto symbol failed on the first pass
    if live_crypto and not dynamic and n_smc_c < len(CRYPTO_SYMBOLS):
        failed = [
            s for s in CRYPTO_SYMBOLS
            if (dash.smc.get(s) is None
                or dash.smc.get(s).state not in ("bullish", "bearish"))
        ]
        if failed:
            console.log(
                f"[bold bright_red]⚠ CRYPTO SMC unresolved for: {', '.join(failed)}[/]"
            )
            console.log(
                "[grey58]  Check: GATE_API_KEY / GATE_SECRET in .env, "
                "futures-read permission, network access to api.gateio.ws. "
                "See diag_crypto.py in the diagnostic notes.[/]"
            )

    # ── 8. Initial Gate balance ───────────────────────────────────────
    if GATE_AVAILABLE and (CRYPTO_SYMBOLS or dynamic):
        await update_gate_balance(dash)

    dash.focus_symbol = pick_focus(dash)

    # ── 9. Telegram notifier ──────────────────────────────────────────
    stop_event = asyncio.Event()
    scan_task: Optional[asyncio.Task] = None
    if dynamic:
        console.log(
            f"[bright_cyan]🔎 Crypto universe scanner started — vol ≥ ${CRYPTO_MIN_VOLUME_USDT:,.0f}, "
            f"top {CRYPTO_TOP_N}, rescan every {_interval_label(CRYPTO_SCAN_INTERVAL)}[/]"
        )
        scan_task = asyncio.create_task(crypto_scan_loop(dash, stop_event))

    telegram_task: Optional[asyncio.Task] = None
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        console.log(
            f"[bright_cyan]📨 Telegram notifier active → chat {TELEGRAM_CHAT_ID} every "
            f"{_interval_label(TELEGRAM_INTERVAL)} (first message once the first crypto scan finishes)[/]"
        )
        telegram_task = asyncio.create_task(telegram_loop(dash, stop_event))
    else:
        console.log("[yellow]⚠ Telegram not configured — skipping notifier[/]")

    # ── 10. Live loop ─────────────────────────────────────────────────
    last_account = 0.0
    last_gate    = 0.0
    last_signals = time.time()

    try:
        with Live(
            build_dashboard(dash),
            console=console,
            refresh_per_second=2,
            screen=True,
            transient=False,
        ) as live:
            while True:
                try:
                    now = time.time()

                    if live_forex and (now - last_account >= ACCOUNT_INTERVAL):
                        await update_account(conn, dash)
                        last_account = now

                    if GATE_AVAILABLE and (CRYPTO_SYMBOLS or dynamic) and (now - last_gate >= GATE_BALANCE_INTERVAL):
                        await update_gate_balance(dash)
                        last_gate = now

                    if driver is not None:
                        if not live_forex and FOREX_SYMBOLS:
                            await update_demo_prices(driver, dash, FOREX_SYMBOLS)
                        if not live_crypto and CRYPTO_SYMBOLS:
                            await update_demo_prices(driver, dash, CRYPTO_SYMBOLS)

                    if now - last_signals >= SIGNAL_FETCH_INTERVAL:
                        forex_task = (
                            update_forex_signals_live(conn, dash) if live_forex
                            else update_forex_signals_demo(driver, dash) if (driver and FOREX_SYMBOLS)
                            else asyncio.sleep(0, result=(0, 0))
                        )
                        crypto_task = (
                            asyncio.sleep(0, result=(0, 0)) if dynamic
                            else update_crypto_signals_live(dash) if live_crypto
                            else update_crypto_signals_demo(driver, dash) if (driver and CRYPTO_SYMBOLS)
                            else asyncio.sleep(0, result=(0, 0))
                        )
                        await asyncio.gather(forex_task, crypto_task)
                        last_signals = now

                    dash.cycle += 1
                    dash.focus_symbol = pick_focus(dash)
                    live.update(build_dashboard(dash))
                    await asyncio.sleep(REFRESH_INTERVAL)

                except KeyboardInterrupt:
                    break
                except Exception as exc:
                    console.log(f"[red]render loop error: {exc}[/]")
                    console.log(f"[grey50]{traceback.format_exc()[:500]}[/]")
                    await asyncio.sleep(1.0)

    except KeyboardInterrupt:
        pass
    finally:
        stop_event.set()
        if scan_task is not None:
            try:
                await asyncio.wait_for(scan_task, timeout=5.0)
            except Exception:
                scan_task.cancel()
        if dynamic:
            save_showcase(dash)
        if telegram_task is not None:
            try:
                await asyncio.wait_for(telegram_task, timeout=5.0)
            except Exception:
                telegram_task.cancel()
        if conn is not None:
            try:
                await conn.shutdown()
            except Exception:
                pass
        console.log("[bright_cyan]👋 HVTS SmartMoney AI — shutdown complete[/]")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="HVTS SmartMoney AI Dashboard (licensed) — "
                    "FOREX (MT5) + CRYPTO (Gate.io) · SMC D1 + Fractals D1"
    )
    ap.add_argument("--demo",    action="store_true",
                    help="force demo mode (no MT5, no Gate)")
    ap.add_argument("--offline", action="store_true",
                    help="ADMIN ONLY — bypass licence check")
    ap.add_argument(
        "--license", "-L",
        choices=["status", "activate", "deactivate", "info"],
        help="License management: status | activate | deactivate | info",
    )
    args = ap.parse_args()

    if args.license:
        console = Console()
        sys.exit(run_license_cli(args.license, console))

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    try:
        asyncio.run(run_dashboard(force_demo=args.demo, offline=args.offline))
    except KeyboardInterrupt:
        print("\n👋 Interrupted by user.")


if __name__ == "__main__":
    main()