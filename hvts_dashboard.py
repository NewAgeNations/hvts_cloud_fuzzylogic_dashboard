#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
════════════════════════════════════════════════════════════════════════════════
HVTS SmartMoney AI — SMC D1 + Fractals D1 Dashboard (v2.0)
════════════════════════════════════════════════════════════════════════════════

CHANGES vs v1.3
───────────────
    • REMOVED: SuperTrendOracleIndicator_M5 (5-indicator oracle), HTF priming,
      M1 data feed, per-leg gauges, ADX panel, sparklines, recent-signals log,
      and the entire SMC H4 pipeline.
    • KEPT:    SMC D1 persistent binary state (basic_smc).
    • ADDED:   Williams Fractals on D1 (fractals.py) — second signal column.
    • ADDED:   Telegram notifier (SMC D1 + Fractals D1) — first message sent
      on startup, then every 12 hours.

TELEGRAM
────────
    Hard-coded defaults below; override via env vars TELEGRAM_BOT_TOKEN /
    TELEGRAM_CHAT_ID. If both are empty, the notifier logs one warning
    and exits; the dashboard keeps running.

USAGE
─────
    python hvts_dashboard.py
    python hvts_dashboard.py --demo
    python hvts_dashboard.py --license status|activate|deactivate|info
    python hvts_dashboard.py --offline        # ADMIN ONLY
════════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import argparse
import asyncio
import html as _html
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

# ---------------------------------------------------------------------------
# MT5
# ---------------------------------------------------------------------------
try:
    from mt5_connection import MT5Connection
    MT5_AVAILABLE = True
    _MT5_IMPORT_ERR: Optional[str] = None
except Exception as exc:
    MT5_AVAILABLE = False
    MT5Connection = None                    # type: ignore[assignment]
    _MT5_IMPORT_ERR = str(exc)

# ---------------------------------------------------------------------------
# SMC (Smart Money Concept) — D1 binary bias
# ---------------------------------------------------------------------------
def _app_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


SMC_DB_LIVE = Path(os.getenv("HVTS_SMC_DB", str(_app_dir() / "hvts_dashboard_smc.db")))
SMC_DB_DEMO = SMC_DB_LIVE.with_name(f"{SMC_DB_LIVE.stem}_demo{SMC_DB_LIVE.suffix}")

_prev_db_path = os.environ.get("DB_PATH")
os.environ["DB_PATH"] = str(SMC_DB_LIVE)
try:
    import basic_smc as _smc
    SMC_AVAILABLE = True
    _SMC_IMPORT_ERR: Optional[str] = None
except Exception as exc:
    _smc = None                             # type: ignore[assignment]
    SMC_AVAILABLE = False
    _SMC_IMPORT_ERR = str(exc)
finally:
    if _prev_db_path is None:
        os.environ.pop("DB_PATH", None)
    else:
        os.environ["DB_PATH"] = _prev_db_path

# ---------------------------------------------------------------------------
# Fractals (Williams) — D1
# ---------------------------------------------------------------------------
try:
    from fractals import Fractals
    FRACTALS_AVAILABLE = True
    _FRACTALS_IMPORT_ERR: Optional[str] = None
except Exception as exc:
    Fractals = None                         # type: ignore[assignment]
    FRACTALS_AVAILABLE = False
    _FRACTALS_IMPORT_ERR = str(exc)

# ---------------------------------------------------------------------------
# Licence
# ---------------------------------------------------------------------------
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
    LicenseManager = None                   # type: ignore[assignment]
    SECRET_KEY = b""                        # type: ignore[assignment]
    LICENSE_FILE = "license.key"            # type: ignore[assignment]
    _LICENSE_IMPORT_ERR = str(exc)


# ═══════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═══════════════════════════════════════════════════════════════════════════

MT5_ACCOUNT  = os.getenv("MT5_ACCOUNT",  "960123153")
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "!d2mEgFw")
MT5_SERVER   = os.getenv("MT5_SERVER",   "HantecMarketsMU-MT5")

SYMBOLS: List[str] = [
    "EURUSD", "GBPUSD", "USDCAD", "USDJPY", "USDCHF", "EURCAD",
    "GBPCHF", "GBPJPY", "CADCHF", "GBPCAD", "CADJPY", "AUDUSD",
    "AUDNZD", "AUDCAD", "AUDCHF", "AUDJPY", "EURAUD", "EURNZD",
    "GBPAUD", "GBPNZD", "NZDCHF", "CHFJPY", "EURGBP", "EURCHF",
    "EURJPY", "US30", "BTCUSD", "XAUUSD", "ETHUSD", "USOIL+",
]

REFRESH_INTERVAL = 2.0
ACCOUNT_INTERVAL = 10.0

# D1 OHLC feed shared by SMC and Fractals
D1_FETCH_INTERVAL  = 60.0
D1_FETCH_BARS      = 120
SMC_LOOKBACK       = 10
SMC_SWING_LOOKBACK = 3
FRACTAL_LEFT_BARS  = 2
FRACTAL_RIGHT_BARS = 2

# ── Telegram ────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN = os.getenv(
    "TELEGRAM_BOT_TOKEN",
    "7426274394:AAHAebYveqkmi-S54yavvXy-WRXfdIG7m7s",
)
TELEGRAM_CHAT_ID = os.getenv(
    "TELEGRAM_CHAT_ID",
    "-1001897246404",
)
TELEGRAM_INTERVAL   = 12 * 60 * 60   # 12 hours
TELEGRAM_PARSE_MODE = "HTML"


# ═══════════════════════════════════════════════════════════════════════════
# DATA STRUCTURES
# ═══════════════════════════════════════════════════════════════════════════

@dataclass
class AccountState:
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
    signal:      str                = ""    # "buy" | "sell" | ""
    state:       str                = ""    # "bullish" | "bearish" | ""
    swing_high:  Optional[float]    = None
    swing_low:   Optional[float]    = None
    updated:     Optional[datetime] = None


@dataclass
class DashboardState:
    account:              AccountState          = field(default_factory=AccountState)
    license:              LicenseState          = field(default_factory=LicenseState)
    symbols:              Dict[str, SymbolState] = field(default_factory=dict)
    smc:                  Dict[str, SmcState]    = field(default_factory=dict)
    fractal:              Dict[str, FractalState]= field(default_factory=dict)
    focus_symbol:         str = "BTCUSD"
    start_time:           datetime = field(default_factory=datetime.now)
    cycle:                int = 0
    mt5_status:           str = "OFFLINE"

    telegram_last_send:   Optional[datetime] = None
    telegram_next_send:   Optional[datetime] = None
    telegram_last_status: str = ""
    telegram_sent_count:  int = 0
    telegram_fail_count:  int = 0


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
    if not SMC_AVAILABLE:
        return Text("n/a", style="grey50")
    if smc is None or smc.state not in ("bullish", "bearish"):
        return Text("·  …", style="grey50")
    if smc.state == "bullish":
        return Text("▲ BULLISH", style="bold bright_green")
    return Text("▼ BEARISH", style="bold bright_red")


def fractal_cell(fr: Optional[FractalState]) -> Text:
    if not FRACTALS_AVAILABLE:
        return Text("n/a", style="grey50")
    if fr is None or fr.state not in ("bullish", "bearish"):
        return Text("·  …", style="grey50")
    if fr.state == "bullish":
        return Text("▲ BULLISH", style="bold bright_green")
    return Text("▼ BEARISH", style="bold bright_red")


def _age_str(ts: Optional[datetime]) -> str:
    if ts is None:
        return "—"
    delta = (datetime.now() - ts).total_seconds()
    if delta < 60:    return f"{int(delta)}s"
    if delta < 3600:  return f"{int(delta // 60)}m"
    return f"{int(delta // 3600)}h {int((delta % 3600) // 60)}m"


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
    title.append("SMC D1 + Fractals D1", style="italic cyan")
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
    except Exception:
        return None


def compute_smc(sym: str, df: Optional[pd.DataFrame]) -> Optional[Dict[str, Any]]:
    if not SMC_AVAILABLE:
        return None

    res = _smc.analyze_basic_smc(
        df if df is not None else pd.DataFrame(),
        lookback=SMC_LOOKBACK,
        swing_lookback=SMC_SWING_LOOKBACK,
        symbol=sym,
        timeframe="D1",
    )
    if res.get("signal") in ("bullish", "bearish"):
        return res

    forced = _forced_binary_bias(df) if df is not None and not df.empty else None
    if forced is None:
        return None

    _smc._set_stored_state(
        symbol=sym,
        state=forced["signal"],
        confidence=50.0,
        strength=0.0,
        swing_high=forced["swing_high"],
        swing_low=forced["swing_low"],
        method_used="forced",
        lookback_used=SMC_LOOKBACK,
        timeframe="D1",
    )
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
    if not res or res.get("signal") not in ("bullish", "bearish"):
        return
    state = dash.smc.setdefault(sym, SmcState())
    state.state      = res["signal"]
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
        up_clean = up_levels.dropna()
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


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════

def _tg_escape(s: str) -> str:
    return _html.escape(str(s), quote=False)


def _fmt_smc_compact(smc: Optional[SmcState]) -> str:
    if smc is None or smc.state not in ("bullish", "bearish"):
        return "— SMC"
    arrow = "▲" if smc.state == "bullish" else "▼"
    return f"{arrow} SMC {smc.confidence:.0f}%"


def _fmt_fractal_compact(fr: Optional[FractalState]) -> str:
    if fr is None or fr.state not in ("bullish", "bearish"):
        return "— FRACTALS"
    arrow = "▲" if fr.state == "bullish" else "▼"
    return f"{arrow} FRACTALS"


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


def build_telegram_message(dash: DashboardState) -> str:
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: List[str] = [
        "⚡ <b>HVTS SmartMoney AI</b>",
        f"📅 {_tg_escape(now_str)}",
        "",
        "<b>📊 D1 SIGNALS — SMC + Fractals</b>",
        "─────────────────────────────",
    ]

    for sym in SYMBOLS:
        smc = dash.smc.get(sym)
        fr  = dash.fractal.get(sym)
        emoji = _symbol_agreement_emoji(smc, fr)
        smc_part = _fmt_smc_compact(smc)
        fr_part  = _fmt_fractal_compact(fr)
        lines.append(
            f"{emoji} <b>{_tg_escape(sym)}</b>   "
            f"{_tg_escape(smc_part)}   |   {_tg_escape(fr_part)}"
        )

    lines.extend([
        "─────────────────────────────",
        "SMC: Smart Money Concept (BOS binary state)",
        "FRACTALS: Williams 2-2 D1 breakout",
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
    """
    Send the SMC D1 + Fractals D1 digest every 12 hours.
    First message is sent immediately on entry (launched after initial D1 pass).
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning(
            "Telegram not configured — set TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID"
        )
        dash.telegram_last_status = "not configured"
        return

    last_send = 0.0
    while not stop_event.is_set():
        now = time.time()
        if now - last_send >= TELEGRAM_INTERVAL:
            try:
                msg = build_telegram_message(dash)
                ok = await _send_telegram_message(msg)
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
# DEMO DATA GENERATOR
# ═══════════════════════════════════════════════════════════════════════════

BASE_PRICES = {
    "BTCUSD": 67250.0, "ETHUSD": 3540.0, "XAUUSD": 2360.0,
    "CHFJPY": 171.20,  "EURCAD": 1.4740, "EURCHF": 0.9830,
    "EURGBP": 0.8520,  "EURJPY": 165.40, "EURUSD": 1.0835,
    "GBPUSD": 1.2715,  "USDCAD": 1.3610, "USDCHF": 0.8965,
    "USDJPY": 150.30,  "USOIL+": 78.45,
}


def gen_demo_d1(symbol: str) -> pd.DataFrame:
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
    def __init__(self):
        self.prices: Dict[str, float] = {}
        self.d1_bars: Dict[str, pd.DataFrame] = {}
        self.rng = np.random.default_rng(42)

    def seed(self, symbol: str) -> pd.DataFrame:
        df = gen_demo_d1(symbol)
        self.d1_bars[symbol] = df
        self.prices[symbol] = float(df["close"].iloc[-1])
        return df

    def next_tick(self, symbol: str) -> float:
        p = self.prices.get(symbol, BASE_PRICES.get(symbol, 100.0))
        p = max(0.01, p + float(self.rng.normal(0, p * 0.0006)))
        self.prices[symbol] = p
        return p


# ═══════════════════════════════════════════════════════════════════════════
# D1 SIGNAL UPDATE (LIVE + DEMO)
# ═══════════════════════════════════════════════════════════════════════════

def _resolve_d1_frames(frames: List[Tuple[str, pd.DataFrame]], dash: DashboardState) -> int:
    resolved = 0
    for sym, df in frames:
        try:
            if df is None or df.empty:
                continue
            res = compute_smc(sym, df)
            if res:
                apply_smc_result(dash, sym, res)

            fr = compute_fractals(sym, df)
            if fr is not None:
                dash.fractal[sym] = fr

            st = dash.symbols.setdefault(sym, SymbolState(sym))
            try:
                st.last_price  = float(df["close"].iloc[-1])
                st.last_update = datetime.now()
                st.error       = ""
            except Exception:
                pass
            resolved += 1
        except Exception as exc:
            logger.warning(f"D1 signal error for {sym}: {exc}")
    return resolved


async def update_d1_signals_live(conn: MT5Connection, dash: DashboardState) -> int:
    if not SMC_AVAILABLE and not FRACTALS_AVAILABLE:
        return 0

    async def _bars(sym: str):
        try:
            df = await conn.get_ohlcv(sym, "D1", D1_FETCH_BARS)
            return sym, _normalize_ohlcv(df)
        except Exception:
            return sym, pd.DataFrame()

    frames = await asyncio.gather(*[_bars(s) for s in SYMBOLS])
    return await asyncio.to_thread(_resolve_d1_frames, frames, dash)


async def update_d1_signals_demo(driver: DemoDriver, dash: DashboardState) -> int:
    frames = [(sym, driver.d1_bars.get(sym, pd.DataFrame())) for sym in SYMBOLS]
    return await asyncio.to_thread(_resolve_d1_frames, frames, dash)


# ═══════════════════════════════════════════════════════════════════════════
# ACCOUNT / DEMO UPDATES
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


async def update_demo_prices(driver: DemoDriver, dash: DashboardState) -> None:
    now = datetime.now()
    for sym in SYMBOLS:
        st = dash.symbols.setdefault(sym, SymbolState(sym))
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

    dash.account.connected   = True
    dash.account.mode        = "DEMO"
    dash.account.login       = MT5_ACCOUNT
    dash.account.server      = MT5_SERVER
    dash.account.balance     = 50_000.0 + (dash.cycle * 0.37)
    dash.account.equity      = dash.account.balance + (dash.cycle % 7) * 12.5
    dash.account.margin      = 320.0
    dash.account.free_margin = dash.account.equity - dash.account.margin


def bootstrap_demo(dash: DashboardState, console: Console) -> DemoDriver:
    driver = DemoDriver()
    console.log("[yellow]⚠ Running in DEMO mode (synthetic D1 data)[/]")
    for sym in SYMBOLS:
        df = driver.seed(sym)
        st = SymbolState(sym)
        st.last_price  = float(df["close"].iloc[-1])
        st.last_update = datetime.now()
        dash.symbols[sym] = st
    console.log(f"[bright_green]✅ Demo primed {len(dash.symbols)}/{len(SYMBOLS)} symbols[/]")
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
    title.append("SMC D1 + Fractals D1", style="italic cyan")
    title.append("  ▸  ", style="bright_cyan")
    if dash.account.connected:
        title.append("● LIVE" if dash.account.mode == "LIVE" else "● DEMO",
                     style="bold bright_green" if dash.account.mode == "LIVE" else "bold bright_yellow")
    else:
        title.append("● DEMO", style="bold bright_yellow")

    sub = Text()
    sub.append(f"Account {MT5_ACCOUNT}", style="bright_white")
    sub.append("  @  ", style="grey58")
    sub.append(MT5_SERVER, style="bright_white")
    sub.append("   │   ", style="grey50")
    sub.append(f"{len(SYMBOLS)} symbols   │   ", style="grey58")
    sub.append(f"Cycle {dash.cycle}", style="grey58")
    if dash.license.valid:
        sub.append("   │   ", style="grey50")
        sub.append("🔑 ", style="bright_green")
        sub.append(dash.license.email or "licensed", style="bright_green")
    sub.append(f"   │   {dash.mt5_status}", style="grey58")

    return Panel(
        Align.center(Group(Align.center(title), Align.center(sub))),
        border_style="bright_cyan",
        box=HEAVY,
        padding=(0, 1),
    )


def render_signals_panel(dash: DashboardState) -> Panel:
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

    for sym in SYMBOLS:
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
        title="[bold bright_cyan]◈ SYMBOL SIGNALS — D1[/]",
        border_style="cyan",
        box=ROUNDED,
        padding=(0, 0),
    )


def render_account_panel(dash: DashboardState) -> Panel:
    acc = dash.account
    tbl = Table(box=None, show_header=False, padding=(0, 1), expand=True)
    tbl.add_column("k", style="grey58", no_wrap=True)
    tbl.add_column("v", justify="right", no_wrap=True)

    if acc.connected:
        if acc.mode == "LIVE":
            tbl.add_row("Status", Text("● LIVE", style="bold bright_green"))
        else:
            tbl.add_row("Status", Text("● DEMO", style="bold bright_yellow"))
    else:
        tbl.add_row("Status", Text(f"○ {acc.mode}", style="bold bright_yellow"))
    tbl.add_row("", "")
    tbl.add_row("Login",  Text(acc.login  or "—", style="bright_white"))
    tbl.add_row("Server", Text(acc.server or "—", style="grey58"))
    tbl.add_row("", "")
    tbl.add_row("Balance", Text(fmt_money(acc.balance),     style="bold bright_white"))
    tbl.add_row("Equity",  Text(fmt_money(acc.equity),      style="bold bright_white"))
    tbl.add_row("Margin",  Text(fmt_money(acc.margin),      style="bright_yellow"))
    tbl.add_row("Free",    Text(fmt_money(acc.free_margin), style="bright_cyan"))
    tbl.add_row("", "")

    up = datetime.now() - dash.start_time
    h, rem = divmod(int(up.total_seconds()), 3600)
    m, s = divmod(rem, 60)
    tbl.add_row("Uptime", Text(f"{h:02d}:{m:02d}:{s:02d}", style="grey58"))

    return Panel(
        tbl,
        title="[bold bright_cyan]💼 ACCOUNT[/]",
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

    body = Group(
        Text.from_markup(
            f"[bold bright_cyan]FOCUS[/] · [bold bright_white]{sym}[/]"
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
    tbl.add_row("Interval", Text("12h", style="bright_white"))
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

    return Panel(
        tbl,
        title="[bold bright_cyan]◈ SYSTEM STATUS[/]",
        border_style="cyan", box=ROUNDED, padding=(0, 1),
    )


def build_dashboard(dash: DashboardState) -> Layout:
    root = Layout(name="root")
    root.split_column(
        Layout(name="header", size=4),
        Layout(name="main",   ratio=1),
        Layout(name="detail", size=12),
        Layout(name="status", size=9),
    )
    root["main"].split_row(
        Layout(name="signals", ratio=3),
        Layout(name="account", ratio=1),
    )

    root["header"].update(render_header(dash))
    root["signals"].update(render_signals_panel(dash))

    account_stack = Layout()
    account_stack.split_column(
        Layout(render_account_panel(dash), name="acct"),
        Layout(render_license_panel(dash), name="lic", size=11),
    )
    root["account"].update(account_stack)

    root["detail"].update(render_detail_panel(dash))
    root["status"].update(render_status_panel(dash))
    return root


def pick_focus(dash: DashboardState) -> str:
    best_sym: Optional[str] = None
    best_score = -1.0
    fallback_sym: Optional[str] = None
    fallback_score = -1.0

    for sym in SYMBOLS:
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

    if best_sym is not None:
        return best_sym
    if fallback_sym is not None:
        return fallback_sym
    return dash.focus_symbol


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

    driver: Optional[DemoDriver] = None
    conn: Optional[MT5Connection] = None
    live_ok = False

    # ── 2. Try MT5 ─────────────────────────────────────────────────────
    if not force_demo and MT5_AVAILABLE:
        try:
            conn = MT5Connection()
            ok = await conn.initialize(
                account=MT5_ACCOUNT, password=MT5_PASSWORD, server=MT5_SERVER,
            )
            if ok:
                console.log(f"[bright_green]✅ MT5 connected — {MT5_ACCOUNT}@{MT5_SERVER}[/]")
                dash.mt5_status = "LIVE"
                await update_account(conn, dash)
                live_ok = True
            else:
                console.log("[yellow]⚠ MT5 initialize() returned False — falling back to DEMO[/]")
                dash.mt5_status = "MT5 init failed"
        except Exception as exc:
            console.log(f"[yellow]⚠ MT5 setup failed: {exc}[/]")
            dash.mt5_status = f"MT5 error: {exc}"[:40]
    elif not MT5_AVAILABLE:
        console.log(f"[yellow]⚠ MT5 module unavailable: {_MT5_IMPORT_ERR}[/]")
        dash.mt5_status = "MT5 module missing"

    # ── 3. Demo fallback ───────────────────────────────────────────────
    if not live_ok:
        driver = bootstrap_demo(dash, console)
        dash.mt5_status = dash.mt5_status or "DEMO"

    # ── 4. Initial D1 signal pass ─────────────────────────────────────
    if live_ok:
        smc_select_db(SMC_DB_LIVE)
        n = await update_d1_signals_live(conn, dash)
        console.log(
            f"[bright_green]✅ D1 signals resolved {n}/{len(SYMBOLS)} "
            f"(SMC DB: {SMC_DB_LIVE.name})[/]"
        )
    else:
        smc_select_db(SMC_DB_DEMO)
        n = await update_d1_signals_demo(driver, dash)
        console.log(
            f"[yellow]⚠ D1 signals running on DEMO data — resolved {n}/{len(SYMBOLS)} "
            f"(SMC DB: {SMC_DB_DEMO.name})[/]"
        )
    dash.focus_symbol = pick_focus(dash)

    # ── 5. Spawn Telegram task (initial message sent immediately) ─────
    stop_event = asyncio.Event()
    telegram_task: Optional[asyncio.Task] = None
    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        console.log(
            f"[bright_cyan]📨 Telegram notifier active → chat {TELEGRAM_CHAT_ID} every 12h "
            f"(first message now)[/]"
        )
        telegram_task = asyncio.create_task(telegram_loop(dash, stop_event))
    else:
        console.log("[yellow]⚠ Telegram not configured — skipping notifier[/]")

    # ── 6. Live loop ───────────────────────────────────────────────────
    last_account = 0.0
    last_d1      = time.time()
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

                    if live_ok:
                        if now - last_account >= ACCOUNT_INTERVAL:
                            await update_account(conn, dash)
                            last_account = now

                    if driver is not None:
                        await update_demo_prices(driver, dash)

                    if now - last_d1 >= D1_FETCH_INTERVAL:
                        if live_ok:
                            await update_d1_signals_live(conn, dash)
                        elif driver is not None:
                            await update_d1_signals_demo(driver, dash)
                        last_d1 = now

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
        description="HVTS SmartMoney AI Dashboard (licensed) — SMC D1 + Fractals D1"
    )
    ap.add_argument("--demo",    action="store_true",
                    help="force demo mode (no MT5)")
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