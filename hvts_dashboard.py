#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
════════════════════════════════════════════════════════════════════════════════
HVTS SmartMoney AI — Futuristic Rich Terminal Dashboard  (v1.3 — LICENSED + SMC H4/D1)
════════════════════════════════════════════════════════════════════════════════

WHAT CHANGED SINCE v1.2
───────────────────────
    • NEW SIGNAL — "SMC D1": same persistent binary SMC bias as SMC H4, on the
      Daily timeframe. New table column + second line in the FOCUS panel.
      The SMC engine now runs per timeframe (SMC_TIMEFRAMES) with independent
      stored states, so H4 and D1 can disagree.

WHAT CHANGED SINCE v1.1
───────────────────────
    • NEW SIGNAL — "SMC H4": a persistent, strictly binary Smart-Money-Concept
      bias (▲ BULLISH / ▼ BEARISH — never neutral) per symbol, computed on the
      H4 timeframe by basic_smc.analyze_basic_smc().
        - Shown as a new column in the SYMBOL SIGNALS table and as an extra
          line (confidence / method / swing levels) in the FOCUS panel.
        - Persistent: the last state is stored in SQLite (basic_smc table) and
          survives restarts. It only flips on a new H4 break of structure.
        - If basic_smc returns neutral (cold start, no breakout, trend and
          structure disagree) the dashboard seeds a bias from the position of
          price inside the recent swing range (premium = bullish, discount =
          bearish) and stores it, so the column is never neutral.
        - Own database file (hvts_dashboard_smc.db next to the script/exe,
          override with HVTS_SMC_DB) so the dashboard never overwrites the
          state used by the trading bots. Demo mode uses a separate *_demo.db.
    • Fixed: --demo crashed on symbols missing from BASE_PRICES (KeyError).

WHAT CHANGED SINCE v1.0
───────────────────────
    • License integration is now mandatory before the dashboard can run.
    • First launch → shows a license activation screen (rich-rendered).
    • Subsequent launches → validates license.key silently, then proceeds.
    • `--license` subcommand provides status / activate / deactivate / info.
    • `--demo` still works WITH license (lets you preview without MT5).
    • `--offline` bypasses the licence gate — ONLY for admin development.
    • All other behaviour (5-indicator oracle, MT5 streaming, rich UI)
      is identical to v1.0.

LICENSE FLOW
────────────
    1. On start, LicenseManager.validate_license() is called.
    2. If valid   → activate and proceed to MT5 / demo boot.
    3. If invalid → render activation screen (paste key or quit).
    4. On paste   → validate the key, save to license.key, proceed.
    5. Ctrl+C during activation → clean exit (no crash).

USAGE
─────
    python hvts_dashboard.py                     # normal run (needs licence)
    python hvts_dashboard.py --demo              # demo mode (needs licence)
    python hvts_dashboard.py --license status    # show licence status
    python hvts_dashboard.py --license activate  # paste a key interactively
    python hvts_dashboard.py --license info      # show machine id + host info
    python hvts_dashboard.py --license deactivate
    python hvts_dashboard.py --offline           # ADMIN ONLY — bypass licence

ADMIN WORKFLOW (you, the developer)
───────────────────────────────────
    On a user's machine:
        python hvts_dashboard.py --license info
        # → they paste the "MACHINE ID" back to you

    On your machine:
        python generate_licenses.py --email them@example.com \
                                    --machine <ID> --expiry 365
        # → send them license_them_at_example_com.key

    Back on their machine:
        # rename/copy that file to "license.key" next to hvts_dashboard.py
        # OR run:  python hvts_dashboard.py --license activate
════════════════════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time
import traceback
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Rich
# ---------------------------------------------------------------------------
try:
    from rich.align import Align
    from rich.box import DOUBLE, HEAVY, ROUNDED
    from rich.columns import Columns
    from rich.console import Console, Group
    from rich.layout import Layout
    from rich.live import Live
    from rich.panel import Panel
    from rich.prompt import Prompt
    from rich.rule import Rule
    from rich.table import Table
    from rich.text import Text
except ImportError:
    print("❌ The 'rich' library is required.  →  pip install rich")
    sys.exit(1)

# ---------------------------------------------------------------------------
# Local imports
# ---------------------------------------------------------------------------
try:
    from SuperTrendOracleIndicator_M5 import (
        SuperTrendOracle_M2,
        SignalType,
        M1Signal,
    )
except Exception as exc:
    print(f"❌ Cannot import SuperTrendOracleIndicator_M5: {exc}")
    print("   Make sure the file is in the same folder as this dashboard.")
    sys.exit(1)

try:
    from mt5_connection import MT5Connection
    MT5_AVAILABLE = True
    _MT5_IMPORT_ERR: Optional[str] = None
except Exception as exc:
    MT5_AVAILABLE = False
    MT5Connection = None                    # type: ignore[assignment]
    _MT5_IMPORT_ERR = str(exc)

# ---------------------------------------------------------------------------
# SMC (Smart Money Concept) — H4 binary bias
#   basic_smc keeps a persistent bullish/bearish state per (symbol, timeframe)
#   in SQLite.  The dashboard uses its OWN db file so it never overwrites the
#   state of the trading bots.  basic_smc reads its db path at import time and
#   creates the table immediately, so DB_PATH is pointed at our file for the
#   duration of the import (then restored), and SMC_STATE_DB is set explicitly.
# ---------------------------------------------------------------------------
def _app_dir() -> Path:
    """Folder of the .exe when frozen by PyInstaller, else of this script."""
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
# Licence imports — REQUIRED
# ---------------------------------------------------------------------------
try:
    from license_manager import (
        LicenseManager,
        LicenseCLI,
        SECRET_KEY,
        LICENSE_FILE,
    )
    LICENSE_AVAILABLE = True
    _LICENSE_IMPORT_ERR: Optional[str] = None
except Exception as exc:
    LICENSE_AVAILABLE = False
    LicenseManager = None                   # type: ignore[assignment]
    LicenseCLI = None                       # type: ignore[assignment]
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
    "EURUSD", "GBPUSD", "USDCAD", "USDJPY", "USDCHF", "BTCUSD",
    "ETHUSD", "BNBUSD", "XRPUSD", "SOLUSD", "US30", 
]

ORACLE_PARAMS = dict(
    fib_lookback_30m=30,
    fib_lookback_1h=20,
    poly_lookback_30m=60,
    poly_lookback_1h=40,
    poly_lookback_12h=20,
    buy_threshold=0.4,
    strong_buy_threshold=0.6,
    sell_threshold=-0.4,
    strong_sell_threshold=-0.6,
    strength_threshold=0.3,
)

REFRESH_INTERVAL   = 2.0
FETCH_INTERVAL     = 15.0
ACCOUNT_INTERVAL   = 10.0
M1_FETCH_COUNT     = 240
HTF_FETCH          = {"M30": 240, "H1": 240, "H12": 120}
MAX_RECENT_SIGNALS = 30

# SMC H4 bias (basic_smc) — values mirror basic_smc's own H4 defaults
SMC_TIMEFRAMES     = ["H4", "D1"]
SMC_BARS           = {"H4": 150, "D1": 100}   # bars fetched per timeframe
SMC_TF_LABEL      = "/".join(SMC_TIMEFRAMES)  # e.g. "H4/D1" (used in log lines)
SMC_LOOKBACK       = 10       # rolling breakout window
SMC_SWING_LOOKBACK = 3        # swing-point fallback window
SMC_FETCH_INTERVAL = 60.0     # H4/D1 structure moves slowly — no need to poll every 15 s


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
    symbol:           str
    signal:           Optional[M1Signal]           = None
    derived_type:     Optional[SignalType]         = None
    net_score:        float                        = 0.0
    strength:         float                        = 0.0
    fib_30m:          float                        = 0.5
    fib_1h:           float                        = 0.5
    poly_30m:         float                        = 0.5
    poly_1h:          float                        = 0.5
    poly_12h:         float                        = 0.5
    adx_30m:          float                        = 20.0
    adx_1h:           float                        = 20.0
    adx_12h:          float                        = 20.0
    warm_legs:        List[str]                    = field(default_factory=list)
    warm_count:       int                          = 0
    last_price:       float                        = 0.0
    last_update:      Optional[datetime]           = None
    last_signal_time: Optional[datetime]           = None
    net_history:      Deque[float]                 = field(default_factory=lambda: deque(maxlen=60))
    error:            str                          = ""


@dataclass
class SmcState:
    """Persistent binary SMC bias for one symbol (never neutral once resolved)."""
    state:       str                = ""      # "bullish" | "bearish" | "" (not yet resolved)
    confidence:  float              = 0.0
    strength:    float              = 0.0
    method:      str                = ""      # breakout | swing | bootstrap | stored | forced
    swing_high:  Optional[float]    = None
    swing_low:   Optional[float]    = None
    updated:     Optional[datetime] = None


@dataclass
class DashboardState:
    account:        AccountState  = field(default_factory=AccountState)
    license:        LicenseState  = field(default_factory=LicenseState)
    symbols:        Dict[str, SymbolState] = field(default_factory=dict)
    smc:            Dict[str, Dict[str, SmcState]] = field(default_factory=dict)  # tf -> symbol -> state
    focus_symbol:   str = "BTCUSD"
    recent_signals: Deque[Dict] = field(default_factory=lambda: deque(maxlen=MAX_RECENT_SIGNALS))
    start_time:     datetime = field(default_factory=datetime.now)
    cycle:          int = 0
    total_signals:  int = 0
    mt5_status:     str = "OFFLINE"


# ═══════════════════════════════════════════════════════════════════════════
# VISUAL HELPERS
# ═══════════════════════════════════════════════════════════════════════════

SPARK = "▁▂▃▄▅▆▇█"


def sparkline(values, width: int = 24) -> Text:
    vals = list(values)[-width:]
    if not vals:
        return Text("─" * width, style="grey27")
    if len(vals) < width:
        vals = [0.0] * (width - len(vals)) + vals
    txt = Text()
    for v in vals:
        idx = int(max(0, min(7, (v + 1.0) / 2.0 * 7)))
        if v >= 0.15:
            color = "bright_green"
        elif v <= -0.15:
            color = "bright_red"
        else:
            color = "grey58"
        txt.append(SPARK[idx], style=color)
    return txt


def fmt_money(v: float) -> str:
    return f"${v:,.2f}"


def fmt_price(p: float) -> str:
    if p >= 1000:
        return f"{p:,.2f}"
    if p >= 10:
        return f"{p:.4f}"
    if p >= 1:
        return f"{p:.5f}"
    return f"{p:.6f}"


def signal_cell(sig_type: Optional[SignalType]) -> Text:
    if sig_type is None:
        return Text("·  STANDBY", style="grey50")
    m = {
        SignalType.STRONG_BUY:  ("▲▲ STRONG",  "bold bright_white on dark_green"),
        SignalType.BUY:         ("▲  BUY   ",  "bright_green"),
        SignalType.NEUTRAL:     ("·  NEUTRAL", "grey58"),
        SignalType.SELL:        ("▼  SELL  ",  "bright_red"),
        SignalType.STRONG_SELL: ("▼▼ STRONG",  "bold bright_white on dark_red"),
    }
    txt, style = m.get(sig_type, ("?", "white"))
    return Text(txt, style=style)


def get_smc(dash: DashboardState, tf: str, sym: str) -> Optional[SmcState]:
    return dash.smc.get(tf, {}).get(sym)


def smc_cell(smc: Optional[SmcState]) -> Text:
    """Binary SMC cell: ▲ BULLISH / ▼ BEARISH (no neutral state exists)."""
    if not SMC_AVAILABLE:
        return Text("n/a", style="grey50")
    if smc is None or smc.state not in ("bullish", "bearish"):
        return Text("·  …", style="grey50")
    if smc.state == "bullish":
        return Text("▲ BULLISH", style="bold bright_green")
    return Text("▼ BEARISH", style="bold bright_red")


def net_text(v: float) -> Text:
    if v >= 0.15:
        return Text(f"{v:+.3f}", style="bold bright_green")
    if v <= -0.15:
        return Text(f"{v:+.3f}", style="bold bright_red")
    return Text(f"{v:+.3f}", style="grey58")


def warm_badge(n: int) -> Text:
    if n >= 5:
        return Text("● 5/5", style="bold bright_green")
    if n >= 4:
        return Text(f"● {n}/5", style="bright_green")
    if n >= 2:
        return Text(f"◐ {n}/5", style="yellow")
    return Text(f"○ {n}/5", style="bright_red")


def adx_color(v: float) -> str:
    if v >= 40: return "bright_green"
    if v >= 25: return "green"
    if v >= 20: return "yellow"
    return "grey58"


def adx_label(v: float) -> str:
    if v >= 40: return "STRONG"
    if v >= 25: return "TREND"
    if v >= 20: return "DEV"
    return "RANGE"


def leg_gauge(label: str, value: float, warm: bool, width: int = 16) -> Text:
    t = Text()
    t.append(f"{label:<10}", style="bright_white" if warm else "grey50")
    if not warm:
        t.append("░" * width, style="grey27")
        t.append("   —    ", style="grey50")
        t.append("COLD", style="italic grey50")
        return t
    val = max(0.0, min(1.0, value))
    filled = int(round(val * width))
    if val >= 0.70:   col = "bright_green"
    elif val >= 0.55: col = "green"
    elif val >= 0.45: col = "yellow"
    elif val >= 0.30: col = "dark_orange"
    else:             col = "bright_red"
    t.append("█" * filled, style=col)
    t.append("░" * (width - filled), style="grey27")
    t.append(f" {val:.2f} ", style=f"bold {col}")
    if val > 0.55:    t.append("▲", style="bright_green")
    elif val < 0.45:  t.append("▼", style="bright_red")
    else:             t.append("▬", style="grey58")
    return t


def adx_row(label: str, value: float, width: int = 12) -> Text:
    t = Text()
    t.append(f"{label:<4}", style="bright_white")
    v = max(0.0, min(100.0, value))
    filled = int(v / 100.0 * width)
    col = adx_color(v)
    t.append("█" * filled, style=col)
    t.append("░" * (width - filled), style="grey27")
    t.append(f" {v:5.1f} ", style="bold bright_white")
    t.append(adx_label(v), style=col)
    return t


# ═══════════════════════════════════════════════════════════════════════════
# LICENSE INTEGRATION
# ═══════════════════════════════════════════════════════════════════════════

def _license_banner(console: Console) -> None:
    """Draw a stylish banner above the activation prompt."""
    title = Text()
    title.append("⚡ ", style="bold bright_yellow")
    title.append("HVTS", style="bold bright_cyan")
    title.append(" SmartMoney ", style="bold bright_white")
    title.append("AI", style="bold bright_magenta")
    title.append("   ▸  ", style="bright_cyan")
    title.append("SuperTrend Oracle v3.3", style="italic cyan")
    console.print()
    console.print(Panel(
        Align.center(title),
        border_style="bright_cyan",
        box=HEAVY,
        padding=(0, 2),
    ))


def load_license_state(manager: LicenseManager) -> LicenseState:
    """Validate the current licence file and return a LicenseState snapshot."""
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
    """
    Show the activation screen. Return the new token on success, or None
    if the user chose to quit / Ctrl+C'd.
    """
    info = manager.get_machine_info()
    _license_banner(console)

    info_tbl = Table(box=ROUNDED, border_style="cyan", show_header=False, padding=(0, 1))
    info_tbl.add_column("k", style="grey58", no_wrap=True)
    info_tbl.add_column("v", style="bright_white")
    info_tbl.add_row("Hostname",     info["hostname"])
    info_tbl.add_row("System",       f"{info['system']} {info['release']}")
    info_tbl.add_row("Architecture", info["machine"])
    info_tbl.add_row("Processor",    info["processor"] or "—")
    info_tbl.add_row(
        "Machine ID",
        Text(info["machine_id"], style="bold bright_yellow"),
    )

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

    # Try to activate
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
            Text.from_markup(
                f"[bold bright_red]❌ Could not save {LICENSE_FILE}[/]"
            ),
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
    """
    Enforce the license gate. Returns a LicenseState on success, or None
    if the user declined / the license could not be obtained.
    """
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
        # Check expiry proximity — warn at 7 days
        if 0 <= state.days_left <= 7:
            console.log(
                f"[bold yellow]⚠ License expires in {state.days_left} day(s)[/]"
            )
        return state

    # Not valid → show activation screen
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
    """Small license card shown in the right-hand account panel column."""
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
            col = (
                "bright_green" if lic.days_left > 30
                else "yellow" if lic.days_left > 7
                else "bright_red"
            )
            tbl.add_row(
                "Expiry",
                Text(f"{lic.expiry} ({lic.days_left}d)", style=f"bold {col}"),
            )
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
# ORACLE ↔ STATE SYNC
# ═══════════════════════════════════════════════════════════════════════════

def sync_symbol_state(sym: str, oracle: SuperTrendOracle_M2, st: SymbolState) -> None:
    try:
        net, strength, comps = oracle._calculate_combined_score()
    except Exception as exc:
        st.error = f"score: {exc}"[:80]
        return

    try:
        st.net_score = float(net)
        st.strength  = float(strength)
        st.fib_30m   = float(oracle._latest_fib_30m)
        st.fib_1h    = float(oracle._latest_fib_1h)
        st.poly_30m  = float(oracle._latest_poly_30m)
        st.poly_1h   = float(oracle._latest_poly_1h)
        st.poly_12h  = float(oracle._latest_poly_12h)
        st.adx_30m   = float(oracle._latest_adx_30m)
        st.adx_1h    = float(oracle._latest_adx_1h)
        st.adx_12h   = float(oracle._latest_adx_12h)
        st.warm_legs = sorted(comps.get("__warm__", []))
        st.warm_count = len(st.warm_legs)
        st.net_history.append(st.net_score)

        try:
            sig_type, _ = oracle._determine_signal(net, strength)
            st.derived_type = sig_type
        except Exception:
            st.derived_type = SignalType.NEUTRAL

        if oracle._last_values is not None:
            st.last_price = float(oracle._last_values.close)

        st.last_update = datetime.now()
        st.error = ""
    except Exception as exc:
        st.error = f"sync: {exc}"[:80]


def record_signals(sym: str, signals: List[M1Signal], dash: DashboardState) -> None:
    for sig in signals:
        dash.recent_signals.appendleft({
            "time":       sig.timestamp,
            "symbol":     sym,
            "type":       sig.type,
            "price":      sig.price,
            "confidence": sig.confidence,
            "net":        float(sig.metadata.get("net_score", 0.0)),
            "reason":     sig.metadata.get("signal_reason", "—"),
        })
        dash.total_signals += 1
        st = dash.symbols.get(sym)
        if st is not None:
            st.signal = sig
            st.last_signal_time = datetime.now()


def pick_focus(dash: DashboardState) -> str:
    best = dash.focus_symbol
    best_score = -1.0
    for sym, st in dash.symbols.items():
        if st.error or st.warm_count < 1:
            continue
        score = abs(st.net_score) + 0.05 * st.warm_count
        if score > best_score:
            best_score = score
            best = sym
    return best


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


def gen_demo_m1(symbol: str, bars: int = 3000) -> pd.DataFrame:
    rng = np.random.default_rng(abs(hash(symbol)) % (2**32))
    base = BASE_PRICES.get(symbol, 100.0)
    drift = rng.normal(0.00005, 0.00002, bars)
    noise = rng.normal(0, 0.0006, bars)
    rets = drift + noise
    close = base * np.exp(np.cumsum(rets))
    open_ = np.roll(close, 1); open_[0] = base
    high = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, 0.0003, bars)))
    low  = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, 0.0003, bars)))
    vol  = rng.integers(100, 4000, bars)
    idx  = pd.date_range(
        end=pd.Timestamp.now().floor("min"),
        periods=bars, freq="1min",
    )
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close, "volume": vol},
        index=idx,
    )


_DEMO_TF = {"H4": ("4h", 0.006), "D1": ("1D", 0.014)}   # pandas freq, per-bar volatility


def gen_demo_bars(symbol: str, tf: str) -> pd.DataFrame:
    """Synthetic history for an SMC timeframe; last close equals the symbol's demo base price."""
    freq, vol = _DEMO_TF[tf]
    bars = SMC_BARS[tf]
    rng = np.random.default_rng((abs(hash(symbol)) + len(tf) + ord(tf[0])) % (2**32))
    base = BASE_PRICES.get(symbol, 100.0)
    close = np.exp(np.cumsum(rng.normal(0.0, vol, bars)))
    close = close * (base / close[-1])
    open_ = np.roll(close, 1); open_[0] = close[0]
    high  = np.maximum(open_, close) * (1 + np.abs(rng.normal(0, vol / 3, bars)))
    low   = np.minimum(open_, close) * (1 - np.abs(rng.normal(0, vol / 3, bars)))
    idx   = pd.date_range(end=pd.Timestamp.now().floor(freq), periods=bars, freq=freq)
    return pd.DataFrame(
        {"open": open_, "high": high, "low": low, "close": close,
         "volume": rng.integers(500, 20000, bars)},
        index=idx,
    )


def resample_htf(df_m1: pd.DataFrame, rule: str) -> pd.DataFrame:
    return (
        df_m1.resample(rule)
        .agg({"open": "first", "high": "max", "low": "min",
              "close": "last", "volume": "sum"})
        .dropna(subset=["open", "high", "low", "close"])
    )


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
    title.append("SuperTrend Oracle v3.3", style="italic cyan")
    title.append("  ▸  ", style="bright_cyan")
    if dash.account.connected:
        title.append("● LIVE", style="bold bright_green")
    else:
        title.append("● DEMO", style="bold bright_yellow")

    sub = Text()
    sub.append(f"Account {MT5_ACCOUNT}", style="bright_white")
    sub.append("  @  ", style="grey58")
    sub.append(MT5_SERVER, style="bright_white")
    sub.append("   │   ", style="grey50")
    sub.append(f"{len(SYMBOLS)} symbols   │   ", style="grey58")
    sub.append(f"Cycle {dash.cycle}   │   ", style="grey58")
    sub.append(f"Signals {dash.total_signals}", style="bold bright_magenta")
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
    tbl.add_column("SYMBOL", style="bold bright_white", no_wrap=True)
    tbl.add_column("SIGNAL", justify="center", no_wrap=True)
    for _tf in SMC_TIMEFRAMES:
        tbl.add_column(f"SMC {_tf}", justify="center", no_wrap=True)
    tbl.add_column("CONF", justify="right", no_wrap=True)
    tbl.add_column("NET", justify="right", no_wrap=True)
    tbl.add_column("STR", justify="right", no_wrap=True)
    tbl.add_column("WARM", justify="center", no_wrap=True)
    tbl.add_column("PRICE", justify="right", no_wrap=True)
    tbl.add_column("TREND (last 60 cycles)", no_wrap=True)

    for sym in SYMBOLS:
        st = dash.symbols.get(sym)
        if st is None:
            tbl.add_row(
                Text(sym, style="bold bright_white"),
                Text("· LOADING", style="grey50"),
                *[smc_cell(get_smc(dash, tf, sym)) for tf in SMC_TIMEFRAMES],
                Text("—"), Text("—"), Text("—"), Text("—"), Text("—"), Text(""),
            )
            continue
        if st.error:
            tbl.add_row(
                Text(sym, style="bold bright_white"),
                Text("⚠ ERROR", style="bold bright_red"),
                *[smc_cell(get_smc(dash, tf, sym)) for tf in SMC_TIMEFRAMES],
                Text("—"), Text("—"), Text("—"), Text("—"), Text("—"),
                Text(st.error[:40], style="italic red"),
            )
            continue

        conf = (
            Text(f"{st.signal.confidence:4.1f}%", style="bright_white")
            if st.signal and st.signal.type != SignalType.NEUTRAL
            else Text("   —  ", style="grey50")
        )
        tbl.add_row(
            Text(sym, style="bold bright_white"),
            signal_cell(st.derived_type),
            *[smc_cell(get_smc(dash, tf, sym)) for tf in SMC_TIMEFRAMES],
            conf,
            net_text(st.net_score),
            Text(f"{st.strength:.2f}",
                 style="bright_white" if st.strength > 0.3 else "grey58"),
            warm_badge(st.warm_count),
            Text(fmt_price(st.last_price), style="bright_white"),
            sparkline(st.net_history, width=26),
        )

    return Panel(
        tbl,
        title="[bold bright_cyan]◈ SYMBOL SIGNALS[/]",
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
        tbl.add_row("Status", Text("● LIVE", style="bold bright_green"))
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
    if acc.margin > 0:
        ml = acc.equity / acc.margin * 100.0
        col = "bright_green" if ml > 500 else "yellow" if ml > 200 else "bright_red"
        tbl.add_row("Margin Lvl", Text(f"{ml:.0f}%", style=f"bold {col}"))
    tbl.add_row("", "")

    up = datetime.now() - dash.start_time
    h, rem = divmod(int(up.total_seconds()), 3600)
    m, s = divmod(rem, 60)
    tbl.add_row("Uptime",  Text(f"{h:02d}:{m:02d}:{s:02d}", style="grey58"))
    tbl.add_row("Signals", Text(str(dash.total_signals), style="bold bright_magenta"))

    return Panel(
        tbl,
        title="[bold bright_cyan]💼 ACCOUNT[/]",
        border_style="cyan",
        box=ROUNDED,
        padding=(0, 1),
    )


def render_detail_panel(dash: DashboardState) -> Panel:
    sym = dash.focus_symbol
    st = dash.symbols.get(sym)
    if st is None or st.error:
        return Panel(
            Align.center(Text("— focus unavailable —", style="grey58")),
            title="[bold bright_cyan]◈ FOCUS[/]",
            border_style="cyan", box=ROUNDED,
        )

    left = Text()
    left.append(leg_gauge("Fib 30m",  st.fib_30m,  "fib_30m"  in st.warm_legs)); left.append("\n")
    left.append(leg_gauge("Fib 1h",   st.fib_1h,   "fib_1h"   in st.warm_legs)); left.append("\n")
    left.append(leg_gauge("Poly 30m", st.poly_30m, "poly_30m" in st.warm_legs)); left.append("\n")
    left.append(leg_gauge("Poly 1h",  st.poly_1h,  "poly_1h"  in st.warm_legs)); left.append("\n")
    left.append(leg_gauge("Poly 12h", st.poly_12h, "poly_12h" in st.warm_legs))

    right = Text()
    right.append(adx_row("30m", st.adx_30m)); right.append("\n")
    right.append(adx_row("1h",  st.adx_1h));  right.append("\n")
    right.append(adx_row("12h", st.adx_12h)); right.append("\n")

    avg_adx = (st.adx_30m + st.adx_1h + st.adx_12h) / 3.0
    right.append(Text("───", style="grey35")); right.append("\n")
    right.append(adx_row("AVG", avg_adx))

    cols = Columns([left, right], expand=True, equal=False, padding=(0, 4))

    sig_txt = signal_cell(st.derived_type)
    info = Text()
    info.append("Net ", style="grey58")
    if st.net_score >= 0.15:
        nc = "bright_green"
    elif st.net_score <= -0.15:
        nc = "bright_red"
    else:
        nc = "grey58"
    info.append(f"{st.net_score:+.3f}", style=f"bold {nc}")
    info.append("   Strength ", style="grey58")
    info.append(f"{st.strength:.2f}", style="bold bright_white")
    info.append("   Warm ", style="grey58")
    info.append(f"{st.warm_count}/5", style="bold bright_white")
    info.append("   Signal ", style="grey58")
    info.append_text(sig_txt)
    info.append("\nTrend ", style="grey58")
    info.append_text(sparkline(st.net_history, width=40))

    smc_lines = []
    for tf in SMC_TIMEFRAMES:
        smc = get_smc(dash, tf, sym)
        line = Text()
        line.append(f"SMC {tf} ", style="grey58")
        line.append_text(smc_cell(smc))
        if smc is not None and smc.state in ("bullish", "bearish"):
            line.append("   Conf ", style="grey58")
            line.append(f"{smc.confidence:.0f}%", style="bold bright_white")
            line.append("   Method ", style="grey58")
            line.append(smc.method or "—", style="bright_white")
            if smc.swing_high is not None and smc.swing_low is not None:
                line.append("   Swing H ", style="grey58")
                line.append(fmt_price(smc.swing_high), style="bright_white")
                line.append(" / L ", style="grey58")
                line.append(fmt_price(smc.swing_low), style="bright_white")
        smc_lines.append(line)

    body = Group(
        Text.from_markup(
            f"[bold bright_cyan]FOCUS[/] · [bold bright_white]{sym}[/]"
            f"  ·  last [bright_white]{fmt_price(st.last_price)}[/]"
        ),
        cols,
        info,
        *smc_lines,
    )
    return Panel(body, border_style="bright_cyan", box=ROUNDED, padding=(0, 1))


def render_log_panel(dash: DashboardState) -> Panel:
    if not dash.recent_signals:
        return Panel(
            Align.center(Text("— awaiting signals —", style="grey58")),
            title="[bold bright_cyan]◈ RECENT SIGNALS[/]",
            border_style="cyan", box=ROUNDED, padding=(0, 1),
        )

    tbl = Table(box=None, show_header=False, padding=(0, 1), expand=True)
    tbl.add_column("time",   style="grey58",                 no_wrap=True)
    tbl.add_column("sym",    style="bold bright_white",      no_wrap=True)
    tbl.add_column("sig",    no_wrap=True)
    tbl.add_column("price",  justify="right", style="bright_white", no_wrap=True)
    tbl.add_column("conf",   justify="right",                no_wrap=True)
    tbl.add_column("net",    justify="right",                no_wrap=True)
    tbl.add_column("reason", style="grey58",                 no_wrap=True)

    for r in list(dash.recent_signals)[:6]:
        tbl.add_row(
            Text(r["time"].strftime("%H:%M:%S"), style="grey58"),
            Text(r["symbol"], style="bold bright_white"),
            signal_cell(r["type"]),
            Text(fmt_price(r["price"]), style="bright_white"),
            Text(f"{r['confidence']:4.1f}%", style="bright_cyan"),
            net_text(r["net"]),
            Text(r["reason"], style="italic grey58"),
        )
    return Panel(
        tbl,
        title="[bold bright_cyan]◈ RECENT SIGNALS[/]",
        border_style="cyan", box=ROUNDED, padding=(0, 1),
    )


def build_dashboard(dash: DashboardState) -> Layout:
    root = Layout(name="root")
    root.split_column(
        Layout(name="header", size=4),
        Layout(name="main",   ratio=1),
        Layout(name="detail", size=13),
        Layout(name="log",    size=8),
    )
    root["main"].split_row(
        Layout(name="signals", ratio=3),
        Layout(name="account", ratio=1),
    )

    root["header"].update(render_header(dash))
    root["signals"].update(render_signals_panel(dash))

    # Account column = account card + license card stacked
    account_stack = Layout()
    account_stack.split_column(
        Layout(render_account_panel(dash), name="acct"),
        Layout(render_license_panel(dash), name="lic", size=11),
    )
    root["account"].update(account_stack)

    root["detail"].update(render_detail_panel(dash))
    root["log"].update(render_log_panel(dash))
    return root


# ═══════════════════════════════════════════════════════════════════════════
# SMC H4 ENGINE  (binary, persistent)
# ═══════════════════════════════════════════════════════════════════════════

def smc_select_db(path: Path) -> None:
    """Point basic_smc at the given state database (live vs demo) and init it."""
    if not SMC_AVAILABLE:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        _smc.SMC_STATE_DB = str(path)
        _smc._init_db()
    except Exception:
        pass


def _forced_binary_bias(df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Last-resort bias used only when basic_smc returns neutral (no stored state,
    no breakout, and its trend/structure bootstrap disagreed).

    Premium / discount read of the recent swing range:
        price above the range midpoint → bullish
        price below the range midpoint → bearish
        exactly on the midpoint        → sign of the regression slope
    """
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


def compute_smc(sym: str, df: Optional[pd.DataFrame], tf: str) -> Optional[Dict[str, Any]]:
    """
    Binary SMC bias for `sym` on timeframe `tf` ("H4" or "D1").

    Returns a basic_smc-style dict whose 'signal' is ALWAYS 'bullish' or
    'bearish', or None when there is neither usable data nor a stored state
    (the UI then shows a pending marker instead of a made-up direction).

    basic_smc does the heavy lifting and persists the state:
        breakout → swing → stored state → trend/structure bootstrap
    Only if all of those yield neutral do we seed a bias ourselves and persist
    it, so subsequent cycles read it back as the 'stored' state.
    """
    if not SMC_AVAILABLE:
        return None

    res = _smc.analyze_basic_smc(
        df if df is not None else pd.DataFrame(),
        lookback=SMC_LOOKBACK,
        swing_lookback=SMC_SWING_LOOKBACK,
        symbol=sym,
        timeframe=tf,
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
        timeframe=tf,
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


def apply_smc_result(dash: DashboardState, sym: str, res: Optional[Dict[str, Any]], tf: str) -> None:
    """Copy a compute_smc() result into the dashboard (keeps last state on None)."""
    if not res or res.get("signal") not in ("bullish", "bearish"):
        return
    smc = dash.smc.setdefault(tf, {}).setdefault(sym, SmcState())
    smc.state      = res["signal"]
    smc.confidence = float(res.get("confidence") or 0.0)
    smc.strength   = float(res.get("strength") or 0.0)
    smc.method     = str(res.get("method_used") or "")
    smc.swing_high = res.get("swing_high")
    smc.swing_low  = res.get("swing_low")
    smc.updated    = datetime.now()


async def update_smc(conn: MT5Connection, dash: DashboardState) -> int:
    """Fetch bars for every symbol/timeframe and refresh the persistent SMC biases."""
    if not SMC_AVAILABLE:
        return 0

    async def _bars(sym: str, tf: str):
        try:
            return sym, tf, _normalize_ohlcv(await conn.get_ohlcv(sym, tf, SMC_BARS[tf]))
        except Exception:
            # No data this cycle → empty frame; basic_smc falls back to the stored state
            return sym, tf, pd.DataFrame()

    frames = await asyncio.gather(*[_bars(s, tf) for tf in SMC_TIMEFRAMES for s in SYMBOLS])

    def _resolve_all():
        out = []
        for sym, tf, df in frames:
            try:
                out.append((sym, tf, compute_smc(sym, df, tf)))
            except Exception:
                out.append((sym, tf, None))
        return out

    resolved = 0
    for sym, tf, res in await asyncio.to_thread(_resolve_all):
        apply_smc_result(dash, sym, res, tf)
        if res:
            resolved += 1
    return resolved


# ═══════════════════════════════════════════════════════════════════════════
# LIVE MT5 UPDATE PATH
# ═══════════════════════════════════════════════════════════════════════════

def _normalize_ohlcv(df: pd.DataFrame) -> pd.DataFrame:
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


async def fetch_and_prime(
    conn: MT5Connection,
    oracles: Dict[str, SuperTrendOracle_M2],
    console: Console,
) -> int:
    async def _get(sym: str, tf: str, n: int):
        try:
            df = await conn.get_ohlcv(sym, tf, n)
            return sym, tf, _normalize_ohlcv(df)
        except Exception as exc:
            return sym, tf, exc

    tasks = []
    for sym in SYMBOLS:
        for tf, n in HTF_FETCH.items():
            tasks.append(_get(sym, tf, n))
    results = await asyncio.gather(*tasks, return_exceptions=True)

    buckets: Dict[str, Dict[str, pd.DataFrame]] = {s: {} for s in SYMBOLS}
    for res in results:
        if isinstance(res, Exception):
            continue
        sym, tf, payload = res
        if isinstance(payload, pd.DataFrame) and not payload.empty:
            buckets[sym][tf] = payload

    primed = 0
    for sym in SYMBOLS:
        b = buckets.get(sym, {})
        df30, df1h, df12 = b.get("M30"), b.get("H1"), b.get("H12")
        if df30 is None or df1h is None or df12 is None:
            missing = [k for k in ("M30", "H1", "H12") if k not in b]
            console.log(f"[yellow]⚠ {sym}: missing {missing}, cannot prime[/]")
            continue
        try:
            report = oracles[sym].prime_from_htf(df30, df1h, df12)
            primed += 1
            if report["warm_count"] < 5:
                console.log(
                    f"[yellow]⚡ {sym}: warm {report['warm_count']}/5 "
                    f"({', '.join(report['warm_legs']) or 'none'})[/]"
                )
        except Exception as exc:
            console.log(f"[red]❌ prime_from_htf({sym}) failed: {exc}[/]")
    return primed


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


async def update_m1(
    conn: MT5Connection,
    oracles: Dict[str, SuperTrendOracle_M2],
    dash: DashboardState,
) -> None:
    async def _m1(sym: str):
        try:
            df = await conn.get_ohlcv(sym, "M1", M1_FETCH_COUNT)
            return sym, df
        except Exception as exc:
            return sym, exc

    results = await asyncio.gather(*[_m1(s) for s in SYMBOLS], return_exceptions=True)

    for res in results:
        if isinstance(res, Exception):
            continue
        sym, df = res
        st = dash.symbols.setdefault(sym, SymbolState(sym))
        if isinstance(df, Exception):
            st.error = f"fetch: {df}"[:80]
            continue
        if df is None or df.empty:
            st.error = "no M1 data"
            continue
        try:
            df = _normalize_ohlcv(df)
            oracle = oracles[sym]
            signals = oracle.update(df)
            if signals:
                record_signals(sym, signals, dash)
            sync_symbol_state(sym, oracle, st)
        except Exception as exc:
            st.error = f"update: {exc}"[:80]


# ═══════════════════════════════════════════════════════════════════════════
# DEMO UPDATE PATH
# ═══════════════════════════════════════════════════════════════════════════

class DemoDriver:
    def __init__(self):
        self.prices = dict(BASE_PRICES)
        self.rng = np.random.default_rng(42)
        self.smc_bars: Dict[str, Dict[str, pd.DataFrame]] = {}   # tf -> symbol -> synthetic history

    def next_bar(self, sym: str, ts: pd.Timestamp) -> pd.Series:
        # setdefault: SYMBOLS may contain symbols that are not in BASE_PRICES
        p = self.prices.setdefault(sym, BASE_PRICES.get(sym, 100.0))
        change = float(self.rng.normal(0, p * 0.0004))
        new_p = p + change
        high = max(p, new_p) * (1 + abs(float(self.rng.normal(0, 0.0002))))
        low  = min(p, new_p) * (1 - abs(float(self.rng.normal(0, 0.0002))))
        self.prices[sym] = new_p
        return pd.Series(
            {"open": p, "high": high, "low": low, "close": new_p,
             "volume": int(self.rng.integers(100, 1000))},
            name=ts,
        )


async def update_demo_smc(driver: DemoDriver, dash: DashboardState) -> None:
    """Demo counterpart of update_smc(): live demo price drives the forming bar."""
    if not SMC_AVAILABLE:
        return
    for tf, per_sym in driver.smc_bars.items():
        for sym, hist in per_sym.items():
            try:
                df = hist.copy()
                px = float(driver.prices.get(sym, df["close"].iloc[-1]))
                df.iloc[-1, df.columns.get_loc("close")] = px
                df.iloc[-1, df.columns.get_loc("high")]  = max(float(df["high"].iloc[-1]), px)
                df.iloc[-1, df.columns.get_loc("low")]   = min(float(df["low"].iloc[-1]), px)
                apply_smc_result(dash, sym, compute_smc(sym, df, tf), tf)
            except Exception:
                continue


async def update_demo(
    driver: DemoDriver,
    oracles: Dict[str, SuperTrendOracle_M2],
    dash: DashboardState,
) -> None:
    now = pd.Timestamp.now().floor("min")
    for sym in SYMBOLS:
        st = dash.symbols.setdefault(sym, SymbolState(sym))
        bar = driver.next_bar(sym, now)
        df = bar.to_frame().T
        df.index = pd.DatetimeIndex([bar.name])
        try:
            signals = oracles[sym].update(df)
            if signals:
                record_signals(sym, signals, dash)
            sync_symbol_state(sym, oracles[sym], st)
        except Exception as exc:
            st.error = f"demo: {exc}"[:80]

    dash.account.connected   = True
    dash.account.mode        = "DEMO"
    dash.account.login       = MT5_ACCOUNT
    dash.account.server      = MT5_SERVER
    dash.account.balance     = 50_000.0 + (dash.cycle * 0.37)
    dash.account.equity      = dash.account.balance + (dash.cycle % 7) * 12.5
    dash.account.margin      = 320.0
    dash.account.free_margin = dash.account.equity - dash.account.margin


def bootstrap_demo(
    oracles: Dict[str, SuperTrendOracle_M2],
    dash: DashboardState,
    console: Console,
) -> DemoDriver:
    driver = DemoDriver()
    console.log("[yellow]⚠ Running in DEMO mode (synthetic data)[/]")
    for sym in SYMBOLS:
        df_m1  = gen_demo_m1(sym, 3000)
        df_30m = resample_htf(df_m1, "30min")
        df_1h  = resample_htf(df_m1, "1h")
        df_12h = resample_htf(df_m1, "12h")
        for _tf in SMC_TIMEFRAMES:
            driver.smc_bars.setdefault(_tf, {})[sym] = gen_demo_bars(sym, _tf)
        try:
            oracles[sym].prime_from_htf(df_30m, df_1h, df_12h)
            st = SymbolState(sym)
            st.last_price = float(df_m1["close"].iloc[-1])
            dash.symbols[sym] = st
            sync_symbol_state(sym, oracles[sym], st)
        except Exception as exc:
            console.log(f"[red]demo prime failed {sym}: {exc}[/]")
    console.log(f"[bright_green]✅ Demo primed {len(dash.symbols)}/{len(SYMBOLS)}[/]")
    return driver


# ═══════════════════════════════════════════════════════════════════════════
# LICENSE CLI SUBCOMMAND
# ═══════════════════════════════════════════════════════════════════════════

def run_license_cli(action: str, console: Console) -> int:
    """Handle `--license {status|activate|deactivate|info}`."""
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

async def run_dashboard(
    force_demo: bool = False,
    skip_prime: bool = False,
    offline: bool = False,
) -> None:
    console = Console()

    # ── 1. Licence gate ────────────────────────────────────────────────
    lic_state = ensure_licensed(console, bypass=offline)
    if lic_state is None:
        return

    # ── 2. Build oracles ───────────────────────────────────────────────
    oracles: Dict[str, SuperTrendOracle_M2] = {}
    for sym in SYMBOLS:
        oracles[sym] = SuperTrendOracle_M2(**ORACLE_PARAMS, name=f"Oracle_{sym}")
    console.log(f"[bright_cyan]⚙ Created {len(oracles)} oracles[/]")

    dash = DashboardState()
    dash.license = lic_state
    driver: Optional[DemoDriver] = None
    conn: Optional[MT5Connection] = None
    live_ok = False

    # ── 3. Try MT5 ─────────────────────────────────────────────────────
    if not force_demo and MT5_AVAILABLE:
        try:
            conn = MT5Connection()
            ok = await conn.initialize(
                account=MT5_ACCOUNT, password=MT5_PASSWORD, server=MT5_SERVER,
            )
            if ok:
                console.log(f"[bright_green]✅ MT5 connected — {MT5_ACCOUNT}@{MT5_SERVER}[/]")
                dash.mt5_status = "LIVE"
                if not skip_prime:
                    console.log("[cyan]⚡ Priming oracles from M30 / H1 / H12 …[/]")
                    primed = await fetch_and_prime(conn, oracles, console)
                    console.log(f"[bright_green]✅ Primed {primed}/{len(SYMBOLS)} oracles[/]")
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

    # ── 4. Demo fallback ───────────────────────────────────────────────
    if not live_ok:
        driver = bootstrap_demo(oracles, dash, console)
        dash.mt5_status = dash.mt5_status or "DEMO"

    # ── 4b. SMC H4 bias (persistent binary state) ──────────────────────
    if SMC_AVAILABLE:
        if live_ok:
            smc_select_db(SMC_DB_LIVE)
            n_smc = await update_smc(conn, dash)
            console.log(f"[bright_green]✅ SMC {SMC_TF_LABEL} bias resolved {n_smc}/{len(SYMBOLS) * len(SMC_TIMEFRAMES)}"
                        f"  ({SMC_DB_LIVE.name})[/]")
        else:
            smc_select_db(SMC_DB_DEMO)
            await update_demo_smc(driver, dash)
            console.log(f"[yellow]⚠ SMC {SMC_TF_LABEL} running on DEMO data  ({SMC_DB_DEMO.name})[/]")
    else:
        console.log(f"[yellow]⚠ basic_smc unavailable — SMC {SMC_TF_LABEL} disabled: {_SMC_IMPORT_ERR}[/]")

    # ── 5. Initial snapshot ────────────────────────────────────────────
    for sym in SYMBOLS:
        if sym in dash.symbols and not dash.symbols[sym].error:
            sync_symbol_state(sym, oracles[sym], dash.symbols[sym])
    dash.focus_symbol = pick_focus(dash)

    # ── 6. Live loop ───────────────────────────────────────────────────
    last_fetch   = 0.0
    last_account = 0.0
    last_smc     = time.time()          # initial SMC pass already done in step 4b
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
                        if now - last_fetch >= FETCH_INTERVAL:
                            await update_m1(conn, oracles, dash)
                            last_fetch = now
                    elif driver is not None:
                        await update_demo(driver, oracles, dash)

                    if now - last_smc >= SMC_FETCH_INTERVAL:
                        if live_ok:
                            await update_smc(conn, dash)
                        elif driver is not None:
                            await update_demo_smc(driver, dash)
                        last_smc = now

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
        if conn is not None:
            try:
                await conn.shutdown()
            except Exception:
                pass
        console.log("[bright_cyan]👋 HVTS SmartMoney AI — shutdown complete[/]")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="HVTS SmartMoney AI Dashboard (licensed)"
    )
    ap.add_argument("--demo",      action="store_true",
                    help="force demo mode (no MT5)")
    ap.add_argument("--no-prime",  action="store_true",
                    help="skip HTF priming (warms up incrementally)")
    ap.add_argument("--offline",   action="store_true",
                    help="ADMIN ONLY — bypass licence check")
    ap.add_argument(
        "--license", "-L",
        choices=["status", "activate", "deactivate", "info"],
        help="License management: status | activate | deactivate | info",
    )
    args = ap.parse_args()

    # Licence CLI takes precedence
    if args.license:
        console = Console()
        sys.exit(run_license_cli(args.license, console))

    try:
        asyncio.run(run_dashboard(
            force_demo=args.demo,
            skip_prime=args.no_prime,
            offline=args.offline,
        ))
    except KeyboardInterrupt:
        print("\n👋 Interrupted by user.")


if __name__ == "__main__":
    main()