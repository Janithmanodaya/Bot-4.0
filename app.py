# app.py
"""
EMA/BB Strategy Bot — Refactored from KAMA base.
 - DualLock for cross-thread locking
 - Exchange info cache to avoid repeated futures_exchange_info calls
 - Monitor thread persists unrealized PnL and SL updates back to managed_trades
 - Telegram thread with commands and Inline Buttons; includes /forcei
 - Blocking Binance/requests calls kept sync and invoked from async via asyncio.to_thread
 - Risk sizing: fixed 0.5 USDT when balance < 50, else 2% (configurable)
 - Defaults to MAINNET unless USE_TESTNET=true
"""
import os
import sys
import time
import math
import asyncio
import threading
import json
import logging
import signal
import sqlite3
import io
import re
import traceback
import psutil
import random
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
from collections import deque
from decimal import Decimal, ROUND_DOWN, getcontext, ROUND_CEILING

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np
import pandas as pd
import pandas_ta as ta
import matplotlib
matplotlib.use('Agg') # Use non-interactive backend for server-side plotting
import matplotlib.pyplot as plt
from fastapi import FastAPI

from binance.client import Client
from binance.exceptions import BinanceAPIException

import telegram
from telegram import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup

import mplfinance as mpf
from stocktrends import Renko

from dotenv import load_dotenv

# Load .env file into environment (if present)
load_dotenv()

# -------------------------
# Secrets (must be set in environment)
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
USE_TESTNET = False  # Force MAINNET — testnet mode removed per user request

# SSH Tunnel Config is now managed via ssh_config.json
# -------------------------

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("ema-bb-bot")

# Globals
client: Optional[Client] = None
request = telegram.utils.request.Request(con_pool_size=10)
telegram_bot: Optional[telegram.Bot] = telegram.Bot(token=TELEGRAM_BOT_TOKEN, request=request) if TELEGRAM_BOT_TOKEN else None
main_loop: Optional[asyncio.AbstractEventLoop] = None

# -------------------------
# CONFIG (edit values here)
# -------------------------
CONFIG = {
    # --- STRATEGY ---
    "STRATEGY_MODE": os.getenv("STRATEGY_MODE", "4"),  # 0=all, or comma-separated, e.g., "1,2"
    "STRATEGY_1": {  # Original Bollinger Band strategy
        "BB_LENGTH": int(os.getenv("BB_LENGTH_CUSTOM", "20")),
        "BB_STD": float(os.getenv("BB_STD_CUSTOM", "2.5")),
        "MIN_RSI_FOR_BUY": int(os.getenv("S1_MIN_RSI_FOR_BUY", "30")),
        "MAX_RSI_FOR_SELL": int(os.getenv("S1_MAX_RSI_FOR_SELL", "70")),
        "MAX_VOLATILITY_FOR_ENTRY": float(os.getenv("S1_MAX_VOL_ENTRY", "0.03")),
    },
    "STRATEGY_2": {  # New SuperTrend strategy
        "SUPERTREND_PERIOD": int(os.getenv("ST_PERIOD", "7")),
        "SUPERTREND_MULTIPLIER": float(os.getenv("ST_MULTIPLIER", "2.0")),
        "ADX_THRESHOLD": int(os.getenv("ST_ADX_THRESHOLD", "15")),
        "MIN_ADX_FOR_ENTRY": int(os.getenv("S2_MIN_ADX_ENTRY", "15")),
        "MIN_RSI_SELL": int(os.getenv("ST_MIN_RSI_SELL", "35")),
        "MAX_RSI_SELL": int(os.getenv("ST_MAX_RSI_SELL", "75")),
        "MIN_RSI_BUY": int(os.getenv("ST_MIN_RSI_BUY", "25")),
        "MAX_RSI_BUY": int(os.getenv("ST_MAX_RSI_BUY", "65")),
        "MIN_MACD_CONF": float(os.getenv("ST_MIN_MACD_CONF", "0.3")),
        "EMA_CONFIRMATION_PERIOD": int(os.getenv("ST_EMA_CONF_PERIOD", "20")),
        "MIN_VOLATILITY_FOR_ENTRY": float(os.getenv("S2_MIN_VOL_ENTRY", "0.003")),
        "MAX_VOLATILITY_FOR_ENTRY": float(os.getenv("S2_MAX_VOL_ENTRY", "0.035")),
        "BASE_CONFIDENCE_THRESHOLD": float(os.getenv("S2_BASE_CONF_THRESH", "55.0")),
        "LOW_VOL_CONF_THRESHOLD": float(os.getenv("S2_LOW_VOL_THRESH", "0.005")),
        "LOW_VOL_CONF_LEVEL": float(os.getenv("S2_LOW_VOL_LEVEL", "50.0")),
        "HIGH_VOL_CONF_THRESHOLD": float(os.getenv("S2_HIGH_VOL_THRESH", "0.01")),
        "HIGH_VOL_CONF_ADJUSTMENT": float(os.getenv("S2_HIGH_VOL_ADJUST", "5.0")),
    },
    "STRATEGY_3": { # Simple MA Cross strategy
        "FAST_MA": int(os.getenv("S3_FAST_MA", 9)),
        "SLOW_MA": int(os.getenv("S3_SLOW_MA", 21)),
        "ATR_SL_MULT": float(os.getenv("S3_ATR_SL_MULT", 1.5)),
        "FALLBACK_SL_PCT": float(os.getenv("S3_FALLBACK_SL_PCT", 0.015)),
        # --- New Trailing Stop parameters for S3 ---
        "TRAILING_ENABLED": os.getenv("S3_TRAILING_ENABLED", "true").lower() in ("true", "1", "yes"),
        "TRAILING_ATR_PERIOD": int(os.getenv("S3_TRAILING_ATR_PERIOD", "14")),
        "TRAILING_ATR_MULTIPLIER": float(os.getenv("S3_TRAILING_ATR_MULTIPLIER", "3.0")),
        "TRAILING_ACTIVATION_PROFIT_PCT": float(os.getenv("S3_TRAILING_ACTIVATION_PROFIT_PCT", "0.01")), # 1% profit
    },
    "STRATEGY_4": { # 3x SuperTrend strategy
        "ST1_PERIOD": int(os.getenv("S4_ST1_PERIOD", "12")),
        "ST1_MULT": float(os.getenv("S4_ST1_MULT", "3")),
        "ST2_PERIOD": int(os.getenv("S4_ST2_PERIOD", "11")),
        "ST2_MULT": float(os.getenv("S4_ST2_MULT", "2.0")),
        "ST3_PERIOD": int(os.getenv("S4_ST3_PERIOD", "10")),
        "ST3_MULT": float(os.getenv("S4_ST3_MULT", "1.4")),
        "RISK_USD": float(os.getenv("S4_RISK_USD", "0.50")), # Fixed risk amount
        "VOLATILITY_EXIT_ATR_MULT": float(os.getenv("S4_VOLATILITY_EXIT_ATR_MULT", "3.0")),
        "EMA_FILTER_PERIOD": int(os.getenv("S4_EMA_FILTER_PERIOD", "200")),
        "EMA_FILTER_ENABLED": os.getenv("S4_EMA_FILTER_ENABLED", "false").lower() in ("true", "1", "yes"),
    },
    "STRATEGY_5": { # Advanced MTF trend/structure strategy
        "H1_EMA_SHORT": int(os.getenv("S5_H1_EMA_SHORT", "21")),
        "H1_EMA_LONG": int(os.getenv("S5_H1_EMA_LONG", "55")),
        "H1_ST_PERIOD": int(os.getenv("S5_H1_ST_PERIOD", "10")),
        "H1_ST_MULT": float(os.getenv("S5_H1_ST_MULT", "3.0")),
        "M15_EMA_SHORT": int(os.getenv("S5_M15_EMA_SHORT", "21")),
        "M15_EMA_LONG": int(os.getenv("S5_M15_EMA_LONG", "55")),
        "ATR_PERIOD": int(os.getenv("S5_ATR_PERIOD", "14")),
        "RSI_PERIOD": int(os.getenv("S5_RSI_PERIOD", "14")),
        "VOL_LOOKBACK": int(os.getenv("S5_VOL_LOOKBACK", "10")),
        "VOL_SPIKE_MULT": float(os.getenv("S5_VOL_SPIKE_MULT", "1.2")),
        "MIN_ATR_PCT": float(os.getenv("S5_MIN_ATR_PCT", "0.003")),
        "MAX_ATR_PCT": float(os.getenv("S5_MAX_ATR_PCT", "0.02")),
        "RISK_USD": float(os.getenv("S5_RISK_USD", "0.50")), # Same quantity sizing mode as S4
        "STOP_ATR_MULT": float(os.getenv("S5_STOP_ATR_MULT", "1.5")),
        "STOP_MIN_ATR_MULT": float(os.getenv("S5_STOP_MIN_ATR_MULT", "1.0")),
        "STOP_BUFFER_PCT": float(os.getenv("S5_STOP_BUFFER_PCT", "0.001")), # 0.1% buffer
        "PARTIAL_TP1_R": float(os.getenv("S5_PARTIAL_TP1_R", "1.0")),
        "PARTIAL_TP1_PCT": float(os.getenv("S5_PARTIAL_TP1_PCT", "0.30")), # 30% at 1R
        "BE_TRIGGER_R": float(os.getenv("S5_BE_TRIGGER_R", "0.5")),
        "BE_OFFSET_ATR": float(os.getenv("S5_BE_OFFSET_ATR", "0.25")),
        "TRAIL_ATR_MULT": float(os.getenv("S5_TRAIL_ATR_MULT", "1.0")),
        "TRAIL_SWING_BUFFER_ATR": float(os.getenv("S5_TRAIL_SWING_BUFFER_ATR", "0.25")),
        "MAX_TRADES_PER_DAY": int(os.getenv("S5_MAX_TRADES_PER_DAY", "2")),
    },
    "STRATEGY_EXIT_PARAMS": {
        "1": {  # BB strategy
            "ATR_MULTIPLIER": float(os.getenv("S1_ATR_MULTIPLIER", "1.5")),
            "BE_TRIGGER": float(os.getenv("S1_BE_TRIGGER", "0.008")),
            "BE_SL_OFFSET": float(os.getenv("S1_BE_SL_OFFSET", "0.002"))
        },
        "2": {  # SuperTrend strategy
            "ATR_MULTIPLIER": float(os.getenv("S2_ATR_MULTIPLIER", "2.0")),
            "BE_TRIGGER": float(os.getenv("S2_BE_TRIGGER", "0.006")),
            "BE_SL_OFFSET": float(os.getenv("S2_BE_SL_OFFSET", "0.001"))
        },
        "3": {  # Advanced SuperTrend strategy (custom trailing logic)
            "ATR_MULTIPLIER": float(os.getenv("S3_TRAIL_ATR_MULT", "3.0")), # Value from S3 config
            "BE_TRIGGER": 0.0, # Not used in S3
            "BE_SL_OFFSET": 0.0 # Not used in S3
        },
        "4": {  # Advanced SuperTrend v2 strategy (custom trailing logic)
            "ATR_MULTIPLIER": float(os.getenv("S4_TRAIL_ATR_MULT", "3.0")), # Value from S4 config
            "BE_TRIGGER": 0.0, # Not used in S4
            "BE_SL_OFFSET": 0.0 # Not used in S4
        },
        "5": {  # Strategy 5 uses custom monitor logic; keep generic params minimal
            "ATR_MULTIPLIER": float(os.getenv("S5_TRAIL_ATR_MULT_GENERIC", "1.0")),
            "BE_TRIGGER": float(os.getenv("S5_BE_TRIGGER_GENERIC", "0.006")),  # ~0.6% as rough proxy if generic path used
            "BE_SL_OFFSET": float(os.getenv("S5_BE_SL_OFFSET_GENERIC", "0.0"))
        }
    },

    "SMA_LEN": int(os.getenv("SMA_LEN", "200")),
    "RSI_LEN": int(os.getenv("RSI_LEN", "2")),
    
    # --- ORDER MANAGEMENT ---
    "USE_LIMIT_ENTRY": os.getenv("USE_LIMIT_ENTRY", "true").lower() in ("true", "1", "yes"),
    "ORDER_ENTRY_TIMEOUT": int(os.getenv("ORDER_ENTRY_TIMEOUT", "1")), # 1 candle timeout for limit orders
    "ORDER_EXPIRY_CANDLES": int(os.getenv("ORDER_EXPIRY_CANDLES", "2")), # How many candles a limit order is valid for
    "ORDER_LIMIT_OFFSET_PCT": float(os.getenv("ORDER_LIMIT_OFFSET_PCT", "0.005")),
    "SL_BUFFER_PCT": float(os.getenv("SL_BUFFER_PCT", "0.02")),
    "LOSS_COOLDOWN_HOURS": int(os.getenv("LOSS_COOLDOWN_HOURS", "6")),

    # --- FAST MOVE FILTER (avoids entry on volatile candles) ---
    "FAST_MOVE_FILTER_ENABLED": os.getenv("FAST_MOVE_FILTER_ENABLED", "true").lower() in ("true", "1", "yes"),
    "FAST_MOVE_ATR_MULT": float(os.getenv("FAST_MOVE_ATR_MULT", "2.0")), # Candle size > ATR * mult
    "FAST_MOVE_RETURN_PCT": float(os.getenv("FAST_MOVE_RETURN_PCT", "0.005")), # 1m return > 0.5%
    "FAST_MOVE_VOL_MULT": float(os.getenv("FAST_MOVE_VOL_MULT", "2.0")), # Volume > avg_vol * mult

    # --- ADX TREND FILTER ---
    "ADX_FILTER_ENABLED": os.getenv("ADX_FILTER_ENABLED", "true").lower() in ("true", "1", "yes"),
    "ADX_PERIOD": int(os.getenv("ADX_PERIOD", "14")),
    "ADX_THRESHOLD": float(os.getenv("ADX_THRESHOLD", "25.0")),

    # --- TP/SL & TRADE MANAGEMENT ---
    "PARTIAL_TP_CLOSE_PCT": float(os.getenv("PARTIAL_TP_CLOSE_PCT", "0.8")),
    # BE_TRIGGER_PROFIT_PCT and BE_SL_PROFIT_PCT are now in STRATEGY_EXIT_PARAMS
    
    # --- CORE ---
    "SYMBOLS": os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT").split(","),
    "TIMEFRAME": os.getenv("TIMEFRAME", "15m"),
    "SCAN_INTERVAL": int(os.getenv("SCAN_INTERVAL", "60")),
    "CANDLE_SYNC_BUFFER_SEC": int(os.getenv("CANDLE_SYNC_BUFFER_SEC", "10")),
    "MAX_CONCURRENT_TRADES": int(os.getenv("MAX_CONCURRENT_TRADES", "3")),
    "START_MODE": os.getenv("START_MODE", "running").lower(),
    "SESSION_FREEZE_ENABLED": os.getenv("SESSION_FREEZE_ENABLED", "true").lower() in ("true", "1", "yes"),

    # --- INDICATOR SETTINGS ---
    # "BB_LENGTH_CUSTOM" and "BB_STD_CUSTOM" are now in STRATEGY_1
    "ATR_LENGTH": int(os.getenv("ATR_LENGTH", "14")),
    # "SL_TP_ATR_MULT" is now in STRATEGY_EXIT_PARAMS as "ATR_MULTIPLIER"

    "RISK_SMALL_BALANCE_THRESHOLD": float(os.getenv("RISK_SMALL_BALANCE_THRESHOLD", "50.0")),
    "RISK_SMALL_FIXED_USDT": float(os.getenv("RISK_SMALL_FIXED_USDT", "0.5")),
    "RISK_SMALL_FIXED_USDT_STRATEGY_2": float(os.getenv("RISK_SMALL_FIXED_S2", "0.6")),
    "MARGIN_USDT_SMALL_BALANCE": float(os.getenv("MARGIN_USDT_SMALL_BALANCE", "1.0")),
    "RISK_PCT_LARGE": float(os.getenv("RISK_PCT_LARGE", "0.02")),
    "RISK_PCT_STRATEGY_2": float(os.getenv("RISK_PCT_S2", "0.025")),
    "MAX_RISK_USDT": float(os.getenv("MAX_RISK_USDT", "0.0")),  # 0 disables cap
    "MAX_BOT_LEVERAGE": int(os.getenv("MAX_BOT_LEVERAGE", "30")),


    "TRAILING_ENABLED": os.getenv("TRAILING_ENABLED", "true").lower() in ("true", "1", "yes"),

    "MAX_DAILY_LOSS": float(os.getenv("MAX_DAILY_LOSS", "-2.0")), # Negative value, e.g. -50.0 for $50 loss
    "MAX_DAILY_PROFIT": float(os.getenv("MAX_DAILY_PROFIT", "5.0")), # 0 disables this
    "AUTO_FREEZE_ON_PROFIT": os.getenv("AUTO_FREEZE_ON_PROFIT", "true").lower() in ("true", "1", "yes"),
    "DAILY_PNL_CHECK_INTERVAL": int(os.getenv("DAILY_PNL_CHECK_INTERVAL", "60")), # In seconds

    "DB_FILE": os.getenv("DB_FILE", "trades.db"),
    
    "DRY_RUN": os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes"),
    "MIN_NOTIONAL_USDT": float(os.getenv("MIN_NOTIONAL_USDT", "5.0")),
    "HEDGING_ENABLED": os.getenv("HEDGING_ENABLED", "true").lower() in ("true", "1", "yes"),
    "MONITOR_LOOP_THRESHOLD_SEC": int(os.getenv("MONITOR_LOOP_THRESHOLD_SEC", "25")),
    "AUTO_RESTART_ON_IP_ERROR": os.getenv("AUTO_RESTART_ON_IP_ERROR", "true").lower() in ("true", "1", "yes"),
    "TELEGRAM_NOTIFY_REJECTIONS": os.getenv("TELEGRAM_NOTIFY_REJECTIONS", "false").lower() in ("true", "1", "yes"),
}

# --- Parse STRATEGY_MODE into a list of ints ---
try:
    # This will now be a list of ints, e.g., [1, 2] or [0]
    CONFIG['STRATEGY_MODE'] = [int(x.strip()) for x in str(CONFIG['STRATEGY_MODE']).split(',')]
except (ValueError, TypeError):
    log.error(f"Invalid STRATEGY_MODE: '{CONFIG['STRATEGY_MODE']}'. Must be a comma-separated list of numbers. Defaulting to auto (0).")
    CONFIG['STRATEGY_MODE'] = [0]


running = (CONFIG["START_MODE"] == "running")
overload_notified = False
frozen = False
daily_loss_limit_hit = False
daily_profit_limit_hit = False
ip_whitelist_error = False # Flag to track IP whitelist error
current_daily_pnl = 0.0

# Session freeze state
session_freeze_active = False
session_freeze_override = False
notified_frozen_session: Optional[str] = None

rejected_trades = deque(maxlen=20)
last_attention_alert_time: Dict[str, datetime] = {}
symbol_loss_cooldown: Dict[str, datetime] = {}
symbol_trade_cooldown: Dict[str, datetime] = {}
last_env_rejection_log: Dict[tuple[str, str], float] = {}

# Account state
IS_HEDGE_MODE: Optional[bool] = None

# DualLock for cross-thread (thread + async) coordination
class DualLock:
    def __init__(self):
        self._lock = threading.Lock()

    def acquire(self, timeout: Optional[float] = None) -> bool:
        if timeout is None:
            return self._lock.acquire()
        return self._lock.acquire(timeout=timeout)

    def release(self) -> None:
        self._lock.release()

    def __enter__(self):
        self._lock.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._lock.release()

    async def __aenter__(self):
        await asyncio.to_thread(self._lock.acquire)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._lock.release()

managed_trades: Dict[str, Dict[str, Any]] = {}
managed_trades_lock = DualLock()  # used by both async and sync code

pending_limit_orders: Dict[str, Dict[str, Any]] = {}
pending_limit_orders_lock = DualLock()

symbol_regimes: Dict[str, str] = {}
symbol_regimes_lock = threading.Lock()

symbol_evaluation_locks: Dict[str, asyncio.Lock] = {}

# S4 Sequential Confirmation State
s4_confirmation_state: Dict[str, Dict[str, Any]] = {}

last_trade_close_time: Dict[str, datetime] = {}

# S5 per-symbol daily trade limiter state
s5_daily_trade_counter: Dict[str, Dict[str, Any]] = {}

telegram_thread: Optional[threading.Thread] = None
monitor_thread_obj: Optional[threading.Thread] = None
pnl_monitor_thread_obj: Optional[threading.Thread] = None
maintenance_thread_obj: Optional[threading.Thread] = None
alerter_thread_obj: Optional[threading.Thread] = None
s5_monitor_thread_obj: Optional[threading.Thread] = None
monitor_stop_event = threading.Event()

# Thread failure counters
pnl_monitor_consecutive_failures = 0
alerter_consecutive_failures = 0

last_maintenance_month = "" # YYYY-MM format

scan_task: Optional[asyncio.Task] = None
rogue_check_task: Optional[asyncio.Task] = None
notified_rogue_symbols: set[str] = set()

# Flag for one-time startup sync
initial_sync_complete: bool = False

# Scan cycle state
scan_cycle_count: int = 0
next_scan_time: Optional[datetime] = None

# Exchange info cache
EXCHANGE_INFO_CACHE = {"ts": 0.0, "data": None, "ttl": 300}  # ttl seconds

async def _import_rogue_position_async(symbol: str, position: Dict[str, Any]) -> Optional[tuple[str, Dict[str, Any]]]:
    """
    Imports a single rogue position, places a default SL order, and returns the trade metadata.
    """
    try:
        log.info(f"❗️ Rogue position for {symbol} detected. Importing for management...")
        entry_price = float(position['entryPrice'])
        qty = abs(float(position['positionAmt']))
        side = 'BUY' if float(position['positionAmt']) > 0 else 'SELL'
        leverage = int(position.get('leverage', CONFIG.get("MAX_BOT_LEVERAGE", 20)))
        notional = qty * entry_price

        try:
            # default_sl_tp_for_import returns three values now
            stop_price, _, current_price = await asyncio.to_thread(default_sl_tp_for_import, symbol, entry_price, side)
        except RuntimeError as e:
            log.error(f"Failed to calculate default SL for {symbol}: {e}")
            return None

        # --- Safety Check for SL Placement ---
        if side == 'BUY' and stop_price >= current_price:
            log.warning(f"Rogue import for {symbol} calculated an invalid SL ({stop_price}) which is >= current price ({current_price}). Skipping SL placement.")
            stop_price = None # Do not place an SL
        elif side == 'SELL' and stop_price <= current_price:
            log.warning(f"Rogue import for {symbol} calculated an invalid SL ({stop_price}) which is <= current price ({current_price}). Skipping SL placement.")
            stop_price = None # Do not place an SL

        trade_id = f"{symbol}_imported_{int(time.time())}"
        meta = {
            "id": trade_id, "symbol": symbol, "side": side, "entry_price": entry_price,
            "initial_qty": qty, "qty": qty, "notional": notional, "leverage": leverage,
            "sl": stop_price if stop_price is not None else 0, "tp": 0, "open_time": datetime.utcnow().isoformat(),
            "sltp_orders": {}, "trailing": CONFIG["TRAILING_ENABLED"],
            "dyn_sltp": False, "tp1": None, "tp2": None, "tp3": None,
            "trade_phase": 0, "be_moved": False, "risk_usdt": 0.0,
            "strategy_id": 4,
        }

        await asyncio.to_thread(add_managed_trade_to_db, meta)

        await asyncio.to_thread(cancel_close_orders_sync, symbol)
        
        if stop_price is not None:
            log.info(f"Attempting to place SL for imported trade {symbol}. SL={stop_price}, Qty={qty}")
            # Pass tp_price=None to prevent placing a Take Profit order
            await asyncio.to_thread(place_batch_sl_tp_sync, symbol, side, sl_price=stop_price, tp_price=None, qty=qty)
            
            msg = (f"ℹ️ **Position Imported**\n\n"
                   f"Found and imported a position for **{symbol}**.\n\n"
                   f"**Side:** {side}\n"
                   f"**Entry Price:** {entry_price}\n"
                   f"**Quantity:** {qty}\n\n"
                   f"A default SL has been calculated and placed:\n"
                   f"**SL:** `{round_price(symbol, stop_price)}`\n\n"
                   f"The bot will now manage this trade.")
            await asyncio.to_thread(send_telegram, msg)
        else:
            log.warning(f"No valid SL placed for imported trade {symbol}. Please manage manually.")
            msg = (f"ℹ️ **Position Imported (No SL)**\n\n"
                   f"Found and imported a position for **{symbol}** but could not place a valid SL.\n\n"
                   f"**Please manage this trade manually.**")
            await asyncio.to_thread(send_telegram, msg)

        return trade_id, meta
    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"Failed to import rogue position for {symbol}. Please manage it manually.", e)
        return None

async def reconcile_open_trades():
    global managed_trades
    log.info("--- Starting Trade Reconciliation (with DB data) ---")

    db_trades = {}
    async with managed_trades_lock:
        db_trades = dict(managed_trades)
    
    log.info(f"Found {len(db_trades)} managed trade(s) in DB to reconcile.")

    try:
        if client is None:
            log.warning("Binance client not initialized. Cannot fetch positions for reconciliation.")
            return
        
        positions = await asyncio.to_thread(client.futures_position_information)
        open_positions = {
            pos['symbol']: pos for pos in positions if float(pos.get('positionAmt', 0.0)) != 0.0
        }
        log.info(f"Found {len(open_positions)} open position(s) on Binance.")

    except Exception as e:
        log.exception("Failed to fetch Binance positions during reconciliation.")
        await asyncio.to_thread(send_telegram, f"⚠️ **CRITICAL**: Failed to fetch positions from Binance during startup reconciliation: {e}. The bot may not manage existing trades correctly.")
        managed_trades = {}
        return

    retained_trades = {}
    
    # 1. Reconcile trades that are already in the database
    for trade_id, trade_meta in db_trades.items():
        symbol = trade_meta['symbol']
        if symbol in open_positions:
            log.info(f"✅ Reconciled DB trade: {trade_id} ({symbol}) is active. Restoring.")
            retained_trades[trade_id] = trade_meta
        else:
            log.warning(f"ℹ️ Reconciled DB trade: {trade_id} ({symbol}) is closed on Binance. Archiving.")
            # This part could be enhanced to fetch last trade details for accurate PnL
            await asyncio.to_thread(
                record_trade,
                {
                    'id': trade_id, 'symbol': symbol, 'side': trade_meta['side'],
                    'entry_price': trade_meta['entry_price'], 'exit_price': None, # Exit price is unknown
                    'qty': trade_meta['initial_qty'], 'notional': trade_meta['notional'], 
                    'pnl': 0.0, 'open_time': trade_meta['open_time'], 
                    'close_time': datetime.utcnow().isoformat(),
                    'risk_usdt': trade_meta.get('risk_usdt', 0.0)
                }
            )
            await asyncio.to_thread(remove_managed_trade_from_db, trade_id)

    # 2. Import "rogue" positions that are on the exchange but not in the DB
    managed_symbols = {t['symbol'] for t in retained_trades.values()}
    for symbol, position in open_positions.items():
        if symbol not in managed_symbols:
            result = await _import_rogue_position_async(symbol, position)
            if result:
                trade_id, meta = result
                retained_trades[trade_id] = meta

    async with managed_trades_lock:
        managed_trades.clear()
        managed_trades.update(retained_trades)
    
    log.info(f"--- Reconciliation Complete. {len(managed_trades)} trades are now being managed. ---")


async def check_and_import_rogue_trades():
    """
    Periodically checks for and imports "rogue" positions that exist on the
    exchange but are not managed by the bot.
    """
    global managed_trades, notified_rogue_symbols
    log.info("Checking for rogue positions...")

    try:
        if client is None:
            log.warning("Binance client not initialized. Cannot check for rogue trades.")
            return

        # Get all open positions from the exchange
        positions = await asyncio.to_thread(client.futures_position_information)
        open_positions = {
            pos['symbol']: pos for pos in positions if float(pos.get('positionAmt', 0.0)) != 0.0
        }

        # Get symbols of trades currently managed by the bot
        async with managed_trades_lock:
            managed_symbols = {t['symbol'] for t in managed_trades.values()}
        
        # Determine which open positions are "rogue"
        rogue_symbols = set(open_positions.keys()) - managed_symbols

        if not rogue_symbols:
            log.info("No rogue positions found.")
            return

        for symbol in rogue_symbols:
            if symbol in notified_rogue_symbols:
                log.debug(f"Ignoring already notified rogue symbol: {symbol}")
                continue

            # Mark as notified BEFORE attempting import to prevent spam on repeated failures.
            notified_rogue_symbols.add(symbol)
            position = open_positions[symbol]
            
            result = await _import_rogue_position_async(symbol, position)
            if result:
                trade_id, meta = result
                async with managed_trades_lock:
                    managed_trades[trade_id] = meta
    
    except Exception as e:
        log.exception("An unhandled exception occurred in check_and_import_rogue_trades.")


async def periodic_rogue_check_loop():
    """
    A background task that runs periodically to check for and import rogue trades.
    """
    log.info("Starting periodic rogue position checker loop.")
    while True:
        try:
            # Wait for 1 hour before the next check
            await asyncio.sleep(3600)

            if not running:
                log.debug("Bot is not running, skipping hourly rogue position check.")
                continue
            
            await check_and_import_rogue_trades()

        except asyncio.CancelledError:
            log.info("Periodic rogue position checker loop cancelled.")
            break
        except Exception as e:
            log.exception("An unhandled error occurred in the periodic rogue check loop.")
            # Wait a bit before retrying to avoid spamming errors
            await asyncio.sleep(60)


def load_state_from_db_sync():
    """
    Loads pending orders and managed trades from the SQLite DB into memory on startup.
    """
    global pending_limit_orders, managed_trades
    log.info("--- Loading State from Database ---")
    
    # Load managed trades
    db_trades = load_managed_trades_from_db()
    if db_trades:
        with managed_trades_lock:
            managed_trades.update(db_trades)
        log.info(f"Loaded {len(db_trades)} managed trade(s) from DB.")
    else:
        log.info("No managed trades found in DB.")

    # Load pending orders
    db_orders = load_pending_orders_from_db()
    if db_orders:
        with pending_limit_orders_lock:
            pending_limit_orders.update(db_orders)
        log.info(f"Loaded {len(db_orders)} pending order(s) from DB.")
    else:
        log.info("No pending orders found in DB.")


# -------------------------
# App Lifespan Manager
# -------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global scan_task, telegram_thread, monitor_thread_obj, pnl_monitor_thread_obj, client, monitor_stop_event, main_loop
    log.info("EMA/BB Strategy Bot starting up...")
    
    # --- Startup Logic ---
    init_db()
    
    await asyncio.to_thread(load_state_from_db_sync)

    main_loop = asyncio.get_running_loop()

    ok, err = await asyncio.to_thread(init_binance_client_sync)
    
    if ok:
        await reconcile_open_trades()

    await asyncio.to_thread(validate_and_sanity_check_sync, True)

    # --- Initialize per-symbol evaluation locks ---
    global symbol_evaluation_locks
    for symbol in CONFIG.get("SYMBOLS", []):
        symbol_evaluation_locks[symbol] = asyncio.Lock()
    log.info(f"Initialized evaluation locks for {len(symbol_evaluation_locks)} symbols.")

    # --- Initialize S4 confirmation state ---
    global s4_confirmation_state
    for symbol in CONFIG.get("SYMBOLS", []):
        s4_confirmation_state[symbol] = {
            'buy_sequence_started': False,
            'sell_sequence_started': False,
            'buy_trade_taken': False,
            'sell_trade_taken': False,
        }
    log.info(f"Initialized S4 stateful confirmation state for {len(s4_confirmation_state)} symbols.")

    if client is not None:
        scan_task = main_loop.create_task(scanning_loop())
        monitor_stop_event.clear()
        monitor_thread_obj = threading.Thread(target=monitor_thread_func, daemon=True)
        monitor_thread_obj.start()
        log.info("Started monitor thread.")

        pnl_monitor_thread_obj = threading.Thread(target=daily_pnl_monitor_thread_func, daemon=True)
        pnl_monitor_thread_obj.start()
        log.info("Started daily PnL monitor thread.")

        maintenance_thread_obj = threading.Thread(target=monthly_maintenance_thread_func, daemon=True)
        maintenance_thread_obj.start()
        log.info("Started monthly maintenance thread.")

        alerter_thread_obj = threading.Thread(target=performance_alerter_thread_func, daemon=True)
        alerter_thread_obj.start()
        log.info("Started performance alerter thread.")

        # Start S5-specific trailing/management thread
        global s5_monitor_thread_obj
        s5_monitor_thread_obj = threading.Thread(target=s5_monitor_thread_func, daemon=True)
        s5_monitor_thread_obj.start()
        log.info("Started S5 management thread.")
    else:
        log.warning("Binance client not initialized -> scanning and monitor threads not started.")

    if telegram_bot:
        telegram_thread = threading.Thread(target=telegram_polling_thread, args=(main_loop,), daemon=True)
        telegram_thread.start()
        log.info("Started telegram polling thread.")
    else:
        log.info("Telegram not configured; telegram thread not started.")
    
    try:
        await asyncio.to_thread(send_telegram, "EMA/BB Strategy Bot started. Running={}".format(running))
    except Exception:
        log.exception("Failed to send startup telegram")

    yield

    # --- Shutdown Logic ---
    log.info("EMA/BB Strategy Bot shutting down...")
    if scan_task:
        scan_task.cancel()
        try:
            await scan_task
        except asyncio.CancelledError:
            log.info("Scanning loop task cancelled successfully.")

    if rogue_check_task:
        rogue_check_task.cancel()
        try:
            await rogue_check_task
        except asyncio.CancelledError:
            log.info("Rogue position checker task cancelled successfully.")

    monitor_stop_event.set()
    if monitor_thread_obj and monitor_thread_obj.is_alive():
        monitor_thread_obj.join(timeout=5)
    if pnl_monitor_thread_obj and pnl_monitor_thread_obj.is_alive():
        pnl_monitor_thread_obj.join(timeout=5)

    if s5_monitor_thread_obj and s5_monitor_thread_obj.is_alive():
        s5_monitor_thread_obj.join(timeout=5)
    
    if telegram_thread and telegram_thread.is_alive():
        # The telegram thread is daemon, so it will exit automatically.
        # We already set the monitor_stop_event which the telegram thread also checks.
        pass

    try:
        await asyncio.to_thread(send_telegram, "EMA/BB Strategy Bot shut down.")
    except Exception:
        pass
    log.info("Shutdown complete.")


app = FastAPI(lifespan=lifespan)

# -------------------------
# Utilities
# -------------------------
def _shorten_for_telegram(text: str, max_len: int = 3500) -> str:
    if not isinstance(text, str):
        text = str(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 200] + "\n\n[...] (truncated)\n\n" + text[-200:]


def format_timedelta(td) -> str:
    """Formats a timedelta object into a human-readable string."""
    from datetime import timedelta
    if not isinstance(td, timedelta) or td.total_seconds() < 0:
        return "N/A"

    seconds = int(td.total_seconds())
    days, seconds = divmod(seconds, 86400)
    hours, seconds = divmod(seconds, 3600)
    minutes, seconds = divmod(seconds, 60)

    parts = []
    if days > 0:
        parts.append(f"{days} day" + ("s" if days != 1 else ""))
    if hours > 0:
        parts.append(f"{hours} hour" + ("s" if hours != 1 else ""))
    if minutes > 0:
        parts.append(f"{minutes} minute" + ("s" if minutes != 1 else ""))
    if seconds > 0 or not parts:
        parts.append(f"{seconds} second" + ("s" if seconds != 1 else ""))

    return ", ".join(parts)


def get_public_ip() -> str:
    try:
        return requests.get("https://api.ipify.org", timeout=5).text
    except Exception:
        return "unable-to-fetch-ip"

def default_sl_tp_for_import(symbol: str, entry_price: float, side: str) -> tuple[float, float, float]:
    # Fetch klines, enough for S4 indicators
    df = fetch_klines_sync(symbol, CONFIG["TIMEFRAME"], 250)
    if df is None or df.empty:
        raise RuntimeError("No kline data to calc default SL/TP")

    # Calculate S4 Supertrend (ST2 - middle one) to determine the SL
    s4_params = CONFIG['STRATEGY_4']
    df['s4_st2'], _ = supertrend(df, period=s4_params['ST2_PERIOD'], multiplier=s4_params['ST2_MULT'])

    # The stop price is the most recent Supertrend value
    stop_price = safe_last(df['s4_st2'])
    current_price = safe_last(df['close'])

    # We are no longer placing a TP, but the function signature requires returning two values.
    # The calling function (_import_rogue_position_async) ignores the second value.
    take_price = 0.0

    return stop_price, take_price, current_price

def timeframe_to_timedelta(tf: str) -> Optional[timedelta]:
    """Converts a timeframe string like '1m', '5m', '1h', '1d' to a timedelta object."""
    match = re.match(r'(\d+)([mhd])', tf)
    if not match:
        return None
    val, unit = match.groups()
    val = int(val)
    if unit == 'm':
        return timedelta(minutes=val)
    elif unit == 'h':
        return timedelta(hours=val)
    elif unit == 'd':
        return timedelta(days=val)
    return None

def send_telegram(msg: str, document_content: Optional[bytes] = None, document_name: str = "error.html", parse_mode: Optional[str] = None):
    """
    Synchronously sends a message to Telegram. Can optionally attach a document.
    This is a blocking call.
    """
    if not telegram_bot or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured; message: %s", msg[:200])
        return
    
    safe_msg = _shorten_for_telegram(msg)
    try:
        if document_content:
            doc_stream = io.BytesIO(document_content)
            doc_stream.name = document_name
            telegram_bot.send_document(
                chat_id=int(TELEGRAM_CHAT_ID),
                document=doc_stream,
                caption=safe_msg,
                timeout=30,
                parse_mode=parse_mode
            )
        else:
            telegram_bot.send_message(
                chat_id=int(TELEGRAM_CHAT_ID), 
                text=safe_msg,
                timeout=30,
                parse_mode=parse_mode
            )
    except Exception:
        log.exception("Failed to send telegram message")


def log_and_send_error(context_msg: str, exc: Optional[Exception] = None):
    """
    Logs an exception and sends a formatted error message to Telegram.
    This is a synchronous, blocking call.
    """
    # Log the full traceback to the console/log file
    if exc:
        log.exception(f"Error during '{context_msg}': {exc}")
    else:
        log.error(f"Error during '{context_msg}' (no exception details).")

    # For Binance API exceptions, extract more details
    if exc and isinstance(exc, BinanceAPIException):
        error_details = f"Code: {exc.code}, Message: {exc.message}"
    elif exc:
        error_details = str(exc)
    else:
        error_details = "N/A"

    # Consolidate dynamic content into a single block to avoid parsing errors
    details_text = (
        f"Context: {context_msg}\n"
        f"Error Type: {type(exc).__name__ if exc else 'N/A'}\n"
        f"Details: {error_details}"
    )

    telegram_msg = (
        f"🚨 **Bot Error** 🚨\n\n"
        f"```\n{details_text}\n```\n\n"
        f"Check the logs for the full traceback if available."
    )
    
    # Send the message, using Markdown for formatting
    send_telegram(telegram_msg, parse_mode='Markdown')


def _record_rejection(symbol: str, reason: str, details: dict, signal_candle: Optional[pd.Series] = None):
    """Adds a rejected trade event to the deque and persists it to a file."""
    global rejected_trades

    # Enrich details with key indicator values from the signal candle if available
    if signal_candle is not None:
        indicator_keys = ['close', 'rsi', 'adx', 's1_bbu', 's1_bbl', 's2_st', 's4_st1', 's4_st2', 's4_st3']
        for key in indicator_keys:
            if key in signal_candle and pd.notna(signal_candle[key]):
                details[key] = signal_candle[key]

    # Format floats in details to a reasonable precision for display
    formatted_details = {k: f"{v:.4f}" if isinstance(v, float) else v for k, v in details.items()}
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "reason": reason,
        "details": formatted_details
    }
    
    # 1. Append to in-memory deque
    rejected_trades.append(record)
    
    # 2. Persist to file
    try:
        with open("rejections.jsonl", "a") as f:
            f.write(json.dumps(record) + "\n")
    except Exception as e:
        log.error(f"Failed to write rejection to file: {e}")

    # Use info level for rejection logs to make them visible.
    log.info(f"Rejected trade for {symbol}. Reason: {reason}, Details: {formatted_details}")

    # --- Conditional Telegram Notification ---
    if CONFIG.get("TELEGRAM_NOTIFY_REJECTIONS", False):
        details_str = ", ".join([f"{k}: {v}" for k, v in formatted_details.items()])
        msg = (
            f"🚫 Trade Rejected\n\n"
            f"Symbol: {symbol}\n"
            f"Reason: {reason}\n"
            f"Details: {details_str}"
        )
        send_telegram(msg)


def handle_reject_cmd():
    """Formats the last 20 in-memory rejections for Telegram."""
    global rejected_trades
    if not rejected_trades:
        send_telegram("No rejected trades have been recorded in memory since the last restart.")
        return

    report_lines = ["*Last 20 Rejected Trades (from memory)*"]
    # The deque stores the most recent items, so we iterate in reverse to show newest first.
    for reject in reversed(list(rejected_trades)):
        try:
            ts = datetime.fromisoformat(reject['timestamp']).strftime('%H:%M:%S')
            details = reject.get('details', {})
            details_str = ", ".join([f"{k}: {v}" for k, v in details.items()])
            
            line_report = (
                f"\n- - - - - - - - - - - - - - - - - -\n"
                f"**Symbol:** `{reject['symbol']}`\n"
                f"**Time:** `{ts} UTC`\n"
                f"**Reason:** `{reject['reason']}`\n"
                f"**Details:** `{details_str if details else 'N/A'}`"
            )
            report_lines.append(line_report)
        except (KeyError) as e:
            log.warning(f"Could not parse rejection record: {reject}. Error: {e}")
    
    send_telegram("\n".join(report_lines), parse_mode='Markdown')


SESSION_FREEZE_WINDOWS = {
    "London": (7, 9),
    "New York": (12, 14),
    "Tokyo": (23, 1)  # Crosses midnight
}


def get_merged_freeze_intervals() -> list[tuple[datetime, datetime, str]]:
    """
    Calculates and merges all freeze windows for the current and next day.
    This handles overlaps and contiguous sessions, returning a clean list of
    absolute (start_datetime, end_datetime, session_name) intervals.
    """
    from datetime import timedelta

    now_utc = datetime.now(timezone.utc)
    today = now_utc.date()
    tomorrow = today + timedelta(days=1)
    day_after = today + timedelta(days=2)

    intervals = []
    # Get all intervals for today and tomorrow
    for name, (start_hour, end_hour) in SESSION_FREEZE_WINDOWS.items():
        if start_hour < end_hour:  # Same day window
            # Today's window
            intervals.append((
                datetime(today.year, today.month, today.day, start_hour, 0, tzinfo=timezone.utc),
                datetime(today.year, today.month, today.day, end_hour, 0, tzinfo=timezone.utc),
                name
            ))
            # Tomorrow's window
            intervals.append((
                datetime(tomorrow.year, tomorrow.month, tomorrow.day, start_hour, 0, tzinfo=timezone.utc),
                datetime(tomorrow.year, tomorrow.month, tomorrow.day, end_hour, 0, tzinfo=timezone.utc),
                name
            ))
        else:  # Overnight window
            # Today into Tomorrow
            intervals.append((
                datetime(today.year, today.month, today.day, start_hour, 0, tzinfo=timezone.utc),
                datetime(tomorrow.year, tomorrow.month, tomorrow.day, end_hour, 0, tzinfo=timezone.utc),
                name
            ))
            # Tomorrow into Day After
            intervals.append((
                datetime(tomorrow.year, tomorrow.month, tomorrow.day, start_hour, 0, tzinfo=timezone.utc),
                datetime(day_after.year, day_after.month, day_after.day, end_hour, 0, tzinfo=timezone.utc),
                name
            ))

    # Sort intervals by start time
    intervals.sort(key=lambda x: x[0])

    if not intervals:
        return []

    # Merge overlapping intervals
    merged = []
    current_start, current_end, current_names = intervals[0]
    current_names = {current_names}

    for next_start, next_end, next_name in intervals[1:]:
        if next_start <= current_end:
            # Overlap or contiguous, merge them
            current_end = max(current_end, next_end)
            current_names.add(next_name)
        else:
            # No overlap, finish the current merged interval
            merged.append((current_start, current_end, " & ".join(sorted(list(current_names)))))
            # Start a new one
            current_start, current_end, current_names = next_start, next_end, {next_name}

    # Add the last merged interval
    merged.append((current_start, current_end, " & ".join(sorted(list(current_names)))))
    
    # Filter out intervals that have already completely passed
    final_intervals = [m for m in merged if now_utc < m[1]]

    return final_intervals


def get_session_freeze_status(now: datetime) -> tuple[bool, Optional[str]]:
    """
    Checks if the current time is within a session freeze window using the merged intervals.
    Returns a tuple of (is_frozen, session_name).
    """
    merged_intervals = get_merged_freeze_intervals()
    for start, end, name in merged_intervals:
        if start <= now < end:
            return True, name
    return False, None


# (The rest of the file from DB Helpers to the end remains the same, except for removing the old startup/shutdown events)
# ... I will paste the full code below ...

# -------------------------
# DB helpers
# -------------------------
def init_db():
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    # Historical trades table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY,
        symbol TEXT,
        side TEXT,
        entry_price REAL,
        exit_price REAL,
        qty REAL,
        notional REAL,
        risk_usdt REAL,
        pnl REAL,
        open_time TEXT,
        close_time TEXT
    )
    """)
    # Add column if it doesn't exist for backward compatibility
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN risk_usdt REAL")
    except sqlite3.OperationalError as e:
        if "duplicate column name" not in str(e):
            raise
    
    # Add new columns for enhanced reporting
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN entry_reason TEXT")
    except sqlite3.OperationalError: pass # Ignore if column exists
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN exit_reason TEXT")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN tp1 REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN tp2 REAL")
    except sqlite3.OperationalError: pass

    # --- New columns for SuperTrend Strategy ---
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN strategy_id INTEGER")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN signal_confidence REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN adx_confirmation REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN rsi_confirmation REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN macd_confirmation REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE trades ADD COLUMN atr_at_entry REAL")
    except sqlite3.OperationalError: pass

    # Persistent open trades table for crash recovery
    cur.execute("""
    CREATE TABLE IF NOT EXISTS managed_trades (
        id TEXT PRIMARY KEY,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        entry_price REAL NOT NULL,
        initial_qty REAL NOT NULL,
        qty REAL NOT NULL,
        notional REAL NOT NULL,
        leverage INTEGER NOT NULL,
        sl REAL NOT NULL,
        tp REAL NOT NULL,
        open_time TEXT NOT NULL,
        sltp_orders TEXT,
        trailing INTEGER NOT NULL,
        dyn_sltp INTEGER NOT NULL,
        tp1 REAL,
        tp2 REAL,
        tp3 REAL,
        trade_phase INTEGER NOT NULL,
        be_moved INTEGER NOT NULL,
        risk_usdt REAL NOT NULL,
        strategy_id INTEGER,
        atr_at_entry REAL
    )
    """)
    # Add strategy_id for strategy-specific logic in monitor thread
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN strategy_id INTEGER")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN atr_at_entry REAL")
    except sqlite3.OperationalError: pass
    # --- New columns for Strategy 3 ---
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s3_trailing_active INTEGER")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s3_trailing_stop REAL")
    except sqlite3.OperationalError: pass
    # --- New columns for Strategy 4 ---
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s4_trailing_stop REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s4_last_candle_ts TEXT")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s4_trailing_active INTEGER")
    except sqlite3.OperationalError: pass
    # --- New columns for Strategy 5 ---
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s5_trailing_stop REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s5_trailing_active INTEGER")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s5_stage INTEGER")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s5_risk_per_unit REAL")
    except sqlite3.OperationalError: pass
    try:
        cur.execute("ALTER TABLE managed_trades ADD COLUMN s5_partial_done INTEGER")
    except sqlite3.OperationalError: pass

    # Table for symbols that require manual attention
    cur.execute("""
    CREATE TABLE IF NOT EXISTS attention_required (
        symbol TEXT PRIMARY KEY,
        reason TEXT,
        details TEXT,
        timestamp TEXT
    )
    """)

    # --- New table for pending limit orders ---
    cur.execute("""
    CREATE TABLE IF NOT EXISTS pending_limit_orders (
        id TEXT PRIMARY KEY,
        order_id TEXT NOT NULL,
        symbol TEXT NOT NULL,
        side TEXT NOT NULL,
        qty REAL NOT NULL,
        limit_price REAL NOT NULL,
        stop_price REAL NOT NULL,
        take_price REAL NOT NULL,
        leverage INTEGER NOT NULL,
        risk_usdt REAL NOT NULL,
        place_time TEXT NOT NULL,
        expiry_time TEXT,
        strategy_id INTEGER,
        atr_at_entry REAL,
        trailing INTEGER
    )
    """)

    conn.commit()
    conn.close()

    # --- Ensure rejections file exists ---
    try:
        # "touch" the file to ensure it's created on startup if it doesn't exist
        with open("rejections.jsonl", "a"):
            pass
        log.info("Ensured rejections.jsonl file exists.")
    except Exception as e:
        log.error(f"Could not create rejections.jsonl file: {e}")


def add_pending_order_to_db(rec: Dict[str, Any]):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    values = (
        rec.get('id'), rec.get('order_id'), rec.get('symbol'), rec.get('side'),
        rec.get('qty'), rec.get('limit_price'), rec.get('stop_price'), rec.get('take_price'),
        rec.get('leverage'), rec.get('risk_usdt'), rec.get('place_time'), rec.get('expiry_time'),
        rec.get('strategy_id'), rec.get('atr_at_entry'), int(rec.get('trailing', False))
    )
    cur.execute("""
    INSERT OR REPLACE INTO pending_limit_orders (
        id, order_id, symbol, side, qty, limit_price, stop_price, take_price,
        leverage, risk_usdt, place_time, expiry_time, strategy_id, atr_at_entry, trailing
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, values)
    conn.commit()
    conn.close()

def remove_pending_order_from_db(pending_order_id: str):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    cur.execute("DELETE FROM pending_limit_orders WHERE id = ?", (pending_order_id,))
    conn.commit()
    conn.close()

def load_pending_orders_from_db() -> Dict[str, Dict[str, Any]]:
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM pending_limit_orders")
    rows = cur.fetchall()
    conn.close()

    orders = {}
    for row in rows:
        rec = dict(row)
        rec['trailing'] = bool(rec.get('trailing'))
        orders[rec['id']] = rec
    return orders

def record_trade(rec: Dict[str, Any]):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO trades (
        id,symbol,side,entry_price,exit_price,qty,notional,risk_usdt,pnl,
        open_time,close_time,entry_reason,exit_reason,tp1,tp2,
        strategy_id, signal_confidence, adx_confirmation, rsi_confirmation, macd_confirmation,
        atr_at_entry
    )
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        rec.get('id'), rec.get('symbol'), rec.get('side'), rec.get('entry_price'), rec.get('exit_price'),
        rec.get('qty'), rec.get('notional'), rec.get('risk_usdt'), rec.get('pnl'), 
        rec.get('open_time'), rec.get('close_time'), rec.get('entry_reason'), rec.get('exit_reason'),
        rec.get('tp1'), rec.get('tp2'),
        rec.get('strategy_id'), rec.get('signal_confidence'), rec.get('adx_confirmation'),
        rec.get('rsi_confirmation'), rec.get('macd_confirmation'),
        rec.get('atr_at_entry')
    ))
    conn.commit()
    conn.close()

def add_managed_trade_to_db(rec: Dict[str, Any]):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    values = (
        rec['id'], rec['symbol'], rec['side'], rec['entry_price'], rec['initial_qty'],
        rec['qty'], rec['notional'], rec['leverage'], rec['sl'], rec['tp'],
        rec['open_time'], json.dumps(rec.get('sltp_orders')),
        int(rec.get('trailing', False)), int(rec.get('dyn_sltp', False)),
        rec.get('tp1'), rec.get('tp2'), rec.get('tp3'),
        rec.get('trade_phase', 0), int(rec.get('be_moved', False)),
        rec.get('risk_usdt'), rec.get('strategy_id', 1),
        rec.get('atr_at_entry'),
        # S3 specific fields
        int(rec.get('s3_trailing_active', False)),
        rec.get('s3_trailing_stop'),
        # S4 specific fields
        rec.get('s4_trailing_stop'),
        rec.get('s4_last_candle_ts'),
        int(rec.get('s4_trailing_active', False)),
        # S5 specific fields
        rec.get('s5_trailing_stop'),
        int(rec.get('s5_trailing_active', False)),
        int(rec.get('s5_stage', 0)),
        rec.get('s5_risk_per_unit'),
        int(rec.get('s5_partial_done', False)),
    )
    cur.execute("""
    INSERT OR REPLACE INTO managed_trades (
        id, symbol, side, entry_price, initial_qty, qty, notional,
        leverage, sl, tp, open_time, sltp_orders, trailing, dyn_sltp,
        tp1, tp2, tp3, trade_phase, be_moved, risk_usdt, strategy_id, atr_at_entry,
        s3_trailing_active, s3_trailing_stop, s4_trailing_stop, s4_last_candle_ts,
        s4_trailing_active,
        s5_trailing_stop, s5_trailing_active, s5_stage, s5_risk_per_unit, s5_partial_done
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, values)
    conn.commit()
    conn.close()

def remove_managed_trade_from_db(trade_id: str):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    cur.execute("DELETE FROM managed_trades WHERE id = ?", (trade_id,))
    conn.commit()
    conn.close()

def mark_attention_required_sync(symbol: str, reason: str, details: str):
    """Adds or updates an attention required flag for a symbol in the database."""
    try:
        conn = sqlite3.connect(CONFIG["DB_FILE"])
        cur = conn.cursor()
        cur.execute("INSERT OR REPLACE INTO attention_required (symbol, reason, details, timestamp) VALUES (?, ?, ?, ?)",
                    (symbol, reason, details, datetime.utcnow().isoformat()))
        conn.commit()
        conn.close()
        log.info(f"Marked '{symbol}' for attention. Reason: {reason}")
    except Exception as e:
        log.exception(f"Failed to mark attention for {symbol}: {e}")

def prune_trades_db(year: int, month: int):
    """Deletes all trades from the database for a specific month."""
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    
    start_date = f"{year}-{month:02d}-01"
    next_month_val = month + 1
    next_year_val = year
    if next_month_val > 12:
        next_month_val = 1
        next_year_val += 1
    end_date = f"{next_year_val}-{next_month_val:02d}-01"

    log.info(f"Pruning trades in DB from {start_date} up to {end_date}")
    try:
        cur.execute("DELETE FROM trades WHERE close_time >= ? AND close_time < ?", (start_date, end_date))
        conn.commit()
        count = cur.rowcount
        log.info(f"Successfully pruned {count} trades from the database.")
        if count > 0:
            send_telegram(f"🧹 Database Maintenance: Pruned {count} old trade records from {year}-{month:02d}.")
    except Exception as e:
        log.exception(f"Failed to prune trades from DB for {year}-{month:02d}")
        send_telegram(f"⚠️ Failed to prune old database records for {year}-{month:02d}. Please check logs.")
    finally:
        conn.close()

def load_managed_trades_from_db() -> Dict[str, Dict[str, Any]]:
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()
    cur.execute("SELECT * FROM managed_trades")
    rows = cur.fetchall()
    conn.close()

    trades = {}
    for row in rows:
        rec = dict(row)
        rec['sltp_orders'] = json.loads(rec.get('sltp_orders', '{}') or '{}')
        rec['trailing'] = bool(rec.get('trailing'))
        rec['dyn_sltp'] = bool(rec.get('dyn_sltp'))
        rec['be_moved'] = bool(rec.get('be_moved'))
        rec['s3_trailing_active'] = bool(rec.get('s3_trailing_active'))
        rec['s4_trailing_active'] = bool(rec.get('s4_trailing_active'))
        rec['s5_trailing_active'] = bool(rec.get('s5_trailing_active'))
        rec['s5_partial_done'] = bool(rec.get('s5_partial_done'))
        trades[rec['id']] = rec
    return trades

# -------------------------
# Indicators
# -------------------------
def atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df['high']; low = df['low']; close = df['close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(length, min_periods=1).mean()

def atr_wilder(df: pd.DataFrame, length: int) -> pd.Series:
    """Calculates the Average True Range (ATR) using Wilder's smoothing."""
    high = df['high']; low = df['low']; close = df['close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    # Wilder's smoothing is an EMA with alpha = 1/length
    return tr.ewm(alpha=1/length, adjust=False).mean()

def hhv(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Highest High Value over a given period."""
    return series.rolling(window=length).max()

def llv(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Lowest Low Value over a given period."""
    return series.rolling(window=length).min()


# --- New Strategy Indicators ---

def sma(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Simple Moving Average (SMA)."""
    return series.rolling(window=length).mean()

def ema(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Exponential Moving Average (EMA)."""
    return series.ewm(span=length, adjust=False).mean()

def rsi(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Relative Strength Index (RSI)."""
    delta = series.diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=length).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=length).mean()
    
    # Avoid division by zero
    rs = gain / loss
    rs = rs.replace([np.inf, -np.inf], np.nan).fillna(0)
    
    return 100 - (100 / (1 + rs))

def bollinger_bands(series: pd.Series, length: int, std: float) -> tuple[pd.Series, pd.Series]:
    """Calculates Bollinger Bands."""
    ma = series.rolling(window=length).mean()
    std_dev = series.rolling(window=length).std()
    upper_band = ma + (std_dev * std)
    lower_band = ma - (std_dev * std)
    return upper_band, lower_band


def safe_latest_atr_from_df(df: Optional[pd.DataFrame]) -> float:
    """Return the latest ATR value from df or 0.0 if df is None/empty or ATR can't be computed."""
    try:
        if df is None or getattr(df, 'empty', True):
            return 0.0
        atr_series = atr(df, CONFIG.get("ATR_LENGTH", 14))
        if atr_series is None or atr_series.empty:
            return 0.0
        return float(atr_series.iloc[-1])
    except Exception:
        log.exception("safe_latest_atr_from_df failed; returning 0.0")
        return 0.0


def safe_last(series: pd.Series, default=0.0) -> float:
    """Safely get the last value of a series, returning a default if it's empty or the value is NaN."""
    if series is None or series.empty:
        return float(default)
    last_val = series.iloc[-1]
    if pd.isna(last_val):
        return float(default)
    return float(last_val)


def adx(df: pd.DataFrame, period: int = 14):
    """
    Calculates ADX, +DI, and -DI and adds them to the DataFrame.
    """
    high = df['high']
    low = df['low']
    close = df['close']

    # Calculate +DM, -DM and TR
    plus_dm = high.diff()
    minus_dm = low.diff().mul(-1)
    
    plus_dm[plus_dm < 0] = 0
    plus_dm[plus_dm < minus_dm] = 0
    
    minus_dm[minus_dm < 0] = 0
    minus_dm[minus_dm < plus_dm] = 0

    tr = pd.concat([high - low, (high - close.shift(1)).abs(), (low - close.shift(1)).abs()], axis=1).max(axis=1)

    # Smoothed values using Wilder's smoothing (approximated by EMA with alpha=1/period)
    alpha = 1 / period
    atr_smooth = tr.ewm(alpha=alpha, adjust=False).mean()
    
    # To avoid division by zero
    atr_smooth.replace(0, 1e-10, inplace=True)

    df['+DI'] = 100 * (plus_dm.ewm(alpha=alpha, adjust=False).mean() / atr_smooth)
    df['-DI'] = 100 * (minus_dm.ewm(alpha=alpha, adjust=False).mean() / atr_smooth)
    
    dx_denominator = (df['+DI'] + df['-DI']).replace(0, 1e-10)
    dx = 100 * (abs(df['+DI'] - df['-DI']) / dx_denominator)
    df['adx'] = dx.ewm(alpha=alpha, adjust=False).mean()


def supertrend(df: pd.DataFrame, period: int = 10, multiplier: float = 3.0, atr_series: Optional[pd.Series] = None, source: Optional[pd.Series] = None) -> tuple[pd.Series, pd.Series]:
    """
    Calculates the SuperTrend indicator using the pandas-ta library.
    Returns two series: supertrend and supertrend_direction.
    """
    # Calculate SuperTrend using pandas_ta.
    # It returns a DataFrame with columns like 'SUPERT_10_3.0', 'SUPERTd_10_3.0', etc.
    st_df = df.ta.supertrend(period=period, multiplier=multiplier)

    if st_df is None or st_df.empty:
        log.error(f"Pandas-TA failed to generate SuperTrend for period={period}, mult={multiplier}.")
        return pd.Series(dtype='float64', index=df.index), pd.Series(dtype='float64', index=df.index)

    # --- Robustly find column names to avoid fragility with float formatting ---
    # The main supertrend line, e.g., 'SUPERT_7_3.0'
    supertrend_col = next((col for col in st_df.columns if col.startswith('SUPERT_') and 'd' not in col and 'l' not in col and 's' not in col), None)
    # The direction column, e.g., 'SUPERTd_7_3.0'
    direction_col = next((col for col in st_df.columns if col.startswith('SUPERTd_')), None)

    # Check if the expected columns were found
    if supertrend_col is None or direction_col is None:
        log.error(f"Pandas-TA did not generate expected SuperTrend columns for period={period}, mult={multiplier}. Got columns: {st_df.columns}")
        return pd.Series(dtype='float64', index=df.index), pd.Series(dtype='float64', index=df.index)

    # Extract the required series.
    st_series = st_df[supertrend_col]
    st_dir_series = st_df[direction_col]
    
    return st_series, st_dir_series


def macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9):
    """
    Calculates the MACD and adds 'MACD', 'MACD_Signal', and 'MACD_Hist' columns to the DataFrame.
    """
    series = df['close']
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    df['MACD'] = ema_fast - ema_slow
    df['MACD_Signal'] = df['MACD'].ewm(span=signal, adjust=False).mean()
    df['MACD_Hist'] = df['MACD'] - df['MACD_Signal']


def dema(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Double Exponential Moving Average (DEMA)."""
    ema1 = series.ewm(span=length, adjust=False).mean()
    ema2 = ema1.ewm(span=length, adjust=False).mean()
    return 2 * ema1 - ema2


def candle_body_crosses_dema(candle: pd.Series, dema_value: float) -> bool:
    """Checks if a candle's body (open to close) crosses a DEMA value."""
    if pd.isna(dema_value) or 'open' not in candle or 'close' not in candle:
        return False
    candle_open = float(candle['open'])
    candle_close = float(candle['close'])
    return min(candle_open, candle_close) < dema_value < max(candle_open, candle_close)

# --- Pivot / Swing Utilities (for S5) ---

def last_pivot_low(df: pd.DataFrame, lookback: int = 3, max_lookback_bars: int = 50) -> tuple[Optional[float], Optional[pd.Timestamp]]:
    """
    Returns the most recent pivot (swing) low price and its timestamp using a simple fractal:
    low[i] is the minimum over window [i - lookback, i + lookback].
    Scans back up to max_lookback_bars from the last CLOSED candle (-2 index).
    """
    try:
        n = len(df)
        if n < (2 * lookback + 3):
            return None, None
        # work on closed candles only, ignore the forming candle at -1
        end_idx = n - 2
        start_idx = max(lookback, end_idx - max_lookback_bars)
        lows = df['low']
        for i in range(end_idx, start_idx - 1, -1):
            left = max(0, i - lookback)
            right = min(n - 1, i + lookback)
            window = lows.iloc[left:right+1]
            if len(window) >= (2 * lookback + 1) and lows.iloc[i] == window.min():
                return float(lows.iloc[i]), df.index[i]
        return None, None
    except Exception:
        log.exception("last_pivot_low failed")
        return None, None

def last_pivot_high(df: pd.DataFrame, lookback: int = 3, max_lookback_bars: int = 50) -> tuple[Optional[float], Optional[pd.Timestamp]]:
    """
    Returns the most recent pivot (swing) high price and its timestamp using a simple fractal:
    high[i] is the maximum over window [i - lookback, i + lookback].
    """
    try:
        n = len(df)
        if n < (2 * lookback + 3):
            return None, None
        end_idx = n - 2
        start_idx = max(lookback, end_idx - max_lookback_bars)
        highs = df['high']
        for i in range(end_idx, start_idx - 1, -1):
            left = max(0, i - lookback)
            right = min(n - 1, i + lookback)
            window = highs.iloc[left:right+1]
            if len(window) >= (2 * lookback + 1) and highs.iloc[i] == window.max():
                return float(highs.iloc[i]), df.index[i]
        return None, None
    except Exception:
        log.exception("last_pivot_high failed")
        return None, None


# -------------------------
# Strategy 5 (MTF Trend/Structure) - Evaluation
# -------------------------
async def evaluate_strategy_5(symbol: str):
    """
    Advanced crypto-futures strategy (S5): MTF trend + structure + orderflow confluence with ATR/pivot trailing.
    - H1 trend filter with EMA(21/55) and SuperTrend(10,3)
    - M15 execution with EMA(21/55), ATR(14), RSI(14), volume spike
    - Entry: limit at M15 EMA21 retest or market on validated liquidity sweep + reclaim
    - Initial SL: structure/ATR hybrid with buffer
    - Sizing: fixed RISK_USD (same mode as S4)
    - Management: partial at 1R, BE at 0.5R (+0.25*ATR), ATR+pivot trailing
    """
    global s5_daily_trade_counter, symbol_trade_cooldown, managed_trades, pending_limit_orders

    try:
        s5 = CONFIG['STRATEGY_5']

        # Restrict to liquid symbols by default
        allowed_symbols = ["BTCUSDT", "ETHUSDT"]
        if symbol not in allowed_symbols:
            _record_rejection(symbol, "S5 Symbol Restricted", {"symbol": symbol})
            return

        # Daily trade limiter per symbol
        today = datetime.now(timezone.utc).strftime('%Y-%m-%d')
        ctr = s5_daily_trade_counter.get(symbol)
        if not ctr or ctr.get('date') != today:
            s5_daily_trade_counter[symbol] = {'date': today, 'count': 0}
        if s5_daily_trade_counter[symbol]['count'] >= s5.get('MAX_TRADES_PER_DAY', 2):
            _record_rejection(symbol, "S5 Daily Trade Limit", {"count": s5_daily_trade_counter[symbol]['count'], "max": s5.get('MAX_TRADES_PER_DAY', 2)})
            return

        # Fetch required MTF data
        df_h1 = await asyncio.to_thread(fetch_klines_sync, symbol, '1h', 400)
        df_m15 = await asyncio.to_thread(fetch_klines_sync, symbol, '15m', 500)
        if df_h1 is None or df_h1.empty or df_m15 is None or df_m15.empty:
            _record_rejection(symbol, "S5 No Data", {"h1_empty": df_h1 is None or df_h1.empty, "m15_empty": df_m15 is None or df_m15.empty})
            return

        # H1 trend filter
        df_h1['s5_h1_ema_s'] = ema(df_h1['close'], s5['H1_EMA_SHORT'])
        df_h1['s5_h1_ema_l'] = ema(df_h1['close'], s5['H1_EMA_LONG'])
        df_h1['s5_h1_st'], df_h1['s5_h1_st_dir'] = supertrend(df_h1, period=s5['H1_ST_PERIOD'], multiplier=s5['H1_ST_MULT'])

        h1_sig = df_h1.iloc[-2]
        if any(pd.isna(h1_sig.get(col)) for col in ['s5_h1_ema_s', 's5_h1_ema_l', 's5_h1_st', 'close']):
            _record_rejection(symbol, "S5 H1 indicators not ready", {})
            return

        bullish_trend = (h1_sig['s5_h1_ema_s'] > h1_sig['s5_h1_ema_l']) and (h1_sig['close'] > h1_sig['s5_h1_st'])
        bearish_trend = (h1_sig['s5_h1_ema_s'] < h1_sig['s5_h1_ema_l']) and (h1_sig['close'] < h1_sig['s5_h1_st'])
        if not (bullish_trend or bearish_trend):
            _record_rejection(symbol, "S5 H1 Trend Filter Failed", {"ema_s": h1_sig['s5_h1_ema_s'], "ema_l": h1_sig['s5_h1_ema_l'], "st": h1_sig['s5_h1_st'], "close": h1_sig['close']})
            return
        side = 'BUY' if bullish_trend else 'SELL'

        # M15 execution indicators
        df_m15['s5_m15_ema_s'] = ema(df_m15['close'], s5['M15_EMA_SHORT'])
        df_m15['s5_m15_ema_l'] = ema(df_m15['close'], s5['M15_EMA_LONG'])
        df_m15['s5_m15_atr'] = atr(df_m15, s5['ATR_PERIOD'])
        df_m15['s5_m15_rsi'] = rsi(df_m15['close'], s5['RSI_PERIOD'])

        if len(df_m15) < 30:
            _record_rejection(symbol, "S5 Not enough M15 bars", {"len": len(df_m15)})
            return

        sig = df_m15.iloc[-2]  # last closed M15 candle
        prev = df_m15.iloc[-3]

        required_m15 = ['s5_m15_ema_s', 's5_m15_ema_l', 's5_m15_atr', 's5_m15_rsi', 'volume', 'close', 'low', 'high']
        if any(pd.isna(sig.get(col)) for col in required_m15):
            _record_rejection(symbol, "S5 M15 indicators not ready", {})
            return

        atr_val = float(sig['s5_m15_atr'])
        price_now = float(sig['close'])
        if atr_val <= 0 or price_now <= 0:
            _record_rejection(symbol, "S5 Invalid ATR/price", {"atr": atr_val, "price": price_now})
            return

        vol_ratio = atr_val / price_now
        if vol_ratio < s5['MIN_ATR_PCT'] or vol_ratio > s5['MAX_ATR_PCT']:
            _record_rejection(symbol, "S5 Volatility Filter", {"atr_pct": vol_ratio, "min": s5['MIN_ATR_PCT'], "max": s5['MAX_ATR_PCT']})
            return

        # Volume spike vs 10-bar average (exclude last two to avoid forward leak)
        vol_avg10 = float(df_m15['volume'].iloc[-12:-2].mean()) if len(df_m15) >= 12 else float(df_m15['volume'].mean())
        volume_ok = float(sig['volume']) >= s5['VOL_SPIKE_MULT'] * max(vol_avg10, 1e-9)

        # Momentum/trend alignment on M15
        if side == 'BUY':
            trend_ok = sig['close'] >= sig['s5_m15_ema_s'] >= sig['s5_m15_ema_l']
            rsi_ok = float(sig['s5_m15_rsi']) <= 70.0
        else:
            trend_ok = sig['close'] <= sig['s5_m15_ema_s'] <= sig['s5_m15_ema_l']
            rsi_ok = float(sig['s5_m15_rsi']) >= 30.0

        if not (trend_ok and rsi_ok and volume_ok):
            _record_rejection(symbol, "S5 M15 Filters Failed", {"trend_ok": trend_ok, "rsi_ok": rsi_ok, "volume_ok": volume_ok})
            return

        # Structure / liquidity sweep check
        sweep_reclaim = False
        swing_price = None

        if side == 'BUY':
            swing_price, _ = last_pivot_low(df_m15, lookback=3)
            if swing_price is not None:
                # A sweep if the previous candle wicks below swing and closes back above it with volume
                if prev['low'] <= swing_price and prev['close'] > swing_price and float(prev['volume']) >= s5['VOL_SPIKE_MULT'] * max(vol_avg10, 1e-9):
                    sweep_reclaim = True
        else:  # SELL
            swing_price, _ = last_pivot_high(df_m15, lookback=3)
            if swing_price is not None:
                if prev['high'] >= swing_price and prev['close'] < swing_price and float(prev['volume']) >= s5['VOL_SPIKE_MULT'] * max(vol_avg10, 1e-9):
                    sweep_reclaim = True

        # Entry mode and price
        entry_mode = 'MARKET' if sweep_reclaim else 'LIMIT'
        if entry_mode == 'MARKET':
            entry_price = float(df_m15.iloc[-1]['open'])
        else:
            # Limit at EMA21 retest
            entry_price = float(sig['s5_m15_ema_s'])

        # Initial stop (structure + ATR hybrid with min distance and buffer)
        stop_buffer = s5['STOP_BUFFER_PCT']
        if side == 'BUY':
            sl_structure = swing_price if swing_price is not None else float(sig['low'])
            sl_by_atr = entry_price - s5['STOP_ATR_MULT'] * atr_val
            sl_candidate = min(sl_structure, sl_by_atr)
            min_dist = s5['STOP_MIN_ATR_MULT'] * atr_val
            if (entry_price - sl_candidate) < min_dist:
                sl_candidate = entry_price - min_dist
            sl_price = sl_candidate * (1 - stop_buffer)
        else:
            sl_structure = swing_price if swing_price is not None else float(sig['high'])
            sl_by_atr = entry_price + s5['STOP_ATR_MULT'] * atr_val
            sl_candidate = max(sl_structure, sl_by_atr)
            min_dist = s5['STOP_MIN_ATR_MULT'] * atr_val
            if (sl_candidate - entry_price) < min_dist:
                sl_candidate = entry_price + min_dist
            sl_price = sl_candidate * (1 + stop_buffer)

        price_distance = abs(entry_price - sl_price)
        if price_distance <= 0:
            _record_rejection(symbol, "S5 Invalid SL distance", {"entry": entry_price, "sl": sl_price})
            return

        # Sizing (same mode as S4: fixed risk_usd)
        risk_usd = float(s5['RISK_USD'])
        ideal_qty_unrounded = risk_usd / price_distance
        ideal_qty = await asyncio.to_thread(round_qty, symbol, ideal_qty_unrounded, rounding=ROUND_DOWN)

        # Enforce min notional
        min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
        qty_min = min_notional / entry_price if entry_price > 0 else 0.0
        qty_min = await asyncio.to_thread(round_qty, symbol, qty_min, rounding=ROUND_CEILING)
        final_qty = max(ideal_qty, qty_min)

        if final_qty <= 0:
            _record_rejection(symbol, "S5 Qty zero after sizing", {"ideal": ideal_qty, "min": qty_min})
            return

        notional = final_qty * entry_price
        balance = await asyncio.to_thread(get_account_balance_usdt)
        actual_risk_usdt = final_qty * price_distance
        margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else actual_risk_usdt

        uncapped_leverage = int(math.ceil(notional / max(margin_to_use, 1e-9)))
        max_leverage_exchange = get_max_leverage(symbol)
        max_leverage_config = CONFIG.get("MAX_BOT_LEVERAGE", 30)
        leverage = max(1, min(uncapped_leverage, min(max_leverage_config, max_leverage_exchange)))

        # Place order
        if entry_mode == 'LIMIT':
            limit_resp = await asyncio.to_thread(place_limit_order_sync, symbol, side, final_qty, entry_price)
            order_id = str(limit_resp.get('orderId'))
            pending_order_id = f"{symbol}_{order_id}"

            candle_duration = timeframe_to_timedelta("15m")
            expiry_candles = CONFIG.get("ORDER_EXPIRY_CANDLES", 2)
            last_idx_time = df_m15.index[-1]
            expiry_time = last_idx_time + (candle_duration * (expiry_candles - 1)) if candle_duration else last_idx_time

            pending_meta = {
                "id": pending_order_id, "order_id": order_id, "symbol": symbol,
                "side": side, "qty": final_qty, "limit_price": entry_price,
                "stop_price": sl_price, "take_price": 0.0, "leverage": leverage,
                "risk_usdt": actual_risk_usdt, "place_time": datetime.utcnow().isoformat(),
                "expiry_time": expiry_time.isoformat(),
                "strategy_id": 5,
                "atr_at_entry": atr_val,
                "trailing": CONFIG["TRAILING_ENABLED"],
            }

            async with pending_limit_orders_lock:
                pending_limit_orders[pending_order_id] = pending_meta
                await asyncio.to_thread(add_pending_order_to_db, pending_meta)

            s5_daily_trade_counter[symbol]['count'] += 1

            msg = (
                f"⏳ New Pending Order: S5-MTF\n\n"
                f"Symbol: {symbol}\n"
                f"Side: {side}\n"
                f"Entry (limit): {entry_price:.4f}\n"
                f"Qty: {final_qty}\n"
                f"Risk: {actual_risk_usdt:.2f} USDT\n"
                f"Leverage: {leverage}x"
            )
            await asyncio.to_thread(send_telegram, msg)
            return

        # MARKET entry on sweep + reclaim
        await asyncio.to_thread(open_market_position_sync, symbol, side, final_qty, leverage)

        # Poll for position
        pos = None
        for i in range(10):
            await asyncio.sleep(0.5)
            positions = await asyncio.to_thread(client.futures_position_information, symbol=symbol)
            position_side = 'LONG' if side == 'BUY' else 'SHORT'
            pos = next((p for p in positions if p.get('positionSide') == position_side and float(p.get('positionAmt', 0)) != 0), None)
            if pos:
                break
        if not pos:
            raise RuntimeError(f"S5: Position for {symbol} not found after market order (waited 5s).")

        actual_entry_price = float(pos['entryPrice'])
        actual_qty = abs(float(pos['positionAmt']))

        # Place SL only (no TP)
        sltp_orders = await asyncio.to_thread(place_batch_sl_tp_sync, symbol, side, sl_price=sl_price, qty=actual_qty)

        trade_id = f"{symbol}_S5_{int(time.time())}"
        meta = {
            "id": trade_id, "symbol": symbol, "side": side, "entry_price": actual_entry_price,
            "initial_qty": actual_qty, "qty": actual_qty, "notional": actual_qty * actual_entry_price,
            "leverage": leverage, "sl": sl_price, "tp": 0,
            "open_time": datetime.utcnow().isoformat(), "sltp_orders": sltp_orders,
            "risk_usdt": actual_risk_usdt, "strategy_id": 5,
            "atr_at_entry": atr_val,
            # S5 fields
            "s5_risk_per_unit": abs(actual_entry_price - sl_price),
            "s5_stage": 0,
            "s5_trailing_active": False,
            "s5_trailing_stop": sl_price,
            "s5_partial_done": False,
            "be_moved": False,
            "trailing": CONFIG["TRAILING_ENABLED"],
        }

        async with managed_trades_lock:
            managed_trades[trade_id] = meta
        await asyncio.to_thread(add_managed_trade_to_db, meta)

        s5_daily_trade_counter[symbol]['count'] += 1
        # Add post-trade cooldown
        symbol_trade_cooldown[symbol] = datetime.now(timezone.utc) + timedelta(minutes=16)

        msg = (
            f"✅ New Trade Opened: S5-MTF\n\n"
            f"Symbol: {symbol}\n"
            f"Side: {side}\n"
            f"Entry: {actual_entry_price:.4f}\n"
            f"Initial SL: {sl_price:.4f}\n"
            f"Risk: {actual_risk_usdt:.2f} USDT\n"
            f"Leverage: {leverage}x"
        )
        await asyncio.to_thread(send_telegram, msg)

    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"Failed to evaluate/execute S5 trade for {symbol}", e)

# -------------------------
# Binance Init
# -------------------------

def init_binance_client_sync():
    """
    Initialize Binance client only when API key + secret are provided.
    Returns (ok: bool, error_message: str)
    """
    global client, BINANCE_API_KEY, BINANCE_API_SECRET, IS_HEDGE_MODE
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        log.warning("Binance API key/secret not set; Binance client will not be initialized.")
        client = None
        return False, "Missing BINANCE_API_KEY or BINANCE_API_SECRET"

    try:
        requests_params = {"timeout": 60}
        client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params=requests_params)

        # --- Configure robust session with retries on the client's existing session ---
        session = client.session
        retry_strategy = Retry(
            total=5,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST", "DELETE"],
            raise_on_status=False
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        log.info("Binance client in MAINNET mode (forced) with retry logic.")
        
        try:
            client.ping()
            log.info("Connected to Binance API (ping ok).")
        except Exception:
            log.warning("Binance ping failed (connection may still succeed for calls).")

        # Fetch and store the actual position mode from the exchange
        try:
            position_mode = client.futures_get_position_mode()
            IS_HEDGE_MODE = position_mode.get('dualSidePosition', False)
            mode_str = "Hedge Mode" if IS_HEDGE_MODE else "One-way Mode"
            log.info(f"Successfully fetched account position mode: {mode_str}")
            # Optional: Compare with local config and warn if different
            if IS_HEDGE_MODE != CONFIG["HEDGING_ENABLED"]:
                log.warning(f"Configuration mismatch! Local HEDGING_ENABLED is {CONFIG['HEDGING_ENABLED']} but account is in {mode_str}.")
                send_telegram(f"⚠️ **Configuration Mismatch**\nYour bot's `HEDGING_ENABLED` setting is `{CONFIG['HEDGING_ENABLED']}`, but your Binance account is in **{mode_str}**. The bot will use the live account setting to place orders, but please update your config to match.")
        except Exception as e:
            log.error("Failed to fetch account position mode. Defaulting to One-way Mode logic. Error: %s", e)
            IS_HEDGE_MODE = False # Default to false on error
            send_telegram("⚠️ Could not determine account position mode (Hedge vs One-way). Defaulting to One-way. Please ensure this is correct.")
        
        EXCHANGE_INFO_CACHE['data'] = None
        EXCHANGE_INFO_CACHE['ts'] = 0.0
        return True, ""
    except Exception as e:
        log.exception("Failed to connect to Binance API: %s", e)
        try:
            ip = get_public_ip()
        except Exception:
            ip = "<unknown>"
        err = f"Binance init error: {e}; server_ip={ip}"
        try:
            send_telegram(f"Binance init failed: {e}\nServer IP: {ip}\nPlease update IP in Binance API whitelist if needed.")
        except Exception:
            log.exception("Failed to notify via telegram about Binance init error.")
        client = None
        return False, err

# -------------------------
# S5 Management Thread (Trailing, BE, Partial)
# -------------------------
def s5_monitor_thread_func():
    global managed_trades
    log.info("S5 management thread started.")
    while not monitor_stop_event.is_set():
        try:
            # Snapshot S5 trades
            with managed_trades_lock:
                trades_snapshot = {tid: dict(meta) for tid, meta in managed_trades.items() if str(meta.get('strategy_id')) == '5'}
            if not trades_snapshot:
                time.sleep(2.0)
                continue

            for tid, meta in trades_snapshot.items():
                try:
                    symbol = meta['symbol']
                    side = meta['side']
                    qty = float(meta.get('qty', 0.0))
                    if qty <= 0:
                        continue

                    entry = float(meta['entry_price'])
                    current_sl = float(meta.get('sl', 0.0))
                    be_moved = bool(meta.get('be_moved', False))
                    stage = int(meta.get('s5_stage', 0))
                    partial_done = bool(meta.get('s5_partial_done', False))
                    trailing_active = bool(meta.get('s5_trailing_active', False))
                    initial_qty = float(meta.get('initial_qty', qty))
                    leverage = int(meta.get('leverage', CONFIG.get("MAX_BOT_LEVERAGE", 20)))

                    df = fetch_klines_sync(symbol, "15m", 250)
                    if df is None or df.empty:
                        continue

                    s5 = CONFIG['STRATEGY_5']
                    atr_now = safe_last(atr(df, s5['ATR_PERIOD']), default=0.0)
                    if atr_now <= 0:
                        continue
                    current_price = safe_last(df['close'], default=0.0)
                    if current_price <= 0:
                        continue

                    risk_unit = float(meta.get('s5_risk_per_unit') or abs(entry - current_sl))
                    if risk_unit <= 0:
                        continue

                    # Compute R-move
                    if side == 'BUY':
                        r_move = (current_price - entry) / risk_unit
                    else:
                        r_move = (entry - current_price) / risk_unit

                    # Stage A - BE at +0.5R
                    if (not be_moved) and r_move >= s5['BE_TRIGGER_R']:
                        be_offset = s5['BE_OFFSET_ATR'] * atr_now
                        if side == 'BUY':
                            new_sl = entry + be_offset
                            # Ensure logical; avoid placing SL above current price
                            if new_sl >= current_price:
                                new_sl = min(current_price * 0.999, entry)
                        else:
                            new_sl = entry - be_offset
                            if new_sl <= current_price:
                                new_sl = max(current_price * 1.001, entry)
                        # Cancel existing close orders then place updated SL only
                        cancel_close_orders_sync(symbol)
                        place_batch_sl_tp_sync(symbol, side, sl_price=new_sl, tp_price=None, qty=qty)
                        meta['sl'] = new_sl
                        meta['be_moved'] = True
                        meta['s5_stage'] = max(stage, 1)
                        meta['s5_trailing_stop'] = new_sl
                        add_managed_trade_to_db(meta)
                        # Update global state
                        with managed_trades_lock:
                            if tid in managed_trades:
                                managed_trades[tid].update({
                                    'sl': new_sl, 'be_moved': True,
                                    's5_stage': meta['s5_stage'],
                                    's5_trailing_stop': new_sl
                                })
                        log.info(f"S5 BE moved for {symbol}. New SL={new_sl}")
                        continue  # Re-evaluate next cycle after BE

                    # Stage B - Partial at 1R and activate trailing
                    if (not partial_done) and r_move >= s5['PARTIAL_TP1_R']:
                        pct = s5['PARTIAL_TP1_PCT']
                        qty_to_close_unrounded = max(0.0, initial_qty * pct)
                        qty_to_close = round_qty(symbol, qty_to_close_unrounded, rounding=ROUND_DOWN)
                        qty_to_close = min(qty_to_close, qty)
                        if qty_to_close > 0:
                            try:
                                close_partial_market_position_sync(symbol, side, qty_to_close)
                                new_qty = qty - qty_to_close
                                # Re-place SL for the remaining position
                                cancel_close_orders_sync(symbol)
                                if new_qty > 0:
                                    place_batch_sl_tp_sync(symbol, side, sl_price=current_sl, tp_price=None, qty=new_qty)
                                meta['qty'] = new_qty
                                with managed_trades_lock:
                                    if tid in managed_trades:
                                        managed_trades[tid]['qty'] = new_qty
                            except Exception:
                                log.exception(f"S5 partial close failed for {symbol}")
                        meta['s5_partial_done'] = True
                        meta['s5_trailing_active'] = True
                        meta['s5_stage'] = 2
                        add_managed_trade_to_db(meta)
                        with managed_trades_lock:
                            if tid in managed_trades:
                                managed_trades[tid].update({
                                    's5_partial_done': True,
                                    's5_trailing_active': True,
                                    's5_stage': 2
                                })
                        log.info(f"S5 partial at 1R executed for {symbol}. Remaining qty={meta['qty']}")
                        # Do not continue; allow trailing in same cycle

                    # Trailing - ATR + swing pivot
                    if trailing_active or meta.get('s5_trailing_active'):
                        buffer = s5['TRAIL_SWING_BUFFER_ATR'] * atr_now
                        pivot_low, _ = last_pivot_low(df, lookback=3)
                        pivot_high, _ = last_pivot_high(df, lookback=3)

                        new_sl = None
                        if side == 'BUY':
                            candidate_atr = current_price - s5['TRAIL_ATR_MULT'] * atr_now
                            candidates = [candidate_atr]
                            if pivot_low is not None:
                                candidates.append(pivot_low - buffer)
                            potential_sl = max([c for c in candidates if c is not None])
                            if potential_sl > current_sl and potential_sl < current_price:
                                new_sl = potential_sl
                        else:
                            candidate_atr = current_price + s5['TRAIL_ATR_MULT'] * atr_now
                            candidates = [candidate_atr]
                            if pivot_high is not None:
                                candidates.append(pivot_high + buffer)
                            # For shorts, we want the lowest acceptable stop above price
                            potential_sl = min([c for c in candidates if c is not None])
                            if potential_sl < current_sl and potential_sl > current_price:
                                new_sl = potential_sl

                        if new_sl is not None and qty > 0:
                            try:
                                cancel_close_orders_sync(symbol)
                                place_batch_sl_tp_sync(symbol, side, sl_price=new_sl, tp_price=None, qty=qty)
                                meta['sl'] = new_sl
                                meta['s5_trailing_stop'] = new_sl
                                add_managed_trade_to_db(meta)
                                with managed_trades_lock:
                                    if tid in managed_trades:
                                        managed_trades[tid].update({'sl': new_sl, 's5_trailing_stop': new_sl})
                                log.info(f"S5 trail updated for {symbol}. New SL={new_sl}")
                            except Exception:
                                log.exception(f"S5 trailing update failed for {symbol}")

                except Exception:
                    log.exception(f"S5 management loop error for trade {tid}")
            time.sleep(2.5)

        except Exception:
            log.exception("S5 management thread iteration failed")
            time.sleep(5.0)

# -------------------------
# Exchange info cache helper
# -------------------------
def get_exchange_info_sync():
    global EXCHANGE_INFO_CACHE, client
    now = time.time()
    if EXCHANGE_INFO_CACHE["data"] and (now - EXCHANGE_INFO_CACHE["ts"] < EXCHANGE_INFO_CACHE["ttl"]):
        return EXCHANGE_INFO_CACHE["data"]
    if client is None:
        return None
    try:
        info = client.futures_exchange_info()
        EXCHANGE_INFO_CACHE["data"] = info
        EXCHANGE_INFO_CACHE["ts"] = now
        return info
    except Exception:
        log.exception("Failed to fetch exchange info for cache")
        return EXCHANGE_INFO_CACHE["data"]

# ... (rest of the functions are unchanged)
def get_symbol_info(symbol: str) -> Optional[Dict[str, Any]]:
    info = get_exchange_info_sync()
    if not info:
        return None
    try:
        symbols = info.get('symbols', [])
        return next((s for s in symbols if s.get('symbol') == symbol), None)
    except Exception:
        return None

def get_min_notional_sync(symbol: str) -> float:
    """
    Retrieves the minimum notional value for a given symbol from exchange info.
    Falls back to the globally configured MIN_NOTIONAL_USDT.
    """
    try:
        info = get_exchange_info_sync()
        if not info or not isinstance(info, dict):
            return float(CONFIG.get("MIN_NOTIONAL_USDT", 5.0))

        symbol_info = next((s for s in info.get('symbols', []) if s.get('symbol') == symbol), None)
        if not symbol_info:
            return float(CONFIG.get("MIN_NOTIONAL_USDT", 5.0))

        for f in symbol_info.get('filters', []):
            if f.get('filterType') == 'MIN_NOTIONAL':
                notional_val = f.get('notional')
                if notional_val:
                    # Add a small buffer to avoid floating point issues
                    return float(notional_val) * 1.01
        
        return float(CONFIG.get("MIN_NOTIONAL_USDT", 5.0))
    except Exception as e:
        log.exception(f"Failed to get min notional for {symbol}, using config fallback. Error: {e}")
        return float(CONFIG.get("MIN_NOTIONAL_USDT", 5.0))

def get_step_size(symbol: str) -> Optional[Decimal]:
    """Retrieves the lot step size for a given symbol from exchange info."""
    try:
        info = get_exchange_info_sync()
        if not info or not isinstance(info, dict): return None
        symbol_info = next((s for s in info.get('symbols', []) if s.get('symbol') == symbol), None)
        if not symbol_info: return None
        for f in symbol_info.get('filters', []):
            if f.get('filterType') == 'LOT_SIZE':
                return Decimal(str(f.get('stepSize', '1')))
    except Exception:
        log.exception("get_step_size failed")
    return None

def get_max_leverage(symbol: str) -> int:
    try:
        s = get_symbol_info(symbol)
        if s:
            ml = s.get('maxLeverage') or s.get('leverage')
            if ml:
                try:
