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
    "STRATEGY_MODE": os.getenv("STRATEGY_MODE", "5,6,7,8,9"),
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
    "STRATEGY_5": {  # Advanced crypto-futures strategy (H1 trend + M15 execution)
        "H1_ST_PERIOD": int(os.getenv("S5_H1_ST_PERIOD", "10")),
        "H1_ST_MULT": float(os.getenv("S5_H1_ST_MULT", "3.0")),
        "EMA_FAST": int(os.getenv("S5_EMA_FAST", "21")),
        "EMA_SLOW": int(os.getenv("S5_EMA_SLOW", "55")),
        "ATR_PERIOD": int(os.getenv("S5_ATR_PERIOD", "14")),
        "RSI_PERIOD": int(os.getenv("S5_RSI_PERIOD", "14")),
        "VOL_MIN_PCT": float(os.getenv("S5_VOL_MIN_PCT", "0.003")),   # 0.3%
        "VOL_MAX_PCT": float(os.getenv("S5_VOL_MAX_PCT", "0.035")),   # 3.5%
        "RISK_USD": float(os.getenv("S5_RISK_USD", "0.50")),          # Same risk model as S4 (fixed risk)
        "TP1_CLOSE_PCT": float(os.getenv("S5_TP1_CLOSE_PCT", "0.3")), # 30% at 1R
        "TRAIL_ATR_MULT": float(os.getenv("S5_TRAIL_ATR_MULT", "1.0")),
        "TRAIL_BUFFER_MULT": float(os.getenv("S5_TRAIL_BUFFER_MULT", "0.25")),
        "MAX_TRADES_PER_SYMBOL_PER_DAY": int(os.getenv("S5_MAX_TRADES_PER_SYMBOL_PER_DAY", "2")),
        "SYMBOLS": os.getenv(
            "S5_SYMBOLS",
            "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,AVAXUSDT,LTCUSDT,ADAUSDT,XRPUSDT,LINKUSDT,DOTUSDT"
        ).split(","),
    },
    "STRATEGY_6": { # Price-Action only (single high-probability trade per day)
        "ATR_PERIOD": int(os.getenv("S6_ATR_PERIOD", "14")),
        "ATR_BUFFER_MULT": float(os.getenv("S6_ATR_BUFFER", "0.25")),
        "FOLLOW_THROUGH_RANGE_RATIO": float(os.getenv("S6_FOLLOW_THROUGH_RATIO", "0.7")),
        "VOL_MA_LEN": int(os.getenv("S6_VOL_MA_LEN", "10")),
        "LIMIT_EXPIRY_CANDLES": int(os.getenv("S6_LIMIT_EXPIRY_CANDLES", "3")),
        "SESSION_START_UTC_HOUR": int(os.getenv("S6_SESSION_START_HOUR", "7")),
        "SESSION_END_UTC_HOUR": int(os.getenv("S6_SESSION_END_HOUR", "15")),
        "RISK_USD": float(os.getenv("S6_RISK_USD", "0.50")),
        "ENFORCE_ONE_TRADE_PER_DAY": os.getenv("S6_ENFORCE_ONE_PER_DAY", "true").lower() in ("true", "1", "yes"),
        "SYMBOLS": os.getenv("S6_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,AVAXUSDT,LTCUSDT,ADAUSDT,XRPUSDT,LINKUSDT,DOTUSDT").split(","),
    },
    "STRATEGY_7": { # SMC (Smart Money Concepts) execution - price-action only
        "ATR_PERIOD": int(os.getenv("S7_ATR_PERIOD", "14")),
        "ATR_BUFFER": float(os.getenv("S7_ATR_BUFFER", "0.25")),
        "BOS_LOOKBACK_H1": int(os.getenv("S7_BOS_LOOKBACK_H1", "72")),
        "OB_MIN_BODY_RATIO": float(os.getenv("S7_OB_MIN_BODY_RATIO", "0.5")),
        "REJECTION_WICK_RATIO": float(os.getenv("S7_REJECTION_WICK_RATIO", "0.6")),
        "LIMIT_EXPIRY_CANDLES": int(os.getenv("S7_LIMIT_EXPIRY_CANDLES", "4")),
        "USE_MIN_NOTIONAL": os.getenv("S7_USE_MIN_NOTIONAL", "true").lower() in ("true", "1", "yes"),
        "ALLOW_M5_MICRO_CONFIRM": os.getenv("S7_ALLOW_M5_MICRO_CONFIRM", "false").lower() in ("true", "1", "yes"),
        "SYMBOLS": os.getenv(
            "S7_SYMBOLS",
            "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,AVAXUSDT,LTCUSDT,ADAUSDT,XRPUSDT,LINKUSDT,DOTUSDT"
        ).split(","),

        "RISK_USD": float(os.getenv("S7_RISK_USD", "0.0")),  # kept optional; default 0 uses min notional
    },
    "STRATEGY_8": {  # SMC + Chart-Pattern Sniper Entry — break+retest inside OB/FVG
        "ATR_PERIOD": int(os.getenv("S8_ATR_PERIOD", "14")),
        "ATR_BUFFER": float(os.getenv("S8_ATR_BUFFER", "0.25")),
        "BOS_LOOKBACK_H1": int(os.getenv("S8_BOS_LOOKBACK_H1", "72")),
        "VOL_MA_LEN": int(os.getenv("S8_VOL_MA_LEN", "10")),
        "RETEST_EXPIRY_CANDLES": int(os.getenv("S8_RETEST_EXPIRY_CANDLES", "3")),
        "USE_OB": os.getenv("S8_USE_OB", "true").lower() in ("true", "1", "yes"),
        "USE_FVG": os.getenv("S8_USE_FVG", "true").lower() in ("true", "1", "yes"),
        "SYMBOLS": os.getenv("S8_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,AVAXUSDT,LTCUSDT,ADAUSDT,XRPUSDT,LINKUSDT,DOTUSDT").split(","),
    },
    "STRATEGY_9": {  # SMC Scalping — high-win probability (M1/M5 execution, H1 BOS, H4/D bias)
        "REJECTION_WICK_RATIO": float(os.getenv("S9_REJECTION_WICK_RATIO", "0.7")),
        "M1_RANGE_AVG_LEN": int(os.getenv("S9_M1_RANGE_AVG_LEN", "20")),
        "M5_ATR_PERIOD": int(os.getenv("S9_M5_ATR_PERIOD", "14")),
        "ATR_BUFFER_MULT_M5": float(os.getenv("S9_ATR_BUFFER_MULT_M5", "0.6")),
        "MAX_STOP_TO_AVG_RANGE_M5": float(os.getenv("S9_MAX_STOP_TO_AVG_RANGE_M5", "1.5")),
        "LIMIT_EXPIRY_M1_CANDLES": int(os.getenv("S9_LIMIT_EXPIRY_M1_CANDLES", "3")),
        "BOS_LOOKBACK_H1_MIN": int(os.getenv("S9_BOS_LOOKBACK_H1_MIN", "12")),
        "BOS_LOOKBACK_H1_MAX": int(os.getenv("S9_BOS_LOOKBACK_H1_MAX", "48")),
        "SESSION_START_UTC_HOUR": int(os.getenv("S9_SESSION_START_HOUR", "7")),
        "SESSION_END_UTC_HOUR": int(os.getenv("S9_SESSION_END_HOUR", "15")),
        "RISK_USD": float(os.getenv("S9_RISK_USD", "0.50")),  # Use S6 fixed-risk sizing model
        "SYMBOLS": os.getenv("S9_SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT,SOLUSDT,AVAXUSDT,LTCUSDT,ADAUSDT,XRPUSDT,LINKUSDT,DOTUSDT").split(","),
        "HIGH_WIN_TP_R_MULT": float(os.getenv("S9_HIGH_WIN_TP_R_MULT", "0.5")),  # Conservative target 0.5R
        "MICRO_SWEEP_LOOKBACK_M1": int(os.getenv("S9_MICRO_SWEEP_LOOKBACK_M1", "20")),
        "SWEEP_RECLAIM_MAX_BARS": int(os.getenv("S9_SWEEP_RECLAIM_MAX_BARS", "5"))
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
        "5": {  # Advanced H1/M15 strategy (custom trailing logic)
            "ATR_MULTIPLIER": float(os.getenv("S5_TRAIL_ATR_MULT", "1.0")),
            "BE_TRIGGER": 0.0,
            "BE_SL_OFFSET": 0.0
        },
        "7": {  # SMC trailing is structural; keep generic minimal trailing disabled by default
            "ATR_MULTIPLIER": float(os.getenv("S7_TRAIL_ATR_MULT", "0.0")),
            "BE_TRIGGER": 0.0,
            "BE_SL_OFFSET": 0.0
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

    # --- ACCOUNT MODE ---
    # Local preference for hedging (dualSidePosition). The live exchange mode takes precedence at runtime.
    "HEDGING_ENABLED": os.getenv("HEDGING_ENABLED", "false").lower() in ("true", "1", "yes"),

    # --- MONITORING / PERFORMANCE ---
    # Warn if a single monitor loop exceeds this duration (in seconds)
    "MONITOR_LOOP_THRESHOLD_SEC": float(os.getenv("MONITOR_LOOP_THRESHOLD_SEC", "5")),






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
}

# --- Ensure required config keys exist with sane defaults ---
# Some downstream code accesses these with direct indexing.
CONFIG.setdefault("HEDGING_ENABLED", os.getenv("HEDGING_ENABLED", "false").lower() in ("true", "1", "yes"))
try:
    CONFIG.setdefault("MONITOR_LOOP_THRESHOLD_SEC", float(os.getenv("MONITOR_LOOP_THRESHOLD_SEC", "5")))
except Exception:
    CONFIG["MONITOR_LOOP_THRESHOLD_SEC"] = 5.0

# --- Parse STRATEGY_MODE into a list of ints (robustly) ---
mode_raw = CONFIG.get('STRATEGY_MODE', 0)
if isinstance(mode_raw, list):
    try:
        CONFIG['STRATEGY_MODE'] = [int(x) for x in mode_raw]
    except Exception:
        log.error(f"Invalid STRATEGY_MODE list: '{mode_raw}'. Defaulting to auto (0).")
        CONFIG['STRATEGY_MODE'] = [0]
else:
    try:
        CONFIG['STRATEGY_MODE'] = [int(x.strip()) for x in str(mode_raw).split(',')]
    except (ValueError, TypeError):
        log.error(f"Invalid STRATEGY_MODE: '{mode_raw}'. Must be a comma-separated list of numbers. Defaulting to auto (0).")
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

telegram_thread: Optional[threading.Thread] = None
monitor_thread_obj: Optional[threading.Thread] = None
pnl_monitor_thread_obj: Optional[threading.Thread] = None
maintenance_thread_obj: Optional[threading.Thread] = None
alerter_thread_obj: Optional[threading.Thread] = None
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

def infer_strategy_for_open_trade_sync(symbol: str, side: str) -> Optional[int]:
    """
    Try to infer the entry timestamp from the actual fill time of the position,
    using account trades first (most accurate), then fall back to the position's
    updateTime, then finally to the most recent closed candle.

    This improves strategy inference accuracy (e.g. distinguishing S6 from S4)
    because using updateTime can reflect later updates (like SL/TP changes)
    rather than the original entry fill time.
    """
    ts_ms: Optional[int] = None
    try:
        if client is not None:
            desired_pos_side = 'LONG' if side == 'BUY' else 'SHORT'

            # 1) Prefer the latest trade fill that contributed to the current position side
            #    This is the most accurate signal time for the entry.
            try:
                trades = client.futures_account_trades(symbol=symbol, limit=50)
                # Find last trade that increased the position on this side
                # For LONG: side == 'BUY' and positionSide == 'LONG'
                # For SHORT: side == 'SELL' and positionSide == 'SHORT'
                contributing = [
                    t for t in trades
                    if str(t.get('positionSide', '')).upper() == desired_pos_side
                    and str(t.get('side', '')).upper() == side
                ]
                if contributing:
                    # Pick the most recent by time
                    last_trade = max(contributing, key=lambda x: int(x.get('time', 0)))
                    if last_trade.get('time'):
                        ts_ms = int(last_trade['time'])
            except Exception:
                # Ignore and fall back to position info
                pass

            # 2) Fallback: use the position's update time (can be later than entry)
            if ts_ms is None:
                positions = client.futures_position_information(symbol=symbol)
                pos = next((p for p in positions if p.get('positionSide') in (desired_pos_side, 'BOTH')), None)
                if pos:
                    ut = pos.get('updateTime') or pos.get('updateTimeMs') or pos.get('time')
                    if ut:
                        ts_ms = int(float(ut))
    except Exception:
        ts_ms = None

    return infer_strategy_for_open_trade_at_time_sync(symbol, side, ts_ms)

def _nearest_closed_index_for_time(df: pd.DataFrame, ts_ms: Optional[int]) -> Optional[int]:
    if df is None or df.empty:
        return None
    if ts_ms is None:
        return len(df) - 1  # last closed
    ts = pd.to_datetime(ts_ms, unit='ms', utc=True)
    # find index of last candle closed at or before ts
    idx = df.index.get_indexer([ts], method='pad')[0]
    if idx is None or idx < 2:
        return None
    return idx

def infer_strategy_for_open_trade_at_time_sync(symbol: str, side: str, ts_ms: Optional[int]) -> Optional[int]:
    """
    Infers strategy likely responsible for an open trade, using signals around a given timestamp (ms).
    Priority: 5, 6, 7, 4, 3, 2, 1.
    If ts_ms is None, uses last closed candles.
    """
    try:
        # Fetch M15
        df = fetch_klines_sync(symbol, CONFIG["TIMEFRAME"], 300)
        if df is None or len(df) < 80:
            return None
        df_ind = calculate_all_indicators(df.copy())

        sig_idx = _nearest_closed_index_for_time(df_ind, ts_ms)
        if sig_idx is None or sig_idx < 3:
            return None

        sig = df_ind.iloc[sig_idx - 1]
        prev = df_ind.iloc[sig_idx - 2]
        ft = df_ind.iloc[sig_idx] if sig_idx < len(df_ind) else df_ind.iloc[-1]

        # 1) S5 check
        try:
            s5 = CONFIG.get('STRATEGY_5', {})
            df_h1 = fetch_klines_sync(symbol, '1h', 300)
            if df_h1 is not None and len(df_h1) >= 80:
                h1_idx = _nearest_closed_index_for_time(df_h1, ts_ms)
                if h1_idx is None or h1_idx < 2:
                    raise RuntimeError("no h1 index")
                df_h1 = df_h1.copy()
                df_h1['ema_fast'] = ema(df_h1['close'], s5.get('EMA_FAST', 21))
                df_h1['ema_slow'] = ema(df_h1['close'], s5.get('EMA_SLOW', 55))
                df_h1['st_h1'], df_h1['st_h1_dir'] = supertrend(df_h1, period=s5.get('H1_ST_PERIOD', 10), multiplier=s5.get('H1_ST_MULT', 3.0))
                h1_last = df_h1.iloc[h1_idx - 1]
                h1_bull = (h1_last['ema_fast'] > h1_last['ema_slow']) and (h1_last['close'] > h1_last['st_h1'])
                h1_bear = (h1_last['ema_fast'] < h1_last['ema_slow']) and (h1_last['close'] < h1_last['st_h1'])

                df_ind['s5_m15_ema_fast'] = ema(df_ind['close'], s5.get('EMA_FAST', 21))
                df_ind['s5_m15_ema_slow'] = ema(df_ind['close'], s5.get('EMA_SLOW', 55))
                df_ind['s5_atr'] = atr(df_ind, s5.get('ATR_PERIOD', 14))
                df_ind['s5_rsi'] = rsi(df_ind['close'], s5.get('RSI_PERIOD', 14))
                df_ind['s5_vol_ma10'] = df_ind['volume'].rolling(10).mean()
                sig = df_ind.iloc[sig_idx - 1]; prev = df_ind.iloc[sig_idx - 2]
                m15_bull_pullback = (sig['s5_m15_ema_fast'] >= sig['s5_m15_ema_slow']) and (prev['low'] <= prev['s5_m15_ema_fast']) and (sig['close'] > sig['s5_m15_ema_fast']) and (sig['close'] > sig['open'])
                m15_bear_pullback = (sig['s5_m15_ema_fast'] <= sig['s5_m15_ema_slow']) and (prev['high'] >= prev['s5_m15_ema_fast']) and (sig['close'] < sig['s5_m15_ema_fast']) and (sig['close'] < sig['open'])
                vol_spike = (sig['volume'] >= 1.2 * sig['s5_vol_ma10']) if pd.notna(sig['s5_vol_ma10']) else False
                rsi_ok = 35 <= sig['s5_rsi'] <= 65
                if (side == 'BUY' and h1_bull and m15_bull_pullback and vol_spike and rsi_ok) or \
                   (side == 'SELL' and h1_bear and m15_bear_pullback and vol_spike and rsi_ok):
                    return 5
        except Exception:
            pass

        # 2) S6 check
        try:
            s6 = CONFIG.get('STRATEGY_6', {})
            df_ind['s6_atr'] = atr(df_ind, s6.get('ATR_PERIOD', 14))
            df_h4 = fetch_klines_sync(symbol, '4h', 200)
            df_d = fetch_klines_sync(symbol, '1d', 200)
            if df_h4 is not None and df_d is not None and len(df_h4) >= 50 and len(df_d) >= 50:
                bias_d = _s6_trend_from_swings(df_d, swing_lookback=20)
                direction = 'BUY' if bias_d == 'BULL' else ('SELL' if bias_d == 'BEAR' else None)
                sig = df_ind.iloc[sig_idx - 1]; prev = df_ind.iloc[sig_idx - 2]; ft = df_ind.iloc[min(sig_idx, len(df_ind)-1)]
                if direction == side:
                    is_pin = _s6_is_pin_bar(sig, direction)
                    is_engulf = _s6_is_engulfing_reclaim(sig, prev, direction, float(sig['close']))
                    vol_ma = float(df_ind['volume'].rolling(int(s6.get('VOL_MA_LEN', 10))).mean().iloc[sig_idx - 1])
                    if (is_pin or is_engulf) and _s6_follow_through_ok(sig, ft, direction, vol_ma, float(s6.get('FOLLOW_THROUGH_RATIO', 0.7))):
                        return 6
        except Exception:
            pass

        # 3) S7 check
        try:
            s7 = CONFIG.get('STRATEGY_7', {})
            df_h1 = fetch_klines_sync(symbol, '1h', 300)
            if df_h1 is not None and len(df_h1) >= 120:
                lookback = int(s7.get('BOS_LOOKBACK_H1', 72))
                h1_idx = _nearest_closed_index_for_time(df_h1, ts_ms)
                if h1_idx is None or h1_idx < lookback + 2:
                    raise RuntimeError("no h1 idx")
                sig_h1 = df_h1.iloc[h1_idx - 1]
                prev_window_high = float(df_h1['high'].iloc[(h1_idx - lookback - 2):(h1_idx - 1)].max())
                prev_window_low = float(df_h1['low'].iloc[(h1_idx - lookback - 2):(h1_idx - 1)].min())
                dir_detected = 'BUY' if float(sig_h1['close']) > prev_window_high else ('SELL' if float(sig_h1['close']) < prev_window_low else None)
                if dir_detected == side:
                    sig = df_ind.iloc[sig_idx - 1]; prev = df_ind.iloc[sig_idx - 2]
                    is_pin = _s6_is_pin_bar(sig, side)
                    is_engulf = _s6_is_engulfing_reclaim(sig, prev, side, float(sig['close']))
                    if is_pin or is_engulf:
                        return 7
        except Exception:
            pass

        # 4) Fallback sims
        try:
            sim4 = simulate_strategy_4(symbol, df_ind)
            if sim4 and sim4.get('side') == side:
                return 4
        except Exception:
            pass
        try:
            sim3 = simulate_strategy_3(symbol, df_ind)
            if sim3 and sim3.get('side') == side:
                return 3
        except Exception:
            pass
        try:
            sim2 = simulate_strategy_supertrend(symbol, df_ind)
            if sim2 and sim2.get('side') == side:
                return 2
        except Exception:
            pass
        try:
            sim1 = simulate_strategy_bb(symbol, df_ind)
            if sim1 and sim1.get('side') == side:
                return 1
        except Exception:
            pass

        return None
    except Exception:
        return None
    except Exception:
        return None

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

        # Infer strategy for better in-trade management. The sync function returns only an int (or None).
        inferred_strategy = await asyncio.to_thread(infer_strategy_for_open_trade_sync, symbol, side)
        infer_src = "last_closed"
        if inferred_strategy is None:
            inferred_strategy = 4  # fallback

        trade_id = f"{symbol}_imported_{int(time.time())}"
        meta = {
            "id": trade_id, "symbol": symbol, "side": side, "entry_price": entry_price,
            "initial_qty": qty, "qty": qty, "notional": notional, "leverage": leverage,
            "sl": stop_price if stop_price is not None else 0.0, "tp": 0.0, "open_time": datetime.utcnow().isoformat(),
            "sltp_orders": {}, "trailing": CONFIG["TRAILING_ENABLED"],
            "dyn_sltp": False, "tp1": None, "tp2": None, "tp3": None,
            "trade_phase": 0, "be_moved": False, "risk_usdt": 0.0,
            "strategy_id": inferred_strategy,
            "inference_time_source": infer_src,
        }

        # Initialize S5-specific management state if inferred as S5
        if inferred_strategy == 5 and stop_price is not None and stop_price > 0:
            r_dist = abs(entry_price - stop_price)
            meta['s5_initial_sl'] = stop_price
            meta['s5_r_per_unit'] = r_dist
            meta['s5_tp1_price'] = (entry_price + r_dist) if side == 'BUY' else (entry_price - r_dist)
            meta['trade_phase'] = 0
            meta['be_moved'] = False
            meta['trailing_active'] = False

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
                   f"**Inferred Strategy:** S{inferred_strategy}\n"
                   f"A default SL has been calculated and placed:\n"
                   f"**SL:** `{round_price(symbol, stop_price)}`\n\n"
                   f"The bot will now manage this trade.")
            await asyncio.to_thread(send_telegram, msg)
        else:
            log.warning(f"No valid SL placed for imported trade {symbol}. Please manage manually.")
            msg = (f"ℹ️ **Position Imported (No SL)**\n\n"
                   f"Found and imported a position for **{symbol}** but could not place a valid SL.\n\n"
                   f"**Inferred Strategy:** S{inferred_strategy}\n"
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

# ------------- Debug Endpoints -------------
@app.get("/debug/scan_once")
async def debug_scan_once(symbols: Optional[str] = None):
    """
    Runs a one-time evaluation-and-enter pass for the provided symbols in DRY_RUN mode
    so no real orders are placed. Returns a simple JSON summary.
    Usage: GET /debug/scan_once?symbols=BTCUSDT,ETHUSDT
    """
    # Parse symbol list or default to configured symbols
    if symbols:
        sym_list = [s.strip().upper() for s in symbols.split(",") if s.strip()]
    else:
        sym_list = [s.strip().upper() for s in CONFIG.get("SYMBOLS", []) if s.strip()]

    # Force DRY_RUN for safety during debug scan
    original_dry = CONFIG.get("DRY_RUN", False)
    CONFIG["DRY_RUN"] = True

    results = {}
    try:
        for s in sym_list:
            try:
                await evaluate_and_enter(s)
                results[s] = "ok"
            except Exception as e:
                results[s] = f"error: {type(e).__name__}: {e}"
        return {"status": "ok", "dry_run": True, "symbols": sym_list, "results": results}
    finally:
        # Restore original DRY_RUN configuration
        CONFIG["DRY_RUN"] = original_dry

# -------------------------
# Utilities
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
    """
    Derive a safe default SL for an imported position.
    Primary: use S4 ST2 supertrend as stop. If invalid (wrong side of price), fall back to ATR-based distance.
    Returns: (stop_price, take_price, current_price). take_price is 0.0 for imports (no TP by default).
    """
    df = fetch_klines_sync(symbol, CONFIG["TIMEFRAME"], 300)
    if df is None or df.empty:
        raise RuntimeError("No kline data to calc default SL/TP")

    # Compute Supertrend (S4 ST2) and ATR
    s4_params = CONFIG['STRATEGY_4']
    st2, _ = supertrend(df.copy(), period=s4_params['ST2_PERIOD'], multiplier=s4_params['ST2_MULT'])
    df['atr'] = atr(df, CONFIG.get("ATR_LENGTH", 14))

    current_price = safe_last(df['close'])
    atr_now = max(1e-9, safe_last(df['atr']))  # avoid zero
    stop_price = safe_last(st2)

    # Validate side and correct if invalid
    if side == 'BUY':
        if stop_price >= current_price or stop_price <= 0:
            stop_price = current_price - 1.5 * atr_now
    else:  # SELL
        if stop_price <= current_price or stop_price <= 0:
            stop_price = current_price + 1.5 * atr_now

    # Final safety: ensure non-negative and reasonable distance
    if not np.isfinite(stop_price) or stop_price <= 0:
        pct = 0.02
        stop_price = current_price * (1 - pct) if side == 'BUY' else current_price * (1 + pct)

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


def _json_native(val: Any) -> Any:
    """Convert numpy/pandas/scalars to JSON-serializable native Python types."""
    try:
        # Numpy scalars -> native
        if isinstance(val, np.generic):
            return val.item()
        # Pandas Timestamp -> ISO string
        if isinstance(val, pd.Timestamp):
            return val.isoformat()
        # Numpy arrays -> list
        if isinstance(val, np.ndarray):
            return val.tolist()
        # Fallback: leave as-is (json.dumps will handle str, int, float, bool, None, dict, list)
        return val
    except Exception:
        # Last resort: stringify
        return str(val)

def _record_rejection(symbol: str, reason: str, details: dict, signal_candle: Optional[pd.Series] = None):
    """Adds a rejected trade event to the deque and persists it to a file."""
    global rejected_trades

    # Enrich details with key indicator values from the signal candle if available
    if signal_candle is not None:
        indicator_keys = ['close', 'rsi', 'adx', 's1_bbu', 's1_bbl', 's2_st', 's4_st1', 's4_st2', 's4_st3']
        for key in indicator_keys:
            if key in signal_candle and pd.notna(signal_candle[key]):
                details[key] = signal_candle[key]

    # Normalize to JSON-native types first (so numpy types don't break json.dumps)
    native_details = {k: _json_native(v) for k, v in details.items()}

    # Format floats in details to a reasonable precision for display
    formatted_details = {
        k: (f"{v:.4f}" if isinstance(v, float) else v)
        for k, v in native_details.items()
    }

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

    # Coalesce NOT NULL columns to safe defaults
    stop_val = rec.get('stop_price', 0.0)
    if stop_val is None:
        stop_val = 0.0
    take_val = rec.get('take_price', 0.0)
    if take_val is None:
        take_val = 0.0

    values = (
        rec.get('id'), rec.get('order_id'), rec.get('symbol'), rec.get('side'),
        rec.get('qty'), rec.get('limit_price'), stop_val, take_val,
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

    # Coalesce required NOT NULL fields to safe defaults
    sl_val = rec.get('sl', 0.0) if rec.get('sl', None) is not None else 0.0
    tp_val = rec.get('tp', 0.0) if rec.get('tp', None) is not None else 0.0

    values = (
        rec['id'], rec['symbol'], rec['side'], rec['entry_price'], rec['initial_qty'],
        rec['qty'], rec['notional'], rec['leverage'], sl_val, tp_val,
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
        int(rec.get('s4_trailing_active', False))
    )
    cur.execute("""
    INSERT OR REPLACE INTO managed_trades (
        id, symbol, side, entry_price, initial_qty, qty, notional,
        leverage, sl, tp, open_time, sltp_orders, trailing, dyn_sltp,
        tp1, tp2, tp3, trade_phase, be_moved, risk_usdt, strategy_id, atr_at_entry,
        s3_trailing_active, s3_trailing_stop, s4_trailing_stop, s4_last_candle_ts,
        s4_trailing_active
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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

def swing_low(series_low: pd.Series, lookback: int = 5) -> float:
    """Returns the most recent swing low over a lookback window."""
    if series_low is None or len(series_low) < lookback:
        return float('nan')
    return float(series_low.iloc[-lookback:].min())

def swing_high(series_high: pd.Series, lookback: int = 5) -> float:
    """Returns the most recent swing high over a lookback window."""
    if series_high is None or len(series_high) < lookback:
        return float('nan')
    return float(series_high.iloc[-lookback:].max())

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
            # Auto-align local config to the live account mode to avoid mismatches.
            if IS_HEDGE_MODE != CONFIG.get("HEDGING_ENABLED", False):
                prev = CONFIG.get("HEDGING_ENABLED", None)
                CONFIG["HEDGING_ENABLED"] = IS_HEDGE_MODE
                log.info(f"Auto-aligned HEDGING_ENABLED from {prev} to {IS_HEDGE_MODE} based on account position mode ({mode_str}).")
                try:
                    send_telegram(
                        f"ℹ️ Auto-aligned config\n\n"
                        f"Detected account position mode: *{mode_str}*.\n"
                        f"Updated local `HEDGING_ENABLED` to `{IS_HEDGE_MODE}` to match.",
                        parse_mode="Markdown"
                    )
                except Exception:
                    pass
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
                    return int(float(ml))
                except Exception:
                    pass
        return 125
    except Exception:
        return 125

def round_qty(symbol: str, qty: float, rounding=ROUND_DOWN) -> float:
    """
    Rounds a quantity to the nearest valid step size for a given symbol.
    Uses ROUND_DOWN by default for safety to not exceed risk capital.
    Can use ROUND_CEILING to meet minimum notional value.
    """
    try:
        info = get_exchange_info_sync()
        if not info or not isinstance(info, dict):
            return float(qty)
        symbol_info = next((s for s in info.get('symbols', []) if s.get('symbol') == symbol), None)
        if not symbol_info:
            return float(qty)
        for f in symbol_info.get('filters', []):
            if f.get('filterType') == 'LOT_SIZE':
                step = Decimal(str(f.get('stepSize', '1')))
                getcontext().prec = 28
                q = Decimal(str(qty))
                quantized_q = (q / step).to_integral_value(rounding=rounding) * step
                if quantized_q <= 0:
                    return 0.0
                return float(quantized_q)
    except Exception:
        log.exception("round_qty failed; falling back to float")
    return float(qty)

def round_price(symbol: str, price: float) -> str:
    try:
        info = get_exchange_info_sync()
        if not info or not isinstance(info, dict):
            return f"{price:.8f}"  # Fallback
        symbol_info = next((s for s in info.get('symbols', []) if s.get('symbol') == symbol), None)
        if not symbol_info:
            return f"{price:.8f}"  # Fallback
        for f in symbol_info.get('filters', []):
            if f.get('filterType') == 'PRICE_FILTER':
                tick_size_str = f.get('tickSize', '0.00000001')
                tick_size = Decimal(tick_size_str)
                
                # Determine the number of decimal places from the tick_size for formatting
                decimal_places = abs(tick_size.as_tuple().exponent)

                getcontext().prec = 28
                p = Decimal(str(price))
                
                # Correctly round down to the nearest multiple of tick_size
                rounded_price = (p / tick_size).to_integral_value(rounding=ROUND_DOWN) * tick_size
                
                # Format with the correct number of decimal places to preserve trailing zeros
                return f"{rounded_price:.{decimal_places}f}"
    except Exception:
        log.exception("round_price failed; falling back to basic formatting")
    return f"{price:.8f}"

def place_limit_order_sync(symbol: str, side: str, qty: float, price: float, leverage: Optional[int] = None):
    """
    Places a single limit order with safety features:
    - Optionally ensure leverage is set before placing the order (if provided).
    - Optional pre-check for available margin and scale down qty if needed.
    - Retry once on Binance -2019 (Margin is insufficient) by halving qty.
    """
    global client, IS_HEDGE_MODE
    if CONFIG["DRY_RUN"]:
        log.info(f"[DRY RUN] Would place LIMIT {side} order for {qty} {symbol} at {price} (lev={leverage}).")
        dry_run_id = int(time.time())
        return {
            "orderId": f"dryrun_limit_{dry_run_id}", "symbol": symbol, "status": "NEW",
            "side": side, "type": "LIMIT", "origQty": str(qty), "price": str(price),
            "cumQuote": "0", "executedQty": "0", "avgPrice": "0.0"
        }

    if client is None:
        raise RuntimeError("Binance client not initialized")

    position_side = 'LONG' if side == 'BUY' else 'SHORT'

    # 1) Try to set requested leverage first (if provided), else set to a safe default
    try:
        max_lev_allowed = min(CONFIG.get("MAX_BOT_LEVERAGE", 30), get_max_leverage(symbol))
        if leverage is None:
            lev_to_set = int(max(1, max_lev_allowed))
        else:
            lev_to_set = int(max(1, min(int(leverage), max_lev_allowed)))
            if lev_to_set != leverage:
                log.info(f"Adjusted requested leverage for {symbol} from {leverage}x to {lev_to_set}x based on caps.")
        client.futures_change_leverage(symbol=symbol, leverage=lev_to_set)
        log.info(f"Leverage set to {lev_to_set}x for {symbol} before LIMIT order.")
    except Exception as e:
        log.warning(f"Failed to change leverage for {symbol} to {leverage if leverage is not None else 'default'}x (non-fatal): {e}")

    # Ensure qty respects step size precision
    qty = round_qty(symbol, float(qty), rounding=ROUND_DOWN)

    # 2) Optional pre-check: estimate initial margin and compare against available balance
    try:
        notional = float(qty) * float(price)
        lev_for_check = max(1, int(leverage)) if leverage else 1
        est_initial_margin = notional / lev_for_check if notional > 0 else 0.0

        # Fetch available balance (USDT) from futures account
        avail_usdt = 0.0
        try:
            balances = client.futures_account_balance()
            usdt_row = next((b for b in balances if str(b.get('asset')) == 'USDT'), None)
            if usdt_row:
                # availableBalance is the recommended field
                avail_usdt = float(usdt_row.get('availableBalance') or usdt_row.get('balance') or 0.0)
        except Exception:
            # Fall back to existing helper if available in codebase
            try:
                avail_usdt = float(get_account_balance_usdt())
            except Exception:
                avail_usdt = 0.0

        if est_initial_margin > 0 and avail_usdt > 0 and est_initial_margin > avail_usdt:
            # Scale down qty to fit available margin with a small buffer
            scale = (avail_usdt * 0.98) / est_initial_margin
            new_qty = round_qty(symbol, max(0.0, qty * scale), rounding=ROUND_DOWN)
            # Enforce min notional if possible
            min_notional = get_min_notional_sync(symbol)
            qty_min = round_qty(symbol, (min_notional / price) if price > 0 else 0.0, rounding=ROUND_CEILING)
            if new_qty < qty_min:
                log.info(f"Pre-check: insufficient margin for {symbol}. Needed ~{est_initial_margin:.4f} USDT, "
                         f"available {avail_usdt:.4f}. Scaled qty {new_qty} below min {qty_min}; "
                         f"rejecting order.")
                raise RuntimeError("Pre-check insufficient margin: qty below minimum after scaling.")
            if new_qty < qty:
                log.info(f"Pre-check: scaling down qty for {symbol} from {qty} to {new_qty} to fit available margin "
                         f"(est_margin={est_initial_margin:.4f}, avail={avail_usdt:.4f}, lev={lev_for_check}).")
                qty = new_qty
    except Exception as e:
        log.warning(f"Pre-check for margin failed for {symbol}: {e}. Proceeding without pre-scale.")

    params = {
        'symbol': symbol,
        'side': side,
        'type': 'LIMIT',
        'quantity': str(qty),
        'price': round_price(symbol, price),
        'timeInForce': 'GTC'  # Good-Til-Cancelled
    }
    if IS_HEDGE_MODE:
        params['positionSide'] = position_side

    # 3) Place order with one retry on -2019 by halving qty
    def _try_place(p):
        log.info(f"Attempting to place limit order with params: {p}")
        resp = client.futures_create_order(**p)
        log.info(f"Limit order placement successful for {symbol}. Response: {resp}")
        return resp

    try:
        return _try_place(params)
    except BinanceAPIException as e:
        if e.code == -2019:
            # Retry once with half qty
            try:
                original_qty = float(params['quantity'])
                retry_qty = round_qty(symbol, max(0.0, original_qty / 2.0), rounding=ROUND_DOWN)
                min_notional = get_min_notional_sync(symbol)
                qty_min = round_qty(symbol, (min_notional / price) if price > 0 else 0.0, rounding=ROUND_CEILING)
                if retry_qty <= 0 or retry_qty < qty_min:
                    log.error(f"Retry on -2019 aborted for {symbol}: halved qty {retry_qty} below min {qty_min}. "
                              f"Rejecting order.")
                    raise
                retry_params = dict(params)
                retry_params['quantity'] = str(retry_qty)
                log.warning(f"Binance -2019 (insufficient margin) for {symbol}. Retrying once with qty {retry_qty} "
                            f"(was {original_qty}).")
                return _try_place(retry_params)
            except Exception as retry_e:
                log.exception(f"Retry after -2019 failed for {symbol}: {retry_e}")
                raise
        log.exception("BinanceAPIException placing limit order: %s", e)
        raise
    except Exception as e:
        log.exception("Exception placing limit order: %s", e)
        raise
    except Exception as e:
        log.exception("Exception placing limit order: %s", e)
        raise

def open_market_position_sync(symbol: str, side: str, qty: float, leverage: int):
    """
    Places a simple market order to open or increase a position.
    This is a blocking call.
    """
    global client, IS_HEDGE_MODE
    if CONFIG["DRY_RUN"]:
        log.info(f"[DRY RUN] Would open market {side} order for {qty} {symbol}.")
        return {"status": "FILLED"}

    if client is None:
        raise RuntimeError("Binance client not initialized")

    try:
        log.info(f"Attempting to change leverage to {leverage}x for {symbol}")
        client.futures_change_leverage(symbol=symbol, leverage=leverage)
    except Exception as e:
        log.warning("Failed to change leverage (non-fatal, may use previous leverage): %s", e)

    # Generate a unique client order ID to make the order idempotent
    client_order_id = f"ema_bb_{symbol}_{int(time.time() * 1000)}"

    position_side = 'LONG' if side == 'BUY' else 'SHORT'

    params = {
        'symbol': symbol,
        'side': side,
        'type': 'MARKET',
        'quantity': str(qty),
        'newClientOrderId': client_order_id,
    }

    if IS_HEDGE_MODE:
        params['positionSide'] = position_side

    max_retries = 3
    last_exception = None
    for attempt in range(max_retries):
        try:
            log.info(f"Placing market order for {symbol} (Attempt {attempt + 1}/{max_retries}): {params}")
            order_response = client.futures_create_order(**params)
            log.info(f"Market order placement successful for {symbol}. Response: {order_response}")
            return order_response # Success
        except BinanceAPIException as e:
            # -4003: Quantity less than zero. This is a final error.
            # -1111: Precision is over the maximum defined for this asset. Final error.
            # -4164: Order's notional must be no less than... Final error.
            if e.code in [-4003, -1111, -4164]:
                 log.error(f"Unrecoverable Binance API error: {e}. Aborting retries.")
                 raise e
            log.exception(f"BinanceAPIException on attempt {attempt + 1} placing market order: {e}")
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(2)
        except Exception as e:
            log.exception(f"Exception on attempt {attempt + 1} placing market order: {e}")
            last_exception = e
            if attempt < max_retries - 1:
                time.sleep(2)

    log.error(f"Failed to place market order for {symbol} after {max_retries} attempts.")
    raise last_exception

def place_market_order_with_sl_tp_sync(symbol: str, side: str, qty: float, leverage: int, stop_price: float, take_price: float):
    """
    Places a market order and associated SL/TP orders in a single batch request.
    This is not an atomic operation on Binance's side. Error handling is included
    to attempt to clean up if some orders in the batch fail.
    """
    global client
    if CONFIG["DRY_RUN"]:
        log.info(f"[DRY RUN] Would open {side} position for {qty} {symbol} with {leverage}x leverage, SL {stop_price}, TP {take_price}.")
        dry_run_id = int(time.time())
        # The first element MUST be the market order for the downstream logic to work
        return [
            {
                "orderId": f"dryrun_mkt_{dry_run_id}", "symbol": symbol, "status": "FILLED",
                "side": side, "type": "MARKET", "origQty": qty, "executedQty": qty,
                "avgPrice": "0", "cumQuote": "0"
            },
            {"orderId": f"dryrun_sl_{dry_run_id}", "status": "NEW", "type": "STOP_MARKET"},
            {"orderId": f"dryrun_tp_{dry_run_id}", "status": "NEW", "type": "TAKE_PROFIT_MARKET"}
        ]

    if client is None:
        raise RuntimeError("Binance client not initialized")

    try:
        log.info(f"Attempting to change leverage to {leverage}x for {symbol}")
        client.futures_change_leverage(symbol=symbol, leverage=leverage)
    except Exception as e:
        log.warning("Failed to change leverage (non-fatal, may use previous leverage): %s", e)

    position_side = 'LONG' if side == 'BUY' else 'SHORT'
    close_side = 'SELL' if side == 'BUY' else 'BUY'

    market_order_params = {
        'symbol': symbol,
        'side': side,
        'type': 'MARKET',
        'quantity': str(qty),
    }
    close_order_params = {
        'symbol': symbol,
        'side': close_side,
        'quantity': str(qty),
    }

    if IS_HEDGE_MODE:
        market_order_params['positionSide'] = position_side
        close_order_params['positionSide'] = position_side
    else:
        # In one-way mode, closing orders must be reduceOnly. Entry order must not.
        close_order_params['reduceOnly'] = True

    # Build the full order batch
    order_batch = [market_order_params]
    
    sl_order = close_order_params.copy()
    sl_order.update({
        'type': 'STOP_MARKET',
        'stopPrice': round_price(symbol, stop_price),
    })
    order_batch.append(sl_order)
    
    tp_order = close_order_params.copy()
    tp_order.update({
        'type': 'TAKE_PROFIT_MARKET',
        'stopPrice': round_price(symbol, take_price),
    })
    order_batch.append(tp_order)

    try:
        log.info(f"Placing batch order for {symbol}: {order_batch}")
        batch_response = client.futures_place_batch_order(batchOrders=order_batch)

        errors = [resp for resp in batch_response if 'code' in resp]
        successful_orders = [resp for resp in batch_response if 'orderId' in resp]

        if errors:
            log.error(f"Batch order placement had failures for {symbol}. Errors: {errors}. Successful: {successful_orders}")

            market_order_resp = batch_response[0]
            if 'orderId' in market_order_resp:
                log.warning(f"Market order for {symbol} was successful but SL/TP failed. Attempting to close the naked position.")
                
                closed_successfully = False
                for i in range(3): # Retry up to 3 times
                    try:
                        time.sleep(1 + i) # Give exchange time, with increasing delay
                        client.futures_create_order(
                            symbol=symbol,
                            side=close_side,
                            type='MARKET',
                            quantity=str(qty),
                            positionSide=position_side,
                            reduceOnly=True
                        )
                        log.info(f"Successfully closed naked position for {symbol} on attempt {i + 1}.")
                        closed_successfully = True
                        break # Exit loop on success
                    except Exception as close_e:
                        log.exception(f"Attempt {i + 1} to close naked position for {symbol} failed. Error: {close_e}")
                
                if not closed_successfully:
                    error_details = f"Failed to close naked position for {qty} {symbol} after multiple attempts."
                    log.critical(error_details)
                    send_telegram(f"🚨 CRITICAL: {error_details} Manual intervention required.")
                    
                    # Persist the failure for the monitor thread to pick up
                    mark_attention_required_sync(symbol, "NAKED_POSITION", error_details)

            sl_tp_orders = [o for o in successful_orders if o.get('type') in ('STOP_MARKET', 'TAKE_PROFIT_MARKET')]
            if sl_tp_orders:
                cancel_ids = [o['orderId'] for o in sl_tp_orders]
                try:
                    client.futures_cancel_batch_order(symbol=symbol, orderIdList=cancel_ids)
                    log.info(f"Successfully cancelled {len(cancel_ids)} pending SL/TP orders for {symbol}.")
                except Exception as cancel_e:
                    log.exception(f"CRITICAL: Failed to cancel pending SL/TP orders for {symbol}. Manual intervention required. Error: {cancel_e}")

            raise RuntimeError(f"Batch order failed with errors: {errors}")

        log.info(f"Batch order successful for {symbol}. Response: {batch_response}")
        return batch_response
    except BinanceAPIException as e:
        log.exception("BinanceAPIException placing batch order: %s", e)
        raise
    except Exception as e:
        log.exception("Exception placing batch order: %s", e)
        raise

def place_batch_sl_tp_sync(symbol: str, side: str, sl_price: Optional[float] = None, tp_price: Optional[float] = None, qty: Optional[float] = None) -> Dict[str, Any]:
    """
    Places SL and/or TP orders in a single batch request.
    If qty is not provided, it will be fetched from the current position.
    Returns a dictionary with the structured order responses.
    """
    global client, IS_HEDGE_MODE
    if CONFIG["DRY_RUN"]:
        if sl_price: log.info(f"[DRY RUN] Would place SL at {sl_price:.4f} for {symbol}.")
        if tp_price: log.info(f"[DRY RUN] Would place TP at {tp_price:.4f} for {symbol}.")
        
        dry_run_id = int(time.time())
        processed_orders = {}
        if sl_price:
            processed_orders['stop_order'] = {"orderId": f"dryrun_sl_{dry_run_id}", "status": "NEW", "type": "STOP_MARKET"}
        if tp_price:
            processed_orders['tp_order'] = {"orderId": f"dryrun_tp_{dry_run_id}", "status": "NEW", "type": "TAKE_PROFIT_MARKET"}
        return processed_orders

    if client is None:
        raise RuntimeError("Binance client not initialized")

    # --- Defensive Re-check of Position Mode ---
    try:
        position_mode = client.futures_get_position_mode()
        current_hedge_mode = position_mode.get('dualSidePosition', False)
        if current_hedge_mode != IS_HEDGE_MODE:
            log.warning(f"STALE HEDGE MODE DETECTED! Global was {IS_HEDGE_MODE}, but current is {current_hedge_mode}. Updating global state.")
            send_telegram(f"⚠️ Stale hedge mode detected, correcting. Was: {IS_HEDGE_MODE}, Now: {current_hedge_mode}")
            IS_HEDGE_MODE = current_hedge_mode
    except Exception as e:
        log.error("Defensive re-check of position mode failed: %s. Proceeding with cached value.", e)
    # --- End Defensive Re-check ---

    position_side = 'LONG' if side == 'BUY' else 'SHORT'
    
    if qty is None:
        try:
            positions = client.futures_position_information(symbol=symbol)
            pos = next((p for p in positions if p.get('positionSide') == position_side), None)
            if not pos or abs(float(pos.get('positionAmt', 0.0))) == 0.0:
                # This is a critical failure, as the caller expects orders to be placed.
                raise RuntimeError(f"No open position found for {symbol} {position_side} when trying to place SL/TP.")
            current_qty = abs(float(pos.get('positionAmt')))
        except Exception as e:
            log.exception(f"Failed to fetch position info for {symbol} in place_batch_sl_tp_sync")
            raise
    else:
        current_qty = qty

    close_side = 'SELL' if side == 'BUY' else 'BUY'
    order_batch = []
    
    base_close_order = {
        'symbol': symbol,
        'side': close_side,
        'quantity': str(current_qty),
    }

    if IS_HEDGE_MODE:
        base_close_order['positionSide'] = position_side
    else:
        base_close_order['reduceOnly'] = True

    if sl_price:
        sl_order = base_close_order.copy()
        sl_order.update({
            'type': 'STOP_MARKET',
            'stopPrice': round_price(symbol, sl_price),
        })
        order_batch.append(sl_order)
    
    if tp_price:
        tp_order = base_close_order.copy()
        tp_order.update({
            'type': 'TAKE_PROFIT_MARKET',
            'stopPrice': round_price(symbol, tp_price),
        })
        order_batch.append(tp_order)

    if not order_batch:
        # This is a critical logic error if this function is called without any action to take.
        raise RuntimeError(f"place_batch_sl_tp_sync called for {symbol} without sl_price or tp_price. This should not happen.")

    try:
        log.info(f"Placing batch SL/TP order for {symbol}: {order_batch}")
        batch_response = client.futures_place_batch_order(batchOrders=order_batch)
        
        # Check for errors within the batch response
        errors = [resp for resp in batch_response if 'code' in resp]
        if errors:
            # If any order in the batch failed, log it for attention and raise an exception.
            error_details = f"Errors: {errors}"
            log.error(f"Batch SL/TP order placement failed for {symbol}. {error_details}")
            mark_attention_required_sync(symbol, "batch_order_failed", error_details)
            raise RuntimeError(f"Batch SL/TP order placement failed for {symbol}. {error_details}")
            
        log.info(f"Batch SL/TP order successful for {symbol}. Response: {batch_response}")
        
        # Process the successful response into a structured dictionary
        processed_orders = {}
        for order_resp in batch_response:
            if order_resp.get('type') == 'STOP_MARKET':
                processed_orders['stop_order'] = order_resp
            elif order_resp.get('type') == 'TAKE_PROFIT_MARKET':
                processed_orders['tp_order'] = order_resp
        
        return processed_orders
    except BinanceAPIException as e:
        log.exception("BinanceAPIException placing batch SL/TP: %s", e)
        raise
    except Exception as e:
        log.exception("Exception placing batch SL/TP: %s", e)
        raise

def close_partial_market_position_sync(symbol: str, side: str, qty_to_close: float):
    global client
    if CONFIG["DRY_RUN"]:
        log.info(f"[DRY RUN] Would close {qty_to_close} of {symbol} position.")
        return {"status": "FILLED"}

    if client is None:
        raise RuntimeError("Binance client not initialized")

    try:
        close_side = 'SELL' if side == 'BUY' else 'BUY'
        position_side = 'LONG' if side == 'BUY' else 'SHORT'

        log.info(f"Placing partial close market order: {close_side} ({position_side}) {qty_to_close} {symbol}")
        
        order_params = {
            'symbol': symbol,
            'side': close_side,
            'type': 'MARKET',
            'quantity': qty_to_close,
        }

        if IS_HEDGE_MODE:
            order_params['positionSide'] = position_side
        else:
            order_params['reduceOnly'] = True

        order = client.futures_create_order(**order_params)
        return order
    except BinanceAPIException as e:
        log.exception("BinanceAPIException closing partial position: %s", e)
        raise
    except Exception as e:
        log.exception("Exception closing partial position: %s", e)
        raise

def cancel_trade_sltp_orders_sync(trade_meta: Dict[str, Any]):
    """
    Cancels the specific SL/TP orders associated with a single trade by using stored order IDs.
    """
    global client
    if CONFIG["DRY_RUN"]:
        log.info(f"[DRY RUN] Would cancel SL/TP orders for trade {trade_meta.get('id')}.")
        return

    if client is None:
        log.warning("Cannot cancel orders, Binance client not initialized.")
        return

    symbol = trade_meta.get('symbol')
    if not symbol:
        log.error(f"Cannot cancel orders for trade {trade_meta.get('id')}, symbol is missing.")
        return

    order_ids_to_cancel = []
    sltp_orders = trade_meta.get('sltp_orders', {})

    orders_to_parse = []
    if isinstance(sltp_orders, list):
        orders_to_parse.extend(sltp_orders)
    elif isinstance(sltp_orders, dict):
        # Handle the nested structure from initial trade opening, which may contain order details
        if 'stop_order' in sltp_orders: orders_to_parse.append(sltp_orders['stop_order'])
        if 'tp_order' in sltp_orders: orders_to_parse.append(sltp_orders['tp_order'])
    
    for order in orders_to_parse:
        if isinstance(order, dict):
            order_id = order.get('orderId')
            # It's safest to only try to cancel orders that are in a pending state
            if order_id and order.get('status') in ['NEW', 'PARTIALLY_FILLED']:
                order_ids_to_cancel.append(order_id)

    # Remove duplicates
    order_ids_to_cancel = list(set(order_ids_to_cancel))

    if not order_ids_to_cancel:
        log.info(f"No valid, pending SL/TP order IDs found for trade {trade_meta.get('id')}. Attempting broad cancel for symbol as a fallback.")
        # Fallback to general cancel for safety during transition
        cancel_close_orders_sync(symbol)
        return

    try:
        log.info(f"Cancelling {len(order_ids_to_cancel)} specific orders for trade {trade_meta.get('id')} on {symbol} one by one.")
        for order_id in order_ids_to_cancel:
            try:
                client.futures_cancel_order(symbol=symbol, orderId=order_id)
            except BinanceAPIException as e:
                if e.code == -2011:
                    log.warning(f"Could not cancel order {order_id} for trade {trade_meta.get('id')} (may already be gone).")
                else:
                    raise
        
        time.sleep(0.5)
        log.info(f"Cancellation request sent for trade {trade_meta.get('id')}.")

    except BinanceAPIException as e:
        if e.code == -2011:
            log.warning(f"Some orders for trade {trade_meta.get('id')} could not be cancelled (may already be filled/cancelled): {e}")
        else:
            log.exception(f"Error batch canceling orders for trade {trade_meta.get('id')}: {e}")
    except Exception as e:
        log.exception(f"Generic error batch canceling orders for trade {trade_meta.get('id')}: {e}")


def cancel_close_orders_sync(symbol: str):
    global client
    if CONFIG["DRY_RUN"]:
        log.info(f"[DRY RUN] Would cancel all open SL/TP orders for {symbol}.")
        return

    if client is None:
        return
    try:
        orders = client.futures_get_open_orders(symbol=symbol)
        order_ids_to_cancel = [
            o['orderId'] for o in orders 
            if o.get('type') in ['STOP_MARKET', 'TAKE_PROFIT_MARKET'] or o.get('closePosition')
        ]
        
        if not order_ids_to_cancel:
            log.info(f"No open SL/TP orders to cancel for {symbol}.")
            return

        log.info(f"Cancelling {len(order_ids_to_cancel)} orders for {symbol} one by one.")
        for order_id in order_ids_to_cancel:
            try:
                client.futures_cancel_order(symbol=symbol, orderId=order_id)
            except BinanceAPIException as e:
                # Ignore error if order is already filled/cancelled
                if e.code == -2011: 
                    log.warning(f"Could not cancel order {order_id} for {symbol} (may already be gone).")
                else:
                    raise # Re-raise other API errors
        
        # Add a short delay to allow the exchange to process the cancellation
        time.sleep(1)
        log.info(f"Waited 1s for order cancellation to process for {symbol}.")

    except BinanceAPIException as e:
        # If the error is "Order does not exist", it's ok, it might have been filled or already cancelled.
        if e.code == -2011:
            log.warning(f"Some orders could not be cancelled for {symbol} (may already be filled/cancelled): {e}")
        else:
            log.exception("Error batch canceling close orders for %s: %s", symbol, e)
    except Exception as e:
        log.exception("Error batch canceling close orders for %s: %s", symbol, e)

def calculate_risk_amount(account_balance: float, strategy_id: Optional[int] = None) -> float:
    # Set defaults
    risk_pct = CONFIG["RISK_PCT_LARGE"]
    fixed_usdt = CONFIG["RISK_SMALL_FIXED_USDT"]

    # Check for strategy-specific overrides
    if strategy_id == 2:
        risk_pct = CONFIG.get("RISK_PCT_STRATEGY_2", risk_pct)
        fixed_usdt = CONFIG.get("RISK_SMALL_FIXED_USDT_STRATEGY_2", fixed_usdt)
    
    if account_balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"]:
        risk = fixed_usdt
    else:
        risk = account_balance * risk_pct
    
    max_cap = CONFIG.get("MAX_RISK_USDT", 0.0)
    if max_cap and max_cap > 0:
        risk = min(risk, max_cap)
    return float(risk)

def validate_and_sanity_check_sync(send_report: bool = True) -> Dict[str, Any]:
    results = {"ok": True, "checks": []}
    missing = []
    for name in ("BINANCE_API_KEY", "BINANCE_API_SECRET", "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHAT_ID"):
        if not globals().get(name):
            missing.append(name)
    if missing:
        results["ok"] = False
        results["checks"].append({"type": "env", "ok": False, "detail": f"Missing env: {missing}"})
    else:
        results["checks"].append({"type": "env", "ok": True})
    if client is None:
        results["ok"] = False
        results["checks"].append({"type": "binance_connect", "ok": False, "detail": "Client not initialized (check keys)"})
    else:
        results["checks"].append({"type": "binance_connect", "ok": True})
    sample_sym = CONFIG["SYMBOLS"][0].strip().upper() if CONFIG["SYMBOLS"] else None
    if sample_sym and client is not None:
        try:
            raw = client.futures_klines(symbol=sample_sym, interval=CONFIG["TIMEFRAME"], limit=120)
            cols = ['open_time','open','high','low','close','volume','close_time','qav','num_trades','taker_base','taker_quote','ignore']
            raw_df = pd.DataFrame(raw, columns=cols)
            raw_df['open_time'] = pd.to_datetime(raw_df['open_time'], unit='ms')
            raw_df['close_time'] = pd.to_datetime(raw_df['close_time'], unit='ms')
            for c in ['open','high','low','close','volume']:
                raw_df[c] = raw_df[c].astype(float)
            raw_df.set_index('close_time', inplace=True)
            
            # Calculate new indicators for the validation report
            sma_s = sma(raw_df['close'], CONFIG["SMA_LEN"])
            rsi_s = rsi(raw_df['close'], CONFIG["RSI_LEN"])
            bbu_s, bbl_s = bollinger_bands(raw_df['close'], CONFIG["STRATEGY_1"]["BB_LENGTH"], CONFIG["STRATEGY_1"]["BB_STD"])
            
            results["checks"].append({"type": "indicators_sample", "ok": True, "detail": {
                "sma": safe_last(sma_s), "rsi": safe_last(rsi_s),
                "bbu": safe_last(bbu_s), "bbl": safe_last(bbl_s)
            }})
        except Exception as e:
            results["ok"] = False
            results["checks"].append({"type": "indicators_sample", "ok": False, "detail": str(e)})
    report_lines = [f"Validation results: OK={results['ok']}"]
    for c in results["checks"]:
        report_lines.append(f"- {c['type']}: ok={c['ok']} detail={c.get('detail')}")
    report_text = "\n".join(report_lines)
    if send_report:
        send_telegram(report_text)
    return results

def candles_since_close(df: pd.DataFrame, close_time: Optional[datetime]) -> int:
    if not close_time:
        return 99999
    if close_time.tzinfo is None:
        close_time = close_time.replace(tzinfo=timezone.utc)
    return int((df.index > close_time).sum())

def fetch_klines_sync(symbol: str, interval: str, limit: int = 200) -> pd.DataFrame:
    global client
    if client is None:
        raise RuntimeError("Binance client not initialized")
    raw = client.futures_klines(symbol=symbol, interval=interval, limit=limit)
    cols = ['open_time','open','high','low','close','volume','close_time','qav','num_trades','taker_base','taker_quote','ignore']
    df = pd.DataFrame(raw, columns=cols)
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms', utc=True)
    df['close_time'] = pd.to_datetime(df['close_time'], unit='ms', utc=True)
    for c in ['open','high','low','close','volume']:
        df[c] = df[c].astype('float32')
    df.set_index('close_time', inplace=True)
    return df[['open','high','low','close','volume']]


def get_renko_data(df_raw: pd.DataFrame, symbol: str) -> Optional[pd.DataFrame]:
    """
    Converts OHLCV data to Renko bricks.
    The brick size is determined by the ATR(14) of the input data.
    """
    if df_raw is None or df_raw.empty:
        log.warning(f"Cannot generate Renko data for {symbol}, input DataFrame is empty.")
        return None

    try:
        # 1. Calculate ATR for brick size from the raw data
        atr_period = CONFIG.get("ATR_LENGTH", 14)
        atr_series = atr(df_raw, length=atr_period)
        brick_size = safe_last(atr_series)
        
        if brick_size <= 0:
            log.warning(f"Calculated Renko brick size for {symbol} is {brick_size:.4f}. Cannot generate Renko chart.")
            return None
        log.info(f"Generating Renko chart for {symbol} with dynamic ATR({atr_period}) brick size: {brick_size:.4f}")

        # 2. Prepare DataFrame for stocktrends library
        df_for_renko = df_raw.copy()
        df_for_renko.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
        
        # 3. Initialize Renko object and build bricks
        renko_processor = Renko(df_for_renko)
        renko_processor.set_brick_size(box_size=brick_size, auto=False)
        renko_df = renko_processor.get_bricks()
        
        if renko_df.empty:
            log.warning(f"Renko chart generation for {symbol} resulted in an empty DataFrame.")
            return None

        # 4. Format the Renko DataFrame for compatibility
        renko_df.set_index('date', inplace=True)
        if renko_df.index.tz is None:
            renko_df.index = renko_df.index.tz_localize('UTC')
        
        renko_df['volume'] = 0
        renko_df.index.name = 'close_time'
        
        # The library already returns 'open', 'high', 'low', 'close'.
        return renko_df[['open', 'high', 'low', 'close', 'volume']]

    except Exception as e:
        log.exception(f"An error occurred during Renko chart generation for {symbol}: {e}")
        return None


def calculate_signal_confidence(signal_candle, side: str) -> tuple[float, dict]:
    """Calculate dynamic confidence score for potential signals."""
    st_settings = CONFIG['STRATEGY_2']
    scores = {
        'primary': 0.0,
        'adx': 0.0,
        'rsi': 0.0,
        'macd': 0.0
    }
    
    # SuperTrend direction strength (Primary Signal) - 40%
    if 'atr' in signal_candle and signal_candle['atr'] > 0:
        trend_strength = abs(signal_candle['close'] - signal_candle['supertrend']) / signal_candle['atr']
        scores['primary'] = min(40.0, trend_strength * 10)
    
    # ADX confirmation (Trend Strength) - 25%
    if 'adx' in signal_candle and signal_candle['adx'] > st_settings['ADX_THRESHOLD']:
        adx_val = signal_candle['adx']
        # Scale score from 0-25 based on ADX value between threshold and 60
        adx_score = ((adx_val - st_settings['ADX_THRESHOLD']) / (60 - st_settings['ADX_THRESHOLD'])) * 25
        adx_score = max(0.0, min(25.0, adx_score))
        if (side == 'BUY' and signal_candle['+DI'] > signal_candle['-DI']) or \
           (side == 'SELL' and signal_candle['-DI'] > signal_candle['+DI']):
            scores['adx'] = adx_score
            
    # RSI confirmation (Momentum) - 20%
    if 'RSI' in signal_candle:
        rsi_val = signal_candle['RSI']
        if side == 'BUY' and st_settings['MIN_RSI_BUY'] < rsi_val < st_settings['MAX_RSI_BUY']:
            # Peak at 45, score decreases as it moves away
            rsi_score = 20.0 - abs(45 - rsi_val)
            scores['rsi'] = max(0.0, min(20.0, rsi_score))
        elif side == 'SELL' and st_settings['MIN_RSI_SELL'] < rsi_val < st_settings['MAX_RSI_SELL']:
            # Peak at 55, score decreases as it moves away
            rsi_score = 20.0 - abs(55 - rsi_val)
            scores['rsi'] = max(0.0, min(20.0, rsi_score))
    
    # MACD confirmation - 15%
    if 'MACD' in signal_candle and 'MACD_Signal' in signal_candle:
        macd_diff = abs(signal_candle['MACD'] - signal_candle['MACD_Signal'])
        if (side == 'BUY' and signal_candle['MACD'] > signal_candle['MACD_Signal']) or \
           (side == 'SELL' and signal_candle['MACD'] < signal_candle['MACD_Signal']):
            # Scale score based on MACD difference, capped at 15
            scores['macd'] = min(15.0, macd_diff * 50)

    total_score = sum(scores.values())
    return min(100.0, max(0.0, total_score)), scores


def select_strategy(df: pd.DataFrame, symbol: str) -> Optional[int]:
    """
    Determines which strategy to use for a symbol based on market conditions and configuration.
    Returns the strategy ID (1, 2, 3, 4, 5) or None if no strategy is suitable.
    """
    if df is None or df.empty:
        log.warning(f"Cannot select strategy for {symbol}, dataframe is empty.")
        return None
    last = df.iloc[-1]
    
    # --- Data validation ---
    required_cols = ['atr', 'adx', 'atr20', 'close']
    if any(pd.isna(last.get(col)) or (last.get(col) == 0 and col=='close') for col in required_cols):
        log.warning(f"Could not determine strategy for {symbol} due to missing indicator data. Skipping.")
        return None

    # --- Pre-condition Filters for each strategy ---
    s1_params = CONFIG['STRATEGY_1']
    s2_params = CONFIG['STRATEGY_2']
    s3_params = CONFIG['STRATEGY_3']
    s5_params = CONFIG.get('STRATEGY_5', {})
    
    volatility_ratio_s1 = last['atr'] / last['close'] if last['close'] > 0 else 0
    s1_allowed = volatility_ratio_s1 <= s1_params.get('MAX_VOLATILITY_FOR_ENTRY', 0.03)
    
    adx_value = last['adx']
    s2_allowed = adx_value >= s2_params.get('MIN_ADX_FOR_ENTRY', 15)

    atr20_pct = (last['atr20'] / last['close']) * 100 if last['close'] > 0 else 0
    s3_allowed = atr20_pct <= s3_params.get('VOLATILITY_MAX_ATR20_PCT', 3.0)
    
    # S4 is an evolution of S3, assume it runs under similar volatility conditions.
    s4_allowed = s3_allowed

    # S5 volatility band filter on M15
    atr_pct = (last['atr'] / last['close']) if last['close'] > 0 else 0
    s5_allowed = (s5_params and (s5_params.get('VOL_MIN_PCT', 0.003) <= atr_pct <= s5_params.get('VOL_MAX_PCT', 0.035)))

    log.info(f"Strategy selection checks for {symbol}: S1_allowed={s1_allowed}, S2_allowed={s2_allowed}, S3_allowed={s3_allowed}, S4_allowed={s4_allowed}, S5_allowed={s5_allowed}")

    # --- Strategy Selection ---
    mode = CONFIG["STRATEGY_MODE"]
    strategy_id: Optional[int] = None

    if mode == 1:
        if s1_allowed: strategy_id = 1
    elif mode == 2:
        if s2_allowed: strategy_id = 2
    elif mode == 3:
        if s3_allowed: strategy_id = 3
    elif mode == 4:
        if s4_allowed: strategy_id = 4
    elif mode == 5:
        if s5_allowed: strategy_id = 5
    elif mode == 0:  # Auto-select
        # Create a list of potential strategies to try in order of priority
        potential_strategies = []
        if s5_allowed: potential_strategies.append(5)
        if s4_allowed: potential_strategies.append(4)
        if s3_allowed: potential_strategies.append(3)
        
        # S1/S2 logic
        if s1_allowed and not s2_allowed:
            potential_strategies.append(1)
        elif not s1_allowed and s2_allowed:
            potential_strategies.append(2)
        elif s1_allowed and s2_allowed:
            if volatility_ratio_s1 > 0.015:
                potential_strategies.append(2)
                potential_strategies.append(1)
            else:
                potential_strategies.append(1)
                potential_strategies.append(2)
        
        # Iterate through potential strategies and find the first one that is not restricted
        for strat in potential_strategies:
            if strat in [3, 4, 5]:
                allowed_symbols = [
                    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "AVAXUSDT",
                    "LTCUSDT", "ADAUSDT", "XRPUSDT", "LINKUSDT", "DOTUSDT"
                ]
                if symbol not in allowed_symbols:
                    log.info(f"Auto-select: Strategy {strat} is restricted for {symbol}. Trying next.")
                    continue # Try the next strategy in the priority list
            
            # If we reach here, the strategy is not restricted (or is S1/S2)
            strategy_id = strat
            log.info(f"Auto-selecting Strategy {strategy_id} for {symbol}.")
            break # Exit the loop once a suitable strategy is found

    if not strategy_id:
        log.info(f"No suitable strategy found for {symbol} based on mode {mode} and market conditions.")
        return None

    # --- Symbol Restriction Check (for non-auto modes) ---
    if mode in [3, 4, 5] and strategy_id in [3, 4, 5]:
        allowed_symbols = [
            "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "AVAXUSDT",
            "LTCUSDT", "ADAUSDT", "XRPUSDT", "LINKUSDT", "DOTUSDT"
        ]
        if symbol not in allowed_symbols:
            log.info(f"Strategy {strategy_id} is restricted and not allowed for {symbol}. Skipping.")
            return None
            
    log.info(f"Selected Strategy {strategy_id} for {symbol}.")
    return strategy_id

def check_for_liquidity_grab(df: pd.DataFrame, symbol: str) -> bool:
    """
    Checks the 3 candles prior to the signal candle for signs of a liquidity grab.
    A liquidity grab is defined as a large candle (>1% total size) with a
    body of at least 0.5% and wicks that are at least 2x the size of the body.
    Returns True if a liquidity grab candle is found, False otherwise.
    """
    # Signal candle is at index -2. We check indices -5, -4, -3.
    if len(df) < 6: # Need at least 5 previous candles + current forming one
        return False

    # Check the three candles before the signal candle
    for i in range(-5, -2):
        candle = df.iloc[i]
        
        open_price = candle['open']
        high_price = candle['high']
        low_price = candle['low']
        close_price = candle['close']

        if open_price == 0: continue # Avoid division by zero on bad data

        # New preliminary check based on total candle size
        total_candle_size_abs = high_price - low_price
        total_candle_size_pct = (total_candle_size_abs / open_price) * 100
        
        if total_candle_size_pct <= 1.0:
            continue # Not a large candle, so not a liquidity grab. Move to the next one.

        # Original checks, now only run on large candles
        body_size_abs = abs(open_price - close_price)
        upper_wick = high_price - max(open_price, close_price)
        lower_wick = min(open_price, close_price) - low_price
        total_wick_size = upper_wick + lower_wick
        body_size_pct = (body_size_abs / open_price) * 100

        is_large_body = body_size_pct >= 0.5
        is_large_wick = body_size_abs > 0 and (total_wick_size / body_size_abs) >= 2.0

        if is_large_body and is_large_wick:
            log.info(f"Liquidity grab candle detected for {symbol} at {candle.name}. Total Size: {total_candle_size_pct:.2f}%, Body: {body_size_pct:.2f}%, Wick/Body Ratio: {total_wick_size/body_size_abs if body_size_abs > 0 else 'inf'}. Skipping trade.")
            _record_rejection(symbol, "Liquidity Grab Detected", {"candle_time": candle.name.isoformat()}, signal_candle=candle)
            return True # Found a liquidity grab candle

    return False # No liquidity grab detected


indicator_cache = {}

def calculate_all_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Pure-like function: accepts df (OHLCV), returns df with added indicator columns.
    Optimized to only calculate indicators for the selected strategy mode.
    """
    max_lookback = max(
        CONFIG.get("SMA_LEN", 200),
        CONFIG.get("STRATEGY_1", {}).get("BB_LENGTH", 20),
        CONFIG.get("ADX_PERIOD", 14),
        CONFIG.get("ATR_LENGTH", 14),
        CONFIG.get("STRATEGY_2", {}).get("SUPERTREND_PERIOD", 7),
        CONFIG.get("STRATEGY_3", {}).get("SLOW_MA", 21),
        CONFIG.get("STRATEGY_4", {}).get("ST1_PERIOD", 50),
        CONFIG.get("STRATEGY_4", {}).get("EMA_FILTER_PERIOD", 200),
    )
    if df is None or len(df) < max_lookback:
        log.warning(f"Not enough data for indicator calculation, need {max_lookback} have {len(df)}")
        return df.copy()

    out = df.copy()
    # Restrict to strategies 5, 6, 7 only per plan
    modes = [m for m in CONFIG["STRATEGY_MODE"] if m in (5, 6, 7)]
    if not modes:
        modes = [5, 6, 7]

    # ---- Common Indicators (calculated for most strategies) ----
    out['atr'] = atr(out, CONFIG["ATR_LENGTH"])
    adx(out, period=CONFIG['ADX_PERIOD'])
    out['rsi'] = rsi(out['close'], length=CONFIG['RSI_LEN'])
    macd(out)

    if 0 in modes or 1 in modes:
        # ---- Strategy 1 (BB) ----
        s1_params = CONFIG['STRATEGY_1']
        out['s1_bbu'], out['s1_bbl'] = bollinger_bands(out['close'], s1_params['BB_LENGTH'], s1_params['BB_STD'])
    
    if 0 in modes or 2 in modes:
        # ---- Strategy 2 (SuperTrend) ----
        s2_params = CONFIG['STRATEGY_2']
        out['s2_st'], out['s2_st_dir'] = supertrend(out, period=s2_params['SUPERTREND_PERIOD'], multiplier=s2_params['SUPERTREND_MULTIPLIER'])
    
    if 0 in modes or 3 in modes:
        # ---- Strategy 3 (MA Cross) ----
        s3_params = CONFIG['STRATEGY_3']
        out['s3_ma_fast'] = sma(out['close'], s3_params['FAST_MA'])
        out['s3_ma_slow'] = sma(out['close'], s3_params['SLOW_MA'])

    if 0 in modes or 4 in modes:
        # ---- Strategy 4 (3x SuperTrend) ----
        s4_params = CONFIG['STRATEGY_4']
        out['s4_st1'], out['s4_st1_dir'] = supertrend(out, period=s4_params['ST1_PERIOD'], multiplier=s4_params['ST1_MULT'])
        out['s4_st2'], out['s4_st2_dir'] = supertrend(out, period=s4_params['ST2_PERIOD'], multiplier=s4_params['ST2_MULT'])
        out['s4_st3'], out['s4_st3_dir'] = supertrend(out, period=s4_params['ST3_PERIOD'], multiplier=s4_params['ST3_MULT'])
        # Conditionally calculate the EMA filter only if it's enabled in the config
        if s4_params.get('EMA_FILTER_ENABLED', False):
            if s4_params.get('EMA_FILTER_PERIOD', 0) > 0:
                out['s4_ema_filter'] = ema(out['close'], length=s4_params['EMA_FILTER_PERIOD'])

    if 0 in modes or 5 in modes:
        # ---- Strategy 5 (M15 execution EMAs) ----
        s5 = CONFIG['STRATEGY_5']
        out['s5_m15_ema_fast'] = ema(out['close'], s5['EMA_FAST'])
        out['s5_m15_ema_slow'] = ema(out['close'], s5['EMA_SLOW'])

    return out

def _log_env_rejection(symbol: str, reason: str, details: dict):
    """
    Records a rejection for an environmental/config reason, but only if it hasn't
    been logged for the same symbol/reason in the last 5 minutes to avoid spam.
    """
    global last_env_rejection_log
    now = time.time()
    key = (symbol, reason)
    # Log only once every 5 minutes (300 seconds) for the same symbol and reason
    if now - last_env_rejection_log.get(key, 0) > 300:
        _record_rejection(symbol, reason, details)
        last_env_rejection_log[key] = now

async def evaluate_and_enter(symbol: str):
    """
    Main evaluation function. Fetches data, calculates all indicators once,
    and then dispatches to the strategy evaluation functions.
    Handles standard OHLCV and Renko data paths.
    """
    # --- Per-symbol lock to prevent race conditions ---
    lock = symbol_evaluation_locks.get(symbol)
    if not lock:
        log.warning(f"No evaluation lock found for {symbol}. Skipping.")
        return

    if lock.locked():
        log.info(f"Evaluation for {symbol} is already in progress. Skipping.")
        return

    async with lock:
        log.info("Evaluating symbol: %s", symbol)
        global running, frozen, symbol_loss_cooldown, symbol_trade_cooldown, managed_trades, pending_limit_orders

        # --- Pre-evaluation checks ---
        if frozen or not running:
            _log_env_rejection(symbol, "Bot Paused", {"running": running, "frozen": frozen})
            return
        if symbol in symbol_trade_cooldown and datetime.now(timezone.utc) < symbol_trade_cooldown[symbol]:
            _log_env_rejection(symbol, "Post-Trade Cooldown", {"ends_at": symbol_trade_cooldown[symbol].strftime('%H:%M:%S')})
            return
        if symbol in symbol_loss_cooldown and datetime.now(timezone.utc) < symbol_loss_cooldown[symbol]:
            _log_env_rejection(symbol, "Loss Cooldown", {"ends_at": symbol_loss_cooldown[symbol].strftime('%H:%M:%S')})
            return
        async with managed_trades_lock, pending_limit_orders_lock:
            if not CONFIG["HEDGING_ENABLED"] and any(t['symbol'] == symbol for t in managed_trades.values()):
                _log_env_rejection(symbol, "Position Already Open", {"symbol": symbol})
                return
            if any(p['symbol'] == symbol for p in pending_limit_orders.values()):
                _log_env_rejection(symbol, "Pending Order Exists", {"symbol": symbol})
                return
            open_trades = len(managed_trades) + len(pending_limit_orders)
            if open_trades >= CONFIG["MAX_CONCURRENT_TRADES"]:
                _log_env_rejection(symbol, "Max Trades Reached", {"open": open_trades, "max": CONFIG["MAX_CONCURRENT_TRADES"]})
                return

        try:
            modes = CONFIG["STRATEGY_MODE"]
            run_s4 = 4 in modes or 0 in modes
            run_s5 = 5 in modes or 0 in modes
            run_s6 = 6 in modes or 0 in modes
            run_others = any(m in modes for m in [1, 2, 3, 5, 6, 7, 8, 9]) or 0 in modes

            # Fetch a larger dataset if S4/Renko is active, otherwise default.
            limit = 1000 if run_s4 else 250
            df_raw = await asyncio.to_thread(fetch_klines_sync, symbol, CONFIG["TIMEFRAME"], limit)

            if df_raw is None or df_raw.empty:
                log.warning(f"fetch_klines_sync returned empty for {symbol}. Skipping all evaluations.")
                return

            # --- S4 Renko Path ---
            if run_s4:
                renko_df = await asyncio.to_thread(get_renko_data, df_raw.copy(), symbol)
                if renko_df is not None and not renko_df.empty:
                    df_s4 = await asyncio.to_thread(calculate_all_indicators, renko_df)
                    if df_s4 is not None and not df_s4.empty:
                        await evaluate_strategy_4(symbol, df_s4)
                else:
                    log.warning(f"Skipping S4 evaluation for {symbol} due to empty Renko data.")

            # --- Standard OHLCV Path for other strategies ---
            if run_others:
                df_standard = await asyncio.to_thread(calculate_all_indicators, df_raw)
                if df_standard is not None and not df_standard.empty:
                    if 1 in modes or 0 in modes:
                        await evaluate_strategy_bb(symbol, df_standard)
                    if 2 in modes or 0 in modes:
                        await evaluate_strategy_supertrend(symbol, df_standard)
                    if 3 in modes or 0 in modes:
                        await evaluate_strategy_3(symbol, df_standard)
                    if run_s5:
                        await evaluate_strategy_5(symbol, df_standard)
                    if run_s6:
                        await evaluate_strategy_6(symbol, df_standard)
                    if 7 in modes or 0 in modes:
                        await evaluate_strategy_7(symbol, df_standard)
                    if 8 in modes or 0 in modes:
                        await evaluate_strategy_8(symbol, df_standard)
                    if 9 in modes or 0 in modes:
                        await evaluate_strategy_9(symbol, df_standard)
                else:
                    log.warning(f"Skipping S1/S2/S3/S5/S6/S7/S8/S9 evaluation for {symbol} due to empty indicator data.")
        except Exception as e:
            await asyncio.to_thread(log_and_send_error, f"Failed to evaluate symbol {symbol} for a new trade", e)


def simulate_strategy_bb(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Simulation version of the Bollinger Band strategy.
    Returns signal details if a signal is found, otherwise None.
    """
    if df is None or len(df) < 20:
        return None

    adx_threshold = CONFIG.get("S1_ADX_MAX", 25)
    atr_mult_for_sl = CONFIG.get("S1_ATR_SL_MULT", 1.5)
    fallback_sl_pct = CONFIG.get("S1_FALLBACK_SL_PCT", 0.01)

    signal_candle = df.iloc[-2]
    prev_candle = df.iloc[-3]

    side = None
    if signal_candle['close'] <= signal_candle['s1_bbl']:
        side = 'BUY'
    elif signal_candle['close'] >= signal_candle['s1_bbu']:
        side = 'SELL'
    else:
        return None

    if (side == 'BUY' and prev_candle['close'] <= prev_candle['open']) or \
       (side == 'SELL' and prev_candle['close'] >= prev_candle['open']):
        return None

    adx_value = float(signal_candle.get('adx', 0.0))
    if adx_value >= adx_threshold:
        return None

    entry_price = float(signal_candle['close'])
    atr_val = float(signal_candle.get('atr', 0.0))
    
    if atr_val > 0:
        sl_price = entry_price - atr_mult_for_sl * atr_val if side == 'BUY' else entry_price + atr_mult_for_sl * atr_val
    else:
        sl_price = entry_price * (1 - fallback_sl_pct) if side == 'BUY' else entry_price * (1 + fallback_sl_pct)
        
    distance = abs(entry_price - sl_price)
    if distance <= 0:
        return None
        
    take_price = entry_price + (2 * distance) if side == 'BUY' else entry_price - (2 * distance)
    
    return {
        "strategy": "S1-BB",
        "side": side,
        "entry_price": entry_price,
        "sl_price": sl_price,
        "tp_price": take_price,
        "timestamp": signal_candle.name.isoformat()
    }


async def evaluate_strategy_bb(symbol: str, df: pd.DataFrame):
    """
    Simple Bollinger strategy. Now includes logic for placing test orders.
    """
    if df is None or df.empty:
        return

    # --- Normal Signal Logic ---
    if len(df) < 20:
        _record_rejection(symbol, "S1-Not enough bars for S1", {"len": len(df)})
        return
    
    signal_candle = df.iloc[-2]
    prev_candle = df.iloc[-3]

    if 's1_bbl' not in signal_candle or 's1_bbu' not in signal_candle or pd.isna(signal_candle['s1_bbl']) or pd.isna(signal_candle['s1_bbu']):
        _record_rejection(symbol, "S1-BBands not ready", {}, signal_candle=signal_candle)
        return

    adx_threshold = CONFIG.get("S1_ADX_MAX", 25)
    atr_mult_for_sl = CONFIG.get("S1_ATR_SL_MULT", 1.5)
    fallback_sl_pct = CONFIG.get("S1_FALLBACK_SL_PCT", 0.01)

    side = None
    if signal_candle['close'] <= signal_candle['s1_bbl']:
        side = 'BUY'
    elif signal_candle['close'] >= signal_candle['s1_bbu']:
        side = 'SELL'
    else:
        _record_rejection(symbol, "S1-Not a BB signal", {"close": signal_candle['close'], "bbu": signal_candle['s1_bbu'], "bbl": signal_candle['s1_bbl']}, signal_candle=signal_candle)
        return

    if side == 'BUY' and prev_candle['close'] <= prev_candle['open']:
        _record_rejection(symbol, "S1-Prev candle not bullish", {"close": prev_candle['close'], "open": prev_candle['open']})
        return
    if side == 'SELL' and prev_candle['close'] >= prev_candle['open']:
        _record_rejection(symbol, "S1-Prev candle not bearish", {"close": prev_candle['close'], "open": prev_candle['open']})
        return

    adx_value = float(df['adx'].iloc[-2]) if 'adx' in df.columns else None
    if adx_value is not None and adx_value >= adx_threshold:
        _record_rejection(symbol, "S1-ADX too strong", {"adx": adx_value, "threshold": adx_threshold}, signal_candle=signal_candle)
        return

    entry_price = float(signal_candle['close'])
    atr_val = float(df['atr'].iloc[-2]) if 'atr' in df.columns else None
    if atr_val and atr_val > 0:
        sl_price = entry_price - atr_mult_for_sl * atr_val if side == 'BUY' else entry_price + atr_mult_for_sl * atr_val
    else:
        sl_price = entry_price * (1 - fallback_sl_pct) if side == 'BUY' else entry_price * (1 + fallback_sl_pct)

    distance = abs(entry_price - sl_price)
    if distance <= 0:
        _record_rejection(symbol, "S1-Zero distance for sizing", {"entry": entry_price, "sl": sl_price}, signal_candle=signal_candle)
        return

    balance = await asyncio.to_thread(get_account_balance_usdt)
    risk_usdt = calculate_risk_amount(balance, strategy_id=1)
    ideal_qty = risk_usdt / distance
    ideal_qty = await asyncio.to_thread(round_qty, symbol, ideal_qty, rounding=ROUND_DOWN)

    min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
    qty_min = min_notional / entry_price if entry_price > 0 else 0.0
    qty_min = await asyncio.to_thread(round_qty, symbol, qty_min, rounding=ROUND_CEILING)
    final_qty = max(ideal_qty, qty_min)

    if final_qty <= 0:
        _record_rejection(symbol, "S1-Qty zero after sizing", {"ideal": ideal_qty, "min": qty_min}, signal_candle=signal_candle)
        return

    notional = final_qty * entry_price
    margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else risk_usdt
    leverage = int(math.floor(notional / max(margin_to_use, 1e-9)))
    max_leverage = min(CONFIG.get("MAX_BOT_LEVERAGE", 30), get_max_leverage(symbol))
    leverage = max(1, min(leverage, max_leverage))
    take_price = entry_price + (2 * distance) if side == 'BUY' else entry_price - (2 * distance)

    limit_order_resp = await asyncio.to_thread(place_limit_order_sync, symbol, side, final_qty, entry_price)
    order_id = str(limit_order_resp.get('orderId'))
    pending_order_id = f"{symbol}_{order_id}"

    candle_duration = timeframe_to_timedelta(CONFIG['TIMEFRAME'])
    expiry_candles = CONFIG.get("ORDER_EXPIRY_CANDLES", 2)
    expiry_time = df.index[-1] + (candle_duration * (expiry_candles - 1))

    pending_meta = {
        "id": pending_order_id, "order_id": order_id, "symbol": symbol,
        "side": side, "qty": final_qty, "limit_price": entry_price,
        "stop_price": sl_price, "take_price": take_price, "leverage": leverage,
        "risk_usdt": risk_usdt, "place_time": datetime.utcnow().isoformat(),
        "expiry_time": expiry_time.isoformat(),
        "strategy_id": 1,
        "atr_at_entry": atr_val,
        "trailing": CONFIG["TRAILING_ENABLED"]
    }
    
    async with pending_limit_orders_lock:
        pending_limit_orders[pending_order_id] = pending_meta
        await asyncio.to_thread(add_pending_order_to_db, pending_meta)

    log.info(f"Placed pending limit order (S1-BB): {pending_meta}")
    title = "⏳ *New Pending Order: S1-BB*"
    new_order_msg = (
        f"{title}\n\n"
        f"**Symbol:** `{symbol}`\n"
        f"**Side:** `{side}`\n"
        f"**Price:** `{entry_price:.4f}`\n"
        f"**Qty:** `{final_qty}`\n"
        f"**Risk:** `{risk_usdt:.2f} USDT`\n"
        f"**Leverage:** `{leverage}x`"
    )
    await asyncio.to_thread(send_telegram, new_order_msg, parse_mode='Markdown')

def simulate_strategy_supertrend(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df is None or len(df) < 30: return None
    atr_buf = CONFIG.get("S2_ATR_BUFFER_MULT", 0.5)
    signal_candle = df.iloc[-2]
    prev_candle = df.iloc[-3]
    if 's2_st' not in prev_candle or 's2_st' not in signal_candle or pd.isna(prev_candle['s2_st']) or pd.isna(signal_candle['s2_st']):
        _record_rejection(symbol, "S2-SuperTrend not ready", {}, signal_candle=signal_candle)
        return

    prev_close_vs_st = float(prev_candle['close']) - float(prev_candle['s2_st'])
    sig_close_vs_st = float(signal_candle['close']) - float(signal_candle['s2_st'])
    side = None
    if prev_close_vs_st < 0 and sig_close_vs_st > 0: side = 'BUY'
    elif prev_close_vs_st > 0 and sig_close_vs_st < 0: side = 'SELL'
    else: return None
    if (side == 'BUY' and prev_candle['close'] <= prev_candle['open']) or \
       (side == 'SELL' and prev_candle['close'] >= prev_candle['open']): return None
    base_sl = float(signal_candle['s2_st'])
    atr_val = float(signal_candle.get('atr', 0.0))
    sl_price = base_sl - atr_buf * atr_val if side == 'BUY' else base_sl + atr_buf * atr_val
    entry_price = float(signal_candle['close'])
    distance = abs(entry_price - sl_price)
    if distance <= 0: return None
    tp_distance = atr_val * CONFIG["STRATEGY_EXIT_PARAMS"]['2']["ATR_MULTIPLIER"] * 1.5
    take_price = entry_price + tp_distance if side == 'BUY' else entry_price - tp_distance
    return {"strategy": "S2-ST", "side": side, "entry_price": entry_price, "sl_price": sl_price, "tp_price": take_price, "timestamp": signal_candle.name.isoformat()}

async def evaluate_strategy_supertrend(symbol: str, df: pd.DataFrame):
    """
    Simple SuperTrend strategy. Now includes logic for placing test orders.
    """
    if df is None or df.empty:
        return

    # --- Normal Signal Logic ---
    if len(df) < 30:
        _record_rejection(symbol, "S2-Not enough bars for S2", {"len": len(df)})
        return

    atr_buf = CONFIG.get("S2_ATR_BUFFER_MULT", 0.5)

    signal_candle = df.iloc[-2]
    prev_candle = df.iloc[-3]

    prev_close_vs_st = float(prev_candle['close']) - float(prev_candle['s2_st'])
    sig_close_vs_st = float(signal_candle['close']) - float(signal_candle['s2_st'])

    side = None
    if prev_close_vs_st < 0 and sig_close_vs_st > 0:
        side = 'BUY'
    elif prev_close_vs_st > 0 and sig_close_vs_st < 0:
        side = 'SELL'
    else:
        _record_rejection(symbol, "S2-No ST flip", {"prev_vs_st": prev_close_vs_st, "sig_vs_st": sig_close_vs_st}, signal_candle=signal_candle)
        return

    if side == 'BUY' and prev_candle['close'] <= prev_candle['open']:
        _record_rejection(symbol, "S2-Prev candle not bullish", {"close": prev_candle['close'], "open": prev_candle['open']})
        return
    if side == 'SELL' and prev_candle['close'] >= prev_candle['open']:
        _record_rejection(symbol, "S2-Prev candle not bearish", {"close": prev_candle['close'], "open": prev_candle['open']})
        return

    base_sl = float(signal_candle['s2_st'])
    atr_val = float(df['atr'].iloc[-2]) if 'atr' in df.columns else None
    if atr_val and atr_val > 0:
        sl_price = base_sl - atr_buf * atr_val if side == 'BUY' else base_sl + atr_buf * atr_val
    else:
        sl_price = base_sl

    entry_price = float(signal_candle['close'])
    distance = abs(entry_price - sl_price)
    if distance <= 0:
        _record_rejection(symbol, "S2-Zero distance for sizing", {"entry": entry_price, "sl": sl_price}, signal_candle=signal_candle)
        return

    balance = await asyncio.to_thread(get_account_balance_usdt)
    risk_usdt = calculate_risk_amount(balance, strategy_id=2)
    ideal_qty = risk_usdt / distance
    ideal_qty = await asyncio.to_thread(round_qty, symbol, ideal_qty, rounding=ROUND_DOWN)

    min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
    qty_min = min_notional / entry_price if entry_price > 0 else 0.0
    qty_min = await asyncio.to_thread(round_qty, symbol, qty_min, rounding=ROUND_CEILING)
    final_qty = max(ideal_qty, qty_min)

    if final_qty <= 0:
        _record_rejection(symbol, "S2-Qty zero after sizing", {"ideal": ideal_qty, "min": qty_min}, signal_candle=signal_candle)
        return

    notional = final_qty * entry_price
    margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else risk_usdt
    leverage = int(math.floor(notional / max(margin_to_use, 1e-9)))
    max_leverage = min(CONFIG.get("MAX_BOT_LEVERAGE", 30), get_max_leverage(symbol))
    leverage = max(1, min(leverage, max_leverage))
    tp_distance = atr_val * CONFIG["STRATEGY_EXIT_PARAMS"]['2']["ATR_MULTIPLIER"] * 1.5
    take_price = entry_price + tp_distance if side == 'BUY' else entry_price - tp_distance

    limit_order_resp = await asyncio.to_thread(place_limit_order_sync, symbol, side, final_qty, entry_price)
    order_id = str(limit_order_resp.get('orderId'))
    pending_order_id = f"{symbol}_{order_id}"

    candle_duration = timeframe_to_timedelta(CONFIG['TIMEFRAME'])
    expiry_candles = CONFIG.get("ORDER_EXPIRY_CANDLES", 2)
    expiry_time = df.index[-1] + (candle_duration * (expiry_candles - 1))

    pending_meta = {
        "id": pending_order_id, "order_id": order_id, "symbol": symbol,
        "side": side, "qty": final_qty, "limit_price": entry_price,
        "stop_price": sl_price, "take_price": take_price, "leverage": leverage,
        "risk_usdt": risk_usdt, "place_time": datetime.utcnow().isoformat(),
        "expiry_time": expiry_time.isoformat(),
        "strategy_id": 2,
        "atr_at_entry": atr_val,
        "signal_confidence": 100.0,
        "trailing": CONFIG["TRAILING_ENABLED"]
    }
    
    async with pending_limit_orders_lock:
        pending_limit_orders[pending_order_id] = pending_meta
        await asyncio.to_thread(add_pending_order_to_db, pending_meta)

    log.info(f"Placed pending limit order (S2-SuperTrend): {pending_meta}")
    title = "⏳ *New Pending Order: S2-ST*"
    new_order_msg = (
        f"{title}\n\n"
        f"**Symbol:** `{symbol}`\n"
        f"**Side:** `{side}`\n"
        f"**Price:** `{entry_price:.4f}`\n"
        f"**Qty:** `{final_qty}`\n"
        f"**Risk:** `{risk_usdt:.2f} USDT`\n"
        f"**Leverage:** `{leverage}x`"
    )
    await asyncio.to_thread(send_telegram, new_order_msg, parse_mode='Markdown')

def simulate_strategy_3(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    if df is None or len(df) < 50: return None
    atr_mult = CONFIG.get("S3_ATR_SL_MULT", 1.5)
    fallback_sl_pct = CONFIG.get("S3_FALLBACK_SL_PCT", 0.015)
    sig = df.iloc[-2]
    prev = df.iloc[-3]
    side = None
    if prev['s3_ma_fast'] < prev['s3_ma_slow'] and sig['s3_ma_fast'] > sig['s3_ma_slow']: side = 'BUY'
    elif prev['s3_ma_fast'] > prev['s3_ma_slow'] and sig['s3_ma_fast'] < sig['s3_ma_slow']: side = 'SELL'
    else: return None
    if (side == 'BUY' and prev['close'] <= prev['open']) or \
       (side == 'SELL' and prev['close'] >= prev['open']): return None
    atr_val = float(sig.get('atr', 0.0))
    entry_price = float(sig['close'])
    if atr_val and atr_val > 0:
        sl_price = entry_price - atr_mult * atr_val if side == 'BUY' else entry_price + atr_mult * atr_val
    else:
        sl_price = entry_price * (1 - fallback_sl_pct) if side == 'BUY' else entry_price * (1 + fallback_sl_pct)
    distance = abs(entry_price - sl_price)
    if distance <= 0: return None
    take_price = entry_price + (2 * distance) if side == 'BUY' else entry_price - (2 * distance)
    return {"strategy": "S3-MA", "side": side, "entry_price": entry_price, "sl_price": sl_price, "tp_price": take_price, "timestamp": sig.name.isoformat()}

async def evaluate_strategy_3(symbol: str, df: pd.DataFrame):
    """
    Simple moving-average strategy (S3). Now includes logic for placing test orders.
    """
    if df is None or df.empty:
        return

    # --- Normal Signal Logic ---
    if len(df) < 50:
        _record_rejection(symbol, "S3-Not enough bars for S3", {"len": len(df)})
        return

    atr_mult = CONFIG.get("S3_ATR_SL_MULT", 1.5)
    fallback_sl_pct = CONFIG.get("S3_FALLBACK_SL_PCT", 0.015)

    sig = df.iloc[-2]
    prev = df.iloc[-3]

    side = None
    if 's3_ma_fast' not in prev or 's3_ma_slow' not in prev or 's3_ma_fast' not in sig or 's3_ma_slow' not in sig or \
       pd.isna(prev['s3_ma_fast']) or pd.isna(prev['s3_ma_slow']) or pd.isna(sig['s3_ma_fast']) or pd.isna(sig['s3_ma_slow']):
        _record_rejection(symbol, "S3-MAs not ready", {}, signal_candle=sig)
        return

    if prev['s3_ma_fast'] < prev['s3_ma_slow'] and sig['s3_ma_fast'] > sig['s3_ma_slow']:
        side = 'BUY'
    elif prev['s3_ma_fast'] > prev['s3_ma_slow'] and sig['s3_ma_fast'] < sig['s3_ma_slow']:
        side = 'SELL'
    else:
        _record_rejection(symbol, "S3-No MA cross", {"prev_fast": prev['s3_ma_fast'], "prev_slow": prev['s3_ma_slow'], "sig_fast": sig['s3_ma_fast'], "sig_slow": sig['s3_ma_slow']}, signal_candle=sig)
        return

    if side == 'BUY' and prev['close'] <= prev['open']:
        _record_rejection(symbol, "S3-Prev candle not bullish", {"close": prev['close'], "open": prev['open']})
        return
    if side == 'SELL' and prev['close'] >= prev['open']:
        _record_rejection(symbol, "S3-Prev candle not bearish", {"close": prev['close'], "open": prev['open']})
        return

    atr_val = float(df['atr'].iloc[-2]) if 'atr' in df.columns else None
    entry_price = float(sig['close'])
    if atr_val and atr_val > 0:
        sl_price = entry_price - atr_mult * atr_val if side == 'BUY' else entry_price + atr_mult * atr_val
    else:
        sl_price = entry_price * (1 - fallback_sl_pct) if side == 'BUY' else entry_price * (1 + fallback_sl_pct)

    distance = abs(entry_price - sl_price)
    if distance <= 0:
        _record_rejection(symbol, "S3-Zero distance for sizing", {"entry": entry_price, "sl": sl_price}, signal_candle=sig)
        return

    balance = await asyncio.to_thread(get_account_balance_usdt)
    risk_usdt = calculate_risk_amount(balance, strategy_id=3)
    ideal_qty = risk_usdt / distance
    ideal_qty = await asyncio.to_thread(round_qty, symbol, ideal_qty, rounding=ROUND_DOWN)

    min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
    qty_min = min_notional / entry_price if entry_price > 0 else 0.0
    qty_min = await asyncio.to_thread(round_qty, symbol, qty_min, rounding=ROUND_CEILING)
    final_qty = max(ideal_qty, qty_min)

    if final_qty <= 0:
        _record_rejection(symbol, "S3-Qty zero after sizing", {"ideal": ideal_qty, "min": qty_min}, signal_candle=sig)
        return

    notional = final_qty * entry_price
    margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else risk_usdt
    leverage = int(math.floor(notional / max(margin_to_use, 1e-9)))
    max_leverage = min(CONFIG.get("MAX_BOT_LEVERAGE", 30), get_max_leverage(symbol))
    leverage = max(1, min(leverage, max_leverage))
    take_price = entry_price + (2 * distance) if side == 'BUY' else entry_price - (2 * distance)

    limit_order_resp = await asyncio.to_thread(place_limit_order_sync, symbol, side, final_qty, entry_price)
    order_id = str(limit_order_resp.get('orderId'))
    pending_order_id = f"{symbol}_{order_id}"

    candle_duration = timeframe_to_timedelta(CONFIG['TIMEFRAME'])
    expiry_candles = CONFIG.get("ORDER_EXPIRY_CANDLES", 2)
    expiry_time = df.index[-1] + (candle_duration * (expiry_candles - 1))

    pending_meta = {
        "id": pending_order_id, "order_id": order_id, "symbol": symbol,
        "side": side, "qty": final_qty, "limit_price": entry_price,
        "stop_price": sl_price, "take_price": take_price, "leverage": leverage,
        "risk_usdt": risk_usdt, "place_time": datetime.utcnow().isoformat(),
        "expiry_time": expiry_time.isoformat(),
        "strategy_id": 3,
        "atr_at_entry": atr_val,
        "trailing": CONFIG["TRAILING_ENABLED"]
    }
    
    async with pending_limit_orders_lock:
        pending_limit_orders[pending_order_id] = pending_meta
        await asyncio.to_thread(add_pending_order_to_db, pending_meta)

    log.info(f"Placed pending limit order (S3-MA): {pending_meta}")
    title = "⏳ *New Pending Order: S3-MA*"
    new_order_msg = (
        f"{title}\n\n"
        f"**Symbol:** `{symbol}`\n"
        f"**Side:** `{side}`\n"
        f"**Price:** `{entry_price:.4f}`\n"
        f"**Qty:** `{final_qty}`\n"
        f"**Risk:** `{risk_usdt:.2f} USDT`\n"
        f"**Leverage:** `{leverage}x`"
    )
    await asyncio.to_thread(send_telegram, new_order_msg, parse_mode='Markdown')

def simulate_strategy_4(symbol: str, df: pd.DataFrame) -> Optional[Dict[str, Any]]:
    """
    Simulation version of the 3x SuperTrend strategy (S4).
    Returns signal details if a signal is found, otherwise None.
    """
    s4_params = CONFIG['STRATEGY_4']
    
    # --- Indicator & Data Check ---
    required_cols = ['s4_st1_dir', 's4_st2_dir', 's4_st3_dir', 's4_st2', 'open', 'close']
    if len(df) < 4 or any(col not in df.columns for col in required_cols):
        return None

    # --- Define Candles ---
    signal_candle = df.iloc[-2]
    prev_candle = df.iloc[-3]
    current_candle = df.iloc[-1]
    entry_price = current_candle['open']

    # --- Signal Detection ---
    side = None
    # BUY Signal: All three STs are bullish now, and at least one was bearish before.
    all_buy_now = (signal_candle['s4_st1_dir'] == 1 and 
                   signal_candle['s4_st2_dir'] == 1 and 
                   signal_candle['s4_st3_dir'] == 1)
    any_sell_before = (prev_candle['s4_st1_dir'] == -1 or 
                       prev_candle['s4_st2_dir'] == -1 or 
                       prev_candle['s4_st3_dir'] == -1)

    if all_buy_now and any_sell_before:
        side = 'BUY'
    
    # SELL Signal: All three STs are bearish now, and at least one was bullish before.
    all_sell_now = (signal_candle['s4_st1_dir'] == -1 and 
                    signal_candle['s4_st2_dir'] == -1 and 
                    signal_candle['s4_st3_dir'] == -1)
    any_buy_before = (prev_candle['s4_st1_dir'] == 1 or 
                      prev_candle['s4_st2_dir'] == 1 or 
                      prev_candle['s4_st3_dir'] == 1)
    
    if all_sell_now and any_buy_before:
        side = 'SELL'

    if not side:
        return None
        
    # --- Passed all filters, return signal ---
    sl_price = signal_candle['s4_st2']
    
    # S4 has no predefined TP, so set it to 0
    tp_price = 0 
    
    return {
        "strategy": "S4-3ST",
        "side": side,
        "entry_price": entry_price,
        "sl_price": sl_price,
        "tp_price": tp_price,
        "timestamp": signal_candle.name.isoformat()
    }

async def evaluate_strategy_4(symbol: str, df: pd.DataFrame, test_signal: Optional[str] = None, full_test: bool = False):
    """
    Evaluates and executes trades based on the 3x SuperTrend strategy (S4).
    Logic:
    1. Wait for the slow SuperTrend (S3/s4_st1) to flip to establish a trend direction.
    2. Once the trend is established, wait for the first candle where all three STs agree.
    3. Take the trade and reset the state, waiting for the next S3 flip.
    """
    global s4_confirmation_state, managed_trades, symbol_trade_cooldown
    if df is None or df.empty:
        return

    # --- Pre-Trade Checks ---
    s4_params = CONFIG['STRATEGY_4']
    async with managed_trades_lock, pending_limit_orders_lock:
        if not CONFIG["HEDGING_ENABLED"] and any(t['symbol'] == symbol for t in managed_trades.values()):
            return
        if any(p['symbol'] == symbol for p in pending_limit_orders.values()):
            return
    
    required_cols = ['s4_st1_dir', 's4_st2_dir', 's4_st3_dir', 's4_st2', 'open', 'close']
    if len(df) < 4 or any(col not in df.columns for col in required_cols) or df[required_cols].iloc[-4:].isnull().values.any():
        log.warning(f"S4: DataFrame for {symbol} is missing required columns or data for evaluation. Kline len: {len(df)}")
        return

    side = None
    signal_candle = df.iloc[-2]
    prev_candle = df.iloc[-3]

    # --- Primary EMA Trend Filter ---
    allowed_side = 'BOTH' # Default to allowing both sides
    if s4_params.get('EMA_FILTER_ENABLED', False):
        ema_period = s4_params.get('EMA_FILTER_PERIOD', 0)
        if ema_period > 0 and 's4_ema_filter' in df.columns:
            # This is intentional: we check the CURRENT open price against the EMA
            # of the CLOSED signal candle to ensure the trend is still valid for entry.
            entry_price_check = df.iloc[-1]['open']
            ema_value = signal_candle['s4_ema_filter']
            
            candle_open = signal_candle['open']
            candle_close = signal_candle['close']

            if pd.isna(ema_value) or pd.isna(entry_price_check) or pd.isna(candle_open) or pd.isna(candle_close):
                _record_rejection(symbol, "S4 EMA Filter Not Ready", {"ema_period": ema_period, "ema_val": ema_value}, signal_candle=signal_candle)
                return
            
            is_cross = min(candle_open, candle_close) < ema_value < max(candle_open, candle_close)

            if is_cross:
                _record_rejection(symbol, "S4 Price crossing EMA", {"open": candle_open, "close": candle_close, "ema": ema_value}, signal_candle=signal_candle)
                return
            elif entry_price_check > ema_value:
                allowed_side = 'BUY'
            elif entry_price_check < ema_value:
                allowed_side = 'SELL'
        # If filter is enabled but period is 0 or indicator is missing, we don't restrict the side.

    side = None
    # --- Test Order Logic ---
    if test_signal:
        side = test_signal
        log.info(f"S4 TEST ORDER: Bypassing signal logic for side: {side}")
    else:
        # --- Stateful Confluence Logic ---
        state = s4_confirmation_state[symbol]
        
        # Determine the current trend from the slow SuperTrend on the signal candle
        slow_st_is_buy = signal_candle['s4_st1_dir'] == 1
        slow_st_is_sell = signal_candle['s4_st1_dir'] == -1

        # Check if the trend has changed since the previous candle
        st1_flipped_buy = prev_candle['s4_st1_dir'] == -1 and slow_st_is_buy
        st1_flipped_sell = prev_candle['s4_st1_dir'] == 1 and slow_st_is_sell
        
        # If the trend flips, we reset the `trade_taken` flag for the new trend direction,
        # allowing one trade to be taken in this new trend cycle.
        if st1_flipped_buy:
            log.info(f"S4: Slow ST flipped to BUY for {symbol}. Resetting trade-taken flag.")
            state['buy_trade_taken'] = False
        elif st1_flipped_sell:
            log.info(f"S4: Slow ST flipped to SELL for {symbol}. Resetting trade-taken flag.")
            state['sell_trade_taken'] = False

        # --- Trade Trigger Logic ---
        # A trade can be taken if the trend is established and a trade for this trend hasn't been taken yet.
        
        # Check for BUY signal
        if allowed_side in ['BUY', 'BOTH'] and slow_st_is_buy and not state.get('buy_trade_taken', False):
            all_buy_now = (signal_candle['s4_st1_dir'] == 1 and signal_candle['s4_st2_dir'] == 1 and signal_candle['s4_st3_dir'] == 1)
            if all_buy_now:
                side = 'BUY'
                state['buy_trade_taken'] = True # Mark that we've taken a trade for this buy trend
                log.info(f"S4 Signal: BUY confluence detected for {symbol}.")
            else:
                _record_rejection(symbol, "S4 Awaiting Buy Confluence", {"st1": signal_candle['s4_st1_dir'], "st2": signal_candle['s4_st2_dir'], "st3": signal_candle['s4_st3_dir']}, signal_candle=signal_candle)
        
        # Check for SELL signal
        elif allowed_side in ['SELL', 'BOTH'] and slow_st_is_sell and not state.get('sell_trade_taken', False):
            all_sell_now = (signal_candle['s4_st1_dir'] == -1 and signal_candle['s4_st2_dir'] == -1 and signal_candle['s4_st3_dir'] == -1)
            if all_sell_now:
                side = 'SELL'
                state['sell_trade_taken'] = True # Mark that we've taken a trade for this sell trend
                log.info(f"S4 Signal: SELL confluence detected for {symbol}.")
            else:
                _record_rejection(symbol, "S4 Awaiting Sell Confluence", {"st1": signal_candle['s4_st1_dir'], "st2": signal_candle['s4_st2_dir'], "st3": signal_candle['s4_st3_dir']}, signal_candle=signal_candle)

    # --- Trade Execution ---
    if not side:
        return # No trade signal on this candle, exit.
        
    log.info(f"S4 Stateful Signal PASSED for {symbol}, side: {side}")
    
    current_candle = df.iloc[-1] 
    entry_price = current_candle['open']
    stop_loss_price = signal_candle['s4_st2']
    risk_usd = s4_params['RISK_USD']
    
    price_distance = abs(entry_price - stop_loss_price)
    if price_distance <= 0:
        _record_rejection(symbol, "S4 Invalid SL Distance", {"entry": entry_price, "sl": stop_loss_price}, signal_candle=signal_candle)
        return

    ideal_qty = risk_usd / price_distance
    ideal_qty = await asyncio.to_thread(round_qty, symbol, ideal_qty, rounding=ROUND_DOWN)

    min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
    qty_min = min_notional / entry_price if entry_price > 0 else 0.0
    qty_min = await asyncio.to_thread(round_qty, symbol, qty_min, rounding=ROUND_CEILING)

    if ideal_qty < qty_min:
        _record_rejection(
            symbol, "S4 Risk Too Low", 
            {"risk_usd": risk_usd, "ideal_qty": ideal_qty, "min_qty": qty_min, "entry": entry_price, "sl": stop_loss_price},
            signal_candle=signal_candle
        )
        return

    final_qty = ideal_qty
    if final_qty <= 0:
        _record_rejection(symbol, "S4 Qty Zero", {"ideal_qty": ideal_qty, "min_qty": qty_min}, signal_candle=signal_candle)
        return

    notional = final_qty * entry_price
    balance = await asyncio.to_thread(get_account_balance_usdt)
    actual_risk_usdt = final_qty * price_distance
    margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else actual_risk_usdt
    
    uncapped_leverage = int(math.ceil(notional / max(margin_to_use, 1e-9)))
    max_leverage_exchange = get_max_leverage(symbol)
    max_leverage_config = CONFIG.get("MAX_BOT_LEVERAGE", 30)
    max_leverage = min(max_leverage_config, max_leverage_exchange)
    leverage = max(1, min(uncapped_leverage, max_leverage))

    log.info(
        f"Leverage Calculation for {symbol} | Side: {side}\n"
        f"  - Notional Value: {notional:.4f} | Margin to Use (Risk): {margin_to_use:.4f}\n"
        f"  - Uncapped Leverage: {uncapped_leverage}x | Final Capped Leverage: {leverage}x"
    )

    try:
        log.info(f"S4: Placing MARKET {side} order for {final_qty} {symbol} at ~{entry_price}")

        # --- Set post-trade cooldown immediately to prevent duplicates ---
        symbol_trade_cooldown[symbol] = datetime.now(timezone.utc) + timedelta(minutes=16)
        log.info(f"S4: Set 16-minute post-trade cooldown for {symbol} to prevent duplicates before placing order.")

        await asyncio.to_thread(open_market_position_sync, symbol, side, final_qty, leverage)

        pos = None
        for i in range(10):
            await asyncio.sleep(0.5)
            positions = await asyncio.to_thread(client.futures_position_information, symbol=symbol)
            position_side = 'LONG' if side == 'BUY' else 'SHORT'
            pos = next((p for p in positions if p.get('positionSide') == position_side and float(p.get('positionAmt', 0)) != 0), None)
            if pos:
                log.info(f"Position for {symbol} found after {i+1} attempts.")
                break
        
        if not pos:
            raise RuntimeError(f"Position for {symbol} not found after S4 market order (waited 5s).")
        
        actual_entry_price = float(pos['entryPrice'])
        actual_qty = abs(float(pos['positionAmt']))
        
        sltp_orders = await asyncio.to_thread(place_batch_sl_tp_sync, symbol, side, sl_price=stop_loss_price, qty=actual_qty)

        trade_id = f"{symbol}_S4_{int(time.time())}"
        meta = {
            "id": trade_id, "symbol": symbol, "side": side, "entry_price": actual_entry_price,
            "initial_qty": actual_qty, "qty": actual_qty, "notional": actual_qty * actual_entry_price,
            "leverage": leverage, "sl": stop_loss_price, "tp": 0,
            "open_time": datetime.utcnow().isoformat(), "sltp_orders": sltp_orders,
            "risk_usdt": actual_risk_usdt, "strategy_id": 4,
        }

        async with managed_trades_lock:
            managed_trades[trade_id] = meta
        await asyncio.to_thread(add_managed_trade_to_db, meta)

        new_trade_msg = (
            f"✅ *New Trade Opened: S4 (Sequential)*\n\n"
            f"**Symbol:** `{symbol}`\n"
            f"**Side:** `{side}`\n"
            f"**Entry:** `{actual_entry_price:.4f}`\n"
            f"**Initial SL (ST2):** `{stop_loss_price:.4f}`\n"
            f"**Risk:** `{actual_risk_usdt:.2f} USDT`\n"
            f"**Leverage:** `{leverage}x`"
        )
        await asyncio.to_thread(send_telegram, new_trade_msg, parse_mode='Markdown')

    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"Failed to execute S4 trade for {symbol}", e)

async def evaluate_strategy_5(symbol: str, df_m15: pd.DataFrame):
    """
    Strategy 5: Advanced crypto-futures strategy
    - H1 trend via EMA(21/55) and SuperTrend(10,3) filter
    - M15 execution with EMA pullback + momentum/volume filter
    - Initial stop: structure/ATR hybrid
    - Risk: fixed USDT like S4 (CONFIG['STRATEGY_5']['RISK_USD'])
    - Management: handled in monitor thread (BE at +0.5R, 30% at 1R, ATR/swing trailing)
    """
    try:
        s5 = CONFIG['STRATEGY_5']

        # Restrict to selected majors only (configurable)
        s5_allowed = s5.get("SYMBOLS")
        if isinstance(s5_allowed, str):
            s5_allowed = [x.strip().upper() for x in s5_allowed.split(",") if x.strip()]
        if not s5_allowed or not isinstance(s5_allowed, (list, tuple)):
            # Default to a safe expanded majors list if not provided
            s5_allowed = [
                "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "AVAXUSDT",
                "LTCUSDT", "ADAUSDT", "XRPUSDT", "LINKUSDT", "DOTUSDT"
            ]
        if symbol not in s5_allowed:
            _record_rejection(symbol, "S5-Restricted symbol", {"allowed": ",".join(s5_allowed)})
            return

        # Basic data checks
        if df_m15 is None or len(df_m15) < 80:
            _record_rejection(symbol, "S5-Not enough M15 data", {"len": len(df_m15) if df_m15 is not None else 0})
            return

        # Compute additional M15 indicators
        df_m15 = df_m15.copy()
        # Use Wilder ATR for stop sizing consistency
        df_m15['s5_atr'] = atr_wilder(df_m15, int(s5.get('ATR_PERIOD', 14)))
        df_m15['s5_rsi'] = rsi(df_m15['close'], int(s5.get('RSI_PERIOD', 14)))
        df_m15['s5_vol_ma10'] = df_m15['volume'].rolling(10).mean()

        sig = df_m15.iloc[-2]
        prev = df_m15.iloc[-3]

        # Volatility band filter (M15 ATR%)
        atr_pct = (sig['s5_atr'] / sig['close']) if sig['close'] > 0 else 0
        if not (s5['VOL_MIN_PCT'] <= atr_pct <= s5['VOL_MAX_PCT']):
            _record_rejection(symbol, "S5-ATR pct out of band", {"atr_pct": atr_pct})
            return

        # Fetch H1 data for trend filter
        df_h1 = await asyncio.to_thread(fetch_klines_sync, symbol, '1h', 300)
        if df_h1 is None or len(df_h1) < 80:
            _record_rejection(symbol, "S5-Not enough H1 data", {"len": len(df_h1) if df_h1 is not None else 0})
            return

        df_h1 = df_h1.copy()
        df_h1['ema_fast'] = ema(df_h1['close'], s5['EMA_FAST'])
        df_h1['ema_slow'] = ema(df_h1['close'], s5['EMA_SLOW'])
        df_h1['st_h1'], df_h1['st_h1_dir'] = supertrend(df_h1, period=s5['H1_ST_PERIOD'], multiplier=s5['H1_ST_MULT'])
        h1_last = df_h1.iloc[-2]  # last closed H1

        # H1 trend direction
        h1_bull = (h1_last['ema_fast'] > h1_last['ema_slow']) and (h1_last['close'] > h1_last['st_h1'])
        h1_bear = (h1_last['ema_fast'] < h1_last['ema_slow']) and (h1_last['close'] < h1_last['st_h1'])

        # M15 execution filters
        m15_bull_pullback = (sig['s5_m15_ema_fast'] >= sig['s5_m15_ema_slow']) and (prev['low'] <= prev['s5_m15_ema_fast']) and (sig['close'] > sig['s5_m15_ema_fast']) and (sig['close'] > sig['open'])
        m15_bear_pullback = (sig['s5_m15_ema_fast'] <= sig['s5_m15_ema_slow']) and (prev['high'] >= prev['s5_m15_ema_fast']) and (sig['close'] < sig['s5_m15_ema_fast']) and (sig['close'] < sig['open'])

        vol_spike = (sig['volume'] >= 1.2 * sig['s5_vol_ma10']) if pd.notna(sig['s5_vol_ma10']) else False
        rsi_ok_long = 35 <= sig['s5_rsi'] <= 65
        rsi_ok_short = 35 <= sig['s5_rsi'] <= 65

        side = None
        if h1_bull and m15_bull_pullback and vol_spike and rsi_ok_long:
            side = 'BUY'
        elif h1_bear and m15_bear_pullback and vol_spike and rsi_ok_short:
            side = 'SELL'
        else:
            _record_rejection(symbol, "S5-No confluence", {"h1_bull": h1_bull, "h1_bear": h1_bear, "vol_spike": bool(vol_spike), "rsi": float(sig['s5_rsi'])})
            return

        # Entry at signal close (limit)
        entry_price = float(sig['close'])

        # Initial stop: structure + ATR hybrid (choose the stricter/closer invalidation)
        swing_lookback = 5
        if side == 'BUY':
            tech_inval = float(df_m15['low'].iloc[-swing_lookback:].min())
            atr_stop = entry_price - 1.5 * float(sig['s5_atr'])
            # For BUY, choose the higher (closer to entry) of ATR stop and swing low
            stop_price = max(atr_stop, tech_inval)
        else:  # SELL
            tech_inval = float(df_m15['high'].iloc[-swing_lookback:].max())
            atr_stop = entry_price + 1.5 * float(sig['s5_atr'])
            # For SELL, choose the lower (closer to entry) of ATR stop and swing high
            stop_price = min(atr_stop, tech_inval)

        distance = abs(entry_price - stop_price)
        if distance <= 0 or not np.isfinite(distance):
            _record_rejection(symbol, "S5-Invalid SL distance", {"entry": entry_price, "sl": stop_price})
            return

        # Risk model: same as S4 (fixed USDT risk)
        risk_usd = float(s5['RISK_USD'])
        ideal_qty = risk_usd / distance
        ideal_qty = await asyncio.to_thread(round_qty, symbol, ideal_qty, rounding=ROUND_DOWN)

        # Enforce min notional
        min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
        qty_min = min_notional / entry_price if entry_price > 0 else 0.0
        qty_min = await asyncio.to_thread(round_qty, symbol, qty_min, rounding=ROUND_CEILING)

        if ideal_qty < qty_min or ideal_qty <= 0:
            _record_rejection(symbol, "S5-Qty below minimum", {"ideal": ideal_qty, "min": qty_min})
            return

        final_qty = ideal_qty
        notional = final_qty * entry_price

        balance = await asyncio.to_thread(get_account_balance_usdt)
        actual_risk_usdt = final_qty * distance
        margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else actual_risk_usdt
        uncapped_leverage = int(math.ceil(notional / max(margin_to_use, 1e-9)))
        max_leverage = min(CONFIG.get("MAX_BOT_LEVERAGE", 30), get_max_leverage(symbol))
        leverage = max(1, min(uncapped_leverage, max_leverage))

        # Place LIMIT order with SL only (TP handled by monitor logic)
        limit_order_resp = await asyncio.to_thread(place_limit_order_sync, symbol, side, final_qty, entry_price)
        order_id = str(limit_order_resp.get('orderId'))
        pending_order_id = f"{symbol}_{order_id}"

        candle_duration = timeframe_to_timedelta(CONFIG['TIMEFRAME'])
        expiry_candles = CONFIG.get("ORDER_EXPIRY_CANDLES", 2)
        expiry_time = df_m15.index[-1] + (candle_duration * (expiry_candles - 1))

        pending_meta = {
            "id": pending_order_id, "order_id": order_id, "symbol": symbol,
            "side": side, "qty": final_qty, "limit_price": entry_price,
            "stop_price": stop_price, "take_price": None, "leverage": leverage,
            "risk_usdt": actual_risk_usdt, "place_time": datetime.utcnow().isoformat(),
            "expiry_time": expiry_time.isoformat(),
            "strategy_id": 5,
            "atr_at_entry": float(sig['s5_atr']),
            "trailing": True
        }

        async with pending_limit_orders_lock:
            pending_limit_orders[pending_order_id] = pending_meta
            await asyncio.to_thread(add_pending_order_to_db, pending_meta)

        title = "⏳ *New Pending Order: S5-Adv*"
        new_order_msg = (
            f"{title}\n\n"
            f"**Symbol:** `{symbol}`\n"
            f"**Side:** `{side}`\n"
            f"**Price:** `{entry_price:.4f}`\n"
            f"**Qty:** `{final_qty}`\n"
            f"**Risk:** `{actual_risk_usdt:.2f} USDT`\n"
            f"**Leverage:** `{leverage}x`"
        )
        await asyncio.to_thread(send_telegram, new_order_msg, parse_mode='Markdown')

    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"S5 evaluation error for {symbol}", e)
        return

    return

    # --- Pre-Trade Checks ---
    s4_params = CONFIG['STRATEGY_4']
    async with managed_trades_lock, pending_limit_orders_lock:
        if not CONFIG["HEDGING_ENABLED"] and any(t['symbol'] == symbol for t in managed_trades.values()):
            return
        if any(p['symbol'] == symbol for p in pending_limit_orders.values()):
            return
    
    required_cols = ['s4_st1_dir', 's4_st2_dir', 's4_st3_dir', 's4_st2', 'open', 'close']
    if len(df) < 4 or any(col not in df.columns for col in required_cols) or df[required_cols].iloc[-4:].isnull().values.any():
        log.warning(f"S4: DataFrame for {symbol} is missing required columns or data for evaluation. Kline len: {len(df)}")
        return

    side = None
    signal_candle = df.iloc[-2]
    prev_candle = df.iloc[-3]

    # --- Primary EMA Trend Filter ---
    s4_params = CONFIG['STRATEGY_4']
    allowed_side = 'BOTH' # Default to allowing both sides
    if s4_params.get('EMA_FILTER_ENABLED', False):
        ema_period = s4_params.get('EMA_FILTER_PERIOD', 0)
        if ema_period > 0 and 's4_ema_filter' in df.columns:
            # This is intentional: we check the CURRENT open price against the EMA
            # of the CLOSED signal candle to ensure the trend is still valid for entry.
            entry_price_check = df.iloc[-1]['open']
            ema_value = signal_candle['s4_ema_filter']
            
            candle_open = signal_candle['open']
            candle_close = signal_candle['close']

            if pd.isna(ema_value) or pd.isna(entry_price_check) or pd.isna(candle_open) or pd.isna(candle_close):
                _record_rejection(symbol, "S4 EMA Filter Not Ready", {"ema_period": ema_period, "ema_val": ema_value}, signal_candle=signal_candle)
                return
            
            is_cross = min(candle_open, candle_close) < ema_value < max(candle_open, candle_close)

            if is_cross:
                _record_rejection(symbol, "S4 Price crossing EMA", {"open": candle_open, "close": candle_close, "ema": ema_value}, signal_candle=signal_candle)
                return
            elif entry_price_check > ema_value:
                allowed_side = 'BUY'
            elif entry_price_check < ema_value:
                allowed_side = 'SELL'
        # If filter is enabled but period is 0 or indicator is missing, we don't restrict the side.

    side = None
    # --- Test Order Logic ---
    if test_signal:
        side = test_signal
        log.info(f"S4 TEST ORDER: Bypassing signal logic for side: {side}")
    else:
        # --- Stateful Confluence Logic ---
        state = s4_confirmation_state[symbol]
        
        # Determine the current trend from the slow SuperTrend on the signal candle
        slow_st_is_buy = signal_candle['s4_st1_dir'] == 1
        slow_st_is_sell = signal_candle['s4_st1_dir'] == -1

        # Check if the trend has changed since the previous candle
        st1_flipped_buy = prev_candle['s4_st1_dir'] == -1 and slow_st_is_buy
        st1_flipped_sell = prev_candle['s4_st1_dir'] == 1 and slow_st_is_sell
        
        # If the trend flips, we reset the `trade_taken` flag for the new trend direction,
        # allowing one trade to be taken in this new trend cycle.
        if st1_flipped_buy:
            log.info(f"S4: Slow ST flipped to BUY for {symbol}. Resetting trade-taken flag.")
            state['buy_trade_taken'] = False
        elif st1_flipped_sell:
            log.info(f"S4: Slow ST flipped to SELL for {symbol}. Resetting trade-taken flag.")
            state['sell_trade_taken'] = False

        # --- Trade Trigger Logic ---
        # A trade can be taken if the trend is established and a trade for this trend hasn't been taken yet.
        
        # Check for BUY signal
        if allowed_side in ['BUY', 'BOTH'] and slow_st_is_buy and not state.get('buy_trade_taken', False):
            all_buy_now = (signal_candle['s4_st1_dir'] == 1 and signal_candle['s4_st2_dir'] == 1 and signal_candle['s4_st3_dir'] == 1)
            if all_buy_now:
                side = 'BUY'
                state['buy_trade_taken'] = True # Mark that we've taken a trade for this buy trend
                log.info(f"S4 Signal: BUY confluence detected for {symbol}.")
            else:
                _record_rejection(symbol, "S4 Awaiting Buy Confluence", {"st1": signal_candle['s4_st1_dir'], "st2": signal_candle['s4_st2_dir'], "st3": signal_candle['s4_st3_dir']}, signal_candle=signal_candle)
        
        # Check for SELL signal
        elif allowed_side in ['SELL', 'BOTH'] and slow_st_is_sell and not state.get('sell_trade_taken', False):
            all_sell_now = (signal_candle['s4_st1_dir'] == -1 and signal_candle['s4_st2_dir'] == -1 and signal_candle['s4_st3_dir'] == -1)
            if all_sell_now:
                side = 'SELL'
                state['sell_trade_taken'] = True # Mark that we've taken a trade for this sell trend
                log.info(f"S4 Signal: SELL confluence detected for {symbol}.")
            else:
                _record_rejection(symbol, "S4 Awaiting Sell Confluence", {"st1": signal_candle['s4_st1_dir'], "st2": signal_candle['s4_st2_dir'], "st3": signal_candle['s4_st3_dir']}, signal_candle=signal_candle)

    # --- Trade Execution ---
    if not side:
        return # No trade signal on this candle, exit.
        
    log.info(f"S4 Stateful Signal PASSED for {symbol}, side: {side}")
    
    current_candle = df.iloc[-1] 
    entry_price = current_candle['open']
    stop_loss_price = signal_candle['s4_st2']
    risk_usd = s4_params['RISK_USD']
    
    price_distance = abs(entry_price - stop_loss_price)
    if price_distance <= 0:
        _record_rejection(symbol, "S4 Invalid SL Distance", {"entry": entry_price, "sl": stop_loss_price}, signal_candle=signal_candle)
        return

    ideal_qty = risk_usd / price_distance
    ideal_qty = await asyncio.to_thread(round_qty, symbol, ideal_qty, rounding=ROUND_DOWN)

    min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
    qty_min = min_notional / entry_price if entry_price > 0 else 0.0
    qty_min = await asyncio.to_thread(round_qty, symbol, qty_min, rounding=ROUND_CEILING)

    if ideal_qty < qty_min:
        _record_rejection(
            symbol, "S4 Risk Too Low", 
            {"risk_usd": risk_usd, "ideal_qty": ideal_qty, "min_qty": qty_min, "entry": entry_price, "sl": stop_loss_price},
            signal_candle=signal_candle
        )
        return

    final_qty = ideal_qty
    if final_qty <= 0:
        _record_rejection(symbol, "S4 Qty Zero", {"ideal_qty": ideal_qty, "min_qty": qty_min}, signal_candle=signal_candle)
        return

    notional = final_qty * entry_price
    balance = await asyncio.to_thread(get_account_balance_usdt)
    actual_risk_usdt = final_qty * price_distance
    margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else actual_risk_usdt
    
    uncapped_leverage = int(math.ceil(notional / max(margin_to_use, 1e-9)))
    max_leverage_exchange = get_max_leverage(symbol)
    max_leverage_config = CONFIG.get("MAX_BOT_LEVERAGE", 30)
    max_leverage = min(max_leverage_config, max_leverage_exchange)
    leverage = max(1, min(uncapped_leverage, max_leverage))

    log.info(
        f"Leverage Calculation for {symbol} | Side: {side}\n"
        f"  - Notional Value: {notional:.4f} | Margin to Use (Risk): {margin_to_use:.4f}\n"
        f"  - Uncapped Leverage: {uncapped_leverage}x | Final Capped Leverage: {leverage}x"
    )

    try:
        log.info(f"S4: Placing MARKET {side} order for {final_qty} {symbol} at ~{entry_price}")

        # --- Set post-trade cooldown immediately to prevent duplicates ---
        symbol_trade_cooldown[symbol] = datetime.now(timezone.utc) + timedelta(minutes=16)
        log.info(f"S4: Set 16-minute post-trade cooldown for {symbol} to prevent duplicates before placing order.")

        await asyncio.to_thread(open_market_position_sync, symbol, side, final_qty, leverage)

        pos = None
        for i in range(10):
            await asyncio.sleep(0.5)
            positions = await asyncio.to_thread(client.futures_position_information, symbol=symbol)
            position_side = 'LONG' if side == 'BUY' else 'SHORT'
            pos = next((p for p in positions if p.get('positionSide') == position_side and float(p.get('positionAmt', 0)) != 0), None)
            if pos:
                log.info(f"Position for {symbol} found after {i+1} attempts.")
                break
        
        if not pos:
            raise RuntimeError(f"Position for {symbol} not found after S4 market order (waited 5s).")
        
        actual_entry_price = float(pos['entryPrice'])
        actual_qty = abs(float(pos['positionAmt']))
        
        sltp_orders = await asyncio.to_thread(place_batch_sl_tp_sync, symbol, side, sl_price=stop_loss_price, qty=actual_qty)

        trade_id = f"{symbol}_S4_{int(time.time())}"
        meta = {
            "id": trade_id, "symbol": symbol, "side": side, "entry_price": actual_entry_price,
            "initial_qty": actual_qty, "qty": actual_qty, "notional": actual_qty * actual_entry_price,
            "leverage": leverage, "sl": stop_loss_price, "tp": 0,
            "open_time": datetime.utcnow().isoformat(), "sltp_orders": sltp_orders,
            "risk_usdt": actual_risk_usdt, "strategy_id": 4,
        }

        async with managed_trades_lock:
            managed_trades[trade_id] = meta
        await asyncio.to_thread(add_managed_trade_to_db, meta)

        new_trade_msg = (
            f"✅ *New Trade Opened: S4 (Sequential)*\n\n"
            f"**Symbol:** `{symbol}`\n"
            f"**Side:** `{side}`\n"
            f"**Entry:** `{actual_entry_price:.4f}`\n"
            f"**Initial SL (ST2):** `{stop_loss_price:.4f}`\n"
            f"**Risk:** `{actual_risk_usdt:.2f} USDT`\n"
            f"**Leverage:** `{leverage}x`"
        )
        await asyncio.to_thread(send_telegram, new_trade_msg, parse_mode='Markdown')

    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"Failed to execute S4 trade for {symbol}", e)



# ------------- Strategy 6 (Price-Action Only) helpers -------------
def _s6_in_session_window(ts: pd.Timestamp) -> bool:
    try:
        s6 = CONFIG['STRATEGY_6']
        start_h = int(s6.get('SESSION_START_UTC_HOUR', 7))
        end_h = int(s6.get('SESSION_END_UTC_HOUR', 15))
        hour = ts.tz_convert('UTC').hour if ts.tzinfo is not None else ts.hour
        if start_h <= end_h:
            return start_h <= hour < end_h
        # overnight window
        return hour >= start_h or hour < end_h
    except Exception:
        return True

def _s6_prev_daily_levels(df_d: pd.DataFrame) -> tuple[float, float]:
    if df_d is None or len(df_d) < 2:
        return float('nan'), float('nan')
    prev = df_d.iloc[-2]
    return float(prev['high']), float(prev['low'])

def _s6_recent_swings(df: pd.DataFrame, lookback: int = 10) -> tuple[float, float]:
    if df is None or len(df) < max(lookback, 3):
        return float('nan'), float('nan')
    sw_high = float(df['high'].iloc[-lookback:].max())
    sw_low = float(df['low'].iloc[-lookback:].min())
    return sw_high, sw_low

def _s6_trend_from_swings(df_htf: pd.DataFrame, swing_lookback: int = 10) -> Optional[str]:
    # Determine simple structure: compare last two swing highs/lows
    if df_htf is None or len(df_htf) < swing_lookback + 5:
        return None
    highs = df_htf['high'].iloc[-(swing_lookback+5):]
    lows = df_htf['low'].iloc[-(swing_lookback+5):]
    last_high = float(highs.iloc[-1]); prev_high = float(highs.iloc[-3])
    last_low = float(lows.iloc[-1]); prev_low = float(lows.iloc[-3])
    is_bull = last_high >= prev_high and last_low >= prev_low
    is_bear = last_high <= prev_high and last_low <= prev_low
    if is_bull and not is_bear:
        return 'BULL'
    if is_bear and not is_bull:
        return 'BEAR'
    return None

def _s6_is_pin_bar(candle: pd.Series, direction: str) -> bool:
    o = float(candle['open']); h = float(candle['high']); l = float(candle['low']); c = float(candle['close'])
    rng = max(1e-9, h - l)
    body = abs(c - o)
    upper_wick = h - max(o, c)
    lower_wick = min(o, c) - l
    wick_ratio = (lower_wick if direction == 'BUY' else upper_wick) / rng
    # wick >= 60% of range and close inside prior range handled elsewhere
    return wick_ratio >= 0.60 and body / rng <= 0.40

def _s6_is_engulfing_reclaim(curr: pd.Series, prev: pd.Series, direction: str, poi_price: float) -> bool:
    # Engulfing body and reclaim the POI
    co, cc = float(curr['open']), float(curr['close'])
    po, pc = float(prev['open']), float(prev['close'])
    if direction == 'BUY':
        engulf = cc > co and cc >= max(po, pc) and co <= min(po, pc)
        reclaim = cc > poi_price
        return engulf and reclaim
    else:
        engulf = cc < co and cc <= min(po, pc) and co >= max(po, pc)
        reclaim = cc < poi_price
        return engulf and reclaim

def _s6_follow_through_ok(rej: pd.Series, ft: pd.Series, direction: str, vol_ma: float, ratio: float) -> bool:
    rej_range = float(rej['high'] - rej['low'])
    ft_range = float(ft['high'] - ft['low'])
    in_dir = (direction == 'BUY' and ft['close'] > ft['open']) or (direction == 'SELL' and ft['close'] < ft['open'])
    vol_ok = float(ft['volume']) >= float(vol_ma) if np.isfinite(vol_ma) and vol_ma > 0 else False
    range_ok = ft_range >= ratio * rej_range if np.isfinite(rej_range) else False
    return in_dir and (range_ok or vol_ok)

def _s6_within_poi(candle: pd.Series, poi_level: float, atr_val: float) -> bool:
    # consider touch if wick crosses within 0.25*ATR of the POI level
    h = float(candle['high']); l = float(candle['low'])
    tol = 0.25 * float(atr_val)
    return (l <= poi_level <= h) or (abs(h - poi_level) <= tol) or (abs(l - poi_level) <= tol)

async def evaluate_strategy_6(symbol: str, df_m15: pd.DataFrame):
    """
    Strategy 6: Price-Action Only — Single High-Probability Trade per Day
    - HTF bias from Daily first, H4 must not contradict
    - POI retest (Daily/H4 swing or previous Daily High/Low)
    - M15 rejection candle (pin bar or engulfing reclaim) at POI
    - Follow-through confirmation by next candle
    - Entry: limit at rejection close; SL beyond wick +/- 0.25*ATR(14)
    - Risk: same fixed USD model as S4
    - One setup per symbol per day
    """
    try:
        s6 = CONFIG['STRATEGY_6']

        # Restrict S6 to the majors set unless overridden via env
        allowed_s6 = s6.get("SYMBOLS", [])
        if isinstance(allowed_s6, str):
            allowed_s6 = [x.strip().upper() for x in allowed_s6.split(",") if x.strip()]
        if not allowed_s6 or not isinstance(allowed_s6, (list, tuple)):
            allowed_s6 = [
                "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "AVAXUSDT",
                "LTCUSDT", "ADAUSDT", "XRPUSDT", "LINKUSDT", "DOTUSDT"
            ]
        if symbol not in allowed_s6:
            _record_rejection(symbol, "S6-Restricted symbol", {"allowed": ",".join(allowed_s6)})
            return

        if df_m15 is None or len(df_m15) < 80:
            _record_rejection(symbol, "S6-Not enough M15 data", {"len": len(df_m15) if df_m15 is not None else 0})
            return

        # Session window check on last closed candle time
        signal_ts = df_m15.index[-2]
        if not _s6_in_session_window(signal_ts):
            _record_rejection(symbol, "S6-Outside session window", {"ts": str(signal_ts)})
            return

        # Prepare M15 indicators
        df_m15 = df_m15.copy()
        atr_period = s6.get('ATR_PERIOD', 14)
        df_m15['s6_atr'] = df_m15.get('s6_atr_m15', atr(df_m15, atr_period))
        df_m15['s6_vol_ma'] = df_m15.get('s6_vol_ma', df_m15['volume'].rolling(int(s6.get('VOL_MA_LEN',10))).mean())

        sig = df_m15.iloc[-2]; prev = df_m15.iloc[-3]

        # Fetch HTF data
        df_h4 = await asyncio.to_thread(fetch_klines_sync, symbol, '4h', 400)
        df_d = await asyncio.to_thread(fetch_klines_sync, symbol, '1d', 400)
        if df_h4 is None or df_d is None or len(df_h4) < 50 or len(df_d) < 50:
            _record_rejection(symbol, "S6-Not enough HTF data", {"h4": len(df_h4) if df_h4 is not None else 0, "d": len(df_d) if df_d is not None else 0})
            return

        # HTF bias
        bias_d = _s6_trend_from_swings(df_d, swing_lookback=20)
        bias_h4 = _s6_trend_from_swings(df_h4, swing_lookback=20)
        if bias_d is None:
            _record_rejection(symbol, "S6-Daily bias unclear", {})
            return
        if bias_h4 is not None and bias_h4 != bias_d:
            _record_rejection(symbol, "S6-H4 contradicts Daily", {"daily": bias_d, "h4": bias_h4})
            return

        direction = 'BUY' if bias_d == 'BULL' else 'SELL'

        # Determine POIs: prior Daily H/L and recent Daily/H4 swing H/L
        y_high, y_low = _s6_prev_daily_levels(df_d)
        h4_sw_high, h4_sw_low = _s6_recent_swings(df_h4, lookback=30)
        d_sw_high, d_sw_low = _s6_recent_swings(df_d, lookback=30)

        candidate_levels = []
        if np.isfinite(y_high): candidate_levels.append(y_high)
        if np.isfinite(y_low): candidate_levels.append(y_low)
        for lvl in (h4_sw_high, h4_sw_low, d_sw_high, d_sw_low):
            if np.isfinite(lvl): candidate_levels.append(lvl)

        # Find a POI that was touched by the signal candle
        atr_sig = float(sig['s6_atr'])
        touched_pois = [lvl for lvl in candidate_levels if _s6_within_poi(sig, lvl, atr_sig)]
        if not touched_pois:
            _record_rejection(symbol, "S6-No POI touch on signal candle", {})
            return
        # Pick the closest POI to close
        poi_level = min(touched_pois, key=lambda lvl: abs(float(sig['close']) - lvl))

        # Rejection candle definition
        # 1) Pin bar with wick >=60% and close inside prior 3-bar range
        prior3_high = float(df_m15['high'].iloc[-5:-2].max())
        prior3_low = float(df_m15['low'].iloc[-5:-2].min())
        close_inside_prior3 = prior3_low <= float(sig['close']) <= prior3_high
        is_pin = _s6_is_pin_bar(sig, direction) and close_inside_prior3

        # 2) Full engulfing reclaim of POI
        is_engulf = _s6_is_engulfing_reclaim(sig, prev, direction, poi_level)

        if not (is_pin or is_engulf):
            _record_rejection(symbol, "S6-No valid rejection candle", {"pin": is_pin, "engulf": is_engulf})
            return

        # Follow-through confirmation by next candle (or same if strong)
        ft = df_m15.iloc[-1]
        vol_ma = float(df_m15['s6_vol_ma'].iloc[-2]) if pd.notna(df_m15['s6_vol_ma'].iloc[-2]) else float('nan')
        ratio = float(s6.get('FOLLOW_THROUGH_RANGE_RATIO', 0.7))
        if not _s6_follow_through_ok(sig, ft, direction, vol_ma, ratio):
            _record_rejection(symbol, "S6-No follow-through", {"vol_ma": vol_ma, "ratio": ratio})
            return

        # Entry and SL
        entry_price = float(sig['close'])  # limit at rejection close
        if direction == 'BUY':
            wick_low = float(sig['low'])
            stop_price = wick_low - s6.get('ATR_BUFFER_MULT', 0.25) * atr_sig
            side = 'BUY'
        else:
            wick_high = float(sig['high'])
            stop_price = wick_high + s6.get('ATR_BUFFER_MULT', 0.25) * atr_sig
            side = 'SELL'

        # Distance and sizing (S4 risk model)
        distance = abs(entry_price - stop_price)
        if distance <= 0 or not np.isfinite(distance):
            _record_rejection(symbol, "S6-Invalid SL distance", {"entry": entry_price, "sl": stop_price})
            return

        risk_usd = float(s6.get('RISK_USD', CONFIG['STRATEGY_4']['RISK_USD']))
        ideal_qty = risk_usd / distance
        ideal_qty = await asyncio.to_thread(round_qty, symbol, ideal_qty, rounding=ROUND_DOWN)

        min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
        qty_min = await asyncio.to_thread(round_qty, symbol, (min_notional / entry_price) if entry_price > 0 else 0.0, rounding=ROUND_CEILING)
        if ideal_qty < qty_min or ideal_qty <= 0:
            _record_rejection(symbol, "S6-Qty below minimum", {"ideal": ideal_qty, "min": qty_min})
            return

        final_qty = ideal_qty
        notional = final_qty * entry_price
        balance = await asyncio.to_thread(get_account_balance_usdt)
        actual_risk_usdt = final_qty * distance
        margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else actual_risk_usdt
        uncapped_leverage = int(math.ceil(notional / max(margin_to_use, 1e-9)))
        max_leverage = min(CONFIG.get("MAX_BOT_LEVERAGE", 30), get_max_leverage(symbol))
        leverage = max(1, min(uncapped_leverage, max_leverage))

        # Place limit order
        limit_order_resp = await asyncio.to_thread(place_limit_order_sync, symbol, side, final_qty, entry_price)
        order_id = str(limit_order_resp.get('orderId'))
        pending_order_id = f"{symbol}_{order_id}"

        candle_duration = timeframe_to_timedelta(CONFIG['TIMEFRAME'])
        expiry_candles = int(s6.get("LIMIT_EXPIRY_CANDLES", 3))
        expiry_time = df_m15.index[-1] + (candle_duration * (expiry_candles - 1))

        pending_meta = {
            "id": pending_order_id, "order_id": order_id, "symbol": symbol,
            "side": side, "qty": final_qty, "limit_price": entry_price,
            "stop_price": stop_price, "take_price": None, "leverage": leverage,
            "risk_usdt": actual_risk_usdt, "place_time": datetime.utcnow().isoformat(),
            "expiry_time": expiry_time.isoformat(),
            "strategy_id": 6,
            "atr_at_entry": float(sig['s6_atr']),
            "trailing": True,
            "s6_poi_level": poi_level,
        }

        async with pending_limit_orders_lock:
            pending_limit_orders[pending_order_id] = pending_meta
            await asyncio.to_thread(add_pending_order_to_db, pending_meta)

        # Enforce one trade per day if configured: cooldown until UTC end of day
        if s6.get('ENFORCE_ONE_TRADE_PER_DAY', True):
            now = datetime.now(timezone.utc)
            eod = datetime(now.year, now.month, now.day, 23, 59, 59, tzinfo=timezone.utc)
            symbol_trade_cooldown[symbol] = eod
            log.info(f"S6: Set end-of-day cooldown for {symbol} to enforce one setup per day.")

        title = "⏳ New Pending Order: S6-PA"
        new_order_msg = (
            f"{title}\n\n"
            f"Symbol: `{symbol}`\n"
            f"Side: `{side}`\n"
            f"Price: `{entry_price:.4f}`\n"
            f"Qty: `{final_qty}`\n"
            f"Risk: `{actual_risk_usdt:.2f} USDT`\n"
            f"Leverage: `{leverage}x`"
        )
        await asyncio.to_thread(send_telegram, new_order_msg, parse_mode='Markdown')
    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"S6 evaluation error for {symbol}", e)
        return

async def evaluate_strategy_7(symbol: str, df_m15: pd.DataFrame):
    """
    Strategy 7: SMC execution (price-action only, min-notional sizing)
    - Allowed only on selected symbols (default: BTCUSDT, ETHUSDT)
    - HTF BOS on H1
    - POI as an H1 Order Block (preferred) or simple recent level
    - M15 entry only (default), rejection or engulfing reclaim at POI
    - SL: beyond OB extreme +/- ATR buffer (0.25*ATR by default)
    - Qty: minimum notional only (no fixed risk by default)
    """
    try:
        s7 = CONFIG['STRATEGY_7']
        allowed = [s.strip().upper() for s in s7.get('SYMBOLS', []) if s.strip()]
        if allowed and symbol not in allowed:
            _record_rejection(symbol, "S7-Restricted symbol", {"allowed": ",".join(allowed)})
            return

        if df_m15 is None or len(df_m15) < 80:
            _record_rejection(symbol, "S7-Not enough M15 data", {"len": len(df_m15) if df_m15 is not None else 0})
            return

        # Clone and compute local ATR/vol
        df_m15 = df_m15.copy()
        atr_period = int(s7.get('ATR_PERIOD', 14))
        df_m15['s7_atr'] = atr(df_m15, atr_period)
        s7_atr = float(df_m15['s7_atr'].iloc[-2]) if 's7_atr' in df_m15.columns else 0.0
        if s7_atr <= 0:
            _record_rejection(symbol, "S7-ATR not ready", {})
            return

        # HTF: H1 BOS detection
        df_h1 = await asyncio.to_thread(fetch_klines_sync, symbol, '1h', 300)
        if df_h1 is None or len(df_h1) < 120:
            _record_rejection(symbol, "S7-Not enough H1 data", {"len": len(df_h1) if df_h1 is not None else 0})
            return
        df_h1 = df_h1.copy()
        lookback = int(s7.get('BOS_LOOKBACK_H1', 72))
        sig_h1 = df_h1.iloc[-2]  # last closed H1
        prev_window_high = float(df_h1['high'].iloc[-(lookback+2):-2].max())
        prev_window_low = float(df_h1['low'].iloc[-(lookback+2):-2].min())

        direction = None
        if float(sig_h1['close']) > prev_window_high:
            direction = 'BUY'
        elif float(sig_h1['close']) < prev_window_low:
            direction = 'SELL'
        else:
            _record_rejection(symbol, "S7-No BOS on H1", {"close": float(sig_h1['close']), "HH": prev_window_high, "LL": prev_window_low})
            return

        # POI: simple H1 OB zone near BOS
        # Find last opposite color candle(s) before the BOS candle
        bos_idx = df_h1.index.get_loc(sig_h1.name)
        start_scan = max(0, bos_idx - 10)
        segment = df_h1.iloc[start_scan:bos_idx]
        ob_lows = []
        ob_highs = []
        for i in range(len(segment)-1, -1, -1):
            c = segment.iloc[i]
            is_opp = (direction == 'BUY' and c['close'] < c['open']) or (direction == 'SELL' and c['close'] > c['open'])
            if is_opp:
                ob_lows.append(float(c['low']))
                ob_highs.append(float(c['high']))
                if len(ob_lows) >= 3:  # use up to 3-bar cluster
                    break
        if ob_lows and ob_highs:
            ob_low = min(ob_lows)
            ob_high = max(ob_highs)
            poi_level = (ob_low + ob_high) / 2.0
            ob_range = ob_high - ob_low
        else:
            # Fallback: use previous swing level
            poi_level = prev_window_high if direction == 'BUY' else prev_window_low
            ob_low = ob_high = poi_level
            ob_range = 0.0

        # M15 entry only
        sig = df_m15.iloc[-2]
        prev = df_m15.iloc[-3]
        ft = df_m15.iloc[-1]

        # Check touch of POI on signal candle
        def _within_poi(cndl: pd.Series, lvl: float, atr_val: float) -> bool:
            h = float(cndl['high']); l = float(cndl['low'])
            tol = 0.25 * float(atr_val)
            return (l <= lvl <= h) or abs(h - lvl) <= tol or abs(l - lvl) <= tol

        if not _within_poi(sig, poi_level, s7_atr):
            _record_rejection(symbol, "S7-No POI touch on M15", {"poi": poi_level})
            return

        # Rejection: pin bar or engulfing reclaim of POI
        ob_min_body_ratio = float(s7.get('OB_MIN_BODY_RATIO', 0.5))
        # Use existing S6 helpers
        is_pin = _s6_is_pin_bar(sig, direction)
        is_engulf = _s6_is_engulfing_reclaim(sig, prev, direction, poi_level)
        if not (is_pin or is_engulf):
            _record_rejection(symbol, "S7-No valid rejection", {"pin": is_pin, "engulf": is_engulf})
            return

        # Follow-through on next candle using S6 helper
        vol_ma10 = float(df_m15['volume'].rolling(10).mean().iloc[-2])
        if not _s6_follow_through_ok(sig, ft, direction, vol_ma10, 0.7):
            _record_rejection(symbol, "S7-No follow-through", {"vol_ma10": vol_ma10})
            return

        # Entry and SL
        entry_price = float(sig['close'])
        if direction == 'BUY':
            zone_extreme = ob_low if ob_range > 0 else float(sig['low'])
            stop_price = zone_extreme - float(s7.get('ATR_BUFFER', 0.25)) * s7_atr
            side = 'BUY'
        else:
            zone_extreme = ob_high if ob_range > 0 else float(sig['high'])
            stop_price = zone_extreme + float(s7.get('ATR_BUFFER', 0.25)) * s7_atr
            side = 'SELL'

        # Sizing: min-notional only by default
        min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
        qty_min = await asyncio.to_thread(round_qty, symbol, (min_notional / entry_price) if entry_price > 0 else 0.0, rounding=ROUND_CEILING)
        if qty_min <= 0:
            _record_rejection(symbol, "S7-Qty min invalid", {"min_notional": min_notional})
            return

        final_qty = qty_min
        notional = final_qty * entry_price
        balance = await asyncio.to_thread(get_account_balance_usdt)
        # With min-notional, use notional as margin basis -> leverage=1 unless capped by config
        uncapped_leverage = 1
        max_leverage = min(CONFIG.get("MAX_BOT_LEVERAGE", 30), get_max_leverage(symbol))
        leverage = max(1, min(uncapped_leverage, max_leverage))

        # Place limit order with SL only
        limit_order_resp = await asyncio.to_thread(place_limit_order_sync, symbol, side, final_qty, entry_price)
        order_id = str(limit_order_resp.get('orderId'))
        pending_order_id = f"{symbol}_{order_id}"

        candle_duration = timeframe_to_timedelta(CONFIG['TIMEFRAME'])
        expiry_candles = int(s7.get("LIMIT_EXPIRY_CANDLES", 4))
        expiry_time = df_m15.index[-1] + (candle_duration * (expiry_candles - 1))

        pending_meta = {
            "id": pending_order_id, "order_id": order_id, "symbol": symbol,
            "side": side, "qty": final_qty, "limit_price": entry_price,
            "stop_price": stop_price, "take_price": None, "leverage": leverage,
            "risk_usdt": 0.0, "place_time": datetime.utcnow().isoformat(),
            "expiry_time": expiry_time.isoformat(),
            "strategy_id": 7,
            "atr_at_entry": s7_atr,
            "trailing": False,
            "s7_poi_level": poi_level,
        }

        async with pending_limit_orders_lock:
            pending_limit_orders[pending_order_id] = pending_meta
            await asyncio.to_thread(add_pending_order_to_db, pending_meta)

        title = "⏳ New Pending Order: S7-SMC"
        new_order_msg = (
            f"{title}\n\n"
            f"Symbol: `{symbol}`\n"
            f"Side: `{side}`\n"
            f"Price: `{entry_price:.4f}`\n"
            f"Qty: `{final_qty}`\n"
            f"Leverage: `{leverage}x`"
        )
        await asyncio.to_thread(send_telegram, new_order_msg, parse_mode='Markdown')

    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"S7 evaluation error for {symbol}", e)
        return

# ------------- Strategy 8 (SMC + Chart-Pattern Sniper Entry) helpers -------------
def _s8_htf_direction(symbol: str) -> Optional[str]:
    try:
        df_h4 = fetch_klines_sync(symbol, '4h', 400)
        df_d = fetch_klines_sync(symbol, '1d', 400)
        if df_h4 is None or df_d is None or len(df_h4) < 50 or len(df_d) < 50:
            return None
        bias_d = _s6_trend_from_swings(df_d, swing_lookback=20)
        bias_h4 = _s6_trend_from_swings(df_h4, swing_lookback=20)
        if bias_d in ('BULL', 'BEAR'):
            return 'BUY' if bias_d == 'BULL' else 'SELL'
        df_h1 = fetch_klines_sync(symbol, '1h', 300)
        if df_h1 is None or len(df_h1) < 80 or bias_h4 not in ('BULL', 'BEAR'):
            return None
        bias_h1 = _s6_trend_from_swings(df_h1, swing_lookback=20)
        if bias_h1 and bias_h1 == bias_h4:
            return 'BUY' if bias_h4 == 'BULL' else 'SELL'
        return None
    except Exception:
        return None

def _s8_last_bos_and_poi(df_h1: pd.DataFrame, lookback: int) -> tuple[Optional[str], Optional[tuple[float, float]], Optional[tuple[float, float]]]:
    if df_h1 is None or len(df_h1) < lookback + 5:
        return None, None, None
    sig_h1 = df_h1.iloc[-2]
    prev_high = float(df_h1['high'].iloc[-(lookback+2):-2].max())
    prev_low = float(df_h1['low'].iloc[-(lookback+2):-2].min())
    direction = None
    if float(sig_h1['close']) > prev_high:
        direction = 'BUY'
    elif float(sig_h1['close']) < prev_low:
        direction = 'SELL'
    else:
        return None, None, None
    bos_idx = df_h1.index.get_loc(sig_h1.name)
    start_scan = max(0, bos_idx - 10)
    segment = df_h1.iloc[start_scan:bos_idx]
    ob_lows, ob_highs = [], []
    for i in range(len(segment)-1, -1, -1):
        c = segment.iloc[i]
        is_opp = (direction == 'BUY' and float(c['close']) < float(c['open'])) or (direction == 'SELL' and float(c['close']) > float(c['open']))
        if is_opp:
            ob_lows.append(float(c['low']))
            ob_highs.append(float(c['high']))
            if len(ob_lows) >= 3:
                break
    ob_zone = (min(ob_lows), max(ob_highs)) if ob_lows and ob_highs else None

    # Find last strict body gap (FVG) near BOS
    def find_last_fvg(idx: int, dir_side: str) -> Optional[tuple[float, float]]:
        start = max(1, idx - 5)
        end = min(len(df_h1) - 1, idx)
        for j in range(end, start - 1, -1):
            prev = df_h1.iloc[j - 1]
            curr = df_h1.iloc[j]
            prev_close = float(prev['close']); curr_open = float(curr['open'])
            if dir_side == 'BUY' and curr_open > prev_close:
                return (prev_close, curr_open)
            if dir_side == 'SELL' and curr_open < prev_close:
                return (curr_open, prev_close)
        return None

    fvg_zone = find_last_fvg(bos_idx, direction)
    return direction, ob_zone, fvg_zone

def _s8_zone_touch(cndl: pd.Series, zone: tuple[float,float], atr_val: float) -> bool:
    zl, zh = zone
    h = float(cndl['high']); l = float(cndl['low'])
    tol = 0.25 * float(atr_val)
    return (l <= zh and h >= zl) or abs(h - zl) <= tol or abs(l - zh) <= tol

def _s8_volume_confirm(breakout: pd.Series, retest: pd.Series, vol_ma: float) -> bool:
    try:
        b_vol = float(breakout['volume']); r_vol = float(retest['volume'])
        b_rng = float(breakout['high'] - breakout['low']); r_rng = float(retest['high'] - retest['low'])
        vol_ok = (b_vol >= vol_ma) or (r_vol >= vol_ma) if np.isfinite(vol_ma) and vol_ma > 0 else False
        rng_ok = r_rng >= 0.7 * b_rng if np.isfinite(b_rng) else False
        return vol_ok or rng_ok
    except Exception:
        return False

async def evaluate_strategy_8(symbol: str, df_m15: pd.DataFrame):
    """
    Strategy 8: SMC + Chart-Pattern Sniper Entry — break + retest inside H1 OB/FVG
    - HTF alignment: Daily/H4 agree (fallback H4+H1 if Daily neutral)
    - Require recent H1 BOS and a POI (Order Block or strict body-gap FVG)
    - Pattern: generic range/flag breakout with retest rejection OR micro pin + confirm
    - Entry: limit at retest (or confirm) candle close; cancel if not filled in 3 candles
    - Stop: beyond OB extreme or pattern extreme ± 0.25*ATR(14)
    - Sizing: reuse central risk model (small account friendly)
    """
    try:
        s8 = CONFIG['STRATEGY_8']

        # Restrict S8 to the majors set unless overridden via env
        allowed_s8 = s8.get("SYMBOLS")
        if isinstance(allowed_s8, str):
            allowed_s8 = [x.strip().upper() for x in allowed_s8.split(",") if x.strip()]
        if not allowed_s8 or not isinstance(allowed_s8, (list, tuple)):
            allowed_s8 = [
                "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "AVAXUSDT",
                "LTCUSDT", "ADAUSDT", "XRPUSDT", "LINKUSDT", "DOTUSDT"
            ]
        if symbol not in allowed_s8:
            _record_rejection(symbol, "S8-Restricted symbol", {"allowed": ",".join(allowed_s8)})
            return

        if df_m15 is None or len(df_m15) < 120:
            _record_rejection(symbol, "S8-Not enough M15 data", {"len": len(df_m15) if df_m15 is not None else 0})
            return

        df_m15 = df_m15.copy()
        atr_period = int(s8.get('ATR_PERIOD', 14))
        df_m15['s8_atr'] = atr(df_m15, atr_period)
        atr_m15 = float(df_m15['s8_atr'].iloc[-2]) if 's8_atr' in df_m15.columns else 0.0
        if atr_m15 <= 0:
            _record_rejection(symbol, "S8-ATR not ready", {})
            return

        # HTF alignment
        direction_bias = await asyncio.to_thread(_s8_htf_direction, symbol)
        if direction_bias not in ('BUY', 'SELL'):
            _record_rejection(symbol, "S8-HTF bias unclear", {})
            return

        # H1 BOS + POI
        df_h1 = await asyncio.to_thread(fetch_klines_sync, symbol, '1h', 300)
        if df_h1 is None or len(df_h1) < 120:
            _record_rejection(symbol, "S8-Not enough H1 data", {"len": len(df_h1) if df_h1 is not None else 0})
            return
        lookback = int(s8.get('BOS_LOOKBACK_H1', 72))
        direction_bos, ob_zone, fvg_zone = _s8_last_bos_and_poi(df_h1.copy(), lookback)
        if direction_bos is None:
            _record_rejection(symbol, "S8-No BOS on H1", {})
            return
        if direction_bos != direction_bias:
            _record_rejection(symbol, "S8-BOS dir != HTF bias", {"bos": direction_bos, "htf": direction_bias})
            return

        use_ob = bool(s8.get('USE_OB', True))
        use_fvg = bool(s8.get('USE_FVG', True))

        poi_zone = None
        zone_type = None
        if use_ob and ob_zone is not None:
            poi_zone = ob_zone
            zone_type = "OB"
        elif use_fvg and fvg_zone is not None:
            poi_zone = fvg_zone
            zone_type = "FVG"
        else:
            _record_rejection(symbol, "S8-No POI zone (OB/FVG)", {"use_ob": use_ob, "use_fvg": use_fvg})
            return

        # Define candidate pattern window (last N bars before retest candle)
        N = 6
        sig = df_m15.iloc[-2]   # breakout candle (for break+retest), or pin for micro pattern
        ret = df_m15.iloc[-1]   # retest/confirm candle
        prev = df_m15.iloc[-3]

        range_high = float(df_m15['high'].iloc[-(N+1):-1].max())
        range_low = float(df_m15['low'].iloc[-(N+1):-1].min())
        # Ensure pattern forms inside or touching POI
        if not _s8_zone_touch(sig, poi_zone, atr_m15) and not _s8_zone_touch(prev, poi_zone, atr_m15):
            _record_rejection(symbol, "S8-Pattern not inside/touching POI", {"zone": zone_type})
            return

        vol_ma_len = int(s8.get('VOL_MA_LEN', 10))
        vol_ma10 = float(df_m15['volume'].rolling(vol_ma_len).mean().iloc[-2]) if vol_ma_len > 0 else float('nan')

        side = None
        entry_price = None
        stop_price = None
        pattern_ok = False
        pattern_name = None

        # --- Generic Break + Retest (flags/wedges/H&S neckline/double) ---
        # Check breakout and retest on last two closed candles
        if direction_bos == 'BUY':
            breakout_ok = float(sig['close']) > range_high
            retest_ok = (float(ret['low']) <= range_high <= float(ret['high'])) and float(ret['close']) > range_high
            # Retrace size 20–50% of prior impulse
            impL = 14
            imp_high = float(df_m15['high'].iloc[-(N+1+impL):-(N+1)].max()) if len(df_m15) >= (N+1+impL) else float('nan')
            imp_low = float(df_m15['low'].iloc[-(N+1+impL):-(N+1)].min()) if len(df_m15) >= (N+1+impL) else float('nan')
            impulse_len = imp_high - imp_low if (np.isfinite(imp_high) and np.isfinite(imp_low)) else float('nan')
            pullback = imp_high - range_low if np.isfinite(imp_high) else float('nan')
            retrace_pct = (pullback / impulse_len) if (np.isfinite(pullback) and impulse_len and impulse_len > 0) else float('nan')
            retrace_ok = (0.2 <= retrace_pct <= 0.5) if np.isfinite(retrace_pct) else True  # soft gate if unknown
            vol_ok = _s8_volume_confirm(sig, ret, vol_ma10)
            if breakout_ok and retest_ok and vol_ok and retrace_ok and _s8_zone_touch(ret, poi_zone, atr_m15):
                side = 'BUY'
                entry_price = float(ret['close'])
                # Stop beyond OB extremes if OB exists, else below pattern extreme
                if zone_type == 'OB':
                    ob_low, ob_high = poi_zone
                    stop_price = float(ob_low) - float(s8.get('ATR_BUFFER', 0.25)) * atr_m15
                else:
                    stop_price = float(range_low) - float(s8.get('ATR_BUFFER', 0.25)) * atr_m15
                pattern_ok = True
                pattern_name = "Break+Retest"
        else:  # SELL
            breakout_ok = float(sig['close']) < range_low
            retest_ok = (float(ret['low']) <= range_low <= float(ret['high'])) and float(ret['close']) < range_low
            impL = 14
            imp_low = float(df_m15['low'].iloc[-(N+1+impL):-(N+1)].min()) if len(df_m15) >= (N+1+impL) else float('nan')
            imp_high = float(df_m15['high'].iloc[-(N+1+impL):-(N+1)].max()) if len(df_m15) >= (N+1+impL) else float('nan')
            impulse_len = imp_high - imp_low if (np.isfinite(imp_high) and np.isfinite(imp_low)) else float('nan')
            pullback = range_high - imp_low if np.isfinite(imp_low) else float('nan')
            retrace_pct = (pullback / impulse_len) if (np.isfinite(pullback) and impulse_len and impulse_len > 0) else float('nan')
            retrace_ok = (0.2 <= retrace_pct <= 0.5) if np.isfinite(retrace_pct) else True
            vol_ok = _s8_volume_confirm(sig, ret, vol_ma10)
            if breakout_ok and retest_ok and vol_ok and retrace_ok and _s8_zone_touch(ret, poi_zone, atr_m15):
                side = 'SELL'
                entry_price = float(ret['close'])
                if zone_type == 'OB':
                    ob_low, ob_high = poi_zone
                    stop_price = float(ob_high) + float(s8.get('ATR_BUFFER', 0.25)) * atr_m15
                else:
                    stop_price = float(range_high) + float(s8.get('ATR_BUFFER', 0.25)) * atr_m15
                pattern_ok = True
                pattern_name = "Break+Retest"

        # --- Micro Pin + Confirm inside POI (alternative) ---
        if not pattern_ok:
            pin = _s6_is_pin_bar(sig, direction_bos)
            follow_ok = _s6_follow_through_ok(sig, ret, direction_bos, vol_ma10, 0.7)
            if pin and _s8_zone_touch(sig, poi_zone, atr_m15) and follow_ok:
                side = direction_bos
                entry_price = float(ret['close'])
                if direction_bos == 'BUY':
                    stop_price = float(sig['low']) - float(s8.get('ATR_BUFFER', 0.25)) * atr_m15
                else:
                    stop_price = float(sig['high']) + float(s8.get('ATR_BUFFER', 0.25)) * atr_m15
                pattern_ok = True
                pattern_name = "MicroPin+Confirm"

        if not pattern_ok or side is None or entry_price is None or stop_price is None:
            _record_rejection(symbol, "S8-No valid pattern/confirmation", {"zone": zone_type, "dir": direction_bos})
            return

        distance = abs(entry_price - stop_price)
        if distance <= 0 or not np.isfinite(distance):
            _record_rejection(symbol, "S8-Invalid SL distance", {"entry": entry_price, "sl": stop_price})
            return

        # Sizing: use central risk model (small-account friendly)
        balance = await asyncio.to_thread(get_account_balance_usdt)
        risk_usdt = calculate_risk_amount(balance, strategy_id=8)
        ideal_qty = risk_usdt / distance
        ideal_qty = await asyncio.to_thread(round_qty, symbol, ideal_qty, rounding=ROUND_DOWN)

        min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
        qty_min = await asyncio.to_thread(round_qty, symbol, (min_notional / entry_price) if entry_price > 0 else 0.0, rounding=ROUND_CEILING)
        final_qty = max(ideal_qty, qty_min)
        if final_qty <= 0:
            _record_rejection(symbol, "S8-Qty zero after sizing", {"ideal": ideal_qty, "min": qty_min})
            return

        notional = final_qty * entry_price
        margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else risk_usdt
        uncapped_leverage = int(math.floor(notional / max(margin_to_use, 1e-9)))
        max_leverage = min(CONFIG.get("MAX_BOT_LEVERAGE", 30), get_max_leverage(symbol))
        leverage = max(1, min(uncapped_leverage, max_leverage))

        # Place limit order at retest/confirm close
        limit_order_resp = await asyncio.to_thread(place_limit_order_sync, symbol, side, final_qty, entry_price)
        order_id = str(limit_order_resp.get('orderId'))
        pending_order_id = f"{symbol}_{order_id}"

        candle_duration = timeframe_to_timedelta(CONFIG['TIMEFRAME'])
        expiry_candles = int(s8.get("RETEST_EXPIRY_CANDLES", 3))
        expiry_time = df_m15.index[-1] + (candle_duration * (expiry_candles - 1))

        pending_meta = {
            "id": pending_order_id, "order_id": order_id, "symbol": symbol,
            "side": side, "qty": final_qty, "limit_price": entry_price,
            "stop_price": stop_price, "take_price": None, "leverage": leverage,
            "risk_usdt": risk_usdt, "place_time": datetime.utcnow().isoformat(),
            "expiry_time": expiry_time.isoformat(),
            "strategy_id": 8,
            "atr_at_entry": atr_m15,
            "trailing": True,
            "s8_zone_type": zone_type,
            "s8_pattern": pattern_name,
        }

        async with pending_limit_orders_lock:
            pending_limit_orders[pending_order_id] = pending_meta
            await asyncio.to_thread(add_pending_order_to_db, pending_meta)

        title = "⏳ New Pending Order: S8-SMC+Pattern"
        new_order_msg = (
            f"{title}\n\n"
            f"Symbol: `{symbol}`\n"
            f"Side: `{side}`\n"
            f"Pattern: `{pattern_name}` in `{zone_type}`\n"
            f"Price: `{entry_price:.4f}`\n"
            f"Qty: `{final_qty}`\n"
            f"Risk: `{risk_usdt:.2f} USDT`\n"
            f"Leverage: `{leverage}x`"
        )
        await asyncio.to_thread(send_telegram, new_order_msg, parse_mode='Markdown')

    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"S8 evaluation error for {symbol}", e)
        return

# --------- Strategy 9 (SMC Scalping — High-Win Probability) helpers ---------
def _s9_in_session_window(ts: pd.Timestamp) -> bool:
    try:
        s9 = CONFIG.get('STRATEGY_9', {})
        start_h = int(s9.get('SESSION_START_UTC_HOUR', 7))
        end_h = int(s9.get('SESSION_END_UTC_HOUR', 15))
        hour = ts.tz_convert('UTC').hour if ts.tzinfo is not None else ts.hour
        if start_h <= end_h:
            return start_h <= hour < end_h
        return hour >= start_h or hour < end_h
    except Exception:
        return True

def _s9_recent_h1_bos(df_h1: pd.DataFrame, min_lb: int, max_lb: int) -> Optional[str]:
    try:
        if df_h1 is None or len(df_h1) < max_lb + 5:
            return None
        sig_h1 = df_h1.iloc[-2]
        prev_high = float(df_h1['high'].iloc[-(max_lb+2):-2].max())
        prev_low = float(df_h1['low'].iloc[-(max_lb+2):-2].min())
        c = float(sig_h1['close'])
        if c > prev_high:
            return 'BUY'
        if c < prev_low:
            return 'SELL'
        return None
    except Exception:
        return None

def _s9_h1_ob_zone(df_h1: pd.DataFrame, direction: str) -> Optional[tuple[float, float]]:
    try:
        if df_h1 is None or len(df_h1) < 20:
            return None
        sig_h1 = df_h1.iloc[-2]
        bos_idx = df_h1.index.get_loc(sig_h1.name)
        start_scan = max(0, bos_idx - 10)
        segment = df_h1.iloc[start_scan:bos_idx]
        ob_lows, ob_highs = [], []
        for i in range(len(segment)-1, -1, -1):
            c = segment.iloc[i]
            is_opp = (direction == 'BUY' and float(c['close']) < float(c['open'])) or (direction == 'SELL' and float(c['close']) > float(c['open']))
            if is_opp:
                ob_lows.append(float(c['low']))
                ob_highs.append(float(c['high']))
                if len(ob_lows) >= 3:
                    break
        if ob_lows and ob_highs:
            return (min(ob_lows), max(ob_highs))
        return None
    except Exception:
        return None

def _s9_rejection_wick_ratio(candle: pd.Series, direction: str) -> float:
    try:
        o = float(candle['open']); h = float(candle['high']); l = float(candle['low']); c = float(candle['close'])
        rng = max(1e-9, h - l)
        up_w = h - max(o, c)
        dn_w = min(o, c) - l
        return (dn_w / rng) if direction == 'BUY' else (up_w / rng)
    except Exception:
        return 0.0

def _s9_avg_range(series_high: pd.Series, series_low: pd.Series, length: int) -> float:
    try:
        if series_high is None or series_low is None or len(series_high) < length + 2 or len(series_low) < length + 2:
            return 0.0
        rng = (series_high - series_low).iloc[-(length+1):-1]
        return float(max(1e-9, rng.mean()))
    except Exception:
        return 0.0

def _s9_detect_sweep_reclaim(df_m1: pd.DataFrame, direction: str, sweep_lookback: int, reclaim_max_bars: int) -> bool:
    try:
        if df_m1 is None or len(df_m1) < sweep_lookback + reclaim_max_bars + 5:
            return False
        # Use last closed as reference
        sig_idx = len(df_m1) - 2
        pre_end = sig_idx - reclaim_max_bars
        pre_start = max(0, pre_end - sweep_lookback)
        if pre_end <= pre_start:
            return False
        pre_window_high = float(df_m1['high'].iloc[pre_start:pre_end].max())
        pre_window_low = float(df_m1['low'].iloc[pre_start:pre_end].min())
        sweep_window = df_m1.iloc[pre_end:sig_idx+1]  # includes signal candle for reclaim
        swept = False
        reclaimed = False
        if direction == 'BUY':
            # Any low that pierces pre_window_low
            for i in range(len(sweep_window)):
                c = sweep_window.iloc[i]
                if float(c['low']) < pre_window_low:
                    swept = True
                    # Reclaim: any close afterwards back above pre_window_low within the window
                    after = sweep_window.iloc[i:min(i+reclaim_max_bars+1, len(sweep_window))]
                    if any(float(x['close']) > pre_window_low for _, x in after.iterrows()):
                        reclaimed = True
                    break
        else:
            for i in range(len(sweep_window)):
                c = sweep_window.iloc[i]
                if float(c['high']) > pre_window_high:
                    swept = True
                    after = sweep_window.iloc[i:min(i+reclaim_max_bars+1, len(sweep_window))]
                    if any(float(x['close']) < pre_window_high for _, x in after.iterrows()):
                        reclaimed = True
                    break
        return swept and reclaimed
    except Exception:
        return False

async def evaluate_strategy_9(symbol: str, df_m15: pd.DataFrame):
    """
    Strategy 9: SMC Scalping — high-win probability (sniper rules)
    - HTF bias: Daily/H4 alignment
    - H1 BOS present (12–48 bars window)
    - Micro liquidity sweep + quick reclaim on M1
    - OB retest on M1 inside H1 OB zone with strong rejection (wick >= 70%)
    - Stop: OB extreme ± 0.6*ATR(14,M5). Skip if stop > 1.5x avg M5 range
    - Target: 0.5R (high win-rate). Limit expires in 3 M1 candles.
    - Sizing: same fixed-risk model as S6 (RISK_USD / SL distance), min-notional enforced, leverage from actual risk.
    """
    try:
        s9 = CONFIG.get('STRATEGY_9', {})
        allowed = [s.strip().upper() for s in s9.get('SYMBOLS', []) if s.strip()]
        if allowed and symbol not in allowed:
            _record_rejection(symbol, "S9-Restricted symbol", {"allowed": ",".join(allowed)})
            return

        # Fetch required TFs
        df_h1 = await asyncio.to_thread(fetch_klines_sync, symbol, '1h', 300)
        df_h4 = await asyncio.to_thread(fetch_klines_sync, symbol, '4h', 400)
        df_d  = await asyncio.to_thread(fetch_klines_sync, symbol, '1d', 400)
        df_m5 = await asyncio.to_thread(fetch_klines_sync, symbol, '5m', 300)
        df_m1 = await asyncio.to_thread(fetch_klines_sync, symbol, '1m', 600)

        if any(x is None or len(x) < 60 for x in [df_h1, df_h4, df_d, df_m5, df_m1]):
            _record_rejection(symbol, "S9-Insufficient TF data", {
                "h1": len(df_h1) if df_h1 is not None else 0,
                "h4": len(df_h4) if df_h4 is not None else 0,
                "d": len(df_d) if df_d is not None else 0,
                "m5": len(df_m5) if df_m5 is not None else 0,
                "m1": len(df_m1) if df_m1 is not None else 0,
            })
            return

        # Session filter on last closed M1
        sig_ts_m1 = df_m1.index[-2]
        if not _s9_in_session_window(sig_ts_m1):
            _record_rejection(symbol, "S9-Outside session window", {"ts": str(sig_ts_m1)})
            return

        # HTF bias from Daily, confirm H4 does not contradict
        bias_d = _s6_trend_from_swings(df_d.copy(), swing_lookback=20)
        bias_h4 = _s6_trend_from_swings(df_h4.copy(), swing_lookback=20)
        if bias_d is None:
            _record_rejection(symbol, "S9-Daily bias unclear", {})
            return
        if bias_h4 is not None and bias_h4 != bias_d:
            _record_rejection(symbol, "S9-H4 contradicts Daily", {"daily": bias_d, "h4": bias_h4})
            return
        direction = 'BUY' if bias_d == 'BULL' else 'SELL'

        # H1 BOS (12–48 bars)
        bos_dir = _s9_recent_h1_bos(df_h1.copy(), int(s9.get('BOS_LOOKBACK_H1_MIN', 12)), int(s9.get('BOS_LOOKBACK_H1_MAX', 48)))
        if bos_dir is None or bos_dir != direction:
            _record_rejection(symbol, "S9-No matching H1 BOS", {"bos": bos_dir, "dir": direction})
            return

        # H1 OB zone near BOS
        ob_zone = _s9_h1_ob_zone(df_h1.copy(), direction)
        if not ob_zone:
            _record_rejection(symbol, "S9-No OB zone", {})
            return
        ob_low, ob_high = ob_zone

        # Micro sweep + reclaim on M1
        sweep_ok = _s9_detect_sweep_reclaim(
            df_m1.copy(), direction,
            int(s9.get('MICRO_SWEEP_LOOKBACK_M1', 20)),
            int(s9.get('SWEEP_RECLAIM_MAX_BARS', 5))
        )
        if not sweep_ok:
            _record_rejection(symbol, "S9-No micro sweep+reclaim", {})
            return

        # Rejection candle inside OB on M1
        sig = df_m1.iloc[-2]          # rejection/entry candle
        if not (ob_low <= float(sig['close']) <= ob_high):
            _record_rejection(symbol, "S9-Entry not inside OB", {"close": float(sig['close']), "ob_low": ob_low, "ob_high": ob_high})
            return

        wick_ratio = _s9_rejection_wick_ratio(sig, direction)
        if wick_ratio < float(s9.get('REJECTION_WICK_RATIO', 0.7)):
            _record_rejection(symbol, "S9-Rejection wick too small", {"ratio": wick_ratio})
            return

        # Confirm candle size vs avg M1 range
        m1_len = int(s9.get('M1_RANGE_AVG_LEN', 20))
        avg_m1_range = _s9_avg_range(df_m1['high'], df_m1['low'], m1_len)
        sig_range = float(sig['high'] - sig['low'])
        if not (sig_range >= 0.60 * avg_m1_range):
            _record_rejection(symbol, "S9-Weak M1 rejection range", {"sig_range": sig_range, "avg_m1_range": avg_m1_range})
            return

        # ATR on M5 and max stop constraint
        atr_p = int(s9.get('M5_ATR_PERIOD', 14))
        atr_m5_series = atr_wilder(df_m5.copy(), atr_p)
        atr_m5 = float(atr_m5_series.iloc[-2]) if atr_m5_series is not None and len(atr_m5_series) >= 2 else 0.0
        if atr_m5 <= 0:
            _record_rejection(symbol, "S9-M5 ATR invalid", {})
            return

        avg_m5_range = _s9_avg_range(df_m5['high'], df_m5['low'], 20)

        entry_price = float(sig['close'])
        if direction == 'BUY':
            zone_extreme = ob_low
            stop_price = zone_extreme - float(s9.get('ATR_BUFFER_MULT_M5', 0.6)) * atr_m5
            side = 'BUY'
        else:
            zone_extreme = ob_high
            stop_price = zone_extreme + float(s9.get('ATR_BUFFER_MULT_M5', 0.6)) * atr_m5
            side = 'SELL'

        distance = abs(entry_price - stop_price)
        if distance <= 0 or not np.isfinite(distance):
            _record_rejection(symbol, "S9-Invalid SL distance", {"entry": entry_price, "sl": stop_price})
            return

        max_stop_ok = distance <= float(s9.get('MAX_STOP_TO_AVG_RANGE_M5', 1.5)) * avg_m5_range
        if not max_stop_ok:
            _record_rejection(symbol, "S9-Stop too wide vs M5 range", {"distance": distance, "max_allowed": float(s9.get('MAX_STOP_TO_AVG_RANGE_M5', 1.5)) * avg_m5_range})
            return

        # Sizing: fixed-risk (S6 model) with min-notional enforcement
        balance = await asyncio.to_thread(get_account_balance_usdt)
        risk_usd = float(s9.get('RISK_USD', 0.50))
        ideal_qty = risk_usd / distance
        ideal_qty = await asyncio.to_thread(round_qty, symbol, ideal_qty, rounding=ROUND_DOWN)

        # Enforce minimum notional
        min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
        qty_min = await asyncio.to_thread(round_qty, symbol, (min_notional / entry_price) if entry_price > 0 else 0.0, rounding=ROUND_CEILING)

        if ideal_qty < qty_min or ideal_qty <= 0:
            _record_rejection(symbol, "S9-Qty below minimum", {"ideal": ideal_qty, "min": qty_min})
            return

        final_qty = ideal_qty
        notional = final_qty * entry_price

        # Leverage from actual risk (like S6)
        actual_risk_usdt = final_qty * distance
        margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else actual_risk_usdt
        uncapped_leverage = int(math.ceil(notional / max(margin_to_use, 1e-9)))
        max_leverage = min(CONFIG.get("MAX_BOT_LEVERAGE", 30), get_max_leverage(symbol))
        leverage = max(1, min(uncapped_leverage, max_leverage))

        # Target: 0.5R
        take_price = entry_price + 0.5 * distance if side == 'BUY' else entry_price - 0.5 * distance

        # Place LIMIT at signal close, expiry in LIMIT_EXPIRY_M1_CANDLES
        limit_order_resp = await asyncio.to_thread(place_limit_order_sync, symbol, side, final_qty, entry_price)
        order_id = str(limit_order_resp.get('orderId'))
        pending_order_id = f"{symbol}_{order_id}"

        candle_duration = timeframe_to_timedelta("1m")
        expiry_candles = int(s9.get("LIMIT_EXPIRY_M1_CANDLES", 3))
        expiry_time = df_m1.index[-1] + (candle_duration * (expiry_candles - 1))

        pending_meta = {
            "id": pending_order_id, "order_id": order_id, "symbol": symbol,
            "side": side, "qty": final_qty, "limit_price": entry_price,
            "stop_price": stop_price, "take_price": take_price, "leverage": leverage,
            "risk_usdt": actual_risk_usdt, "place_time": datetime.utcnow().isoformat(),
            "expiry_time": expiry_time.isoformat(),
            "strategy_id": 9,
            "atr_at_entry": atr_m5,
            "trailing": False,
        }

        async with pending_limit_orders_lock:
            pending_limit_orders[pending_order_id] = pending_meta
            await asyncio.to_thread(add_pending_order_to_db, pending_meta)

        title = "⏳ New Pending Order: S9-Scalp"
        new_order_msg = (
            f"{title}\n\n"
            f"Symbol: `{symbol}`\n"
            f"Side: `{side}`\n"
            f"Price: `{entry_price:.4f}`\n"
            f"Qty: `{final_qty}`\n"
            f"Risk: `{actual_risk_usdt:.2f} USDT`\n"
            f"Leverage: `{leverage}x`\n"
            f"Target: `0.5R`"
        )
        await asyncio.to_thread(send_telegram, new_order_msg, parse_mode='Markdown')

        return
    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"S9 evaluation error for {symbol}", e)
        return
        if not max_stop_ok:
            _record_rejection(symbol, "S9-Stop too wide vs M5 range", {"dist": distance, "avg_m5_range": avg_m5_range})
            return

        # Sizing (use S6 model): fixed RISK_USD, min-notional enforcement, leverage by actual risk
        risk_usd = float(s9.get('RISK_USD', CONFIG.get('STRATEGY_6', {}).get('RISK_USD', 0.5)))
        ideal_qty = risk_usd / distance
        ideal_qty = await asyncio.to_thread(round_qty, symbol, ideal_qty, rounding=ROUND_DOWN)

        min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
        qty_min = await asyncio.to_thread(round_qty, symbol, (min_notional / entry_price) if entry_price > 0 else 0.0, rounding=ROUND_CEILING)
        if ideal_qty < qty_min or ideal_qty <= 0:
            _record_rejection(symbol, "S9-Qty below minimum", {"ideal": ideal_qty, "min": qty_min})
            return

        final_qty = ideal_qty
        notional = final_qty * entry_price
        balance = await asyncio.to_thread(get_account_balance_usdt)
        actual_risk_usdt = final_qty * distance
        margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else actual_risk_usdt
        uncapped_leverage = int(math.ceil(notional / max(margin_to_use, 1e-9)))
        max_leverage = min(CONFIG.get("MAX_BOT_LEVERAGE", 30), get_max_leverage(symbol))
        leverage = max(1, min(uncapped_leverage, max_leverage))

        # High win-rate TP: 0.5R
        r_mult = float(s9.get('HIGH_WIN_TP_R_MULT', 0.5))
        take_price = entry_price + r_mult * distance if side == 'BUY' else entry_price - r_mult * distance

        # Place limit order at rejection close
        limit_order_resp = await asyncio.to_thread(place_limit_order_sync, symbol, side, final_qty, entry_price)
        order_id = str(limit_order_resp.get('orderId'))
        pending_order_id = f"{symbol}_{order_id}"

        # Expiry: 3 M1 candles
        candle_duration = timedelta(minutes=1)
        expiry_candles = int(s9.get("LIMIT_EXPIRY_M1_CANDLES", 3))
        expiry_time = df_m1.index[-1] + (candle_duration * (expiry_candles - 1))

        pending_meta = {
            "id": pending_order_id, "order_id": order_id, "symbol": symbol,
            "side": side, "qty": final_qty, "limit_price": entry_price,
            "stop_price": stop_price, "take_price": take_price, "leverage": leverage,
            "risk_usdt": actual_risk_usdt, "place_time": datetime.utcnow().isoformat(),
            "expiry_time": expiry_time.isoformat(),
            "strategy_id": 9,
            "atr_at_entry": atr_m5,
            "trailing": False
        }

        async with pending_limit_orders_lock:
            pending_limit_orders[pending_order_id] = pending_meta
            await asyncio.to_thread(add_pending_order_to_db, pending_meta)

        title = "⏳ New Pending Order: S9-SMC Scalping"
        new_order_msg = (
            f"{title}\n\n"
            f"Symbol: `{symbol}`\n"
            f"Side: `{side}`\n"
            f"Price: `{entry_price:.4f}`\n"
            f"Qty: `{final_qty}`\n"
            f"Risk: `{actual_risk_usdt:.2f} USDT`\n"
            f"Leverage: `{leverage}x`"
        )
        await asyncio.to_thread(send_telegram, new_order_msg, parse_mode='Markdown')

        return
    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"S9 evaluation error for {symbol}", e)
        return
        expiry_time = df_m1.index[-1] + (candle_duration * (expiry_candles - 1))

        pending_meta = {
            "id": pending_order_id, "order_id": order_id, "symbol": symbol,
            "side": side, "qty": final_qty, "limit_price": entry_price,
            "stop_price": stop_price, "take_price": take_price, "leverage": leverage,
            "risk_usdt": actual_risk_usdt, "place_time": datetime.utcnow().isoformat(),
            "expiry_time": expiry_time.isoformat(),
            "strategy_id": 9,
            "atr_at_entry": atr_m5,
            "trailing": False,
        }

        async with pending_limit_orders_lock:
            pending_limit_orders[pending_order_id] = pending_meta
            await asyncio.to_thread(add_pending_order_to_db, pending_meta)

        title = "⏳ New Pending Order: S9-SMC Scalping"
        new_order_msg = (
            f"{title}\n\n"
            f"Symbol: `{symbol}`\n"
            f"Side: `{side}`\n"
            f"Price: `{entry_price:.4f}`\n"
            f"Qty: `{final_qty}`\n"
            f"Risk: `{actual_risk_usdt:.2f} USDT`\n"
            f"Leverage: `{leverage}x`"
        )
        await asyncio.to_thread(send_telegram, new_order_msg, parse_mode='Markdown')

    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"S9 evaluation error for {symbol}", e)
        return

async def force_trade_entry(strategy_id: int, symbol: str, side: str):
    """
    Forces an immediate market entry for a given strategy, symbol, and side,
    bypassing normal signal checks but using the strategy's risk and SL logic.
    """
    log.info(f"--- FORCE TRADE INITIATED: S{strategy_id} {symbol} {side} ---")
    
    # Pre-trade check: is a trade already open for this symbol?
    async with managed_trades_lock:
        if not CONFIG["HEDGING_ENABLED"] and any(t['symbol'] == symbol for t in managed_trades.values()):
            await asyncio.to_thread(send_telegram, f"❌ Cannot force trade. A trade for `{symbol}` already exists and hedging is disabled.", parse_mode='Markdown')
            return

    try:
        df = await asyncio.to_thread(fetch_klines_sync, symbol, CONFIG["TIMEFRAME"], 300)
        if df is None or df.empty:
            await asyncio.to_thread(send_telegram, f"❌ Cannot force trade. Could not fetch kline data for `{symbol}`.", parse_mode='Markdown')
            return
        current_price = safe_last(df['close'])
        
        # --- Strategy-Specific Parameter Calculation ---
        sl_price = 0.0
        risk_usdt = 0.0
        qty = 0.0
        leverage = 0
        trade_meta_extra = {}

        if strategy_id == 1:
            s_params = CONFIG['STRATEGY_1']
            df['atr'] = atr(df, CONFIG["ATR_LENGTH"])
            atr_now = safe_last(df['atr'])
            sl_distance = CONFIG["STRATEGY_EXIT_PARAMS"]['1']["ATR_MULTIPLIER"] * atr_now
            sl_price = current_price - sl_distance if side == 'BUY' else current_price + sl_distance
            
            price_distance = abs(current_price - sl_price)
            balance = await asyncio.to_thread(get_account_balance_usdt)
            risk_usdt = calculate_risk_amount(balance, strategy_id=1)
            qty = risk_usdt / price_distance if price_distance > 0 else 0.0
            trade_meta_extra = {"atr_at_entry": atr_now}

        elif strategy_id == 2:
            s_params = CONFIG['STRATEGY_2']
            df['supertrend'], _ = supertrend(df, period=s_params['SUPERTREND_PERIOD'], multiplier=s_params['SUPERTREND_MULTIPLIER'])
            sl_price = safe_last(df['supertrend'])
            
            price_distance = abs(current_price - sl_price)
            balance = await asyncio.to_thread(get_account_balance_usdt)
            risk_usdt = calculate_risk_amount(balance, strategy_id=2)
            # For forced trades, we use full risk, no confidence scaling
            qty = risk_usdt / price_distance if price_distance > 0 else 0.0
            trade_meta_extra = {"atr_at_entry": safe_latest_atr_from_df(df)}

        elif strategy_id == 3:
            s_params = CONFIG['STRATEGY_3']
            df['atr'] = atr(df, CONFIG["ATR_LENGTH"])
            atr_now = safe_last(df['atr'])
            
            atr_mult = s_params.get("ATR_SL_MULT", 1.5)
            fallback_sl_pct = s_params.get("FALLBACK_SL_PCT", 0.015)
            
            if atr_now > 0:
                sl_price = current_price - atr_mult * atr_now if side == 'BUY' else current_price + atr_mult * atr_now
            else:
                sl_price = current_price * (1 - fallback_sl_pct) if side == 'BUY' else current_price * (1 + fallback_sl_pct)

            balance = await asyncio.to_thread(get_account_balance_usdt)
            risk_usdt = calculate_risk_amount(balance, strategy_id=3) # Use the central function
            price_distance = abs(current_price - sl_price)
            qty = risk_usdt / price_distance if price_distance > 0 else 0.0
            
            trade_meta_extra = {
                "atr_at_entry": atr_now,
                "s3_trailing_active": False,
                "s3_trailing_stop": sl_price, # Initialize trailing stop to the initial SL
            }

        elif strategy_id == 4:
            s_params = CONFIG['STRATEGY_4']
            
            # --- Correct logic: Calculate indicators and use the main SuperTrend for initial SL ---
            df = calculate_all_indicators(df.copy()) # Get all indicators
            # For a forced entry, we should use the most recent available data for the SL.
            sl_price = safe_last(df['s4_st2'])

            # Safety check: Ensure SL is not triggering an immediate stop-out on a forced entry
            if side == 'BUY' and sl_price >= current_price:
                log.warning(f"S4 Force Trade: Calculated SL {sl_price} is above current price {current_price}. Using 2% fallback SL.")
                sl_price = current_price * 0.98 
            elif side == 'SELL' and sl_price <= current_price:
                log.warning(f"S4 Force Trade: Calculated SL {sl_price} is below current price {current_price}. Using 2% fallback SL.")
                sl_price = current_price * 1.02

            risk_usdt = s_params['RISK_USD']
            price_distance = abs(current_price - sl_price)
            qty = risk_usdt / price_distance if price_distance > 0 else 0.0

            trade_meta_extra = {
                "s4_trailing_stop": sl_price, # Initialize with the initial SL
                "s4_last_candle_ts": df.index[-1].isoformat(),
                "s4_trailing_active": False, # This is managed by the monitor thread
            }
        
        else:
            await asyncio.to_thread(send_telegram, f"❌ Invalid strategy ID `{strategy_id}` for force trade.", parse_mode='Markdown')
            return

        # --- Common Sizing & Leverage Calculation ---
        # Round down ideal quantity to not exceed risk
        ideal_qty = await asyncio.to_thread(round_qty, symbol, qty, rounding=ROUND_DOWN)

        # Calculate minimum quantity to meet notional value
        min_notional = await asyncio.to_thread(get_min_notional_sync, symbol)
        qty_min = min_notional / current_price if current_price > 0 else 0.0
        qty_min = await asyncio.to_thread(round_qty, symbol, qty_min, rounding=ROUND_CEILING)

        # Final quantity is the larger of the two
        qty = max(ideal_qty, qty_min)

        if qty <= 0:
            await asyncio.to_thread(send_telegram, f"❌ Calculated quantity for force trade is zero. Aborting.", parse_mode='Markdown')
            return
            
        actual_risk_usdt = abs(current_price - sl_price) * qty
        notional = qty * current_price
        balance = await asyncio.to_thread(get_account_balance_usdt)
        margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"] if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"] else actual_risk_usdt
        leverage = int(math.floor(notional / max(margin_to_use, 1e-9)))
        max_leverage = min(CONFIG.get("MAX_BOT_LEVERAGE", 30), get_max_leverage(symbol))
        leverage = max(1, min(leverage, max_leverage))
        
        # --- Execute Trade ---
        log.info(f"FORCE TRADE: Placing MARKET {side} order for {qty} {symbol} with {leverage}x leverage.")
        await asyncio.to_thread(open_market_position_sync, symbol, side, qty, leverage)
        
        # Poll for up to 5 seconds for the position to appear
        pos = None
        for i in range(5):
            await asyncio.sleep(1)
            positions = await asyncio.to_thread(client.futures_position_information, symbol=symbol)
            position_side = 'LONG' if side == 'BUY' else 'SHORT'
            pos = next((p for p in positions if p.get('positionSide') == position_side and float(p.get('positionAmt', 0)) != 0), None)
            if pos:
                log.info(f"Forced position for {symbol} found after {i+1} second(s).")
                break
        
        if not pos:
             raise RuntimeError(f"Position for {symbol} not found after forced market order (waited 5s).")
        
        actual_entry_price = float(pos['entryPrice'])
        actual_qty = abs(float(pos['positionAmt']))

        # Recalculate SL based on actual entry price for accuracy
        if strategy_id in [1, 2]:
            # ATR/Supertrend based SLs don't change with entry price
            pass 
        elif strategy_id == 3:
            sl_price = actual_entry_price * (1 - CONFIG['STRATEGY_3']['INITIAL_STOP_PCT']) if side == 'BUY' else actual_entry_price * (1 + CONFIG['STRATEGY_3']['INITIAL_STOP_PCT'])
        elif strategy_id == 4:
            sl_price = actual_entry_price * (1 - CONFIG['STRATEGY_4']['INITIAL_STOP_PCT']) if side == 'BUY' else actual_entry_price * (1 + CONFIG['STRATEGY_4']['INITIAL_STOP_PCT'])

        log.info(f"FORCE TRADE: Placing SL for {symbol} at {sl_price}")
        sltp_orders = await asyncio.to_thread(place_batch_sl_tp_sync, symbol, side, sl_price=sl_price, qty=actual_qty)
        
        # --- Create and Store Trade Metadata ---
        trade_id = f"{symbol}_S{strategy_id}_FORCED_{int(time.time())}"
        meta = {
            "id": trade_id, "symbol": symbol, "side": side, "entry_price": actual_entry_price,
            "initial_qty": actual_qty, "qty": actual_qty, "notional": actual_qty * actual_entry_price,
            "leverage": leverage, "sl": sl_price, "tp": 0, # Forced trades have no TP
            "open_time": datetime.utcnow().isoformat(), "sltp_orders": sltp_orders,
            "risk_usdt": actual_risk_usdt, "strategy_id": strategy_id,
            "entry_reason": "FORCED_MANUAL",
            "be_moved": False, # Ensure this is set for new trades
            "trailing_active": False,
            "trailing": CONFIG["TRAILING_ENABLED"],
        }
        meta.update(trade_meta_extra)

        async with managed_trades_lock:
            managed_trades[trade_id] = meta
        
        await asyncio.to_thread(add_managed_trade_to_db, meta)

        msg = (
            f"✅ *Forced Trade Executed: S{strategy_id}*\n\n"
            f"**Symbol:** `{symbol}`\n"
            f"**Side:** `{side}`\n"
            f"**Entry:** `{actual_entry_price:.4f}`\n"
            f"**Stop Loss:** `{sl_price:.4f}`\n"
            f"**Risk:** `{actual_risk_usdt:.2f} USDT`\n"
            f"**Leverage:** `{leverage}x`"
        )
        await asyncio.to_thread(send_telegram, msg, parse_mode='Markdown')

    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"Failed to execute force trade for S{strategy_id} on {symbol}", e)


def calculate_trailing_distance(strategy_id: str, volatility_ratio: float, trend_strength: float) -> float:
    """Calculate dynamic trailing distance based on multiple factors."""
    # Ensure strategy_id is a valid key
    strategy_id_str = str(strategy_id)
    if strategy_id_str not in CONFIG['STRATEGY_EXIT_PARAMS']:
        strategy_id_str = '1' # Default to BB strategy params if not found

    # Base multiplier from config
    base_multiplier = CONFIG['STRATEGY_EXIT_PARAMS'][strategy_id_str]["ATR_MULTIPLIER"]

    # New adaptive logic for Strategy 2
    if strategy_id_str == '2':
        if volatility_ratio > 0.02:
            return 2.5
        if volatility_ratio < 0.008:
            return 1.8
        # Fallback to base multiplier for S2 if in between
        return base_multiplier 
    else: # Keep old logic for Strategy 1
        volatility_factor = 1.0
        if volatility_ratio > 0.02:
            volatility_factor = 1.2
        elif volatility_ratio < 0.008:
            volatility_factor = 0.8
        
        trend_factor = 1.0
        if trend_strength > 30:
            trend_factor = 1.1
        
        return base_multiplier * volatility_factor * trend_factor


def get_account_balance_usdt():
    global client
    try:
        if client is None:
            return 0.0
        acct = client.futures_account_balance()
        for entry in acct:
            if entry.get('asset') == 'USDT':
                return float(entry.get('withdrawAvailable') or entry.get('balance') or 0.0)
    except Exception:
        log.exception("Failed to fetch account balance")
    return 0.0

def monitor_thread_func():
    global managed_trades, last_trade_close_time, running, overload_notified, symbol_loss_cooldown, last_attention_alert_time, ip_whitelist_error
    log.info("Monitor thread started.")
    while not monitor_stop_event.is_set():
        if ip_whitelist_error:
            log.warning("IP whitelist error detected. Monitor thread sleeping for 1 hour.")
            time.sleep(3600)
            continue
        try:
            # Check for symbols that require manual attention
            conn = sqlite3.connect(CONFIG["DB_FILE"])
            cur = conn.cursor()
            cur.execute("SELECT * FROM attention_required")
            attention_needed = cur.fetchall()
            conn.close()

            for row in attention_needed:
                symbol, reason, details, ts = row
                now = datetime.utcnow()
                last_alert = last_attention_alert_time.get(symbol)
                # Alert every 5 minutes
                if last_alert is None or (now - last_alert).total_seconds() > 300:
                    send_telegram(f"🚨 ATTENTION REQUIRED on {symbol} 🚨\nReason: {reason}\nDetails: {details}\nTimestamp: {ts}", parse_mode='Markdown')
                    last_attention_alert_time[symbol] = now
        except Exception as e:
            log.exception(f"Failed to check for attention_required symbols: {e}")

        loop_start_time = time.time()
        log.info("Monitor thread loop started.")
        try:
            if client is None:
                time.sleep(5)
                continue

            # --- Process Pending Limit Orders ---
            with pending_limit_orders_lock:
                pending_snapshot = dict(pending_limit_orders)

            if pending_snapshot:
                to_remove_pending = []
                # Check status of all pending orders
                for p_id, p_meta in pending_snapshot.items():
                    try:
                        order_info = client.futures_get_order(symbol=p_meta['symbol'], orderId=p_meta['order_id'])
                        order_status = order_info['status']
                        
                        if order_status == 'FILLED':
                            log.info(f"✅ Limit order {p_id} for {p_meta['symbol']} has been filled!")
                            
                            actual_entry_price = float(order_info.get('avgPrice', p_meta['limit_price']))
                            
                            # The stop_price and take_price are now passed directly from the pending order metadata
                            stop_price = p_meta['stop_price']
                            take_price = p_meta['take_price'] # This is for internal monitoring

                            # Place SL and TP orders together after the limit order is filled.
                            sltp_orders = place_batch_sl_tp_sync(
                                symbol=p_meta['symbol'], side=p_meta['side'],
                                sl_price=stop_price, tp_price=take_price, qty=p_meta['qty']
                            )

                            trade_id = f"{p_meta['symbol']}_managed_{p_meta['order_id']}"
                            meta = {
                                "id": trade_id, "symbol": p_meta['symbol'], "side": p_meta['side'], "entry_price": actual_entry_price,
                                "initial_qty": p_meta['qty'], "qty": p_meta['qty'], "notional": p_meta['qty'] * actual_entry_price,
                                "leverage": p_meta['leverage'],
                                "sl": stop_price,
                                "tp": take_price, # The internally monitored TP level (may be None)
                                "open_time": datetime.utcnow().isoformat(), "sltp_orders": sltp_orders,
                                "be_moved": False,
                                "trailing_active": False,
                                "tp_hit": False,
                                "risk_usdt": p_meta['risk_usdt'],
                                "entry_reason": "LIMIT_FILL",
                                # --- Carry over strategy metadata from pending order ---
                                "strategy_id": p_meta.get('strategy_id', 1),
                                "signal_confidence": p_meta.get('signal_confidence'),
                                "adx_confirmation": p_meta.get('adx_confirmation'),
                                "rsi_confirmation": p_meta.get('rsi_confirmation'),
                                "macd_confirmation": p_meta.get('macd_confirmation'),
                                "atr_at_entry": p_meta.get('atr_at_entry')
                            }
                            # --- S5 initialization of R and TP1 ---
                            if meta['strategy_id'] == 5:
                                r_dist = abs(actual_entry_price - stop_price)
                                meta['s5_initial_sl'] = stop_price
                                meta['s5_r_per_unit'] = r_dist
                                if r_dist and r_dist > 0:
                                    tp1_price = actual_entry_price + r_dist if meta['side'] == 'BUY' else actual_entry_price - r_dist
                                    meta['s5_tp1_price'] = tp1_price
                                else:
                                    meta['s5_tp1_price'] = None
                                # Management state
                                meta['trade_phase'] = 0  # 0: pre-TP1, 1: after TP1 (trailing active)
                                meta['be_moved'] = False
                                meta['trailing_active'] = False

                            with managed_trades_lock:
                                managed_trades[trade_id] = meta

                            # --- Set post-trade cooldown ---
                            symbol_trade_cooldown[p_meta['symbol']] = datetime.now(timezone.utc) + timedelta(minutes=16)
                            log.info(f"Set 16-minute post-trade cooldown for {p_meta['symbol']} after limit order fill.")
                            
                            # Use a simplified record_trade call, as many fields are for the new management style
                            record_trade({
                                'id': trade_id, 'symbol': meta['symbol'], 'side': meta['side'], 'entry_price': meta['entry_price'],
                                'exit_price': None, 'qty': meta['qty'], 'notional': meta['notional'], 'pnl': None,
                                'open_time': meta['open_time'], 'close_time': None, 'risk_usdt': meta['risk_usdt'],
                                'entry_reason': meta.get('entry_reason'), 'tp1': take_price # Store main TP here for reporting
                            })
                            add_managed_trade_to_db(meta) # Keep for now as backup
                            
                            strategy_id_str = f"S{p_meta.get('strategy_id', 'N/A')}"
                            trade_type_str = "BB" if strategy_id_str == "S1" else "ST"
                            
                            filled_order_msg = (
                                f"✅ *Trade Opened: {strategy_id_str}-{trade_type_str}*\n\n"
                                f"**Symbol:** `{p_meta['symbol']}`\n"
                                f"**Side:** `{p_meta['side']}`\n"
                                f"**Entry:** `{actual_entry_price:.4f}`\n"
                                f"**Stop Loss:** `{stop_price:.4f}`\n"
                                f"**Risk:** `{p_meta.get('risk_usdt', 0.0):.2f} USDT`\n"
                                f"**Leverage:** `{p_meta.get('leverage', 0)}x`"
                            )
                            send_telegram(filled_order_msg, parse_mode='Markdown')
                            to_remove_pending.append(p_id)

                        elif order_status in ['CANCELED', 'EXPIRED', 'REJECTED']:
                            log.info(f"Pending order {p_id} was {order_status}. Removing from tracking.")
                            send_telegram(f"❌ Limit Order {order_status} for {p_meta['symbol']}.")
                            to_remove_pending.append(p_id)

                        else: # Still NEW or PARTIALLY_FILLED, check for expiry
                            expiry_time_str = p_meta.get('expiry_time')
                            if expiry_time_str:
                                # New logic: expire at the end of the candle
                                expiry_time = datetime.fromisoformat(expiry_time_str)
                                if datetime.now(timezone.utc) > expiry_time:
                                    log.warning(f"Pending order {p_id} for {p_meta['symbol']} has expired at candle close. Cancelling.")
                                    try:
                                        client.futures_cancel_order(symbol=p_meta['symbol'], orderId=p_meta['order_id'])
                                        send_telegram(f"⌛️ Limit Order for {p_meta['symbol']} expired at candle close and was cancelled.")
                                    except Exception as e:
                                        log_and_send_error(f"Failed to cancel expired order {p_id}", e)
                                    to_remove_pending.append(p_id)
                            else:
                                # Fallback to old logic for orders created before this change
                                timeout_duration = timeframe_to_timedelta(CONFIG['TIMEFRAME']) * CONFIG['ORDER_ENTRY_TIMEOUT']
                                if timeout_duration:
                                    placed_time = datetime.fromisoformat(p_meta['place_time'])
                                    if datetime.utcnow() - placed_time > timeout_duration:
                                        log.warning(f"Pending order {p_id} for {p_meta['symbol']} has timed out (legacy). Cancelling.")
                                        try:
                                            client.futures_cancel_order(symbol=p_meta['symbol'], orderId=p_meta['order_id'])
                                            send_telegram(f"⌛️ Limit Order for {p_meta['symbol']} timed out and was cancelled.")
                                        except Exception as e:
                                            log_and_send_error(f"Failed to cancel timed-out order {p_id}", e)
                                        to_remove_pending.append(p_id)
                    except BinanceAPIException as e:
                        if e.code == -2013: # Order does not exist
                            log.warning(f"Pending order {p_id} not found on exchange. Assuming it was filled/canceled and removing.", e)
                            to_remove_pending.append(p_id)
                            continue
                        else:
                            log_and_send_error(f"API Error processing pending order {p_id}", e)
                            to_remove_pending.append(p_id) # Remove to prevent error loops
                    except Exception as e:
                        log_and_send_error(f"Error processing pending order {p_id}", e)
                        to_remove_pending.append(p_id)

                if to_remove_pending:
                    with pending_limit_orders_lock:
                        for p_id in to_remove_pending:
                            pending_limit_orders.pop(p_id, None)
                            remove_pending_order_from_db(p_id)

            positions = []
            try:
                max_retries = 3
                retry_delay = 10  # seconds
                for attempt in range(max_retries):
                    try:
                        log.debug(f"Monitor thread: fetching positions (attempt {attempt + 1}/{max_retries})...")
                        positions_fetch_start = time.time()
                        positions = client.futures_position_information()
                        positions_fetch_duration = time.time() - positions_fetch_start
                        log.debug(f"Monitor thread: fetching positions took {positions_fetch_duration:.2f}s.")
                        break  # Success
                    except BinanceAPIException as e:
                        if e.code == -1007 and attempt < max_retries - 1:
                            log.warning(f"Timeout fetching positions (attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay}s...")
                            send_telegram(f"⚠️ Binance API timeout, retrying... ({attempt + 1}/{max_retries})")
                            time.sleep(retry_delay)
                            continue
                        raise  # Re-raise the exception if it's not a retryable timeout or the last attempt
                    except requests.exceptions.ReadTimeout as e:
                        if attempt < max_retries - 1:
                            log.warning(f"Read timeout fetching positions (attempt {attempt + 1}/{max_retries}). Retrying in {retry_delay}s: {e}")
                            send_telegram(f"⚠️ Binance API read timeout, retrying... ({attempt + 1}/{max_retries})")
                            time.sleep(retry_delay)
                            continue
                        log.error(f"Final attempt to fetch positions failed due to ReadTimeout: {e}")
                        raise # Re-raise on last attempt
            except BinanceAPIException as e:
                log.error("Caught BinanceAPIException in monitor thread: %s", e)
                
                if e.code == -2015:
                    ip = get_public_ip()
                    error_msg = (
                        f"🚨 **CRITICAL AUTH ERROR** 🚨\n\n"
                        f"Binance API keys are invalid or the server's IP is not whitelisted.\n\n"
                        f"Error Code: `{e.code}`\n"
                        f"Server IP: `{ip}`\n\n"
                        f"The bot is now paused. Please add the new IP to your Binance whitelist and use /startbot to resume."
                    )
                    send_telegram(error_msg, parse_mode='Markdown')
                    ip_whitelist_error = True

                # Handle other, potentially transient, API errors
                html_content = None
                if len(e.args) >= 3:
                    html_content = e.args[2]

                if html_content and isinstance(html_content, str) and html_content.strip().lower().startswith('<!doctype html>'):
                    error_msg = f"Binance API returned an HTML error page. This could be an IP ban or server issue.\nServer IP: {get_public_ip()}"
                    send_telegram(error_msg, document_content=html_content.encode('utf-8'), document_name="binance_error.html")
                else:
                    tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                    safe_tb = _shorten_for_telegram(tb)
                    error_msg = f"Binance API Error fetching positions: {e}\nTrace:\n{safe_tb}\nServer IP: {get_public_ip()}"
                    send_telegram(error_msg)
                
                running = False
                log.info("Bot paused due to API error. Waiting 2 minutes before next attempt...")
                time.sleep(120)
                continue
            
            # --- Position monitoring logic (from original code) ---
            log.debug("Monitor thread: attempting to acquire lock...")
            managed_trades_lock.acquire()
            log.debug("Monitor thread: lock acquired.")
            try:
                trades_snapshot = dict(managed_trades)
            finally:
                managed_trades_lock.release()
                log.debug("Monitor thread: lock released.")
            
            # --- Pre-fetch kline data for all active symbols to reduce API calls ---
            active_symbols = {meta['symbol'] for meta in trades_snapshot.values()}
            kline_data_cache = {}
            for sym_key in active_symbols:
                try:
                    # Fetch enough data for S4 DEMA(200) calculation
                    kline_data_cache[sym_key] = fetch_klines_sync(sym_key, CONFIG["TIMEFRAME"], 250)
                except Exception as e:
                    log.error(f"Failed to pre-fetch klines for {sym_key} in monitor loop: {e}")
                    kline_data_cache[sym_key] = None

            to_remove = []
            for tid, meta in trades_snapshot.items():
                sym = meta['symbol']
                side = meta['side']

                # Determine expected positionSide based on the trade's side
                expected_pos_side = 'LONG' if side == 'BUY' else 'SHORT'

                # Find the specific position (LONG or SHORT)
                pos = next((p for p in positions if p.get('symbol') == sym and p.get('positionSide') == expected_pos_side), None)

                # If we are not in hedge mode, the positionSide is 'BOTH'
                if pos is None and not IS_HEDGE_MODE:
                     pos = next((p for p in positions if p.get('symbol') == sym and p.get('positionSide') == 'BOTH'), None)

                is_closed = False
                unreal_pnl_for_trade = 0.0 # Default PnL to 0
                
                if pos:
                    # We found a matching position, check if its amount is zero.
                    pos_amt = float(pos.get('positionAmt') or 0.0)
                    unreal_pnl_for_trade = float(pos.get('unRealizedProfit') or 0.0)

                    if abs(pos_amt) < 1e-8:
                        is_closed = True
                        # For closed positions, the final PnL is in the unrealizedProfit field upon closure.
                    else:
                        # Position is still open, update its PnL in our managed state
                        with managed_trades_lock:
                            if tid in managed_trades:
                                managed_trades[tid]['unreal'] = unreal_pnl_for_trade
                else:
                    # If we didn't find a position for our specific side, it must be closed.
                    is_closed = True

                if is_closed:
                    close_time = datetime.utcnow().replace(tzinfo=timezone.utc)
                    meta['close_time'] = close_time.isoformat()
                    exit_reason = meta.get('exit_reason', 'SL/TP')

                    # --- Reliable PnL Fetching for Closed Trades ---
                    final_pnl = 0.0
                    exit_price = meta.get('entry_price')  # Fallback exit price

                    if pos and pos.get('unRealizedProfit') is not None and float(pos.get('unRealizedProfit')) != 0.0:
                        final_pnl = float(pos.get('unRealizedProfit'))
                        if pos.get('entryPrice') is not None and float(pos.get('entryPrice')) != 0.0:
                           exit_price = float(pos.get('entryPrice'))
                        log.info(f"Trade PnL for {tid} from position info: {final_pnl:.4f}")
                    else:
                        log.warning(f"Position info for closed trade {tid} is missing/stale. Fetching from income history...")
                        try:
                            open_time_dt = datetime.fromisoformat(meta['open_time'])
                            open_time_ms = int(open_time_dt.timestamp() * 1000)
                            income_history = client.futures_income_history(symbol=sym, incomeType='REALIZED_PNL', startTime=open_time_ms, limit=10)
                            if income_history:
                                last_pnl_record = income_history[0]
                                final_pnl = float(last_pnl_record['income'])
                                log.info(f"PnL for {tid} from income history: {final_pnl:.4f}")
                            else:
                                log.warning(f"No REALIZED_PNL found in income history for {tid} since it opened. PnL is 0.")
                                final_pnl = 0.0
                        except Exception as e:
                            log.exception(f"Failed to fetch income history for {tid}. PnL is 0.")
                            final_pnl = 0.0
                    
                    unreal_pnl_for_trade = final_pnl
                    # --- End of PnL Fetching ---

                    # --- Post-Loss Cooldown Logic ---
                    if unreal_pnl_for_trade < 0:
                        cooldown_end_time = close_time + timedelta(hours=CONFIG['LOSS_COOLDOWN_HOURS'])
                        symbol_loss_cooldown[sym] = cooldown_end_time
                        log.info(f"Symbol {sym} has been placed on a {CONFIG['LOSS_COOLDOWN_HOURS']}h cooldown (until {cooldown_end_time}). PnL: {unreal_pnl_for_trade:.4f}.")
                        send_telegram(f"🧊 {sym} is on a {CONFIG['LOSS_COOLDOWN_HOURS']}h cooldown after a loss.")

                    log.info(f"TRADE_CLOSE_EVENT: ID={tid}, Symbol={sym}, PnL={unreal_pnl_for_trade:.4f}, Reason={exit_reason}. Preparing to record and remove from managed trades.")
                    
                    trade_record = meta.copy()
                    trade_record.update({
                        'exit_price': exit_price,
                        'pnl': unreal_pnl_for_trade,
                        'close_time': meta['close_time'],
                        'exit_reason': exit_reason
                    })
                    record_trade(trade_record)
                    remove_managed_trade_from_db(tid)
                    with managed_trades_lock:
                        last_trade_close_time[sym] = close_time
                    
                    close_msg = (
                        f"✅ *Trade Closed*\n\n"
                        f"**ID:** `{meta['id']}`\n"
                        f"**Symbol:** {sym}\n"
                        f"**Reason:** {exit_reason}\n"
                        f"**PnL:** `{unreal_pnl_for_trade:.4f} USDT`"
                    )
                    send_telegram(close_msg, parse_mode='Markdown')
                    to_remove.append(tid)
                    continue
                
                df_monitor = kline_data_cache.get(sym)
                if df_monitor is None or df_monitor.empty:
                    log.warning(f"Skipping monitoring cycle for {tid} due to missing kline data.")
                    continue

                # --- New In-Trade Management Logic ---
                try:
                    strategy_id = str(meta.get('strategy_id', 1))
                    exit_params = CONFIG['STRATEGY_EXIT_PARAMS'].get(strategy_id, CONFIG['STRATEGY_EXIT_PARAMS']['1'])
                    current_price = df_monitor['close'].iloc[-1]
                    entry_price = meta['entry_price']
                    side = meta['side']

                    if strategy_id == '2':
                        # --- SuperTrend Strategy Exit Logic (Multi-Stage TP) ---
                        trade_phase = meta.get('trade_phase', 0)
                        initial_qty = meta['initial_qty']
                        atr_at_entry = meta.get('atr_at_entry')

                        if not atr_at_entry or atr_at_entry <= 0:
                            log.warning(f"Skipping ST exit logic for {tid} due to invalid atr_at_entry: {atr_at_entry}")
                            continue

                        # TP 1: 30% at 1.2x ATR, move SL to BE
                        if trade_phase == 0:
                            tp1_price = entry_price + 1.2 * atr_at_entry if side == 'BUY' else entry_price - 1.2 * atr_at_entry
                            if (side == 'BUY' and current_price >= tp1_price) or (side == 'SELL' and current_price <= tp1_price):
                                log.info(f"S2-TP1 HIT for {tid}. Closing 30%.")
                                qty_to_close = round_qty(sym, initial_qty * 0.3)
                                if qty_to_close > 0:
                                    close_partial_market_position_sync(sym, side, qty_to_close)
                                
                                cancel_trade_sltp_orders_sync(meta)
                                new_qty = meta['qty'] - qty_to_close
                                new_sl_price = entry_price # Move to BE
                                new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl_price, qty=new_qty) if new_qty > 0 else {}

                                trade_to_update_in_db = None
                                with managed_trades_lock:
                                    if tid in managed_trades:
                                        managed_trades[tid].update({
                                            'qty': new_qty, 'sltp_orders': new_orders, 'sl': new_sl_price,
                                            'trade_phase': 1, 'be_moved': True, 'trailing_active': False # Deactivate normal trailing until final phase
                                        })
                                        trade_to_update_in_db = managed_trades[tid].copy()
                                if trade_to_update_in_db:
                                    add_managed_trade_to_db(trade_to_update_in_db)
                                send_telegram(f"✅ S2-TP1 Hit for {sym}. Closed 30%, SL moved to BE.")
                                continue
                        
                        # TP 2: 30% at 2.5x ATR, move SL to 1x ATR profit
                        if trade_phase == 1:
                            tp2_price = entry_price + 2.5 * atr_at_entry if side == 'BUY' else entry_price - 2.5 * atr_at_entry
                            if (side == 'BUY' and current_price >= tp2_price) or (side == 'SELL' and current_price <= tp2_price):
                                log.info(f"S2-TP2 HIT for {tid}. Closing another 30%.")
                                qty_to_close = round_qty(sym, initial_qty * 0.3)
                                if qty_to_close > 0:
                                    close_partial_market_position_sync(sym, side, qty_to_close)
                                
                                cancel_trade_sltp_orders_sync(meta)
                                new_qty = meta['qty'] - qty_to_close
                                new_sl_price = entry_price + atr_at_entry if side == 'BUY' else entry_price - atr_at_entry # Move SL to 1R
                                new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl_price, qty=new_qty) if new_qty > 0 else {}

                                trade_to_update_in_db = None
                                with managed_trades_lock:
                                    if tid in managed_trades:
                                        managed_trades[tid].update({
                                            'qty': new_qty, 'sltp_orders': new_orders, 'sl': new_sl_price,
                                            'trade_phase': 2, 'trailing_active': True # Activate trailing for final part
                                        })
                                        trade_to_update_in_db = managed_trades[tid].copy()
                                if trade_to_update_in_db:
                                    add_managed_trade_to_db(trade_to_update_in_db)
                                send_telegram(f"✅ S2-TP2 Hit for {sym}. Closed 30%, SL moved to 1R profit. Trailing activated.")
                                continue
                        continue # End of S2 logic
                    
                    elif strategy_id == '3':
                        # --- Strategy 3: Simple ATR Trailing Stop ---
                        s3_params = CONFIG['STRATEGY_3']
                        if not meta.get('trailing', True) or not s3_params.get('TRAILING_ENABLED', True):
                            continue # Skip if trailing is disabled for the trade or globally for S3

                        is_trailing_active = meta.get('s3_trailing_active', False)
                        
                        # Activate trailing if profit target is hit
                        if not is_trailing_active:
                            profit_pct = (current_price / entry_price - 1) if side == 'BUY' else (1 - current_price / entry_price)
                            if profit_pct >= s3_params['TRAILING_ACTIVATION_PROFIT_PCT']:
                                log.info(f"S3: Activating trailing stop for {tid} at {profit_pct:.2f}% profit.")
                                is_trailing_active = True
                                with managed_trades_lock:
                                    if tid in managed_trades:
                                        managed_trades[tid]['s3_trailing_active'] = True
                                        add_managed_trade_to_db(managed_trades[tid]) # Persist the state change
                        
                        # If trailing is active, calculate and update the SL
                        if is_trailing_active:
                            # Calculate ATR for trailing
                            df_monitor['atr_trail'] = atr(df_monitor, length=s3_params['TRAILING_ATR_PERIOD'])
                            atr_now = safe_last(df_monitor['atr_trail'])
                            
                            if atr_now > 0:
                                trail_dist = s3_params['TRAILING_ATR_MULTIPLIER'] * atr_now
                                current_sl = meta.get('s3_trailing_stop', meta['sl'])
                                new_sl = None

                                if side == 'BUY':
                                    potential_sl = current_price - trail_dist
                                    if potential_sl > current_sl: new_sl = potential_sl
                                else: # SELL
                                    potential_sl = current_price + trail_dist
                                    if potential_sl < current_sl: new_sl = potential_sl
                                
                                if new_sl:
                                    log.info(f"S3 Trailing SL for {tid}. Old: {current_sl:.4f}, New: {new_sl:.4f}")
                                    cancel_trade_sltp_orders_sync(meta)
                                    new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl, qty=meta['qty'])
                                    with managed_trades_lock:
                                        if tid in managed_trades:
                                            managed_trades[tid].update({
                                                'sl': new_sl,
                                                'sltp_orders': new_orders,
                                                's3_trailing_stop': new_sl
                                            })
                                            add_managed_trade_to_db(managed_trades[tid])
                                    # send_telegram(f"📈 S3 Trailing SL updated for {tid} ({sym}) to `{new_sl:.4f}`", parse_mode='Markdown')
                        continue # End of S3 logic
                    
                    elif strategy_id == '4':
                        # --- 3x SuperTrend (S4) In-Trade Management ---
                        df_with_indicators = calculate_all_indicators(df_monitor.copy())
                        if df_with_indicators is None or len(df_with_indicators) < 3:
                            log.warning(f"S4 Monitor: Not enough data for {sym} to manage trade, skipping.")
                            continue
                        
                        # --- Panic Exit Logic (Intra-candle) ---
                        s4_params = CONFIG['STRATEGY_4']
                        forming_candle = df_with_indicators.iloc[-1]
                        
                        # Check if forming_candle has valid data for panic check
                        panic_check_cols = ['high', 'low', 'atr', 'close', 's4_st1', 's4_st2', 's4_st3']
                        if all(col in forming_candle and pd.notna(forming_candle[col]) for col in panic_check_cols):
                            candle_size = forming_candle['high'] - forming_candle['low']
                            atr_threshold = forming_candle['atr'] * s4_params['VOLATILITY_EXIT_ATR_MULT']
                            
                            price_cross_st = False
                            current_price = forming_candle['close']
                            st1 = forming_candle['s4_st1']
                            st2 = forming_candle['s4_st2']
                            st3 = forming_candle['s4_st3']
                            
                            if side == 'BUY' and (current_price < st1 or current_price < st2 or current_price < st3):
                                price_cross_st = True
                            elif side == 'SELL' and (current_price > st1 or current_price > st2 or current_price > st3):
                                price_cross_st = True
                            
                            if price_cross_st and candle_size > atr_threshold:
                                log.warning(f"S4 PANIC EXIT for {tid} ({sym}). Price crossed ST and candle size ({candle_size:.4f}) > ATR threshold ({atr_threshold:.4f}).")
                                try:
                                    cancel_trade_sltp_orders_sync(meta)
                                    close_partial_market_position_sync(sym, side, meta['qty'])
                                    send_telegram(f"🚨 S4 PANIC EXIT for {sym} due to high volatility against the trend.")
                                    with managed_trades_lock:
                                        if tid in managed_trades:
                                            managed_trades[tid]['exit_reason'] = "VOLATILITY_EXIT"
                                            add_managed_trade_to_db(managed_trades[tid])
                                except Exception as e:
                                    log_and_send_error(f"Failed to execute S4 panic exit for {tid}", e)
                                continue # Skip other logic for this trade as it's being closed

                        # --- Standard Exit Logic (Post-candle) ---
                        
                        # Use the last closed candle for the normal exit signal check
                        exit_check_candle = df_with_indicators.iloc[-2]
                        
                        # --- Defensive check for exit signal columns ---
                        exit_check_cols = ['s4_st1_dir', 's4_st2_dir', 's4_st3_dir']
                        if not all(col in exit_check_candle and pd.notna(exit_check_candle[col]) for col in exit_check_cols):
                            log.warning(f"S4 Monitor: Skipping exit check for {tid} due to missing indicator data on closed candle.")
                            continue

                        st1_dir = exit_check_candle['s4_st1_dir']
                        st2_dir = exit_check_candle['s4_st2_dir']
                        st3_dir = exit_check_candle['s4_st3_dir']

                        # 1. Exit on Color Change (based on closed candle)
                        exit_signal = False
                        if side == 'BUY' and (st1_dir == -1 or st2_dir == -1 or st3_dir == -1):
                            exit_signal = True
                            log.info(f"S4 Exit Signal: BUY trade {tid} for {sym} closing due to ST color change on closed candle.")
                        elif side == 'SELL' and (st1_dir == 1 or st2_dir == 1 or st3_dir == 1):
                            exit_signal = True
                            log.info(f"S4 Exit Signal: SELL trade {tid} for {sym} closing due to ST color change on closed candle.")

                        if exit_signal:
                            try:
                                cancel_trade_sltp_orders_sync(meta)
                                close_partial_market_position_sync(sym, side, meta['qty'])
                                send_telegram(f"🚨 S4 trade for {sym} closed due to SuperTrend color change.")
                                with managed_trades_lock:
                                    if tid in managed_trades:
                                        managed_trades[tid]['exit_reason'] = "ST_COLOR_CHANGE"
                                        add_managed_trade_to_db(managed_trades[tid])
                            except Exception as e:
                                log_and_send_error(f"Failed to execute S4 exit for {tid}", e)
                            continue

                        # 2. Trailing Stop with Middle SuperTrend (ST2) (based on latest data)
                        if meta.get('trailing', True):
                            last_candle = df_with_indicators.iloc[-1]
                            
                            # --- Defensive check for trailing stop columns ---
                            trail_check_cols = ['s4_st2', 'close']
                            if not all(col in last_candle and pd.notna(last_candle[col]) for col in trail_check_cols):
                                log.warning(f"S4 Monitor: Skipping trailing stop check for {tid} due to missing indicator data on last candle.")
                                continue

                            potential_sl = last_candle['s4_st2']
                            current_sl = meta['sl']
                            current_price = last_candle['close']
                            new_sl = None

                            if side == 'BUY' and potential_sl > current_sl and potential_sl < current_price:
                                new_sl = potential_sl
                            elif side == 'SELL' and potential_sl < current_sl and potential_sl > current_price:
                                new_sl = potential_sl

                            if new_sl:
                                log.info(f"S4 Trailing SL for {tid}. Old: {current_sl:.4f}, New: {new_sl:.4f}")
                                cancel_trade_sltp_orders_sync(meta)
                                new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl, qty=meta['qty'])
                                trade_to_update_in_db = None
                                with managed_trades_lock:
                                    if tid in managed_trades:
                                        managed_trades[tid].update({
                                            'sl': new_sl,
                                            'sltp_orders': new_orders
                                        })
                                        trade_to_update_in_db = managed_trades[tid].copy()
                                if trade_to_update_in_db:
                                    add_managed_trade_to_db(trade_to_update_in_db)
                                    # send_telegram(f"📈 S4 Trailing SL updated for {tid} ({sym}) to `{new_sl:.4f}`", parse_mode='Markdown')
                        continue

                    elif strategy_id == '5':
                        # --- Strategy 5: In-Trade Manager ---
                        s5 = CONFIG['STRATEGY_5']
                        entry_price = meta['entry_price']
                        current_price = float(df_monitor['close'].iloc[-1])
                        r_dist = float(meta.get('s5_r_per_unit') or abs(entry_price - meta.get('s5_initial_sl', entry_price)))
                        if r_dist <= 0:
                            # Cannot manage without valid R
                            continue

                        # 1) Move SL to BE at +0.5R
                        if not meta.get('be_moved', False):
                            half_r_price = entry_price + 0.5 * r_dist if side == 'BUY' else entry_price - 0.5 * r_dist
                            if (side == 'BUY' and current_price >= half_r_price) or (side == 'SELL' and current_price <= half_r_price):
                                try:
                                    cancel_trade_sltp_orders_sync(meta)
                                    new_sl = entry_price
                                    new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl, qty=meta['qty'])
                                    with managed_trades_lock:
                                        if tid in managed_trades:
                                            managed_trades[tid].update({
                                                'sl': new_sl,
                                                'sltp_orders': new_orders,
                                                'be_moved': True
                                            })
                                            add_managed_trade_to_db(managed_trades[tid])
                                    send_telegram(f"📈 S5 BE moved for {sym}. SL set to BE at {new_sl:.4f}.")
                                except Exception as e:
                                    log_and_send_error(f"Failed to move S5 SL to BE for {tid}", e)
                                # fall-through to TP1 logic if price already beyond

                        # 2) Partial 30% at 1R, then activate trailing
                        trade_phase = int(meta.get('trade_phase', 0))
                        if trade_phase == 0:
                            tp1_price = meta.get('s5_tp1_price')
                            if tp1_price and (
                                (side == 'BUY' and current_price >= tp1_price) or
                                (side == 'SELL' and current_price <= tp1_price)
                            ):
                                try:
                                    qty_to_close = round_qty(sym, meta['initial_qty'] * s5.get('TP1_CLOSE_PCT', 0.3))
                                    if qty_to_close > 0:
                                        close_partial_market_position_sync(sym, side, qty_to_close)
                                        # reduce qty and move SL to BE if not already
                                        remaining_qty = max(0.0, meta['qty'] - qty_to_close)
                                        cancel_trade_sltp_orders_sync(meta)
                                        new_sl = entry_price  # BE after TP1
                                        new_orders = {}
                                        if remaining_qty > 0:
                                            new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl, qty=remaining_qty)
                                        with managed_trades_lock:
                                            if tid in managed_trades:
                                                managed_trades[tid].update({
                                                    'qty': remaining_qty,
                                                    'sl': new_sl,
                                                    'sltp_orders': new_orders,
                                                    'trade_phase': 1,
                                                    'be_moved': True,
                                                    'trailing_active': True  # Activate trailing after TP1
                                                })
                                                add_managed_trade_to_db(managed_trades[tid])
                                        send_telegram(f"✅ S5 TP1 hit for {sym}. Closed {s5.get('TP1_CLOSE_PCT',0.3)*100:.0f}%, SL to BE, trailing activated.")
                                        continue
                                except Exception as e:
                                    log_and_send_error(f"Failed S5 TP1 management for {tid}", e)

                        # 3) Hybrid ATR/pivot trailing with buffer when trailing_active
                        if meta.get('trailing_active', False) and meta.get('trailing', True) and meta.get('qty', 0) > 0:
                            try:
                                # Compute ATR on monitor data
                                atr_series = atr(df_monitor, s5.get('ATR_PERIOD', 14))
                                atr_now = safe_last(atr_series, default=0.0)
                                if atr_now <= 0:
                                    continue
                                buffer = s5.get('TRAIL_BUFFER_MULT', 0.25) * atr_now
                                atr_trail_component = s5.get('TRAIL_ATR_MULT', 1.0) * atr_now

                                # Pivot: recent swing low/high
                                pivot_lookback = 5
                                pivot_low = swing_low(df_monitor['low'], lookback=pivot_lookback)
                                pivot_high = swing_high(df_monitor['high'], lookback=pivot_lookback)

                                current_sl = float(meta.get('sl', entry_price))

                                new_sl = None
                                if side == 'BUY':
                                    candidate = max(pivot_low, current_price - atr_trail_component) - buffer
                                    # ensure SL stays below price and only ratchets up
                                    if candidate > current_sl and candidate < current_price:
                                        new_sl = candidate
                                else:  # SELL
                                    candidate = min(pivot_high, current_price + atr_trail_component) + buffer
                                    if candidate < current_sl and candidate > current_price:
                                        new_sl = candidate

                                if new_sl:
                                    log.info(f"S5 Trailing SL update for {tid}. Old: {current_sl:.4f}, New: {new_sl:.4f}")
                                    cancel_trade_sltp_orders_sync(meta)
                                    new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl, qty=meta['qty'])
                                    with managed_trades_lock:
                                        if tid in managed_trades:
                                            managed_trades[tid].update({
                                                'sl': new_sl,
                                                'sltp_orders': new_orders
                                            })
                                            add_managed_trade_to_db(managed_trades[tid])
                            except Exception as e:
                                log_and_send_error(f"Failed S5 trailing management for {tid}", e)
                        continue

                    # --- Generic BE & Trailing Logic ---
                    # This block handles Strategy 1's BE/Trailing, and Strategy 2's final trailing phase.
                    
                    # Break-Even Trigger (Only for S1, or S2 if it hasn't hit TP1 yet)
                    if not meta.get('be_moved'):
                        profit_pct = (current_price / entry_price - 1) if side == 'BUY' else (1 - current_price / entry_price)
                        if profit_pct >= exit_params['BE_TRIGGER']:
                            log.info(f"Trade {tid} (S{strategy_id}) hit BE trigger. Moving SL.")
                            cancel_trade_sltp_orders_sync(meta)
                            new_sl_price = entry_price * (1 + exit_params['BE_SL_OFFSET'] if side == 'BUY' else 1 - exit_params['BE_SL_OFFSET'])
                            new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl_price, qty=meta['qty'])
                            trade_to_update_in_db = None
                            with managed_trades_lock:
                                if tid in managed_trades:
                                    managed_trades[tid].update({'sl': new_sl_price, 'sltp_orders': new_orders, 'be_moved': True, 'trailing_active': True})
                                    trade_to_update_in_db = managed_trades[tid].copy()
                            if trade_to_update_in_db:
                                add_managed_trade_to_db(trade_to_update_in_db)
                            send_telegram(f"📈 Breakeven triggered for {tid} ({sym}). SL moved to {new_sl_price:.4f} and trailing stop activated.")
                            continue

                    # Trailing Stop (For S1 after BE, and for S2 after TP2)
                    if meta.get('trailing_active') and meta.get('trailing', True):
                        atr_now = safe_latest_atr_from_df(df_monitor)
                        
                        volatility_ratio = atr_now / current_price if current_price > 0 else 0
                        adx(df_monitor, period=CONFIG['ADX_PERIOD'])
                        trend_strength = safe_last(df_monitor.get('adx'), default=0)
                        
                        if strategy_id == '2':
                            atr_multiplier = 2.5
                        else:
                            atr_multiplier = calculate_trailing_distance(strategy_id, volatility_ratio, trend_strength)
                        
                        log.debug(f"Trailing for {sym} (S{strategy_id}): vol_ratio={volatility_ratio:.4f}, adx={trend_strength:.2f}, atr_mult={atr_multiplier:.2f}")

                        new_sl = None
                        current_sl = meta['sl']
                        
                        if side == 'BUY':
                            potential_sl = current_price - (atr_now * atr_multiplier)
                            if potential_sl > current_sl: new_sl = potential_sl
                        else: # SELL
                            potential_sl = current_price + (atr_now * atr_multiplier)
                            if potential_sl < current_sl: new_sl = potential_sl
                        
                        if new_sl:
                            log.info(f"Trailing SL for {tid}. Old: {current_sl:.4f}, New: {new_sl:.4f}")
                            cancel_trade_sltp_orders_sync(meta)
                            new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl, qty=meta['qty'])
                            trade_to_update_in_db = None
                            with managed_trades_lock:
                                if tid in managed_trades:
                                    managed_trades[tid].update({'sl': new_sl, 'sltp_orders': new_orders})
                                    trade_to_update_in_db = managed_trades[tid].copy()
                            if trade_to_update_in_db:
                                add_managed_trade_to_db(trade_to_update_in_db)
                                # send_telegram(f"📈 Trailing SL updated for {tid} ({sym}) to `{new_sl:.4f}`")
                
                except Exception as e:
                    log_and_send_error(f"Failed to process in-trade management logic for {tid}", e)

            if to_remove:
                log.info(f"Preparing to remove {len(to_remove)} closed trade(s) from managed state: {to_remove}")
                with managed_trades_lock:
                    log.info(f"State before removal: {len(managed_trades)} trades. Keys: {list(managed_trades.keys())}")
                    for tid in to_remove:
                        removed_trade = managed_trades.pop(tid, None)
                        if removed_trade:
                            log.info(f"Successfully removed trade {tid} from in-memory state.")
                        else:
                            log.warning(f"Attempted to remove trade {tid} from state, but it was not found.")
                    log.info(f"State after removal: {len(managed_trades)} trades. Keys: {list(managed_trades.keys())}")

            # --- Overload Monitoring ---
            loop_end_time = time.time()
            duration = loop_end_time - loop_start_time
            if duration > CONFIG["MONITOR_LOOP_THRESHOLD_SEC"]:
                if not overload_notified:
                    log.warning(f"Monitor loop took {duration:.2f}s to complete, exceeding threshold of {CONFIG['MONITOR_LOOP_THRESHOLD_SEC']}s.")
                    send_telegram(f"⚠️ Bot Alert: The main monitoring loop is running slow ({duration:.2f}s), which may indicate server overload and could affect performance.")
                    overload_notified = True
            elif overload_notified:
                # Reset notification flag if performance is back to normal
                log.info("Monitor loop performance is back to normal.")
                overload_notified = False
            
            # The loop should sleep for at least a little bit, but subtract processing time
            # to keep the cycle time relatively constant.
            log.debug(f"Monitor thread loop finished. Total duration: {duration:.2f}s.")
            sleep_duration = max(0.1, 5 - duration)
            time.sleep(sleep_duration)

        except Exception as e:
            log.exception("An unhandled exception occurred in monitor thread. Bot will be paused.")
            try:
                tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                safe_tb = _shorten_for_telegram(tb)
                send_telegram(f"CRITICAL ERROR in monitor thread: {e}\nTrace:\n{safe_tb}\nBot paused.")
            except Exception as send_exc:
                log.error("Failed to send critical error notification from monitor thread: %s", send_exc)
            running = False
            time.sleep(30) # Sleep before next attempt after a critical failure

    log.info("Monitor thread exiting.")

def daily_pnl_monitor_thread_func():
    global running, daily_loss_limit_hit, daily_profit_limit_hit, current_daily_pnl, last_trade_close_time, frozen
    log.info("Daily PnL monitor thread started.")

    last_check_date = datetime.now(timezone.utc).date()

    while not monitor_stop_event.is_set():
        try:
            # Daily Reset Logic
            current_date = datetime.now(timezone.utc).date()
            if current_date != last_check_date:
                log.info(f"New day detected. Resetting daily PnL limits.")
                if daily_loss_limit_hit:
                    send_telegram("☀️ New day, daily loss limit has been reset.")
                if daily_profit_limit_hit:
                    send_telegram("☀️ New day, daily profit limit has been reset.")
                
                daily_loss_limit_hit = False
                daily_profit_limit_hit = False
                current_daily_pnl = 0.0
                last_check_date = current_date
                
                with managed_trades_lock:
                    last_trade_close_time.clear()
                    log.info("Cleared last_trade_close_time for all symbols.")

            # PnL Check Logic
            conn = sqlite3.connect(CONFIG["DB_FILE"])
            cur = conn.cursor()
            today_str = datetime.now(timezone.utc).strftime('%Y-%m-%d')
            cur.execute("SELECT SUM(pnl) FROM trades WHERE DATE(close_time) = ?", (today_str,))
            result = cur.fetchone()[0]
            conn.close()
            daily_pnl = result if result is not None else 0.0
            
            if daily_pnl != current_daily_pnl:
                log.info(f"DAILY_PNL_UPDATE: Old PnL: {current_daily_pnl:.4f}, New PnL from DB: {daily_pnl:.4f}")

            current_daily_pnl = daily_pnl

            # Loss Limit Check
            if not daily_loss_limit_hit and CONFIG["MAX_DAILY_LOSS"] != 0:
                log.info(f"Daily PnL check: {daily_pnl:.2f} USDT vs Loss Limit {CONFIG['MAX_DAILY_LOSS']:.2f}")
                if daily_pnl <= CONFIG["MAX_DAILY_LOSS"]:
                    log.warning(f"MAX DAILY LOSS LIMIT HIT! PnL: {daily_pnl:.2f}, Limit: {CONFIG['MAX_DAILY_LOSS']:.2f}")
                    frozen = True
                    daily_loss_limit_hit = True
                    send_telegram(f"🚨 MAX DAILY LOSS LIMIT HIT! 🚨\nToday's PnL: {daily_pnl:.2f} USDT\nLimit: {CONFIG['MAX_DAILY_LOSS']:.2f} USDT\nBot is now FROZEN. Use /unfreeze to resume trading.")
            
            # Profit Limit Check
            if not daily_profit_limit_hit and CONFIG["MAX_DAILY_PROFIT"] > 0:
                log.info(f"Daily PnL check: {daily_pnl:.2f} USDT vs Profit Target {CONFIG['MAX_DAILY_PROFIT']:.2f}")
                if daily_pnl >= CONFIG["MAX_DAILY_PROFIT"]:
                    log.warning(f"MAX DAILY PROFIT TARGET HIT! PnL: {daily_pnl:.2f}, Target: {CONFIG['MAX_DAILY_PROFIT']:.2f}")
                    daily_profit_limit_hit = True
                    
                    freeze_msg = ""
                    if CONFIG["AUTO_FREEZE_ON_PROFIT"]:
                        frozen = True
                        freeze_msg = "\nBot is now FROZEN (no new entries)."

                    send_telegram(f"🎉 MAX DAILY PROFIT TARGET HIT! 🎉\nToday's PnL: {daily_pnl:.2f} USDT\nTarget: {CONFIG['MAX_DAILY_PROFIT']:.2f} USDT{freeze_msg}")

            # On success, reset failure counter
            pnl_monitor_consecutive_failures = 0
            # Sleep for the configured interval
            time.sleep(CONFIG["DAILY_PNL_CHECK_INTERVAL"])

        except Exception as e:
            pnl_monitor_consecutive_failures += 1
            log.exception(f"An unhandled exception occurred in the daily PnL monitor thread (failure #{pnl_monitor_consecutive_failures}).")
            
            sleep_duration = 120  # Default 2 minutes sleep on error
            if pnl_monitor_consecutive_failures >= 3:
                send_telegram(f"🚨 CRITICAL: The Daily PnL Monitor thread has failed {pnl_monitor_consecutive_failures} consecutive times. It will now sleep for 1 hour. Please investigate the logs.")
                sleep_duration = 3600 # 1 hour
            
            time.sleep(sleep_duration)
    
    log.info("Daily PnL monitor thread exiting.")


def monthly_maintenance_thread_func():
    global last_maintenance_month
    log.info("Monthly maintenance thread started.")
    
    # Load the last run month from a state file to persist across restarts
    try:
        with open("maintenance_state.json", "r") as f:
            state = json.load(f)
            last_maintenance_month = state.get("last_maintenance_month", "")
    except FileNotFoundError:
        last_maintenance_month = ""
        log.info("maintenance_state.json not found, starting fresh.")

    while not monitor_stop_event.is_set():
        try:
            now = datetime.now(timezone.utc)
            current_month_str = now.strftime('%Y-%m')

            # Run on the 2nd day of the month to ensure all data from the 1st is settled
            if now.day == 2 and current_month_str != last_maintenance_month:
                log.info(f"Running monthly maintenance for previous month...")

                first_day_of_current_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                last_day_of_previous_month = first_day_of_current_month - timedelta(days=1)
                year = last_day_of_previous_month.year
                month = last_day_of_previous_month.month

                log.info(f"Generating report for {year}-{month:02d}...")
                asyncio.run_coroutine_threadsafe(generate_and_send_monthly_report(year, month), main_loop)
                
                # Add a small delay to ensure the report sends before we prune the data
                time.sleep(15)

                log.info(f"Pruning database records for {year}-{month:02d}...")
                prune_trades_db(year, month)
                
                last_maintenance_month = current_month_str
                # Persist state
                try:
                    with open("maintenance_state.json", "w") as f:
                        json.dump({"last_maintenance_month": last_maintenance_month}, f)
                except IOError as e:
                    log.error(f"Could not write maintenance state file: {e}")
                
                log.info(f"Monthly maintenance for {year}-{month:02d} complete. Next check in 1 hour.")

            # Sleep for an hour before checking again
            time.sleep(3600)

        except Exception as e:
            log.exception("An error occurred in the monthly maintenance thread.")
            time.sleep(3600) # Wait an hour before retrying on error

    log.info("Monthly maintenance thread exiting.")


# --- Performance Alerter ---
alert_states = {
    "trades_per_day": {"alerted": False, "threshold": 5},
    "win_rate_3d": {"alerted": False, "threshold": 50.0},
    "avg_rr_7d": {"alerted": False, "threshold": 1.8},
}

def performance_alerter_thread_func():
    log.info("Performance alerter thread started.")
    # Initial sleep to allow some data to accumulate
    time.sleep(3600) 

    while not monitor_stop_event.is_set():
        try:
            now = datetime.now(timezone.utc)
            conn = sqlite3.connect(CONFIG["DB_FILE"])
            
            # 1. Check Trades per Day (last 24 hours)
            one_day_ago = (now - timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')
            trades_last_24h_df = pd.read_sql_query("SELECT COUNT(*) FROM trades WHERE open_time >= ?", conn, params=(one_day_ago,))
            if not trades_last_24h_df.empty:
                trades_last_24h = trades_last_24h_df.iloc[0,0]
                if trades_last_24h < alert_states["trades_per_day"]["threshold"] and not alert_states["trades_per_day"]["alerted"]:
                    send_telegram(f"📉 *Performance Alert: Low Trade Frequency*\n\n- Trades in last 24h: {trades_last_24h}\n- Threshold: < {alert_states['trades_per_day']['threshold']}")
                    alert_states["trades_per_day"]["alerted"] = True
                elif trades_last_24h >= alert_states["trades_per_day"]["threshold"] and alert_states["trades_per_day"]["alerted"]:
                    send_telegram("✅ *Performance Restored: Trade Frequency*")
                    alert_states["trades_per_day"]["alerted"] = False

            # 2. Check Win Rate (last 3 days)
            three_days_ago = (now - timedelta(days=3)).strftime('%Y-%m-%d %H:%M:%S')
            df_3d = pd.read_sql_query("SELECT pnl FROM trades WHERE open_time >= ?", conn, params=(three_days_ago,))
            if not df_3d.empty and len(df_3d) > 5: # Only check if there's a reasonable number of trades
                win_rate_3d = (len(df_3d[df_3d['pnl'] > 0]) / len(df_3d)) * 100
                if win_rate_3d < alert_states["win_rate_3d"]["threshold"] and not alert_states["win_rate_3d"]["alerted"]:
                    send_telegram(f"📉 *Performance Alert: Low Win Rate*\n\n- Win Rate (last 3d): {win_rate_3d:.2f}%\n- Threshold: < {alert_states['win_rate_3d']['threshold']}%")
                    alert_states["win_rate_3d"]["alerted"] = True
                elif win_rate_3d >= alert_states["win_rate_3d"]["threshold"] and alert_states["win_rate_3d"]["alerted"]:
                    send_telegram("✅ *Performance Restored: Win Rate*")
                    alert_states["win_rate_3d"]["alerted"] = False

            # 3. Check Avg R:R (last 7 days)
            seven_days_ago = (now - timedelta(days=7)).strftime('%Y-%m-%d %H:%M:%S')
            df_7d = pd.read_sql_query("SELECT pnl, risk_usdt FROM trades WHERE open_time >= ? AND risk_usdt > 0", conn, params=(seven_days_ago,))
            if not df_7d.empty and len(df_7d) > 5: # Only check if there's a reasonable number of trades
                avg_rr_7d = (df_7d['pnl'] / df_7d['risk_usdt']).mean()
                if avg_rr_7d < alert_states["avg_rr_7d"]["threshold"] and not alert_states["avg_rr_7d"]["alerted"]:
                     send_telegram(f"📉 *Performance Alert: Low Avg R:R*\n\n- Avg R:R (last 7d): {avg_rr_7d:.2f}R\n- Threshold: < {alert_states['avg_rr_7d']['threshold']}R")
                     alert_states["avg_rr_7d"]["alerted"] = True
                elif avg_rr_7d >= alert_states["avg_rr_7d"]["threshold"] and alert_states["avg_rr_7d"]["alerted"]:
                    send_telegram("✅ *Performance Restored: Average R:R*")
                    alert_states["avg_rr_7d"]["alerted"] = False

            conn.close()
            
            # On success, reset failure counter
            alerter_consecutive_failures = 0
            # Sleep for 6 hours
            time.sleep(6 * 3600)
        except Exception as e:
            alerter_consecutive_failures += 1
            log.exception(f"An unhandled exception occurred in the performance alerter thread (failure #{alerter_consecutive_failures}).")
            
            sleep_duration = 3600  # Default 1 hour sleep on error
            if alerter_consecutive_failures >= 3:
                send_telegram(f"🚨 CRITICAL: The Performance Alerter thread has failed {alerter_consecutive_failures} consecutive times. It will now sleep for 6 hours. Please investigate the logs.")
                sleep_duration = 6 * 3600 # 6 hours
            
            time.sleep(sleep_duration)


async def manage_session_freeze_state():
    """
    Checks session freeze status, sends notifications, and returns the effective freeze state.
    Returns True if the bot's trading logic should be frozen, False otherwise.
    """
    global frozen, session_freeze_override, session_freeze_active, notified_frozen_session

    if not CONFIG.get("SESSION_FREEZE_ENABLED", True):
        # If the feature is disabled, it can never be frozen by a session.
        # Reset any lingering session state and return only the manual freeze status.
        if session_freeze_active:
            log.info("Session freeze feature is disabled. Resetting state.")
            session_freeze_active = False
            session_freeze_override = False
        return frozen

    is_naturally_frozen, session_name = get_session_freeze_status(datetime.now(timezone.utc))

    # Determine the effective freeze status for the bot's trading logic.
    # The bot is frozen if it's manually frozen OR if it's in a natural session freeze that has NOT been overridden.
    is_effectively_frozen = frozen or (is_naturally_frozen and not session_freeze_override)

    # --- Handle notifications and state changes for natural session freezes ---
    if is_naturally_frozen:
        if not session_freeze_active:  # A new natural freeze period has just begun
            log.info(f"Entering session freeze for {session_name}.")
            # A new natural freeze always resets any previous user override.
            if session_freeze_override:
                log.info("Resetting user override because a new session freeze has started.")
                session_freeze_override = False
            
            await asyncio.to_thread(send_telegram, f"⚠️ Session Change: {session_name}\\nThe bot is now frozen for this session. Use /unfreeze to override.")
            session_freeze_active = True
            notified_frozen_session = session_name
    
    else:  # Not in a natural freeze window
        if session_freeze_active:  # A natural freeze period has just ended
            log.info("Exiting session freeze period.")
            await asyncio.to_thread(send_telegram, f"✅ Session freeze for {notified_frozen_session} has ended. The bot is now active again.")
            session_freeze_active = False
            notified_frozen_session = None
            # Also reset the override flag when a session naturally ends, so it doesn't carry over.
            if session_freeze_override:
                session_freeze_override = False

    return is_effectively_frozen


async def run_scan_cycle():
    """
    Runs a single concurrent scan of all symbols, limited by a semaphore
    to prevent overwhelming the thread pool.
    """
    global scan_cycle_count
    sem = asyncio.Semaphore(4)

    if await manage_session_freeze_state():
        log.info("Scan cycle skipped due to session freeze.")
        return

    async def throttled_eval(symbol):
        async with sem:
            # The return value of evaluate_and_enter is not used, but we await it
            # to ensure the semaphore is held for the duration of the evaluation.
            return await evaluate_and_enter(symbol)

    log.info("Starting concurrent symbol scan (concurrency limit: 4)...")
    symbols = [s.strip().upper() for s in CONFIG["SYMBOLS"] if s.strip()]
    tasks = [throttled_eval(s) for s in symbols]
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for symbol, result in zip(symbols, results):
        if isinstance(result, Exception):
            # Log the exception with traceback for better debugging
            log.exception(f"Error evaluating symbol {symbol} during concurrent scan: {result}")
    
    scan_cycle_count += 1
    
    # --- NEW: Clear indicator cache to free memory ---
    log.info("Clearing indicator cache to conserve memory.")
    indicator_cache.clear()


async def scanning_loop():
    global initial_sync_complete, scan_cycle_count, next_scan_time
    initial_sync_complete = True # Set this so status message works correctly from the start

    while True:
        try:
            if not running:
                await asyncio.sleep(2)
                continue

            # --- Calculate time to next candle ---
            timeframe_str = CONFIG["TIMEFRAME"]
            timeframe_delta = timeframe_to_timedelta(timeframe_str)
            
            if not timeframe_delta:
                # Fallback for invalid timeframe
                log.error(f"Invalid timeframe '{timeframe_str}', falling back to fixed 60s scan interval.")
                await run_scan_cycle()
                await asyncio.sleep(CONFIG.get("SCAN_INTERVAL", 60))
                continue

            now = datetime.now(timezone.utc)
            timeframe_seconds = timeframe_delta.total_seconds()
            next_candle_open_ts = ((now.timestamp() // timeframe_seconds) + 1) * timeframe_seconds
            next_candle_open_dt = datetime.fromtimestamp(next_candle_open_ts, tz=timezone.utc)
            seconds_to_next_candle = (next_candle_open_dt - now).total_seconds()
            
            # --- Pre-candle Pause Logic (with interruptible sleep) ---
            if seconds_to_next_candle < 120: # 2 minutes
                sync_buffer = CONFIG.get("CANDLE_SYNC_BUFFER_SEC", 10)
                sleep_duration = seconds_to_next_candle + sync_buffer
                
                log.info(f"Approaching new {timeframe_str} candle. Pausing all scanning for {sleep_duration:.2f} seconds to sync.")
                next_scan_time = datetime.now(timezone.utc) + timedelta(seconds=sleep_duration)

                end_time = datetime.now(timezone.utc) + timedelta(seconds=sleep_duration)
                while datetime.now(timezone.utc) < end_time:
                    if not running:
                        log.info("Bot stopped during pre-candle pause.")
                        break  # Exit the sleep loop
                    await asyncio.sleep(1)  # Sleep in 1-second increments
                
                continue # Restart the loop (will immediately check the 'running' flag at the top)

            # --- Continuous Scanning Logic ---
            log.info(f"Continuous scan cycle starting. Stopping in {seconds_to_next_candle - 120:.2f} seconds.")
            while seconds_to_next_candle >= 120:
                if not running:
                    log.info("Bot stopped during continuous scan cycle.")
                    break

                await run_scan_cycle()
                
                # Short sleep to prevent CPU pegging and allow other tasks to run
                await asyncio.sleep(1) 

                # Re-check time to next candle to see if we should break the inner loop
                now = datetime.now(timezone.utc)
                seconds_to_next_candle = (next_candle_open_dt - now).total_seconds()
                next_scan_time = now + timedelta(seconds=1) # Update for status message

            log.info("Continuous scan cycle finished. Will pause until next candle.")

        except asyncio.CancelledError:
            log.info("Scanning loop cancelled.")
            break
        except Exception as e:
            log.exception("An unhandled error occurred in the main scanning loop: %s", e)
            await asyncio.sleep(60)

def _generate_pnl_report_sync(query: str, params: tuple, title: str) -> tuple[str, Optional[bytes]]:
    """A helper function to generate a PnL report from a given SQL query."""
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    try:
        df = pd.read_sql_query(query, conn, params=params)
    finally:
        conn.close()

    if df.empty:
        return (f"No trades found for the report period: {title}", None)

    # --- Calculate Metrics ---
    total_trades = len(df)
    winning_trades = len(df[df['pnl'] > 0])
    losing_trades = total_trades - winning_trades
    win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0.0
    
    total_pnl = df['pnl'].sum()

    # R:R Calculation
    rr_df = df[df['risk_usdt'] > 0].copy()
    if not rr_df.empty:
        rr_df['rr'] = rr_df['pnl'] / rr_df['risk_usdt']
        average_rr = rr_df['rr'].mean()
    else:
        average_rr = 0.0

    # Max Drawdown Calculation
    df['cumulative_pnl'] = df['pnl'].cumsum()
    df['running_max'] = df['cumulative_pnl'].cummax()
    df['drawdown'] = df['running_max'] - df['cumulative_pnl']
    max_drawdown = df['drawdown'].max()
    
    # --- Format Text Report ---
    summary_text = (
        f"*Summary*\n"
        f"  - Total Trades: {total_trades}\n"
        f"  - Winning Trades: {winning_trades}\n"
        f"  - Losing Trades: {losing_trades}\n"
        f"  - Win Rate: {win_rate:.2f}%\n\n"
        f"*PnL & Risk*\n"
        f"  - Total PnL: {total_pnl:.2f} USDT\n"
        f"  - Max Drawdown: -{max_drawdown:.2f} USDT\n"
        f"  - Avg R:R: {average_rr:.2f}R\n"
    )
    report_text = f"{title}\n\n{summary_text}"

    # --- Generate PnL Chart ---
    df['close_time'] = pd.to_datetime(df['close_time'])
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(df['close_time'], df['cumulative_pnl'], marker='o', linestyle='-')
    
    ax.set_title(f'Cumulative PnL: {title.splitlines()[0]}')
    ax.set_xlabel('Date')
    ax.set_ylabel('Cumulative PnL (USDT)')
    ax.grid(True)
    fig.autofmt_xdate()

    buf = io.BytesIO()
    fig.savefig(buf, format='png', bbox_inches='tight')
    plt.close(fig)
    buf.seek(0)
    
    return (report_text, buf.getvalue())


def _generate_strategy_report_sync() -> str:
    """
    Generates a comparative performance report for each strategy, including
    advanced metrics like trades per day, confidence/volatility analysis.
    """
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    try:
        # Fetch all necessary columns
        query = "SELECT strategy_id, pnl, risk_usdt, signal_confidence, open_time, entry_price, atr_at_entry FROM trades WHERE strategy_id IS NOT NULL AND pnl IS NOT NULL"
        df = pd.read_sql_query(query, conn)
    finally:
        conn.close()

    if df.empty:
        return "No trades with strategy information found to generate a report."

    report_lines = ["📊 *Aggressive Strategy Performance Report*\n"]
    
    df['strategy_id'] = df['strategy_id'].astype(int).astype(str)
    df['open_time'] = pd.to_datetime(df['open_time'])

    # Calculate total trading days for "trades per day" metric
    total_days = (df['open_time'].max() - df['open_time'].min()).days
    total_days = max(1, total_days) # Avoid division by zero

    strategy_names = {'1': "Bollinger Bands (S1)", '2': "SuperTrend (S2)"}
    
    for strategy_id in sorted(list(df['strategy_id'].unique())):
        group = df[df['strategy_id'] == strategy_id].copy()
        strategy_name = strategy_names.get(strategy_id, f"Unknown Strategy ({strategy_id})")
        
        # --- Basic Metrics ---
        total_trades = len(group)
        trades_per_day = total_trades / total_days
        winning_trades = len(group[group['pnl'] > 0])
        win_rate = (winning_trades / total_trades) * 100 if total_trades > 0 else 0.0
        total_pnl = group['pnl'].sum()
        rr_df = group[group['risk_usdt'] > 0].copy()
        average_rr = (rr_df['pnl'] / rr_df['risk_usdt']).mean() if not rr_df.empty else 0.0
        
        report_lines.append(f"\n*{strategy_name}*")
        report_lines.append(f"  - Total Trades: {total_trades} (~{trades_per_day:.2f}/day)")
        report_lines.append(f"  - Win Rate: {win_rate:.2f}%")
        report_lines.append(f"  - Total PnL: {total_pnl:.2f} USDT")
        report_lines.append(f"  - Avg R:R: {average_rr:.2f}R")
        
        # --- Volatility-Adjusted Performance ---
        vol_group = group.dropna(subset=['atr_at_entry', 'entry_price'])
        if not vol_group.empty:
            vol_group['vol_ratio'] = vol_group['atr_at_entry'] / vol_group['entry_price']
            
            # Define volatility buckets
            low_vol_trades = vol_group[vol_group['vol_ratio'] < 0.01]
            high_vol_trades = vol_group[vol_group['vol_ratio'] >= 0.01]

            report_lines.append("\n  *Volatility Analysis*")
            for name, bucket in [("Low Vol (<1%)", low_vol_trades), ("High Vol (>=1%)", high_vol_trades)]:
                if not bucket.empty:
                    b_trades = len(bucket)
                    b_wr = ((bucket['pnl'] > 0).sum() / b_trades * 100) if b_trades > 0 else 0
                    b_pnl = bucket['pnl'].sum()
                    report_lines.append(f"    - **{name}**: {b_trades} trades | WR: {b_wr:.1f}% | PnL: {b_pnl:.2f} USDT")

        # --- Confidence Score Analysis for Strategy 2 ---
        if strategy_id == '2':
            confidence_group = group.dropna(subset=['signal_confidence'])
            if not confidence_group.empty:
                bins = [55, 65, 75, 101]
                labels = ['55-64%', '65-74%', '75-100%']
                confidence_group['confidence_bin'] = pd.cut(confidence_group['signal_confidence'], bins=bins, labels=labels, right=False)

                report_lines.append("\n  *Confidence Score Analysis*")
                
                analysis = confidence_group.groupby('confidence_bin', observed=True).agg(
                    total_trades=('pnl', 'count'),
                    winning_trades=('pnl', lambda x: (x > 0).sum()),
                    average_pnl=('pnl', 'mean')
                ).reset_index()

                if not analysis.empty:
                    analysis['win_rate'] = (analysis['winning_trades'] / analysis['total_trades'] * 100).fillna(0)
                    for _, row in analysis.iterrows():
                        if row['total_trades'] > 0:
                            report_lines.append(f"    - **{row['confidence_bin']}**: {row['total_trades']} trades | WR: {row['win_rate']:.1f}% | Avg PnL: {row['average_pnl']:.2f} USDT")

    return "\n".join(report_lines)


async def generate_and_send_monthly_report(year: int, month: int):
    """Generates and sends a performance report for a specific month."""
    title = f"🗓️ *Monthly Performance Report for {year}-{month:02d}*"
    start_date = f"{year}-{month:02d}-01"
    next_month_val = month + 1
    next_year_val = year
    if next_month_val > 12:
        next_month_val = 1
        next_year_val += 1
    end_date = f"{next_year_val}-{next_month_val:02d}-01"
    
    query = "SELECT close_time, pnl, risk_usdt FROM trades WHERE close_time >= ? AND close_time < ? AND pnl IS NOT NULL ORDER BY close_time ASC"
    params = (start_date, end_date)
    
    try:
        report_text, chart_bytes = await asyncio.to_thread(_generate_pnl_report_sync, query, params, title)
        await asyncio.to_thread(
            send_telegram,
            msg=report_text,
            document_content=chart_bytes,
            document_name=f"pnl_report_{year}-{month:02d}.png",
            parse_mode='Markdown'
        )
    except Exception as e:
        log.exception(f"Error generating monthly report for {year}-{month:02d}")
        await asyncio.to_thread(send_telegram, f"An error occurred while generating the monthly report: {e}")


async def generate_and_send_strategy_report():
    """
    Generates and sends a strategy performance comparison report.
    """
    try:
        report_text = await asyncio.to_thread(_generate_strategy_report_sync)
        await asyncio.to_thread(
            send_telegram,
            msg=report_text,
            parse_mode='Markdown'
        )
    except Exception as e:
        log.exception("Error generating strategy report")
        await asyncio.to_thread(send_telegram, f"An error occurred while generating the strategy report: {e}")


def get_memory_info() -> dict:
    """
    Gets memory usage, attempting to be container-aware by checking cgroups.
    Falls back to system-wide memory if not in a container.
    """
    # Default values using psutil for host system
    mem_info = psutil.virtual_memory()
    total_mem_gb = mem_info.total / (1024**3)
    used_mem_gb = mem_info.used / (1024**3)
    percent_used = mem_info.percent
    is_container = False

    cgroup_paths = [
        "/sys/fs/cgroup/memory.max",  # cgroup v2
        "/sys/fs/cgroup/memory/memory.limit_in_bytes"  # cgroup v1
    ]

    for path in cgroup_paths:
        try:
            with open(path, "r") as f:
                limit = int(f.read().strip())
                # If limit is valid and smaller than host memory, use it
                if 0 < limit < mem_info.total:
                    process = psutil.Process(os.getpid())
                    used_mem_bytes = process.memory_info().rss
                    
                    total_mem_gb = limit / (1024**3)
                    used_mem_gb = used_mem_bytes / (1024**3)
                    percent_used = (used_mem_bytes / limit) * 100
                    is_container = True
                    break # Found a valid cgroup limit, no need to check further
        except (IOError, ValueError):
            continue # Try next path or fallback to host metrics
            
    return {
        "percent": percent_used,
        "total_gb": total_mem_gb,
        "used_gb": used_mem_gb,
        "is_container": is_container
    }


async def generate_and_send_report():
    """
    Fetches all trade data, calculates analytics, generates a PnL chart,
    and sends the report via Telegram.
    """
    title = "📊 *Overall Performance Report*"
    query = "SELECT close_time, pnl, risk_usdt FROM trades WHERE close_time IS NOT NULL AND pnl IS NOT NULL ORDER BY close_time ASC"
    params = ()
    
    try:
        report_text, chart_bytes = await asyncio.to_thread(_generate_pnl_report_sync, query, params, title)
        
        await asyncio.to_thread(
            send_telegram,
            msg=report_text,
            document_content=chart_bytes,
            document_name="pnl_report_overall.png",
            parse_mode='Markdown'
        )
    except Exception as e:
        log.exception("Error generating report")
        await asyncio.to_thread(send_telegram, f"An error occurred while generating the report: {e}")

def generate_adv_chart_sync(symbol: str):
    try:
        df = fetch_klines_sync(symbol, CONFIG["TIMEFRAME"], limit=200)
        if df is None or df.empty:
            return "Could not fetch k-line data for " + symbol, None

        df['sma'] = sma(df['close'], CONFIG["SMA_LEN"])
        df['bbu'], df['bbl'] = bollinger_bands(df['close'], CONFIG["BB_LENGTH_CUSTOM"], CONFIG["BB_STD_CUSTOM"])

        conn = sqlite3.connect(CONFIG["DB_FILE"])
        trades_df = pd.read_sql_query(f"SELECT * FROM trades WHERE symbol = '{symbol}' AND close_time IS NOT NULL", conn)
        conn.close()

        addplots = []
        if not trades_df.empty:
            trades_df['open_time'] = pd.to_datetime(trades_df['open_time'])
            trades_df['close_time'] = pd.to_datetime(trades_df['close_time'])
            
            buy_entries = trades_df[trades_df['side'] == 'BUY']['open_time']
            sell_entries = trades_df[trades_df['side'] == 'SELL']['open_time']
            exits = trades_df['close_time']

            # Create a dataframe with the same index as the main df for plotting
            plot_buy_entries = pd.Series(np.nan, index=df.index)
            plot_sell_entries = pd.Series(np.nan, index=df.index)
            plot_exits = pd.Series(np.nan, index=df.index)

            plot_buy_entries.loc[buy_entries] = df['low'].loc[buy_entries] * 0.98
            plot_sell_entries.loc[sell_entries] = df['high'].loc[sell_entries] * 1.02
            plot_exits.loc[exits] = df['close'].loc[exits]

            addplots.append(mpf.make_addplot(plot_buy_entries, type='scatter', marker='^', color='g', markersize=100))
            addplots.append(mpf.make_addplot(plot_sell_entries, type='scatter', marker='v', color='r', markersize=100))
            addplots.append(mpf.make_addplot(plot_exits, type='scatter', marker='x', color='blue', markersize=100))

        sma_plot = mpf.make_addplot(df['sma'], color='purple', width=0.7)
        bb_plots = mpf.make_addplot(df[['bbu', 'bbl']], color=['blue', 'blue'], width=0.5, linestyle='--')
        addplots.insert(0, bb_plots)
        addplots.insert(0, sma_plot)
        
        fig, axes = mpf.plot(
            df,
            type='candle',
            style='yahoo',
            title=f'{symbol} Chart with SMA/BB and Trades',
            ylabel='Price (USDT)',
            addplot=addplots,
            returnfig=True,
            figsize=(15, 8),
            volume=True,
            panel_ratios=(3, 1)
        )
        
        buf = io.BytesIO()
        fig.savefig(buf, format='png', bbox_inches='tight')
        buf.seek(0)
        
        return f"Chart for {symbol}", buf.getvalue()

    except Exception as e:
        log.exception(f"Failed to generate advanced chart for {symbol}")
        return f"Error generating chart for {symbol}: {e}", None

async def get_managed_trades_snapshot():
    log.debug("Attempting to acquire managed_trades_lock in get_managed_trades_snapshot...")
    async with managed_trades_lock:
        log.debug("Acquired managed_trades_lock in get_managed_trades_snapshot.")
        snapshot = dict(managed_trades)
    log.debug("Released managed_trades_lock in get_managed_trades_snapshot.")
    return snapshot

async def get_pending_orders_snapshot():
    async with pending_limit_orders_lock:
        return dict(pending_limit_orders)

async def run_simulation(strategy_to_run: str, symbol: str, days: int):
    """
    Runs a simulation of the bot's strategies over a historical period.
    """
    await asyncio.to_thread(send_telegram, f"🚀 Starting simulation for strategy `{strategy_to_run}` on `{symbol}` over the last `{days}` day(s)...", parse_mode='Markdown')
    
    original_strat_mode = CONFIG["STRATEGY_MODE"]
    try:
        # Temporarily override strategy mode for simulation
        if strategy_to_run == "ALL":
            CONFIG["STRATEGY_MODE"] = [0] # Auto mode calculates all
        else:
            CONFIG["STRATEGY_MODE"] = [int(strategy_to_run[1:])]

        timeframe = CONFIG["TIMEFRAME"]
        tf_delta = timeframe_to_timedelta(timeframe)
        if not tf_delta:
            await asyncio.to_thread(send_telegram, f"❌ Invalid timeframe for simulation: {timeframe}")
            return
            
        candles_per_day = int(timedelta(days=1).total_seconds() / tf_delta.total_seconds())
        num_candles_to_simulate = candles_per_day * days
        
        # Fetch extra data for indicator lookback periods
        lookback_period = 250 
        total_candles_to_fetch = num_candles_to_simulate + lookback_period
        
        await asyncio.to_thread(send_telegram, f"Fetching {total_candles_to_fetch} candles of `{timeframe}` data for `{symbol}`...", parse_mode='Markdown')
        
        df_full = await asyncio.to_thread(fetch_klines_sync, symbol, timeframe, total_candles_to_fetch)
        
        if df_full is None or len(df_full) < total_candles_to_fetch:
            await asyncio.to_thread(send_telegram, "❌ Not enough historical data available to run the full simulation.")
            return

        simulated_open_trades = []
        signal_count = 0
        
        # Main simulation loop
        for i in range(lookback_period, len(df_full)):
            df_slice = df_full.iloc[:i]
            
            # It's more efficient to calculate all indicators once
            df_with_indicators = calculate_all_indicators(df_slice.copy())

            # --- Manage existing simulated trades ---
            active_trades_copy = list(simulated_open_trades) # Iterate over a copy
            for trade in active_trades_copy:
                result = manage_simulated_trade(trade, df_with_indicators)
                if result:
                    event, price = result
                    timestamp_dt = df_with_indicators.index[-1].to_pydatetime()
                    
                    if event in ["SL_HIT", "TP_HIT"]:
                        pnl = (price - trade['entry_price']) * (1 if trade['side'] == 'BUY' else -1)
                        duration = timestamp_dt - trade['entry_time']
                        
                        msg = (
                            f"💥 *SIM: Trade Closed* 💥\n\n"
                            f"**Reason:** {event}\n"
                            f"**Strategy:** `{trade['strategy']}`\n"
                            f"**Exit Price:** `{price:.4f}`\n"
                            f"**PnL:** `{pnl:.4f}` (approx.)\n"
                            f"**Duration:** `{format_timedelta(duration)}`"
                        )
                        await asyncio.to_thread(send_telegram, msg, parse_mode='Markdown')
                        simulated_open_trades.remove(trade)

                    elif event == "SL_MOVED":
                        msg = (
                            f"📈 *SIM: Trailing SL Moved*\n\n"
                            f"**Strategy:** `{trade['strategy']}`\n"
                            f"**New SL:** `{price:.4f}`"
                        )
                        await asyncio.to_thread(send_telegram, msg, parse_mode='Markdown')

            # --- Check for new signals ---
            # Avoid opening a new trade if one is already open for this symbol
            if not any(t['symbol'] == symbol for t in simulated_open_trades):
                signals = []
                if strategy_to_run in ["S1", "ALL"]:
                    signals.append(simulate_strategy_bb(symbol, df_with_indicators))
                if strategy_to_run in ["S2", "ALL"]:
                    signals.append(simulate_strategy_supertrend(symbol, df_with_indicators))
                if strategy_to_run in ["S3", "ALL"]:
                    signals.append(simulate_strategy_3(symbol, df_with_indicators))
                if strategy_to_run in ["S4", "ALL"]:
                    signals.append(simulate_strategy_4(symbol, df_with_indicators))

                for signal in signals:
                    if signal:
                        signal_count += 1
                        timestamp_dt = datetime.fromisoformat(signal['timestamp'])
                        
                        # Create a simulated trade object
                        simulated_trade = {
                            "symbol": symbol,
                            "side": signal['side'],
                            "entry_price": signal['entry_price'],
                            "sl": signal['sl_price'],
                            "tp": signal['tp_price'],
                            "entry_time": timestamp_dt,
                            "strategy": signal['strategy']
                        }
                        simulated_open_trades.append(simulated_trade)
                        
                        msg = (
                            f"🔔 *SIM: Signal Found* 🔔\n\n"
                            f"**Strategy:** `{signal['strategy']}`\n"
                            f"**Symbol:** `{symbol}`\n"
                            f"**Side:** `{signal['side']}`\n"
                            f"**Timestamp:** `{timestamp_dt.strftime('%Y-%m-%d %H:%M:%S UTC')}`\n"
                            f"**Entry:** `{signal['entry_price']:.4f}`\n"
                            f"**SL:** `{signal['sl_price']:.4f}`\n"
                            f"**TP:** `{signal['tp_price']:.4f}`"
                        )
                        await asyncio.to_thread(send_telegram, msg, parse_mode='Markdown')
                        # Since we opened a trade, we don't check for other signals on this candle
                        break

        summary_msg = f"✅ Simulation for `{symbol}` complete. Found `{signal_count}` total signal(s)."
        await asyncio.to_thread(send_telegram, summary_msg, parse_mode='Markdown')

    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"An error occurred during simulation for {symbol}", e)
    finally:
        # Restore original strategy mode
        CONFIG["STRATEGY_MODE"] = original_strat_mode


def manage_simulated_trade(trade: Dict[str, Any], df_slice: pd.DataFrame) -> Optional[tuple[str, float]]:
    """
    Manages a single simulated trade for one candle tick.
    Checks for SL/TP hits and updates trailing stops.
    Returns a tuple of (event_type, price) if an event occurs, otherwise None.
    """
    current_candle = df_slice.iloc[-1]
    side = trade['side']
    sl_price = trade['sl']
    tp_price = trade['tp']
    candle_low = current_candle['low']
    candle_high = current_candle['high']

    # Check for Stop Loss
    if side == 'BUY' and candle_low <= sl_price:
        return ("SL_HIT", sl_price)
    if side == 'SELL' and candle_high >= sl_price:
        return ("SL_HIT", sl_price)

    # Check for Take Profit (if it exists)
    if tp_price > 0:
        if side == 'BUY' and candle_high >= tp_price:
            return ("TP_HIT", tp_price)
        if side == 'SELL' and candle_low <= tp_price:
            return ("TP_HIT", tp_price)

    # Trailing Stop Logic (adapted from monitor_thread_func)
    strategy_id = trade.get('strategy', '1') # Default to S1 for safety
    exit_params = CONFIG['STRATEGY_EXIT_PARAMS'].get(strategy_id, CONFIG['STRATEGY_EXIT_PARAMS']['1'])
    
    # Simple trailing for now, can be enhanced with BE logic later
    atr_now = safe_last(df_slice.get('atr'), default=0)
    if atr_now > 0:
        atr_multiplier = exit_params.get("ATR_MULTIPLIER", 1.5)
        new_sl = None
        
        if side == 'BUY':
            potential_sl = candle_high - (atr_now * atr_multiplier)
            if potential_sl > sl_price:
                new_sl = potential_sl
        else: # SELL
            potential_sl = candle_low + (atr_now * atr_multiplier)
            if potential_sl < sl_price:
                new_sl = potential_sl
        
        if new_sl:
            trade['sl'] = new_sl # Update the trade dict directly
            return ("SL_MOVED", new_sl)
            
    return None


def build_control_keyboard():
    buttons = [
        [KeyboardButton("/startbot"), KeyboardButton("/stopbot")],
        [KeyboardButton("/freeze"), KeyboardButton("/unfreeze"), KeyboardButton("/sessionfreeze")],
        [KeyboardButton("/listorders"), KeyboardButton("/listpending")],
        [KeyboardButton("/status"), KeyboardButton("/showparams")],
        [KeyboardButton("/usage"), KeyboardButton("/report"), KeyboardButton("/stratreport")],
        [KeyboardButton("/reject"), KeyboardButton("/help"), KeyboardButton("/simulate")]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def handle_callback_query_sync(update, loop):
    query = update.callback_query
    try:
        query.answer()
        data = query.data
        log.info(f"Received callback query: {data}")

        parts = data.split('_')
        action, percent_str, trade_id = parts[0], parts[1], "_".join(parts[2:])
        
        percent = int(percent_str)

        async def _task():
            trades = await get_managed_trades_snapshot()
            if trade_id not in trades:
                await asyncio.to_thread(send_telegram, f"Trade {trade_id} not found or already closed.")
                return

            trade = trades[trade_id]
            symbol = trade['symbol']
            side = trade['side']
            initial_qty = trade['initial_qty']
            
            qty_to_close = initial_qty * (percent / 100.0)
            qty_to_close = await asyncio.to_thread(round_qty, symbol, qty_to_close)

            if qty_to_close <= 0:
                await asyncio.to_thread(send_telegram, f"Calculated quantity to close for {trade_id} is zero. No action taken.")
                return

            try:
                if percent == 100:
                    # Closing 100% is a full close, let the monitor thread handle it by cancelling orders and closing position
                    await asyncio.to_thread(cancel_close_orders_sync, symbol)
                    pos = client.futures_position_information(symbol=symbol)[0]
                    qty_to_close = float(pos['positionAmt'])
                    await asyncio.to_thread(close_partial_market_position_sync, symbol, side, abs(qty_to_close))
                    msg = f"✅ Closing 100% of {trade_id} ({symbol})."
                else:
                    await asyncio.to_thread(close_partial_market_position_sync, symbol, side, qty_to_close)
                    msg = f"✅ Closing {percent}% of {trade_id} ({symbol})."
                
                await asyncio.to_thread(query.edit_message_text, text=f"{query.message.text}\n\nAction: {msg}")
            except Exception as e:
                log.exception(f"Failed to execute action for callback {data}")
                await asyncio.to_thread(send_telegram, f"❌ Error processing action for {trade_id}: {e}")

        asyncio.run_coroutine_threadsafe(_task(), loop)

    except Exception as e:
        log.exception("Error in handle_callback_query_sync")

async def run_test_order(strategy_id: int, symbol: str, full_test: bool):
    """
    Dispatcher for test order commands. Fetches data and calls the relevant
    strategy function with test parameters.
    """
    try:
        # Fetch fresh data to ensure all calculations are based on the latest market state
        df = await asyncio.to_thread(fetch_klines_sync, symbol, CONFIG["TIMEFRAME"], 300)
        df['atr'] = atr(df, CONFIG["ATR_LENGTH"])
        
        # Use a random side for the test signal
        side = random.choice(['BUY', 'SELL'])
        log.info(f"Running test order with random side: {side}")
        test_params = {'test_signal': side, 'full_test': full_test}

        # Dispatch to the correct strategy evaluation function with test parameters
        if strategy_id == 1:
            await evaluate_strategy_bb(symbol, df, **test_params)
        elif strategy_id == 2:
            await evaluate_strategy_supertrend(symbol, df, **test_params)
        elif strategy_id == 3:
            await evaluate_strategy_3(symbol, df, **test_params)
        elif strategy_id == 4:
            await evaluate_strategy_4(symbol, df, **test_params)
        else:
            # This case should ideally not be hit due to checks in the command handler
            await asyncio.to_thread(send_telegram, f"Invalid strategy ID {strategy_id} for test order.")
            return

    except Exception as e:
        log.exception(f"Error during run_test_order for S{strategy_id} on {symbol}")
        await asyncio.to_thread(send_telegram, f"❌ An error occurred during the test: {e}")

def handle_update_sync(update, loop):
    try:
        if update is None:
            return
        if update.callback_query:
            handle_callback_query_sync(update, loop)
            return
        if getattr(update, 'message', None):
            msg = update.message
            text = (msg.text or "").strip()

            # --- Automatic Parameter Editing ---
            param_match = re.match(r'^\s*([A-Z_]+)\s*=\s*(.+)$', text, re.IGNORECASE)
            if param_match:
                key, val_str = param_match.groups()
                key = key.upper()
                
                if key in CONFIG:
                    old_val = CONFIG[key]
                    try:
                        new_val = None
                        if isinstance(old_val, bool):
                            new_val = val_str.lower() in ("1", "true", "yes", "on")
                        elif isinstance(old_val, int):
                            new_val = int(val_str)
                        elif isinstance(old_val, float):
                            new_val = float(val_str)
                        elif isinstance(old_val, list):
                            new_val = [x.strip().upper() for x in val_str.split(",")]
                        else: # Assumes string
                            new_val = val_str
                        
                        CONFIG[key] = new_val
                        send_telegram(f"✅ Parameter updated: {key} = {CONFIG[key]}")
                        return # Stop further processing
                    except (ValueError, TypeError) as e:
                        send_telegram(f"❌ Error setting {key}: Invalid value '{val_str}'. Please provide a valid value. Error: {e}")
                        return
            # --- End Automatic Parameter Editing ---

            if text.startswith("/startbot"):
                if daily_loss_limit_hit:
                    send_telegram(f"❌ Cannot start bot: Daily loss limit of {CONFIG['MAX_DAILY_LOSS']:.2f} USDT has been reached. Bot will remain paused until the next UTC day.")
                else:
                    fut = asyncio.run_coroutine_threadsafe(_set_running(True), loop)
                    try: fut.result(timeout=5)
                    except Exception as e: log.error("Failed to execute /startbot action: %s", e)
                    send_telegram("✅ Bot is now **RUNNING**.", parse_mode='Markdown')
                    
                    # Start the periodic rogue position checker
                    async def start_rogue_checker():
                        global rogue_check_task
                        if rogue_check_task and not rogue_check_task.done():
                            log.info("Rogue position checker task is already running.")
                            send_telegram("Rogue position checker is already active.")
                            return
                        
                        log.info("Performing initial check for rogue positions...")
                        send_telegram("Performing initial check for rogue positions...")
                        await check_and_import_rogue_trades()
                        
                        log.info("Starting hourly rogue position checker...")
                        rogue_check_task = asyncio.create_task(periodic_rogue_check_loop())
                        send_telegram("Hourly rogue position checker started.")

                    asyncio.run_coroutine_threadsafe(start_rogue_checker(), loop)
            elif text.startswith("/stopbot"):
                fut = asyncio.run_coroutine_threadsafe(_set_running(False), loop)
                try: fut.result(timeout=5)
                except Exception as e: log.error("Failed to execute /stopbot action: %s", e)
                send_telegram("🛑 Bot is now **STOPPED**.", parse_mode='Markdown')

                # Stop the periodic rogue position checker
                async def stop_rogue_checker():
                    global rogue_check_task
                    if rogue_check_task and not rogue_check_task.done():
                        rogue_check_task.cancel()
                        try:
                            await rogue_check_task
                        except asyncio.CancelledError:
                            log.info("Rogue position checker task cancelled successfully.")
                        rogue_check_task = None
                        send_telegram("Hourly rogue position checker stopped.")
                    else:
                        send_telegram("Hourly rogue position checker was not running.")

                asyncio.run_coroutine_threadsafe(stop_rogue_checker(), loop)
            elif text.startswith("/freeze"):
                fut = asyncio.run_coroutine_threadsafe(_freeze_command(), loop)
                try: fut.result(timeout=5)
                except Exception as e: log.error("Failed to execute /freeze action: %s", e)
                send_telegram("❄️ Bot is now **FROZEN**. It will not open new trades.", parse_mode='Markdown')
            elif text.startswith("/unfreeze"):
                fut = asyncio.run_coroutine_threadsafe(_unfreeze_command(), loop)
                try: fut.result(timeout=5)
                except Exception as e: log.error("Failed to execute /unfreeze action: %s", e)
                send_telegram("✅ Bot is now **UNFROZEN**. Active session freeze has been overridden.", parse_mode='Markdown')
            elif text.startswith("/status"):
                log.info("Received /status command. Scheduling task.")
                fut = asyncio.run_coroutine_threadsafe(get_managed_trades_snapshot(), loop)
                trades = {}
                try:
                    log.info("Waiting for /status task future result...")
                    trades = fut.result(timeout=10) # Increased timeout for debugging
                    log.info("Got /status task future result.")
                except Exception as e:
                    log.error("Failed to get managed trades for /status: %s", e, exc_info=True)
                
                unrealized_pnl = sum(float(v.get('unreal', 0.0)) for v in trades.values())
                
                # Account & PnL Info
                balance = get_account_balance_usdt()
                balance_str = f"{balance:.2f} USDT" if balance > 0 else "N/A (API Error?)"
                
                pnl_info = (
                    f"Today's Realized PnL: {current_daily_pnl:.2f} USDT\n"
                    f"Current Unrealized PnL: {unrealized_pnl:.2f} USDT"
                )
                if daily_loss_limit_hit:
                    pnl_info += f"\n(LIMIT REACHED: {CONFIG['MAX_DAILY_LOSS']:.2f})"

                # Bot Status section
                status_lines = [f"▶️ Running: *{running}*"]
                status_lines.append(f"✋ Manual Freeze: *{frozen}*")
                
                # Session freeze status
                is_natural_freeze, session_name = get_session_freeze_status(datetime.now(timezone.utc))
                effective_freeze = frozen or (is_natural_freeze and not session_freeze_override)
                session_status_text = f"❄️ Effective Freeze: *{effective_freeze}*"
                details = [d for d in [("Manual" if frozen else None), (f"Session: {session_name}" if is_natural_freeze else None), ("Overridden" if session_freeze_override else None)] if d]
                if details: session_status_text += f" ({'; '.join(details)})"
                status_lines.append(session_status_text)
                
                status_lines.append(f"📈 Managed Trades: *{len(trades)}*")
                
                # Scan Cycle Info
                status_lines.append(f"🔄 Total Scans: *{scan_cycle_count}*")
                if next_scan_time and running:
                    time_until_next_scan = next_scan_time - datetime.now(timezone.utc)
                    if time_until_next_scan.total_seconds() > 0:
                        status_lines.append(f"⏳ Next Scan In: *{format_timedelta(time_until_next_scan)}*")
                        status_lines.append(f"🗓️ Next Scan At: *{next_scan_time.strftime('%H:%M:%S')} UTC*")

                # Next Candle Info
                timeframe_str = CONFIG["TIMEFRAME"]
                timeframe_delta = timeframe_to_timedelta(timeframe_str)
                if timeframe_delta:
                    now = datetime.now(timezone.utc)
                    timeframe_seconds = timeframe_delta.total_seconds()
                    last_close_timestamp = (now.timestamp() // timeframe_seconds) * timeframe_seconds
                    next_candle_open_dt = datetime.fromtimestamp(last_close_timestamp + timeframe_seconds, tz=timezone.utc)
                    status_lines.append(f"🕯️ Next {timeframe_str} Candle: *{next_candle_open_dt.strftime('%H:%M:%S')} UTC*")

                # Combine sections
                txt = (
                    f"📊 *Bot Status*\n\n"
                    f"{'\n'.join(status_lines)}\n\n"
                    f"💰 *Account Info*\n"
                    f"Available Balance: *{balance_str}*\n\n"
                    f"📈 *Trade PnL Info*\n{pnl_info}"
                )
                send_telegram(txt, parse_mode='Markdown')
                try:
                    telegram_bot.send_message(chat_id=int(TELEGRAM_CHAT_ID), text="Controls:", reply_markup=build_control_keyboard())
                except Exception:
                    log.exception("Failed to send telegram keyboard")
            elif text.startswith("/ip") or text.startswith("/forceip"):
                ip = get_public_ip()
                send_telegram(f"Server IP: {ip}")
            elif text.startswith("/listorders"):
                fut = asyncio.run_coroutine_threadsafe(get_managed_trades_snapshot(), loop)
                trades = {}
                try:
                    trades = fut.result(timeout=5)
                except Exception:
                    pass
                if not trades:
                    send_telegram("No managed trades.")
                else:
                    send_telegram("Open Trades:")
                    for trade_id, v in trades.items():
                        unreal = v.get('unreal')
                        unreal_str = "N/A" if unreal is None else f"{float(unreal):.6f}"
                        
                        text = (f"📈 *{v['symbol']}* `{v['side']}`\n"
                                f"   - **Qty:** `{v['qty']}`\n"
                                f"   - **Entry:** `{v['entry_price']:.4f}`\n"
                                f"   - **SL/TP:** `{v['sl']:.4f}` / `{v['tp']:.4f}`\n"
                                f"   - **PnL:** `{unreal_str} USDT`\n"
                                f"   - **ID:** `{trade_id}`")

                        keyboard = InlineKeyboardMarkup([
                            [
                                InlineKeyboardButton("Close 50%", callback_data=f"close_50_{trade_id}"),
                                InlineKeyboardButton("Close 100%", callback_data=f"close_100_{trade_id}")
                            ]
                        ])
                        
                        try:
                            telegram_bot.send_message(
                                chat_id=int(TELEGRAM_CHAT_ID),
                                text=text,
                                reply_markup=keyboard,
                                parse_mode='Markdown'
                            )
                        except Exception as e:
                            log.error(f"Failed to send /listorders message for {trade_id}: {e}")

            elif text.startswith("/listpending"):
                fut = asyncio.run_coroutine_threadsafe(get_pending_orders_snapshot(), loop)
                pending_orders = {}
                try:
                    pending_orders = fut.result(timeout=5)
                except Exception as e:
                    log.error("Failed to get pending orders for /listpending: %s", e)
                
                if not pending_orders:
                    send_telegram("No pending limit orders.")
                else:
                    send_telegram("Pending Limit Orders:")
                    for p_id, p_meta in pending_orders.items():
                        placed_time_dt = datetime.fromisoformat(p_meta['place_time'])
                        age = format_timedelta(datetime.utcnow() - placed_time_dt)
                        text = (f"⏳ *{p_meta['symbol']}* `{p_meta['side']}`\n"
                                f"   - **Qty:** `{p_meta['qty']}`\n"
                                f"   - **Price:** `{p_meta['limit_price']:.4f}`\n"
                                f"   - **Age:** `{age}`\n"
                                f"   - **ID:** `{p_id}`")
                        
                        send_telegram(text, parse_mode='Markdown')

            elif text.startswith("/sessions"):
                send_telegram("Checking session status...")
                now_utc = datetime.now(timezone.utc)
                merged_intervals = get_merged_freeze_intervals()
                
                in_freeze = False
                for start, end, name in merged_intervals:
                    if start <= now_utc < end:
                        time_left = end - now_utc
                        send_telegram(f"❄️ Bot is FROZEN for {name}.\n\nTime until unfreeze: {format_timedelta(time_left)}")
                        in_freeze = True
                        break
                
                if not in_freeze:
                    if merged_intervals:
                        next_start, _, next_name = merged_intervals[0]
                        time_to_next = next_start - now_utc
                        send_telegram(f"✅ Bot is ACTIVE.\n\nNext freeze for {next_name} in: {format_timedelta(time_to_next)}")
                    else:
                        send_telegram("✅ Bot is ACTIVE.\n\nNo session freezes are scheduled in the next 48 hours.")
            
            elif text.startswith("/showparams"):
                param_list = [f" - `{k}` = `{v}`" for k, v in CONFIG.items()]
                out = "⚙️ *Current Bot Parameters*\n\n" + "\n".join(param_list)
                send_telegram(out, parse_mode='Markdown')
            elif text.startswith("/setparam"):
                parts = text.split()
                if len(parts) >= 3:
                    key = parts[1]
                    val = " ".join(parts[2:])
                    if key not in CONFIG:
                        send_telegram(f"Parameter {key} not found.")
                    else:
                        old = CONFIG[key]
                        try:
                            if isinstance(old, bool):
                                CONFIG[key] = val.lower() in ("1","true","yes","on")
                            elif isinstance(old, int):
                                CONFIG[key] = int(val)
                            elif isinstance(old, float):
                                CONFIG[key] = float(val)
                            elif isinstance(old, list):
                                CONFIG[key] = [x.strip().upper() for x in val.split(",")]
                            else:
                                CONFIG[key] = val
                            send_telegram(f"Set {key} = {CONFIG[key]}")
                        except Exception as e:
                            send_telegram(f"Failed to set {key}: {e}")
                else:
                    send_telegram("Usage: /setparam KEY VALUE")
            elif text.startswith("/validate"):
                result = validate_and_sanity_check_sync(send_report=False)
                send_telegram("Validation result: " + ("OK" if result["ok"] else "ERROR"))
                for c in result["checks"]:
                    send_telegram(f"{c['type']}: ok={c['ok']} detail={c.get('detail')}")
            elif text.startswith("/report"):
                # Handler for the /report command to generate and send the PnL report
                send_telegram("Generating performance report, please wait...")
                log.info("Scheduling /report task.")
                fut = asyncio.run_coroutine_threadsafe(generate_and_send_report(), loop)
                try:
                    log.info("Waiting for /report task future...")
                    fut.result(timeout=60) # Give it a long timeout for report generation
                    log.info("/report task finished.")
                except Exception as e:
                    log.error("Failed to execute /report action: %s", e, exc_info=True)
                    send_telegram(f"Failed to generate report: {e}")
            elif text.startswith("/stratreport"):
                send_telegram("Generating strategy performance report, please wait...")
                fut = asyncio.run_coroutine_threadsafe(generate_and_send_strategy_report(), loop)
                try:
                    fut.result(timeout=60)
                except Exception as e:
                    log.error("Failed to execute /stratreport action: %s", e)
                    send_telegram(f"Failed to generate strategy report: {e}")
            elif text.startswith("/chart"):
                parts = text.split()
                if len(parts) < 2:
                    send_telegram("Usage: /chart <SYMBOL>")
                else:
                    symbol = parts[1].upper()
                    send_telegram(f"Generating chart for {symbol}, please wait...")
                    
                    async def _task():
                        title, chart_bytes = await asyncio.to_thread(generate_adv_chart_sync, symbol)
                        await asyncio.to_thread(
                            send_telegram,
                            msg=title,
                            document_content=chart_bytes,
                            document_name=f"{symbol}_chart.png"
                        )
                    
                    fut = asyncio.run_coroutine_threadsafe(_task(), loop)
                    try:
                        fut.result(timeout=60)
                    except Exception as e:
                        log.error(f"Failed to execute /chart action for {symbol}: {e}")
                        send_telegram(f"Failed to generate chart for {symbol}: {e}")
            elif text.startswith("/reject"):
                # Note: This is a synchronous call within an async context
                handle_reject_cmd()
            elif text.startswith("/testreject"):
                log.info("Manual rejection test triggered.")
                _record_rejection(
                    symbol="TEST/USDT",
                    reason="MANUAL_TEST",
                    details={"price": "12345", "note": "This is a test entry"},
                )
                send_telegram("✅ Dummy rejection has been recorded. Use /reject to view it.")
            elif text.startswith("/trail"):
                parts = text.split()
                if len(parts) != 3:
                    send_telegram("Usage: /trail <trade_id> <on|off>")
                else:
                    trade_id, state = parts[1], parts[2].lower()
                    if state not in ['on', 'off']:
                        send_telegram("Invalid state. Use 'on' or 'off'.")
                    else:
                        new_trailing_state = (state == 'on')
                        
                        async def _task():
                            trade_to_update = None
                            # --- Lock, update in-memory state, and get a copy ---
                            async with managed_trades_lock:
                                if trade_id in managed_trades:
                                    managed_trades[trade_id]['trailing'] = new_trailing_state
                                    trade_to_update = managed_trades[trade_id].copy()
                            
                            # --- Perform slow I/O (DB, Telegram) outside the lock ---
                            if trade_to_update:
                                await asyncio.to_thread(add_managed_trade_to_db, trade_to_update)
                                status_msg = "ENABLED" if new_trailing_state else "DISABLED"
                                msg = f"✅ Trailing stop for trade `{trade_id}` has been manually {status_msg}."
                                await asyncio.to_thread(send_telegram, msg, parse_mode='Markdown')
                            else:
                                await asyncio.to_thread(send_telegram, f"❌ Trade with ID `{trade_id}` not found.", parse_mode='Markdown')

                        fut = asyncio.run_coroutine_threadsafe(_task(), loop)
                        try:
                            fut.result(timeout=10)
                        except Exception as e:
                            log.error(f"Failed to execute /trail command: {e}")
                            send_telegram(f"An error occurred while processing the /trail command: {e}")

            elif text.startswith("/sessionfreeze"):
                parts = text.split()
                if len(parts) != 2 or parts[1].lower() not in ['on', 'off']:
                    send_telegram("Usage: /sessionfreeze <on|off>")
                else:
                    new_state = parts[1].lower() == 'on'
                    CONFIG["SESSION_FREEZE_ENABLED"] = new_state
                    status_msg = "ENABLED" if new_state else "DISABLED"
                    send_telegram(f"✅ Automatic session freeze feature has been {status_msg}.")

            elif text.startswith("/help"):
                help_text = (
                    "*KAMA Bot Commands*\n\n"
                    "*Trading Control*\n"
                    "- `/startbot`: Starts the bot (resumes scanning for trades).\n"
                    "- `/stopbot`: Stops the bot (pauses scanning for trades).\n"
                    "- `/freeze`: Manually freezes the bot, preventing all new trades.\n"
                    "- `/unfreeze`: Lifts a manual freeze and overrides any active session freeze.\n"
                    "- `/sessionfreeze <on|off>`: Enables or disables the automatic session freeze feature.\n"
                    "- `/forcetrade <S1-S4> <SYMBOL> <buy|sell>`: Forces an immediate market trade.\n"
                    "- `/scalein <trade_id> <risk_usd>`: Adds to an existing position by a risk amount.\n"
                    "- `/trail <trade_id> <on|off>`: Manually enable or disable the automatic trailing stop for a trade.\n\n"
                    "*Information & Reports*\n"
                    "- `/status`: Shows a detailed status of the bot.\n"
                    "- `/listorders`: Lists all currently open trades with details.\n"
                    "- `/listpending`: Lists all pending limit orders that have not been filled.\n"
                    "- `/sessions`: Reports the current session freeze status.\n"
                    "- `/reject`: Shows a report of the last 20 persisted rejected trade opportunities.\n"
                    "- `/report`: Generates an overall performance report.\n"
                    "- `/stratreport`: Generates a side-by-side strategy performance report.\n"
                    "- `/chart <SYMBOL>`: Generates a detailed chart for a symbol.\n\n"
                    "*Configuration*\n"
                    "- `/showparams`: Displays all configurable bot parameters.\n"
                    "- `<KEY> = <VALUE>`: Sets a parameter (e.g., `MAX_CONCURRENT_TRADES = 4`).\n\n"
                    "*Utilities*\n"
                    "- `/ip`: Shows the bot's public server IP address.\n"
                    "- `/usage`: Displays the current CPU and memory usage.\n"
                    "- `/validate`: Performs a sanity check on the configuration.\n"
                    "- `/help`: Displays this help message.\n\n"
                    "*Testing*\n"
                    "- `/simulate [STRAT] [SYM] [DAYS]`: Runs a simulation (all args optional).\n"
                    "- `/testorder <S1|S2|S3|S4> [SYMBOL]`: Places a non-executable test limit order.\n"
                    "- `/testrun`: Runs a full end-to-end test on the Binance testnet."
                )
                async def _task():
                    await asyncio.to_thread(send_telegram, help_text, parse_mode='Markdown')
                fut = asyncio.run_coroutine_threadsafe(_task(), loop)
                try:
                    fut.result(timeout=10)
                except Exception as e:
                    log.error("Failed to execute /help action: %s", e)
            elif text.startswith("/simulate"):
                parts = text.split()[1:] # Ignore the command itself
                
                # Defaults
                strategy = "ALL"
                symbol = "BTCUSDT"
                days = 1
                
                if parts:
                    # Check for strategy first
                    if parts[0].upper() in ["S1", "S2", "S3", "S4", "ALL"]:
                        strategy = parts[0].upper()
                        parts.pop(0)
                    
                    # Check for symbol next (must not be a number)
                    if parts and not parts[0].isdigit():
                        symbol = parts[0].upper()
                        parts.pop(0)
                        
                    # Anything left must be the days
                    if parts and parts[0].isdigit():
                        days = int(parts[0])

                send_telegram("Scheduling simulation task...")
                # Run the simulation in a coroutine
                async def _task():
                    await run_simulation(strategy, symbol, days)
                
                fut = asyncio.run_coroutine_threadsafe(_task(), loop)
                try:
                    # We don't wait for the result here to keep the bot responsive
                    pass
                except Exception as e:
                    log.error(f"Failed to schedule /simulate task: {e}")
            elif text.startswith("/usage"):
                cpu_usage = psutil.cpu_percent(interval=1)
                mem_data = get_memory_info()
                
                usage_report = (
                    f"🖥️ *System Resource Usage*\n\n"
                    f"  - *CPU Usage:* {cpu_usage:.1f}%\n"
                    f"  - *Memory Usage:* {mem_data['percent']:.2f}%"
                )
                if mem_data['is_container']:
                    usage_report += " `(Container-aware)`\n"
                else:
                    usage_report += "\n"

                usage_report += (
                    f"    - Total: {mem_data['total_gb']:.2f} GB\n"
                    f"    - Used: {mem_data['used_gb']:.2f} GB"
                )
                send_telegram(usage_report, parse_mode='Markdown')
            elif text.startswith("/testorder"):
                import random
                parts = text.split()
                if len(parts) < 2:
                    send_telegram("Usage: /testorder <S1|S2|S3|S4> [SYMBOL]\nExample: /testorder S1 BTCUSDT")
                    return

                strategy_id_str = parts[1].upper()
                if strategy_id_str not in ["S1", "S2", "S3", "S4"]:
                    send_telegram(f"Invalid strategy: {strategy_id_str}. Please use S1, S2, S3, or S4.")
                    return
                
                strategy_id = int(strategy_id_str[1:])

                symbol = None
                if len(parts) > 2:
                    symbol = parts[2].upper()
                    if symbol not in CONFIG["SYMBOLS"]:
                       send_telegram(f"Symbol {symbol} is not in the bot's symbol list.")
                       return
                else:
                    symbol = random.choice(CONFIG["SYMBOLS"])

                # The `full_test` parameter is no longer needed.
                full_test = False
                
                send_telegram(f"🚀 Initiating simple test order for Strategy {strategy_id} on {symbol}...")
                
                async def _task():
                    await run_test_order(strategy_id, symbol, full_test)

                fut = asyncio.run_coroutine_threadsafe(_task(), loop)
                try:
                    fut.result(timeout=60)
                except Exception as e:
                    log.error(f"Failed to execute test order for S{strategy_id} on {symbol}: {e}")
                    send_telegram(f"❌ Failed to execute test order: {e}")
            elif text.startswith("/forcetrade"):
                parts = text.split()
                if len(parts) != 4:
                    send_telegram("Usage: /forcetrade <S1|S2|S3|S4> <SYMBOL> <buy|sell>")
                    return

                strategy_id_str = parts[1].upper()
                symbol = parts[2].upper()
                side_str = parts[3].lower()

                if strategy_id_str not in ["S1", "S2", "S3", "S4"]:
                    send_telegram(f"Invalid strategy: {strategy_id_str}. Please use S1, S2, S3, or S4.")
                    return
                
                strategy_id = int(strategy_id_str[1:])
                
                if symbol not in CONFIG["SYMBOLS"]:
                   send_telegram(f"Symbol {symbol} is not in the bot's symbol list. Add it via config if you want to trade it.")
                   return

                if side_str not in ['buy', 'sell']:
                    send_telegram(f"Invalid side: `{side_str}`. Must be 'buy' or 'sell'.", parse_mode='Markdown')
                    return
                
                side = 'BUY' if side_str == 'buy' else 'SELL'
                
                send_telegram(f"🚀 Initiating force trade for S{strategy_id} on {symbol} ({side})...")
                
                async def _task():
                    await force_trade_entry(strategy_id, symbol, side)

                fut = asyncio.run_coroutine_threadsafe(_task(), loop)
                try:
                    fut.result(timeout=60) # Give it a long timeout for the trade execution
                except Exception as e:
                    log.error(f"Failed to execute force trade for S{strategy_id} on {symbol}: {e}")
                    send_telegram(f"❌ Failed to execute force trade: {e}")
            elif text.startswith("/scalein"):
                parts = text.split()
                if len(parts) < 3:
                    send_telegram("Usage: /scalein <trade_id> <risk_usd_to_add>")
                else:
                    trade_id, risk_to_add_str = parts[1], parts[2]
                    try:
                        risk_to_add = float(risk_to_add_str)
                        
                        async def _task():
                            # --- Get trade data without holding lock for long ---
                            trades = await get_managed_trades_snapshot()
                            if trade_id not in trades:
                                await asyncio.to_thread(send_telegram, f"Trade {trade_id} not found.")
                                return
                            
                            trade = trades[trade_id]
                            price_distance = abs(trade['entry_price'] - trade['sl'])
                            if price_distance <= 0:
                                await asyncio.to_thread(send_telegram, f"Cannot scale in, price distance is zero.")
                                return

                            qty_to_add = risk_to_add / price_distance
                            qty_to_add = await asyncio.to_thread(round_qty, trade['symbol'], qty_to_add)

                            if qty_to_add > 0:
                                # --- Perform slow I/O (API call) outside the lock ---
                                await asyncio.to_thread(open_market_position_sync, trade['symbol'], trade['side'], qty_to_add, trade['leverage'])
                                
                                trade_to_update = None
                                # --- Lock, update in-memory state, and get a copy ---
                                async with managed_trades_lock:
                                    if trade_id in managed_trades:
                                        managed_trades[trade_id]['qty'] += qty_to_add
                                        managed_trades[trade_id]['notional'] += qty_to_add * trade['entry_price'] # Approximate
                                        managed_trades[trade_id]['risk_usdt'] += risk_to_add
                                        trade_to_update = managed_trades[trade_id].copy()

                                # --- Perform slow I/O (DB, Telegram) outside the lock ---
                                if trade_to_update:
                                    await asyncio.to_thread(add_managed_trade_to_db, trade_to_update)
                                    await asyncio.to_thread(send_telegram, f"✅ Scaled in {trade_id} by {qty_to_add} {trade['symbol']}.")
                                else:
                                    # This case is unlikely but handled for safety
                                    await asyncio.to_thread(send_telegram, f"Could not update trade {trade_id} after scaling in. Please check status.")
                            else:
                                await asyncio.to_thread(send_telegram, "Calculated quantity to add is zero.")

                        fut = asyncio.run_coroutine_threadsafe(_task(), loop)
                        fut.result(timeout=30)
                    except ValueError:
                        send_telegram("Invalid risk amount.")
                    except Exception as e:
                        log.exception(f"Failed to scale in {trade_id}")
                        send_telegram(f"❌ Error scaling in {trade_id}: {e}")
            elif text.startswith("/testrun"):
                send_telegram("🚀 Initiating full test run on testnet...")
                
                async def _task():
                    await run_full_testnet_test()

                fut = asyncio.run_coroutine_threadsafe(_task(), loop)
                try:
                    fut.result(timeout=300) # Long timeout for the full test
                except Exception as e:
                    log.error(f"Failed to execute /testrun: {e}")
                    send_telegram(f"❌ Test run failed: {e}")
            else:
                send_telegram("Unknown command. Use /status to see the keyboard.")
    except Exception:
        log.exception("Error in handle_update_sync")

def telegram_polling_thread(loop):
    global telegram_bot
    if not telegram_bot:
        log.info("telegram thread not started: bot not configured")
        return
    offset = None
    while not monitor_stop_event.is_set():
        try:
            updates = telegram_bot.get_updates(offset=offset, timeout=20)
            for u in updates:
                offset = u.update_id + 1
                handle_update_sync(u, loop)
            time.sleep(0.2)
        except Exception as e:
            if "timed out" in str(e).lower():
                log.debug("Telegram get_updates timed out, retrying...")
                continue
            log.exception("Telegram polling thread error")
            try:
                ip = get_public_ip()
                send_telegram(f"Telegram polling error: {e}")
            except Exception:
                pass
            time.sleep(5)

async def _set_running(val: bool):
    global running, ip_whitelist_error
    running = val
    if val:
        log.info("Resetting ip_whitelist_error flag.")
        ip_whitelist_error = False

async def _freeze_command():
    global frozen, session_freeze_override
    frozen = True
    session_freeze_override = False # A manual freeze clears any override
    log.info("Manual freeze issued.")

async def _unfreeze_command():
    global frozen, session_freeze_override
    frozen = False
    session_freeze_override = True
    log.info("Manual unfreeze issued. Overriding current session freeze if active.")


async def run_full_testnet_test():
    """
    Runs a full, end-to-end integration test on the Binance testnet.
    This function is self-contained and uses its own testnet client.
    """
    # Use environment variables for testnet keys, with fallback to the old hardcoded keys
    TESTNET_API_KEY = os.getenv("TESTNET_API_KEY", "7fabcde36d70d00d01f9a3d21b38855450aec5c4348d2361fac5c6bd44afd872")
    TESTNET_API_SECRET = os.getenv("TESTNET_API_SECRET", "4c22d3644841e6912dd957a0dfbfd4a6475b7bb6bc3022261173de41f165949c")
    
    temp_client = None
    report_lines = ["*Binance Testnet End-to-End Test Report*"]
    test_symbol = "BTCUSDT"
    test_trade_id = None # Initialize to None
    
    try:
        # --- Step 1: Initialize Testnet Client ---
        report_lines.append("\n*Step 1: Initialization*")
        await asyncio.to_thread(send_telegram, "1. Initializing testnet client...")
        
        # Use a temporary client for the test run, correctly enabling testnet mode
        temp_client = await asyncio.to_thread(
            Client, TESTNET_API_KEY, TESTNET_API_SECRET, testnet=True
        )
        report_lines.append("✅ Testnet client initialized.")

        # Set Hedge Mode
        try:
            await asyncio.to_thread(temp_client.futures_change_position_mode, dualSidePosition=True)
            report_lines.append("✅ Hedge mode enabled successfully.")
            log.info("Testnet client set to Hedge Mode.")
        except BinanceAPIException as e:
            # Error code for "No need to change position side"
            if e.code == -4059:
                report_lines.append("ℹ️ Hedge mode was already enabled.")
                log.info("Testnet client was already in Hedge Mode.")
            else:
                raise # Re-raise other exceptions

        # --- Step 2: Sanity Checks ---
        report_lines.append("\n*Step 2: Sanity Checks*")
        await asyncio.to_thread(send_telegram, "2. Pinging testnet server...")
        await asyncio.to_thread(temp_client.ping)
        report_lines.append("✅ Ping successful.")

        await asyncio.to_thread(send_telegram, f"Fetching 1m klines for {test_symbol}...")
        klines = await asyncio.to_thread(temp_client.futures_klines, symbol=test_symbol, interval='1m', limit=100)
        if not klines:
            raise RuntimeError("Failed to fetch klines from testnet.")
        report_lines.append(f"✅ Fetched {len(klines)} klines successfully.")

        # --- Step 3: Open Position ---
        report_lines.append("\n*Step 3: Open Position*")
        await asyncio.to_thread(send_telegram, "3. Placing a small market BUY order...")
        qty_to_open = 0.001
        
        # Manually construct and send the order using the temporary client
        market_order = await asyncio.to_thread(
            temp_client.futures_create_order,
            symbol=test_symbol, side='BUY', type='MARKET', quantity=qty_to_open, positionSide='LONG'
        )
        report_lines.append(f"✅ Market order placed. Order ID: `{market_order['orderId']}`")
        
        await asyncio.sleep(2)

        # --- Step 4: Verify Position & Create Mock Trade ---
        report_lines.append("\n*Step 4: Verify Position*")
        await asyncio.to_thread(send_telegram, "4. Verifying open position...")
        positions = await asyncio.to_thread(temp_client.futures_position_information, symbol=test_symbol)
        pos = next((p for p in positions if p.get('symbol') == test_symbol and float(p.get('positionAmt', 0)) != 0), None)
        
        if not pos or abs(float(pos.get('positionAmt', 0))) < qty_to_open:
            raise RuntimeError(f"Position for {test_symbol} not found or quantity mismatch after opening.")
        
        entry_price = float(pos['entryPrice'])
        report_lines.append(f"✅ Position confirmed. Entry Price: `{entry_price}`")

        # Create the mock trade object immediately after position confirmation
        # This ensures test_trade_id is set before operations that might fail (like placing SL/TP)
        sl_price = entry_price * 0.98
        tp_price = entry_price * 1.02
        test_trade_id = f"test_{int(time.time())}"
        mock_trade = {
            "id": test_trade_id, "symbol": test_symbol, "side": "BUY", "entry_price": entry_price,
            "initial_qty": qty_to_open, "qty": qty_to_open, "notional": qty_to_open * entry_price,
            "leverage": 10, "sl": sl_price, "tp": tp_price, "open_time": datetime.utcnow().isoformat(),
            "sltp_orders": {}, "trailing_active": True, "be_moved": True, 'strategy_id': '1'
        }
        
        # Add the mock trade to be managed. The monitor thread can now see it.
        async with managed_trades_lock:
            managed_trades[test_trade_id] = mock_trade
        report_lines.append(f"✅ Mock trade `{test_trade_id}` created and is being monitored.")

        # --- Step 5: Place SL/TP ---
        report_lines.append("\n*Step 5: Place SL/TP*")
        await asyncio.to_thread(send_telegram, "5. Placing SL/TP orders...")
        
        # --- Hedge Mode Aware SL/TP ---
        position_mode = await asyncio.to_thread(temp_client.futures_get_position_mode)
        log.info(f"Testnet position mode response: {position_mode}")
        is_hedge_mode = position_mode.get('dualSidePosition', False)
        log.info(f"Testnet is_hedge_mode determined as: {is_hedge_mode}")
        
        sl_order = {'symbol': test_symbol, 'side': 'SELL', 'type': 'STOP_MARKET', 'quantity': str(qty_to_open), 'stopPrice': round_price(test_symbol, sl_price)}
        tp_order = {'symbol': test_symbol, 'side': 'SELL', 'type': 'TAKE_PROFIT_MARKET', 'quantity': str(qty_to_open), 'stopPrice': round_price(test_symbol, tp_price)}
        
        if is_hedge_mode:
            sl_order['positionSide'] = 'LONG'
            tp_order['positionSide'] = 'LONG'
        else:
            sl_order['reduceOnly'] = True
            tp_order['reduceOnly'] = True

        sl_tp_batch = [sl_order, tp_order]
        # --- End Hedge Mode Aware ---

        sl_tp_orders = await asyncio.to_thread(
            temp_client.futures_place_batch_order,
            batchOrders=sl_tp_batch
        )
        if any('code' in o for o in sl_tp_orders):
            raise RuntimeError(f"Failed to place SL/TP orders: {sl_tp_orders}. Batch sent: {sl_tp_batch}")
        
        # If successful, update the managed trade with the real order IDs
        async with managed_trades_lock:
            if test_trade_id in managed_trades:
                managed_trades[test_trade_id]['sltp_orders'] = sl_tp_orders
        report_lines.append("✅ SL/TP orders placed successfully.")

        # --- Step 6: Trailing Stop Check ---
        report_lines.append("\n*Step 6: Trailing Stop Check*")
        await asyncio.to_thread(send_telegram, "6. Waiting 2 minutes to observe trailing stop... (This will depend on market movement)")
            
        await asyncio.sleep(120)

        async with managed_trades_lock:
            final_trade_state = managed_trades.get(test_trade_id, mock_trade)
        
        final_sl = final_trade_state.get('sl', sl_price)
        if final_sl > sl_price:
            report_lines.append(f"✅ Trailing Stop Moved! Initial: `{sl_price:.4f}`, Final: `{final_sl:.4f}`")
        else:
            report_lines.append(f"ℹ️ Trailing stop did not move. Initial: `{sl_price:.4f}`, Final: `{final_sl:.4f}`")

    except Exception as e:
        log.exception("Test run failed.")
        error_msg = f"❌ Test run failed: {e}"
        report_lines.append(error_msg)
        
    finally:
        # --- Step 7: Cleanup ---
        report_lines.append("\n*Step 7: Cleanup*")
        await asyncio.to_thread(send_telegram, "7. Cleaning up test orders and position...")
        
        if test_trade_id:
            async with managed_trades_lock:
                managed_trades.pop(test_trade_id, None)

        if temp_client:
            try:
                await asyncio.to_thread(temp_client.futures_cancel_all_open_orders, symbol=test_symbol)
                report_lines.append("✅ Canceled open orders on testnet.")
                
                positions = await asyncio.to_thread(temp_client.futures_position_information, symbol=test_symbol)
                pos = next((p for p in positions if p.get('symbol') == test_symbol and float(p.get('positionAmt', 0)) != 0), None)
                if pos:
                    close_qty = abs(float(pos['positionAmt']))
                    await asyncio.to_thread(
                        temp_client.futures_create_order,
                        symbol=test_symbol, side='SELL', type='MARKET', quantity=close_qty, positionSide='LONG'
                    )
                    report_lines.append("✅ Closed open position on testnet.")
                else:
                    report_lines.append("✅ No open position to close.")

            except Exception as e:
                log.exception("Test cleanup failed.")
                report_lines.append(f"❌ Cleanup failed: {e}")

        await asyncio.to_thread(send_telegram, "\n".join(report_lines), parse_mode='Markdown')

async def handle_critical_error_async(exc: Exception, context: str = None):
    global running
    running = False
    ip = await asyncio.to_thread(get_public_ip)
    tb = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)) if exc else "No traceback"
    safe_tb = _shorten_for_telegram(tb)
    msg = f"CRITICAL ERROR: {context or ''}\nException: {str(exc)}\n\nTraceback:\n{safe_tb}\nServer IP: {ip}\nBot paused."
    await asyncio.to_thread(send_telegram, msg)

@app.get("/")
async def root():
    return {"status": "ok", "running": running, "managed_trades": len(managed_trades)}

if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        log.info("Running in standalone mode. Initializing...")
        main_loop = loop
        init_db()
        ok, err = init_binance_client_sync()
        validate_and_sanity_check_sync(True)
        if ok:
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
        else:
            log.warning("Binance client not initialized, monitor threads not started.")
        if telegram_bot:
            telegram_thread = threading.Thread(target=telegram_polling_thread, args=(loop,), daemon=True)
            telegram_thread.start()
            log.info("Started telegram polling thread.")
        scan_task = None
        if ok:
            scan_task = loop.create_task(scanning_loop())
            log.info("Scanning loop scheduled.")
        else:
            log.warning("Binance client not initialized, scanning loop not started.")
        loop.run_until_complete(asyncio.to_thread(send_telegram, "KAMA strategy bot started (standalone). Running={}".format(running)))
        loop.run_forever()
    except KeyboardInterrupt:
        log.info("Keyboard interrupt received. Shutting down.")
    finally:
        log.info("Exiting.")
        monitor_stop_event.set()
        if scan_task:
            scan_task.cancel()
        
        async def gather_tasks():
            tasks = [t for t in asyncio.all_tasks(loop=loop) if t is not asyncio.current_task(loop=loop)]
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

        loop.run_until_complete(gather_tasks())
        if monitor_thread_obj and monitor_thread_obj.is_alive():
            monitor_thread_obj.join(timeout=2)
        if pnl_monitor_thread_obj and pnl_monitor_thread_obj.is_alive():
            pnl_monitor_thread_obj.join(timeout=2)
        loop.close()
