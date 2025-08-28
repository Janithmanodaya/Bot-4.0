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
import logging
import json
import signal
import sqlite3
import io
import re
import traceback
import psutil
from contextlib import asynccontextmanager
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, Optional
from collections import deque
from decimal import Decimal, ROUND_DOWN, getcontext

import requests
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use('Agg') # Use non-interactive backend for server-side plotting
import matplotlib.pyplot as plt
from fastapi import FastAPI

from binance.client import Client
from binance.exceptions import BinanceAPIException

import telegram
from telegram import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardButton, InlineKeyboardMarkup

import mplfinance as mpf

import firebase_admin
from firebase_admin import credentials, db

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
telegram_bot: Optional[telegram.Bot] = telegram.Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
main_loop: Optional[asyncio.AbstractEventLoop] = None
firebase_db = None

# -------------------------
# CONFIG (edit values here)
# -------------------------
CONFIG = {
    # --- STRATEGY ---
    "SMA_LEN": int(os.getenv("SMA_LEN", "200")),
    "BB_LENGTH_CUSTOM": int(os.getenv("BB_LENGTH_CUSTOM", "20")),
    "BB_STD_CUSTOM": float(os.getenv("BB_STD_CUSTOM", "2.5")),
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
    "BE_TRIGGER_PROFIT_PCT": float(os.getenv("BE_TRIGGER_PROFIT_PCT", "0.006")),
    "BE_SL_PROFIT_PCT": float(os.getenv("BE_SL_PROFIT_PCT", "0.001")),
    
    # --- CORE ---
    "SYMBOLS": os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT").split(","),
    "TIMEFRAME": os.getenv("TIMEFRAME", "15m"),
    "SCAN_INTERVAL": int(os.getenv("SCAN_INTERVAL", "20")),
    "SCAN_COOLDOWN_MINUTES": int(os.getenv("SCAN_COOLDOWN_MINUTES", "5")),
    "MAX_CONCURRENT_TRADES": int(os.getenv("MAX_CONCURRENT_TRADES", "3")),
    "START_MODE": os.getenv("START_MODE", "running").lower(),

    # --- INDICATOR SETTINGS ---
    "BB_LENGTH_CUSTOM": int(os.getenv("BB_LENGTH_CUSTOM", "20")),
    "BB_STD_CUSTOM": float(os.getenv("BB_STD_CUSTOM", "2.5")),
    "ATR_LENGTH": int(os.getenv("ATR_LENGTH", "14")),
    "SL_TP_ATR_MULT": float(os.getenv("SL_TP_ATR_MULT", "2.5")),

    "RISK_SMALL_BALANCE_THRESHOLD": float(os.getenv("RISK_SMALL_BALANCE_THRESHOLD", "50.0")),
    "RISK_SMALL_FIXED_USDT": float(os.getenv("RISK_SMALL_FIXED_USDT", "0.5")),
    "MARGIN_USDT_SMALL_BALANCE": float(os.getenv("MARGIN_USDT_SMALL_BALANCE", "2.0")),
    "RISK_PCT_LARGE": float(os.getenv("RISK_PCT_LARGE", "0.02")),
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
    "HEDGING_ENABLED": os.getenv("HEDGING_ENABLED", "false").lower() in ("true", "1", "yes"),
    "MONITOR_LOOP_THRESHOLD_SEC": int(os.getenv("MONITOR_LOOP_THRESHOLD_SEC", "10")),
    "FIREBASE_DATABASE_URL": os.getenv("FIREBASE_DATABASE_URL", "https://techno-a3e6c-default-rtdb.firebaseio.com/"),
}

running = (CONFIG["START_MODE"] == "running")
overload_notified = False
frozen = False
daily_loss_limit_hit = False
daily_profit_limit_hit = False
current_daily_pnl = 0.0

# Session freeze state
session_freeze_active = False
session_freeze_override = False
notified_frozen_session: Optional[str] = None

rejected_trades = deque(maxlen=5)
symbol_loss_cooldown: Dict[str, datetime] = {}

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

last_trade_close_time: Dict[str, datetime] = {}

telegram_thread: Optional[threading.Thread] = None
monitor_thread_obj: Optional[threading.Thread] = None
pnl_monitor_thread_obj: Optional[threading.Thread] = None
maintenance_thread_obj: Optional[threading.Thread] = None
monitor_stop_event = threading.Event()

last_maintenance_month = "" # YYYY-MM format

scan_task: Optional[asyncio.Task] = None
rogue_check_task: Optional[asyncio.Task] = None
notified_rogue_symbols: set[str] = set()

# Exchange info cache
EXCHANGE_INFO_CACHE = {"ts": 0.0, "data": None, "ttl": 300}  # ttl seconds

async def reconcile_open_trades():
    global managed_trades
    log.info("--- Starting Trade Reconciliation (with Firebase data) ---")

    fb_trades = {}
    async with managed_trades_lock:
        fb_trades = dict(managed_trades)
    
    log.info(f"Found {len(fb_trades)} managed trade(s) in Firebase to reconcile.")

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
    for trade_id, trade_meta in fb_trades.items():
        symbol = trade_meta['symbol']
        if symbol in open_positions:
            log.info(f"✅ Reconciled DB trade: {trade_id} ({symbol}) is active. Restoring.")
            retained_trades[trade_id] = trade_meta
        else:
            log.warning(f"ℹ️ Reconciled DB trade: {trade_id} ({symbol}) is closed on Binance. Archiving.")
            # This part could be enhanced to fetch last trade details for accurate PnL
            record_trade({
                'id': trade_id, 'symbol': symbol, 'side': trade_meta['side'],
                'entry_price': trade_meta['entry_price'], 'exit_price': None, # Exit price is unknown
                'qty': trade_meta['initial_qty'], 'notional': trade_meta['notional'], 
                'pnl': 0.0, 'open_time': trade_meta['open_time'], 
                'close_time': datetime.utcnow().isoformat(),
                'risk_usdt': trade_meta.get('risk_usdt', 0.0)
            })
            await asyncio.to_thread(remove_managed_trade_from_db, trade_id)

    # 2. Import "rogue" positions that are on the exchange but not in the DB
    managed_symbols = {t['symbol'] for t in retained_trades.values()}
    for symbol, position in open_positions.items():
        if symbol not in managed_symbols:
            log.info(f"❗️ Rogue position for {symbol} detected. Importing for management...")
            
            try:
                # Get position details from Binance
                entry_price = float(position['entryPrice'])
                qty = abs(float(position['positionAmt']))
                side = 'BUY' if float(position['positionAmt']) > 0 else 'SELL'
                leverage = int(position.get('leverage', CONFIG.get("MAX_BOT_LEVERAGE", 20)))
                notional = qty * entry_price

                # Calculate a default SL/TP based on current ATR
                df = await asyncio.to_thread(fetch_klines_sync, symbol, CONFIG["TIMEFRAME"], 200)
                atr_now = atr(df, CONFIG["ATR_LENGTH"]).iloc[-1]
                sl_distance = CONFIG["SL_TP_ATR_MULT"] * atr_now
                
                stop_price = entry_price - sl_distance if side == 'BUY' else entry_price + sl_distance
                take_price = entry_price + sl_distance if side == 'BUY' else entry_price - sl_distance

                # Create a new trade record
                trade_id = f"{symbol}_imported_{int(time.time())}"
                meta = {
                    "id": trade_id, "symbol": symbol, "side": side, "entry_price": entry_price,
                    "initial_qty": qty, "qty": qty, "notional": notional, "leverage": leverage,
                    "sl": stop_price, "tp": take_price, "open_time": datetime.utcnow().isoformat(),
                    "sltp_orders": {}, "trailing": CONFIG["TRAILING_ENABLED"],
                    "dyn_sltp": False, "tp1": None, "tp2": None, "tp3": None,
                    "trade_phase": 0, "be_moved": False, "risk_usdt": 0.0 # Risk is unknown for imported trades
                }

                # Add to managed trades and save to DB
                retained_trades[trade_id] = meta
                await asyncio.to_thread(add_managed_trade_to_db, meta) # Keep as backup
                if firebase_db:
                    try:
                        await asyncio.to_thread(firebase_db.child("managed_trades").child(trade_id).set, meta)
                    except Exception as e:
                        log.exception(f"Failed to save imported trade {trade_id} to Firebase: {e}")

                # Cancel any existing SL/TP orders for this symbol before placing new ones
                await asyncio.to_thread(cancel_close_orders_sync, symbol)
                
                # Place the new SL/TP orders
                log.info(f"Attempting to place SL/TP for imported trade {symbol}. SL={stop_price}, TP={take_price}, Qty={qty}")
                await asyncio.to_thread(place_batch_sl_tp_sync, symbol, side, sl_price=stop_price, tp_price=take_price, qty=qty)
                
                msg = (f"ℹ️ **Position Imported**\n\n"
                       f"Found and imported a position for **{symbol}**.\n\n"
                       f"**Side:** {side}\n"
                       f"**Entry Price:** {entry_price}\n"
                       f"**Quantity:** {qty}\n\n"
                       f"A default SL/TP has been calculated and placed based on current market volatility:\n"
                       f"**SL:** `{round_price(symbol, stop_price)}`\n"
                       f"**TP:** `{round_price(symbol, take_price)}`\n\n"
                       f"The bot will now manage this trade.")
                await asyncio.to_thread(send_telegram, msg)

            except Exception as e:
                await asyncio.to_thread(log_and_send_error, f"Failed to import rogue position for {symbol}. Please manage it manually.", e)

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

            log.info(f"❗️ New rogue position for {symbol} detected. Attempting to import...")
            # Mark as notified BEFORE attempting import to prevent spam on repeated failures.
            notified_rogue_symbols.add(symbol)
            position = open_positions[symbol]

            try:
                # This is the same import logic from reconcile_open_trades
                entry_price = float(position['entryPrice'])
                qty = abs(float(position['positionAmt']))
                side = 'BUY' if float(position['positionAmt']) > 0 else 'SELL'
                leverage = int(position.get('leverage', CONFIG.get("MAX_BOT_LEVERAGE", 20)))
                notional = qty * entry_price

                df = await asyncio.to_thread(fetch_klines_sync, symbol, CONFIG["TIMEFRAME"], 200)
                atr_now = atr(df, CONFIG["ATR_LENGTH"]).iloc[-1]
                sl_distance = CONFIG["SL_TP_ATR_MULT"] * atr_now
                
                stop_price = entry_price - sl_distance if side == 'BUY' else entry_price + sl_distance
                take_price = entry_price + sl_distance if side == 'BUY' else entry_price - sl_distance

                trade_id = f"{symbol}_imported_{int(time.time())}"
                meta = {
                    "id": trade_id, "symbol": symbol, "side": side, "entry_price": entry_price,
                    "initial_qty": qty, "qty": qty, "notional": notional, "leverage": leverage,
                    "sl": stop_price, "tp": take_price, "open_time": datetime.utcnow().isoformat(),
                    "sltp_orders": {}, "trailing": CONFIG["TRAILING_ENABLED"],
                    "dyn_sltp": False, "tp1": None, "tp2": None, "tp3": None,
                    "trade_phase": 0, "be_moved": False, "risk_usdt": 0.0
                }

                # Cancel any existing SL/TP orders for this symbol before placing new ones
                await asyncio.to_thread(cancel_close_orders_sync, symbol)
                
                # Place the new SL/TP orders
                await asyncio.to_thread(place_batch_sl_tp_sync, symbol, side, sl_price=stop_price, tp_price=take_price, qty=qty)
                
                # Add to managed trades and save to DB
                async with managed_trades_lock:
                    managed_trades[trade_id] = meta
                await asyncio.to_thread(add_managed_trade_to_db, meta) # Keep as backup
                if firebase_db:
                    try:
                        await asyncio.to_thread(firebase_db.child("managed_trades").child(trade_id).set, meta)
                    except Exception as e:
                        log.exception(f"Failed to save imported trade {trade_id} to Firebase: {e}")

                msg = (f"ℹ️ **Position Auto-Imported**\n\n"
                       f"Found and imported a rogue position for **{symbol}**.\n\n"
                       f"**Side:** {side}\n"
                       f"**Entry Price:** {entry_price}\n"
                       f"**Quantity:** {qty}\n\n"
                       f"A default SL/TP has been calculated and placed:\n"
                       f"**SL:** `{round_price(symbol, stop_price)}`\n"
                       f"**TP:** `{round_price(symbol, take_price)}`\n\n"
                       f"The bot will now manage this trade.")
                await asyncio.to_thread(send_telegram, msg)

            except Exception as e:
                await asyncio.to_thread(log_and_send_error, f"Failed to import rogue position for {symbol}. Please manage it manually.", e)
    
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


def init_firebase_sync():
    """
    Initializes the Firebase Admin SDK from an environment variable or a file.
    Returns (ok: bool, error_message: str)
    """
    global firebase_db
    try:
        # Check for credentials in environment variable first
        cred_json_str = os.getenv("FIREBASE_CREDENTIALS_JSON")
        if cred_json_str:
            log.info("Initializing Firebase from environment variable.")
            cred_dict = json.loads(cred_json_str)
            cred = credentials.Certificate(cred_dict)
        else:
            # Fallback to local file
            log.info("Initializing Firebase from firebase_credentials.json file.")
            if not os.path.exists("firebase_credentials.json"):
                log.error("firebase_credentials.json not found and FIREBASE_CREDENTIALS_JSON env var is not set.")
                send_telegram("🔥 Firebase Init Failed: Credentials not found.")
                return False, "Credentials not found"
            cred = credentials.Certificate("firebase_credentials.json")

        # Check if Firebase app is already initialized
        if not firebase_admin._apps:
            firebase_admin.initialize_app(cred, {
                'databaseURL': CONFIG['FIREBASE_DATABASE_URL']
            })
        else:
            log.warning("Firebase app already initialized. Skipping re-initialization.")

        firebase_db = db.reference()
        log.info("Firebase connection initialized successfully.")
        return True, ""
    except Exception as e:
        log.exception("Failed to initialize Firebase: %s", e)
        err = f"Firebase init error: {e}"
        send_telegram(f"🔥 Firebase Init Failed: {e}\nThe bot cannot persist state and may not function correctly.")
        return False, err


def load_state_from_firebase_sync():
    """
    Loads pending orders and managed trades from Firebase into memory on startup.
    Handles cases where the database paths do not exist.
    """
    global pending_limit_orders, managed_trades
    if not firebase_db:
        log.warning("Firebase not initialized, cannot load state from Firebase.")
        return

    log.info("--- Loading State from Firebase ---")
    try:
        # Load pending orders
        try:
            pending_data = firebase_db.child("pending_limit_orders").get()
            if pending_data:
                with pending_limit_orders_lock:
                    pending_limit_orders.update(pending_data)
                log.info(f"Loaded {len(pending_data)} pending order(s) from Firebase.")
            else:
                log.info("No pending orders found in Firebase.")
        except firebase_admin.exceptions.NotFoundError:
            log.info("'/pending_limit_orders' path not found in Firebase. Assuming no pending orders.")
            pass # Path doesn't exist, which is fine

        # Load managed trades
        try:
            trades_data = firebase_db.child("managed_trades").get()
            if trades_data:
                with managed_trades_lock:
                    managed_trades.update(trades_data)
                log.info(f"Loaded {len(trades_data)} managed trade(s) from Firebase.")
            else:
                log.info("No managed trades found in Firebase.")
        except firebase_admin.exceptions.NotFoundError:
            log.info("'/managed_trades' path not found in Firebase. Assuming no managed trades.")
            pass # Path doesn't exist, which is fine

    except Exception as e:
        # Catch any other unexpected errors during state loading
        log.exception("Failed to load state from Firebase: %s", e)
        send_telegram("⚠️ Failed to load state from Firebase on startup. The bot may not be aware of existing trades.")


# -------------------------
# App Lifespan Manager
# -------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global scan_task, telegram_thread, monitor_thread_obj, pnl_monitor_thread_obj, client, monitor_stop_event, main_loop
    log.info("EMA/BB Strategy Bot starting up...")
    
    # --- Startup Logic ---
    init_db()
    
    # New Firebase init
    await asyncio.to_thread(init_firebase_sync)
    await asyncio.to_thread(load_state_from_firebase_sync)

    main_loop = asyncio.get_running_loop()

    ok, err = await asyncio.to_thread(init_binance_client_sync)
    
    if ok:
        await reconcile_open_trades()

    await asyncio.to_thread(validate_and_sanity_check_sync, True)

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
        await send_telegram("EMA/BB Strategy Bot shut down.")
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
        error_details = f"Code: `{exc.code}`, Message: `{exc.message}`"
    elif exc:
        error_details = str(exc)
    else:
        error_details = "N/A"

    # Sanitize the error details for Telegram's Markdown
    error_details = error_details.replace('`', "'")

    # Format a user-friendly message
    telegram_msg = (
        f"🚨 **Bot Error** 🚨\n\n"
        f"**Context:** {context_msg}\n"
        f"**Error Type:** `{type(exc).__name__ if exc else 'N/A'}`\n"
        f"**Details:** {error_details}\n\n"
        f"Check the logs for the full traceback if available."
    )
    
    # Send the message, using Markdown for formatting
    send_telegram(telegram_msg, parse_mode='Markdown')


def _record_rejection(symbol: str, reason: str, details: dict):
    """Adds a rejected trade event to the deque."""
    global rejected_trades
    # Format floats in details to a reasonable precision for display
    formatted_details = {k: f"{v:.4f}" if isinstance(v, float) else v for k, v in details.items()}
    record = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "symbol": symbol,
        "reason": reason,
        "details": formatted_details
    }
    rejected_trades.append(record)
    # Use debug level for rejection logs to avoid spamming the main log
    log.debug(f"Rejected trade for {symbol}. Reason: {reason}")


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
        risk_usdt REAL NOT NULL
    )
    """)
    conn.commit()
    conn.close()

def record_trade(rec: Dict[str, Any]):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO trades (id,symbol,side,entry_price,exit_price,qty,notional,risk_usdt,pnl,open_time,close_time,entry_reason,exit_reason,tp1,tp2)
    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (rec['id'], rec['symbol'], rec['side'], rec['entry_price'], rec.get('exit_price'),
          rec['qty'], rec['notional'], rec.get('risk_usdt'), rec.get('pnl'), 
          rec['open_time'], rec.get('close_time'), rec.get('entry_reason'), rec.get('exit_reason'),
          rec.get('tp1'), rec.get('tp2')))
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
        rec.get('risk_usdt')
    )
    cur.execute("""
    INSERT OR REPLACE INTO managed_trades (
        id, symbol, side, entry_price, initial_qty, qty, notional,
        leverage, sl, tp, open_time, sltp_orders, trailing, dyn_sltp,
        tp1, tp2, tp3, trade_phase, be_moved, risk_usdt
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, values)
    conn.commit()
    conn.close()

def remove_managed_trade_from_db(trade_id: str):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    cur.execute("DELETE FROM managed_trades WHERE id = ?", (trade_id,))
    conn.commit()
    conn.close()

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

# --- New Strategy Indicators ---

def sma(series: pd.Series, length: int) -> pd.Series:
    """Calculates the Simple Moving Average (SMA)."""
    return series.rolling(window=length).mean()

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


def adx(df: pd.DataFrame, period: int = 14) -> pd.Series:
    """
    Calculates the Average Directional Index (ADX).
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
    plus_di = 100 * (plus_dm.ewm(alpha=alpha, adjust=False).mean() / atr_smooth)
    minus_di = 100 * (minus_dm.ewm(alpha=alpha, adjust=False).mean() / atr_smooth)
    
    # DX and ADX
    # To avoid division by zero, set denominator to 1 where it's zero
    dx_denominator = plus_di + minus_di
    dx_denominator[dx_denominator == 0] = 1
    dx = 100 * (abs(plus_di - minus_di) / dx_denominator)
    adx_series = dx.ewm(alpha=alpha, adjust=False).mean()
    
    return adx_series


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
        # Set a longer timeout for all requests to Binance
        requests_params = {"timeout": 60}
        client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params=requests_params)
        log.info("Binance client in MAINNET mode (forced).")
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

def round_qty(symbol: str, qty: float) -> float:
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
                steps = (q // step)
                quant = (steps * step).quantize(step, rounding=ROUND_DOWN)
                if quant <= 0:
                    return 0.0
                return float(quant)
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
                
                # Determine the number of decimal places from the tick_size
                decimal_places = abs(tick_size.as_tuple().exponent)

                getcontext().prec = 28
                p = Decimal(str(price))
                rounded_price = p.quantize(tick_size, rounding=ROUND_DOWN)
                
                # Format with the correct number of decimal places to preserve trailing zeros
                return f"{rounded_price:.{decimal_places}f}"
    except Exception:
        log.exception("round_price failed; falling back to basic formatting")
    return f"{price:.8f}"

def place_limit_order_sync(symbol: str, side: str, qty: float, price: float):
    """
    Places a single limit order. This is a blocking call.
    """
    global client, IS_HEDGE_MODE
    if CONFIG["DRY_RUN"]:
        log.info(f"[DRY RUN] Would place LIMIT {side} order for {qty} {symbol} at {price}.")
        dry_run_id = int(time.time())
        return {
            "orderId": f"dryrun_limit_{dry_run_id}", "symbol": symbol, "status": "NEW",
            "side": side, "type": "LIMIT", "origQty": str(qty), "price": str(price),
            "cumQuote": "0", "executedQty": "0", "avgPrice": "0.0"
        }

    if client is None:
        raise RuntimeError("Binance client not initialized")

    position_side = 'LONG' if side == 'BUY' else 'SHORT'

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

    try:
        # Enhanced logging for debugging price formatting issues
        log.info(f"Attempting to place limit order with params: {params}")
        order_response = client.futures_create_order(**params)
        log.info(f"Limit order placement successful for {symbol}. Response: {order_response}")
        return order_response
    except BinanceAPIException as e:
        log.exception("BinanceAPIException placing limit order: %s", e)
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

    position_side = 'LONG' if side == 'BUY' else 'SHORT'

    params = {
        'symbol': symbol,
        'side': side,
        'type': 'MARKET',
        'quantity': str(qty),
    }

    if IS_HEDGE_MODE:
        params['positionSide'] = position_side

    try:
        log.info(f"Placing market order for {symbol}: {params}")
        order_response = client.futures_create_order(**params)
        log.info(f"Market order placement successful for {symbol}. Response: {order_response}")
        return order_response
    except BinanceAPIException as e:
        log.exception("BinanceAPIException placing market order: %s", e)
        raise
    except Exception as e:
        log.exception("Exception placing market order: %s", e)
        raise

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
        close_order_params['reduceOnly'] = 'true'

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
                try:
                    time.sleep(1) # Give exchange time to register position
                    client.futures_create_order(
                        symbol=symbol,
                        side=close_side,
                        type='MARKET',
                        quantity=str(qty),
                        positionSide=position_side,
                        reduceOnly=True
                    )
                    log.info(f"Successfully closed naked position for {symbol}.")
                except Exception as close_e:
                    log.exception(f"CRITICAL: FAILED TO CLOSE NAKED POSITION for {symbol}. Manual intervention required. Error: {close_e}")
                    send_telegram(f"🚨 CRITICAL: FAILED TO CLOSE NAKED POSITION for {symbol}. Manual intervention required.")

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
        base_close_order['reduceOnly'] = 'true'

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
            # Raise an exception if any order in the batch failed
            raise RuntimeError(f"Batch SL/TP order placement failed for {symbol}. Errors: {errors}")
            
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
        log.info(f"Cancelling {len(order_ids_to_cancel)} specific orders for trade {trade_meta.get('id')} on {symbol}.")
        str_order_ids = [str(oid) for oid in order_ids_to_cancel]
        client.futures_cancel_batch_order(symbol=symbol, orderIdList=str_order_ids)
        
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

        log.info(f"Cancelling batch of {len(order_ids_to_cancel)} orders for {symbol}.")
        client.futures_cancel_batch_order(symbol=symbol, orderIdList=order_ids_to_cancel)
        
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

def calculate_risk_amount(account_balance: float) -> float:
    if account_balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"]:
        risk = CONFIG["RISK_SMALL_FIXED_USDT"]
    else:
        risk = account_balance * CONFIG["RISK_PCT_LARGE"]
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
            bbu_s, bbl_s = bollinger_bands(raw_df['close'], CONFIG["BB_LENGTH_CUSTOM"], CONFIG["BB_STD_CUSTOM"])
            
            results["checks"].append({"type": "indicators_sample", "ok": True, "detail": {
                "sma": float(sma_s.iloc[-1]), "rsi": float(rsi_s.iloc[-1]), 
                "bbu": float(bbu_s.iloc[-1]), "bbl": float(bbl_s.iloc[-1])
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
        df[c] = df[c].astype(float)
    df.set_index('close_time', inplace=True)
    return df[['open','high','low','close','volume']]

async def evaluate_and_enter(symbol: str):
    log.info("Evaluating symbol: %s", symbol)
    global managed_trades, running, frozen, symbol_loss_cooldown
    if frozen or not running:
        reason = "Bot is frozen or not running"
        _record_rejection(symbol, reason, {"running": running, "frozen": frozen})
        return

    # --- Cooldown Check ---
    if symbol in symbol_loss_cooldown:
        cooldown_end_time = symbol_loss_cooldown[symbol] + timedelta(hours=CONFIG['LOSS_COOLDOWN_HOURS'])
        if datetime.now(timezone.utc) < cooldown_end_time:
            log.info(f"Symbol {symbol} is in cooldown period until {cooldown_end_time}. Skipping evaluation.")
            _record_rejection(symbol, "In post-loss cooldown", {"cooldown_ends": cooldown_end_time.isoformat()})
            return
        else:
            # Cooldown has expired, remove it
            log.info(f"Cooldown for {symbol} has expired. Removing from list.")
            del symbol_loss_cooldown[symbol]

    try:
        # 1. Fetch klines and calculate indicators
        df = await asyncio.to_thread(fetch_klines_sync, symbol, CONFIG["TIMEFRAME"], 201) # Fetch 201 to have 200 for indicators and one last closed candle
        df['BBU'], df['BBL'] = bollinger_bands(df['close'], CONFIG["BB_LENGTH_CUSTOM"], CONFIG["BB_STD_CUSTOM"])
        df['atr'] = atr(df, CONFIG["ATR_LENGTH"])
        
        # Use the most recently closed candle for the signal, not the current open one.
        signal_candle = df.iloc[-2]

        # --- ADX Trend Filter ---
        if CONFIG.get("ADX_FILTER_ENABLED", True):
            df['adx'] = adx(df, period=CONFIG.get("ADX_PERIOD", 14))
            adx_value = df['adx'].iloc[-2]
            if not pd.isna(adx_value) and adx_value > CONFIG.get("ADX_THRESHOLD", 25.0):
                reason = "ADX > threshold (trending market)"
                details = {'adx': f"{adx_value:.2f}", 'threshold': CONFIG.get("ADX_THRESHOLD", 25.0)}
                _record_rejection(symbol, reason, details)
                return

        # --- Fast Move Filter ---
        if CONFIG.get("FAST_MOVE_FILTER_ENABLED", False):
            # 1. Candle Size Filter
            candle_size = signal_candle['high'] - signal_candle['low']
            atr_threshold = signal_candle['atr'] * CONFIG.get("FAST_MOVE_ATR_MULT", 2.0)
            if candle_size > atr_threshold:
                reason = "Candle size > ATR threshold"
                details = {'candle_size': f"{candle_size:.4f}", 'atr_threshold': f"{atr_threshold:.4f}"}
                _record_rejection(symbol, reason, details)
                return

            # 2. Volume Spike Filter
            df['avg_volume'] = df['volume'].rolling(window=CONFIG["BB_LENGTH_CUSTOM"]).mean()
            signal_candle_avg_vol = df['avg_volume'].iloc[-2]
            if signal_candle_avg_vol > 0: # Avoid division by zero if avg vol is 0
                vol_threshold = signal_candle_avg_vol * CONFIG.get("FAST_MOVE_VOL_MULT", 2.0)
                if signal_candle['volume'] > vol_threshold:
                    reason = "Volume spike detected"
                    details = {'volume': f"{signal_candle['volume']:.2f}", 'avg_volume': f"{signal_candle_avg_vol:.2f}"}
                    _record_rejection(symbol, reason, details)
                    return
            
            # 3. 1-Minute Return Filter
            try:
                df_1m = await asyncio.to_thread(fetch_klines_sync, symbol, '1m', limit=2)
                if len(df_1m) >= 2:
                    last_close = df_1m['close'].iloc[-1]
                    prev_close = df_1m['close'].iloc[-2]
                    if prev_close > 0: # Avoid division by zero
                        one_min_return = abs((last_close / prev_close) - 1)
                        return_threshold = CONFIG.get("FAST_MOVE_RETURN_PCT", 0.005)
                        if one_min_return > return_threshold:
                            reason = "1-minute return > threshold"
                            details = {'1m_return_pct': f"{one_min_return:.4%}", 'threshold_pct': f"{return_threshold:.4%}"}
                            _record_rejection(symbol, reason, details)
                            return
            except Exception as e:
                log.warning(f"Could not perform 1-minute return check for {symbol}: {e}")

        # 2. Check for entry signal based on Bollinger Bands
        side = None
        if signal_candle['close'] <= signal_candle['BBL']:
            side = 'BUY'
        elif signal_candle['close'] >= signal_candle['BBU']:
            side = 'SELL'
        
        if not side:
            _record_rejection(symbol, "No BB entry signal", {'price': f"{signal_candle['close']:.4f}", 'BBU': f"{signal_candle['BBU']:.4f}", 'BBL': f"{signal_candle['BBL']:.4f}"})
            return

        # 3. Pre-trade checks
        async with managed_trades_lock, pending_limit_orders_lock:
            if not CONFIG["HEDGING_ENABLED"] and any(t['symbol'] == symbol for t in managed_trades.values()):
                _record_rejection(symbol, "Trade exists and hedging is disabled", {})
                return
            if any(p['symbol'] == symbol for p in pending_limit_orders.values()):
                _record_rejection(symbol, "Pending limit order already exists", {})
                return
            if len(managed_trades) + len(pending_limit_orders) >= CONFIG["MAX_CONCURRENT_TRADES"]:
                _record_rejection(symbol, "Max concurrent trades reached", {'open_trades': len(managed_trades), 'pending_orders': len(pending_limit_orders)})
                return

        # 4. Calculate Limit Price using the close of the signal candle
        offset = CONFIG["ORDER_LIMIT_OFFSET_PCT"]
        if side == 'BUY':
            limit_price = signal_candle['close'] * (1 - offset)
        else: # SELL
            limit_price = signal_candle['close'] * (1 + offset)

        # 5. Calculate SL and TP
        atr_now = signal_candle['atr']
        if atr_now <= 0:
            _record_rejection(symbol, "Invalid ATR value", {'atr': atr_now})
            return

        sl_distance_atr = CONFIG["SL_TP_ATR_MULT"] * atr_now
        
        if side == 'BUY':
            stop_price = limit_price - sl_distance_atr
            take_price = limit_price + (2 * sl_distance_atr)
        else: # SELL
            stop_price = limit_price + sl_distance_atr
            take_price = limit_price - (2 * sl_distance_atr)

        # 6. Calculate Quantity (reusing existing robust logic)
        price_distance = abs(limit_price - stop_price)
        balance = await asyncio.to_thread(get_account_balance_usdt)
        risk_usdt = calculate_risk_amount(balance)
        qty = risk_usdt / price_distance
        
        notional = qty * limit_price
        min_notional = CONFIG["MIN_NOTIONAL_USDT"]
        if notional < min_notional:
            log.warning(f"Notional value for {symbol} is {notional:.2f}, below min {min_notional:.2f}. Attempting to boost risk.")
            required_qty = min_notional / limit_price
            new_risk = required_qty * price_distance
            
            if new_risk > balance:
                reason = "Notional value too small and cannot boost risk"
                _record_rejection(symbol, reason, {'notional': notional, 'required_risk': new_risk, 'balance': balance})
                await asyncio.to_thread(send_telegram, f"⚠️ Trade Skipped: {symbol}\nReason: {reason}\nNotional: {notional:.4f} USDT\nRequired Risk: {new_risk:.2f} USDT\nBalance: {balance:.2f} USDT")
                return

            risk_usdt = new_risk
            qty = required_qty
            notional = qty * limit_price
            await asyncio.to_thread(send_telegram, f"📈 Risk Boosted: {symbol}\nNew Risk: {risk_usdt:.2f} USDT\nNotional: {notional:.2f} USDT")

        qty_before_round = qty
        qty = await asyncio.to_thread(round_qty, symbol, qty)

        if qty <= 0:
            log.warning(f"Quantity for {symbol} rounded to zero. Initial: {qty_before_round}. Attempting to boost to min step size.")
            step_size_dec = await asyncio.to_thread(get_step_size, symbol)
            if step_size_dec:
                qty = float(step_size_dec)
                new_risk = qty * price_distance
                
                if new_risk > balance:
                     _record_rejection(symbol, "Qty too small, cannot boost risk", {'min_qty': qty, 'required_risk': new_risk, 'balance': balance})
                     return

                risk_usdt = new_risk
                notional = qty * limit_price
                log.info(f"Quantity for {symbol} boosted to {qty}, new risk is {risk_usdt:.2f} USDT.")
            else:
                _record_rejection(symbol, "Quantity rounded to zero", {'risk_usdt': risk_usdt, 'price_dist': price_distance})
                return

        # Leverage calculation
        if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"]:
            margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"]
        else:
            margin_to_use = risk_usdt
        margin_to_use = min(margin_to_use, notional)
        leverage = int(math.floor(notional / max(margin_to_use, 1e-9)))
        max_leverage_from_config = CONFIG.get("MAX_BOT_LEVERAGE", 30)
        max_leverage_from_exchange = get_max_leverage(symbol)
        leverage = max(1, min(leverage, max_leverage_from_config, max_leverage_from_exchange))
        
        # 7. Place Limit Order
        try:
            limit_order_resp = await asyncio.to_thread(
                place_limit_order_sync, symbol, side, qty, limit_price
            )
            
            order_id = str(limit_order_resp.get('orderId'))
            pending_order_id = f"{symbol}_{order_id}"

            # Calculate expiry time based on the number of candles configured
            candle_duration = timeframe_to_timedelta(CONFIG['TIMEFRAME'])
            expiry_candles = CONFIG.get("ORDER_EXPIRY_CANDLES", 2)
            # df.index[-1] is the close time of the current (incomplete) candle.
            # We subtract 1 from expiry_candles because the first candle's duration is already included.
            expiry_time = df.index[-1] + (candle_duration * (expiry_candles - 1))

            # Store all necessary info for when the order is filled
            pending_meta = {
                "id": pending_order_id,
                "order_id": order_id,
                "symbol": symbol,
                "side": side,
                "qty": qty,
                "limit_price": limit_price,
                "stop_price": stop_price,
                "take_price": take_price, # Store the calculated TP for later use
                "leverage": leverage,
                "risk_usdt": risk_usdt,
                "place_time": datetime.utcnow().isoformat(),
                "expiry_time": expiry_time.isoformat(),
            }
            
            async with pending_limit_orders_lock:
                pending_limit_orders[pending_order_id] = pending_meta
                if firebase_db:
                    try:
                        await asyncio.to_thread(
                            firebase_db.child("pending_limit_orders").child(pending_order_id).set,
                            pending_meta
                        )
                    except Exception as e:
                        log.exception(f"Failed to save pending order {pending_order_id} to Firebase: {e}")

            log.info(f"Placed pending limit order: {pending_meta}")
            await asyncio.to_thread(send_telegram, f"⏳ New Limit Order Placed for {symbol} | Side: {side}, Qty: {qty}, Price: {limit_price:.4f}")

        except Exception as e:
            await asyncio.to_thread(log_and_send_error, f"Failed to place limit order for {symbol}", e)
        
    except Exception as e:
        await asyncio.to_thread(log_and_send_error, f"Failed to evaluate symbol {symbol} for a new trade", e)

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
    global managed_trades, last_trade_close_time, running, overload_notified, symbol_loss_cooldown
    log.info("Monitor thread started.")
    while not monitor_stop_event.is_set():
        loop_start_time = time.time()
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
                                "tp": take_price, # The internally monitored TP level
                                "open_time": datetime.utcnow().isoformat(), "sltp_orders": sltp_orders, 
                                "be_moved": False, 
                                "trailing_active": False,
                                "tp_hit": False,
                                "risk_usdt": p_meta['risk_usdt'],
                                "entry_reason": "LIMIT_FILL"
                            }

                            with managed_trades_lock:
                                managed_trades[trade_id] = meta
                            
                            # Use a simplified record_trade call, as many fields are for the new management style
                            record_trade({
                                'id': trade_id, 'symbol': meta['symbol'], 'side': meta['side'], 'entry_price': meta['entry_price'],
                                'exit_price': None, 'qty': meta['qty'], 'notional': meta['notional'], 'pnl': None,
                                'open_time': meta['open_time'], 'close_time': None, 'risk_usdt': meta['risk_usdt'],
                                'entry_reason': meta.get('entry_reason'), 'tp1': take_price # Store main TP here for reporting
                            })
                            add_managed_trade_to_db(meta) # Keep for now as backup
                            if firebase_db:
                                try:
                                    firebase_db.child("managed_trades").child(trade_id).set(meta)
                                except Exception as e:
                                    log.exception(f"Failed to save new managed trade {trade_id} to Firebase: {e}")
                            
                            send_telegram(f"✅ Limit Order Filled & Trade Opened for {p_meta['symbol']} | Side: {p_meta['side']}, Entry: {actual_entry_price:.4f}, SL: {stop_price:.4f}, TP: {take_price:.4f}")
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
                            if firebase_db:
                                try:
                                    firebase_db.child("pending_limit_orders").child(p_id).delete()
                                except Exception as e:
                                    log.exception(f"Failed to delete pending order {p_id} from Firebase: {e}")

            positions = []
            try:
                max_retries = 3
                retry_delay = 10  # seconds
                for attempt in range(max_retries):
                    try:
                        positions = client.futures_position_information()
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
                    # This is a fatal auth/IP error. The hosting platform should restart the service.
                    ip = get_public_ip()
                    error_msg = (
                        f"🚨 **CRITICAL AUTH ERROR** 🚨\n\n"
                        f"Binance API keys are invalid or the server's IP is not whitelisted.\n\n"
                        f"Error Code: `{e.code}`\n"
                        f"Server IP: `{ip}`\n\n"
                        f"The bot will now attempt to restart to get a new IP address. Please add the new IP to your Binance whitelist if the error persists."
                    )
                    send_telegram(error_msg, parse_mode='Markdown')
                    log.warning(f"IP Whitelist Error (Code: -2015). Server IP: {ip}. Restarting in 60 seconds...")
                    time.sleep(60)
                    sys.exit(1) # Exit with error code to trigger platform restart

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
            managed_trades_lock.acquire()
            try:
                trades_snapshot = dict(managed_trades)
            finally:
                managed_trades_lock.release()
            
            # --- Pre-fetch kline data for all active symbols to reduce API calls ---
            active_symbols = {meta['symbol'] for meta in trades_snapshot.values()}
            kline_data_cache = {}
            for sym_key in active_symbols:
                try:
                    # Fetch enough data for RSI calculation
                    kline_data_cache[sym_key] = fetch_klines_sync(sym_key, CONFIG["TIMEFRAME"], 50)
                except Exception as e:
                    log.error(f"Failed to pre-fetch klines for {sym_key} in monitor loop: {e}")
                    kline_data_cache[sym_key] = None

            to_remove = []
            for tid, meta in trades_snapshot.items():
                sym = meta['symbol']
                pos = next((p for p in positions if p.get('symbol') == sym), None)
                if not pos:
                    continue
                
                pos_amt = float(pos.get('positionAmt') or 0.0)
                unreal = float(pos.get('unRealizedProfit') or 0.0)
                
                with managed_trades_lock:
                    if tid in managed_trades:
                        managed_trades[tid]['unreal'] = unreal

                if abs(pos_amt) < 1e-8:
                    close_time = datetime.utcnow().replace(tzinfo=timezone.utc)
                    meta['close_time'] = close_time.isoformat()
                    exit_reason = meta.get('exit_reason', 'SL/TP') # Default to SL/TP if not set by an early exit
                    
                    # --- Post-Loss Cooldown Logic ---
                    if unreal < 0:
                        symbol_loss_cooldown[sym] = close_time
                        log.info(f"Symbol {sym} has been placed on a {CONFIG['LOSS_COOLDOWN_HOURS']}h cooldown due to a losing trade (PnL: {unreal:.4f}).")
                        send_telegram(f"🧊 Symbol {sym} is on a {CONFIG['LOSS_COOLDOWN_HOURS']}h cooldown after a loss.")

                    log.info(f"TRADE_CLOSE_EVENT: ID={tid}, Symbol={sym}, PnL={unreal:.4f}, Reason={exit_reason}. Preparing to record and remove from managed trades.")
                    record_trade({
                        'id': meta['id'], 'symbol': meta['symbol'], 'side': meta['side'],
                        'entry_price': meta['entry_price'], 'exit_price': float(pos.get('entryPrice') or 0.0),
                        'qty': meta['initial_qty'], 'notional': meta['notional'], 'pnl': unreal,
                        'open_time': meta['open_time'], 'close_time': meta['close_time'],
                        'risk_usdt': meta.get('risk_usdt', 0.0),
                        'entry_reason': meta.get('entry_reason'), 'exit_reason': exit_reason,
                        'tp1': meta.get('tp1'), 'tp2': meta.get('tp2')
                    })
                    remove_managed_trade_from_db(tid) # Keep as backup
                    if firebase_db:
                        try:
                            firebase_db.child("managed_trades").child(tid).delete()
                        except Exception as e:
                            log.exception(f"Failed to delete managed trade {tid} from Firebase: {e}")
                    with managed_trades_lock:
                        last_trade_close_time[sym] = close_time
                    
                    close_msg = (
                        f"✅ *Trade Closed*\n\n"
                        f"**ID:** `{meta['id']}`\n"
                        f"**Symbol:** {sym}\n"
                        f"**Reason:** {exit_reason}\n"
                        f"**PnL:** `{unreal:.4f} USDT`"
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
                    current_price = df_monitor['close'].iloc[-1]
                    entry_price = meta['entry_price']
                    side = meta['side']

                    # 1. Check for Partial Take Profit first
                    if not meta.get('tp_hit'):
                        tp_price = meta.get('tp')
                        if tp_price:
                            hit_tp = (side == 'BUY' and current_price >= tp_price) or \
                                     (side == 'SELL' and current_price <= tp_price)
                            
                            if hit_tp:
                                log.info(f"Trade {tid} hit internal TP at {tp_price}. Closing {CONFIG['PARTIAL_TP_CLOSE_PCT']*100}%.")
                                qty_to_close = meta['initial_qty'] * CONFIG['PARTIAL_TP_CLOSE_PCT']
                                qty_to_close = round_qty(sym, qty_to_close)
                                
                                if qty_to_close > 0:
                                    close_partial_market_position_sync(sym, side, qty_to_close)
                                
                                cancel_trade_sltp_orders_sync(meta)
                                
                                new_qty = meta['qty'] - qty_to_close
                                new_sl_price = entry_price * (1.001 if side == 'BUY' else 0.999)
                                new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl_price, tp_price=None, qty=new_qty)

                                with managed_trades_lock:
                                    if tid in managed_trades:
                                        managed_trades[tid]['qty'] = new_qty
                                        managed_trades[tid]['sltp_orders'] = new_orders
                                        managed_trades[tid]['tp_hit'] = True
                                        managed_trades[tid]['be_moved'] = True
                                        managed_trades[tid]['trailing_active'] = True
                                        managed_trades[tid]['sl'] = new_sl_price
                                        add_managed_trade_to_db(managed_trades[tid]) # Keep as backup
                                        if firebase_db:
                                            try:
                                                firebase_db.child("managed_trades").child(tid).set(managed_trades[tid])
                                            except Exception as e:
                                                log.exception(f"Failed to update managed trade {tid} in Firebase after partial TP: {e}")
                                
                                send_telegram(f"✅ TP hit for {tid} ({sym}). Closed {CONFIG['PARTIAL_TP_CLOSE_PCT']*100}%. SL moved to BE. Remainder is trailing.")
                                continue

                    # 2. Check for Break-Even Trigger
                    if not meta.get('be_moved'):
                        profit_pct = (current_price / entry_price - 1) if side == 'BUY' else (1 - current_price / entry_price)
                        if profit_pct >= CONFIG['BE_TRIGGER_PROFIT_PCT']:
                            log.info(f"Trade {tid} hit BE trigger at {profit_pct:.2%}. Moving SL.")
                            
                            cancel_trade_sltp_orders_sync(meta)
                            
                            new_sl_price = entry_price * (1 + CONFIG['BE_SL_PROFIT_PCT'] if side == 'BUY' else 1 - CONFIG['BE_SL_PROFIT_PCT'])
                            
                            new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl_price, tp_price=None, qty=meta['qty'])
                            
                            with managed_trades_lock:
                                if tid in managed_trades:
                                    managed_trades[tid]['sl'] = new_sl_price
                                    managed_trades[tid]['sltp_orders'] = new_orders
                                    managed_trades[tid]['be_moved'] = True
                                    managed_trades[tid]['trailing_active'] = True
                                    add_managed_trade_to_db(managed_trades[tid]) # Keep as backup
                                    if firebase_db:
                                        try:
                                            firebase_db.child("managed_trades").child(tid).set(managed_trades[tid])
                                        except Exception as e:
                                            log.exception(f"Failed to update managed trade {tid} in Firebase after BE trigger: {e}")
                            
                            send_telegram(f"📈 Breakeven triggered for {tid} ({sym}). SL moved to {new_sl_price:.4f} and trailing stop activated.")
                            continue

                    # 3. Check for Trailing Stop
                    if meta.get('trailing_active'):
                        atr_now = atr(df_monitor, CONFIG["ATR_LENGTH"]).iloc[-1]
                        
                        new_sl = None
                        current_sl = meta['sl']
                        
                        if side == 'BUY':
                            potential_sl = current_price - (atr_now * CONFIG["SL_TP_ATR_MULT"])
                            if potential_sl > current_sl:
                                new_sl = potential_sl
                        else: # SELL
                            potential_sl = current_price + (atr_now * CONFIG["SL_TP_ATR_MULT"])
                            if potential_sl < current_sl:
                                new_sl = potential_sl
                        
                        if new_sl:
                            log.info(f"Trailing SL for {tid}. Old: {current_sl:.4f}, New: {new_sl:.4f}")
                            cancel_trade_sltp_orders_sync(meta)
                            new_orders = place_batch_sl_tp_sync(sym, side, sl_price=new_sl, tp_price=None, qty=meta['qty'])
                            
                            with managed_trades_lock:
                                if tid in managed_trades:
                                    managed_trades[tid]['sl'] = new_sl
                                    managed_trades[tid]['sltp_orders'] = new_orders
                                    add_managed_trade_to_db(managed_trades[tid]) # Keep as backup
                                    if firebase_db:
                                        try:
                                            firebase_db.child("managed_trades").child(tid).set(managed_trades[tid])
                                        except Exception as e:
                                            log.exception(f"Failed to update managed trade {tid} in Firebase after trailing stop: {e}")
                            
                            send_telegram(f"📈 Trailing SL updated for {tid} ({sym}) to `{new_sl:.4f}`")
                
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
                    running = False
                    daily_loss_limit_hit = True
                    send_telegram(f"🚨 MAX DAILY LOSS LIMIT HIT! 🚨\nToday's PnL: {daily_pnl:.2f} USDT\nLimit: {CONFIG['MAX_DAILY_LOSS']:.2f} USDT\nBot is now PAUSED until the next UTC day.")
            
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

            # Sleep for the configured interval
            time.sleep(CONFIG["DAILY_PNL_CHECK_INTERVAL"])

        except Exception as e:
            log.exception("An unhandled exception occurred in the daily PnL monitor thread.")
            time.sleep(120)
    
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


async def manage_session_freeze_state():
    """
    Checks session freeze status, sends notifications, and returns the effective freeze state.
    Returns True if the bot's trading logic should be frozen, False otherwise.
    """
    global frozen, session_freeze_override, session_freeze_active, notified_frozen_session

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


async def scanning_loop():
    while True:
        try:
            if not running:
                await asyncio.sleep(2)
                continue

            # Check for session freeze
            if await manage_session_freeze_state():
                log.info("Scan cycle skipped due to session freeze.")
                cooldown_seconds = CONFIG["SCAN_COOLDOWN_MINUTES"] * 60
                await asyncio.sleep(cooldown_seconds)
                continue

            log.info("Starting concurrent symbol scan...")
            symbols = [s.strip().upper() for s in CONFIG["SYMBOLS"] if s.strip()]
            tasks = [evaluate_and_enter(s) for s in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            for symbol, result in zip(symbols, results):
                if isinstance(result, Exception):
                    log.error(f"Error evaluating symbol {symbol} during concurrent scan: {result}")
            
            cooldown_seconds = CONFIG["SCAN_COOLDOWN_MINUTES"] * 60
            log.info(f"Scan cycle complete. Cooling down for {CONFIG['SCAN_COOLDOWN_MINUTES']} minutes.")
            await asyncio.sleep(cooldown_seconds)

        except asyncio.CancelledError:
            log.info("Scanning loop cancelled.")
            break
        except Exception as e:
            log.exception("An unhandled error occurred in the main scanning loop: %s", e)
            # To prevent rapid-fire errors, wait a bit before retrying.
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
        if df.empty:
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
    async with managed_trades_lock:
        return dict(managed_trades)

async def get_pending_orders_snapshot():
    async with pending_limit_orders_lock:
        return dict(pending_limit_orders)

def build_control_keyboard():
    buttons = [
        [KeyboardButton("/startbot"), KeyboardButton("/stopbot")],
        [KeyboardButton("/freeze"), KeyboardButton("/unfreeze")],
        [KeyboardButton("/listorders"), KeyboardButton("/listpending")],
        [KeyboardButton("/status"), KeyboardButton("/showparams")],
        [KeyboardButton("/usage"), KeyboardButton("/report")],
        [KeyboardButton("/rejects"), KeyboardButton("/help")]
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
                fut = asyncio.run_coroutine_threadsafe(get_managed_trades_snapshot(), loop)
                trades = {}
                try: trades = fut.result(timeout=5)
                except Exception as e: log.error("Failed to get managed trades for /status: %s", e)
                
                unrealized_pnl = sum(float(v.get('unreal', 0.0)) for v in trades.values())
                
                # PnL Info section
                pnl_info = (
                    f"Today's Realized PnL: {current_daily_pnl:.2f} USDT\n"
                    f"Current Unrealized PnL: {unrealized_pnl:.2f} USDT"
                )
                if daily_loss_limit_hit:
                    pnl_info += f"\n(LIMIT REACHED: {CONFIG['MAX_DAILY_LOSS']:.2f})"

                # Bot Status section
                status_lines = [f"▶️ Running: *{running}*"]
                status_lines.append(f"✋ Manual Freeze: *{frozen}*")
                
                # Build a more descriptive session freeze status
                is_natural_freeze, session_name = get_session_freeze_status(datetime.now(timezone.utc))
                effective_freeze = frozen or (is_natural_freeze and not session_freeze_override)
                
                session_status_text = f"❄️ Effective Freeze: *{effective_freeze}*"
                details = []
                if frozen:
                    details.append("Manual")
                if is_natural_freeze:
                    details.append(f"Session: {session_name}")
                if session_freeze_override:
                    details.append("Overridden")
                if details:
                    session_status_text += f" ({'; '.join(details)})"
                
                status_lines.append(session_status_text)
                status_lines.append(f"📈 Managed Trades: *{len(trades)}*")

                # Combine sections
                txt = (
                    f"📊 *Bot Status*\n\n"
                    f"{'\n'.join(status_lines)}\n\n"
                    f"💰 *PnL Info*\n{pnl_info}"
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
                fut = asyncio.run_coroutine_threadsafe(generate_and_send_report(), loop)
                try:
                    fut.result(timeout=60) # Give it a long timeout for report generation
                except Exception as e:
                    log.error("Failed to execute /report action: %s", e)
                    send_telegram(f"Failed to generate report: {e}")
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
            elif text.startswith("/rejects"):
                async def _task():
                    if not rejected_trades:
                        await asyncio.to_thread(send_telegram, "No rejected trades have been recorded yet.")
                        return

                    report_lines = ["*Last 5 Rejected Trades*"]
                    # Using list() to create a copy for safe iteration
                    for reject in reversed(list(rejected_trades)):
                        ts = datetime.fromisoformat(reject['timestamp']).strftime('%Y-%m-%d %H:%M:%S UTC')
                        details_str = ", ".join([f"{k}: {v}" for k, v in reject['details'].items()])
                        
                        line = (
                            f"\n*Symbol:* {reject['symbol']} at {ts}\n"
                            f"  - *Reason:* {reject['reason']}\n"
                            f"  - *Details:* `{details_str}`"
                        )
                        report_lines.append(line)
                    
                    await asyncio.to_thread(send_telegram, "\n".join(report_lines), parse_mode='Markdown')

                fut = asyncio.run_coroutine_threadsafe(_task(), loop)
                try: fut.result(timeout=10)
                except Exception as e: log.error("Failed to execute /rejects action: %s", e)
            elif text.startswith("/help"):
                help_text = (
                    "*KAMA Bot Commands*\n\n"
                    "*Trading Control*\n"
                    "- `/startbot`: Starts the bot (resumes scanning for trades).\n"
                    "- `/stopbot`: Stops the bot (pauses scanning for trades).\n"
                    "- `/freeze`: Manually freezes the bot, preventing all new trades.\n"
                    "- `/unfreeze`: Lifts a manual freeze and overrides any active session freeze.\n\n"
                    "*Information & Reports*\n"
                    "- `/status`: Shows a detailed status of the bot.\n"
                    "- `/listorders`: Lists all currently open trades with details.\n"
                    "- `/listpending`: Lists all pending limit orders that have not been filled.\n"
                    "- `/sessions`: Reports the current session freeze status.\n"
                    "- `/rejects`: Shows a report of the last 5 rejected trade opportunities.\n"
                    "- `/report`: Generates an overall performance report.\n"
                    "- `/chart <SYMBOL>`: Generates a detailed chart for a symbol.\n\n"
                    "*Configuration*\n"
                    "- `/showparams`: Displays all configurable bot parameters.\n"
                    "- `<KEY> = <VALUE>`: Sets a parameter (e.g., `MAX_CONCURRENT_TRADES = 4`).\n\n"
                    "*Utilities*\n"
                    "- `/ip`: Shows the bot's public server IP address.\n"
                    "- `/usage`: Displays the current CPU and memory usage.\n"
                    "- `/validate`: Performs a sanity check on the configuration.\n"
                    "- `/help`: Displays this help message."
                )
                async def _task():
                    await asyncio.to_thread(send_telegram, help_text, parse_mode='Markdown')
                fut = asyncio.run_coroutine_threadsafe(_task(), loop)
                try:
                    fut.result(timeout=10)
                except Exception as e:
                    log.error("Failed to execute /help action: %s", e)
            elif text.startswith("/usage"):
                cpu_usage = psutil.cpu_percent(interval=1)
                memory_info = psutil.virtual_memory()
                
                usage_report = (
                    f"🖥️ *System Resource Usage*\n\n"
                    f"  - *CPU Usage:* {cpu_usage}%\n"
                    f"  - *Memory Usage:* {memory_info.percent}%\n"
                    f"    - Total: {memory_info.total / (1024**3):.2f} GB\n"
                    f"    - Used: {memory_info.used / (1024**3):.2f} GB\n"
                    f"    - Free: {memory_info.free / (1024**3):.2f} GB"
                )
                send_telegram(usage_report, parse_mode='Markdown')
            elif text.startswith("/scalein"):
                parts = text.split()
                if len(parts) < 3:
                    send_telegram("Usage: /scalein <trade_id> <risk_usd_to_add>")
                else:
                    trade_id, risk_to_add_str = parts[1], parts[2]
                    try:
                        risk_to_add = float(risk_to_add_str)
                        
                        async def _task():
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
                                await asyncio.to_thread(open_market_position_sync, trade['symbol'], trade['side'], qty_to_add, trade['leverage'])
                                
                                async with managed_trades_lock:
                                    trade['qty'] += qty_to_add
                                    trade['notional'] += qty_to_add * trade['entry_price'] # Approximate
                                    trade['risk_usdt'] += risk_to_add
                                    await asyncio.to_thread(add_managed_trade_to_db, trade)

                                await asyncio.to_thread(send_telegram, f"✅ Scaled in {trade_id} by {qty_to_add} {trade['symbol']}.")
                            else:
                                await asyncio.to_thread(send_telegram, "Calculated quantity to add is zero.")

                        fut = asyncio.run_coroutine_threadsafe(_task(), loop)
                        fut.result(timeout=30)
                    except ValueError:
                        send_telegram("Invalid risk amount.")
                    except Exception as e:
                        log.exception(f"Failed to scale in {trade_id}")
                        send_telegram(f"❌ Error scaling in {trade_id}: {e}")
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
    global running
    running = val

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

def _signal_handler(sig, frame):
    log.info("Received signal %s, shutting down", sig)
    monitor_stop_event.set()
    try:
        send_telegram(f"Received signal {sig}. Shutting down.")
    except Exception:
        pass
    sys.exit(0)

signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)

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
