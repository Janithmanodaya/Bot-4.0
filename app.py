# app.py
"""
KAMA trend-following bot — complete version with fixes:
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
log = logging.getLogger("kama-bot")

# Globals
client: Optional[Client] = None
telegram_bot: Optional[telegram.Bot] = telegram.Bot(token=TELEGRAM_BOT_TOKEN) if TELEGRAM_BOT_TOKEN else None
main_loop: Optional[asyncio.AbstractEventLoop] = None

# -------------------------
# CONFIG (edit values here)
# -------------------------
CONFIG = {
    "SYMBOLS": os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT,BNBUSDT").split(","),
    "TIMEFRAME": os.getenv("TIMEFRAME", "15m"),
    "BIG_TIMEFRAME": os.getenv("BIG_TIMEFRAME", "4h"),

    "SCAN_INTERVAL": int(os.getenv("SCAN_INTERVAL", "20")),
    "SCAN_COOLDOWN_MINUTES": int(os.getenv("SCAN_COOLDOWN_MINUTES", "5")),
    "MAX_CONCURRENT_TRADES": int(os.getenv("MAX_CONCURRENT_TRADES", "3")),
    "START_MODE": os.getenv("START_MODE", "running").lower(),

    "KAMA_LENGTH": int(os.getenv("KAMA_LENGTH", "10")),
    "KAMA_FAST": int(os.getenv("KAMA_FAST", "2")),
    "KAMA_SLOW": int(os.getenv("KAMA_SLOW", "30")),

    "ATR_LENGTH": int(os.getenv("ATR_LENGTH", "14")),
    "SL_TP_ATR_MULT": float(os.getenv("SL_TP_ATR_MULT", "2.5")),

    "RISK_SMALL_BALANCE_THRESHOLD": float(os.getenv("RISK_SMALL_BALANCE_THRESHOLD", "50.0")),
    "RISK_SMALL_FIXED_USDT": float(os.getenv("RISK_SMALL_FIXED_USDT", "0.5")),
    "MARGIN_USDT_SMALL_BALANCE": float(os.getenv("MARGIN_USDT_SMALL_BALANCE", "2.0")),
    "RISK_PCT_LARGE": float(os.getenv("RISK_PCT_LARGE", "0.02")),
    "MAX_RISK_USDT": float(os.getenv("MAX_RISK_USDT", "0.0")),  # 0 disables cap
    "MAX_BOT_LEVERAGE": int(os.getenv("MAX_BOT_LEVERAGE", "30")),

    "VOLATILITY_ADJUST_ENABLED": os.getenv("VOLATILITY_ADJUST_ENABLED", "true").lower() in ("true", "1", "yes"),
    "TRENDING_ADX": float(os.getenv("TRENDING_ADX", "40.0")),
    "TRENDING_CHOP": float(os.getenv("TRENDING_CHOP", "40.0")),
    "TRENDING_RISK_MULT": float(os.getenv("TRENDING_RISK_MULT", "1.5")),
    "CHOPPY_ADX": float(os.getenv("CHOPPY_ADX", "25.0")),
    "CHOPPY_CHOP": float(os.getenv("CHOPPY_CHOP", "60.0")),
    "CHOPPY_RISK_MULT": float(os.getenv("CHOPPY_RISK_MULT", "0.5")),

    "ADX_LENGTH": int(os.getenv("ADX_LENGTH", "14")),
    "ADX_THRESHOLD": float(os.getenv("ADX_THRESHOLD", "35.0")),

    "CHOP_LENGTH": int(os.getenv("CHOP_LENGTH", "14")),
    "CHOP_THRESHOLD": float(os.getenv("CHOP_THRESHOLD", "55.0")),

    "BB_LENGTH": int(os.getenv("BB_LENGTH", "20")),
    "BB_STD": float(os.getenv("BB_STD", "2.0")),
    "BBWIDTH_THRESHOLD": float(os.getenv("BBWIDTH_THRESHOLD", "12.0")),

    "MIN_CANDLES_AFTER_CLOSE": int(os.getenv("MIN_CANDLES_AFTER_CLOSE", "10")),

    "TRAILING_ENABLED": os.getenv("TRAILING_ENABLED", "true").lower() in ("true", "1", "yes"),
    "BE_AUTO_MOVE_ENABLED": os.getenv("BE_AUTO_MOVE_ENABLED", "true").lower() in ("true", "1", "yes"),

    "DYN_SLTP_ENABLED": os.getenv("DYN_SLTP_ENABLED", "true").lower() in ("true", "1", "yes"),
    "TP1_ATR_MULT": float(os.getenv("TP1_ATR_MULT", "1.0")),
    "TP2_ATR_MULT": float(os.getenv("TP2_ATR_MULT", "2.0")),
    "TP3_ATR_MULT": float(os.getenv("TP3_ATR_MULT", "3.0")),
    "TP1_CLOSE_PCT": float(os.getenv("TP1_CLOSE_PCT", "0.5")), # 50%
    "TP2_CLOSE_PCT": float(os.getenv("TP2_CLOSE_PCT", "0.25")), # 25%

    "MAX_DAILY_LOSS": float(os.getenv("MAX_DAILY_LOSS", "-2.0")), # Negative value, e.g. -50.0 for $50 loss
    "MAX_DAILY_PROFIT": float(os.getenv("MAX_DAILY_PROFIT", "5.0")), # 0 disables this
    "AUTO_FREEZE_ON_PROFIT": os.getenv("AUTO_FREEZE_ON_PROFIT", "true").lower() in ("true", "1", "yes"),
    "DAILY_PNL_CHECK_INTERVAL": int(os.getenv("DAILY_PNL_CHECK_INTERVAL", "60")), # In seconds

    "DB_FILE": os.getenv("DB_FILE", "trades.db"),
    
    "DRY_RUN": os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes"),
    "MIN_NOTIONAL_USDT": float(os.getenv("MIN_NOTIONAL_USDT", "5.0")),
    "HEDGING_ENABLED": os.getenv("HEDGING_ENABLED", "false").lower() in ("true", "1", "yes"),
    "MONITOR_LOOP_THRESHOLD_SEC": int(os.getenv("MONITOR_LOOP_THRESHOLD_SEC", "10")),
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
    log.info("--- Starting Trade Reconciliation ---")

    db_trades = await asyncio.to_thread(load_managed_trades_from_db)
    log.info(f"Found {len(db_trades)} managed trade(s) in the database.")

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
                    "dyn_sltp": CONFIG["DYN_SLTP_ENABLED"], "tp1": None, "tp2": None, "tp3": None,
                    "trade_phase": 0, "be_moved": False, "risk_usdt": 0.0 # Risk is unknown for imported trades
                }

                # Add to managed trades and save to DB
                retained_trades[trade_id] = meta
                await asyncio.to_thread(add_managed_trade_to_db, meta)

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
                    "dyn_sltp": CONFIG["DYN_SLTP_ENABLED"], "tp1": None, "tp2": None, "tp3": None,
                    "trade_phase": 0, "be_moved": False, "risk_usdt": 0.0
                }

                # Cancel any existing SL/TP orders for this symbol before placing new ones
                await asyncio.to_thread(cancel_close_orders_sync, symbol)
                
                # Place the new SL/TP orders
                await asyncio.to_thread(place_batch_sl_tp_sync, symbol, side, sl_price=stop_price, tp_price=take_price, qty=qty)
                
                # Add to managed trades and save to DB
                async with managed_trades_lock:
                    managed_trades[trade_id] = meta
                await asyncio.to_thread(add_managed_trade_to_db, meta)

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


# -------------------------
# App Lifespan Manager
# -------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global scan_task, telegram_thread, monitor_thread_obj, pnl_monitor_thread_obj, client, monitor_stop_event, main_loop
    log.info("KAMA strategy bot starting up...")
    
    # --- Startup Logic ---
    init_db()

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
        await asyncio.to_thread(send_telegram, "KAMA strategy bot started. Running={}".format(running))
    except Exception:
        log.exception("Failed to send startup telegram")

    yield

    # --- Shutdown Logic ---
    log.info("KAMA strategy bot shutting down...")
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
        await send_telegram("KAMA strategy bot shut down.")
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


def get_session_freeze_status(now: datetime) -> (bool, Optional[str]):
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
    INSERT OR REPLACE INTO trades (id,symbol,side,entry_price,exit_price,qty,notional,risk_usdt,pnl,open_time,close_time)
    VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (rec['id'], rec['symbol'], rec['side'], rec['entry_price'], rec.get('exit_price'),
          rec['qty'], rec['notional'], rec.get('risk_usdt'), rec.get('pnl'), 
          rec['open_time'], rec.get('close_time')))
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
def kama(series: pd.Series, length: int, fast: int, slow: int) -> pd.Series:
    price = series.values
    n = len(price)
    kama_arr = np.zeros(n)
    sc_fast = 2 / (fast + 1)
    sc_slow = 2 / (slow + 1)
    if n >= length:
        kama_arr[:length] = np.mean(price[:length])
    else:
        kama_arr[:] = price.mean()
    for i in range(length, n):
        change = abs(price[i] - price[i - length])
        volatility = np.sum(np.abs(price[i - length + 1:i + 1] - price[i - length:i]))
        er = 0.0
        if volatility != 0:
            er = change / volatility
        sc = (er * (sc_fast - sc_slow) + sc_slow) ** 2
        kama_arr[i] = kama_arr[i - 1] + sc * (price[i] - kama_arr[i - 1])
    return pd.Series(kama_arr, index=series.index)

def atr(df: pd.DataFrame, length: int) -> pd.Series:
    high = df['high']; low = df['low']; close = df['close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    return tr.rolling(length, min_periods=1).mean()

def adx(df: pd.DataFrame, length: int) -> pd.Series:
    high = df['high']; low = df['low']; close = df['close']
    up_move = high.diff()
    down_move = -low.diff()
    plus_dm = ((up_move > down_move) & (up_move > 0)) * up_move
    minus_dm = ((down_move > up_move) & (down_move > 0)) * down_move
    tr1 = (high - low)
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    atr_w = tr.rolling(length, min_periods=1).mean()
    plus_di = 100 * (plus_dm.rolling(length, min_periods=1).sum() / atr_w)
    minus_di = 100 * (minus_dm.rolling(length, min_periods=1).sum() / atr_w)
    dx = (abs(plus_di - minus_di) / (plus_di + minus_di)).replace([np.inf, -np.inf], 0).fillna(0) * 100
    return dx.rolling(length, min_periods=1).mean()

def choppiness_index(df: pd.DataFrame, length: int) -> pd.Series:
    high = df['high']; low = df['low']; close = df['close']
    tr1 = high - low
    tr2 = (high - close.shift(1)).abs()
    tr3 = (low - close.shift(1)).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
    sum_tr = tr.rolling(length, min_periods=1).sum()
    hh = high.rolling(length, min_periods=1).max()
    ll = low.rolling(length, min_periods=1).min()
    denom = hh - ll
    denom = denom.replace(0, np.nan)
    chop = 100 * (np.log10(sum_tr / denom) / np.log10(length))
    chop = chop.replace([np.inf, -np.inf], 100).fillna(100)
    return chop

def bb_width(df: pd.DataFrame, length: int, std_mult: float) -> pd.Series:
    ma = df['close'].rolling(length, min_periods=1).mean()
    std = df['close'].rolling(length, min_periods=1).std().fillna(0)
    upper = ma + std_mult * std
    lower = ma - std_mult * std
    mid = ma.replace(0, np.nan)
    width = (upper - lower) / mid
    width = width.replace([np.inf, -np.inf], 100).fillna(100)
    return width

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
        requests_params = {"timeout": 30}
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
            return f"{price:.8f}" # Fallback
        symbol_info = next((s for s in info.get('symbols', []) if s.get('symbol') == symbol), None)
        if not symbol_info:
            return f"{price:.8f}" # Fallback
        for f in symbol_info.get('filters', []):
            if f.get('filterType') == 'PRICE_FILTER':
                tick_size = Decimal(str(f.get('tickSize', '0.00000001')))
                getcontext().prec = 28
                p = Decimal(str(price))
                rounded_price = p.quantize(tick_size, rounding=ROUND_DOWN)
                return f"{rounded_price}"
    except Exception:
        log.exception("round_price failed; falling back to basic formatting")
    return f"{price:.8f}"

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
    adx_val = CONFIG["ADX_THRESHOLD"]
    if not (0 <= adx_val <= 100):
        results["ok"] = False
        results["checks"].append({"type": "adx_threshold", "ok": False, "detail": adx_val})
    else:
        results["checks"].append({"type": "adx_threshold", "ok": True})
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
            k = kama(raw_df['close'], CONFIG["KAMA_LENGTH"], CONFIG["KAMA_FAST"], CONFIG["KAMA_SLOW"])
            a = atr(raw_df, CONFIG["ATR_LENGTH"])
            ad = adx(raw_df, CONFIG["ADX_LENGTH"])
            ch = choppiness_index(raw_df, CONFIG["CHOP_LENGTH"])
            bw = bb_width(raw_df, CONFIG["BB_LENGTH"], CONFIG["BB_STD"])
            results["checks"].append({"type": "indicators_sample", "ok": True, "detail": {
                "kama": float(k.iloc[-1]), "atr": float(a.iloc[-1]), "adx": float(ad.iloc[-1]),
                "chop": float(ch.iloc[-1]), "bbw": float(bw.iloc[-1])
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
    global managed_trades, running, frozen
    if frozen or not running:
        reason = "Bot is frozen or not running"
        _record_rejection(symbol, reason, {"running": running, "frozen": frozen})
        return
    try:
        df = await asyncio.to_thread(fetch_klines_sync, symbol, CONFIG["TIMEFRAME"], 500)
        df['kama'] = kama(df['close'], CONFIG["KAMA_LENGTH"], CONFIG["KAMA_FAST"], CONFIG["KAMA_SLOW"])
        df['atr'] = atr(df, CONFIG["ATR_LENGTH"])
        df['adx'] = adx(df, CONFIG["ADX_LENGTH"])
        df['chop'] = choppiness_index(df, CONFIG["CHOP_LENGTH"])
        df['bbw'] = bb_width(df, CONFIG["BB_LENGTH"], CONFIG["BB_STD"])
        last = df.iloc[-1]; prev = df.iloc[-2]
        price = last['close']
        kama_now = last['kama']; kama_prev = prev['kama']
        atr_now = last['atr']; adx_now = last['adx']; chop_now = last['chop']; bbw_now = last['bbw']

        details = {'price': price, 'adx': adx_now, 'chop': chop_now, 'bbw': bbw_now * 100}

        # --- Volatility Regime Detection ---
        risk_multiplier = 1.0
        regime = "NORMAL"
        if CONFIG["VOLATILITY_ADJUST_ENABLED"]:
            if adx_now > CONFIG["TRENDING_ADX"] and chop_now < CONFIG["TRENDING_CHOP"]:
                risk_multiplier = CONFIG["TRENDING_RISK_MULT"]
                regime = "TRENDING"
            elif adx_now < CONFIG["CHOPPY_ADX"] or chop_now > CONFIG["CHOPPY_CHOP"]:
                risk_multiplier = CONFIG["CHOPPY_RISK_MULT"]
                regime = "CHOPPY"
        
        notify_regime_change = False
        with symbol_regimes_lock:
            previous_regime = symbol_regimes.get(symbol, "NORMAL")
            if regime != previous_regime:
                symbol_regimes[symbol] = regime
                notify_regime_change = True
        
        if notify_regime_change:
            log.info(f"Risk regime for {symbol} changed to: {regime}. Risk multiplier: {risk_multiplier}x")
        # --- End Volatility Regime ---

        trend_small = 'bull' if (kama_now - kama_prev) > 0 else 'bear'
        df_big = await asyncio.to_thread(fetch_klines_sync, symbol, CONFIG["BIG_TIMEFRAME"], 200)
        df_big['kama'] = kama(df_big['close'], CONFIG["KAMA_LENGTH"], CONFIG["KAMA_FAST"], CONFIG["KAMA_SLOW"])
        trend_big = 'bull' if (df_big['kama'].iloc[-1] - df_big['kama'].iloc[-2]) > 0 else 'bear'
        
        details.update({'trend_small': trend_small, 'trend_big': trend_big})

        if adx_now < CONFIG["ADX_THRESHOLD"]:
            _record_rejection(symbol, f"ADX too low", details)
            return
        if chop_now >= CONFIG["CHOP_THRESHOLD"]:
            _record_rejection(symbol, f"CHOP too high", details)
            return
        if (bbw_now * 100.0) >= CONFIG["BBWIDTH_THRESHOLD"]:
            _record_rejection(symbol, f"BBW too high", details)
            return
        if trend_small != trend_big:
            _record_rejection(symbol, f"Trend mismatch", details)
            return
        
        crossed_above = (prev['close'] <= prev['kama']) and (last['close'] > kama_now)
        crossed_below = (prev['close'] >= prev['kama']) and (last['close'] < kama_now)
        if not (crossed_above or crossed_below):
            _record_rejection(symbol, "No KAMA cross", details)
            return
            
        side = None
        if crossed_above and trend_small == 'bull':
            side = 'BUY'
        elif crossed_below and trend_small == 'bear':
            side = 'SELL'
        
        if not side:
            _record_rejection(symbol, "No valid side for entry", details)
            return

        details['side'] = side

        async with managed_trades_lock:
            existing_trades_for_symbol = [t for t in managed_trades.values() if t['symbol'] == symbol]
            
            if existing_trades_for_symbol:
                if not CONFIG["HEDGING_ENABLED"]:
                    _record_rejection(symbol, "Trade exists and hedging is disabled", details)
                    return
                
                existing_side = existing_trades_for_symbol[0]['side']
                if side == existing_side:
                    _record_rejection(symbol, f"Signal is same side ({side}) as existing trade", details)
                    return
                
                if len(existing_trades_for_symbol) > 1:
                    _record_rejection(symbol, "Already have a hedge position", details)
                    return

                log.info(f"Hedge opportunity detected for {symbol}. New signal is {side}, opposite to existing {existing_side}.")

            if len(managed_trades) >= CONFIG["MAX_CONCURRENT_TRADES"]:
                _record_rejection(symbol, "Max concurrent trades reached", {'open_trades': len(managed_trades)})
                return

        async with managed_trades_lock:
            last_close = last_trade_close_time.get(symbol)
        if last_close:
            n_since = candles_since_close(df, last_close)
            if n_since < CONFIG["MIN_CANDLES_AFTER_CLOSE"]:
                _record_rejection(symbol, f"In cooldown period ({n_since}/{CONFIG['MIN_CANDLES_AFTER_CLOSE']} candles)", details)
                return
        
        sl_distance = CONFIG["SL_TP_ATR_MULT"] * atr_now
        if sl_distance <= 0 or math.isnan(sl_distance):
            _record_rejection(symbol, f"Invalid SL distance ({sl_distance})", details)
            return
        
        stop_price = price - sl_distance if side == 'BUY' else price + sl_distance
        
        tp1, tp2, tp3 = None, None, None
        if CONFIG["DYN_SLTP_ENABLED"]:
            tp1_dist = CONFIG["TP1_ATR_MULT"] * atr_now
            tp2_dist = CONFIG["TP2_ATR_MULT"] * atr_now
            tp3_dist = CONFIG["TP3_ATR_MULT"] * atr_now
            if side == 'BUY':
                tp1 = price + tp1_dist
                tp2 = price + tp2_dist
                tp3 = price + tp3_dist
            else: # SELL
                tp1 = price - tp1_dist
                tp2 = price - tp2_dist
                tp3 = price - tp3_dist
            take_price = tp3
        else:
            take_price = price + sl_distance if side == 'BUY' else price - sl_distance
        balance = await asyncio.to_thread(get_account_balance_usdt)
        risk_usdt = calculate_risk_amount(balance)
        risk_usdt *= risk_multiplier
        if risk_usdt <= 0:
            _record_rejection(symbol, "Risk amount non-positive", {'risk_usdt': risk_usdt})
            return
        price_distance = abs(price - stop_price)
        if price_distance <= 0:
            _record_rejection(symbol, "Price distance for SL is zero", {'price': price, 'sl': stop_price})
            return
        qty = risk_usdt / price_distance
        qty = await asyncio.to_thread(round_qty, symbol, qty)
        if qty <= 0:
            _record_rejection(symbol, "Quantity rounded to zero", {'risk_usdt': risk_usdt, 'price_dist': price_distance})
            return
        notional = qty * price
        
        min_notional = CONFIG["MIN_NOTIONAL_USDT"]
        if notional < min_notional:
            
            required_qty = min_notional / price
            new_risk = required_qty * price_distance
            
            if new_risk > balance:
                reason = "Notional value too small and cannot boost risk"
                _record_rejection(symbol, reason, {'notional': notional, 'required_risk': new_risk, 'balance': balance})
                await asyncio.to_thread(send_telegram, f"⚠️ Trade Skipped: {symbol}\nReason: {reason}\nNotional: {notional:.4f} USDT\nRequired Risk: {new_risk:.2f} USDT\nBalance: {balance:.2f} USDT")
                return

            risk_usdt = new_risk
            qty = required_qty
            qty = await asyncio.to_thread(round_qty, symbol, qty)
            if qty <= 0:
                _record_rejection(symbol, "Boosted quantity rounded to zero", {'new_risk': new_risk})
                return
            
            notional = qty * price
            
            await asyncio.to_thread(send_telegram, f"📈 Risk Boosted: {symbol}\nNew Risk: {risk_usdt:.2f} USDT\nNotional: {notional:.2f} USDT")

        if notional < min_notional:
            reason = "Notional value is less than minimum"
            _record_rejection(symbol, reason, {'notional': notional, 'min_notional': min_notional})
            await asyncio.to_thread(send_telegram, f"⚠️ Trade Skipped: {symbol}\nReason: {reason}\nNotional: {notional:.4f} USDT")
            return

        # --- Leverage Calculation ---
        # For small balances, use a dedicated margin amount for leverage calculation,
        # separating it from the amount being risked (risk_usdt).
        if balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"]:
            margin_to_use = CONFIG["MARGIN_USDT_SMALL_BALANCE"]
        else:
            # For larger balances, the margin used will be equal to the amount risked.
            margin_to_use = risk_usdt

        # Ensure margin is not greater than the notional value (which would be impossible anyway)
        margin_to_use = min(margin_to_use, notional)
        
        # Calculate leverage based on the desired margin.
        leverage = int(math.floor(notional / max(margin_to_use, 1e-9)))

        # Apply safety caps on leverage
        max_leverage_from_config = CONFIG.get("MAX_BOT_LEVERAGE", 20)
        max_leverage_from_exchange = get_max_leverage(symbol)
        leverage = max(1, min(leverage, max_leverage_from_config, max_leverage_from_exchange))

        try:
            batch_response = await asyncio.to_thread(
                place_market_order_with_sl_tp_sync, symbol, side, qty, leverage, stop_price, take_price
            )
            
            market_order = batch_response[0]
            sl_order = batch_response[1]
            tp_order = batch_response[2]

            order_id = str(market_order.get('orderId', f"mkt_{int(time.time())}"))
            sltp = {"stop_order": sl_order, "tp_order": tp_order}
            trade_id = f"{symbol}_{order_id}"
            
            meta = {
                "id": trade_id, "symbol": symbol, "side": side, "entry_price": price,
                "initial_qty": qty, "qty": qty, "notional": notional, "leverage": leverage,
                "sl": stop_price, "tp": take_price, "open_time": datetime.utcnow().isoformat(),
                "sltp_orders": sltp, "trailing": CONFIG["TRAILING_ENABLED"],
                "dyn_sltp": CONFIG["DYN_SLTP_ENABLED"],
                "tp1": tp1, "tp2": tp2, "tp3": tp3,
                "trade_phase": 0,
                "be_moved": False,
                "risk_usdt": risk_usdt
            }
            async with managed_trades_lock:
                managed_trades[trade_id] = meta
            record_trade({'id': trade_id, 'symbol': symbol, 'side': side, 'entry_price': price,
                          'exit_price': None, 'qty': qty, 'notional': notional, 'pnl': None,
                          'open_time': meta['open_time'], 'close_time': None, 'risk_usdt': risk_usdt})
            await asyncio.to_thread(add_managed_trade_to_db, meta)
            dry_run_prefix = "[DRY RUN] " if CONFIG["DRY_RUN"] else ""
            
            open_msg = (
                f"✅ *New Trade Opened* {dry_run_prefix}\n\n"
                f"**Symbol:** {symbol}\n"
                f"**Side:** {side}\n"
                f"**Entry:** `{price:.4f}`\n"
                f"**Qty:** `{qty}`\n"
                f"**Leverage:** {leverage}x\n"
                f"**Risk:** `{risk_usdt:.2f} USDT`\n"
                f"**SL:** `{stop_price:.4f}`\n"
                f"**TP:** `{take_price:.4f}`\n\n"
                f"**ID:** `{trade_id}`"
            )
            await asyncio.to_thread(send_telegram, open_msg, parse_mode='Markdown')
            log.info("%sOpened trade: %s", dry_run_prefix, meta)
        except Exception as e:
            # Use the new centralized error handler
            await asyncio.to_thread(log_and_send_error, f"Failed to open trade for {symbol}", e)
        return
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
    global managed_trades, last_trade_close_time, running, overload_notified
    log.info("Monitor thread started.")
    while not monitor_stop_event.is_set():
        loop_start_time = time.time()
        try:
            if client is None:
                time.sleep(5)
                continue

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
            except BinanceAPIException as e:
                log.error("Caught BinanceAPIException in monitor thread: %s", e)
                
                if e.code == -2015:
                    # This is a fatal auth/IP error. Freeze the bot and retry periodically.
                    ip = get_public_ip()
                    error_msg = (
                        f"🚨 **CRITICAL AUTH ERROR** 🚨\n\n"
                        f"Binance API keys are invalid, have incorrect permissions, or the server's IP address is not whitelisted.\n\n"
                        f"Error Code: `{e.code}`\n"
                        f"Server IP: {ip} \n\n"
                        f"Please add this IP to your Binance API key's whitelist. "
                        f"The bot is now FROZEN and will retry every 2 minutes."
                    )
                    send_telegram(error_msg)
                    running = False
                    frozen = True
                    log.info("Bot frozen due to auth error. Waiting 2 minutes before next attempt...")
                    time.sleep(120)
                    continue

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
                    # Reduced limit from 200 to 100 to conserve memory
                    kline_data_cache[sym_key] = fetch_klines_sync(sym_key, CONFIG["TIMEFRAME"], 100)
                except Exception as e:
                    log.error(f"Failed to pre-fetch klines for {sym_key} in monitor loop: {e}")
                    kline_data_cache[sym_key] = None # Mark as failed

            to_remove = []
            for tid, meta in trades_snapshot.items():
                sym = meta['symbol']
                pos = next((p for p in positions if p.get('symbol') == sym), None)
                if not pos:
                    continue
                
                pos_amt = float(pos.get('positionAmt') or 0.0)
                unreal = float(pos.get('unRealizedProfit') or 0.0)
                
                managed_trades_lock.acquire()
                try:
                    if tid in managed_trades:
                        managed_trades[tid]['unreal'] = unreal
                finally:
                    managed_trades_lock.release()

                # Check for closed positions
                if abs(pos_amt) < 1e-8:
                    close_time = datetime.utcnow().replace(tzinfo=timezone.utc)
                    meta['close_time'] = close_time.isoformat()
                    record_trade({
                        'id': meta['id'], 'symbol': meta['symbol'], 'side': meta['side'],
                        'entry_price': meta['entry_price'], 'exit_price': float(pos.get('entryPrice') or 0.0),
                        'qty': meta['initial_qty'], 'notional': meta['notional'], 'pnl': unreal,
                        'open_time': meta['open_time'], 'close_time': meta['close_time'],
                        'risk_usdt': meta.get('risk_usdt', 0.0) # Add risk, with a default for old trades
                    })
                    remove_managed_trade_from_db(tid)
                    managed_trades_lock.acquire()
                    try:
                        last_trade_close_time[sym] = close_time
                    finally:
                        managed_trades_lock.release()
                    # Re-enabled trade close notification per user request, with improved formatting
                    close_msg = (
                        f"✅ *Trade Closed*\n\n"
                        f"**ID:** `{meta['id']}`\n"
                        f"**Symbol:** {sym}\n"
                        f"**PnL:** `{unreal:.4f} USDT`"
                    )
                    send_telegram(close_msg, parse_mode='Markdown')
                    to_remove.append(tid)
                    continue
                
                # Get the pre-fetched kline data for this trade's symbol
                df_monitor = kline_data_cache.get(sym)
                if df_monitor is None or df_monitor.empty:
                    log.warning(f"Skipping monitoring cycle for {tid} due to missing kline data.")
                    continue

                # --- Break-Even Auto-Move Logic ---
                if CONFIG.get("BE_AUTO_MOVE_ENABLED", True) and not meta.get('be_moved') and meta.get('trade_phase', 0) == 0:
                    try:
                        atr_now_be = atr(df_monitor, CONFIG["ATR_LENGTH"]).iloc[-1]
                        current_price_be = df_monitor['close'].iloc[-1]

                        entry_price = meta['entry_price']
                        side = meta['side']
                        
                        profit_target_price = entry_price + atr_now_be if side == 'BUY' else entry_price - atr_now_be
                        
                        moved_to_be = False
                        if (side == 'BUY' and current_price_be >= profit_target_price and entry_price > meta['sl']) or \
                           (side == 'SELL' and current_price_be <= profit_target_price and entry_price < meta['sl']):
                            
                            cancel_trade_sltp_orders_sync(meta)
                            new_orders = place_batch_sl_tp_sync(sym, side, sl_price=entry_price, tp_price=meta['tp'])
                            moved_to_be = True

                        if moved_to_be:
                            log.info(f"Trade {tid} hit 1x ATR profit target. Moving SL to breakeven at {entry_price}.")
                            
                            with managed_trades_lock:
                                if tid in managed_trades:
                                    managed_trades[tid]['sl'] = entry_price
                                    managed_trades[tid]['be_moved'] = True
                                    managed_trades[tid]['sltp_orders'] = new_orders
                                    add_managed_trade_to_db(managed_trades[tid])
                                
                            send_telegram(f"🔒 SL moved to breakeven for Trade ID: {tid}")
                            continue
                    except Exception as e:
                        log_and_send_error(f"Failed to process Break-Even logic for {tid}", e)

                # Dynamic SL/TP Logic
                if meta.get('dyn_sltp'):
                    try:
                        current_price = df_monitor['close'].iloc[-1]
                        
                        # Check for TP1
                        if meta.get('trade_phase') == 0 and meta.get('tp1') is not None:
                            hit_tp1 = (meta['side'] == 'BUY' and current_price >= meta['tp1']) or \
                                      (meta['side'] == 'SELL' and current_price <= meta['tp1'])
                            if hit_tp1:
                                log.info(f"Trade {tid} hit TP1 at {meta['tp1']}.")
                                qty_to_close = meta['initial_qty'] * CONFIG['TP1_CLOSE_PCT']
                                qty_to_close = round_qty(sym, qty_to_close)
                                
                                if qty_to_close > 0:
                                    close_partial_market_position_sync(sym, meta['side'], qty_to_close)
                                
                                new_qty = meta['qty'] - qty_to_close
                                cancel_trade_sltp_orders_sync(meta)
                                new_sl = meta['entry_price']
                                new_orders = place_batch_sl_tp_sync(sym, meta['side'], sl_price=new_sl, tp_price=meta['tp3'])
                                
                                with managed_trades_lock:
                                    if tid in managed_trades:
                                        managed_trades[tid]['trade_phase'] = 1
                                        managed_trades[tid]['qty'] = new_qty
                                        managed_trades[tid]['sl'] = new_sl
                                        managed_trades[tid]['sltp_orders'] = new_orders
                                        add_managed_trade_to_db(managed_trades[tid])
                                send_telegram(f"✅ TP1 hit for {tid} ({sym}). Closed {CONFIG['TP1_CLOSE_PCT']*100}%. SL moved to breakeven.")
                                continue

                        # Check for TP2
                        if meta.get('trade_phase') == 1 and meta.get('tp2') is not None:
                            hit_tp2 = (meta['side'] == 'BUY' and current_price >= meta['tp2']) or \
                                      (meta['side'] == 'SELL' and current_price <= meta['tp2'])
                            if hit_tp2:
                                log.info(f"Trade {tid} hit TP2 at {meta['tp2']}.")
                                qty_to_close = meta['initial_qty'] * CONFIG['TP2_CLOSE_PCT']
                                qty_to_close = round_qty(sym, qty_to_close)

                                if qty_to_close > 0:
                                    close_partial_market_position_sync(sym, meta['side'], qty_to_close)
                                
                                new_qty = meta['qty'] - qty_to_close
                                cancel_trade_sltp_orders_sync(meta)
                                new_sl = meta['tp1']
                                new_orders = place_batch_sl_tp_sync(sym, meta['side'], sl_price=new_sl)

                                with managed_trades_lock:
                                    if tid in managed_trades:
                                        managed_trades[tid]['trade_phase'] = 2
                                        managed_trades[tid]['qty'] = new_qty
                                        managed_trades[tid]['sl'] = new_sl
                                        managed_trades[tid]['sltp_orders'] = new_orders
                                        add_managed_trade_to_db(managed_trades[tid])
                                send_telegram(f"✅ TP2 hit for {tid} ({sym}). Closed {CONFIG['TP2_CLOSE_PCT']*100}%. SL moved to TP1. Trailing stop is active.")
                                continue
                    except Exception as e:
                        log_and_send_error(f"Failed to process Dynamic SL/TP logic for {tid}", e)

                # Trailing SL logic
                if meta.get('trailing') and not meta.get('be_moved'):
                    try:
                        atr_now = atr(df_monitor, CONFIG["ATR_LENGTH"]).iloc[-1]
                        current_price = df_monitor['close'].iloc[-1]
                        moved = False
                        new_sl = None
                        new_orders = None

                        if meta['side'] == 'BUY' and current_price > meta['entry_price'] + 1.0 * atr_now:
                            new_sl = meta['entry_price'] + 0.5 * atr_now
                            if new_sl > meta['sl'] and new_sl < current_price:
                                cancel_trade_sltp_orders_sync(meta)
                                new_orders = place_batch_sl_tp_sync(sym, meta['side'], sl_price=new_sl, tp_price=meta['tp'])
                                moved = True
                        elif meta['side'] == 'SELL' and current_price < meta['entry_price'] - 1.0 * atr_now:
                            new_sl = meta['entry_price'] - 0.5 * atr_now
                            if new_sl < meta['sl'] and new_sl > current_price:
                                cancel_trade_sltp_orders_sync(meta)
                                new_orders = place_batch_sl_tp_sync(sym, meta['side'], sl_price=new_sl, tp_price=meta['tp'])
                                moved = True
                        
                        if moved and new_sl is not None:
                            with managed_trades_lock:
                                if tid in managed_trades:
                                    managed_trades[tid]['sl'] = new_sl
                                    managed_trades[tid]['sltp_last_updated'] = datetime.utcnow().isoformat()
                                    if new_orders:
                                        managed_trades[tid]['sltp_orders'] = new_orders
                                    add_managed_trade_to_db(managed_trades[tid])
                            meta['sl'] = new_sl
                            sl_update_msg = (
                                f"📈 *Trailing Stop Update*\n\n"
                                f"**ID:** `{meta['id']}` ({sym})\n"
                                f"**New SL:** `{new_sl:.6f}`"
                            )
                            send_telegram(sl_update_msg, parse_mode='Markdown')
                    except Exception as e:
                        log_and_send_error(f"Failed to process Trailing SL for {tid}", e)

            if to_remove:
                managed_trades_lock.acquire()
                try:
                    for tid in to_remove:
                        managed_trades.pop(tid, None)
                finally:
                    managed_trades_lock.release()

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
    Checks session freeze status and sends notifications on state changes.
    Returns True if the bot should be frozen, False otherwise.
    Manual override is possible via `session_freeze_override` global.
    """
    global session_freeze_active, notified_frozen_session, session_freeze_override

    # Check for manual override first
    if session_freeze_override:
        # If a session freeze was active, ensure it's cleared so it doesn't pop up again
        if session_freeze_active:
            session_freeze_active = False
            notified_frozen_session = None # Clear this to prevent the "ended" message
            log.info("Session freeze is currently overridden by user command.")
        return False # Report "not frozen" because of the override

    now_utc = datetime.now(timezone.utc)
    is_frozen, session_name = get_session_freeze_status(now_utc)

    if is_frozen:
        # If we are entering a NEW freeze window
        if not session_freeze_active or notified_frozen_session != session_name:
            log.info(f"Entering freeze period for {session_name} session. Bot will not open new trades.")
            # When a new natural freeze starts, any previous override is reset.
            session_freeze_override = False
            await asyncio.to_thread(send_telegram, f"⚠️ Session Change: {session_name}\\nThe bot is now frozen for 2 hours and will not open new trades. Existing trades are still monitored.")
            session_freeze_active = True
            notified_frozen_session = session_name
        return True # Is frozen
    else:
        # If we are exiting a freeze window
        if session_freeze_active:
            log.info("Exiting session freeze period. Bot is now active for new trades.")
            await asyncio.to_thread(send_telegram, f"✅ Session freeze for {notified_frozen_session} has ended. The bot is now active again.")
            session_freeze_active = False
            notified_frozen_session = None
        return False # Is not frozen


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
            symbols = [s.strip().upper() for s in CONFIG["SYMBOLS"]]
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

def _generate_pnl_report_sync(query: str, params: tuple, title: str) -> (str, Optional[bytes]):
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

        df['kama'] = kama(df['close'], CONFIG["KAMA_LENGTH"], CONFIG["KAMA_FAST"], CONFIG["KAMA_SLOW"])

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

        kama_plot = mpf.make_addplot(df['kama'], color='purple', width=0.7)
        addplots.insert(0, kama_plot)
        
        fig, axes = mpf.plot(
            df,
            type='candle',
            style='yahoo',
            title=f'{symbol} Chart with KAMA and Trades',
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

def build_control_keyboard():
    buttons = [
        [KeyboardButton("/startbot"), KeyboardButton("/stopbot")],
        [KeyboardButton("/freeze"), KeyboardButton("/unfreeze")],
        [KeyboardButton("/listorders"), KeyboardButton("/sessions")],
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
                session_status_text = f"⏰ Session Freeze: *{session_freeze_active}*"
                if session_freeze_active:
                    session_status_text += f" ({notified_frozen_session})"
                if session_freeze_override:
                    session_status_text += " (Overridden)"
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
