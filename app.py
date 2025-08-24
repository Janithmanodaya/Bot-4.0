# app.py
"""
KAMA trend-following bot — complete version with fixes:
 - DualLock for cross-thread locking
 - Exchange info cache to avoid repeated futures_exchange_info calls
 - Monitor thread persists unrealized PnL and SL updates back to managed_trades
 - Telegram thread with commands and Inline Buttons; includes /forceip
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
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from decimal import Decimal, ROUND_DOWN, getcontext

import requests
import numpy as np
import pandas as pd
from fastapi import FastAPI

from binance.client import Client
from binance.exceptions import BinanceAPIException

import telegram
from telegram import ReplyKeyboardMarkup, KeyboardButton

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
    "SYMBOLS": os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(","),
    "TIMEFRAME": os.getenv("TIMEFRAME", "1h"),
    "BIG_TIMEFRAME": os.getenv("BIG_TIMEFRAME", "4h"),

    "SCAN_INTERVAL": int(os.getenv("SCAN_INTERVAL", "20")),
    "MAX_CONCURRENT_TRADES": int(os.getenv("MAX_CONCURRENT_TRADES", "3")),
    "START_MODE": os.getenv("START_MODE", "running").lower(),

    "KAMA_LENGTH": int(os.getenv("KAMA_LENGTH", "10")),
    "KAMA_FAST": int(os.getenv("KAMA_FAST", "2")),
    "KAMA_SLOW": int(os.getenv("KAMA_SLOW", "30")),

    "ATR_LENGTH": int(os.getenv("ATR_LENGTH", "14")),
    "SL_TP_ATR_MULT": float(os.getenv("SL_TP_ATR_MULT", "2.5")),

    "RISK_SMALL_BALANCE_THRESHOLD": float(os.getenv("RISK_SMALL_BALANCE_THRESHOLD", "50.0")),
    "RISK_SMALL_FIXED_USDT": float(os.getenv("RISK_SMALL_FIXED_USDT", "0.5")),
    "RISK_PCT_LARGE": float(os.getenv("RISK_PCT_LARGE", "0.02")),
    "MAX_RISK_USDT": float(os.getenv("MAX_RISK_USDT", "0.0")),  # 0 disables cap

    "ADX_LENGTH": int(os.getenv("ADX_LENGTH", "14")),
    "ADX_THRESHOLD": float(os.getenv("ADX_THRESHOLD", "50.0")),

    "CHOP_LENGTH": int(os.getenv("CHOP_LENGTH", "14")),
    "CHOP_THRESHOLD": float(os.getenv("CHOP_THRESHOLD", "50.0")),

    "BB_LENGTH": int(os.getenv("BB_LENGTH", "20")),
    "BB_STD": float(os.getenv("BB_STD", "2.0")),
    "BBWIDTH_THRESHOLD": float(os.getenv("BBWIDTH_THRESHOLD", "7.0")),

    "MIN_CANDLES_AFTER_CLOSE": int(os.getenv("MIN_CANDLES_AFTER_CLOSE", "10")),

    "TRAILING_ENABLED": os.getenv("TRAILING_ENABLED", "true").lower() in ("true", "1", "yes"),

    "DB_FILE": os.getenv("DB_FILE", "trades.db"),
    
    "DRY_RUN": os.getenv("DRY_RUN", "false").lower() in ("true", "1", "yes"),
}

running = (CONFIG["START_MODE"] == "running")
frozen = False

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

    async def __aenter__(self):
        await asyncio.to_thread(self._lock.acquire)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self._lock.release()

managed_trades: Dict[str, Dict[str, Any]] = {}
managed_trades_lock = DualLock()  # used by both async and sync code

last_trade_close_time: Dict[str, datetime] = {}

telegram_thread: Optional[threading.Thread] = None
monitor_thread_obj: Optional[threading.Thread] = None
monitor_stop_event = threading.Event()

scan_task: Optional[asyncio.Task] = None

# Exchange info cache
EXCHANGE_INFO_CACHE = {"ts": 0.0, "data": None, "ttl": 300}  # ttl seconds

# -------------------------
# App Lifespan Manager
# -------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global scan_task, telegram_thread, monitor_thread_obj, client, monitor_stop_event, main_loop
    log.info("KAMA strategy bot starting up...")
    
    # --- Startup Logic ---
    init_db()

    main_loop = asyncio.get_running_loop()

    ok, err = await asyncio.to_thread(init_binance_client_sync)
    await asyncio.to_thread(validate_and_sanity_check_sync, True)

    if client is not None:
        scan_task = main_loop.create_task(scanning_loop())
        monitor_stop_event.clear()
        monitor_thread_obj = threading.Thread(target=monitor_thread_func, daemon=True)
        monitor_thread_obj.start()
        log.info("Started monitor thread.")
    else:
        log.warning("Binance client not initialized -> scanning and monitor thread not started.")

    if telegram_bot:
        telegram_thread = threading.Thread(target=telegram_polling_thread, args=(main_loop,), daemon=True)
        telegram_thread.start()
        log.info("Started telegram polling thread.")
    else:
        log.info("Telegram not configured; telegram thread not started.")
    
    try:
        await send_telegram("KAMA strategy bot started. Running={}".format(running))
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

    monitor_stop_event.set()
    if monitor_thread_obj and monitor_thread_obj.is_alive():
        monitor_thread_obj.join(timeout=5)
    
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

def get_public_ip() -> str:
    try:
        return requests.get("https://api.ipify.org", timeout=5).text
    except Exception:
        return "unable-to-fetch-ip"

async def send_telegram(msg: str, document_content: Optional[bytes] = None, document_name: str = "error.html"):
    """
    Asynchronously sends a message to Telegram. Can optionally attach a document.
    """
    if not telegram_bot or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured; message: %s", msg[:200])
        return
    
    safe_msg = _shorten_for_telegram(msg)
    try:
        if document_content:
            await telegram_bot.send_document(
                chat_id=int(TELEGRAM_CHAT_ID),
                document=document_content,
                filename=document_name,
                caption=safe_msg
            )
        else:
            await telegram_bot.send_message(
                chat_id=int(TELEGRAM_CHAT_ID), 
                text=safe_msg
            )
    except Exception:
        log.exception("Failed to send telegram message (async)")

def send_telegram_sync(msg: str, document_content: Optional[bytes] = None, document_name: str = "error.html"):
    """
    Schedules a message to be sent from a synchronous thread.
    """
    if not main_loop or not main_loop.is_running():
        log.warning("Event loop not ready for sync telegram message. Message: %s", msg[:100])
        return

    # Fire and forget on the running loop
    asyncio.run_coroutine_threadsafe(
        send_telegram(msg, document_content=document_content, document_name=document_name), 
        main_loop
    )

# (The rest of the file from DB Helpers to the end remains the same, except for removing the old startup/shutdown events)
# ... I will paste the full code below ...

# -------------------------
# DB helpers
# -------------------------
def init_db():
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS trades (
        id TEXT PRIMARY KEY,
        symbol TEXT,
        side TEXT,
        entry_price REAL,
        exit_price REAL,
        qty REAL,
        notional REAL,
        pnl REAL,
        open_time TEXT,
        close_time TEXT
    )
    """)
    conn.commit()
    conn.close()

def record_trade(rec: Dict[str, Any]):
    conn = sqlite3.connect(CONFIG["DB_FILE"])
    cur = conn.cursor()
    cur.execute("""
    INSERT OR REPLACE INTO trades (id,symbol,side,entry_price,exit_price,qty,notional,pnl,open_time,close_time)
    VALUES (?,?,?,?,?,?,?,?,?,?)
    """, (rec['id'], rec['symbol'], rec['side'], rec['entry_price'], rec.get('exit_price'),
          rec['qty'], rec['notional'], rec.get('pnl'), rec['open_time'], rec.get('close_time')))
    conn.commit()
    conn.close()

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
# Binance init (sync)
# -------------------------

def init_binance_client_sync():
    """
    Initialize Binance client only when API key + secret are provided.
    Returns (ok: bool, error_message: str)
    """
    global client, BINANCE_API_KEY, BINANCE_API_SECRET, USE_TESTNET
    if not BINANCE_API_KEY or not BINANCE_API_SECRET:
        log.warning("Binance API key/secret not set; Binance client will not be initialized.")
        client = None
        return False, "Missing BINANCE_API_KEY or BINANCE_API_SECRET"

    try:
        client = Client(BINANCE_API_KEY, BINANCE_API_SECRET)
        log.info("Binance client in MAINNET mode (forced).")
        try:
            client.ping()
            log.info("Connected to Binance API (ping ok).")
        except Exception:
            log.warning("Binance ping failed (connection may still succeed for calls).")
        
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
            send_telegram_sync(f"Binance init failed: {e}\nServer IP: {ip}\nPlease update IP in Binance API whitelist if needed.")
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

def open_market_position_sync(symbol: str, side: str, qty: float, leverage: int):
    global client
    if CONFIG["DRY_RUN"]:
        log.info(f"[DRY RUN] Would open {side} position for {qty} {symbol} with {leverage}x leverage.")
        # Return a mock order response
        return {
            "orderId": f"dryrun_{int(time.time())}",
            "symbol": symbol,
            "status": "FILLED",
            "side": side,
            "type": "MARKET",
            "origQty": qty,
            "executedQty": qty,
            "cumQuote": "0",
            "avgPrice": "0"
        }

    if client is None:
        raise RuntimeError("Binance client not initialized")
    
    try:
        log.info(f"Attempting to change leverage to {leverage}x for {symbol}")
        client.futures_change_leverage(symbol=symbol, leverage=leverage)
    except Exception as e:
        log.warning("Failed to change leverage (non-fatal, may use previous leverage): %s", e)

    try:
        log.info(f"Placing real market order: {side} {qty} {symbol}")
        order = client.futures_create_order(symbol=symbol, side=side, type='MARKET', quantity=qty)
        return order
    except BinanceAPIException as e:
        log.exception("BinanceAPIException opening position: %s", e)
        raise
    except Exception as e:
        log.exception("Exception opening position: %s", e)
        raise

def place_sl_tp_orders_sync(symbol: str, side: str, stop_price: float, take_price: float):
    global client
    if CONFIG["DRY_RUN"]:
        log.info(f"[DRY RUN] Would place SL at {stop_price:.4f} and TP at {take_price:.4f} for {symbol}.")
        # Return a mock response
        return {
            "stop_order": {"orderId": f"dryrun_sl_{int(time.time())}"},
            "tp_order": {"orderId": f"dryrun_tp_{int(time.time())}"}
        }

    out = {}
    if client is None:
        out['error'] = "client not initialized"
        return out
    try:
        out['stop_order'] = client.futures_create_order(
            symbol=symbol,
            side='SELL' if side == 'BUY' else 'BUY',
            type='STOP_MARKET',
            stopPrice=round(stop_price, 8),
            closePosition=True,
            reduceOnly=True
        )
    except Exception as e:
        out['stop_error'] = str(e)
        log.exception("Failed to create stop loss order: %s", e)
    try:
        out['tp_order'] = client.futures_create_order(
            symbol=symbol,
            side='SELL' if side == 'BUY' else 'BUY',
            type='TAKE_PROFIT_MARKET',
            stopPrice=round(take_price, 8),
            closePosition=True,
            reduceOnly=True
        )
    except Exception as e:
        out['tp_error'] = str(e)
        log.exception("Failed to create take profit order: %s", e)
    return out

def cancel_close_orders_sync(symbol: str):
    global client
    if CONFIG["DRY_RUN"]:
        log.info(f"[DRY RUN] Would cancel all open SL/TP orders for {symbol}.")
        return

    if client is None:
        return
    try:
        orders = client.futures_get_open_orders(symbol=symbol)
        for o in orders:
            if o.get('type') in ['STOP_MARKET', 'TAKE_PROFIT_MARKET'] or o.get('closePosition'):
                try:
                    client.futures_cancel_order(symbol=symbol, orderId=o['orderId'])
                except Exception:
                    log.exception("Failed to cancel order %s", o.get('orderId'))
    except Exception:
        log.exception("Error canceling close orders: %s", symbol)

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
        send_telegram_sync(report_text)
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
        trend_small = 'bull' if (kama_now - kama_prev) > 0 else 'bear'
        df_big = await asyncio.to_thread(fetch_klines_sync, symbol, CONFIG["BIG_TIMEFRAME"], 200)
        df_big['kama'] = kama(df_big['close'], CONFIG["KAMA_LENGTH"], CONFIG["KAMA_FAST"], CONFIG["KAMA_SLOW"])
        trend_big = 'bull' if (df_big['kama'].iloc[-1] - df_big['kama'].iloc[-2]) > 0 else 'bear'
        if adx_now < CONFIG["ADX_THRESHOLD"]:
            log.debug("%s skip: ADX %.2f < %.2f", symbol, adx_now, CONFIG["ADX_THRESHOLD"]); return
        if chop_now >= CONFIG["CHOP_THRESHOLD"]:
            log.debug("%s skip: CHOP %.2f >= %.2f", symbol, chop_now, CONFIG["CHOP_THRESHOLD"]); return
        if (bbw_now * 100.0) >= CONFIG["BBWIDTH_THRESHOLD"]:
            log.debug("%s skip: BBwidth*100 %.4f >= %.2f", symbol, (bbw_now * 100.0), CONFIG["BBWIDTH_THRESHOLD"]); return
        if trend_small != trend_big:
            log.debug("%s skip: trend mismatch small=%s big=%s", symbol, trend_small, trend_big); return
        crossed_above = (prev['close'] <= prev['kama']) and (last['close'] > kama_now)
        crossed_below = (prev['close'] >= prev['kama']) and (last['close'] < kama_now)
        if not (crossed_above or crossed_below):
            return
        side = None
        if crossed_above and trend_small == 'bull':
            side = 'BUY'
        elif crossed_below and trend_small == 'bear':
            side = 'SELL'
        if not side:
            return
        async with managed_trades_lock:
            if len(managed_trades) >= CONFIG["MAX_CONCURRENT_TRADES"]:
                log.debug("Max concurrent trades reached")
                return
        async with managed_trades_lock:
            last_close = last_trade_close_time.get(symbol)
        if last_close:
            n_since = candles_since_close(df, last_close)
            if n_since < CONFIG["MIN_CANDLES_AFTER_CLOSE"]:
                log.info("%s skip due to cooldown: %d/%d", symbol, n_since, CONFIG["MIN_CANDLES_AFTER_CLOSE"])
                return
        sl_distance = CONFIG["SL_TP_ATR_MULT"] * atr_now
        if sl_distance <= 0 or math.isnan(sl_distance):
            return
        stop_price = price - sl_distance if side == 'BUY' else price + sl_distance
        take_price = price + sl_distance if side == 'BUY' else price - sl_distance
        balance = await asyncio.to_thread(get_account_balance_usdt)
        risk_usdt = calculate_risk_amount(balance)
        if risk_usdt <= 0:
            log.warning("Risk amount non-positive"); return
        price_distance = abs(price - stop_price)
        if price_distance <= 0:
            return
        qty = risk_usdt / price_distance
        qty = await asyncio.to_thread(round_qty, symbol, qty)
        if qty <= 0:
            log.warning("Qty rounded to zero; skipping"); return
        notional = qty * price
        target_initial_margin = risk_usdt
        leverage = max(1, int(min(get_max_leverage(symbol), math.floor(notional / max(target_initial_margin, 1e-8)))))
        if notional <= target_initial_margin:
            leverage = 1
        try:
            order = await asyncio.to_thread(open_market_position_sync, symbol, side, qty, leverage)
            order_id = str(order.get('orderId', f"mkt_{int(time.time())}"))
            sltp = await asyncio.to_thread(place_sl_tp_orders_sync, symbol, side, stop_price, take_price)
            trade_id = f"{symbol}_{order_id}"
            meta = {
                "id": trade_id, "symbol": symbol, "side": side, "entry_price": price,
                "qty": qty, "notional": notional, "leverage": leverage,
                "sl": stop_price, "tp": take_price, "open_time": datetime.utcnow().isoformat(),
                "sltp_orders": sltp, "trailing": CONFIG["TRAILING_ENABLED"]
            }
            async with managed_trades_lock:
                managed_trades[trade_id] = meta
            record_trade({'id': trade_id, 'symbol': symbol, 'side': side, 'entry_price': price,
                          'exit_price': None, 'qty': qty, 'notional': notional, 'pnl': None,
                          'open_time': meta['open_time'], 'close_time': None})
            dry_run_prefix = "[DRY RUN] " if CONFIG["DRY_RUN"] else ""
            await send_telegram(f"{dry_run_prefix}Opened {side} on {symbol}\nEntry: {price:.4f}\nQty: {qty}\nLeverage: {leverage}\nRisk: {risk_usdt:.4f} USDT\nSL: {stop_price:.4f}\nTP: {take_price:.4f}\nTrade ID: {trade_id}")
            log.info("%sOpened trade: %s", dry_run_prefix, meta)
        except Exception as e:
            log.exception("Failed to open trade for %s: %s", symbol, e)
            tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            safe_tb = _shorten_for_telegram(tb)
            await send_telegram(f"ERROR opening {symbol}: {e}\nTrace:\n{safe_tb}\nServer IP: {get_public_ip()}")
            running = False
        return
    except Exception as e:
        log.exception("evaluate_and_enter error %s: %s", symbol, e)
        tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        safe_tb = _shorten_for_telegram(tb)
        await send_telegram(f"Scan error for {symbol}: {e}\nTrace:\n{safe_tb}\nServer IP: {get_public_ip()}")

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
    global managed_trades, last_trade_close_time, running
    log.info("Monitor thread started.")
    while not monitor_stop_event.is_set():
        try:
            if client is None:
                time.sleep(5)
                continue
            try:
                positions = client.futures_position_information()
            except BinanceAPIException as e:
                log.error("Caught Binance API error in monitor thread. Type: %s. See Telegram for details.", type(e).__name__)
                ip = get_public_ip()
                is_html_error = e.text and e.text.strip().lower().startswith('<!doctype html>')

                if is_html_error:
                    # Binance returned an HTML page instead of JSON
                    error_msg = f"Binance API returned an HTML error page. This could be an IP ban, a WAF block, or a server issue.\nServer IP: {ip}"
                    html_content = e.text.encode('utf-8')
                    send_telegram_sync(error_msg, document_content=html_content, document_name="binance_error.html")
                else:
                    # A different kind of API error
                    tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                    safe_tb = _shorten_for_telegram(tb)
                    error_msg = f"Binance API Error fetching positions: {e}\nTrace:\n{safe_tb}\nServer IP: {ip}"
                    send_telegram_sync(error_msg)
                
                running = False
                log.info("Pausing bot due to Binance API error.")
                time.sleep(30) # Sleep longer on API errors
                continue
            except Exception as e:
                log.exception("Error fetching positions in monitor thread: %s", e)
                tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
                safe_tb = _shorten_for_telegram(tb)
                send_telegram_sync(f"Error fetching positions: {e}\nTrace:\n{safe_tb}\nServer IP: {get_public_ip()}")
                running = False
                time.sleep(10)
                continue
            managed_trades_lock.acquire()
            try:
                trades_snapshot = dict(managed_trades)
            finally:
                managed_trades_lock.release()
            to_remove = []
            for tid, meta in trades_snapshot.items():
                try:
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
                    if abs(pos_amt) < 1e-8:
                        close_time = datetime.utcnow().replace(tzinfo=timezone.utc)
                        meta['close_time'] = close_time.isoformat()
                        record_trade({'id': meta['id'], 'symbol': meta['symbol'], 'side': meta['side'],
                                      'entry_price': meta['entry_price'], 'exit_price': float(pos.get('entryPrice') or 0.0),
                                      'qty': meta['qty'], 'notional': meta['notional'], 'pnl': unreal,
                                      'open_time': meta['open_time'], 'close_time': meta['close_time']})
                        managed_trades_lock.acquire()
                        try:
                            last_trade_close_time[sym] = close_time
                        finally:
                            managed_trades_lock.release()
                        send_telegram_sync(f"Trade closed {meta['id']} on {sym}\nPNL: {unreal:.6f} USDT\nCooldown: {CONFIG['MIN_CANDLES_AFTER_CLOSE']} candles")
                        to_remove.append(tid)
                        continue
                    if meta.get('trailing'):
                        try:
                            df = fetch_klines_sync(sym, CONFIG["TIMEFRAME"], 200)
                            atr_now = atr(df, CONFIG["ATR_LENGTH"]).iloc[-1]
                            current_price = df['close'].iloc[-1]
                            moved = False
                            new_sl = None
                            if meta['side'] == 'BUY':
                                if current_price > meta['entry_price'] + 1.0 * atr_now:
                                    new_sl = meta['entry_price'] + 0.5 * atr_now
                                    if new_sl > meta['sl'] and new_sl < current_price:
                                        cancel_close_orders_sync(sym)
                                        place_sl_tp_orders_sync(sym, meta['side'], new_sl, meta['tp'])
                                        moved = True
                            else:
                                if current_price < meta['entry_price'] - 1.0 * atr_now:
                                    new_sl = meta['entry_price'] - 0.5 * atr_now
                                    if new_sl < meta['sl'] and new_sl > current_price:
                                        cancel_close_orders_sync(sym)
                                        place_sl_tp_orders_sync(sym, meta['side'], new_sl, meta['tp'])
                                        moved = True
                            if moved and new_sl is not None:
                                managed_trades_lock.acquire()
                                try:
                                    if tid in managed_trades:
                                        managed_trades[tid]['sl'] = new_sl
                                        managed_trades[tid]['sltp_last_updated'] = datetime.utcnow().isoformat()
                                finally:
                                    managed_trades_lock.release()
                                meta['sl'] = new_sl
                                send_telegram_sync(f"Adjusted SL for {meta['id']} ({sym}) -> {new_sl:.6f}")
                        except Exception:
                            log.exception("Trailing internal error in monitor thread")
                except Exception:
                    log.exception("Error handling trade in monitor thread")
            if to_remove:
                managed_trades_lock.acquire()
                try:
                    for tid in to_remove:
                        managed_trades.pop(tid, None)
                finally:
                    managed_trades_lock.release()
            time.sleep(5)
        except Exception:
            log.exception("Unhandled exception in monitor thread")
            time.sleep(5)
    log.info("Monitor thread exiting.")

async def scanning_loop():
    while True:
        try:
            if not running:
                await asyncio.sleep(2)
                continue
            symbols = [s.strip().upper() for s in CONFIG["SYMBOLS"]]
            for s in symbols:
                try:
                    await evaluate_and_enter(s)
                except Exception:
                    log.exception("Error in evaluate loop for %s", s)
                await asyncio.sleep(max(0.5, CONFIG["SCAN_INTERVAL"] / max(1, len(symbols))))
        except asyncio.CancelledError:
            log.info("Scanning loop cancelled.")
            break
        except Exception:
            log.exception("Scanning loop error")
        await asyncio.sleep(CONFIG["SCAN_INTERVAL"])

async def get_managed_trades_snapshot():
    async with managed_trades_lock:
        return dict(managed_trades)

def build_control_keyboard():
    buttons = [
        [KeyboardButton("/startbot"), KeyboardButton("/stopbot")],
        [KeyboardButton("/freeze"), KeyboardButton("/unfreeze")],
        [KeyboardButton("/listorders"), KeyboardButton("/showparams")],
        [KeyboardButton("/status")]
    ]
    return ReplyKeyboardMarkup(buttons, resize_keyboard=True)

def handle_update_sync(update, loop):
    try:
        if update is None:
            return
        if getattr(update, 'message', None):
            msg = update.message
            text = (msg.text or "").strip()
            if text.startswith("/startbot"):
                fut = asyncio.run_coroutine_threadsafe(_set_running(True), loop)
                try: fut.result(timeout=5)
                except Exception as e: log.error("Failed to execute /startbot action: %s", e)
                send_telegram_sync("Bot state -> RUNNING")
            elif text.startswith("/stopbot"):
                fut = asyncio.run_coroutine_threadsafe(_set_running(False), loop)
                try: fut.result(timeout=5)
                except Exception as e: log.error("Failed to execute /stopbot action: %s", e)
                send_telegram_sync("Bot state -> STOPPED")
            elif text.startswith("/freeze"):
                fut = asyncio.run_coroutine_threadsafe(_set_frozen(True), loop)
                try: fut.result(timeout=5)
                except Exception as e: log.error("Failed to execute /freeze action: %s", e)
                send_telegram_sync("Bot -> FROZEN (no new entries)")
            elif text.startswith("/unfreeze"):
                fut = asyncio.run_coroutine_threadsafe(_set_frozen(False), loop)
                try: fut.result(timeout=5)
                except Exception as e: log.error("Failed to execute /unfreeze action: %s", e)
                send_telegram_sync("Bot -> UNFROZEN")
            elif text.startswith("/status"):
                fut = asyncio.run_coroutine_threadsafe(get_managed_trades_snapshot(), loop)
                trades = {}
                try: trades = fut.result(timeout=5)
                except Exception as e: log.error("Failed to get managed trades for /status: %s", e)
                txt = f"Status:\nrunning={running}\nfrozen={frozen}\nmanaged_trades={len(trades)}"
                send_telegram_sync(txt)
                try:
                    coro = telegram_bot.send_message(chat_id=int(TELEGRAM_CHAT_ID), text="Controls:", reply_markup=build_control_keyboard())
                    asyncio.run_coroutine_threadsafe(coro, loop)
                except Exception:
                    log.exception("Failed to schedule telegram keyboard")
            elif text.startswith("/ip") or text.startswith("/forceip"):
                ip = get_public_ip()
                send_telegram_sync(f"Server IP: {ip}")
            elif text.startswith("/listorders"):
                fut = asyncio.run_coroutine_threadsafe(get_managed_trades_snapshot(), loop)
                trades = {}
                try: trades = fut.result(timeout=5)
                except Exception: pass
                if not trades:
                    send_telegram_sync("No managed trades.")
                else:
                    out = "Open trades:\n"
                    for k, v in trades.items():
                        unreal = v.get('unreal')
                        unreal_str = "N/A" if unreal is None else f"{float(unreal):.6f}"
                        out += f"{k} {v['symbol']} {v['side']} entry={v['entry_price']:.4f} qty={v['qty']} sl={v['sl']:.4f} tp={v['tp']:.4f} unreal={unreal_str}\n"
                    send_telegram_sync(out)
            elif text.startswith("/showparams"):
                out = "Current runtime params:\n"
                for k,v in CONFIG.items():
                    out += f"{k} = {v}\n"
                send_telegram_sync(out)
            elif text.startswith("/setparam"):
                parts = text.split()
                if len(parts) >= 3:
                    key = parts[1]
                    val = " ".join(parts[2:])
                    if key not in CONFIG:
                        send_telegram_sync(f"Parameter {key} not found.")
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
                            send_telegram_sync(f"Set {key} = {CONFIG[key]}")
                        except Exception as e:
                            send_telegram_sync(f"Failed to set {key}: {e}")
                else:
                    send_telegram_sync("Usage: /setparam KEY VALUE")
            elif text.startswith("/validate"):
                result = validate_and_sanity_check_sync(send_report=False)
                send_telegram_sync("Validation result: " + ("OK" if result["ok"] else "ERROR"))
                for c in result["checks"]:
                    send_telegram_sync(f"{c['type']}: ok={c['ok']} detail={c.get('detail')}")
            else:
                send_telegram_sync("Unknown command. Use /status to see the keyboard.")
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
            coro = telegram_bot.get_updates(offset=offset, timeout=20)
            future = asyncio.run_coroutine_threadsafe(coro, loop)
            updates = future.result(timeout=30)
            for u in updates:
                offset = u.update_id + 1
                handle_update_sync(u, loop)
            time.sleep(0.2)
        except Exception as e:
            if isinstance(e, asyncio.TimeoutError):
                log.debug("Telegram get_updates timed out, retrying...")
                continue
            log.exception("Telegram polling thread error")
            try:
                ip = get_public_ip()
                send_telegram_sync(f"Telegram polling error: {e}")
            except Exception:
                pass
            time.sleep(5)

async def _set_running(val: bool):
    global running
    running = val

async def _set_frozen(val: bool):
    global frozen
    frozen = val

async def handle_critical_error_async(exc: Exception, context: str = None):
    global running
    running = False
    ip = await asyncio.to_thread(get_public_ip)
    tb = ''.join(traceback.format_exception(type(exc), exc, exc.__traceback__)) if exc else "No traceback"
    safe_tb = _shorten_for_telegram(tb)
    msg = f"CRITICAL ERROR: {context or ''}\nException: {str(exc)}\n\nTraceback:\n{safe_tb}\nServer IP: {ip}\nBot paused."
    await send_telegram(msg)

@app.get("/")
async def root():
    return {"status": "ok", "running": running, "managed_trades": len(managed_trades)}

def _signal_handler(sig, frame):
    log.info("Received signal %s, shutting down", sig)
    monitor_stop_event.set()
    try:
        send_telegram_sync(f"Received signal {sig}. Shutting down.")
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
        else:
            log.warning("Binance client not initialized, monitor thread not started.")
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
        loop.run_until_complete(send_telegram("KAMA strategy bot started (standalone). Running={}".format(running)))
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
        loop.close()
