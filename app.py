# app.py
"""
KAMA trend-following bot — fixed for:
 - HTML error pages returned by Binance (sanitize and summarize before notifying)
 - Telegram 'Message is too long' handled (truncate and fallback to send_document)
 - Maintains DualLock, monitor thread, telegram thread, exchange-info cache, etc.
"""
import os
import sys
import time
import math
import asyncio
import logging
import sqlite3
import signal
import traceback
import threading
import io
import re
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from decimal import Decimal, ROUND_DOWN, getcontext
import html

import requests
import numpy as np
import pandas as pd
from fastapi import FastAPI

from binance.client import Client
from binance.exceptions import BinanceAPIException

import telegram
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import BadRequest
from telegram.utils.request import Request
import sshtunnel

def _get_env_int(key: str, default: int) -> int:
    """Safely get an integer from environment variables."""
    val = os.getenv(key)
    if val is None or not val.strip().isdigit():
        return default
    return int(val)

# -------------------------
# CONFIG (edit values here)
# -------------------------
CONFIG = {
    "SYMBOLS": os.getenv("SYMBOLS", "BTCUSDT,ETHUSDT").split(","),
    "TIMEFRAME": os.getenv("TIMEFRAME", "1h"),
    "BIG_TIMEFRAME": os.getenv("BIG_TIMEFRAME", "4h"),

    "SCAN_INTERVAL": _get_env_int("SCAN_INTERVAL", 20),
    "MAX_CONCURRENT_TRADES": _get_env_int("MAX_CONCURRENT_TRADES", 3),
    "START_MODE": os.getenv("START_MODE", "running").lower(),

    "KAMA_LENGTH": _get_env_int("KAMA_LENGTH", 10),
    "KAMA_FAST": _get_env_int("KAMA_FAST", 2),
    "KAMA_SLOW": _get_env_int("KAMA_SLOW", 30),

    "ATR_LENGTH": _get_env_int("ATR_LENGTH", 14),
    "SL_TP_ATR_MULT": float(os.getenv("SL_TP_ATR_MULT", "2.5")),

    "RISK_SMALL_BALANCE_THRESHOLD": float(os.getenv("RISK_SMALL_BALANCE_THRESHOLD", "50.0")),
    "RISK_SMALL_FIXED_USDT": float(os.getenv("RISK_SMALL_FIXED_USDT", "0.5")),
    "RISK_PCT_LARGE": float(os.getenv("RISK_PCT_LARGE", "0.02")),
    "MAX_RISK_USDT": float(os.getenv("MAX_RISK_USDT", "0.0")),

    "ADX_LENGTH": _get_env_int("ADX_LENGTH", 14),
    "ADX_THRESHOLD": float(os.getenv("ADX_THRESHOLD", "50.0")),

    "CHOP_LENGTH": _get_env_int("CHOP_LENGTH", 14),
    "CHOP_THRESHOLD": float(os.getenv("CHOP_THRESHOLD", "50.0")),

    "BB_LENGTH": _get_env_int("BB_LENGTH", 20),
    "BB_STD": float(os.getenv("BB_STD", "2.0")),
    "BBWIDTH_THRESHOLD": float(os.getenv("BBWIDTH_THRESHOLD", "7.0")),

    "MIN_CANDLES_AFTER_CLOSE": _get_env_int("MIN_CANDLES_AFTER_CLOSE", 10),

    "TRAILING_ENABLED": os.getenv("TRAILING_ENABLED", "true").lower() in ("true", "1", "yes"),

    "DB_FILE": os.getenv("DB_FILE", "trades.db"),
}
# -------------------------
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
USE_TESTNET = os.getenv("USE_TESTNET", "false").lower() in ("1", "true", "yes")
# -------------------------

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger("kama-bot")

app = FastAPI()

# Globals
client: Optional[Client] = None
TUNNEL_CONFIG = {
    "enabled": os.getenv("TUNNEL_ENABLED", "false").lower() in ("true", "1", "yes"),
    "host": os.getenv("TUNNEL_HOST"),
    "port": _get_env_int("TUNNEL_PORT", 22),
    "user": os.getenv("TUNNEL_USER"),
    "password": os.getenv("TUNNEL_PASSWORD"),
    "local_bind_port": _get_env_int("TUNNEL_LOCAL_PORT", 1080),
    "remote_bind_host": os.getenv("TUNNEL_REMOTE_HOST", "fapi.binance.com"),
    "remote_bind_port": _get_env_int("TUNNEL_REMOTE_PORT", 443),
}
tunnel_server = None

if TELEGRAM_BOT_TOKEN:
    tg_request = Request(con_pool_size=8)
    telegram_bot: Optional[telegram.Bot] = telegram.Bot(token=TELEGRAM_BOT_TOKEN, request=tg_request)
else:
    telegram_bot: Optional[telegram.Bot] = None

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
managed_trades_lock = DualLock()

last_trade_close_time: Dict[str, datetime] = {}

telegram_thread: Optional[threading.Thread] = None
monitor_thread_obj: Optional[threading.Thread] = None
monitor_stop_event = threading.Event()

scan_task = None

EXCHANGE_INFO_CACHE = {"ts": 0.0, "data": None, "ttl": 300}

# -------------------------
# Helpers for sanitized error messages
# -------------------------
def _shorten_for_telegram(text: str, max_len: int = 3500) -> str:
    if not isinstance(text, str):
        text = str(text)
    if len(text) <= max_len:
        return text
    return text[: max_len - 200] + "\n\n[...] (truncated)\n\n" + text[-200:]

def sanitize_error_for_telegram(raw: str, max_len: int = 1000) -> str:
    """
    If raw looks like HTML, extract a small plaintext summary (title + first paragraph).
    Otherwise, return a shortened raw string.
    """
    if not raw:
        return ""
    low = raw.lower()
    # HTML-like response
    if "<html" in low or "<!doctype html" in low or "<head" in low:
        # try to extract <title> and text content (strip tags)
        title_match = re.search(r"<title>(.*?)</title>", raw, flags=re.IGNORECASE | re.DOTALL)
        title = title_match.group(1).strip() if title_match else ""
        # remove tags for a text body preview
        text = re.sub(r"<[^>]+>", " ", raw)
        text = html.unescape(text)
        text = re.sub(r"\s+", " ", text).strip()
        preview = f"HTML error page: {title}\n{ text[: max_len] }"
        return preview
    # long JSON / traceback or other text
    text = raw.strip()
    if len(text) > max_len:
        return text[:max_len] + "\n\n[...] (truncated)"
    return text

def get_public_ip() -> str:
    """Fetches the public IP address from an external service."""
    try:
        # Use a reliable and simple service that returns JSON
        response = requests.get("https://api.ipify.org?format=json", timeout=5)
        response.raise_for_status()  # Raise an exception for bad status codes
        return response.json().get("ip", "N/A (key missing)")
    except requests.exceptions.RequestException as e:
        log.warning(f"Could not get public IP via requests: {e}")
        return "N/A (request failed)"
    except Exception as e:
        # Catch any other exceptions, e.g., JSON parsing
        log.warning(f"An unexpected error occurred while getting public IP: {e}")
        return "N/A (unexpected error)"

# -------------------------
# Robust Telegram send (sync)
# -------------------------
def send_telegram_sync(msg: str):
    """
    Send a Telegram message synchronously from threads.
    - Truncates message to safe length.
    - If Telegram rejects due to length, attempt to send as a document (file).
    - If document fails, send a short fallback message.
    """
    if not telegram_bot or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured; message (trimmed): %s", (msg or "")[:200])
        return
    try:
        # first attempt: truncated text message
        safe_msg = _shorten_for_telegram(msg, max_len=3500)
        telegram_bot.send_message(chat_id=int(TELEGRAM_CHAT_ID), text=safe_msg)
        return
    except BadRequest as bre:
        # message too long or other bad request: try fallback
        msg_err = str(bre)
        log.warning("Telegram BadRequest: %s -- attempting document fallback", msg_err)
        # attempt send full content as a document (txt)
        try:
            bio = io.BytesIO()
            bio.write((msg or "").encode("utf-8", errors="replace"))
            bio.seek(0)
            telegram_bot.send_document(chat_id=int(TELEGRAM_CHAT_ID), document=bio, filename="bot_log.txt")
            return
        except Exception as e2:
            log.exception("Failed to send telegram document fallback: %s", e2)
            # final fallback: very short message
            try:
                short = f"Message too long; failed to send full content. Error: {msg_err[:300]}"
                telegram_bot.send_message(chat_id=int(TELEGRAM_CHAT_ID), text=short)
                return
            except Exception:
                log.exception("Final telegram fallback failed")
                return
    except Exception:
        log.exception("Telegram send failed (sync)")
        # nothing else we can do safely here

async def send_telegram(msg: str):
    """Async wrapper to send telegram via thread so it doesn't block the event loop."""
    if not telegram_bot or not TELEGRAM_CHAT_ID:
        log.warning("Telegram not configured; message (trimmed): %s", (msg or "")[:200])
        return
    # run sync variant in background
    await asyncio.to_thread(send_telegram_sync, msg)

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
# Indicators (same as before)
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
# Tunnel Management
# -------------------------
def start_tunnel_sync():
    global tunnel_server
    if not TUNNEL_CONFIG.get("enabled"):
        return True, "Tunnel not enabled."

    # Validate required fields if tunnel is enabled
    required_fields = ["host", "user", "password"]
    missing_fields = [f for f in required_fields if not TUNNEL_CONFIG.get(f)]
    if missing_fields:
        error_msg = (
            f"Tunnel is enabled but required configuration is missing: {', '.join(missing_fields)}. "
            "Please set them using /settunnel or environment variables and restart."
        )
        return False, error_msg
    
    if tunnel_server and tunnel_server.is_active:
        log.info("Tunnel is already active.")
        return True, "Tunnel already active."

    try:
        # Use a fixed local bind address. The port can be configured.
        local_bind_address = '127.0.0.1'
        
        tunnel_server = sshtunnel.SSHTunnelForwarder(
            (TUNNEL_CONFIG["host"], TUNNEL_CONFIG["port"]),
            ssh_username=TUNNEL_CONFIG["user"],
            ssh_password=TUNNEL_CONFIG["password"],
            remote_bind_address=(TUNNEL_CONFIG["remote_bind_host"], TUNNEL_CONFIG["remote_bind_port"]),
            local_bind_address=(local_bind_address, TUNNEL_CONFIG["local_bind_port"])
        )
        tunnel_server.start()
        log.info(f"SSH tunnel started: {local_bind_address}:{tunnel_server.local_bind_port} -> {TUNNEL_CONFIG['remote_bind_host']}:{TUNNEL_CONFIG['remote_bind_port']}")
        return True, "Tunnel started successfully."
    except Exception as e:
        log.exception("Failed to start SSH tunnel.")
        tunnel_server = None
        return False, str(e)

def stop_tunnel_sync():
    global tunnel_server
    if tunnel_server and tunnel_server.is_active:
        log.info("Stopping SSH tunnel.")
        tunnel_server.stop()
        tunnel_server = None
    else:
        log.info("SSH tunnel is not running or already stopped.")

# -------------------------
# Binance init (sync)
# -------------------------
def init_binance_client_sync():
    global client, tunnel_server, EXCHANGE_INFO_CACHE
    
    # Reset client at the start of initialization
    client = None
    EXCHANGE_INFO_CACHE['data'] = None
    EXCHANGE_INFO_CACHE['ts'] = 0.0

    # Start the tunnel if it's enabled
    tunnel_ok, tunnel_msg = start_tunnel_sync()
    if not tunnel_ok:
        return False, f"Tunnel failed to start: {tunnel_msg}"

    # Prepare requests parameters (for proxy, if tunnel is active)
    req_params = {}
    if tunnel_server and tunnel_server.is_active:
        local_bind_port = TUNNEL_CONFIG["local_bind_port"]
        proxies = {
            'http': f'socks5h://127.0.0.1:{local_bind_port}',
            'https': f'socks5h://127.0.0.1:{local_bind_port}'
        }
        req_params['proxies'] = proxies
        log.info(f"Tunnel active, using proxies: {proxies}")

    try:
        log.info("Initializing Binance client...")
        if USE_TESTNET:
            # Note: Tunneling for testnet might require different remote_bind_address.
            client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, testnet=True, requests_params=req_params)
            log.warning("Binance client in TESTNET mode.")
        else:
            client = Client(BINANCE_API_KEY, BINANCE_API_SECRET, requests_params=req_params)
            log.info("Binance client in MAINNET mode.")
        
        client.ping()
        log.info("Connected to Binance API (ping ok).")
        return True, ""
    except Exception as e:
        log.exception("Failed to connect to Binance API")
        stop_tunnel_sync() # Stop tunnel if connection failed, to allow clean retry
        err = f"Binance init error: {e}"
        # No need to get public IP if we are using a tunnel or failing for other reasons
        try:
            send_telegram_sync(f"Binance init failed: {sanitize_error_for_telegram(str(e))}")
        except Exception:
            log.exception("Failed to notify via telegram about Binance init error.")
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

# -------------------------
# Symbol helpers & rounding
# -------------------------
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

# -------------------------
# Orders (sync)
# -------------------------
def open_market_position_sync(symbol: str, side: str, qty: float, leverage: int):
    global client
    if client is None:
        raise RuntimeError("Binance client not initialized")
    try:
        try:
            client.futures_change_leverage(symbol=symbol, leverage=leverage)
        except Exception:
            log.exception("Failed to change leverage (non-fatal).")
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

# -------------------------
# Risk
# -------------------------
def calculate_risk_amount(account_balance: float) -> float:
    if account_balance < CONFIG["RISK_SMALL_BALANCE_THRESHOLD"]:
        risk = CONFIG["RISK_SMALL_FIXED_USDT"]
    else:
        risk = account_balance * CONFIG["RISK_PCT_LARGE"]
    max_cap = CONFIG.get("MAX_RISK_USDT", 0.0)
    if max_cap and max_cap > 0:
        risk = min(risk, max_cap)
    return float(risk)

# -------------------------
# Validation (sync)
# -------------------------
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
    
    # Check Binance connection status
    binance_ok = False
    if client:
        try:
            client.ping()
            results["checks"].append({"type": "binance_connect", "ok": True})
            binance_ok = True
        except Exception as e:
            results["ok"] = False
            results["checks"].append({"type": "binance_connect", "ok": False, "detail": str(e)})
    else:
        results["ok"] = False
        results["checks"].append({"type": "binance_connect", "ok": False, "detail": "Client not initialized"})

    sample_sym = CONFIG["SYMBOLS"][0].strip().upper() if CONFIG["SYMBOLS"] else None
    if sample_sym and binance_ok:
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

# -------------------------
# Candles helper
# -------------------------
def candles_since_close(df: pd.DataFrame, close_time: Optional[datetime]) -> int:
    if not close_time:
        return 99999
    if close_time.tzinfo is None:
        close_time = close_time.replace(tzinfo=timezone.utc)
    return int((df.index > close_time).sum())

# -------------------------
# fetch klines (sync)
# -------------------------
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

# -------------------------
# evaluate & enter (async)
# -------------------------
async def evaluate_and_enter(symbol: str):
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
                log.info("%s skip due cooldown: %d/%d", symbol, n_since, CONFIG["MIN_CANDLES_AFTER_CLOSE"])
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
            await send_telegram(f"Opened {side} on {symbol}\nEntry: {price:.4f}\nQty: {qty}\nLeverage: {leverage}\nRisk: {risk_usdt:.4f} USDT\nSL: {stop_price:.4f}\nTP: {take_price:.4f}\nTrade ID: {trade_id}")
            log.info("Opened trade: %s", meta)
        except Exception as e:
            log.exception("Failed to open trade for %s: %s", symbol, e)
            tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
            teaser = sanitize_error_for_telegram(tb, max_len=1500)
            await send_telegram(f"ERROR opening {symbol}:\n{teaser}\nServer IP: {get_public_ip()}")
            running = False
        return

    except Exception as e:
        log.exception("evaluate_and_enter error %s: %s", symbol, e)
        tb = ''.join(traceback.format_exception(type(e), e, e.__traceback__))
        teaser = sanitize_error_for_telegram(tb, max_len=1500)
        await send_telegram(f"Scan error for {symbol}:\n{teaser}\nServer IP: {get_public_ip()}")

# -------------------------
# account balance (sync)
# -------------------------
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

# -------------------------
# Monitor thread (sync)
# -------------------------
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
            except Exception as e:
                # sanitize the Binance response (may be HTML)
                raw = str(e)
                if isinstance(e, BinanceAPIException) and "<html" in raw.lower():
                    log.warning(
                        "Binance API returned an HTML page instead of JSON. "
                        "This commonly happens when the server's IP address is not whitelisted in the Binance API key settings. "
                        "Please verify the IP whitelist."
                    )
                teaser = sanitize_error_for_telegram(raw, max_len=1500)
                log.exception("Error fetching positions in monitor thread: %s", teaser)
                send_telegram_sync(f"Error fetching positions: {teaser}\nServer IP: {get_public_ip()}")
                running = False
                time.sleep(10)
                continue

            # snapshot trades under lock
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

                    # persist unreal into managed_trades
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

                    # trailing
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
                        except Exception as e_tr:
                            teaser = sanitize_error_for_telegram(str(e_tr), max_len=1000)
                            log.exception("Trailing internal error in monitor thread: %s", teaser)
                except Exception:
                    log.exception("Error handling trade in monitor thread")
            # remove closed trades under lock
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

# -------------------------
# Scanning loop (async)
# -------------------------
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
        except Exception:
            log.exception("Scanning loop error")
        await asyncio.sleep(CONFIG["SCAN_INTERVAL"])

# -------------------------
# Telegram helpers & thread
# -------------------------
async def get_managed_trades_snapshot():
    async with managed_trades_lock:
        return dict(managed_trades)

def build_control_keyboard():
    buttons = [
        [InlineKeyboardButton("Start", callback_data="start"), InlineKeyboardButton("Stop", callback_data="stop")],
        [InlineKeyboardButton("Freeze", callback_data="freeze"), InlineKeyboardButton("Unfreeze", callback_data="unfreeze")],
        [InlineKeyboardButton("List Orders", callback_data="listorders"), InlineKeyboardButton("Params", callback_data="showparams")],
        [InlineKeyboardButton("Tunnel Status", callback_data="tunnelstatus")]
    ]
    return InlineKeyboardMarkup(buttons)

from fastapi import Request, Response

@app.post("/webhook/{token}")
async def telegram_webhook(token: str, request: Request):
    if token != TELEGRAM_BOT_TOKEN:
        return Response(content="unauthorized", status_code=401)
    
    update_data = await request.json()
    asyncio.create_task(handle_update(update_data))
    return Response(content="ok", status_code=200)

async def handle_update(update: dict):
    upd = telegram.Update.de_json(update, telegram_bot)
    try:
        if upd is None:
            return
        if getattr(upd, 'message', None):
            msg = upd.message
            text = (msg.text or "").strip()
            if text.startswith("/startbot"):
                await _set_running(True)
                await send_telegram("Bot state -> RUNNING")
            elif text.startswith("/stopbot"):
                await _set_running(False)
                await send_telegram("Bot state -> STOPPED")
            elif text.startswith("/freeze"):
                await _set_frozen(True)
                await send_telegram("Bot -> FROZEN (no new entries)")
            elif text.startswith("/unfreeze"):
                await _set_frozen(False)
                await send_telegram("Bot -> UNFROZEN")
            elif text.startswith("/help"):
                help_text = (
                    "**KAMA Bot Commands**\\n\\n"
                    "/startbot: Starts the trading bot.\\n"
                    "/stopbot: Stops the trading bot.\\n"
                    "/status: Shows current bot status.\\n"
                    "/freeze: Freezes new trades, but continues monitoring existing ones.\\n"
                    "/unfreeze: Unfreezes the bot to allow new trades.\\n"
                    "/ip: Shows the server's public IP address.\\n"
                    "/listorders: Lists all currently managed open trades.\\n"
                    "/showparams: Shows the current trading parameters.\\n"
                    "/setparam <p> <v>: Sets a trading parameter `p` to value `v`.\\n"
                    "/validate: Runs a sanity check of the configuration.\\n"
                    "--- Tunnel Commands ---\\n"
                    "/tunnelstatus: Shows the SSH tunnel configuration and status.\\n"
                    "/settunnel <host> <user> <password> [port]: Sets tunnel credentials and enables it."
                )
                await send_telegram(help_text)
            elif text.startswith("/ip") or text.startswith("/forceip"):
                ip = await asyncio.to_thread(get_public_ip)
                await send_telegram(f"Server IP: {ip}")
            elif text.startswith("/status"):
                trades = await get_managed_trades_snapshot()
                txt = f"Status:\nrunning={running}\nfrozen={frozen}\nmanaged_trades={len(trades)}"
                await send_telegram(txt)
                try:
                    await asyncio.to_thread(telegram_bot.send_message, chat_id=int(TELEGRAM_CHAT_ID), text="Controls:", reply_markup=build_control_keyboard())
                except Exception:
                    pass
            elif text.startswith("/listorders"):
                trades = await get_managed_trades_snapshot()
                if not trades:
                    await send_telegram("No managed trades.")
                else:
                    out = "Open trades:\n"
                    for k, v in trades.items():
                        unreal = v.get('unreal')
                        unreal_str = "N/A" if unreal is None else f"{float(unreal):.6f}"
                        out += f"{k} {v['symbol']} {v['side']} entry={v['entry_price']:.4f} qty={v['qty']} sl={v['sl']:.4f} tp={v['tp']:.4f} unreal={unreal_str}\n"
                    await send_telegram(out)
            elif text.startswith("/showparams"):
                out = "Current runtime params:\n"
                for k,v in CONFIG.items():
                    out += f"{k} = {v}\n"
                await send_telegram(out)
            elif text.startswith("/setparam"):
                parts = text.split()
                if len(parts) >= 3:
                    key = parts[1]
                    val = " ".join(parts[2:])
                    if key not in CONFIG:
                        await send_telegram(f"Parameter {key} not found.")
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
                            await send_telegram(f"Set {key} = {CONFIG[key]}")
                        except Exception as e:
                            await send_telegram(f"Failed to set {key}: {e}")
                else:
                    await send_telegram("Usage: /setparam KEY VALUE")
            elif text.startswith("/validate"):
                result = await asyncio.to_thread(validate_and_sanity_check_sync, send_report=False)
                await send_telegram("Validation result: " + ("OK" if result["ok"] else "ERROR"))
                for c in result["checks"]:
                    await send_telegram(f"{c['type']}: ok={c['ok']} detail={c.get('detail')}")
            elif text.startswith("/tunnelstatus"):
                status = "active" if tunnel_server and tunnel_server.is_active else "inactive"
                out = f"Tunnel Status: {status}\\n-- Config --\\n"
                cfg_safe = {k: (v if k != 'password' else '******') for k,v in TUNNEL_CONFIG.items()}
                for k,v in cfg_safe.items():
                    out += f"{k} = {v}\\n"
                await send_telegram(out)
            elif text.startswith("/settunnel"):
                parts = text.split()
                if len(parts) == 2 and parts[1].lower() == 'off':
                    TUNNEL_CONFIG['enabled'] = False
                    await send_telegram("Tunnel disabled. Restart required.")
                elif len(parts) >= 4:
                    try:
                        TUNNEL_CONFIG['host'] = parts[1]
                        TUNNEL_CONFIG['user'] = parts[2]
                        TUNNEL_CONFIG['password'] = parts[3]
                        if len(parts) >= 5:
                            TUNNEL_CONFIG['port'] = int(parts[4])
                        else:
                            TUNNEL_CONFIG['port'] = 22 # Default SSH port
                        TUNNEL_CONFIG['enabled'] = True
                        await send_telegram("Tunnel configured and enabled. Restart required.")
                    except Exception as e:
                        await send_telegram(f"Error processing command: {e}")
                else:
                    await send_telegram("Usage:\n/settunnel <host> <user> <password> [port]\n/settunnel off")
            else:
                await send_telegram("Unknown command. Use /status or buttons.")
        elif getattr(upd, 'callback_query', None):
            cq = upd.callback_query
            data = cq.data
            try:
                await asyncio.to_thread(telegram_bot.answer_callback_query, callback_query_id=cq.id, text=f"Action: {data}")
            except Exception:
                pass
            if data == "start":
                await _set_running(True)
                await send_telegram("Bot state -> RUNNING")
            elif data == "stop":
                await _set_running(False)
                await send_telegram("Bot state -> STOPPED")
            elif data == "freeze":
                await _set_frozen(True)
                await send_telegram("Bot -> FROZEN")
            elif data == "unfreeze":
                await _set_frozen(False)
                await send_telegram("Bot -> UNFROZEN")
            elif data == "listorders":
                trades = await get_managed_trades_snapshot()
                if not trades:
                    await send_telegram("No managed trades.")
                else:
                    out = "Open trades:\n"
                    for k, v in trades.items():
                        unreal = v.get('unreal')
                        unreal_str = "N/A" if unreal is None else f"{float(unreal):.6f}"
                        out += f"{k} {v['symbol']} {v['side']} entry={v['entry_price']:.4f} qty={v['qty']} sl={v['sl']:.4f} tp={v['tp']:.4f} unreal={unreal_str}\n"
                    await send_telegram(out)
            elif data == "showparams":
                out = "Current runtime params:\n"
                for k,v in CONFIG.items():
                    out += f"{k} = {v}\n"
                await send_telegram(out)
    except Exception:
        log.exception("Error in handle_update")

def telegram_polling_thread(loop):
    global telegram_bot
    if not telegram_bot:
        log.info("telegram thread not started: bot not configured")
        return
    offset = None
    while True:
        try:
            updates = telegram_bot.get_updates(offset=offset, timeout=20)
            for u in updates:
                offset = u.update_id + 1
                handle_update_sync(u, loop)
            time.sleep(0.2)
        except Exception:
            log.exception("Telegram polling thread error")
            try:
                ip = get_public_ip()
                send_telegram_sync(f"Telegram polling error. Server IP: {ip}")
            except Exception:
                pass
            time.sleep(5)

# -------------------------
# small control coroutines
# -------------------------
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
    teaser = sanitize_error_for_telegram(tb, max_len=1500)
    msg = f"CRITICAL ERROR: {context or ''}\nException: {teaser}\nServer IP: {ip}\nBot paused."
    await send_telegram(msg)

# -------------------------
# FastAPI health
# -------------------------
@app.get("/")
async def root():
    return {"status": "ok", "running": running, "managed_trades": len(managed_trades)}

# -------------------------
# Startup / Shutdown
# -------------------------
@app.on_event("startup")
async def startup_event():
    log.info("Web server started. Initializing bot in background.")
    asyncio.create_task(initialize_bot())

async def initialize_bot():
    global scan_task, monitor_thread_obj, client, monitor_stop_event
    
    WEBHOOK_URL = os.getenv("WEBHOOK_URL")
    if telegram_bot and WEBHOOK_URL:
        webhook_url_with_token = f"{WEBHOOK_URL}/webhook/{TELEGRAM_BOT_TOKEN}"
        success = await asyncio.to_thread(telegram_bot.set_webhook, url=webhook_url_with_token)
        if success:
            log.info(f"Webhook set to {webhook_url_with_token}")
        else:
            log.error("Failed to set webhook.")
    elif telegram_bot:
        log.warning("WEBHOOK_URL not set. Bot will not receive updates from Telegram.")
    
    # --- Pre-startup Network Diagnostic Test ---
    try:
        log.info("Performing pre-startup network diagnostic test to Binance...")
        diag_url = "https://fapi.binance.com/fapi/v1/ping"
        response = requests.get(diag_url, timeout=10)
        if response.status_code != 200:
            err_msg = f"Network diagnostic test failed: Received status code {response.status_code} from Binance."
            log.critical(err_msg)
            await send_telegram(f"CRITICAL: Bot startup failed. {err_msg}")
            return
        if "text/html" in response.headers.get('Content-Type', ''):
            err_msg = "Network diagnostic test failed: Binance returned an HTML page. This confirms a network block or firewall issue."
            log.critical(err_msg)
            await send_telegram(f"CRITICAL: Bot startup failed. {err_msg}")
            return
        log.info("Network diagnostic test successful.")
    except requests.exceptions.RequestException as e:
        err_msg = f"Network diagnostic test failed with a connection error: {e}"
        log.critical(err_msg)
        await send_telegram(f"CRITICAL: Bot startup failed. {err_msg}")
        return

    init_db()
    ok, err = await asyncio.to_thread(init_binance_client_sync)
    if not ok:
        log.critical(f"Binance client failed to initialize: {err}. The trading loops will not be started.")
        if "Tunnel is enabled but required configuration is missing" in err:
            await send_telegram(f"CRITICAL: Bot startup failed. {err}")
        else:
            await send_telegram(f"CRITICAL: Bot startup failed. Binance client initialization failed: {err}")
        return

    await asyncio.to_thread(validate_and_sanity_check_sync, True)
    
    # Start trading-related tasks only if initialization was successful
    scan_task = loop.create_task(scanning_loop())
    monitor_stop_event.clear()
    monitor_thread_obj = threading.Thread(target=monitor_thread_func, daemon=True)
    monitor_thread_obj.start()
    log.info("Started monitor thread.")
    try:
        await send_telegram("KAMA strategy bot started. Running={}".format(running))
    except Exception:
        log.exception("Failed to send startup telegram")

@app.on_event("shutdown")
async def shutdown_event():
    if telegram_bot:
        log.info("Deleting webhook...")
        await asyncio.to_thread(telegram_bot.delete_webhook)
    stop_tunnel_sync()
    try:
        monitor_stop_event.set()
        if monitor_thread_obj and monitor_thread_obj.is_alive():
            monitor_thread_obj.join(timeout=5)
    except Exception:
        log.exception("Error stopping monitor thread")
    try:
        await send_telegram("KAMA strategy bot shutting down.")
    except Exception:
        pass

def _signal_handler(sig, frame):
    log.info("Received signal %s, shutting down", sig)
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
    loop.run_until_complete(startup_event())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

