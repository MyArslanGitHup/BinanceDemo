"""
Claude Trading Bot — Binance Futures
TradingView webhook → Binance Futures işlem açma botu
+ Telegram Bot entegrasyonu

SABİT STRATEJİ:
  Kaldıraç : 10x
  TP1 %1.0  → %20 kapat
  TP2 %2.0  → %20 kapat
  TP3 %3.0  → %20 kapat
  TP4 %4.0  → %20 kapat  + TSL devreye girer (callback %2.0)
  SL  %2.0  sabit
  Kalan %20 → TSL ile korunur
"""

import os
import json
import logging
import hmac
import hashlib
import time
import threading
import requests as req
from flask import Flask, request, jsonify
from binance.um_futures import UMFutures
from binance.error import ClientError
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)

# ─── LOGGING ────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("bot.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger(__name__)

# ─── AYARLAR ────────────────────────────────────────────────────
API_KEY          = os.getenv("BINANCE_API_KEY",    "YOUR_API_KEY")
API_SECRET       = os.getenv("BINANCE_API_SECRET", "YOUR_API_SECRET")
WEBHOOK_SECRET   = os.getenv("WEBHOOK_SECRET",     "YOUR_WEBHOOK_SECRET")
PORT             = int(os.getenv("PORT", 5000))
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN",     "YOUR_TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID",   "YOUR_CHAT_ID")

TRADE_USDT_FIXED = 100  # Sabit işlem miktarı (USDT)
MARGIN_TYPE   = "ISOLATED"
BASE_URL      = "https://fapi.binance.com"

# ─── SABİT STRATEJİ ─────────────────────────────────────────────
FIXED_LEVERAGE   = 10
TP_STEP_PCT      = 1.0    # Her TP bir öncekinden %1.0 uzakta
SL_PCT           = 2.0    # Stop Loss %2
TSL_CALLBACK_PCT = 2.0    # Trailing Stop callback %2.0
TP_QTY_PCT       = 20     # Her TP'de pozisyonun %20'si kapatılır

# ─── BİNANCE CLIENT ─────────────────────────────────────────────
client = UMFutures(key=API_KEY, secret=API_SECRET)

# ─── FLASK ──────────────────────────────────────────────────────
app = Flask(__name__)

# ─── TELEGRAM UYGULAMA (global) ─────────────────────────────────
telegram_app = None


# ════════════════════════════════════════════════════════════════
# TELEGRAM BİLDİRİM FONKSİYONLARI
# ════════════════════════════════════════════════════════════════

def send_telegram(message: str, parse_mode="HTML"):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        payload = {"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": parse_mode}
        req.post(url, json=payload, timeout=10)
    except Exception as e:
        log.error(f"Telegram mesajı gönderilemedi: {e}")


def notify_trade_opened(symbol, side, entry_price, tp_prices, sl_price):
    emoji = "🟢" if side == "BUY" else "🔴"
    direction = "LONG (AL)" if side == "BUY" else "SHORT (SAT)"
    msg = (
        f"{emoji} <b>YENİ POZİSYON</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📌 <b>Coin:</b> {symbol}\n"
        f"📊 <b>Yön:</b> {direction}\n"
        f"⚡ <b>Kaldıraç:</b> {FIXED_LEVERAGE}x\n"
        f"💰 <b>Giriş:</b> {entry_price}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"🎯 <b>TP1 (%1.0):</b> {tp_prices[0]}  →  %{TP_QTY_PCT} kapat\n"
        f"🎯 <b>TP2 (%2.0):</b> {tp_prices[1]}  →  %{TP_QTY_PCT} kapat\n"
        f"🎯 <b>TP3 (%3.0):</b> {tp_prices[2]}  →  %{TP_QTY_PCT} kapat\n"
        f"🎯 <b>TP4 (%4.0):</b> {tp_prices[3]}  →  %{TP_QTY_PCT} kapat + TSL\n"
        f"🔒 <b>SL (%2.0):</b>  {sl_price}\n"
        f"📉 <b>TSL Callback:</b> %{TSL_CALLBACK_PCT}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"🕐 {time.strftime('%H:%M:%S')}"
    )
    send_telegram(msg)


def notify_position_reversed(symbol, old_side, new_side, pnl=None):
    msg = (
        f"🔄 <b>POZİSYON TERSİNE ÇEVRİLDİ</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📌 <b>Coin:</b> {symbol}\n"
        f"📊 <b>Kapatılan:</b> {'LONG' if old_side == 'BUY' else 'SHORT'}\n"
        f"📊 <b>Açılan:</b> {'LONG' if new_side == 'BUY' else 'SHORT'}\n"
    )
    if pnl is not None:
        msg += f"💵 <b>Kapatma PnL:</b> {pnl:.2f} USDT\n"
    msg += f"🕐 {time.strftime('%H:%M:%S')}"
    send_telegram(msg)


def notify_webhook_error(symbol, error_msg):
    msg = (
        f"⚠️ <b>WEBHOOK HATASI</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📌 <b>Coin:</b> {symbol}\n"
        f"❌ <b>Hata:</b> {error_msg}\n"
        f"🕐 {time.strftime('%H:%M:%S')}\n"
        f"━━━━━━━━━━━━━━━\n"
        f"⚡ Manuel sinyal için /sinyal komutunu kullanın"
    )
    send_telegram(msg)


def notify_position_closed(symbol, side, pnl=None):
    emoji = "✅" if (pnl and pnl > 0) else "❌"
    msg = (
        f"{emoji} <b>POZİSYON KAPATILDI</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📌 <b>Coin:</b> {symbol}\n"
        f"📊 <b>Yön:</b> {'LONG' if side == 'BUY' else 'SHORT'}\n"
    )
    if pnl is not None:
        msg += f"💵 <b>PnL:</b> {pnl:.2f} USDT\n"
    msg += f"🕐 {time.strftime('%H:%M:%S')}"
    send_telegram(msg)


# ════════════════════════════════════════════════════════════════
# TELEGRAM BOT KOMUTLARI
# ════════════════════════════════════════════════════════════════

async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = (
        "🤖 <b>Claude Trading Bot</b>\n"
        "━━━━━━━━━━━━━━━\n"
        f"⚡ {FIXED_LEVERAGE}x | SL %{SL_PCT} | TP %{TP_STEP_PCT} aralıklı | TSL %{TSL_CALLBACK_PCT}\n"
        "━━━━━━━━━━━━━━━\n"
        "📋 <b>Komutlar:</b>\n\n"
        "/sinyal — Manuel sinyal gönder\n"
        "/pozisyonlar — Açık pozisyonları göster\n"
        "/kapat [COIN] — Pozisyon kapat\n"
        "/bakiye — USDT bakiyesi\n"
        "/yardim — Bu menü\n"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_bakiye(update: Update, context: ContextTypes.DEFAULT_TYPE):
    balance = get_usdt_balance()
    await update.message.reply_text(
        f"💰 <b>USDT Bakiye:</b> {balance:.2f} USDT",
        parse_mode="HTML"
    )


async def cmd_pozisyonlar(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        positions = client.get_position_risk()
        open_pos = [p for p in positions if float(p["positionAmt"]) != 0]

        if not open_pos:
            await update.message.reply_text("📭 Açık pozisyon yok.")
            return

        msg = "📊 <b>AÇIK POZİSYONLAR</b>\n━━━━━━━━━━━━━━━\n"
        for p in open_pos:
            amt = float(p["positionAmt"])
            direction = "🟢 LONG" if amt > 0 else "🔴 SHORT"
            pnl = float(p["unRealizedProfit"])
            pnl_emoji = "📈" if pnl >= 0 else "📉"
            msg += (
                f"\n📌 <b>{p['symbol']}</b>\n"
                f"  {direction} | {abs(amt)} adet\n"
                f"  Giriş: {float(p['entryPrice']):.6f}\n"
                f"  {pnl_emoji} PnL: {pnl:.2f} USDT\n"
            )

        await update.message.reply_text(msg, parse_mode="HTML")
    except Exception as e:
        await update.message.reply_text(f"❌ Hata: {e}")


async def cmd_kapat(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text(
            "⚠️ Kullanım: /kapat SOLUSDT\nveya /kapat all (hepsini kapat)"
        )
        return

    symbol_arg = context.args[0].upper()

    if symbol_arg == "ALL":
        try:
            positions = client.get_position_risk()
            open_pos = [p for p in positions if float(p["positionAmt"]) != 0]
            if not open_pos:
                await update.message.reply_text("📭 Kapatılacak pozisyon yok.")
                return
            for p in open_pos:
                await _close_position(update, p["symbol"])
        except Exception as e:
            await update.message.reply_text(f"❌ Hata: {e}")
        return

    await _close_position(update, symbol_arg)


async def _close_position(update, symbol):
    try:
        pos = get_open_position(symbol)
        if not pos:
            await update.message.reply_text(f"📭 {symbol} için açık pozisyon yok.")
            return

        amt   = float(pos["positionAmt"])
        side  = "SELL" if amt > 0 else "BUY"
        qty   = abs(amt)
        _, qty_precision, _ = get_symbol_info(symbol)
        qty   = round(qty, qty_precision)

        client.new_order(
            symbol=symbol, side=side, type="MARKET",
            quantity=qty, positionSide="BOTH", reduceOnly=True
        )
        log.info(f"{symbol} pozisyon kapatıldı")

        cancel_all_orders(symbol)

        pnl = float(pos.get("unRealizedProfit", 0))
        notify_position_closed(symbol, "BUY" if amt > 0 else "SELL", pnl)
        await update.message.reply_text(
            f"✅ <b>{symbol}</b> kapatıldı (tüm emirler iptal edildi)\n💵 PnL: {pnl:.2f} USDT",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {symbol} kapatılamadı: {e}")


# ─── MANUEL SİNYAL ──────────────────────────────────────────────
user_states = {}

async def cmd_sinyal(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_states[update.effective_user.id] = {"step": "coin"}
    await update.message.reply_text(
        "📌 <b>Manuel Sinyal</b>\n━━━━━━━━━━━━━━━\n"
        f"⚡ {FIXED_LEVERAGE}x | SL %{SL_PCT} | TP %{TP_STEP_PCT} aralıklı\n\n"
        "Coin adını girin (örn: SOL, BTC, ETH):",
        parse_mode="HTML"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in user_states:
        return

    state = user_states[uid]
    text  = update.message.text.strip().upper()

    if state["step"] == "coin":
        symbol = text.replace(".P", "").replace("USDT", "") + "USDT"
        state["symbol"] = symbol
        state["step"]   = "side"
        keyboard = [[
            InlineKeyboardButton("🟢 AL (LONG)",   callback_data="side_BUY"),
            InlineKeyboardButton("🔴 SAT (SHORT)", callback_data="side_SELL"),
        ]]
        await update.message.reply_text(
            f"📌 Coin: <b>{symbol}</b>\n\nYön seçin:",
            parse_mode="HTML",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )


async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    uid  = update.effective_user.id
    data = query.data

    if data.startswith("side_"):
        side  = data.split("_")[1]
        state = user_states.get(uid, {})
        if not state:
            await query.edit_message_text("⚠️ Oturum süresi doldu. /sinyal ile tekrar başlayın.")
            return

        state["side"] = side
        state["step"] = "confirm"

        current_price = get_current_price(state["symbol"])
        _, _, tick_size = get_symbol_info(state["symbol"])

        if side == "BUY":
            tp_prices = [round_to_tick(current_price * (1 + TP_STEP_PCT * i / 100), tick_size) for i in range(1, 5)]
            sl_price  = round_to_tick(current_price * (1 - SL_PCT / 100), tick_size)
        else:
            tp_prices = [round_to_tick(current_price * (1 - TP_STEP_PCT * i / 100), tick_size) for i in range(1, 5)]
            sl_price  = round_to_tick(current_price * (1 + SL_PCT / 100), tick_size)

        state["tp_prices"] = tp_prices
        state["sl_price"]  = sl_price

        side_text = "🟢 LONG (AL)" if side == "BUY" else "🔴 SHORT (SAT)"
        msg = (
            f"📋 <b>SİNYAL ÖNİZLEME</b>\n"
            f"━━━━━━━━━━━━━━━\n"
            f"📌 <b>Coin:</b> {state['symbol']}\n"
            f"📊 <b>Yön:</b> {side_text}\n"
            f"⚡ <b>Kaldıraç:</b> {FIXED_LEVERAGE}x\n"
            f"💰 <b>Tahmini Giriş:</b> {current_price}\n"
            f"━━━━━━━━━━━━━━━\n"
            f"🎯 TP1 (%1.0): {tp_prices[0]}  →  %{TP_QTY_PCT} kapat\n"
            f"🎯 TP2 (%2.0): {tp_prices[1]}  →  %{TP_QTY_PCT} kapat\n"
            f"🎯 TP3 (%3.0): {tp_prices[2]}  →  %{TP_QTY_PCT} kapat\n"
            f"🎯 TP4 (%4.0): {tp_prices[3]}  →  %{TP_QTY_PCT} kapat + TSL\n"
            f"🔒 SL (%2.0):  {sl_price}\n"
            f"📉 TSL Callback: %{TSL_CALLBACK_PCT}\n"
            f"━━━━━━━━━━━━━━━"
        )
        keyboard = [[
            InlineKeyboardButton("✅ ONAYLA & GÖNDER", callback_data="confirm_yes"),
            InlineKeyboardButton("❌ İPTAL",           callback_data="confirm_no"),
        ]]
        await query.edit_message_text(msg, parse_mode="HTML",
                                      reply_markup=InlineKeyboardMarkup(keyboard))

    elif data == "confirm_yes":
        state = user_states.get(uid, {})
        if not state:
            await query.edit_message_text("⚠️ Oturum süresi doldu.")
            return

        await query.edit_message_text("⏳ Sinyal gönderiliyor...")

        payload = {
            "secret": WEBHOOK_SECRET,
            "symbol": state["symbol"],
            "side":   state["side"].lower(),
        }
        try:
            result = process_signal(payload)
            if result.get("status") == "ok":
                await query.edit_message_text(
                    f"✅ <b>Sinyal gönderildi!</b>\n"
                    f"📌 {state['symbol']} | "
                    f"{'🟢 LONG' if state['side'] == 'BUY' else '🔴 SHORT'} | "
                    f"{FIXED_LEVERAGE}x",
                    parse_mode="HTML"
                )
            else:
                await query.edit_message_text(f"❌ Hata: {result.get('error', 'Bilinmeyen hata')}")
        except Exception as e:
            await query.edit_message_text(f"❌ Hata: {e}")

        user_states.pop(uid, None)

    elif data == "confirm_no":
        user_states.pop(uid, None)
        await query.edit_message_text("❌ Sinyal iptal edildi.")


# ════════════════════════════════════════════════════════════════
# İMZALI İSTEK
# ════════════════════════════════════════════════════════════════

def signed_request(method, path, params):
    params["timestamp"] = int(time.time() * 1000)
    query = "&".join(f"{k}={v}" for k, v in params.items())
    sig = hmac.new(API_SECRET.encode(), query.encode(), hashlib.sha256).hexdigest()
    params["signature"] = sig
    headers = {"X-MBX-APIKEY": API_KEY}
    url = BASE_URL + path
    try:
        if method == "POST":
            r = req.post(url, headers=headers, data=params, timeout=10)
        elif method == "DELETE":
            r = req.delete(url, headers=headers, params=params, timeout=10)
        else:
            r = req.get(url, headers=headers, params=params, timeout=10)
        if not r.content:
            log.error(f"Boş response geldi: {path}")
            return {}
        return r.json()
    except Exception as e:
        log.error(f"signed_request hatası [{path}]: {e}")
        return {}


# ════════════════════════════════════════════════════════════════
# YARDIMCI FONKSİYONLAR
# ════════════════════════════════════════════════════════════════

def get_usdt_balance():
    try:
        balances = client.balance()
        for b in balances:
            if b["asset"] == "USDT":
                return float(b["balance"])
    except ClientError as e:
        log.error(f"Kasa bakiyesi alınamadı: {e}")
    return 0.0


def get_open_position(symbol):
    try:
        positions = client.get_position_risk(symbol=symbol)
        for p in positions:
            if float(p["positionAmt"]) != 0:
                return p
    except ClientError as e:
        log.error(f"Pozisyon bilgisi alınamadı: {e}")
    return None


def get_symbol_info(symbol):
    """Returns: (price_precision, qty_precision, tick_size)"""
    try:
        info = client.exchange_info()
        for s in info["symbols"]:
            if s["symbol"] == symbol:
                tick_size = 0.01
                step_size = 0.001
                for f in s.get("filters", []):
                    if f["filterType"] == "PRICE_FILTER":
                        tick_size = float(f["tickSize"])
                    if f["filterType"] == "LOT_SIZE":
                        step_size = float(f["stepSize"])
                step_str      = f"{step_size:.10f}".rstrip("0")
                qty_precision = len(step_str.split(".")[1]) if "." in step_str else 0
                log.info(f"{symbol} tickSize={tick_size}, stepSize={step_size}, qtyPrecision={qty_precision}")
                return s["pricePrecision"], qty_precision, tick_size
    except ClientError as e:
        log.error(f"Sembol bilgisi alınamadı: {e}")
    return 2, 3, 0.01


def round_to_tick(price: float, tick_size: float) -> float:
    import math
    if tick_size <= 0:
        return price
    rounded   = math.floor(price / tick_size + 0.5) * tick_size
    tick_str  = f"{tick_size:.10f}".rstrip("0")
    decimals  = len(tick_str.split(".")[1]) if "." in tick_str else 0
    return round(rounded, decimals)


def get_valid_leverage(symbol, requested_leverage):
    try:
        brackets = client.leverage_brackets(symbol=symbol)
        for item in brackets:
            if item["symbol"] == symbol:
                max_lev = item["brackets"][0]["initialLeverage"]
                if requested_leverage > max_lev:
                    log.warning(f"{symbol} max kaldıraç: {max_lev}x")
                    return max_lev
                return requested_leverage
    except Exception as e:
        log.error(f"Kaldıraç bracket alınamadı: {e}")
    return requested_leverage


def get_current_price(symbol):
    try:
        ticker = client.ticker_price(symbol=symbol)
        return float(ticker["price"])
    except ClientError as e:
        log.error(f"Fiyat alınamadı: {e}")
    return 0.0


def get_entry_price(order, symbol):
    try:
        avg = float(order.get("avgPrice", 0) or 0)
        if avg > 0:
            return avg
    except Exception:
        pass
    try:
        pos = get_open_position(symbol)
        if pos:
            ep = float(pos.get("entryPrice", 0) or 0)
            if ep > 0:
                log.info(f"avgPrice 0 geldi, pozisyondan alındı: {ep}")
                return ep
    except Exception:
        pass
    price = get_current_price(symbol)
    log.warning(f"avgPrice ve entryPrice 0, anlık fiyat kullanılıyor: {price}")
    return price


def set_leverage(symbol, leverage):
    try:
        client.change_leverage(symbol=symbol, leverage=leverage)
        log.info(f"{symbol} kaldıraç {leverage}x ayarlandı")
        return True
    except ClientError as e:
        log.error(f"Kaldıraç ayarlanamadı: {e}")
        return False


def set_margin_type(symbol):
    try:
        client.change_margin_type(symbol=symbol, marginType=MARGIN_TYPE)
        log.info(f"{symbol} margin tipi {MARGIN_TYPE} ayarlandı")
    except ClientError as e:
        if "No need to change margin type" in str(e):
            log.info(f"{symbol} zaten {MARGIN_TYPE}")
        else:
            log.error(f"Margin tipi ayarlanamadı: {e}")


def calculate_quantity(symbol, leverage, price, qty_precision):
    # balance    = get_usdt_balance()
    # trade_usdt = balance * (TRADE_PERCENT / 100) * leverage
    trade_usdt = TRADE_USDT_FIXED * leverage  # Sabit 100 USDT marjin
    quantity   = trade_usdt / price
    quantity   = round(quantity, qty_precision)
    log.info(f"Sabit işlem: {TRADE_USDT_FIXED} USDT marjin | {trade_usdt:.2f} USDT pozisyon | Miktar: {quantity}")
    return quantity


def place_order(symbol, side, quantity):
    try:
        order = client.new_order(
            symbol=symbol, side=side, type="MARKET",
            quantity=quantity, positionSide="BOTH"
        )
        log.info(f"Pozisyon açıldı: {symbol} {side} {quantity}")
        return order
    except ClientError as e:
        log.error(f"Emir açılamadı: {e}")
        return None


def place_algo_order(params, label="Emir"):
    params["algoType"] = "CONDITIONAL"
    resp = signed_request("POST", "/fapi/v1/algoOrder", params)
    algo_id = resp.get("algoId") or resp.get("clientAlgoId") or resp.get("orderId")
    if algo_id:
        log.info(f"{label} yerleştirildi ✓ algoId={algo_id}")
        return algo_id
    else:
        log.error(f"{label} yerleştirilemedi: {resp}")
        return None


def cancel_all_orders(symbol):
    """Tüm emirleri (normal + algo TP/SL/TSL) iptal eder."""
    from urllib.parse import urlencode

    def hashing(query_string):
        return hmac.new(
            API_SECRET.encode("utf-8"), query_string.encode("utf-8"), hashlib.sha256
        ).hexdigest()

    def get_timestamp():
        return int(time.time() * 1000)

    def dispatch_request(http_method):
        session = req.Session()
        session.headers.update({
            "Content-Type": "application/json;charset=utf-8",
            "X-MBX-APIKEY": API_KEY
        })
        return {"DELETE": session.delete}.get(http_method)

    def send_signed_request(http_method, url_path, payload={}):
        query_string = urlencode(payload)
        query_string = query_string.replace("%27", "%22")
        if query_string:
            query_string = "{}&timestamp={}".format(query_string, get_timestamp())
        else:
            query_string = "timestamp={}".format(get_timestamp())
        url = BASE_URL + url_path + "?" + query_string + "&signature=" + hashing(query_string)
        params = {"url": url, "params": {}}
        response = dispatch_request(http_method)(**params)
        return response.json()

    # 1. Normal emirleri iptal et
    try:
        r = send_signed_request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})
        log.info(f"{symbol} allOpenOrders iptal: {r}")
    except Exception as e:
        log.warning(f"{symbol} allOpenOrders iptal hatası: {e}")

    # 2. TSL için algoOpenOrders iptal et
    try:
        r = send_signed_request("DELETE", "/fapi/v1/algoOpenOrders", {"symbol": symbol})
        log.info(f"{symbol} algoOpenOrders iptal: {r}")
    except Exception as e:
        log.warning(f"{symbol} algoOpenOrders iptal hatası: {e}")


# ════════════════════════════════════════════════════════════════
# EMİR FONKSİYONLARI
# ════════════════════════════════════════════════════════════════

def place_tp_orders(symbol, side, quantity, entry_price, tp_prices, price_precision, qty_precision, tick_size):
    close_side = "SELL" if side == "BUY" else "BUY"
    tp_qty     = round(quantity * (TP_QTY_PCT / 100), qty_precision)
    algo_ids   = []

    for i, tp_price in enumerate(tp_prices, start=1):
        tp_price_rounded = round_to_tick(tp_price, tick_size)
        if tp_price_rounded <= 0:
            log.error(f"TP{i} fiyatı geçersiz ({tp_price_rounded}), atlanıyor")
            continue
        params = {
            "symbol":       symbol,
            "side":         close_side,
            "type":         "TAKE_PROFIT_MARKET",
            "quantity":     tp_qty,
            "triggerPrice": tp_price_rounded,
            "workingType":  "MARK_PRICE",
            "reduceOnly":   "true",
            "timeInForce":  "GTE_GTC",
        }
        algo_id = place_algo_order(params, label=f"TP{i} ({tp_price_rounded} / %{TP_STEP_PCT * i})")
        if algo_id:
            algo_ids.append(algo_id)

    return algo_ids


def place_sl_order(symbol, side, quantity, entry_price, price_precision, tick_size):
    close_side = "SELL" if side == "BUY" else "BUY"
    if side == "BUY":
        sl_price = round_to_tick(entry_price * (1 - SL_PCT / 100), tick_size)
    else:
        sl_price = round_to_tick(entry_price * (1 + SL_PCT / 100), tick_size)
    params = {
        "symbol":       symbol,
        "side":         close_side,
        "type":         "STOP_MARKET",
        "quantity":     quantity,
        "triggerPrice": sl_price,
        "workingType":  "MARK_PRICE",
        "reduceOnly":   "true",
        "timeInForce":  "GTE_GTC",
    }
    return place_algo_order(params, label=f"SL ({sl_price} / %{SL_PCT})")


def place_trailing_stop(symbol, side, quantity, tp4_price, price_precision, qty_precision, tick_size):
    close_side    = "SELL" if side == "BUY" else "BUY"
    remaining_pct = 100 - (TP_QTY_PCT * 4)
    tsl_qty       = round(quantity * (remaining_pct / 100), qty_precision)
    if tsl_qty <= 0:
        log.warning("TSL: miktar 0, atlanıyor")
        return None
    activation = round_to_tick(tp4_price, tick_size)
    params = {
        "symbol":        symbol,
        "side":          close_side,
        "type":          "TRAILING_STOP_MARKET",
        "quantity":      tsl_qty,
        "activatePrice": activation,
        "callbackRate":  TSL_CALLBACK_PCT,
        "workingType":   "MARK_PRICE",
        "reduceOnly":    "true",
    }
    return place_algo_order(params, label=f"TSL (aktivasyon={activation}, callback={TSL_CALLBACK_PCT}%)")


# ════════════════════════════════════════════════════════════════
# AÇIK POZİSYONU KAPAT — KARŞI YÖN SİNYALİ İÇİN
# ════════════════════════════════════════════════════════════════

def close_existing_position(symbol, existing_pos):
    try:
        amt      = float(existing_pos["positionAmt"])
        side     = "SELL" if amt > 0 else "BUY"
        qty      = abs(amt)
        _, qty_precision, _ = get_symbol_info(symbol)
        qty      = round(qty, qty_precision)
        pnl      = float(existing_pos.get("unRealizedProfit", 0))
        old_side = "BUY" if amt > 0 else "SELL"

        client.new_order(
            symbol=symbol, side=side, type="MARKET",
            quantity=qty, positionSide="BOTH", reduceOnly=True
        )
        log.info(f"{symbol} mevcut pozisyon kapatıldı | PnL: {pnl:.2f} USDT")

        cancel_all_orders(symbol)

        return old_side, pnl

    except ClientError as e:
        log.error(f"{symbol} pozisyon kapatılamadı: {e}")
        return None, None


# ════════════════════════════════════════════════════════════════
# SINYAL İŞLEME
# ════════════════════════════════════════════════════════════════

def process_signal(data: dict) -> dict:
    symbol   = data.get("symbol", "").upper().replace(".P", "").replace(".PERP", "")
    symbol   = symbol + "USDT" if not symbol.endswith("USDT") else symbol
    side_raw = data.get("side", "").lower()

    if side_raw not in ["buy", "sell"]:
        return {"error": "Geçersiz side"}
    side = "BUY" if side_raw == "buy" else "SELL"

    # ── Açık pozisyon kontrolü ──────────────────────────────────
    existing = get_open_position(symbol)
    if existing:
        amt           = float(existing["positionAmt"])
        existing_side = "BUY" if amt > 0 else "SELL"

        if existing_side == side:
            log.info(f"{symbol} aynı yönde pozisyon zaten açık, sinyal atlandı.")
            return {"status": "ignored", "reason": "same direction position already open"}

        log.info(f"{symbol} karşı yön sinyali: {existing_side} kapatılıyor, {side} açılıyor.")
        old_side, pnl = close_existing_position(symbol, existing)
        if old_side is None:
            return {"error": "Mevcut pozisyon kapatılamadı"}

        notify_position_reversed(symbol, old_side, side, pnl)
        time.sleep(1)

    # ── Sembol bilgisi ──────────────────────────────────────────
    price_precision, qty_precision, tick_size = get_symbol_info(symbol)
    current_price = get_current_price(symbol)
    if current_price == 0:
        return {"error": "Fiyat alınamadı"}

    # ── Margin & Kaldıraç ───────────────────────────────────────
    set_margin_type(symbol)
    #valid_leverage = get_valid_leverage(symbol, FIXED_LEVERAGE)
    #if not set_leverage(symbol, valid_leverage):
    #    return {"error": "Kaldıraç ayarlanamadı"}
  # kaldıraç sorunu çözülüncü, sonradan hemen üstteki 3 satırı aktif edersin. alttaki satırı ise silersin. veya pasife alırsın. 
    valid_leverage = FIXED_LEVERAGE

    # ── Miktar ─────────────────────────────────────────────────
    quantity = calculate_quantity(symbol, valid_leverage, current_price, qty_precision)
    if quantity <= 0:
        return {"error": "Yetersiz bakiye"}

    # ── Pozisyon aç ─────────────────────────────────────────────
    order = place_order(symbol, side, quantity)
    if not order:
        return {"error": "Emir açılamadı"}

    entry_price = get_entry_price(order, symbol)
    log.info(f"Giriş fiyatı: {entry_price}")

    # ── TP/SL fiyatları ─────────────────────────────────────────
    if side == "BUY":
        tp_prices = [round_to_tick(entry_price * (1 + TP_STEP_PCT * i / 100), tick_size) for i in range(1, 5)]
        sl_price  = round_to_tick(entry_price * (1 - SL_PCT / 100), tick_size)
    else:
        tp_prices = [round_to_tick(entry_price * (1 - TP_STEP_PCT * i / 100), tick_size) for i in range(1, 5)]
        sl_price  = round_to_tick(entry_price * (1 + SL_PCT / 100), tick_size)

    log.info(f"TP seviyeleri: {tp_prices} | SL: {sl_price}")

    # ── Emirleri yerleştir ──────────────────────────────────────
    place_tp_orders(symbol, side, quantity, entry_price, tp_prices, price_precision, qty_precision, tick_size)
    place_sl_order(symbol, side, quantity, entry_price, price_precision, tick_size)
    place_trailing_stop(symbol, side, quantity, tp_prices[3], price_precision, qty_precision, tick_size)

    notify_trade_opened(symbol, side, entry_price, tp_prices, sl_price)

    log.info(f"✅ {symbol} {side} tamamlandı | {valid_leverage}x | Giriş: {entry_price}")
    return {"status": "ok", "symbol": symbol, "side": side, "leverage": valid_leverage}


# ════════════════════════════════════════════════════════════════
# WEBHOOK ENDPOINT
# ════════════════════════════════════════════════════════════════

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)
        log.info(f"Webhook alındı: {json.dumps(data)}")

        if data.get("secret") != WEBHOOK_SECRET:
            log.warning("Geçersiz secret!")
            return jsonify({"error": "Unauthorized"}), 401

        result = process_signal(data)

        if "error" in result:
            notify_webhook_error(data.get("symbol", "?"), result["error"])
            return jsonify(result), 500

        return jsonify(result), 200

    except Exception as e:
        log.error(f"Beklenmedik hata: {e}", exc_info=True)
        symbol = ""
        try:
            symbol = request.get_json(force=True).get("symbol", "?")
        except Exception:
            pass
        notify_webhook_error(symbol, str(e))
        return jsonify({"error": str(e)}), 500


@app.route("/health", methods=["GET"])
def health():
    balance = get_usdt_balance()
    return jsonify({"status": "running", "balance_usdt": balance}), 200


# ════════════════════════════════════════════════════════════════
# TELEGRAM BOT BAŞLATMA
# ════════════════════════════════════════════════════════════════

def run_telegram():
    import asyncio

    async def main():
        global telegram_app
        telegram_app = Application.builder().token(TELEGRAM_TOKEN).build()

        telegram_app.add_handler(CommandHandler(["start", "yardim"], cmd_start))
        telegram_app.add_handler(CommandHandler("bakiye",      cmd_bakiye))
        telegram_app.add_handler(CommandHandler("pozisyonlar", cmd_pozisyonlar))
        telegram_app.add_handler(CommandHandler("kapat",       cmd_kapat))
        telegram_app.add_handler(CommandHandler("sinyal",      cmd_sinyal))
        telegram_app.add_handler(CallbackQueryHandler(handle_callback))
        telegram_app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

        log.info("🤖 Telegram botu başlatıldı")

        await telegram_app.initialize()
        await telegram_app.start()
        await telegram_app.updater.start_polling(allowed_updates=Update.ALL_TYPES)

        while True:
            await asyncio.sleep(1)

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(main())


# ════════════════════════════════════════════════════════════════
# ANA BAŞLATMA
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("🤖 Claude Trading Bot başlatıldı...")
    log.info(f"⚡ Strateji: {FIXED_LEVERAGE}x | SL %{SL_PCT} | TP aralığı %{TP_STEP_PCT} | TSL %{TSL_CALLBACK_PCT}")

    if TELEGRAM_TOKEN:
        t = threading.Thread(target=run_telegram, daemon=True)
        t.start()
        send_telegram(
            f"🚀 <b>Claude Trading Bot başlatıldı!</b>\n"
            f"⚡ {FIXED_LEVERAGE}x | SL %{SL_PCT} | TP %{TP_STEP_PCT} aralıklı\n"
            f"/yardim ile komutları görebilirsiniz."
        )
    else:
        log.warning("TELEGRAM_TOKEN tanımlı değil, Telegram devre dışı.")

    log.info(f"📡 Webhook dinleniyor: http://0.0.0.0:{PORT}/webhook")
    app.run(host="0.0.0.0", port=PORT, debug=False)
