"""
Claude Trading Bot — Binance Futures
TradingView webhook → Binance Futures işlem açma botu
+ Telegram Bot entegrasyonu

ATR BAZLI DİNAMİK STRATEJİ:
  Kaldıraç : 5x (sabit)

  ATR% < 1.5  → DAR AYARLAR
    SL: %1.0  | TP1:%1.0 TP2:%2.0 TP3:%3.0 TP4:%4.0 | TSL:%1.0

  ATR% 1.5-3.0 → ORTA AYARLAR
    SL: %2.0  | TP1:%2.0 TP2:%4.0 TP3:%6.0 TP4:%8.0 | TSL:%1.5

  ATR% > 3.0   → GENİŞ AYARLAR
    İşlem açılmaz.

  Her TP → %20 kapat, kalan %20 → TSL ile korunur
"""

import os
import json
import logging
import hmac
import hashlib
import time
import threading
import requests as req
import websocket
import psycopg2
from psycopg2.extras import RealDictCursor
from flask import Flask, request, jsonify
from binance.um_futures import UMFutures
from binance.error import ClientError
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler,
    MessageHandler, filters, ContextTypes
)
from datetime import datetime, timezone

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
BASE_URL      = "https://demo-fapi.binance.com"

# ─── SABİT PARAMETRELER ─────────────────────────────────────────
FIXED_LEVERAGE   = 5      # 10x yerine 5x
TP_QTY_PCT       = 20     # Her TP'de %20 kapat

# ─── ATR BAZLI STRATEJİ TANIMLARI ───────────────────────────────
# Her senaryo: (sl_pct, [tp1, tp2, tp3, tp4], tsl_callback)
ATR_STRATEGIES = {
    "dar":   {"sl": 1.0, "tps": [1.0, 2.0, 3.0, 4.0],  "tsl": 1.0, "label": "DAR   (ATR<%1.5)"},
    "orta":  {"sl": 2.0, "tps": [2.0, 4.0, 6.0, 8.0],  "tsl": 1.5, "label": "ORTA  (ATR%1.5-3.0)"},
    "genis": {"sl": 3.5, "tps": [3.0, 6.0, 9.0, 12.0], "tsl": 3.0, "label": "GENİŞ (ATR%>3.0)"},
}
ATR_MID_PCT  = 1.5   # Dar / Orta sınırı
ATR_HIGH_PCT = 3.0   # Orta / Geniş sınırı
ATR_PERIOD   = 14    # ATR periyodu (kline sayısı)

# ─── BİNANCE CLIENT ─────────────────────────────────────────────
client = UMFutures(key=API_KEY, secret=API_SECRET, base_url=BASE_URL)

# ─── FLASK ──────────────────────────────────────────────────────
app = Flask(__name__)

# ─── TELEGRAM UYGULAMA (global) ─────────────────────────────────
telegram_app = None

# ─── PostgreSQL BAĞLANTISI ──────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "")


def get_db():
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """Tablo yoksa oluştur."""
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS trade_events (
                id         SERIAL PRIMARY KEY,
                ts         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                event      TEXT NOT NULL,
                symbol     TEXT NOT NULL,
                side       TEXT NOT NULL,
                pnl        FLOAT,
                extra      JSONB
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        log.info("✅ DB tablosu hazır.")
    except Exception as e:
        log.error(f"DB init hatası: {e}")


# ════════════════════════════════════════════════════════════════
# ATR BAZLI STRATEJİ SEÇİCİ
# ════════════════════════════════════════════════════════════════

def get_atr_percent(symbol: str, interval: str = "4h") -> float:
    """
    Binance'den son ATR_PERIOD mumun TR ortalamasını alır,
    fiyata bölerek ATR% döner.
    Hata durumunda -1 döner.
    """
    try:
        klines = client.klines(symbol=symbol, interval=interval, limit=ATR_PERIOD + 1)
        if len(klines) < 2:
            log.error(f"ATR için yeterli kline yok: {symbol}")
            return -1.0

        tr_values = []
        for i in range(1, len(klines)):
            high      = float(klines[i][2])
            low       = float(klines[i][3])
            prev_close = float(klines[i - 1][4])
            tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
            tr_values.append(tr)

        atr       = sum(tr_values) / len(tr_values)
        cur_price = float(klines[-1][4])
        atr_pct   = (atr / cur_price) * 100
        log.info(f"{symbol} ATR%={atr_pct:.2f} (interval={interval})")
        return round(atr_pct, 2)

    except Exception as e:
        log.error(f"ATR hesaplanamadı ({symbol}): {e}")
        return -1.0


def select_strategy(atr_pct: float) -> dict:
    """ATR%'ye göre strateji seç. Her zaman bir strateji döner."""
    if atr_pct < 0:
        log.warning("ATR alınamadı, varsayılan ORTA strateji kullanılıyor.")
        return ATR_STRATEGIES["orta"]
    if atr_pct < ATR_MID_PCT:
        return ATR_STRATEGIES["dar"]
    if atr_pct < ATR_HIGH_PCT:
        return ATR_STRATEGIES["orta"]
    return ATR_STRATEGIES["genis"]


# ════════════════════════════════════════════════════════════════
# İSTATİSTİK LOGLAMA FONKSİYONLARI
# ════════════════════════════════════════════════════════════════

def log_trade_event(event_type: str, symbol: str, side: str, pnl: float = None, extra: dict = None):
    try:
        conn = get_db()
        cur  = conn.cursor()
        cur.execute(
            "INSERT INTO trade_events (event, symbol, side, pnl, extra) VALUES (%s, %s, %s, %s, %s)",
            (event_type, symbol, side, pnl, json.dumps(extra) if extra else None)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        log.error(f"DB yazma hatası: {e}")


def read_stats_log(since_dt: datetime) -> list:
    """since_dt'den itibaren tüm kayıtları döner."""
    try:
        conn = get_db()
        cur  = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute(
            "SELECT * FROM trade_events WHERE ts >= %s ORDER BY ts ASC",
            (since_dt,)
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
        records = []
        for row in rows:
            r = dict(row)
            r["ts"] = r["ts"].isoformat()
            records.append(r)
        return records
    except Exception as e:
        log.error(f"DB okuma hatası: {e}")
        return []


def build_stats_message(records: list, period_label: str) -> str:
    trades = {}
    opened_by_symbol = {}

    for rec in sorted(records, key=lambda r: r["ts"]):
        sym = rec["symbol"]
        ev  = rec["event"]

        if ev == "OPENED":
            key = (sym, rec["ts"])
            trades[key] = {
                "symbol": sym,
                "side":   rec["side"],
                "open_ts": rec["ts"],
                "events": ["OPENED"],
                "pnl":    0.0,
            }
            opened_by_symbol[sym] = key
        else:
            key = opened_by_symbol.get(sym)
            if key and key in trades:
                trades[key]["events"].append(ev)
                if rec.get("pnl") is not None:
                    trades[key]["pnl"] = (trades[key]["pnl"] or 0) + rec["pnl"]
            else:
                key = (sym, rec["ts"])
                trades[key] = {
                    "symbol": sym,
                    "side":   rec["side"],
                    "open_ts": rec["ts"],
                    "events": ["OPENED", ev],
                    "pnl":    rec.get("pnl") or 0.0,
                }
                opened_by_symbol[sym] = key

    cats = {
        "açılan":           {"count": 0, "pnl": 0.0},
        "stop los":         {"count": 0, "pnl": 0.0},
        "Tp1+SL":           {"count": 0, "pnl": 0.0},
        "Tp2+SL":           {"count": 0, "pnl": 0.0},
        "Tp3+SL":           {"count": 0, "pnl": 0.0},
        "Tp4+SL":           {"count": 0, "pnl": 0.0},
        "TSL + SL":         {"count": 0, "pnl": 0.0},
        "TSL(halen açık)":  {"count": 0, "pnl": 0.0},
        "Giriş-Açık":       {"count": 0, "pnl": 0.0},
        "TP1(halen açık)":  {"count": 0, "pnl": 0.0},
        "TP2(halen açık)":  {"count": 0, "pnl": 0.0},
        "TP3(halen açık)":  {"count": 0, "pnl": 0.0},
        "TP4(halen açık)":  {"count": 0, "pnl": 0.0},
    }

    total_opened = len([t for t in trades.values() if "OPENED" in t["events"]])

    for trade in trades.values():
        evs = trade["events"]
        pnl = trade["pnl"] or 0.0

        has_tp1 = "TP1_HIT" in evs
        has_tp2 = "TP2_HIT" in evs
        has_tp3 = "TP3_HIT" in evs
        has_tp4 = "TP4_HIT" in evs
        has_sl  = "SL_HIT"  in evs
        has_tsl_active = "TSL_ACTIVE" in evs
        has_tsl_hit    = "TSL_HIT"    in evs
        is_closed = has_sl or has_tsl_hit or "CLOSED_MANUAL" in evs or "REVERSED" in evs

        if is_closed:
            if has_tsl_hit:
                cats["TSL + SL"]["count"] += 1
                cats["TSL + SL"]["pnl"]   += pnl
            elif has_tp4 and has_sl:
                cats["Tp4+SL"]["count"] += 1
                cats["Tp4+SL"]["pnl"]   += pnl
            elif has_tp3 and has_sl:
                cats["Tp3+SL"]["count"] += 1
                cats["Tp3+SL"]["pnl"]   += pnl
            elif has_tp2 and has_sl:
                cats["Tp2+SL"]["count"] += 1
                cats["Tp2+SL"]["pnl"]   += pnl
            elif has_tp1 and has_sl:
                cats["Tp1+SL"]["count"] += 1
                cats["Tp1+SL"]["pnl"]   += pnl
            elif has_sl:
                cats["stop los"]["count"] += 1
                cats["stop los"]["pnl"]   += pnl
        else:
            if has_tsl_active:
                cats["TSL(halen açık)"]["count"] += 1
                cats["TSL(halen açık)"]["pnl"]   += pnl
            elif has_tp4:
                cats["TP4(halen açık)"]["count"] += 1
                cats["TP4(halen açık)"]["pnl"]   += pnl
            elif has_tp3:
                cats["TP3(halen açık)"]["count"] += 1
                cats["TP3(halen açık)"]["pnl"]   += pnl
            elif has_tp2:
                cats["TP2(halen açık)"]["count"] += 1
                cats["TP2(halen açık)"]["pnl"]   += pnl
            elif has_tp1:
                cats["TP1(halen açık)"]["count"] += 1
                cats["TP1(halen açık)"]["pnl"]   += pnl
            else:
                cats["Giriş-Açık"]["count"] += 1
                cats["Giriş-Açık"]["pnl"]   += pnl

    cats["açılan"]["count"] = total_opened

    try:
        all_positions = client.get_position_risk()
        live_pnl = {}
        for p in all_positions:
            amt = float(p.get("positionAmt", 0))
            if amt != 0:
                live_pnl[p["symbol"]] = float(p.get("unRealizedProfit", 0))
    except Exception:
        live_pnl = {}

    for trade in trades.values():
        evs = trade["events"]
        sym = trade["symbol"]
        is_closed = ("SL_HIT" in evs or "TSL_HIT" in evs or
                     "CLOSED_MANUAL" in evs or "REVERSED" in evs)
        if not is_closed and sym in live_pnl:
            if "TSL_ACTIVE" in evs:
                cats["TSL(halen açık)"]["pnl"] += live_pnl[sym]
            elif "TP4_HIT" in evs:
                cats["TP4(halen açık)"]["pnl"] += live_pnl[sym]
            elif "TP3_HIT" in evs:
                cats["TP3(halen açık)"]["pnl"] += live_pnl[sym]
            elif "TP2_HIT" in evs:
                cats["TP2(halen açık)"]["pnl"] += live_pnl[sym]
            elif "TP1_HIT" in evs:
                cats["TP1(halen açık)"]["pnl"] += live_pnl[sym]
            else:
                cats["Giriş-Açık"]["pnl"] += live_pnl[sym]

    cats["açılan"]["pnl"] = sum(
        cats[c]["pnl"] for c in cats if c != "açılan"
    )

    total_pnl = sum(
        v["pnl"] for k, v in cats.items()
        if k not in ("açılan",)
    )

    lines = [
        f"📊 <b>İSTATİSTİK — {period_label}</b>",
        "━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"{'Kategori':<20} {'Adet':>5} {'Kazanç(USD)':>12}",
        "─────────────────────────────────",
    ]

    row_order = [
        "açılan", "stop los",
        "Tp1+SL", "Tp2+SL", "Tp3+SL", "Tp4+SL",
        "TSL + SL", "TSL(halen açık)", "Giriş-Açık",
        "TP1(halen açık)", "TP2(halen açık)", "TP3(halen açık)", "TP4(halen açık)",
    ]

    for cat in row_order:
        v = cats[cat]
        pnl_str = f"{v['pnl']:+.2f}" if v["pnl"] != 0 else "0"
        lines.append(f"{cat:<20} {v['count']:>5} {pnl_str:>12}")

    lines.append("─────────────────────────────────")
    lines.append(f"{'TOPLAM PNL':<20} {'':>5} {total_pnl:>+.2f}")

    return "\n".join(lines)


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


def notify_trade_opened(symbol, side, entry_price, tp_prices, sl_price, strategy: dict, atr_pct: float):
    emoji = "🟢" if side == "BUY" else "🔴"
    direction = "LONG (AL)" if side == "BUY" else "SHORT (SAT)"
    tps = strategy["tps"]
    msg = (
        f"{emoji} <b>YENİ POZİSYON</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"📌 <b>Coin:</b> {symbol}\n"
        f"📊 <b>Yön:</b> {direction}\n"
        f"⚡ <b>Kaldıraç:</b> {FIXED_LEVERAGE}x\n"
        f"💰 <b>Giriş:</b> {entry_price}\n"
        f"📈 <b>ATR%:</b> {atr_pct:.2f} → <b>{strategy['label']}</b>\n"
        f"━━━━━━━━━━━━━━━\n"
        f"🎯 <b>TP1 (%{tps[0]}):</b> {tp_prices[0]}  →  %{TP_QTY_PCT} kapat\n"
        f"🎯 <b>TP2 (%{tps[1]}):</b> {tp_prices[1]}  →  %{TP_QTY_PCT} kapat\n"
        f"🎯 <b>TP3 (%{tps[2]}):</b> {tp_prices[2]}  →  %{TP_QTY_PCT} kapat\n"
        f"🎯 <b>TP4 (%{tps[3]}):</b> {tp_prices[3]}  →  %{TP_QTY_PCT} kapat + TSL\n"
        f"🔒 <b>SL (%{strategy['sl']}):</b>  {sl_price}\n"
        f"📉 <b>TSL Callback:</b> %{strategy['tsl']}\n"
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
        f"⚡ {FIXED_LEVERAGE}x | ATR Bazlı SL/TP | TSL Dinamik\n"
        f"📈 ATR &lt;{ATR_MID_PCT}% → DAR | {ATR_MID_PCT}-{ATR_HIGH_PCT}% → ORTA | | &gt;{ATR_HIGH_PCT}% → Sinyal Yokgt;{ATR_HIGH_PCT}% → GENİŞ\n"
        "━━━━━━━━━━━━━━━\n"
        "📋 <b>Komutlar:</b>\n\n"
        "/sinyal — Manuel sinyal gönder\n"
        "/pozisyonlar — Açık pozisyonları göster\n"
        "/kapat [COIN] — Pozisyon kapat\n"
        "/bakiye — USDT bakiyesi\n"
        "/atr [COIN] — Coin ATR% göster\n"
        "/istatistik — Son 24 saatin özeti\n"
        "/istatistik_tarih — Tarih seçerek özet\n"
        "/istatistik 01.03.2026 — Tarihten itibaren özet\n"
        "/yardim — Bu menü\n"
    )
    await update.message.reply_text(msg, parse_mode="HTML")


async def cmd_bakiye(update: Update, context: ContextTypes.DEFAULT_TYPE):
    balance = get_usdt_balance()
    await update.message.reply_text(
        f"💰 <b>USDT Bakiye:</b> {balance:.2f} USDT",
        parse_mode="HTML"
    )


async def cmd_atr(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/atr BTCUSDT — Coin'in ATR% ve strateji tipini gösterir."""
    if not context.args:
        await update.message.reply_text(
            "⚠️ Kullanım: /atr BTCUSDT\nveya /atr BTC"
        )
        return

    raw = context.args[0].upper().replace(".P", "").replace("USDT", "") + "USDT"
    atr_pct  = get_atr_percent(raw)
    strategy = select_strategy(atr_pct)

    if atr_pct < 0:
        await update.message.reply_text(f"❌ {raw} için ATR alınamadı.")
        return

    log.info(f"{symbol} strateji: {strategy['label']} | SL:{strategy['sl']} TP:{strategy['tps']} TSL:{strategy['tsl']}")

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
    set_leverage(symbol, FIXED_LEVERAGE)
    valid_leverage = FIXED_LEVERAGE

    # ── Miktar ──────────────────────────────────────────────────
    quantity = calculate_quantity(symbol, valid_leverage, current_price, qty_precision)
    if quantity <= 0:
        return {"error": "Yetersiz bakiye"}

    # ── Pozisyon aç ─────────────────────────────────────────────
    order = place_order(symbol, side, quantity)
    if not order:
        return {"error": "Emir açılamadı"}

    entry_price = get_entry_price(order, symbol)
    log.info(f"Giriş fiyatı: {entry_price}")

    # ── ATR bazlı TP/SL fiyatları ───────────────────────────────
    tps = strategy["tps"]
    sl_pct  = strategy["sl"]
    tsl_cb  = strategy["tsl"]

    if side == "BUY":
        tp_prices = [round_to_tick(entry_price * (1 + t / 100), tick_size) for t in tps]
        sl_price  = round_to_tick(entry_price * (1 - sl_pct / 100), tick_size)
    else:
        tp_prices = [round_to_tick(entry_price * (1 - t / 100), tick_size) for t in tps]
        sl_price  = round_to_tick(entry_price * (1 + sl_pct / 100), tick_size)

    log.info(f"TP seviyeleri: {tp_prices} | SL: {sl_price} | TSL callback: {tsl_cb}%")

    # ── Emirleri yerleştir ──────────────────────────────────────
    place_tp_orders(symbol, side, quantity, entry_price, tp_prices, price_precision, qty_precision, tick_size)
    place_sl_order(symbol, side, quantity, sl_price, price_precision, tick_size)
    place_trailing_stop(symbol, side, quantity, tp_prices[3], tsl_cb, price_precision, qty_precision, tick_size)

    notify_trade_opened(symbol, side, entry_price, tp_prices, sl_price, strategy, atr_pct)

    log_trade_event("OPENED", symbol, side, extra={
        "entry_price": entry_price,
        "leverage":    valid_leverage,
        "atr_pct":     atr_pct,
        "strategy":    strategy["label"],
        "tp_prices":   tp_prices,
        "sl_price":    sl_price,
    })

    log.info(f"✅ {symbol} {side} tamamlandı | {valid_leverage}x | Giriş: {entry_price} | ATR%: {atr_pct:.2f}")
    return {"status": "ok", "symbol": symbol, "side": side, "leverage": valid_leverage, "strategy": strategy["label"]}


# ════════════════════════════════════════════════════════════════
# WEBHOOK — TP/SL/TSL CALLBACK ENDPOINT'İ
# ════════════════════════════════════════════════════════════════

@app.route("/algo_callback", methods=["POST"])
def algo_callback():
    try:
        data   = request.get_json(force=True)
        symbol = data.get("s", "")
        etype  = data.get("X", "")
        side   = data.get("S", "")
        pnl    = float(data.get("rp", 0) or 0)

        event_map = {
            "TAKE_PROFIT_MARKET":   "TP_HIT",
            "STOP_MARKET":          "SL_HIT",
            "TRAILING_STOP_MARKET": "TSL_HIT",
        }
        ev = event_map.get(etype, etype)
        log_trade_event(ev, symbol, side, pnl=pnl, extra={"raw": data})
        return jsonify({"ok": True}), 200
    except Exception as e:
        log.error(f"algo_callback hatası: {e}")
        return jsonify({"error": str(e)}), 500


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
        telegram_app.add_handler(CommandHandler("bakiye",           cmd_bakiye))
        telegram_app.add_handler(CommandHandler("pozisyonlar",      cmd_pozisyonlar))
        telegram_app.add_handler(CommandHandler("kapat",            cmd_kapat))
        telegram_app.add_handler(CommandHandler("atr",              cmd_atr))
        telegram_app.add_handler(CommandHandler("sinyal",           cmd_sinyal))
        telegram_app.add_handler(CommandHandler("istatistik",       cmd_istatistik))
        telegram_app.add_handler(CommandHandler("istatistik_tarih", cmd_istatistik_tarih))
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
# USER DATA STREAM — TP/SL/TSL TETİKLENME DİNLEYİCİSİ
# ════════════════════════════════════════════════════════════════

WS_BASE = "wss://fstream.binancefuture.com/ws/"

def get_listen_key():
    r = req.post(
        f"{BASE_URL}/fapi/v1/listenKey",
        headers={"X-MBX-APIKEY": API_KEY},
        timeout=10
    )
    return r.json().get("listenKey")


def keepalive_listen_key(listen_key):
    while True:
        time.sleep(30 * 60)
        try:
            req.put(
                f"{BASE_URL}/fapi/v1/listenKey",
                headers={"X-MBX-APIKEY": API_KEY},
                params={"listenKey": listen_key},
                timeout=10
            )
            log.info("Listen key yenilendi.")
        except Exception as e:
            log.error(f"Listen key yenilenemedi: {e}")


def on_order_event(msg: dict):
    event_type = msg.get("e")

    if event_type == "ORDER_TRADE_UPDATE":
        order  = msg.get("o", {})
        status = order.get("X")
        otype  = order.get("ot")
        symbol = order.get("s", "")
        side   = order.get("S", "")
        pnl    = float(order.get("rp", 0) or 0)

        if status != "FILLED":
            return

        is_algo        = bool(order.get("si"))
        is_liquidation = order.get("ot") == "LIQUIDATION"
        if not is_algo and not is_liquidation:
            return

        event_map = {
            "TAKE_PROFIT_MARKET":   "TP_HIT",
            "STOP_MARKET":          "SL_HIT",
            "TRAILING_STOP_MARKET": "TSL_HIT",
            "LIQUIDATION":          "SL_HIT",
        }
        ev = event_map.get(otype)
        if not ev:
            return

        log.info(f"📥 {ev} tetiklendi → {symbol} {side} PnL={pnl:.2f}")
        log_trade_event(ev, symbol, side, pnl=pnl)

        emoji = {"TP_HIT": "🎯", "SL_HIT": "🔴", "TSL_HIT": "📉"}.get(ev, "ℹ️")
        if order.get("ot") == "LIQUIDATION":
            label = "LİKİDASYON"
            emoji = "💥"
        else:
            label = {"TP_HIT": "TAKE PROFIT", "SL_HIT": "STOP LOSS", "TSL_HIT": "TRAILING STOP"}.get(ev, ev)
        send_telegram(
            f"{emoji} <b>{label} TETİKLENDİ</b>\n"
            f"📌 {symbol}\n"
            f"💵 PnL: {pnl:+.2f} USDT\n"
            f"🕐 {time.strftime('%H:%M:%S')}"
        )


def run_user_stream():
    while True:
        try:
            listen_key = get_listen_key()
            if not listen_key:
                log.error("Listen key alınamadı, 30sn sonra tekrar denenecek.")
                time.sleep(30)
                continue

            log.info(f"📡 User Data Stream bağlandı. key={listen_key[:10]}...")

            threading.Thread(
                target=keepalive_listen_key, args=(listen_key,), daemon=True
            ).start()

            def on_message(ws, message):
                try:
                    data = json.loads(message)
                    log.info(f"WS RAW: {json.dumps(data)}")
                    on_order_event(data)
                except Exception as e:
                    log.error(f"WS mesaj hatası: {e}")

            def on_error(ws, error):
                log.error(f"WS hata: {error}")

            def on_close(ws, *args):
                log.warning("WS kapandı, yeniden bağlanılıyor...")

            ws = websocket.WebSocketApp(
                WS_BASE + listen_key,
                on_message=on_message,
                on_error=on_error,
                on_close=on_close,
            )
            ws.run_forever(ping_interval=60, ping_timeout=10)

        except Exception as e:
            log.error(f"User stream hatası: {e}")

        time.sleep(5)


# ════════════════════════════════════════════════════════════════
# ANA BAŞLATMA
# ════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    log.info("🤖 Claude Trading Bot başlatıldı...")
    log.info(f"⚡ Strateji: {FIXED_LEVERAGE}x | ATR Bazlı SL/TP ")

    init_db()

    if TELEGRAM_TOKEN:
        t = threading.Thread(target=run_telegram, daemon=True)
        t.start()
        send_telegram(
            f"🚀 <b>Claude Trading Bot başlatıldı!</b>\n"
            f"⚡ {FIXED_LEVERAGE}x | ATR Bazlı SL/TP\n"
            f"📈 ATR &lt;{ATR_MID_PCT}% → DAR | {ATR_MID_PCT}-{ATR_HIGH_PCT}% → ORTA | | &gt;{ATR_HIGH_PCT}% → Sinyal Yokgt;{ATR_HIGH_PCT}% → GENİŞ\n"
            f"/yardim ile komutları görebilirsiniz."
        )
    else:
        log.warning("TELEGRAM_TOKEN tanımlı değil, Telegram devre dışı.")

    ws_thread = threading.Thread(target=run_user_stream, daemon=True)
    ws_thread.start()

    log.info(f"📡 Webhook dinleniyor: http://0.0.0.0:{PORT}/webhook")
    app.run(host="0.0.0.0", port=PORT, debug=False)
