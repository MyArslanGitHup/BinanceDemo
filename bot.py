"""
Claude Trading Bot — Binance Futures
TradingView webhook → Binance Futures işlem açma botu
+ Telegram Bot entegrasyonu

SABİT STRATEJİ:
  Kaldıraç : 10x
  TP1 %1.0  → %20 kapat
  TP2 %2.0  → %20 kapat
  TP3 %3.0  → %20 kapat
  TP4 %4.0  → %20 kapat  + TSL devreye girer (callback %3.0)
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

# ─── SABİT STRATEJİ ─────────────────────────────────────────────
FIXED_LEVERAGE   = 10
TP_STEP_PCT      = 1.0
SL_PCT           = 2.0
TSL_CALLBACK_PCT = 3.0
TP_QTY_PCT       = 20

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
        # dict listesine çevir, ts'yi string yap
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
    """
    Kayıtlardan tablodaki gibi istatistik mesajı üretir.

    Kategoriler:
      açılan        → OPENED
      stop los      → SL_HIT (TP tetiklenmeden SL)
      Tp1+SL        → TP1_HIT + SL_HIT aynı trade'de
      Tp2+SL        → TP2_HIT + SL_HIT
      Tp3+SL        → TP3_HIT + SL_HIT
      Tp4+SL        → TP4_HIT + SL_HIT
      TSL + SL      → TSL_HIT
      TSL(halen açık) → TSL_ACTIVE (TSL tetiklendi ama pozisyon hâlâ açık)
      Giriş-Açık    → OPENED ama henüz hiç TP/SL/TSL yok
      TP1(halen açık) → TP1_HIT ama pozisyon hâlâ açık
      TP2(halen açık) → TP2_HIT
      TP3(halen açık) → TP3_HIT
      TP4(halen açık) → TP4_HIT
    """

    # trade bazında grupla: symbol + açılış ts ile
    # Her OPENED bir trade başlatır, sonraki eventler ona bağlanır
    trades = {}   # key: (symbol, open_ts_str)
    orphans = []  # OPENED olmadan gelen eventler

    opened_by_symbol = {}  # son açılan trade key'ini tutar per symbol

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
                orphans.append(rec)

    # Kategori sayaçları
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

        # Kapalı pozisyon kategorileri
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
            # Hâlâ açık pozisyon kategorileri
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
    cats["açılan"]["pnl"]   = sum(t["pnl"] or 0 for t in trades.values())

    total_pnl = sum(
        v["pnl"] for k, v in cats.items()
        if k not in ("açılan",)
    )

    # Mesaj oluştur
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

        # İstatistik logu
        log_trade_event("CLOSED_MANUAL", symbol, "BUY" if amt > 0 else "SELL", pnl=pnl)

        await update.message.reply_text(
            f"✅ <b>{symbol}</b> kapatıldı (tüm emirler iptal edildi)\n💵 PnL: {pnl:.2f} USDT",
            parse_mode="HTML"
        )
    except Exception as e:
        await update.message.reply_text(f"❌ {symbol} kapatılamadı: {e}")


# ─── İSTATİSTİK KOMUTU ──────────────────────────────────────────

async def cmd_istatistik(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Kullanım:
      /istatistik           → son 24 saat
      /istatistik 01.03.2026 → o tarihten (00:00 UTC) itibaren
    """
    now = datetime.now(timezone.utc)

    # Tüm mesajı al, komut kısmını çıkar
    full_text = update.message.text.strip()
    parts = full_text.split(maxsplit=1)
    date_str = parts[1].strip() if len(parts) > 1 else ""

    if date_str:
        try:
            since_dt = datetime.strptime(date_str, "%d.%m.%Y").replace(tzinfo=timezone.utc)
            period_label = f"{date_str} – şimdi arası"
        except ValueError:
            await update.message.reply_text(
                "⚠️ Tarih formatı hatalı.\n"
                "Doğru format: /istatistik 01.03.2026"
            )
            return
    else:
        from datetime import timedelta
        since_dt = now - timedelta(hours=24)
        period_label = "Son 24 saat"

    await update.message.reply_text("⏳ İstatistikler hesaplanıyor...")

    records = read_stats_log(since_dt)

    if not records:
        await update.message.reply_text(
            f"📭 <b>{period_label}</b> için kayıt bulunamadı.",
            parse_mode="HTML"
        )
        return

    msg = build_stats_message(records, period_label)
    await update.message.reply_text(f"<pre>{msg}</pre>", parse_mode="HTML")


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


async def cmd_istatistik_tarih(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """/istatistik_tarih — kullanıcıdan tarih girmesini ister."""
    user_states[update.effective_user.id] = {"step": "tarih"}
    await update.message.reply_text(
        "📅 <b>Tarihten İtibaren İstatistik</b>\n"
        "━━━━━━━━━━━━━━━\n"
        "Başlangıç tarihini girin:\n"
        "<i>Örnek: 01.03.2026</i>",
        parse_mode="HTML"
    )


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    uid = update.effective_user.id
    if uid not in user_states:
        return

    state = user_states[uid]
    text  = update.message.text.strip()

    # ── Tarih adımı ─────────────────────────────────────────────
    if state["step"] == "tarih":
        try:
            since_dt = datetime.strptime(text, "%d.%m.%Y").replace(tzinfo=timezone.utc)
            period_label = f"{text} – şimdi arası"
        except ValueError:
            await update.message.reply_text(
                "⚠️ Format hatalı. Tekrar deneyin:\n<i>Örnek: 01.03.2026</i>",
                parse_mode="HTML"
            )
            return  # state'i silme, tekrar girsin

        user_states.pop(uid, None)
        await update.message.reply_text("⏳ İstatistikler hesaplanıyor...")
        records = read_stats_log(since_dt)
        if not records:
            await update.message.reply_text(
                f"📭 <b>{period_label}</b> için kayıt bulunamadı.",
                parse_mode="HTML"
            )
            return
        msg = build_stats_message(records, period_label)
        await update.message.reply_text(f"<pre>{msg}</pre>", parse_mode="HTML")
        return

    # ── Coin adımı ──────────────────────────────────────────────
    text = text.upper()
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
    trade_usdt = TRADE_USDT_FIXED * leverage
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

    try:
        r = send_signed_request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})
        log.info(f"{symbol} allOpenOrders iptal: {r}")
    except Exception as e:
        log.warning(f"{symbol} allOpenOrders iptal hatası: {e}")

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

        # İstatistik logu
        log_trade_event("REVERSED", symbol, old_side, pnl=pnl)

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
    set_leverage(symbol, FIXED_LEVERAGE)
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

    # ── İstatistik logu: pozisyon açıldı ───────────────────────
    log_trade_event("OPENED", symbol, side, extra={
        "entry_price": entry_price,
        "leverage":    valid_leverage,
        "tp_prices":   tp_prices,
        "sl_price":    sl_price,
    })

    log.info(f"✅ {symbol} {side} tamamlandı | {valid_leverage}x | Giriş: {entry_price}")
    return {"status": "ok", "symbol": symbol, "side": side, "leverage": valid_leverage}


# ════════════════════════════════════════════════════════════════
# WEBHOOK — TP/SL/TSL CALLBACK ENDPOINT'İ
# (Binance algo emirleri tetiklendiğinde buraya POST atar — opsiyonel)
# ════════════════════════════════════════════════════════════════

@app.route("/algo_callback", methods=["POST"])
def algo_callback():
    """
    Binance'in algo emir callback'i (eğer URL ayarladıysanız).
    Gelen veriyi parse edip istatistik loguna yazar.
    Kullanmıyorsanız bu endpoint'i görmezden gelin.
    """
    try:
        data   = request.get_json(force=True)
        symbol = data.get("s", "")
        etype  = data.get("X", "")   # ORDER_TYPE: TAKE_PROFIT_MARKET, STOP_MARKET, TRAILING_STOP_MARKET
        side   = data.get("S", "")   # BUY / SELL
        pnl    = float(data.get("rp", 0) or 0)

        event_map = {
            "TAKE_PROFIT_MARKET": "TP_HIT",
            "STOP_MARKET":        "SL_HIT",
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
        telegram_app.add_handler(CommandHandler("bakiye",      cmd_bakiye))
        telegram_app.add_handler(CommandHandler("pozisyonlar", cmd_pozisyonlar))
        telegram_app.add_handler(CommandHandler("kapat",       cmd_kapat))
        telegram_app.add_handler(CommandHandler("sinyal",            cmd_sinyal))
        telegram_app.add_handler(CommandHandler("istatistik",        cmd_istatistik))
        telegram_app.add_handler(CommandHandler("istatistik_tarih",  cmd_istatistik_tarih))  # ← YENİ
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

WS_BASE = "wss://fstream.binance.com/ws/"
# Demo hesap kullanıyorsan yukarıdaki satırı şununla değiştir:
# WS_BASE = "wss://stream.binancefuture.com/ws/"

def get_listen_key():
    r = req.post(
        f"{BASE_URL}/fapi/v1/listenKey",
        headers={"X-MBX-APIKEY": API_KEY},
        timeout=10
    )
    return r.json().get("listenKey")


def keepalive_listen_key(listen_key):
    """Her 30 dakikada bir listen key'i yenile."""
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
    """TP / SL / TSL tetiklendiğinde çağrılır."""
    if msg.get("e") != "ORDER_TRADE_UPDATE":
        return

    order  = msg.get("o", {})
    status = order.get("X")           # FILLED, CANCELED, NEW ...
    otype  = order.get("ot")          # emir tipi
    symbol = order.get("s", "")
    side   = order.get("S", "")       # BUY / SELL
    pnl    = float(order.get("rp", 0) or 0)

    if status != "FILLED":
        return

    event_map = {
        "TAKE_PROFIT_MARKET":   "TP_HIT",
        "STOP_MARKET":          "SL_HIT",
        "TRAILING_STOP_MARKET": "TSL_HIT",
    }
    ev = event_map.get(otype)
    if not ev:
        return

    log.info(f"📥 {ev} tetiklendi → {symbol} {side} PnL={pnl:.2f}")
    log_trade_event(ev, symbol, side, pnl=pnl)

    emoji = {"TP_HIT": "🎯", "SL_HIT": "🔴", "TSL_HIT": "📉"}.get(ev, "ℹ️")
    label = {"TP_HIT": "TAKE PROFIT", "SL_HIT": "STOP LOSS", "TSL_HIT": "TRAILING STOP"}.get(ev, ev)
    send_telegram(
        f"{emoji} <b>{label} TETİKLENDİ</b>\n"
        f"📌 {symbol}\n"
        f"💵 PnL: {pnl:+.2f} USDT\n"
        f"🕐 {time.strftime('%H:%M:%S')}"
    )


def run_user_stream():
    """User Data Stream WebSocket'i başlat, kopunca yeniden bağlan."""
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
                    on_order_event(json.loads(message))
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
    log.info(f"⚡ Strateji: {FIXED_LEVERAGE}x | SL %{SL_PCT} | TP aralığı %{TP_STEP_PCT} | TSL %{TSL_CALLBACK_PCT}")

    init_db()  # PostgreSQL tablosunu oluştur

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

    # ── User Data Stream başlat (TP/SL/TSL dinleyici) ──────────
    ws_thread = threading.Thread(target=run_user_stream, daemon=True)
    ws_thread.start()

    log.info(f"📡 Webhook dinleniyor: http://0.0.0.0:{PORT}/webhook")
    app.run(host="0.0.0.0", port=PORT, debug=False)
