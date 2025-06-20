import asyncio
import functools
import json
import logging
import os
from datetime import datetime, timezone

import aiohttp
from clickhouse_driver import Client
from dotenv import load_dotenv

# ======================= الإعدادات ========================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
load_dotenv()

CLICKHOUSE_HOST = "b2ldg6nk61.europe-west4.gcp.clickhouse.cloud"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_DB_PASSWORD", "8SLA3MyJ_12r0")

SYMBOL = 'btcusdt'
BATCH_SIZE = 200
MAX_WAIT_SECONDS = 5

# ================== متغيرات تخزين ==================
trade_batch = []
client = None
flush_lock = asyncio.Lock()

last_printed_trade_price = 0.0

# ================== ClickHouse ==================
def connect_clickhouse():
    global client
    client = Client(
        host=CLICKHOUSE_HOST, user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        secure=True, port=9440
    )
    client.execute('SELECT 1')
    client.execute("""
    CREATE TABLE IF NOT EXISTS market_realtime (
        symbol LowCardinality(String),
        timestamp DateTime64(3, 'UTC'),
        price Float64,
        qty Float64,
        best_bid Float64,
        best_ask Float64
    ) ENGINE = MergeTree() PARTITION BY toYYYYMM(timestamp) ORDER BY (symbol, timestamp)
    """)
    logging.info("✅ تم الاتصال وإنشاء جدول market_realtime.")

# ================== إدخال البيانات ==================
def _blocking_insert_trades(batch):
    try:
        client.execute('INSERT INTO market_realtime (symbol, timestamp, price, qty, best_bid, best_ask) VALUES', batch)
        logging.info(f"📥 TRADE: تم إدخال {len(batch)} سجل في قاعدة البيانات.")
    except Exception as e:
        logging.error(f"❌ خطأ إدخال TRADE: {e}")

async def flush():
    global trade_batch
    if not client: return

    async with flush_lock:
        loop = asyncio.get_running_loop()
        if trade_batch:
            temp = trade_batch[:]
            trade_batch.clear()
            await loop.run_in_executor(None, functools.partial(_blocking_insert_trades, temp))

async def batch_writer():
    while True:
        await asyncio.sleep(MAX_WAIT_SECONDS)
        await flush()

# ================== التعامل مع الصفقات ==================
async def safe_append_trade(trade):
    async with flush_lock:
        trade_batch.append(trade)

def handle_trade(data):
    global last_printed_trade_price

    trade_for_db = {
        'symbol': data['s'],
        'timestamp': datetime.fromtimestamp(data['T'] / 1000, tz=timezone.utc),
        'price': float(data['p']),
        'qty': float(data['q']),
        'best_bid': 0.0,  # غير متوفر حالياً
        'best_ask': 0.0,  # غير متوفر حالياً
    }

    # ✅ نسجل الصفقة دايمًا
    asyncio.create_task(safe_append_trade(trade_for_db))

    # ✅ نطبع فقط لو السعر تغير
    current_price = trade_for_db['price']
    if current_price != last_printed_trade_price:
        trade_time_str = trade_for_db['timestamp'].strftime('%H:%M:%S.%f')[:-3]
        side = "🔼 شراء" if data['m'] else "🔽 بيع"
        print(f"📈 تغير السعر: {side} | السعر الجديد: {current_price:.2f} | الكمية: {trade_for_db['qty']:.5f} | الوقت: {trade_time_str}")
        last_printed_trade_price = current_price

# ================== WebSocket ==================
async def binance_ws():
    stream = f"{SYMBOL.lower()}@trade"
    url = f"wss://stream.binance.com:9443/ws/{stream}"
    logging.info(f"🔌 الاتصال بـ Binance WebSocket: {url}")

    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(url) as ws:
                    logging.info("✅ تم الاتصال بنجاح. في انتظار الصفقات...")
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            handle_trade(data)
                            if len(trade_batch) >= BATCH_SIZE:
                                await flush()
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logging.warning("🔁 الاتصال انقطع. إعادة المحاولة...")
                            break
        except Exception as e:
            logging.error(f"💥 خطأ في WebSocket: {e}")
            logging.info("⏳ الانتظار 5 ثواني...")
            await asyncio.sleep(5)

# ================== التشغيل ==================
async def main():
    try:
        connect_clickhouse()
    except Exception as e:
        logging.fatal(f"❌ فشل الاتصال بـ ClickHouse: {e}")
        return

    writer_task = asyncio.create_task(batch_writer())
    ws_task = asyncio.create_task(binance_ws())
    await asyncio.gather(writer_task, ws_task)

if __name__ == "__main__":
    if CLICKHOUSE_PASSWORD == "8SLA3MyJ_12r0":
        logging.warning("⚠️ من الأفضل تخزين كلمة المرور في ملف .env")
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("\n🛑 تم إيقاف البرنامج.")
