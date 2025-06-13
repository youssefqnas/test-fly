import asyncio
import websockets
import json
import os
from datetime import datetime
import clickhouse_connect

# --- بيانات الاتصال بـ ClickHouse Cloud ---
CLICKHOUSE_HOST = "b2ldg6nk61.europe-west4.gcp.clickhouse.cloud"
CLICKHOUSE_USER = "default"
# <<<--- ضع هنا كلمة المرور الجديدة والسرية
CLICKHOUSE_PASSWORD = "8SLA3MyJ_12r0" 


# 🆕 تحديد الحد الأقصى لعدد الاتصالات المتزامنة
MAX_CONCURRENT_CONNECTIONS = 506 # يمكنك تعديل هذا الرقم، ابدأ بـ 5-10 وجرب

client = None
try:
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=8443,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        secure=True,
        settings={'max_threads': os.cpu_count() or 4} 
    )
    print("✅ تم الاتصال بنجاح بـ ClickHouse Cloud.")
except Exception as e:
    print(f"❌ فشل الاتصال بـ ClickHouse: {e}")
    exit()

def get_all_available_symbols():
    query = "SELECT symbol FROM symbols WHERE used = 0"
    result = client.query(query)
    if result.result_rows:
        return [row[0] for row in result.result_rows]
    return []

def mark_symbols_used(symbols: list[str]):
    if not symbols:
        return True 
    try:
        data_to_insert = [[symbol, 1] for symbol in symbols]
        client.insert("symbols", data_to_insert, column_names=['symbol', 'used'])
        print(f"✅ تم تحديث حالة {len(symbols)} رمز إلى 'مستخدم'.")
        return True
    except Exception as e:
        print(f"❌ حدث خطأ أثناء تحديث الرموز: {e}")
        return False

# 🆕 تعريف Semaphore كمتغير عام أو تمريره للدالة
connection_semaphore = asyncio.Semaphore(MAX_CONCURRENT_CONNECTIONS)

async def listen_trades(symbol: str):
    """
    (محسّن) يتصل بـ WebSocket، ويقوم بإعادة الاتصال تلقائيًا عند الفشل.
    """
    url = f"wss://stream.binance.com:9443/ws/{symbol.lower()}@trade"
    
    while True:
        # 🆕 هنا يتم الانتظار للحصول على "تصريح" للاتصال
        async with connection_semaphore: # سيسمح هذا بـ MAX_CONCURRENT_CONNECTIONS فقط في نفس الوقت
            try:
                async with websockets.connect(url, open_timeout=10) as ws: # 🆕 يمكن زيادة open_timeout
                    print(f"🔗 متصل على {symbol} trade stream... بدأ تخزين البيانات فورا.")
                    
                    while True:
                        message = await ws.recv()
                        data = json.loads(message)

                        trade_data = {
                            'symbol': data['s'],
                            'trade_time': datetime.fromtimestamp(data['T'] / 1000.0),
                            'trade_id': data['t'],
                            'price': float(data['p']),
                            'quantity': float(data['q']),
                        }
                        
                        try:
                            client.insert('trades', [list(trade_data.values())], column_names=list(trade_data.keys()))
                            # طباعة أقل تكراراً لتجنب إغراق الكونسول بالرسائل
                            # print(f"💾 [{symbol}] تم إدراج صف واحد في جدول trades.")
                        except Exception as insert_e:
                            print(f"❌ [{symbol}] حدث خطأ أثناء إدراج صف: {insert_e}")
            
            except (websockets.exceptions.ConnectionClosed, ConnectionRefusedError, asyncio.TimeoutError) as e: # 🆕 إضافة asyncio.TimeoutError
                print(f"🔌 [{symbol}] انقطع الاتصال أو فشل: {e}. إعادة المحاولة بعد 5 ثوانٍ...")
            except Exception as e:
                print(f"🔥 [{symbol}] حدث خطأ غير متوقع: {e}. إعادة المحاولة بعد 5 ثوانٍ...")
        
        await asyncio.sleep(5) # انتظار قبل إعادة محاولة الاتصال

async def main():
    symbols_to_process = get_all_available_symbols()
    
    if not symbols_to_process:
        print("🟡 لا يوجد أي رمز غير مستخدم حاليا.")
        return

    print(f"▶️ سيتم تشغيل المراقبة للرموز التالية: {symbols_to_process}")

    if mark_symbols_used(symbols_to_process):
        tasks = [listen_trades(symbol) for symbol in symbols_to_process]
        
        print(f"🚀 إطلاق {len(tasks)} مستمع... (اضغط Ctrl+C للإيقاف)")
        await asyncio.gather(*tasks)
    else:
        print(f"🛑 فشل في حجز الرموز. سيتم إنهاء البرنامج.")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nتم إيقاف البرنامج.")
    finally:
        if client:
            client.close()
            print("🔌 تم إغلاق الاتصال بـ ClickHouse.")