import logging
import os
import time
from datetime import datetime

import pandas as pd
from clickhouse_driver import Client
from dotenv import load_dotenv

# ======================= الإعدادات العامة ========================
# إعداد تسجيل الأحداث بشكل واضح
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# تحميل المتغيرات الحساسة من ملف .env
load_dotenv()

# ======================= إعدادات ClickHouse ========================
CLICKHOUSE_HOST = "b2ldg6nk61.europe-west4.gcp.clickhouse.cloud"
CLICKHOUSE_USER = "default"
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_DB_PASSWORD") # يُفضل دائمًا قراءتها من .env

# اسم الجدول المصدر الذي نقرأ منه
SOURCE_TABLE = 'market_realtime'
# اسم الجدول الجديد الذي سنخزن فيه النتائج
ANALYSIS_TABLE = 'hourly_price_percentage'

# ======================= إعدادات عملية التحليل ========================
# عدد السطور التي سيتم سحبها ومعالجتها في كل مرة
CHUNK_SIZE = 500_000
# مدة الانتظار (بالثواني) إذا لم تكن هناك بيانات جديدة للمعالجة
SLEEP_INTERVAL_SECONDS = 60

# متغير عالمي للاحتفاظ باتصال قاعدة البيانات
client = None

def connect_and_setup_db():
    """
    يقوم بالاتصال بقاعدة بيانات ClickHouse وإنشاء جدول التحليل إذا لم يكن موجودًا.
    """
    global client
    try:
        logging.info("🔌 جارٍ الاتصال بـ ClickHouse...")
        client = Client(
            host=CLICKHOUSE_HOST,
            user=CLICKHOUSE_USER,
            password=CLICKHOUSE_PASSWORD,
            secure=True,
            port=9440,
            # إضافة إعدادات لإعادة المحاولة التلقائية عند فشل الاتصال
            settings={'receive_timeout': 300, 'send_timeout': 300}
        )
        # التأكد من أن الاتصال يعمل
        client.execute('SELECT 1')
        logging.info("✅ تم الاتصال بنجاح بـ ClickHouse.")

        # إنشاء جدول النتائج. الجدول سيحتوي على النسبة المئوية للسعر والكمية
        # بالإضافة إلى بيانات الصف الأساسي (100%) لكل ساعة.
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {ANALYSIS_TABLE} (
            symbol LowCardinality(String),
            timestamp DateTime64(3, 'UTC'),
            base_timestamp DateTime64(3, 'UTC'),
            base_price Float64,
            price_percentage Float64,
            base_qty Float64,
            qty_percentage Float64
        ) ENGINE = MergeTree()
        PARTITION BY toYYYYMM(timestamp)
        ORDER BY (symbol, timestamp)
        """
        client.execute(create_table_query)
        logging.info(f"✅ تم التأكد من وجود جدول '{ANALYSIS_TABLE}'.")
        return True

    except Exception as e:
        logging.error(f"❌ فشل الاتصال أو إعداد قاعدة البيانات: {e}")
        return False

def get_last_processed_timestamp():
    """
    يحصل على آخرタイムスタンプ تم تحليله من جدول النتائج لتجنب إعادة العمل.
    """
    try:
        result = client.execute(f"SELECT max(timestamp) FROM {ANALYSIS_TABLE}")
        if result and result[0][0] is not None:
            last_ts = result[0][0]
            logging.info(f"🔍 آخر timestamp تمت معالجته هو: {last_ts}")
            return last_ts
        else:
            logging.info("🤔 جدول التحليل فارغ، سنبدأ من أول صف في الجدول المصدر.")
            # إذا كان جدول التحليل فارغًا، نبدأ من أول صف متاح في الجدول المصدر
            result = client.execute(f"SELECT min(timestamp) FROM {SOURCE_TABLE}")
            if result and result[0][0] is not None:
                 # نطرح ثانية واحدة لضمان إدراج أول صف
                first_ts = result[0][0] - pd.Timedelta(seconds=1)
                logging.info(f"🌅 أول timestamp في المصدر هو: {result[0][0]}. سنبدأ من {first_ts}.")
                return first_ts
            return None # لا يوجد بيانات على الإطلاق
    except Exception as e:
        logging.error(f"⚠️ لم نتمكن من الحصول على آخر timestamp: {e}")
        return None

def process_data_chunk(df: pd.DataFrame):
    """
    الوظيفة الأساسية للتحليل: تحسب النسب المئوية للسعر والكمية لكل ساعة.
    """
    if df.empty:
        return None

    logging.info(f"🧠 بدء تحليل {len(df)} صف...")

    # 1. تحديد مجموعة الساعة لكل صف (مثال: 09:15 -> 09:00)
    df['hour_group'] = df['timestamp'].dt.floor('H')

    # 2. إيجاد الصف "الأساسي" (أول صف) لكل ساعة
    # نستخدم transform('first') لنسخ قيمة أول صف لكل الصفوف في نفس المجموعة
    base_values = df.groupby('hour_group')[['timestamp', 'price', 'qty']].transform('first')
    
    # 3. دمج القيم الأساسية مع الداتا فريم الأصلي
    df['base_timestamp'] = base_values['timestamp']
    df['base_price'] = base_values['price']
    df['base_qty'] = base_values['qty']

    # 4. حساب النسبة المئوية
    # نضيف قيمة صغيرة جدًا للمقام لتجنب القسمة على صفر
    epsilon = 1e-9
    df['price_percentage'] = (df['price'] / (df['base_price'] + epsilon)) * 100
    df['qty_percentage'] = (df['qty'] / (df['base_qty'] + epsilon)) * 100

    # 5. تجهيز البيانات للإدخال في قاعدة البيانات
    # اختيار الأعمدة النهائية بالترتيب الصحيح
    output_df = df[[
        'symbol', 'timestamp', 'base_timestamp', 'base_price',
        'price_percentage', 'base_qty', 'qty_percentage'
    ]]

    # تحويل الأعمدة الرقمية لضمان التوافق مع ClickHouse
    for col in ['price_percentage', 'qty_percentage']:
        output_df[col] = pd.to_numeric(output_df[col], errors='coerce').fillna(0)

    logging.info(f"✅ اكتمل تحليل {len(output_df)} صف.")
    return output_df

def analysis_loop():
    """
    الحلقة الرئيسية التي تسحب البيانات، تحللها، وتخزنها بشكل مستمر.
    """
    while True:
        last_ts = get_last_processed_timestamp()
        
        # إذا لم يكن هناك أي بيانات على الإطلاق، ننتظر
        if last_ts is None:
            logging.info(f"لا توجد بيانات للمعالجة. الانتظار لمدة {SLEEP_INTERVAL_SECONDS} ثانية...")
            time.sleep(SLEEP_INTERVAL_SECONDS)
            continue
            
        try:
            # 2. سحب الدفعة التالية من البيانات
            query = f"""
            SELECT symbol, timestamp, price, qty
            FROM {SOURCE_TABLE}
            WHERE timestamp > %(last_ts)s
            ORDER BY timestamp
            LIMIT {CHUNK_SIZE}
            """
            logging.info(f"🚚 سحب دفعة بيانات جديدة بحجم يصل إلى {CHUNK_SIZE} صف...")
            
            # استخدام query_dataframe لسحب البيانات مباشرة إلى Pandas DataFrame
            df = client.query_dataframe(query, params={'last_ts': last_ts})

            if df.empty:
                logging.info(f"😴 لا توجد بيانات جديدة. الانتظار لمدة {SLEEP_INTERVAL_SECONDS} ثانية...")
                time.sleep(SLEEP_INTERVAL_SECONDS)
                continue

            # 3. معالجة البيانات
            analysis_result_df = process_data_chunk(df)

            # 4. تخزين النتائج في الجدول الجديد
            if analysis_result_df is not None and not analysis_result_df.empty:
                logging.info(f"📥 جارٍ إدخال {len(analysis_result_df)} صف نتيجة في جدول '{ANALYSIS_TABLE}'...")
                client.insert_dataframe(f'INSERT INTO {ANALYSIS_TABLE} VALUES', analysis_result_df)
                logging.info("🎉 تم تخزين النتائج بنجاح!")

                # إذا قمنا بمعالجة دفعة كاملة، فمن المحتمل وجود المزيد من البيانات، لذا نستمر فورًا
                if len(df) < CHUNK_SIZE:
                    logging.info("وصلنا إلى نهاية البيانات الحالية. سننتظر قبل المحاولة مرة أخرى.")
                    time.sleep(SLEEP_INTERVAL_SECONDS)

        except Exception as e:
            logging.critical(f"💥 حدث خطأ فادح في حلقة التحليل الرئيسية: {e}")
            logging.info(f"🔌 سنحاول إعادة الاتصال والانتظار لمدة {SLEEP_INTERVAL_SECONDS} ثانية...")
            time.sleep(SLEEP_INTERVAL_SECONDS)
            # محاولة إعادة الاتصال في حال انقطاعه
            connect_and_setup_db()


def main():
    """
    نقطة الدخول الرئيسية للتطبيق.
    """
    if not CLICKHOUSE_PASSWORD:
        logging.error("❌ كلمة مرور ClickHouse غير موجودة. قم بإنشاء ملف .env وضع فيه CLICKHOUSE_DB_PASSWORD=your_password")
        return

    if not connect_and_setup_db():
        logging.fatal("لا يمكن بدء التطبيق بدون اتصال بقاعدة البيانات. الخروج.")
        return
        
    analysis_loop()

if __name__ == "__main__":
    main()
