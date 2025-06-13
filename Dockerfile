# استخدم صورة Python خفيفة كقاعدة
FROM python:3.10-slim-buster

# تعيين مجلد العمل داخل الحاوية
WORKDIR /app

# نسخ ملف المتطلبات وتثبيت التبعيات
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# نسخ باقي ملفات التطبيق إلى مجلد العمل
COPY . .

# الأمر الذي سيتم تنفيذه عند بدء تشغيل الحاوية
CMD ["python", "main.py"]