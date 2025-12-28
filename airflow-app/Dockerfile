FROM apache/airflow:2.7.1-python3.10

USER root
# نصب ابزارهای لازم برای پکیج‌های پایتونی که نیاز به کامپایل دارند
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# کپی کردن فایل requirements و نصب پکیج‌ها
COPY requirements.txt .
RUN pip install --no-cache-dir --user -r requirements.txt