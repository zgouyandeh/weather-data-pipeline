FROM apache/airflow:2.7.1

# نصب کتابخانه‌های مورد نیاز به صورت دائمی
RUN pip install --no-cache-dir \
    kafka-python \
    boto3 \
    snowflake-connector-python \
    apache-airflow-providers-amazon