import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook



# --- Constants (دقیقاً مطابق با Producer شما) ---
RAW_BUCKET = "weather-data"
KAFKA_CONN_ID = "kafka_weather"
S3_CONN_ID = "minio_s3"
SNOWFLAKE_CONN_ID = "snowflake_weather"

# اصلاح نام تاپیک‌ها مطابق با کد پرودیوسر شما
TOPIC_WEATHER = "weather_data_complex"
TOPIC_AQI = "air_quality_complex"
TOPICS = [TOPIC_WEATHER, TOPIC_AQI]

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# --- Task 1: Kafka → MinIO ---
def ingest_all_topics_to_minio():
    from kafka import KafkaConsumer
    kafka_conn = BaseHook.get_connection(KAFKA_CONN_ID)
    bootstrap_server = f"{kafka_conn.host}:{kafka_conn.port}"
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    
    if not s3.check_for_bucket(RAW_BUCKET):
        s3.create_bucket(RAW_BUCKET)

    for topic in TOPICS:
        logger.info(f"Connecting to Kafka topic: {topic}")
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[bootstrap_server],
            group_id="airflow_consumer_group_v7", 
            auto_offset_reset="earliest",
            consumer_timeout_ms=5000,
            value_deserializer=lambda x: json.loads(x.decode("utf-8")),
        )

        for message in consumer:
            data = message.value
            # منطق دسته بندی: اگر شهر وجود داشت valid وگرنه quarantine
            is_valid = bool(data.get("city"))
            status_folder = "valid" if is_valid else "quarantine"
            
            city = data.get("city") if data.get("city") else "unknown_city"
            timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S_%f")
            
            # ذخیره در مسیر Bronze
            key = f"bronze/{topic}/{status_folder}/city={city}/{timestamp}.json"
            s3.load_string(json.dumps(data), key, RAW_BUCKET, replace=True)
            
    logger.info("Kafka to MinIO ingestion completed.")

# --- Task 2: MinIO → Snowflake ---
def load_to_snowflake_via_internal_stage():
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    s3 = S3Hook(aws_conn_id=S3_CONN_ID)
    
    conn = sf_hook.get_conn()
    cs = conn.cursor()

    BATCH_SIZE = 1000  # محدودیت برای هر اجرا

    try:
        for topic in TOPICS:
            table_name = "WEATHER_DB.RAW.RAW_WEATHER_DATA_COMPLEX" if topic == TOPIC_WEATHER else "WEATHER_DB.RAW.RAW_AIR_QUALITY_COMPLEX"
            
            prefix = f"bronze/{topic}/valid/"
            all_keys = s3.list_keys(bucket_name=RAW_BUCKET, prefix=prefix)
            
            if not all_keys:
                logger.info(f"No new files for {topic}")
                continue

            # انتخاب فقط ۱۰۰۰ فایل اول برای این Batch
            keys_to_process = all_keys[:BATCH_SIZE]
            logger.info(f"Processing batch of {len(keys_to_process)} files for {topic}...")

            # مرحله ۱: انتقال فایل‌ها به Stage اسنوفلیک
            processed_in_this_run = []
            for k in keys_to_process:
                content = s3.read_key(k, bucket_name=RAW_BUCKET)
                with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as tf:
                    tf.write(content)
                    temp_path = tf.name
                
                try:
                    cs.execute(f"PUT 'file://{temp_path}' @WEATHER_DB.RAW.MY_INTERNAL_STAGE/{topic}/")
                    processed_in_this_run.append(k)
                finally:
                    if os.path.exists(temp_path):
                        os.remove(temp_path)

            # مرحله ۲: اجرای یکباره دستور کپی برای کل Batch
            if processed_in_this_run:
                copy_sql = f"""
                    COPY INTO {table_name} (JSON_DATA)
                    FROM @WEATHER_DB.RAW.MY_INTERNAL_STAGE/{topic}/
                    FILE_FORMAT = (TYPE = 'JSON')
                    PURGE = TRUE;
                """
                cs.execute(copy_sql)
                logger.info(f"Successfully loaded {len(processed_in_this_run)} records into {table_name}")

                # مرحله ۳: جابجایی فایل‌های موفق به پوشه processed
                for k in processed_in_this_run:
                    processed_key = k.replace("bronze/", "processed/")
                    s3.copy_object(
                        source_bucket_key=k, dest_bucket_key=processed_key,
                        source_bucket_name=RAW_BUCKET, dest_bucket_name=RAW_BUCKET
                    )
                    s3.delete_objects(bucket=RAW_BUCKET, keys=[k])
                
                logger.info(f"Batch movement to processed folder finished.")

    finally:
        cs.close()
        conn.close()

        
# --- DAG Structure ---
with DAG(
    dag_id="weather_pipeline_final",
    default_args=default_args,
    schedule=timedelta(minutes=5),
    max_active_runs=1, # اجازه نده بیش از یک اجرای فعال همزمان وجود داشته باشد
    catchup=False,
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_kafka_to_minio",
        python_callable=ingest_all_topics_to_minio,
    )

    load_task = PythonOperator(
        task_id="load_minio_to_snowflake",
        python_callable=load_to_snowflake_via_internal_stage,
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_bronze_transform",
        trigger_dag_id="weather_transformation_bronze_v2_medallion",
    )

    ingest_task >> load_task >> trigger_transform