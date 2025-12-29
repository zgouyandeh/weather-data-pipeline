from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeSqlOperator

from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 1, 1),
}

with DAG(
    dag_id="snowflake_infrastructure_setup",
    default_args=default_args,
    schedule_interval="@once", # فقط یکبار اجرا شود
    template_searchpath="/opt/airflow/sql/", # آدرس پوشه فایل‌های SQL
    catchup=False,
) as dag:

    # Task to create tables and schemas
    setup_db_objects = SnowflakeSqlOperator(
        task_id="create_snowflake_objects",
        snowflake_conn_id="snowflake_weather",
        sql="setup_snowflake.sql" # ایرفلو در پوشه /sql/ به دنبال این فایل می‌گردد
    )