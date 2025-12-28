from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

SNOWFLAKE_CONN_ID = "snowflake_weather"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="weather_transformation_bronze_v2_medallion",
    default_args=default_args,
    schedule_interval=None, # Triggered by the Loader DAG
    template_searchpath="/opt/airflow/sql/", # Important: Tell Airflow where to look for SQL files
    catchup=False,
) as dag:

    # Task 1: Process Weather Data
    transform_weather = SnowflakeOperator(
        task_id="transform_weather_stream",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="merge_weather.sql"
    )

    # Task 2: Process Air Quality Data
    transform_aqi = SnowflakeOperator(
        task_id="transform_aqi_stream",
        snowflake_conn_id=SNOWFLAKE_CONN_ID,
        sql="merge_air_quality.sql"
    )

    # Parallel execution
    [transform_weather, transform_aqi]