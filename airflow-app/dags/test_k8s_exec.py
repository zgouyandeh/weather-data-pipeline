from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kubernetes.client import models as k8s
import os

# تابع اصلی که قرار است اجرا شود
def connect_to_db():
    # خواندن پسورد از متغیر محیطی که کوبرنتیز تزریق کرده است
    db_pass = os.getenv('MY_DB_PASSWORD')
    print("Hello from the data-pipeline namespace!")
    if db_pass:
        # برای امنیت فقط دو حرف اول را چاپ می‌کنیم
        print(f"Connecting to DB with password: {db_pass[:2]}***")
    else:
        print("Error: Secret not found in environment variables!")

# تعریف متغیر محیطی که از Secret کوبرنتیز مقدار می‌گیرد
# این بخش می‌تواند بیرون یا داخل DAG باشد، اما بهتر است برای نظم داخل باشد
secret_env = k8s.V1EnvVar(
    name='MY_DB_PASSWORD',
    value_from=k8s.V1EnvVarSource(
        secret_key_ref=k8s.V1SecretKeySelector(
            name='airflow-db-secret', # نام سکرتی که با فایل YAML ساختیم
            key='DB_PASSWORD'         # کلیدی که در فایل YAML تعریف کردیم
        )
    )
)

with DAG(
    dag_id='test_kubernetes_executor_with_secret',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id='run_in_k8s_with_secret',
        python_callable=connect_to_db,
        # بخش حیاتی: تنظیمات اختصاصی برای پادِ این تسک
        executor_config={
            "pod_override": k8s.V1Pod(
                spec=k8s.V1PodSpec(
                    containers=[
                        k8s.V1Container(
                            name="base", # نام کانتینر اصلی در پادهای ایرفلو همیشه base است
                            env=[secret_env] # تزریق سکرت
                        )
                    ]
                )
            )
        }
    )