from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os # برای خواندن متغیرهای سیستم

def check_secret_function():
    # ما به ایرفلو می‌گوییم رمز را در متغیری به نام 'DB_PASSWORD' برای ما بگذارد
    my_password = os.getenv('DB_PASSWORD')
    
    print("--- شروع عملیات بررسی رمز ---")
    if my_password:
        print(f"تبریک! رمز با موفقیت خوانده شد: {my_password}")
    else:
        print("خطا: رمز پیدا نشد!")
    print("--- پایان عملیات ---")

with DAG(
    dag_id='simple_secret_test',
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    task = PythonOperator(
        task_id='read_secret_task',
        python_callable=check_secret_function,
        # بخش زیر تنها بخشی است که شاید کمی عجیب باشد
        # اینجا فقط داریم آدرس گاوصندوق را به ایرفلو می‌دهیم
        executor_config={
            "pod_override": {
                "spec": {
                    "containers": [{
                        "name": "base",
                        "env": [{
                            "name": "DB_PASSWORD", # نامی که در پایتون استفاده کردیم
                            "valueFrom": {
                                "secretKeyRef": {
                                    "name": "my-db-secret", # نام گاوصندوق
                                    "key": "password"      # نام کلید داخل گاوصندوق
                                }
                            }
                        }]
                    }]
                }
            }
        }
    )