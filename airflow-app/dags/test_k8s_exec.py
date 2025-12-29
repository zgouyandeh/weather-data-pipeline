from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello_kubernetes():
    print("Hello from the data-pipeline namespace!")
    print("This task is running as a separate Pod.")

with DAG(
    dag_id='test_kubernetes_executor',
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:

    test_task = PythonOperator(
        task_id='run_in_k8s',
        python_callable=hello_kubernetes
    )