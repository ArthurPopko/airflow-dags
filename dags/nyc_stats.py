from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def check_aws_connection():
    try:
        # Используем Connection aws_default (или своё имя)
        hook = S3Hook(aws_conn_id="aws_default")
        
        # Просто проверяем, можем ли получить список бакетов
        buckets = hook.list_buckets()
        print("Connected to AWS successfully!")
        print("Buckets available:", buckets)
    except Exception as e:
        print("AWS connection failed:", e)
        raise

with DAG(
    dag_id="check_aws_connection",
    start_date=datetime(2025, 11, 11),
    schedule=None,
    catchup=False
) as dag:

    PythonOperator(
        task_id="check_connection",
        python_callable=check_aws_connection
    )
