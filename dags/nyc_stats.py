from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def list_files_from_s3():
    # Используем Connection с aws_conn_id="my_aws_conn"
    hook = S3Hook(aws_conn_id="aws_default")
    
    # Получаем список файлов в бакете с префиксом
    keys = hook.list_keys(bucket_name="nyc-tlc", prefix="trip data/")
    
    # Выводим первые 50 файлов
    for k in keys[:50]:
        print(k)

with DAG(
    dag_id="list_nyc_tlc_files",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    PythonOperator(
        task_id="list_files",
        python_callable=list_files_from_s3
    )
