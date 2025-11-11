from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime

def list_files_from_s3():
    # Create S3 hook using default AWS connection
    hook = S3Hook(aws_conn_id="aws_default")
    
    # List all files in the bucket
    keys = hook.list_keys(bucket_name="nyc-tlc")
    
    # Print first 50 files to Airflow logs
    # (Too many files → limit for readability)
    for k in keys[:50]:
        print(k)

with DAG(
    dag_id="list_nyc_tlc_files",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # run manually
    catchup=False,
    tags=["s3", "example"],
):

    list_task = PythonOperator(
        task_id="list_files",
        python_callable=list_files_from_s3,
    )
