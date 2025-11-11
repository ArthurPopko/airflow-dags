import boto3
from botocore import UNSIGNED
from botocore.config import Config

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

def list_files_from_s3():
    s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    resp = s3.list_objects_v2(
        Bucket="nyc-tlc",
        Prefix="trip data/"
    )
    for obj in resp.get("Contents", [])[:50]:
        print(obj["Key"])

with DAG(
    dag_id="list_nyc_tlc_files",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
):
    PythonOperator(
        task_id="list_files",
        python_callable=list_files_from_s3
    )
