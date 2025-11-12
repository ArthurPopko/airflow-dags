from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import boto3

def list_nyc_tlc_files():
    try:
        conn = BaseHook.get_connection("aws_default")
        aws_access_key = conn.login
        aws_secret_key = conn.password
        region_name = conn.extra_dejson.get("region_name", "us-east-1")

        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )

        resp = s3.list_objects_v2(
            Bucket="nyc-tlc",
            Prefix="trip data/"
        )

        contents = resp.get("Contents", [])
        if not contents:
            print("No files found in bucket.")
            return

        for obj in contents[:50]:
            print(obj["Key"])

    except Exception as e:
        print("Failed to list files:", e)
        raise

with DAG(
    dag_id="list_nyc_tlc_files",
    start_date=datetime(2025, 11, 12),
    schedule=None,
    catchup=False
) as dag:

    PythonOperator(
        task_id="list_files",
        python_callable=list_nyc_tlc_files
    )
