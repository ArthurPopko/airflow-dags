from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import boto3

def check_aws_connection_from_conn():
    try:
        # Получаем креды из Airflow Connection
        conn = BaseHook.get_connection("aws_default")
        aws_access_key = conn.login
        aws_secret_key = conn.password
        region_name = conn.extra_dejson.get("region_name", "us-east-1")

        # Создаём boto3 клиент STS с этими кредами
        sts = boto3.client(
            "sts",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )

        # Проверяем соединение
        identity = sts.get_caller_identity()
        print("Connected to AWS successfully!")
        print("Account:", identity["Account"])
        print("UserId:", identity["UserId"])
        print("ARN:", identity["Arn"])

    except Exception as e:
        print("AWS connection failed:", e)
        raise

with DAG(
    dag_id="check_aws_connection_boto3_conn",
    start_date=datetime(2025, 11, 12),
    schedule=None,
    catchup=False
) as dag:

    PythonOperator(
        task_id="check_connection_boto3_conn",
        python_callable=check_aws_connection_from_conn
    )
