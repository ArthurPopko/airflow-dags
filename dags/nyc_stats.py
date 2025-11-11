from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import boto3

def check_aws_credentials():
    try:
        sts = boto3.client("s3")
        
        identity = sts.get_caller_identity()
        print("Connected to AWS successfully!")
        print("Account:", identity["Account"])
        print("UserId:", identity["UserId"])
        print("ARN:", identity["Arn"])
    except Exception as e:
        print("AWS connection failed:", e)
        raise

with DAG(
    dag_id="check_aws_connection_boto3",
    start_date=datetime(2025, 11, 11),
    schedule=None,
    catchup=False
) as dag:

    PythonOperator(
        task_id="check_connection",
        python_callable=check_aws_credentials
    )
