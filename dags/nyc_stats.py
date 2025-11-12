from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS = ["2024-06", "2024-07", "2024-08", "2024-09", "2024-10", "2024-11"]
CAB_TYPES = ["yellow", "green"]
S3_BUCKET = "nyc-tlc-stats"
S3_PREFIX = "nyc_tlc_data/"
AWS_REGION = "eu-central-1"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="nyc_tlc_stream_to_s3",
    start_date=datetime(2024, 12, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Stream 6 months of NYC TLC Yellow/Green Cab data directly to S3 without local storage",
) as dag:

    for cab in CAB_TYPES:
        for month in MONTHS:
            file_name = f"{cab}_tripdata_{month}.parquet"
            url = f"{BASE_URL}/{file_name}"
            s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}{file_name}"

            stream_to_s3 = BashOperator(
                task_id=f"stream_{cab}_{month}_to_s3",
                bash_command=(
                    f"curl -s {url} | "
                    f"aws s3 cp - {s3_path} "
                    f"--region {AWS_REGION} "
                    f"--expected-size 200000000 || echo 'Failed {file_name}'"
                )
            )
