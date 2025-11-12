from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
import os

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
MONTHS = ["2024-06", "2024-07", "2024-08", "2024-09", "2024-10", "2024-11"]
CAB_TYPES = ["yellow", "green"]
LOCAL_DIR = "/tmp/nyc_tlc"
S3_BUCKET = "airflow-art"
S3_PREFIX = "nyc_tlc/"

with DAG(
    dag_id="nyc_tlc_download_to_s3",
    start_date=datetime(2024, 12, 1),
    schedule=None,
    catchup=False,
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Download 6 months of NYC TLC Yellow/Green Cab data and upload to S3",
) as dag:

    make_tmp_dir = BashOperator(
        task_id="make_tmp_dir",
        bash_command=f"mkdir -p {LOCAL_DIR}"
    )

    for cab in CAB_TYPES:
        for month in MONTHS:
            file_name = f"{cab}_tripdata_{month}.parquet"
            url = f"{BASE_URL}/{file_name}"
            local_path = os.path.join(LOCAL_DIR, file_name)
            s3_key = f"{S3_PREFIX}{file_name}"

            download_task = BashOperator(
                task_id=f"download_{cab}_{month}",
                bash_command=f"curl -s -o {local_path} {url}"
            )

            upload_task = LocalFilesystemToS3Operator(
                task_id=f"upload_{cab}_{month}_to_s3",
                filename=local_path,
                dest_key=s3_key,
                dest_bucket=S3_BUCKET,
                replace=True,
                aws_conn_id="aws_default",
            )

            make_tmp_dir >> download_task >> upload_task
