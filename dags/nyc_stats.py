import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import Variable

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DAG_CONFIG = Variable.get(f"{DAG_ID.lower()}__config", {}, deserialize_json=True)

AWS_REGION = DAG_CONFIG.get("AWS_REGION")
S3_BUCKET = DAG_CONFIG.get("S3_BUCKET")
S3_PREFIX = DAG_CONFIG.get("S3_PREFIX")
BASE_URL = DAG_CONFIG.get("BASE_URL")
MONTHS = DAG_CONFIG.get("MONTHS")
CAB_TYPES = DAG_CONFIG.get("CAB_TYPES")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2024, 12, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    description="Stream 6 months of NYC TLC Yellow/Green Cab data directly to S3, fail on error",
) as dag:

    for cab in CAB_TYPES:
        for month in MONTHS:
            file_name = f"{cab}_tripdata_{month}.parquet"
            url = f"{BASE_URL}/{file_name}"
            s3_path = f"s3://{S3_BUCKET}/{S3_PREFIX}{file_name}"

            stream_to_s3 = BashOperator(
                task_id=f"stream_{cab}_{month}_to_s3",
                bash_command=(
                    # -f → fail on HTTP errors, -S → show error, -L → follow redirects
                    f"curl -fSL {url} | "
                    f"aws s3 cp - {s3_path} --region {AWS_REGION}"
                ),
            )
