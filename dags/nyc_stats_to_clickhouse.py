import os
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import s3fs
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DAG_CONFIG = Variable.get(f"{DAG_ID.lower()}__config", {}, deserialize_json=True)

AWS_REGION = DAG_CONFIG.get("AWS_REGION")
S3_BUCKET = DAG_CONFIG.get("S3_BUCKET")
BASE_URL = DAG_CONFIG.get("BASE_URL")
MONTHS = DAG_CONFIG.get("MONTHS")
CAB_TYPES = ["yellow", "green"]
LOCAL_DIR = "/etl_cache/nyc"
SCHEMA = "staging"
TABLE = "nyc_tlc_tripdata_local"

os.makedirs(LOCAL_DIR, exist_ok=True)
fs = s3fs.S3FileSystem(anon=False)

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 11, 1),
    schedule="@monthly",
    max_active_tasks=1,
    catchup=False,
    tags=["nyc", "etl"],
) as dag:

    @task
    def download_file(cab: str, month: str):
        file_name = f"{cab}_tripdata_{month}.parquet"
        local_path = os.path.join(LOCAL_DIR, file_name)

        if not os.path.exists(local_path):
            url = f"{BASE_URL}/{file_name}"
            resp = requests.get(url)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(resp.content)
        return local_path

    @task
    def prepare_month(file_path: str):
        df = pd.read_parquet(file_path)

        cab_type = os.path.basename(file_path).split("_")[0]
        df["cab_type"] = cab_type

        df["tpep_pickup_datetime"] = pd.to_datetime(
            df["tpep_pickup_datetime"], errors="coerce"
        )
        df["tpep_dropoff_datetime"] = pd.to_datetime(
            df["tpep_dropoff_datetime"], errors="coerce"
        )
        df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime"])

        numeric_cols = [
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "PULocationID",
            "DOLocationID",
        ]
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors="coerce")
        df = df.dropna(subset=numeric_cols)

        df = df.query(
            "1 <= passenger_count <= 9 & "
            "0 < trip_distance < 100 & "
            "0 < fare_amount < 1000 & "
            "0 < total_amount < 1000"
        )

        np.random.seed(42)
        df["driver_id"] = np.random.randint(1, 101, size=len(df))
        df["trip_time_min"] = (
            df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
        ).dt.total_seconds() / 60

        df = df[
            [
                "cab_type",
                "tpep_pickup_datetime",
                "tpep_dropoff_datetime",
                "driver_id",
                "passenger_count",
                "trip_distance",
                "PULocationID",
                "DOLocationID",
                "fare_amount",
                "total_amount",
                "trip_time_min",
            ]
        ]

        clean_name = os.path.basename(file_path).replace(".parquet", "_clean.parquet")
        output_path = os.path.join(LOCAL_DIR, clean_name)
        df.to_parquet(output_path, index=False)

        return output_path

    @task
    def load_month(file_path: str):
        hook = ClickHouseHook(clickhouse_conn_id="click")
        df = pd.read_parquet(file_path)

        chunk_size = 200_000
        total = len(df)

        for start in range(0, total, chunk_size):
            chunk = df.iloc[start : start + chunk_size]
            hook.insert_dataframe(table=f"{SCHEMA}.{TABLE}", df=chunk)
        print(f"Inserted {total} rows")

    for cab in CAB_TYPES:
        for month in MONTHS:
            raw_file = download_file(cab, month)
            clean_file = prepare_month(raw_file)
            load_month(clean_file)
