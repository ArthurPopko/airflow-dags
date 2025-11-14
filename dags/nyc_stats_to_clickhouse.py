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

# --- Настройки ---
DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DAG_CONFIG = Variable.get(f"{DAG_ID.lower()}__config", {}, deserialize_json=True)

AWS_REGION = DAG_CONFIG.get("AWS_REGION")
S3_BUCKET = DAG_CONFIG.get("S3_BUCKET")
BASE_URL = DAG_CONFIG.get("BASE_URL")
MONTHS = DAG_CONFIG.get("MONTHS")
CAB_TYPES = ["yellow", "green"]
LOCAL_DIR = "/tmp/nyc_tlc_data"
SCHEMA = "staging"
TABLE = "nyc_tlc_tripdata_local"

os.makedirs(LOCAL_DIR, exist_ok=True)
fs = s3fs.S3FileSystem(anon=False)

# --- DAG ---
with DAG(
    dag_id="nyc_tlc_etl_clickhouse",
    start_date=datetime(2025, 11, 1),
    schedule="@monthly",
    catchup=False,
    tags=["nyc", "etl"],
) as dag:

    @task
    def download_and_upload(cab: str, month: str):
        file_name = f"{cab}_tripdata_{month}.parquet"
        local_path = os.path.join(LOCAL_DIR, file_name)
        s3_path = f"s3://{S3_BUCKET}/{file_name}"

        # Скачивание через requests
        if not os.path.exists(local_path):
            url = f"{BASE_URL}/{file_name}"
            resp = requests.get(url)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                f.write(resp.content)
        else:
            print(f"File exists locally: {local_path}")

        # Загрузка в S3 через s3fs
        if not fs.exists(s3_path):
            fs.put(local_path, s3_path)
        else:
            print(f"File already exists in S3: {s3_path}")

        return local_path

    @task
    def prepare_data(files: list):
        all_dfs = []
        for file in files:
            df_month = pd.read_parquet(file)
            cab_type = os.path.basename(file).split("_")[0]
            df_month["cab_type"] = cab_type
            all_dfs.append(df_month)

        df = pd.concat(all_dfs, ignore_index=True)

        # Очистка данных
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

        df["passenger_count"] = df["passenger_count"].astype(int)
        df["PULocationID"] = df["PULocationID"].astype(int)
        df["DOLocationID"] = df["DOLocationID"].astype(int)

        df = df.query(
            "1 <= passenger_count <= 9 & 0 < trip_distance < 100 & 0 < fare_amount < 1000 & 0 < total_amount < 1000"
        )

        np.random.seed(42)
        df["driver_id"] = np.random.randint(1, 101, size=len(df))
        df["trip_time_min"] = (
            df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
        ).dt.total_seconds() / 60

        df_clickhouse_clean = df[
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

        output_path = os.path.join(LOCAL_DIR, "clickhouse_ready.parquet")
        df_clickhouse_clean.to_parquet(output_path, index=False)
        return output_path

    @task
    def load_to_clickhouse(file_path: str):
        hook = ClickHouseHook(clickhouse_conn_id="click")
        hook.insert_dataframe(table=f"{SCHEMA}.{TABLE}", df=pd.read_parquet(file_path))

    # --- Flow ---
    files = [download_and_upload(cab, month) for cab in CAB_TYPES for month in MONTHS]
    clickhouse_file = prepare_data(files)
    load_to_clickhouse(clickhouse_file)
