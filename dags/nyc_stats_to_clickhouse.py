import os
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import s3fs
from airflow import DAG
from airflow.models import Variable
from airflow.sdk.bases.hook import BaseHook
from airflow.sdk.task import task
from clickhouse_driver import Client

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
            resp = requests.get(url, stream=True)
            resp.raise_for_status()
            with open(local_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
        return local_path

    @task
    def prepare_month(file_path: str):
        df_iter = pd.read_parquet(file_path, engine="pyarrow")

        cab_type = os.path.basename(file_path).split("_")[0]
        if cab_type == "green":
            df_iter = df_iter.rename(
                columns={
                    "lpep_pickup_datetime": "pickup_datetime",
                    "lpep_dropoff_datetime": "dropoff_datetime",
                }
            )
        else:
            df_iter = df_iter.rename(
                columns={
                    "tpep_pickup_datetime": "pickup_datetime",
                    "tpep_dropoff_datetime": "dropoff_datetime",
                }
            )

        df_iter["cab_type"] = cab_type
        df_iter["pickup_datetime"] = pd.to_datetime(
            df_iter["pickup_datetime"], errors="coerce"
        )
        df_iter["dropoff_datetime"] = pd.to_datetime(
            df_iter["dropoff_datetime"], errors="coerce"
        )
        df_iter = df_iter.dropna(subset=["pickup_datetime", "dropoff_datetime"])

        numeric_cols = [
            "passenger_count",
            "trip_distance",
            "fare_amount",
            "total_amount",
            "PULocationID",
            "DOLocationID",
        ]
        df_iter[numeric_cols] = df_iter[numeric_cols].apply(
            pd.to_numeric, errors="coerce"
        )
        df_iter = df_iter.dropna(subset=numeric_cols)

        df_iter = df_iter.query(
            "1 <= passenger_count <= 9 & "
            "0 < trip_distance < 100 & "
            "0 < fare_amount < 1000 & "
            "0 < total_amount < 1000"
        )

        np.random.seed(42)
        df_iter["driver_id"] = np.random.randint(1, 101, size=len(df_iter))
        df_iter["trip_time_min"] = (
            df_iter["dropoff_datetime"] - df_iter["pickup_datetime"]
        ).dt.total_seconds() / 60

        clean_path = os.path.join(
            LOCAL_DIR, os.path.basename(file_path).replace(".parquet", "_clean.parquet")
        )
        df_iter.to_parquet(clean_path, index=False)
        return clean_path

    @task
    def load_month(file_path: str):
        conn = BaseHook.get_connection("click")
        client = Client(
            host=conn.host,
            port=int(conn.port or 9000),
            user=conn.login,
            password=conn.password,
            database=conn.schema or SCHEMA,
        )

        df = pd.read_parquet(file_path)

        # Приводим типы к ClickHouse
        df["passenger_count"] = df["passenger_count"].astype("uint8")
        df["driver_id"] = df["driver_id"].astype("uint32")
        df["PULocationID"] = df["PULocationID"].astype("uint16")
        df["DOLocationID"] = df["DOLocationID"].astype("uint16")
        df["trip_distance"] = df["trip_distance"].astype("float32")
        df["fare_amount"] = df["fare_amount"].astype("float32")
        df["total_amount"] = df["total_amount"].astype("float32")
        df["trip_time_min"] = df["trip_time_min"].astype("float32")

        cols = [
            "cab_type",
            "pickup_datetime",
            "dropoff_datetime",
            "driver_id",
            "passenger_count",
            "trip_distance",
            "PULocationID",
            "DOLocationID",
            "fare_amount",
            "total_amount",
            "trip_time_min",
        ]

        # Загружаем батчами, чтобы экономить память
        batch_size = 5000
        for i in range(0, len(df), batch_size):
            batch = df.iloc[i : i + batch_size].to_dict("records")
            client.execute(
                f"INSERT INTO {SCHEMA}.{TABLE} ({', '.join(cols)}) VALUES",
                batch,
                types_check=True,
            )

    # --- Flow ---
    for cab in CAB_TYPES:
        for month in MONTHS:
            file_path = download_file(cab, month)
            clean_file = prepare_month(file_path)
            load_month(clean_file)
