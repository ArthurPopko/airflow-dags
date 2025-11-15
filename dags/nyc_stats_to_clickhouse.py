import logging
import os
import re
from datetime import datetime

import numpy as np
import pandas as pd
import requests
import s3fs
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
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
COLUMNS = [
    "driver_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pu_location_id",
    "do_location_id",
    "fare_amount",
    "total_amount",
    "cab_type",
    "vendor_id",
    "store_and_fwd_flag",
    "ratecode_id",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "ehail_fee",
    "improvement_surcharge",
    "payment_type",
    "trip_type",
    "congestion_surcharge",
    "cbd_congestion_fee",
]


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
        else:
            logging.warning(f"File {local_path} already exists. Skipping download.")
        return local_path

    @task
    def prepare_month(file_path: str):

        df_iter = pd.read_parquet(file_path, engine="pyarrow")

        # Генерируем driver_id
        np.random.seed(42)
        df_iter["driver_id"] = np.random.randint(1, 1001, size=len(df_iter))

        # Приводим pickup/dropoff к datetime
        df_iter["pickup_datetime"] = pd.to_datetime(
            df_iter.get("lpep_pickup_datetime", df_iter.get("tpep_pickup_datetime")),
            errors="coerce",
        )
        df_iter["dropoff_datetime"] = pd.to_datetime(
            df_iter.get("lpep_dropoff_datetime", df_iter.get("tpep_dropoff_datetime")),
            errors="coerce",
        )

        # Дропаем строки с некорректными датами
        df_iter = df_iter.dropna(subset=["pickup_datetime", "dropoff_datetime"])

        # Функция для конвертации в snake_case
        def to_snake_case(name):
            name = re.sub(r"([A-Z]+)", r"_\1", name).lower()
            name = re.sub(r"^_", "", name)  # удаляем ведущий _
            return name

        df_iter.columns = [to_snake_case(col) for col in df_iter.columns]

        # Все колонки, содержащие "_id" — UInt32
        for col in df_iter.columns:
            if "_id" in col:
                df_iter[col] = df_iter[col].astype("UInt32")

        # passenger_count — UInt8
        if "passenger_count" in df_iter.columns:
            df_iter["passenger_count"] = df_iter["passenger_count"].astype("UInt8")

        # payment_type и trip_type — UInt8
        for col in ["payment_type", "trip_type"]:
            if col in df_iter.columns:
                df_iter[col] = df_iter[col].astype("UInt8")

        # Остальные числовые поля — float32
        float_cols = [
            "trip_distance",
            "fare_amount",
            "total_amount",
            "extra",
            "mta_tax",
            "tip_amount",
            "tolls_amount",
            "ehail_fee",
            "improvement_surcharge",
            "congestion_surcharge",
            "cbd_congestion_fee",
        ]
        for col in float_cols:
            if col in df_iter.columns:
                df_iter[col] = df_iter[col].astype("float32")

        # Строковые поля
        str_cols = ["cab_type", "store_and_fwd_flag"]
        for col in str_cols:
            if col in df_iter.columns:
                df_iter[col] = df_iter[col].astype("string")

        # Сохраняем только существующие колонки
        df_iter = df_iter[[col for col in COLUMNS if col in df_iter.columns]]

        # Сохраняем в parquet
        clean_path = os.path.join(
            LOCAL_DIR,
            os.path.basename(file_path).replace(".parquet", "_clean.parquet"),
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

        cols = [col for col in COLUMNS if col in df.columns]

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
