import logging
import os
import re
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from clickhouse_driver import Client

DAG_ID = os.path.basename(__file__).replace(".pyc", "").replace(".py", "")
DAG_CONFIG = Variable.get(f"{DAG_ID.lower()}__config", {}, deserialize_json=True)

AWS_REGION = DAG_CONFIG.get("AWS_REGION")
S3_BUCKET = DAG_CONFIG.get("S3_BUCKET")
BASE_URL = DAG_CONFIG.get("BASE_URL")
MONTHS = DAG_CONFIG.get("MONTHS", [])
CAB_TYPES = ["yellow", "green"]

LOCAL_DIR = "/etl_cache/nyc"
SCHEMA = "staging"
TABLE = "nyc_tlc_tripdata_local"
COLUMNS = [
    "driver_id",
    "pu_location_id",
    "do_location_id",
    "vendor_id",
    "ratecode_id",
    "pickup_datetime",
    "dropoff_datetime",
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
    "passenger_count",
    "payment_type",
    "trip_type",
    "cab_type",
    "store_and_fwd_flag",
]


@task(retries=3, retry_delay=timedelta(seconds=30))
def download_file(cab: str, month: str):
    file_name = f"{cab}_tripdata_{month}.parquet"
    local_path = os.path.join(LOCAL_DIR, file_name)
    if os.path.exists(local_path):
        logging.info(f"[DOWNLOAD] File {local_path} already exists. Skipping download.")
        return local_path

    url = f"{BASE_URL}/{file_name}"
    logging.info(f"[DOWNLOAD] Starting download: {url}")
    resp = requests.get(url, stream=True)
    resp.raise_for_status()
    with open(local_path, "wb") as f:
        for chunk in resp.iter_content(chunk_size=(1024 * 1024) * 10):
            f.write(chunk)
    logging.info(f"[DOWNLOAD] Finished downloading {file_name}")
    return local_path


@task(retries=2, retry_delay=timedelta(seconds=10))
def prepare_month(file_path: str):
    logging.info(f"[PREPARE] Reading parquet file {file_path}")
    df = pd.read_parquet(file_path, engine="pyarrow")

    np.random.seed(42)
    df["driver_id"] = np.random.randint(1, 1001, size=len(df))
    logging.info("[PREPARE] Generated driver_id")

    df["pickup_datetime"] = pd.to_datetime(
        df.get("lpep_pickup_datetime", df.get("tpep_pickup_datetime")),
        errors="coerce",
    )
    df["dropoff_datetime"] = pd.to_datetime(
        df.get("lpep_dropoff_datetime", df.get("tpep_dropoff_datetime")),
        errors="coerce",
    )

    df = df.dropna(subset=["pickup_datetime", "dropoff_datetime"])

    df.columns = [
        re.sub(r"^_", "", re.sub(r"([A-Z]+)", r"_\1", col).lower())
        for col in df.columns
    ]

    for col in df.columns:
        if "_id" in col:
            df[col] = df[col].astype("UInt32")
    if "passenger_count" in df.columns:
        df["passenger_count"] = df["passenger_count"].astype("UInt8")
    for col in ["payment_type", "trip_type"]:
        if col in df.columns:
            df[col] = df[col].astype("UInt8")

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
        if col in df.columns:
            df[col] = df[col].astype("float32")
    for col in ["cab_type", "store_and_fwd_flag"]:
        if col in df.columns:
            df[col] = df[col].astype("string")

    df = df[[col for col in COLUMNS if col in df.columns]]

    clean_path = os.path.join(
        LOCAL_DIR, os.path.basename(file_path).replace(".parquet", "_clean.parquet")
    )
    df.to_parquet(clean_path, index=False)
    logging.info(f"[PREPARE] Saved cleaned parquet to {clean_path}")
    return clean_path


@task(retries=2, retry_delay=timedelta(seconds=10))
def insert_month(file_path: str):
    logging.info(f"[LOAD] Loading file {file_path} into ClickHouse")
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

    batch_size = 50000
    total_rows = len(df)
    logging.info(f"[LOAD] Total rows to insert: {total_rows}")
    for start in range(0, total_rows, batch_size):
        batch = df.iloc[start : start + batch_size].to_dict("records")
        client.execute(
            f"INSERT INTO {SCHEMA}.{TABLE} ({', '.join(cols)}) VALUES", batch
        )
        logging.info(f"[LOAD] Inserted rows {start}-{start + len(batch)}")


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 11, 1),
    schedule="@monthly",
    catchup=False,
    max_active_tasks=1,
    tags=["nyc", "etl"],
) as dag:

    for cab in CAB_TYPES:
        for month in MONTHS:
            with TaskGroup(group_id=f"{cab}_{month}") as tg:
                download = download_file(cab, month)
                clean = prepare_month(download)
                insert = insert_month(clean)
                download >> clean >> insert
