import boto3
from botocore import UNSIGNED
from botocore.config import Config

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime


def list_bucket_contents(bucket='nyc-tlc', match='2018', size_mb=250):
    
    s3 = boto3.client('s3')
    s3_resource = boto3.resource('s3')
    
    bucket_resource = s3_resource.Bucket(bucket)
    total_size_gb = 0
    total_files = 0
    match_size_gb = 0
    match_files = 0
    for key in bucket_resource.objects.all():
        key_size_mb = key.size/1024/1024
        total_size_gb += key_size_mb
        total_files += 1
        list_check = False
        if not match:
            list_check = True
        elif match in key.key:
            list_check = True
        if list_check and not size_mb:
            match_files += 1
            match_size_gb += key_size_mb
            print(f'{key.key} ({key_size_mb:3.0f}MB)')
        elif list_check and key_size_mb <= size_mb:
            match_files += 1
            match_size_gb += key_size_mb
            print(f'{key.key} ({key_size_mb:3.0f}MB)')

    if match:
        print(f'Matched file size is {match_size_gb/1024:3.1f}GB with {match_files} files')            
    
    print(f'Bucket {bucket} total size is {total_size_gb/1024:3.1f}GB with {total_files} files')


with DAG(
    dag_id="list_nyc_tlc_files",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
):

    PythonOperator(
        task_id="list_files",
        python_callable=list_bucket_contents
    )
