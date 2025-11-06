from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

# Simple function for the task
def hello():
    print("Airflow is up and!")

with DAG(
    dag_id="test_simple_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    test_task = PythonOperator(
        task_id="print_message",
        python_callable=hello
    )
