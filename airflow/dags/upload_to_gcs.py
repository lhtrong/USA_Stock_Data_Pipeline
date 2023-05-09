import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
# from airflow.providers.google.cloud.transfer.gcs_to_gcs import GCSToGCSOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET_ID = os.environ.get("GCP_GCS_BUCKET")

INTERVAL=2
DATE=3
API_KEY=4
SPARK_MASTER=5

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

dag =  DAG(
    dag_id = "upload_to_gcs",
    schedule_interval="@daily",
    default_args = default_args,
    catchup = False,
    max_active_runs = 1,
)

# Task
def hello():
    logging.info('HELLO WORLD, THIS IS MY FIRST TASK')


hello_world = PythonOperator(
    dag = dag,
    task_id = 'hello_world',
    python_callable = hello,
    retry_delay = timedelta(seconds=30)
)
spark_load_data = SparkSubmitOperator(
    dag = dag,
    task_id = 'spark_load_stock_data',
    conn_id = 'spark_default',
    verbose=1,
    # application = '/opt/bitnami/spark/app/load_data.py',
    application = '/opt/spark/app/load_data.py',
    # conf = {"spark.master":SPARK_MASTER},
    # application_args=[INTERVAL, DATE, API_KEY, BUCKET_ID],
    retry_delay = timedelta(seconds=30)
)

hello_world >> spark_load_data
