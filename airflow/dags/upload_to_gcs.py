import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
# from airflow.providers.google.cloud.transfer.gcs_to_gcs import GCSToGCSOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models.variable import Variable



GCP_PROJECT_ID=os.getenv("GCP_PROJECT_ID", "project_id")
GCP_BUCKET_ID=os.getenv("GCP_BUCKET_ID", "bucket")
API_KEY=os.getenv("API_KEY", "api")

default_args = {
    "owner": "airflow",
    "start_date":datetime(2019,1,10),
    "depends_on_past": False,
    "retries": 1,
}

dag =  DAG(
    dag_id = "upload_to_gcs",
    schedule_interval="@daily",
    default_args = default_args,
    catchup = True,
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
    application_args=['{{ds}}', API_KEY,GCP_BUCKET_ID],
    retry_delay = timedelta(seconds=60)
)

hello_world >> spark_load_data
