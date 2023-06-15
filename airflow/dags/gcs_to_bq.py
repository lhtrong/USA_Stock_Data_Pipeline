import os
import logging

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateTableOperator, BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.models import Connection 
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_BUCKET_ID")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'stock_table')

DATASET = "tripdata"
INPUT_FILETYPE = "parquet"

current_year = datetime.now().year
year = 2019





default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}


# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="gcs_2_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:

    while year <= current_year:
        # move_files_gcs_task = GCSToGCSOperator(
        #     task_id=f'move_{colour}_{DATASET}_files_task',
        #     source_bucket=BUCKET,
        #     source_object=f'{INPUT_PART}/{colour}_{DATASET}*.{INPUT_FILETYPE}',
        #     destination_bucket=BUCKET,
        #     destination_object=f'{colour}/{colour}_{DATASET}',
        #     move_object=True
        # )
        YEAR = str(year)

        bigquery_external_table_task = BigQueryCreateTableOperator(
            task_id=f"bq_{YEAR}_{DATASET}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{year}_{DATASET}",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": f"{INPUT_FILETYPE.upper()}",
                    "sourceUris": [f"gs://{BUCKET}/stock_data/{YEAR}/*"],
                },
            },
            gcp_conn_id = 'gcloud',
            location = 'us-east1'
        )

        # CREATE_BQ_TBL_QUERY = (
        #     f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{YEAR}_{DATASET} \
        #     PARTITION BY DATE({ds_col}) \
        #     AS \
        #     SELECT * FROM {BIGQUERY_DATASET}.{YEAR}_{DATASET}_external_table;"
        # )

        # # Create a partitioned table from external table
        # bq_create_partitioned_table_job = BigQueryInsertJobOperator(
        #     task_id=f"bq_create_{colour}_{DATASET}_partitioned_table_task",
        #     configuration={
        #         "query": {
        #             "query": CREATE_BQ_TBL_QUERY,
        #             "useLegacySql": False,
        #         }
        #     }
        # )
        year = year + 1

        # bigquery_external_table_task >> bq_create_partitioned_table_job
        bigquery_external_table_task 