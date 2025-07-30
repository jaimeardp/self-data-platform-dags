# dags/bq_customer_pipeline.py
from airflow import DAG
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import timedelta

default_args = {
    "owner": "data-team",
    "gcp_conn_id": "google_cloud_default",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="customer_events_hourly",
    start_date=days_ago(1),
    schedule="0 * * * *",
    catchup=False,
    template_searchpath=["/usr/local/airflow/sql"],
    default_args=default_args,
    params={
        # Airflow automatically injects execution_date – use it for partition params
        "p_year": "{{ data_interval_start.strftime('%Y') }}",
        "p_month": "{{ data_interval_start.strftime('%m') }}",
        "p_day": "{{ data_interval_start.strftime('%d') }}",
        "p_hour": "{{ data_interval_start.strftime('%H') }}",
    },
    tags=["bigquery","elt"],
) as dag:

    merge_raw = BigQueryInsertJobOperator(
        task_id="merge_into_raw",
        configuration={
            "query": {
                "query": "{% include 'merge_into_raw.sql' %}",
                "useLegacySql": False,
                "parameterMode": "NAMED",
                "queryParameters": [
                    {"name": "p_year",  "parameterType": {"type": "STRING"}, "parameterValue": {"value": "{{ params.p_year }}" }},
                    {"name": "p_month", "parameterType": {"type": "STRING"}, "parameterValue": {"value": "{{ params.p_month }}" }},
                    {"name": "p_day",   "parameterType": {"type": "STRING"}, "parameterValue": {"value": "{{ params.p_day }}" }},
                    {"name": "p_hour",  "parameterType": {"type": "STRING"}, "parameterValue": {"value": "{{ params.p_hour }}" }},
                ],
            }
        },
    )

    refresh_curated = BigQueryInsertJobOperator(
        task_id="refresh_curated",
        configuration={
            "query": {
                "query": "{% include 'refresh_curated.sql' %}",
                "useLegacySql": False,
                "writeDisposition": "WRITE_TRUNCATE",
            }
        },
    )

    merge_raw >> refresh_curated   # simple linear flow
