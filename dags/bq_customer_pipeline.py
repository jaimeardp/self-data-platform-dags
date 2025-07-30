# dags/customer_events_pipeline.py
from pathlib import Path
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ─── Rutas dinámicas ───────────────────────────────────────────────────────────
DAG_DIR = Path(__file__).parent          # /home/airflow/gcs/dags
SQL_DIR = DAG_DIR / "sql"                # /home/airflow/gcs/dags/sql

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
    template_searchpath=[str(SQL_DIR)],  # ← aquí la carpeta SQL
    default_args=default_args,
    params={
        "p_year":  "{{ data_interval_start.strftime('%Y') }}",
        "p_month": "{{ data_interval_start.strftime('%m') }}",
        "p_day":   "{{ data_interval_start.strftime('%d') }}",
        "p_hour":  "{{ data_interval_start.strftime('%H') }}",
    },
    tags=["bigquery", "elt"],
) as dag:

    merge_raw = BigQueryInsertJobOperator(
        task_id="merge_into_raw",
        location="us-central1",
        sql="{% include 'merge_into_raw.sql' %}",  # templated SQL file
        use_legacy_sql=False,
        parameter_mode="NAMED",
        # <‑‑‑ pass the params here – Airflow converts them to queryParameters
        parameters={
            "p_year":  "{{ params.p_year }}",
            "p_month": "{{ params.p_month }}",
            "p_day":   "{{ params.p_day }}",
            "p_hour":  "{{ params.p_hour }}",
        },
    )

    refresh_curated = BigQueryInsertJobOperator(
        task_id="refresh_curated",
        location="us-central1",
        sql="{% include 'refresh_curated.sql' %}",
        use_legacy_sql=False,
        write_disposition="WRITE_TRUNCATE",
    )

    merge_raw >> refresh_curated
