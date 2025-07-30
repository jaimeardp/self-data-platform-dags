"""
Customer Events Hourly Processing Pipeline
==========================================
Processes customer events data hourly with proper error handling,
monitoring, and BigQuery optimization.
"""

from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.sensors.bigquery import BigQueryTableExistenceSensor
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import logging

# ─── Configuration ─────────────────────────────────────────────────────────────
DAG_DIR = Path(__file__).parent
SQL_DIR = DAG_DIR / "sql"

# Get configuration from Airflow Variables with defaults
GCP_PROJECT_ID = Variable.get("gcp_project_id", default_var="your-project-id")
BQ_DATASET_RAW = Variable.get("bq_dataset_raw", default_var="raw_zone")
BQ_DATASET_STAGING = Variable.get("bq_dataset_staging", default_var="staging_zone")
BQ_DATASET_CURATED = Variable.get("bq_dataset_curated", default_var="curated_zone")
BQ_LOCATION = Variable.get("bq_location", default_var="us-central1")

# ─── Default Arguments ─────────────────────────────────────────────────────────
default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": ["data-team@company.com"],
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=1),
    "sla": timedelta(minutes=30),
}

# ─── Helper Functions ───────────────────────────────────────────────────────────
def validate_partition_parameters(**context):
    """Validate that partition parameters are properly formatted"""
    params = context['params']
    
    # Validate year (4 digits)
    if not params['p_year'].isdigit() or len(params['p_year']) != 4:
        raise ValueError(f"Invalid year parameter: {params['p_year']}")
    
    # Validate month (01-12)
    if not params['p_month'].isdigit() or not (1 <= int(params['p_month']) <= 12):
        raise ValueError(f"Invalid month parameter: {params['p_month']}")
    
    # Validate day (01-31)
    if not params['p_day'].isdigit() or not (1 <= int(params['p_day']) <= 31):
        raise ValueError(f"Invalid day parameter: {params['p_day']}")
    
    # Validate hour (00-23)
    if not params['p_hour'].isdigit() or not (0 <= int(params['p_hour']) <= 23):
        raise ValueError(f"Invalid hour parameter: {params['p_hour']}")
    
    logging.info(f"Validated partition parameters: {params['p_year']}-{params['p_month']}-{params['p_day']} {params['p_hour']}:00")
    return True

def get_bq_job_config(sql_file: str, use_params: bool = True) -> dict:
    """Generate BigQuery job configuration with proper settings"""
    config = {
        "query": {
            "query": f"{{% include '{sql_file}' %}}",
            "useLegacySql": False,
            "priority": "INTERACTIVE",
            "maximumBytesBilled": "1000000000",  # 1GB limit
            "jobTimeoutMs": "1800000",  # 30 minutes
            "labels": {
                "team": "data",
                "pipeline": "customer-events",
                "env": "{{ var.value.environment | default('dev', true) }}"
            }
        }
    }
    
    if use_params:
        config["query"]["parameterMode"] = "NAMED"
        config["query"]["queryParameters"] = [
            {
                "name": "p_year",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": "{{ params.p_year }}"}
            },
            {
                "name": "p_month",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": "{{ params.p_month }}"}
            },
            {
                "name": "p_day",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": "{{ params.p_day }}"}
            },
            {
                "name": "p_hour",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": "{{ params.p_hour }}"}
            },
            {
                "name": "p_project_id",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": GCP_PROJECT_ID}
            },
            {
                "name": "p_dataset_raw",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": BQ_DATASET_RAW}
            },
            {
                "name": "p_dataset_staging",
                "parameterType": {"type": "STRING"},
                "parameterValue": {"value": BQ_DATASET_STAGING}
            }
        ]
    
    return config

# ─── DAG Definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="customer_events_hourly_v2",
    description="Hourly processing of customer events data with validation and monitoring",
    start_date=datetime(2024, 1, 1),
    schedule="5 * * * *",  # Run at 5 minutes past each hour
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    template_searchpath=[str(SQL_DIR)],
    default_args=default_args,
    params={
        "p_year": "{{ data_interval_start.strftime('%Y') }}",
        "p_month": "{{ data_interval_start.strftime('%m') }}",
        "p_day": "{{ data_interval_start.strftime('%d') }}",
        "p_hour": "{{ data_interval_start.strftime('%H') }}",
    },
    tags=["bigquery", "elt", "customer-data", "hourly"],
    doc_md=__doc__,
) as dag:

    # ─── Validation Tasks ──────────────────────────────────────────────────────
    validate_params = PythonOperator(
        task_id="validate_partition_parameters",
        python_callable=validate_partition_parameters,
        doc_md="Validates that all partition parameters are properly formatted",
    )

    # Check if source tables exist
    check_staging_table = BigQueryTableExistenceSensor(
        task_id="check_staging_table_exists",
        project_id=GCP_PROJECT_ID,
        dataset_id=BQ_DATASET_STAGING,
        table_id="customer_events_ext",
        gcp_conn_id="google_cloud_default",
        timeout=300,
        poke_interval=30,
        doc_md="Ensures the source staging table exists before processing",
    )

    # ─── Data Processing Tasks ─────────────────────────────────────────────────
    with TaskGroup("raw_data_processing", tooltip="Process raw customer events data") as raw_processing:
        
        merge_raw_events = BigQueryInsertJobOperator(
            task_id="merge_customer_events_raw",
            location=BQ_LOCATION,
            gcp_conn_id="google_cloud_default",
            configuration=get_bq_job_config("merge_into_raw.sql", use_params=True),
            doc_md="""
            Merges hourly customer events from staging into raw zone.
            Uses MERGE statement to handle deduplication based on event_uuid.
            """,
        )

        # Data quality check after raw merge
        validate_raw_data = BigQueryInsertJobOperator(
            task_id="validate_raw_data_quality",
            location=BQ_LOCATION,
            gcp_conn_id="google_cloud_default",
            configuration=get_bq_job_config("validate_raw_data.sql", use_params=True),
            doc_md="Validates data quality metrics for the processed hour",
        )

        merge_raw_events >> validate_raw_data

    with TaskGroup("curated_data_processing", tooltip="Process curated customer data") as curated_processing:
        
        refresh_curated_events = BigQueryInsertJobOperator(
            task_id="refresh_curated_customer_events",
            location=BQ_LOCATION,
            gcp_conn_id="google_cloud_default",
            configuration={
                **get_bq_job_config("refresh_curated.sql", use_params=False),
                "query": {
                    **get_bq_job_config("refresh_curated.sql", use_params=False)["query"],
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
            doc_md="""
            Refreshes the curated customer events table with aggregated data.
            Truncates and rebuilds the entire table for consistency.
            """,
        )

        # Data freshness check
        check_curated_freshness = BigQueryInsertJobOperator(
            task_id="check_curated_data_freshness",
            location=BQ_LOCATION,
            gcp_conn_id="google_cloud_default",
            configuration=get_bq_job_config("check_data_freshness.sql", use_params=False),
            doc_md="Validates that curated data is fresh and complete",
        )

        refresh_curated_events >> check_curated_freshness

    # ─── Monitoring Task ───────────────────────────────────────────────────────
    def log_pipeline_metrics(**context):
        """Log pipeline execution metrics"""
        task_instances = context['dag_run'].get_task_instances()
        
        metrics = {
            'dag_run_id': context['dag_run'].run_id,
            'execution_date': context['execution_date'].isoformat(),
            'total_tasks': len(task_instances),
            'successful_tasks': len([ti for ti in task_instances if ti.state == 'success']),
            'failed_tasks': len([ti for ti in task_instances if ti.state == 'failed']),
            'partition_processed': f"{context['params']['p_year']}-{context['params']['p_month']}-{context['params']['p_day']} {context['params']['p_hour']}:00"
        }
        
        logging.info(f"Pipeline metrics: {metrics}")
        return metrics

    log_metrics = PythonOperator(
        task_id="log_pipeline_metrics",
        python_callable=log_pipeline_metrics,
        doc_md="Logs pipeline execution metrics for monitoring",
        trigger_rule="all_done",  # Run regardless of upstream success/failure
    )

    # ─── Task Dependencies ─────────────────────────────────────────────────────
    validate_params >> check_staging_table >> raw_processing
    raw_processing >> curated_processing >> log_metrics