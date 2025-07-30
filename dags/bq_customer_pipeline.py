"""
Customer Events Hourly Processing Pipeline (TaskFlow API)
========================================================
Simple and clean Airflow DAG using TaskFlow API for processing
customer events data hourly with BigQuery.

Author: Data Team
Version: 3.0 (TaskFlow API - Simple)
"""

import json
import pendulum
from pathlib import Path
from typing import Dict, Any

from airflow.sdk import dag, task
from airflow.sdk.types import RuntimeTaskInstanceProtocol
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

# ─── Project Configuration ─────────────────────────────────────────────────────
DAG_DIR = Path(__file__).parent
SQL_DIR = DAG_DIR / "sql"

# Configuration from Airflow Variables
GCP_PROJECT = Variable.get("gcp_project_id", "your-project-id")
DATASET_RAW = Variable.get("bq_dataset_raw", "raw_zone")  
DATASET_STAGING = Variable.get("bq_dataset_staging", "staging_zone")
DATASET_CURATED = Variable.get("bq_dataset_curated", "curated_zone")
BQ_LOCATION = Variable.get("bq_location", "us-central1")

# ─── TaskFlow DAG ───────────────────────────────────────────────────────────────
@dag(
    schedule=None,  # Run manually or set schedule like "5 * * * *"
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    template_searchpath=[str(SQL_DIR)],
    default_args={
        "owner": "data-team",
        "retries": 2,
        "retry_delay": pendulum.duration(minutes=3),
        "email_on_failure": True,
        "email": ["data-team@company.com"],
    },
    params={
        "p_year": "{{ data_interval_start.strftime('%Y') }}",
        "p_month": "{{ data_interval_start.strftime('%m') }}",
        "p_day": "{{ data_interval_start.strftime('%d') }}",
        "p_hour": "{{ data_interval_start.strftime('%H') }}",
    },
    tags=["bigquery", "elt", "taskflow"],
)
def customer_events_pipeline():
    """
    ### Customer Events ETL Pipeline
    
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using four simple tasks for Extract, Transform, and Load.
    
    The pipeline processes customer events hourly:
    - Extract: Validate partition parameters
    - Transform: Merge staging data to raw zone  
    - Transform: Refresh curated summaries
    - Load: Log execution summary
    """

    @task()
    def validate_parameters(logical_date: pendulum.DateTime, ti: RuntimeTaskInstanceProtocol) -> Dict[str, str]:
        """
        #### Validate Parameters Task
        
        A simple validation task to check partition parameters 
        are properly formatted for the data pipeline.
        """
        # Get parameters from the DAG run
        params = ti.dag_run.conf or {}
        
        # Extract date components from logical_date
        year = logical_date.strftime('%Y')
        month = logical_date.strftime('%m')
        day = logical_date.strftime('%d')
        hour = logical_date.strftime('%H')
        
        # Use params if provided, otherwise use logical_date
        year = params.get('p_year', year)
        month = params.get('p_month', month)
        day = params.get('p_day', day)
        hour = params.get('p_hour', hour)
        
        # Simple validation
        if not (year.isdigit() and len(year) == 4):
            raise ValueError(f"Invalid year: {year}")
        if not (month.isdigit() and 1 <= int(month) <= 12):
            raise ValueError(f"Invalid month: {month}")
        if not (day.isdigit() and 1 <= int(day) <= 31):
            raise ValueError(f"Invalid day: {day}")
        if not (hour.isdigit() and 0 <= int(hour) <= 23):
            raise ValueError(f"Invalid hour: {hour}")
            
        return {
            "partition": f"{year}-{month}-{day} {hour}:00:00",
            "p_year": year,
            "p_month": month,
            "p_day": day,
            "p_hour": hour,
            "status": "validated"
        }

    @task()
    def merge_raw_data(validation_result: Dict[str, str]) -> Dict[str, Any]:
        """
        #### Merge Raw Data Task
        
        A merge task which takes validated parameters and merges
        staging data into the raw zone using BigQuery MERGE statement.
        """
        merge_job = BigQueryInsertJobOperator(
            task_id="merge_bq_job",
            location=BQ_LOCATION,
            configuration={
                "query": {
                    "query": "{% include 'merge_into_raw.sql' %}",
                    "useLegacySql": False,
                    "parameterMode": "NAMED",
                    "queryParameters": [
                        {"name": "p_year", "parameterType": {"type": "STRING"}, 
                         "parameterValue": {"value": validation_result["p_year"]}},
                        {"name": "p_month", "parameterType": {"type": "STRING"}, 
                         "parameterValue": {"value": validation_result["p_month"]}},
                        {"name": "p_day", "parameterType": {"type": "STRING"}, 
                         "parameterValue": {"value": validation_result["p_day"]}},
                        {"name": "p_hour", "parameterType": {"type": "STRING"}, 
                         "parameterValue": {"value": validation_result["p_hour"]}},
                        {"name": "p_project_id", "parameterType": {"type": "STRING"}, 
                         "parameterValue": {"value": GCP_PROJECT}},
                        {"name": "p_dataset_staging", "parameterType": {"type": "STRING"}, 
                         "parameterValue": {"value": DATASET_STAGING}},
                        {"name": "p_dataset_raw", "parameterType": {"type": "STRING"}, 
                         "parameterValue": {"value": DATASET_RAW}},
                    ],
                }
            },
        )
        
        result = merge_job.execute(context={})
        return {
            "job_id": result.job_id if result else "unknown",
            "partition": validation_result["partition"],
            "status": "merged"
        }

    @task()
    def refresh_curated_data(merge_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        #### Refresh Curated Data Task
        
        A transform task which takes the merge result and refreshes
        curated tables with daily customer event summaries.
        """
        curated_job = BigQueryInsertJobOperator(
            task_id="curated_bq_job",
            location=BQ_LOCATION,
            configuration={
                "query": {
                    "query": "{% include 'refresh_curated.sql' %}",
                    "useLegacySql": False,
                    "writeDisposition": "WRITE_TRUNCATE",
                    "createDisposition": "CREATE_IF_NEEDED",
                }
            },
        )
        
        result = curated_job.execute(context={})
        return {
            "job_id": result.job_id if result else "unknown",
            "status": "refreshed",
            "merge_job_id": merge_result["job_id"],
            "partition": merge_result["partition"]
        }

    @task()
    def log_pipeline_summary(curated_result: Dict[str, Any], logical_date: pendulum.DateTime, ti: RuntimeTaskInstanceProtocol) -> None:
        """
        #### Log Pipeline Summary Task
        
        A simple logging task which takes the curated result and logs
        the pipeline execution summary for monitoring purposes.
        """
        dag_run_id = ti.dag_run.run_id
        
        summary = {
            "dag_run_id": dag_run_id,
            "logical_date": logical_date.isoformat(),
            "partition_processed": curated_result.get("partition", "unknown"),
            "merge_job_id": curated_result["merge_job_id"],
            "curated_job_id": curated_result["job_id"],
            "pipeline_status": "completed"
        }
        
        print(f"Pipeline Summary: {json.dumps(summary, indent=2)}")
        print(f"Logical Date: {logical_date}")
        print(f"Task Instance: {ti.task_id}")

    # ─── Pipeline Flow ─────────────────────────────────────────────────────────
    # Simple 4-step pipeline: validate → merge → curated → log
    validation = validate_parameters()
    merge_result = merge_raw_data(validation)
    curated_result = refresh_curated_data(merge_result)
    log_pipeline_summary(curated_result)


# ─── DAG Instantiation ─────────────────────────────────────────────────────────
customer_events_dag = customer_events_pipeline()