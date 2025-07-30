# dags/example_dag.py
#
# A simple "hello world" DAG to test the CI/CD pipeline.
# -----------------------------------------------------------------------------
import pendulum
from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="cicd_test_dag",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["example", "cicd"],
    doc_md="""
    ### CI/CD Test DAG
    This is a simple DAG deployed via GitHub Actions to verify that the
    CI/CD process for uploading DAGs to Composer is working correctly.
    """,
) as dag:
    # This task simply prints the execution date.
    say_hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Hello from Airflow! This DAG was deployed via CI/CD. Execution date is {{ ds }}'",
    )

