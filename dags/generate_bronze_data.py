from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 1, 1),
    "depends_on_past": False,
    "retries": 0,
}

with DAG(
    dag_id="generate_bronze_data",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["bronze"],
) as dag:

    hello = BashOperator(
        task_id="example_task",
        bash_command="echo hello from Airflow!",
    )

    cancellations = BashOperator(
        task_id='generate_cancellations',
        bash_command='python /opt/airflow/generate_data/generate_cancellations.py',
    )

    checkins = BashOperator(
        task_id='generate_checkins',
        bash_command='python /opt/airflow/generate_data/generate_checkins.py',
    )

    facility = BashOperator(
        task_id='generate_facility_usage',
        bash_command='python /opt/airflow/generate_data/generate_facility_usage.py',
    )

    members = BashOperator(
        task_id='generate_members',
        bash_command='python /opt/airflow/generate_data/generate_members.py',
    )

    retail = BashOperator(
        task_id='generate_retail',
        bash_command='python /opt/airflow/generate_data/generate_retail.py',
    )

    hello >> [cancellations, checkins, facility, members, retail]
