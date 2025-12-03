from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import duration
import logging
from telegram_notif import telegram_failure_notification, telegram_retry_notification

DBT_DIR = "/opt/airflow/justicia_finpro_dbt"


with DAG(
    dag_id="dag_business_sales",
    start_date=datetime(2025, 11, 29),
    schedule_interval=None,
    schedule='0 1 * * *',
    catchup=False,  # Changed to False to avoid backfill
    description="DAG For Business Layer",
    tags=["finpro", "mart", "business"],
    default_args={"retries": 1},
    dagrun_timeout=duration(minutes=20)
):

    dbt_dag_business_sales = BashOperator(
        task_id="sales_mart",
        bash_command=f"cd {DBT_DIR} && dbt run -s sales_mart --profiles-dir .",
        on_failure_callback=telegram_failure_notification,
        on_retry_callback=telegram_retry_notification
    )
    
    dbt_dag_business_sales