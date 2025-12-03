from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime
from pendulum import duration
import logging
from telegram_notif import telegram_failure_notification, telegram_retry_notification
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

DBT_DIR = "/opt/airflow/justicia_finpro_dbt"


with DAG(
    dag_id="dim_gold",
    start_date=datetime(2025, 11, 29),
    schedule='@daily',
    catchup=False,  # Changed to False to avoid backfill
    description="DAG For Gold Layer",
    tags=["finpro", "dimension", "gold"],
    default_args={"retries": 1},
    dagrun_timeout=duration(minutes=20)
):

    dbt_dim_users = BashOperator(
        task_id="dim_users",
        bash_command=f"cd {DBT_DIR} && dbt run -s dim_users --profiles-dir .",
        on_failure_callback=telegram_failure_notification,
        on_retry_callback=telegram_retry_notification
    )
    
    dbt_dim_products = BashOperator(
        task_id="dim_products",
        bash_command=f"cd {DBT_DIR} && dbt run -s dim_products --profiles-dir .",
        on_failure_callback=telegram_failure_notification,
        on_retry_callback=telegram_retry_notification
    )

    dbt_fact_orders = BashOperator(
        task_id="fact_orders",
        bash_command=f"cd {DBT_DIR} && dbt run -s fact_orders --profiles-dir .",
        on_failure_callback=telegram_failure_notification,
        on_retry_callback=telegram_retry_notification
    )

    trigger_business_fraud_dag = TriggerDagRunOperator(
        task_id='trigger_DAG_Business_Fraud',
        trigger_dag_id='dag_business_fraud', # The DAG ID of the downstream DAG
        wait_for_completion=True,
        on_failure_callback=telegram_failure_notification,
        on_retry_callback=telegram_retry_notification
    )

    trigger_business_sales_dag = TriggerDagRunOperator(
        task_id='trigger_DAG_Business_Sales',
        trigger_dag_id='dag_business_sales', # The DAG ID of the downstream DAG
        wait_for_completion=True,
        on_failure_callback=telegram_failure_notification,
        on_retry_callback=telegram_retry_notification
    )

    [dbt_dim_users, dbt_dim_products] >> dbt_fact_orders >> [trigger_business_fraud_dag, trigger_business_sales_dag]