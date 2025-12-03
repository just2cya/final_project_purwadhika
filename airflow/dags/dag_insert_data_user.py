from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from generate_data import insert_users

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="hourly_insert_data_user_dag",
    start_date=datetime(2025, 11, 30),
    schedule_interval='@hourly',
    catchup=False,
    description="Hourly user data generation for ecommerce system",
    tags=["ecommerce", "hourly", "hourly_insert_data_user_dag"],
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=20)
) as dag:

    insert_users_task = PythonOperator(
        task_id="insert_users",
        python_callable=insert_users,
        provide_context=True,
    )

    [insert_users_task]