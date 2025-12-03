from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from generate_data import insert_products

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="hourly_insert_data_product_dag",
    start_date=datetime(2025, 11, 30),
    schedule_interval='@hourly',
    catchup=False,
    description="Hourly product data generation for ecommerce system",
    tags=["ecommerce", "hourly", "hourly_insert_data_product_dag"],
    default_args=default_args,
    dagrun_timeout=timedelta(minutes=20)
) as dag:

    insert_products_task = PythonOperator(
        task_id="insert_products",
        python_callable=insert_products,
        provide_context=True,
    )

    [insert_products_task]