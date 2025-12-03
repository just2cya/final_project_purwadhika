from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from airflow.models import Variable
from utils import bq_connection, bq_insert
import logging
from telegram_notif import telegram_failure_notification, telegram_retry_notification



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



# ===== CONFIGURATION =====
GCP_PROJECT_ID = 'jcdeah-006' #'capstoneprojectpurwadhika'
DATASET_ID = 'justicia_finpro'
GCP_CONN_ID = 'bq_finpro'
PG_CONN_ID = 'pg_finpro'


# ===== HELPER FUNCTIONS =====
def get_bigquery_client():
    """Get BigQuery client"""
    bq_hook = BigQueryHook(gcp_conn_id=GCP_CONN_ID, use_legacy_sql=False)
    return bq_hook.get_client(project_id=GCP_PROJECT_ID)


# ===== raw users TASKS =====
def extract_users(**context):
    """Extract user data from PostgreSQL - D-1"""
    execution_date = context['execution_date']
    ti = context['ti']
    
    # Calculate D-1 (yesterday)
    yesterday = execution_date - timedelta(days=1)
    yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    print(f"Extracting users from {yesterday_start} to {yesterday_end}")
    
    # Extract from PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    query = f"""
    SELECT 
        user_id,
        name,
        email,
        created_date
    FROM users
    WHERE created_date >= '{yesterday_start}'
    AND created_date <= '{yesterday_end}'
    ORDER BY created_date
    """
    
    df = pg_hook.get_pandas_df(query)
    
    if df.empty:
        print(f"No users found for {yesterday.date()}")
        ti.xcom_push(key='user_data', value=None)
        return
    
    print(f"Extracted {len(df)} users from D-1 ({yesterday.date()})")
    
    # Push to XCom
    ti.xcom_push(key='user_data', value=df.to_json(orient='records', date_format='iso'))
    print(f"Pushed {len(df)} user records to XCom")

def load_users_to_bq(**context):
    """Load user data to BigQuery"""

    logging.info("Starting load_users_to_bq function")
    ti = context['ti']
    
    # Pull data from XCom
    data_json = ti.xcom_pull(task_ids='raw_user_group.extract_users', key='user_data')
    
    if not data_json:
        logging.info("No user data to load")
        return
    
    # Convert JSON back to DataFrame
    df = pd.read_json(data_json, orient='records')
    df['created_date'] = pd.to_datetime(df['created_date'])
    
    logging.info(f"Loading {len(df)} users to BigQuery")
    
    # Load to BigQuery
    # client = get_bigquery_client()
    # table_id = f"{GCP_PROJECT_ID}.{DATASET_ID}.raw_users"
    
    # job_config = bigquery.LoadJobConfig(
    #     write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    #     schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    # )
    
    # job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    # job.result()
    
    bq_client = bq_connection("/opt/airflow/gcp-credentials/gcp-key.json")
    bq_insert(
        dataframe=df,
        table_id="raw_users",
        project_id=GCP_PROJECT_ID,
        dataset_id=DATASET_ID,
        client=bq_client,
        mode="WRITE_APPEND"
    )

    logging.info(f"Successfully loaded {len(df)} users to BigQuery")


# ===== RAW Products TASKS =====
def extract_products(**context):
    """Extract product data from PostgreSQL - D-1"""
    execution_date = context['execution_date']
    ti = context['ti']
    
    # Calculate D-1 (yesterday)
    yesterday = execution_date - timedelta(days=1)
    yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    print(f"Extracting products from {yesterday_start} to {yesterday_end}")
    
    # Extract from PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    query = f"""
    SELECT 
        product_id,
        product_name,
        category,
        price,
        created_date
    FROM products
    WHERE created_date >= '{yesterday_start}'
    AND created_date <= '{yesterday_end}'
    ORDER BY created_date
    """

    df = pg_hook.get_pandas_df(query)
    
    if df.empty:
        logging.info(f"No products found for {yesterday.date()}")
        ti.xcom_push(key='product_data', value=None)
        return
    
    logging.info(f"Extracted {len(df)} products from D-1 ({yesterday.date()})")
    
    # Push to XCom
    ti.xcom_push(key='product_data', value=df.to_json(orient='records', date_format='iso'))
    logging.info(f"Pushed {len(df)} product records to XCom")


def load_products_to_bq(**context):
    """Load product data to BigQuery"""
    ti = context['ti']
    
    # Pull data from XCom
    data_json = ti.xcom_pull(task_ids='raw_product_group.extract_products', key='product_data')
    
    if not data_json:
        print("No product data to load")
        return
    
    # Convert JSON back to DataFrame
    df = pd.read_json(data_json, orient='records')
    df['created_date'] = pd.to_datetime(df['created_date'])
    
    print(f"Loading {len(df)} products to BigQuery")
    
    # Load to BigQuery
    # client = get_bigquery_client()
    # table_id = f"{GCP_PROJECT_ID}.{DATASET_ID}.raw_products"
    
    # job_config = bigquery.LoadJobConfig(
    #     write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    #     schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    # )
    
    # job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    # job.result()

    bq_client = bq_connection("/opt/airflow/gcp-credentials/gcp-key.json")
    bq_insert(
        dataframe=df,
        table_id="raw_products",
        project_id=GCP_PROJECT_ID,
        dataset_id=DATASET_ID,
        client=bq_client,
        mode="WRITE_APPEND"
    )

    
    print(f"Successfully loaded {len(df)} products to BigQuery")


# ===== RAW Orders TASKS =====
def extract_orders(**context):
    """Extract order data from PostgreSQL - D-1"""
    execution_date = context['execution_date']
    ti = context['ti']
    
    # Calculate D-1 (yesterday)
    yesterday = execution_date - timedelta(days=1)
    yesterday_start = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)
    yesterday_end = yesterday.replace(hour=23, minute=59, second=59, microsecond=999999)
    
    print(f"Extracting orders from {yesterday_start} to {yesterday_end}")
    
    # Extract from PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=PG_CONN_ID)
    query = f"""
    SELECT 
        order_id,
        user_id,
        product_id,
        quantity,
        amount,
        country,
        created_date,
        status
    FROM orders
    WHERE created_date >= '{yesterday_start}'
    AND created_date <= '{yesterday_end}'
    ORDER BY created_date
    """
    df = pg_hook.get_pandas_df(query)
    
    if df.empty:
        print(f"No order found for {yesterday.date()}")
        ti.xcom_push(key='order_data', value=None)
        return
    
    print(f"Extracted {len(df)} orders from D-1 ({yesterday.date()})")
    
    # Push to XCom
    ti.xcom_push(key='order_data', value=df.to_json(orient='records', date_format='iso'))
    print(f"Pushed {len(df)} order records to XCom")


def load_orders_to_bq(**context):
    """Load order data to BigQuery"""
    ti = context['ti']
    
    # Pull data from XCom
    data_json = ti.xcom_pull(task_ids='raw_order_group.extract_orders', key='order_data')
    
    if not data_json:
        print("No order data to load")
        return
    
    # Convert JSON back to DataFrame
    df = pd.read_json(data_json, orient='records')
    df['created_date'] = pd.to_datetime(df['created_date'])
    
    print(f"Loading {len(df)} orders to BigQuery")
    
    # Load to BigQuery
    client = get_bigquery_client()
    table_id = f"{GCP_PROJECT_ID}.{DATASET_ID}.raw_orders"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )
    
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    
    print(f"Successfully loaded {len(df)} orders to BigQuery")


# ===== DAG DEFINITION =====
with DAG(
    dag_id='postgres_to_bigquery_dwh',
    default_args=default_args,
    description='Daily incremental load ecommerce dwh',
    schedule_interval='@daily',
    catchup=False,
    tags=['bigquery', 'data-warehouse', 'etl', 'incremental', 'daily'],
    dagrun_timeout=timedelta(minutes=30)
) as dag:
    
   
    
    # Task Group: raw_users
    with TaskGroup('raw_user_group', tooltip='Load user raw data') as raw_user_group:
        extract_users = PythonOperator(
            task_id='extract_users',
            python_callable=extract_users,
            provide_context=True,
            on_failure_callback=telegram_failure_notification,
            on_retry_callback=telegram_retry_notification
        )
        
        load_users = PythonOperator(
            task_id='load_users_to_bq',
            python_callable=load_users_to_bq,
            provide_context=True,
            on_failure_callback=telegram_failure_notification,
            on_retry_callback=telegram_retry_notification
        )
        
        extract_users >> load_users
    
    # Task Group: raw_products
    with TaskGroup('raw_product_group', tooltip='Load product raw data') as raw_product_group:
        extract_products = PythonOperator(
            task_id='extract_products',
            python_callable=extract_products,
            provide_context=True,
            on_failure_callback=telegram_failure_notification,
            on_retry_callback=telegram_retry_notification
        )
        
        load_products = PythonOperator(
            task_id='load_products_to_bq',
            python_callable=load_products_to_bq,
            provide_context=True,
            on_failure_callback=telegram_failure_notification,
            on_retry_callback=telegram_retry_notification
        )
        
        extract_products >> load_products
    
    # Task Group: orders
    with TaskGroup('raw_order_group', tooltip='Load order raw data') as raw_order_group:
        extract_orders = PythonOperator(
            task_id='extract_orders',
            python_callable=extract_orders,
            provide_context=True,
            on_failure_callback=telegram_failure_notification,
            on_retry_callback=telegram_retry_notification
        )
        
        load_orders = PythonOperator(
            task_id='load_orders_to_bq',
            python_callable=load_orders_to_bq,
            provide_context=True,
            on_failure_callback=telegram_failure_notification,
            on_retry_callback=telegram_retry_notification
        )
        
        extract_orders >> load_orders
    
  
    # Task dependencies
    [raw_user_group, raw_product_group] >> raw_order_group 
        
      
    