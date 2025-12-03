from google.cloud import bigquery

def bq_connection(key):
    """
        Function untuk konek ke bigquery
    """
    client = bigquery.Client.from_service_account_json(key)
    return client

def bq_insert(dataframe, table_id, project_id, dataset_id, client, mode):
    """Insert data into BigQuery table

    Args:
        dataframe (DataFrame): The DataFrame containing the data to insert
        table_id (str): The target table name
        project_id (str): The GCP project ID
        dataset_id (str): The BigQuery dataset ID
        client (Client): The BigQuery client object
    """
    # Create full table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Configure job
    job_config = bigquery.LoadJobConfig(
        write_disposition=mode,  # Append to table
        autodetect=True  # Auto-detect schema
    )
    
    # Load data to BigQuery
    job = client.load_table_from_dataframe(
        dataframe, 
        table_ref, 
        job_config=job_config
    )
    
    # Wait for job to complete
    job.result()
    
    print(f"Loaded {len(dataframe)} rows into {table_ref}")