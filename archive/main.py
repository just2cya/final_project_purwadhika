from utils import bq_connection, bq_insert
import psycopg2
import pandas as pd
from google.cloud import bigquery

if __name__ == "__main__":
    connection = psycopg2.connect(
        user="myuser",
        password="mypassword",
        host="34.123.225.122",
        port="5434",
        database="ecommerce"
    )
   
    # cur.execute("SELECT * FROM users limit 10;")

    df = pd.read_sql_query("SELECT * FROM products limit 10;", connection)
  

    connection.close()
    print(f"Extracted {len(df)} rows from Mysql")
    print(df.head())

    # Load data to BigQuery
    bq_client = bq_connection("gcp-keyjj.json")
    bq_insert(
        dataframe=df,
        table_id="raw_products",
        project_id="capstoneprojectpurwadhika",
        dataset_id="justicia_finpro",
        client=bq_client,
        mode="WRITE_APPEND"
    )

    print("Data successfully transferred from postgres to BigQuery!")