import psycopg2
from google.cloud import pubsub_v1
from json import loads, dumps
import json
from datetime import datetime, timezone, time

project_id = "CapstoneProjectPurwadhika"
subscription_id = "Justicia-FinPro-InsertToDB-sub"

topic_id = "Justicia-FinPro-InsertToDB"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id,subscription_id)

def insertToPostgres(message):
    print(f"Received message id {message.message_id} for insertion to Postgres : {message.data.decode('utf-8')}")
    try:
        connection = psycopg2.connect(
            user="myuser",
            password="mypassword",
            host="34.123.225.122",
            port="5434",
            database="ecommerce"
        )
        cursor = connection.cursor()

        data_to_insert = loads(message.data.decode('utf-8'))

        created_date = data_to_insert.get("created_date", "")

        dt_object = datetime.strptime(created_date, "%Y-%m-%dT%H:%M:%SZ")

        
        sql_query = """INSERT INTO public.orders
                        (order_id, user_id, product_id, quantity, amount, country, created_date, status)
                        VALUES(%s, %s, %s, %s, %s, %s, %s, %s);"""
        cursor.execute(
            sql_query, 
            (data_to_insert['order_id'], 
            data_to_insert['user_id'], 
            data_to_insert['product_id'], 
            data_to_insert['quantity'], 
            data_to_insert['amount'], 
            data_to_insert['country'], 
            dt_object,data_to_insert['status'])
        )

        connection.commit()
        cursor.close()
        connection.close()

        message.ack()
        print("Data inserted successfully into orders table")

    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        # Handle the error appropriately, e.g., exit or raise


    

streaming_pull_future = subscriber.subscribe(subscription_path, callback=insertToPostgres)

with subscriber:
    try:
        streaming_pull_future.result()
    except TimeoutError:
        streaming_pull_future.cancel()
        streaming_pull_future.result()
