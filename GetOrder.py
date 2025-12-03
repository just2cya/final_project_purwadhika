import threading
import time
import psycopg2
from google.cloud import pubsub_v1
from json import loads, dumps
import json
from datetime import datetime, timezone
from datetime import time as dt_time
import string 
import random
import calendar

# Global variables for timestamp generation
today = datetime.now()
current_datetime = datetime.now()

def generate_random_data(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))


def random_timestamp():
    year = 2025
    month = today.month
    day = today.day
    #month = random.randint(1,12)

    #num_days = calendar.monthrange(year, month)[1]
    #day = random.randint(1,num_days)

    hour = random.randint(0,23)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    # dt_string = f"{year}-{month:02d}-{day:02d}T{hour:02d}:{minute:02d}:{second:02d}"
    utc_datetime = datetime(year, month, day, hour, minute, second, tzinfo=timezone.utc)
    dt_string = utc_datetime.strftime("%Y-%m-%dT%H:%M:%SZ")

    print(f"Generated random timestamp: {dt_string}")
    return dt_string

def generate_Order():
    # Fetch existing customer & product IDs
    try:
        connection = psycopg2.connect(
            user="myuser",
            password="mypassword",
            host="34.123.225.122",
            port="5434",
            database="ecommerce"
        )
        cur = connection.cursor()

        cur.execute("SELECT user_id FROM users;")
        users = [r[0] for r in cur.fetchall()]
        cur.execute("SELECT product_id, price FROM products;")
        products = cur.fetchall()
        
        cur.close()
        connection.close()

        if not users or not products:
            print("No users or products found - skipping transactions.")
            conn.close()
            return
        else:
            user_id = random.choice(users)
            prod = random.choice(products)
            prod_id = prod[0]
            price = prod[1]
            quantity = random.randint(1, 150)  
            amount = price * quantity
            country = random.choice(['ID', 'US', 'UK'])
            created_date = random_timestamp()

            order_data = {
                "order_id": generate_random_data(10),
                "user_id": user_id,
                "product_id": prod_id,
                "quantity": quantity,
                "amount": str(amount),
                "country": country,
                "created_date": created_date
            }

            return order_data

    except (Exception, psycopg2.Error) as error:
        print(f"Error while generating order: {error}")


    

def send_order_to_pubsub(order_data):
    project_id = "CapstoneProjectPurwadhika"
    topic_id = "Justicia-FinPro-Order"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    
    json_string = json.dumps(order_data, ensure_ascii=False)  # ensure_ascii=False handles non-ASCII characters
    data = json_string.encode("utf-8")

    print(order_data)
    print(f"Sending order data to {topic_path}: {json_string}")

    future = publisher.publish(topic_path, data=data)
    message_id = future.result()

    print(f"Order sent to {topic_path} ; message ID: {message_id}")  

def is_time_between(start_time, end_time, check_time):
    """
    Checks if a given time falls within a specified time range.
    Handles cases where the time range spans across midnight.
    """
    if start_time <= end_time:
        return start_time <= check_time <= end_time
    else:  # Range spans across midnight (e.g., 22:00 to 06:00)
        return start_time <= check_time or check_time <= end_time

def validator(message):
    data = message.data.decode('utf-8')
    data = loads(data)
    country = data.get("country", "")
    quantity = data.get("quantity", 0)
    created_date = data.get("created_date", "")
    print(f"Received date: {created_date}")
    dt_object = datetime.strptime(created_date, "%Y-%m-%dT%H:%M:%SZ")

    # Convert the datetime object to UTC.
    # Since the input string does not contain timezone information,
    # we assume it's in local time and then convert to UTC.
    # If the input was known to be in a specific timezone, you would localize it first.
    utc_dt_object = dt_object.astimezone(timezone.utc)


    status = ""

    start_suspicious_hour = dt_time(0, 0, 0)  # 00:00:00 AM
    end_suspicious_hour = dt_time(4, 0, 0)  # 4:00:00 AM


    # print(data)
    # print(type(data))
    # message.ack()

    
    if country!="ID" or quantity > 100 or is_time_between(start_suspicious_hour, end_suspicious_hour, utc_dt_object.time()):
        status += "fraud"
    else:
        status += "genuine"

   
    data["status"] = status

    project_id = "CapstoneProjectPurwadhika"
    topic_id = "Justicia-FinPro-InsertToDB"
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    
    json_string = json.dumps(data, ensure_ascii=False)  # ensure_ascii=False handles non-ASCII characters
    newdata = json_string.encode("utf-8")

    future = publisher.publish(topic_path, data=newdata)
    message_id = future.result()

    print(f"Transaksi mencurigakan : {message.data.decode('utf-8')}" ) if status == "fraud" else print(f"Transaksi genuine : {message.data.decode('utf-8')}")
    print(f"Diteruskan ke {topic_path} ; message ID: {message_id}")

    message.ack()

def pubsub_listener():  
    project_id = "CapstoneProjectPurwadhika"
    subscription_id = "Justicia-FinPro-Order-sub"

    topic_id = "Justicia-FinPro-InsertToDB"

    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id,subscription_id)

    streaming_pull_future = subscriber.subscribe(subscription_path, callback=validator)

    with subscriber:
        try:
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()
            streaming_pull_future.result()

if __name__ == "__main__":
    pubsub_thread = threading.Thread(target=pubsub_listener)
    pubsub_thread.start()

    interval_seconds = 5

    while True:
        order_data = generate_Order()
        send_order_to_pubsub(order_data)
        time.sleep(interval_seconds)