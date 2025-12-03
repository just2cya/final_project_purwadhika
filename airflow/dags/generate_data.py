
import string 
import random
from datetime import datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook


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
    hour = random.randint(max(0, current_datetime.hour-1), current_datetime.hour)
    minute = random.randint(0, 59)
    second = random.randint(0, 59)

    created_date = f"{year}-{month:02d}-{day:02d} {hour:02d}:{minute:02d}:{second:02d}"

    return datetime.strptime(created_date, "%Y-%m-%d %H:%M:%S")

def generate_product():
    hook = PostgresHook(postgres_conn_id="pg_finpro")
    conn = hook.get_conn()
    cur = conn.cursor()


    # Fetch existing customer & product IDs
    cur.execute("SELECT max(cast(right(product_id,4) as integer)) lastID FROM products;")
    last_ID = [r[0] for r in cur.fetchall()]

    product_id = f"P{(last_ID[0] + 1):04d}"
    created_date = random_timestamp()
    category = random.choice(['Laptop', 'AC', 'Printer', 'Fridge'])
    prod_name = category + ' ' + generate_random_data(10)
    price = round(random.uniform(500000.0, 10000000.0), 2)
    product = [product_id, prod_name, category, price, created_date]
    return product

def generate_user():
    hook = PostgresHook(postgres_conn_id="pg_finpro")
    conn = hook.get_conn()
    cur = conn.cursor()

    # Fetch existing customer IDs
    cur.execute("SELECT max(cast(right(user_id,3) as integer)) lastID FROM users;")
    last_ID = [r[0] for r in cur.fetchall()]

    user_id = f"U{(last_ID[0] + 1):03d}"
    created_date = random_timestamp()
    name = generate_random_data(10)
    email = generate_random_data(10) + "@init.com"
    user = [user_id, name, email, created_date]
    return user


# ---------------------------------------------------------------------------
# 1. Generate & insert product data
# ---------------------------------------------------------------------------
def insert_products(**kwargs):
    hook = PostgresHook(postgres_conn_id="pg_finpro")
    conn = hook.get_conn()
    cur = conn.cursor()
   
    newproduct = generate_product()
    
    hook.insert_rows(
        table="products",
        rows=[newproduct],
        target_fields=["product_id", "product_name", "category", "price", "created_date"]
    )

    print(f"Inserted 1 new product.")

    conn.close()


# ---------------------------------------------------------------------------
# 2. Generate & insert user data
# ---------------------------------------------------------------------------
def insert_users(**kwargs):
    hook = PostgresHook(postgres_conn_id="pg_finpro")
    conn = hook.get_conn()
    cur = conn.cursor()

    newuser = generate_user()
    
    hook.insert_rows(
        table="users",
        rows=[newuser],
        target_fields=["user_id", "name", "email", "created_date"]
    )

    print(f"Inserted 1 new user.")
    
    conn.close()
