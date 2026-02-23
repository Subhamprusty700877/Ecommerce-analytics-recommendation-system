import pandas as pd
import snowflake.connector

print("Starting connection...")

conn = snowflake.connector.connect(
    user="#*#*#*#*",
    password="#*#*#*#*#*",
    account="#*#*#*#*#*",
    warehouse="COMPUTE_WH",
    database="ECOMMERCE_DW",
    schema="ANALYTICS",
    role="ACCOUNTADMIN"
)

print("Connected.")

cur = conn.cursor()

# -----------------------
# Read CSV files
# -----------------------
customers = pd.read_csv("../data/customers.csv")
products = pd.read_csv("../data/products.csv")
orders = pd.read_csv("../data/orders.csv")
order_items = pd.read_csv("../data/order_items.csv")

# Reset index to avoid pandas row-index confusion
customers = customers.reset_index(drop=True)
products = products.reset_index(drop=True)
orders = orders.reset_index(drop=True)
order_items = order_items.reset_index(drop=True)

# -----------------------
# Load staging tables
# -----------------------
cur.execute("TRUNCATE TABLE orders_temp")
cur.execute("TRUNCATE TABLE order_items_temp")

for _, r in orders.iterrows():
    cur.execute(
        "INSERT INTO orders_temp VALUES (%s, %s, %s)",
        (int(r["order_id"]), int(r["customer_id"]), r["order_date"])
    )

for _, r in order_items.iterrows():
    cur.execute(
        "INSERT INTO order_items_temp VALUES (%s, %s, %s, %s)",
        (
            int(r["order_item_id"]),
            int(r["order_id"]),
            int(r["product_id"]),
            int(r["quantity"])
        )
    )

# -----------------------
# Load dimensions
# -----------------------
cur.execute("TRUNCATE TABLE dim_customer")
cur.execute("TRUNCATE TABLE dim_product")
cur.execute("TRUNCATE TABLE dim_date")

# ---- DIM_CUSTOMER (bug fixed here)
for _, r in customers.iterrows():
    cur.execute(
        """
        INSERT INTO dim_customer(customer_id, name, email, city, country)
        VALUES (%s, %s, %s, %s, %s)
        """,
        (
            int(r["customer_id"]),
            r["name"],
            r["email"],
            r["city"],
            r["country"]
        )
    )

# ---- DIM_PRODUCT
for _, r in products.iterrows():
    cur.execute(
        """
        INSERT INTO dim_product(product_id, product_name, category, price)
        VALUES (%s, %s, %s, %s)
        """,
        (
            int(r["product_id"]),
            r["product_name"],
            r["category"],
            float(r["price"])
        )
    )

# ---- DIM_DATE
dates = pd.to_datetime(orders["order_date"]).dt.date.drop_duplicates()

for d in dates:
    cur.execute(
        "INSERT INTO dim_date(full_date) VALUES (%s)",
        (str(d.date()),)
    )

# -----------------------
# Load fact table
# -----------------------
cur.execute("TRUNCATE TABLE fact_sales")

fact_sql = """
INSERT INTO fact_sales(customer_key, product_key, date_key, order_id, quantity)
SELECT
    c.customer_key,
    p.product_key,
    d.date_key,
    oi.order_id,
    oi.quantity
FROM order_items_temp oi
JOIN orders_temp o
    ON oi.order_id = o.order_id
JOIN dim_customer c
    ON c.customer_id = o.customer_id
JOIN dim_product p
    ON p.product_id = oi.product_id
JOIN dim_date d
    ON d.full_date = o.order_date
"""

cur.execute(fact_sql)

conn.commit()

cur.close()
conn.close()

print("ETL completed successfully")