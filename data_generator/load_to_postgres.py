"""
PostgreSQL OLTP Ingestion & Schema Migration Script
Connects to PostgreSQL (Local or Neon Cloud), creates normalized tables,
and bulk-inserts the generated synthetic marketplace dataset.
"""

import os
import csv
from pathlib import Path
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL not found in environment or .env file!")


def get_connection():
    return psycopg2.connect(DATABASE_URL)


def initialize_schema(conn, sql_path: Path):
    print(f"\n[1/3] Executing schema DDL from '{sql_path}'...")
    with open(sql_path, "r", encoding="utf-8") as f:
        ddl = f.read()
    
    with conn.cursor() as cur:
        cur.execute(ddl)
    conn.commit()
    print("  -> PostgreSQL schema tables and indexes verified successfully!")


def load_csv_data(filepath: Path):
    if not filepath.exists():
        print(f"  [WARN] File not found: {filepath}")
        return []
    with open(filepath, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def bulk_insert_dataset(conn, data_dir: Path):
    print(f"\n[2/3] Ingesting CSV datasets from '{data_dir}' into Neon PostgreSQL...")
    
    with conn.cursor() as cur:
        # 1. Customers
        customers = load_csv_data(data_dir / "customers.csv")
        if customers:
            cust_rows = [
                (c["customer_id"], c["first_name"], c["last_name"], c["full_name"],
                 c["email"], c["phone"], c["city"], c["state"], c["segment"], c["signup_ts"])
                for c in customers
            ]
            query = """
                INSERT INTO customers 
                (customer_id, first_name, last_name, full_name, email, phone, city, state, segment, signup_ts)
                VALUES %s
                ON CONFLICT (customer_id) DO NOTHING;
            """
            execute_values(cur, query, cust_rows, page_size=2000)
            print(f"  -> Inserted {len(cust_rows):,} customers")

        # 2. Sellers
        sellers = load_csv_data(data_dir / "sellers.csv")
        if sellers:
            seller_rows = [
                (s["seller_id"], s["seller_name"], s["category"], s["city"],
                 s["state"], float(s["rating"]), s["is_top_seller"].lower() == "true", s["registration_ts"])
                for s in sellers
            ]
            query = """
                INSERT INTO sellers 
                (seller_id, seller_name, category, city, state, rating, is_top_seller, registration_ts)
                VALUES %s
                ON CONFLICT (seller_id) DO NOTHING;
            """
            execute_values(cur, query, seller_rows, page_size=1000)
            print(f"  -> Inserted {len(seller_rows):,} sellers")

        # 3. Products
        products = load_csv_data(data_dir / "products.csv")
        if products:
            prod_rows = [
                (p["product_id"], p["seller_id"], p["product_name"], p["category"],
                 p["sub_category"], float(p["price"]), float(p["cost"]), int(p["stock_quantity"]), p["created_ts"])
                for p in products
            ]
            query = """
                INSERT INTO products 
                (product_id, seller_id, product_name, category, sub_category, price, cost, stock_quantity, created_ts)
                VALUES %s
                ON CONFLICT (product_id) DO NOTHING;
            """
            execute_values(cur, query, prod_rows, page_size=2000)
            print(f"  -> Inserted {len(prod_rows):,} products")

        # 4. Orders
        orders = load_csv_data(data_dir / "orders.csv")
        if orders:
            order_rows = [
                (o["order_id"], o["customer_id"], o["seller_id"], o["product_id"],
                 o["order_ts"], o["ingested_ts"], int(o["quantity"]), float(o["unit_price"]),
                 float(o["order_amount"]), o["status"], o["is_late_arrival"].lower() == "true")
                for o in orders
            ]
            query = """
                INSERT INTO orders 
                (order_id, customer_id, seller_id, product_id, order_ts, ingested_ts, quantity, unit_price, order_amount, status, is_late_arrival)
                VALUES %s
                ON CONFLICT (order_id) DO NOTHING;
            """
            execute_values(cur, query, order_rows, page_size=2000)
            print(f"  -> Inserted {len(order_rows):,} orders")

        # 5. Payments
        payments = load_csv_data(data_dir / "payments.csv")
        if payments:
            pay_rows = [
                (p["payment_id"], p["order_id"], p["customer_id"], p["payment_ts"],
                 p["payment_method"], float(p["payment_amount"]), p["payment_status"],
                 p["failure_reason"] if p["failure_reason"] != "None" and p["failure_reason"] else None)
                for p in payments
            ]
            query = """
                INSERT INTO payments 
                (payment_id, order_id, customer_id, payment_ts, payment_method, payment_amount, payment_status, failure_reason)
                VALUES %s
                ON CONFLICT (payment_id) DO NOTHING;
            """
            execute_values(cur, query, pay_rows, page_size=2000)
            print(f"  -> Inserted {len(pay_rows):,} payments")

        # 6. Inventory Events
        inv_events = load_csv_data(data_dir / "inventory_events.csv")
        if inv_events:
            inv_rows = [
                (e["inventory_event_id"], e["product_id"], e["seller_id"],
                 e["event_ts"], int(e["quantity_delta"]), e["event_type"])
                for e in inv_events
            ]
            query = """
                INSERT INTO inventory_events 
                (inventory_event_id, product_id, seller_id, event_ts, quantity_delta, event_type)
                VALUES %s
                ON CONFLICT (inventory_event_id) DO NOTHING;
            """
            execute_values(cur, query, inv_rows, page_size=2000)
            print(f"  -> Inserted {len(inv_rows):,} inventory events")

    conn.commit()


def verify_counts(conn):
    print("\n[3/3] Live Verification: Row counts in Neon Cloud PostgreSQL:")
    tables = ["customers", "sellers", "products", "orders", "payments", "inventory_events"]
    with conn.cursor() as cur:
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM {t};")
            count = cur.fetchone()[0]
            print(f"  Table '{t}': {count:,} rows")


def main():
    print("=" * 70)
    print("  NEON CLOUD POSTGRESQL INGESTION")
    print("=" * 70)
    
    root_dir = Path(__file__).parent.parent
    sql_path = root_dir / "sql" / "init_oltp.sql"
    data_dir = root_dir / "data" / "sample"

    conn = get_connection()
    try:
        initialize_schema(conn, sql_path)
        bulk_insert_dataset(conn, data_dir)
        verify_counts(conn)
        print("\nSUCCESS: All transactional data successfully populated in Neon Cloud PostgreSQL!")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
