"""
Live Continuous Order & Event Stream Producer
Simulates live incoming marketplace traffic.
Queries active customers, sellers, and products from PostgreSQL,
generates a batch of new orders with realistic lifecycles, and commits them.
Can run locally or as a scheduled GitHub Action in the cloud.
"""

import os
import random
from datetime import datetime, timedelta
from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

from data_generator.orders import PAYMENT_METHODS, PAYMENT_FAILURE_REASONS

load_dotenv()


def get_connection():
    db_url = os.getenv("DATABASE_URL") or os.getenv("NEON_DATABASE_URL")
    if not db_url:
        raise ValueError("Neither DATABASE_URL nor NEON_DATABASE_URL found in environment!")
    return psycopg2.connect(db_url)


def fetch_existing_entities(conn):
    """Fetch existing customers, sellers, and products for live transaction generation."""
    with conn.cursor() as cur:
        # Customers
        cur.execute("SELECT customer_id FROM customers;")
        customers = [r[0] for r in cur.fetchall()]

        # Sellers and skew status
        cur.execute("SELECT seller_id, is_top_seller FROM sellers;")
        seller_records = cur.fetchall()
        seller_ids = [r[0] for r in seller_records]
        
        # Calculate skew weights (Top 1% sellers get 60% of orders)
        top_sellers = [r[0] for r in seller_records if r[1]]
        regular_sellers = [r[0] for r in seller_records if not r[1]]
        
        seller_weights = []
        top_count = max(1, len(top_sellers))
        reg_count = max(1, len(regular_sellers))
        for sid, is_top in seller_records:
            if is_top:
                seller_weights.append(0.60 / top_count)
            else:
                seller_weights.append(0.40 / reg_count)

        # Products mapped by seller
        cur.execute("SELECT product_id, seller_id, price FROM products;")
        prod_records = cur.fetchall()
        seller_to_prods = {}
        all_prods = []
        for pid, sid, price in prod_records:
            item = {"product_id": pid, "seller_id": sid, "price": float(price)}
            all_prods.append(item)
            if sid not in seller_to_prods:
                seller_to_prods[sid] = []
            seller_to_prods[sid].append(item)

        # Get current MAX order count for sequential IDs
        cur.execute("SELECT COUNT(*) FROM orders;")
        current_order_count = cur.fetchone()[0]

    return customers, seller_ids, seller_weights, seller_to_prods, all_prods, current_order_count


def generate_live_order_batch(
    batch_size: int,
    customers: List[str],
    seller_ids: List[str],
    seller_weights: List[float],
    seller_to_prods: Dict[str, Any],
    all_prods: List[Dict[str, Any]],
    start_index: int,
    rng: random.Random
):
    orders = []
    payments = []
    inv_events = []
    now = datetime.now()

    for idx in range(1, batch_size + 1):
        order_num = start_index + idx
        order_id = f"ORD_{order_num:08d}"
        payment_id = f"PAY_{order_num:08d}"
        inv_event_id = f"INV_{order_num:08d}"

        # Skewed seller selection
        seller_id = rng.choices(seller_ids, weights=seller_weights, k=1)[0]
        customer_id = rng.choice(customers)

        seller_prods = seller_to_prods.get(seller_id)
        product = rng.choice(seller_prods) if seller_prods else rng.choice(all_prods)

        quantity = rng.choices([1, 2, 3], weights=[0.80, 0.15, 0.05], k=1)[0]
        unit_price = product["price"]
        order_amount = round(quantity * unit_price, 2)

        order_ts = now - timedelta(seconds=rng.randint(0, 300))
        ingested_ts = now

        # Payment status
        is_fail = (rng.random() < 0.07)
        if is_fail:
            payment_status = "FAILED"
            order_status = "CANCELLED"
            fail_reason = rng.choice(PAYMENT_FAILURE_REASONS)
        else:
            payment_status = "SUCCESS"
            order_status = "COMPLETED"
            fail_reason = None

        orders.append((
            order_id, customer_id, seller_id, product["product_id"],
            order_ts.strftime("%Y-%m-%d %H:%M:%S"),
            ingested_ts.strftime("%Y-%m-%d %H:%M:%S"),
            quantity, unit_price, order_amount, order_status, False
        ))

        payments.append((
            payment_id, order_id, customer_id,
            order_ts.strftime("%Y-%m-%d %H:%M:%S"),
            rng.choice(PAYMENT_METHODS),
            order_amount, payment_status, fail_reason
        ))

        if order_status == "COMPLETED":
            inv_events.append((
                inv_event_id, product["product_id"], seller_id,
                order_ts.strftime("%Y-%m-%d %H:%M:%S"),
                -quantity, "STOCK_RESERVED"
            ))

    return orders, payments, inv_events


def insert_batch(conn, orders, payments, inv_events):
    with conn.cursor() as cur:
        # Orders
        execute_values(
            cur,
            """
            INSERT INTO orders 
            (order_id, customer_id, seller_id, product_id, order_ts, ingested_ts, quantity, unit_price, order_amount, status, is_late_arrival)
            VALUES %s
            ON CONFLICT (order_id) DO NOTHING;
            """,
            orders
        )
        # Payments
        execute_values(
            cur,
            """
            INSERT INTO payments 
            (payment_id, order_id, customer_id, payment_ts, payment_method, payment_amount, payment_status, failure_reason)
            VALUES %s
            ON CONFLICT (payment_id) DO NOTHING;
            """,
            payments
        )
        # Inventory
        if inv_events:
            execute_values(
                cur,
                """
                INSERT INTO inventory_events 
                (inventory_event_id, product_id, seller_id, event_ts, quantity_delta, event_type)
                VALUES %s
                ON CONFLICT (inventory_event_id) DO NOTHING;
                """,
                inv_events
            )
    conn.commit()


def run_stream_batch(batch_size: int = 50):
    conn = get_connection()
    try:
        rng = random.Random()
        (
            customers, seller_ids, seller_weights,
            seller_to_prods, all_prods, current_count
        ) = fetch_existing_entities(conn)

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Generating live batch of {batch_size} orders (Starting from #{current_count + 1})...")
        orders, payments, inv_events = generate_live_order_batch(
            batch_size, customers, seller_ids, seller_weights,
            seller_to_prods, all_prods, current_count, rng
        )
        insert_batch(conn, orders, payments, inv_events)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Successfully streamed {len(orders)} live orders into Neon Cloud!")
    finally:
        conn.close()


if __name__ == "__main__":
    import sys
    count = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    run_stream_batch(batch_size=count)
