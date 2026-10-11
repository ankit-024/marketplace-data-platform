"""
Orders, Payments Lifecycle, and Inventory Events Generator
Simulates marketplace transactions with realistic status transitions, payment failures,
and inventory decrements. Applies skew weights so top sellers receive ~60% of total volume.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import random
import uuid

PAYMENT_METHODS = ["UPI", "CREDIT_CARD", "DEBIT_CARD", "NET_BANKING", "COD"]
PAYMENT_FAILURE_REASONS = ["BANK_GATEWAY_TIMEOUT", "INSUFFICIENT_BALANCE", "AUTH_FAILED", "USER_CANCELLED"]


def generate_orders_and_payments(
    orders_count: int,
    customers: List[Dict[str, Any]],
    sellers: List[Dict[str, Any]],
    seller_weights: List[float],
    products: List[Dict[str, Any]],
    start_date: datetime,
    end_date: datetime,
    payment_failure_rate: float,
    cancellation_rate: float,
    late_event_pct: float,
    max_delay_minutes: int,
    rng: random.Random
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Generate orders, corresponding payment attempts, and inventory delta events.
    
    Returns:
        (orders, payments, inventory_events)
    """
    orders = []
    payments = []
    inventory_events = []

    total_seconds = int((end_date - start_date).total_seconds())
    customer_ids = [c["customer_id"] for c in customers]
    seller_ids = [s["seller_id"] for s in sellers]

    # Group products by seller_id for fast lookup
    seller_to_products = {}
    for p in products:
        s_id = p["seller_id"]
        if s_id not in seller_to_products:
            seller_to_products[s_id] = []
        seller_to_products[s_id].append(p)

    for i in range(1, orders_count + 1):
        order_id = f"ORD_{i:08d}"

        # 1. Select seller (skew-weighted!)
        seller_id = rng.choices(seller_ids, weights=seller_weights, k=1)[0]
        customer_id = rng.choice(customer_ids)

        # 2. Select product belonging to this seller (or fallback to any product)
        seller_prods = seller_to_products.get(seller_id)
        if seller_prods:
            product = rng.choice(seller_prods)
        else:
            product = rng.choice(products)

        product_id = product["product_id"]
        unit_price = product["price"]

        # Realistic quantity distribution (most orders are 1 or 2 items)
        quantity = rng.choices([1, 2, 3, 4, 5], weights=[0.70, 0.18, 0.07, 0.03, 0.02], k=1)[0]
        total_amount = round(unit_price * quantity, 2)

        # 3. Base Order Timestamp
        rand_sec = rng.randint(0, total_seconds)
        base_order_ts = start_date + timedelta(seconds=rand_sec)

        # Late-arrival injection simulation (out-of-order event streaming)
        is_late_event = (rng.random() < late_event_pct)
        if is_late_event:
            delay_sec = rng.randint(60, max_delay_minutes * 60)
            ingested_ts = base_order_ts + timedelta(seconds=delay_sec)
        else:
            ingested_ts = base_order_ts + timedelta(seconds=rng.randint(1, 5))

        # 4. Payment Lifecycle
        payment_id = f"PAY_{i:08d}"
        payment_method = rng.choice(PAYMENT_METHODS)
        payment_ts = base_order_ts + timedelta(seconds=rng.randint(30, 300))

        # Check if payment fails
        is_payment_failed = (rng.random() < payment_failure_rate)
        if is_payment_failed:
            payment_status = "FAILED"
            order_status = "CANCELLED"
            failure_reason = rng.choice(PAYMENT_FAILURE_REASONS)
        else:
            payment_status = "SUCCESS"
            failure_reason = None
            # Some orders get cancelled post-payment (refunds)
            if rng.random() < cancellation_rate:
                order_status = "CANCELLED"
            else:
                order_status = "COMPLETED"

        order_record = {
            "order_id": order_id,
            "customer_id": customer_id,
            "seller_id": seller_id,
            "product_id": product_id,
            "order_ts": base_order_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "ingested_ts": ingested_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "quantity": quantity,
            "unit_price": unit_price,
            "order_amount": total_amount,
            "status": order_status,
            "is_late_arrival": is_late_event
        }
        orders.append(order_record)

        payment_record = {
            "payment_id": payment_id,
            "order_id": order_id,
            "customer_id": customer_id,
            "payment_ts": payment_ts.strftime("%Y-%m-%d %H:%M:%S"),
            "payment_method": payment_method,
            "payment_amount": total_amount,
            "payment_status": payment_status,
            "failure_reason": failure_reason
        }
        payments.append(payment_record)

        # 5. Inventory reservation event (decrements stock on valid order)
        if order_status != "CANCELLED":
            inv_event_id = f"INV_{i:08d}"
            inventory_events.append({
                "inventory_event_id": inv_event_id,
                "product_id": product_id,
                "seller_id": seller_id,
                "event_ts": base_order_ts.strftime("%Y-%m-%d %H:%M:%S"),
                "quantity_delta": -quantity,
                "event_type": "STOCK_RESERVED"
            })

    return orders, payments, inventory_events
