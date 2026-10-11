"""
Clickstream Events Generator (Product Views & Search Events)
Generates high-throughput streaming events for Kafka and Spark Structured Streaming.
Popular products receive more views, simulating realistic user engagement.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import random
import uuid

DEVICES = ["MOBILE_APP", "MOBILE_WEB", "DESKTOP_WEB"]
DEVICE_WEIGHTS = [0.65, 0.20, 0.15]

POPULAR_SEARCH_TERMS = [
    "wireless earphones", "running shoes", "cotton t shirt", "smart watch", "car charger",
    "laptop bag", "water bottle", "yoga mat", "bluetooth speaker", "face serum",
    "gaming mouse", "power bank", "drill machine", "cricket bat", "coffee maker"
]


def generate_clickstream_events(
    views_count: int,
    searches_count: int,
    customers: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    start_date: datetime,
    end_date: datetime,
    rng: random.Random
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """
    Generate product views and search events.
    
    Returns:
        (product_views, search_events)
    """
    product_views = []
    search_events = []

    total_seconds = int((end_date - start_date).total_seconds())
    customer_ids = [c["customer_id"] for c in customers]

    # Pre-weight products (Top 5% products drive ~50% of views)
    num_products = len(products)
    top_p_count = max(1, int(num_products * 0.05))
    product_weights = []
    for i in range(num_products):
        if i < top_p_count:
            product_weights.append(0.50 / top_p_count)
        else:
            product_weights.append(0.50 / (num_products - top_p_count))

    # 1. Product Views
    for i in range(1, views_count + 1):
        product = rng.choices(products, weights=product_weights, k=1)[0]
        customer_id = rng.choice(customer_ids)
        device = rng.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]

        rand_sec = rng.randint(0, total_seconds)
        event_ts = start_date + timedelta(seconds=rand_sec)
        session_id = f"SESS_{customer_id[-5:]}_{rng.randint(1000, 9999)}"

        view_record = {
            "view_id": f"VIEW_{i:08d}",
            "customer_id": customer_id,
            "product_id": product["product_id"],
            "seller_id": product["seller_id"],
            "device": device,
            "session_id": session_id,
            "event_ts": event_ts.strftime("%Y-%m-%d %H:%M:%S")
        }
        product_views.append(view_record)

    # 2. Search Events
    for j in range(1, searches_count + 1):
        customer_id = rng.choice(customer_ids)
        query = rng.choice(POPULAR_SEARCH_TERMS)
        device = rng.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0]

        rand_sec = rng.randint(0, total_seconds)
        event_ts = start_date + timedelta(seconds=rand_sec)

        search_record = {
            "search_id": f"SRCH_{j:08d}",
            "customer_id": customer_id,
            "query": query,
            "results_count": rng.randint(5, 120),
            "device": device,
            "event_ts": event_ts.strftime("%Y-%m-%d %H:%M:%S")
        }
        search_events.append(search_record)

    return product_views, search_events
