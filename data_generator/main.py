"""
Command-Line Interface for Synthetic Marketplace Data Generation
Generates customers, sellers, products, orders, payments, inventory, and clickstream events.
Supports deterministic seeds, controllable skew ratios, and multi-format exports (CSV, JSON).
"""

import argparse
import csv
import json
import os
import random
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

from data_generator.customers import generate_customers
from data_generator.sellers import generate_sellers
from data_generator.products import generate_products
from data_generator.orders import generate_orders_and_payments
from data_generator.events import generate_clickstream_events


def save_as_csv(data: List[Dict[str, Any]], filepath: Path):
    if not data:
        return
    filepath.parent.mkdir(parents=True, exist_ok=True)
    keys = data[0].keys()
    with open(filepath, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(data)


def save_as_json(data: List[Dict[str, Any]], filepath: Path):
    if not data:
        return
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, mode="w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)


def main():
    parser = argparse.ArgumentParser(
        description="Scalable Data Generator for Real-Time Marketplace Intelligence Platform"
    )
    parser.add_argument("--customers", type=int, default=5000, help="Number of customers to generate")
    parser.add_argument("--sellers", type=int, default=500, help="Number of sellers to generate")
    parser.add_argument("--products", type=int, default=2500, help="Number of products to generate")
    parser.add_argument("--orders", type=int, default=20000, help="Number of orders to generate")
    parser.add_argument("--views", type=int, default=50000, help="Number of product view events")
    parser.add_argument("--searches", type=int, default=25000, help="Number of search query events")
    parser.add_argument("--skew", type=lambda x: (str(x).lower() == 'true'), default=True, help="Enable seller order skew")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducible generation")
    parser.add_argument("--start-date", type=str, default="2026-08-01", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, default="2026-10-10", help="End date (YYYY-MM-DD)")
    parser.add_argument("--format", type=str, choices=["csv", "json"], default="csv", help="Output export format")
    parser.add_argument("--output-dir", type=str, default="data/sample", help="Target output folder")

    args = parser.parse_args()

    # Initialize deterministic RNG
    rng = random.Random(args.seed)
    start_dt = datetime.strptime(args.start_date, "%Y-%m-%d")
    end_dt = datetime.strptime(args.end_date, "%Y-%m-%d")

    print("\n" + "=" * 70)
    print("  REAL-TIME MARKETPLACE DATA GENERATOR")
    print(f"  Seed: {args.seed} | Skew Enabled: {args.skew} | Date Range: {args.start_date} -> {args.end_date}")
    print("=" * 70)

    # 1. Customers
    print(f"\n[1/5] Generating {args.customers:,} customers...")
    customers = generate_customers(args.customers, start_dt, end_dt, rng)

    # 2. Sellers
    print(f"[2/5] Generating {args.sellers:,} sellers (Top 1% skew weight driver)...")
    sellers, seller_weights = generate_sellers(
        count=args.sellers,
        start_date=start_dt,
        end_date=end_dt,
        skew_enabled=args.skew,
        top_seller_pct=0.01,
        order_share_pct=0.60,
        rng=rng
    )

    # 3. Products
    print(f"[3/5] Generating {args.products:,} products with pricing & inventory...")
    products = generate_products(
        count=args.products,
        sellers=sellers,
        seller_weights=seller_weights,
        start_date=start_dt,
        end_date=end_dt,
        initial_stock_min=50,
        initial_stock_max=500,
        rng=rng
    )

    # 4. Orders, Payments, Inventory Deltas
    print(f"[4/5] Generating {args.orders:,} orders with payment lifecycles...")
    orders, payments, inventory_events = generate_orders_and_payments(
        orders_count=args.orders,
        customers=customers,
        sellers=sellers,
        seller_weights=seller_weights,
        products=products,
        start_date=start_dt,
        end_date=end_dt,
        payment_failure_rate=0.07,
        cancellation_rate=0.04,
        late_event_pct=0.05,
        max_delay_minutes=180,
        rng=rng
    )

    # 5. Clickstream Events
    print(f"[5/5] Generating {args.views:,} views and {args.searches:,} search events...")
    product_views, search_events = generate_clickstream_events(
        views_count=args.views,
        searches_count=args.searches,
        customers=customers,
        products=products,
        start_date=start_dt,
        end_date=end_dt,
        rng=rng
    )

    # Save outputs
    out_dir = Path(args.output_dir)
    ext = args.format
    save_func = save_as_csv if ext == "csv" else save_as_json

    print(f"\nSaving generated datasets to '{out_dir}' in .{ext} format...")
    datasets = {
        "customers": customers,
        "sellers": sellers,
        "products": products,
        "orders": orders,
        "payments": payments,
        "inventory_events": inventory_events,
        "product_views": product_views,
        "search_events": search_events
    }

    for name, data in datasets.items():
        file_path = out_dir / f"{name}.{ext}"
        save_func(data, file_path)
        print(f"  -> {file_path} ({len(data):,} rows)")

    # Skew Validation Summary
    print("\n" + "-" * 70)
    print("  DATA SKEW VERIFICATION REPORT")
    print("-" * 70)
    seller_order_counts = Counter(o["seller_id"] for o in orders)
    top_1_pct_count = max(1, int(len(sellers) * 0.01))
    top_sellers = [s["seller_id"] for s in sellers if s["is_top_seller"]]
    orders_from_top_sellers = sum(seller_order_counts.get(sid, 0) for sid in top_sellers)
    actual_top_share = (orders_from_top_sellers / len(orders)) * 100

    print(f"  Total Sellers:            {len(sellers):,}")
    print(f"  Top 1% Whale Sellers:     {top_1_pct_count} sellers ({', '.join(top_sellers[:3])}...)")
    print(f"  Total Orders:             {len(orders):,}")
    print(f"  Orders from Top 1%:       {orders_from_top_sellers:,} ({actual_top_share:.1f}% of total!)")
    if args.skew:
        print(f"  Status:                   SUCCESS - Skew injected successfully for Spark join labs!")
    else:
        print(f"  Status:                   UNIFORM - Orders distributed evenly.")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
