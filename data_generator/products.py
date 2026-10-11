"""
Product Catalog Dimension & Inventory Baseline Generator
Creates products linked to sellers, categorized with realistic pricing and base stock levels.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import random

PRODUCT_TAXONOMY = {
    "Electronics": {
        "Smartphones": (10000, 90000),
        "Laptops": (35000, 150000),
        "Wireless Earbuds": (999, 14999),
        "Smartwatches": (1999, 29999),
        "Cables & Chargers": (199, 2499)
    },
    "Apparel & Fashion": {
        "Men's T-Shirts": (399, 2499),
        "Women's Kurtas": (699, 4999),
        "Denim Jeans": (999, 5999),
        "Running Shoes": (1299, 11999),
        "Watches & Accessories": (799, 8999)
    },
    "Home & Kitchen": {
        "Cookware Sets": (1299, 9999),
        "Water Purifiers": (4999, 24999),
        "Bed Linens": (599, 3999),
        "Storage Containers": (299, 1999),
        "Coffee Makers": (1899, 14999)
    },
    "Beauty & Health": {
        "Skin Care Serums": (399, 2999),
        "Organic Shampoos": (299, 1499),
        "Fitness Supplements": (999, 5999),
        "Fragrances": (799, 6999)
    },
    "Industrial & Tools": {
        "Power Drills": (1999, 14999),
        "Hand Tool Kits": (499, 3999),
        "Safety Equipment": (299, 2999),
        "Measurement Lasers": (1299, 8999)
    },
    "Sports & Outdoors": {
        "Yoga Mats": (399, 1999),
        "Cricket Bats": (799, 14999),
        "Badminton Rackets": (499, 6999),
        "Trekking Backpacks": (1299, 7999)
    },
    "Books & Media": {
        "Business & Investing": (299, 999),
        "Technology & Coding": (499, 2499),
        "Fiction & Novels": (199, 799),
        "Self-Help": (250, 899)
    },
    "Automotive Parts": {
        "Car Dashcams": (2499, 12999),
        "Tire Inflators": (1299, 4999),
        "Helmets": (899, 5999),
        "Car Cleaning Kits": (399, 1999)
    }
}


def generate_products(
    count: int,
    sellers: List[Dict[str, Any]],
    seller_weights: List[float],
    start_date: datetime,
    end_date: datetime,
    initial_stock_min: int,
    initial_stock_max: int,
    rng: random.Random
) -> List[Dict[str, Any]]:
    """
    Generate products mapped to sellers with realistic pricing and starting inventory.
    """
    products = []
    total_seconds = int((end_date - start_date).total_seconds())

    seller_ids = [s["seller_id"] for s in sellers]
    seller_cat_map = {s["seller_id"]: s["category"] for s in sellers}

    for i in range(1, count + 1):
        # Sample a seller using the skew weights (top sellers list more products too!)
        seller_id = rng.choices(seller_ids, weights=seller_weights, k=1)[0]
        category = seller_cat_map.get(seller_id, "Electronics")

        if category not in PRODUCT_TAXONOMY:
            category = "Electronics"

        sub_cat = rng.choice(list(PRODUCT_TAXONOMY[category].keys()))
        min_p, max_p = PRODUCT_TAXONOMY[category][sub_cat]
        price = round(rng.uniform(min_p, max_p), 2)
        # Wholesale cost is ~65% - 85% of retail price
        cost = round(price * rng.uniform(0.65, 0.85), 2)

        stock = rng.randint(initial_stock_min, initial_stock_max)

        created_offset = rng.randint(0, total_seconds // 2)
        created_ts = start_date + timedelta(seconds=created_offset)

        product_id = f"PROD_{i:07d}"
        product_name = f"{sub_cat} Pro #{i}"

        products.append({
            "product_id": product_id,
            "seller_id": seller_id,
            "product_name": product_name,
            "category": category,
            "sub_category": sub_cat,
            "price": price,
            "cost": cost,
            "stock_quantity": stock,
            "created_ts": created_ts.strftime("%Y-%m-%d %H:%M:%S")
        })

    return products
