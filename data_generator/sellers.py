"""
Seller Dimension Data Generator & Skew Distribution Driver
Generates marketplace sellers and computes order probability weights to simulate
severe real-world data skew (Top 1% sellers capture ~60% of volume).
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple
import random
from data_generator.customers import CITIES_AND_STATES

SELLER_CATEGORIES = [
    "Electronics",
    "Apparel & Fashion",
    "Home & Kitchen",
    "Beauty & Health",
    "Industrial & Tools",
    "Sports & Outdoors",
    "Books & Media",
    "Automotive Parts"
]

SELLER_PREFIXES = [
    "Apex", "Prime", "Royal", "Global", "NextGen", "Metro", "United", "Zenith", "Vanguard",
    "Kalyan", "Shree", "Mahalaxmi", "Bharat", "Surya", "Ganesh", "Balaji", "Om", "Sai"
]

SELLER_SUFFIXES = [
    "Enterprises", "Retailers", "Solutions", "Suppliers", "Trading Co", "Wholesalers",
    "Direct", "Hub", "World", "Mart", "Industries", "Logistics"
]


def generate_sellers(
    count: int,
    start_date: datetime,
    end_date: datetime,
    skew_enabled: bool,
    top_seller_pct: float,
    order_share_pct: float,
    rng: random.Random
) -> Tuple[List[Dict[str, Any]], List[float]]:
    """
    Generate sellers and their corresponding selection weights.
    
    Returns:
        (sellers_list, selection_weights)
    """
    sellers = []
    weights = []
    total_seconds = int((end_date - start_date).total_seconds())

    # Calculate number of top "whale" sellers
    top_count = max(1, int(count * top_seller_pct))
    remaining_count = max(1, count - top_count)

    for i in range(1, count + 1):
        city, state = rng.choice(CITIES_AND_STATES)
        category = rng.choice(SELLER_CATEGORIES)
        company_name = f"{rng.choice(SELLER_PREFIXES)} {rng.choice(SELLER_SUFFIXES)} {i}"
        
        # Registration timestamp
        reg_offset = rng.randint(0, total_seconds // 2) # registered in first half
        reg_ts = start_date + timedelta(seconds=reg_offset)

        seller_id = f"SELL_{i:06d}"
        rating = round(rng.uniform(3.2, 5.0), 2)

        is_top_seller = (i <= top_count)

        # Compute sampling weight for skew
        if skew_enabled:
            if is_top_seller:
                # Top 1% share order_share_pct (e.g. 60% of total probability mass)
                weight = order_share_pct / top_count
            else:
                # Remaining sellers share the remaining probability mass (e.g. 40%)
                weight = (1.0 - order_share_pct) / remaining_count
        else:
            # Uniform distribution
            weight = 1.0 / count

        sellers.append({
            "seller_id": seller_id,
            "seller_name": company_name,
            "category": category,
            "city": city,
            "state": state,
            "rating": rating,
            "is_top_seller": is_top_seller,
            "registration_ts": reg_ts.strftime("%Y-%m-%d %H:%M:%S")
        })
        weights.append(weight)

    return sellers, weights
