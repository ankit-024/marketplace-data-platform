"""
Customer Dimension Data Generator
Simulates marketplace customer demographics, geographic locations, and registration dates.
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
import random


CITIES_AND_STATES = [
    ("Mumbai", "Maharashtra"),
    ("Pune", "Maharashtra"),
    ("Nagpur", "Maharashtra"),
    ("New Delhi", "Delhi"),
    ("Bangalore", "Karnataka"),
    ("Mysore", "Karnataka"),
    ("Hyderabad", "Telangana"),
    ("Ahmedabad", "Gujarat"),
    ("Surat", "Gujarat"),
    ("Vadodara", "Gujarat"),
    ("Chennai", "Tamil Nadu"),
    ("Coimbatore", "Tamil Nadu"),
    ("Kolkata", "West Bengal"),
    ("Jaipur", "Rajasthan"),
    ("Lucknow", "Uttar Pradesh"),
    ("Noida", "Uttar Pradesh"),
    ("Gurgaon", "Haryana"),
    ("Chandigarh", "Punjab"),
    ("Kochi", "Kerala"),
    ("Indore", "Madhya Pradesh")
]

FIRST_NAMES = [
    "Aarav", "Vivaan", "Aditya", "Vihaan", "Arjun", "Sai", "Reyansh", "Ayaan", "Krishna", "Ishaan",
    "Ananya", "Diya", "Saanvi", "Aadhya", "Pari", "Chiara", "Riya", "Anushka", "Myra", "Navya",
    "Rohan", "Pooja", "Vikram", "Sneha", "Rahul", "Priya", "Amit", "Kavita", "Suresh", "Neha",
    "Kunal", "Meera", "Deepak", "Shweta", "Rajesh", "Sunita", "Manish", "Divya", "Gaurav", "Simran"
]

LAST_NAMES = [
    "Sharma", "Verma", "Gupta", "Malhotra", "Bhatia", "Saxena", "Mehta", "Patel", "Shah", "Joshi",
    "Kulkarni", "Deshmukh", "Nair", "Pillai", "Reddy", "Rao", "Iyer", "Sundaram", "Mukherjee", "Banerjee",
    "Singh", "Choudhury", "Yadav", "Mishra", "Pandey", "Trivedi", "Agarwal", "Bansal", "Kapoor", "Chopra"
]

CUSTOMER_SEGMENTS = ["RETAIL_BRONZE", "RETAIL_SILVER", "RETAIL_GOLD", "B2B_WHOLESALE"]


def generate_customers(
    count: int,
    start_date: datetime,
    end_date: datetime,
    rng: random.Random
) -> List[Dict[str, Any]]:
    """
    Generate a deterministic list of customer records.
    """
    customers = []
    total_seconds = int((end_date - start_date).total_seconds())

    for i in range(1, count + 1):
        first_name = rng.choice(FIRST_NAMES)
        last_name = rng.choice(LAST_NAMES)
        city, state = rng.choice(CITIES_AND_STATES)
        segment = rng.choices(
            CUSTOMER_SEGMENTS,
            weights=[0.60, 0.25, 0.12, 0.03],
            k=1
        )[0]

        # Random signup date within the window
        random_offset = rng.randint(0, total_seconds)
        signup_ts = start_date + timedelta(seconds=random_offset)

        cust_id = f"CUST_{i:07d}"
        email = f"{first_name.lower()}.{last_name.lower()}{i % 997}@example.com"
        phone = f"+91{rng.randint(6000000000, 9999999999)}"

        customers.append({
            "customer_id": cust_id,
            "first_name": first_name,
            "last_name": last_name,
            "full_name": f"{first_name} {last_name}",
            "email": email,
            "phone": phone,
            "city": city,
            "state": state,
            "segment": segment,
            "signup_ts": signup_ts.strftime("%Y-%m-%d %H:%M:%S")
        })

    return customers
