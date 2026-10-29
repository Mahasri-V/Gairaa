
import random
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd

# Reproducible sample data
random.seed(42)

RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

# Helper function to generate random dates
def random_date(start_date, days=700):
    return (
        start_date
        + timedelta(
            days=random.randint(0, days),
            hours=random.randint(0, 23)
        )
    )


start_date = datetime(2024, 1, 1)

# ============================================================
# 1. USERS DATASET
# ============================================================

users = pd.DataFrame([
    {
        "user_id": f"U{i:04d}",
        "name": f"User {i}",
        "email": f"user{i}@example.com",
        "city": random.choice([
            "Chennai", "Coimbatore", "Tiruppur",
            "Madurai", "Salem", "Trichy"
        ]),
        "signup_date": random_date(start_date).date()
    }
    for i in range(1, 301)
])

# Add missing value
users.loc[0, "email"] = None

# ============================================================
# 2. MOVIES DATASET
# ============================================================

genres = [
    "Action", "Comedy", "Drama",
    "Thriller", "Romance", "Sci-Fi"
]

movies = pd.DataFrame([
    {
        "movie_id": f"M{i:04d}",
        "title": f"Movie {i}",
        "genre": random.choice(genres),
        "release_year": random.randint(2010, 2026),
        "language": random.choice([
            "Tamil", "English", "Hindi", "Telugu"
        ]),
        "duration_minutes": random.randint(80, 180)
    }
    for i in range(1, 201)
])

# Add invalid duration
movies.loc[1, "duration_minutes"] = -10

# ============================================================
# 3. SUBSCRIPTIONS DATASET
# ============================================================

subscriptions = pd.DataFrame([
    {
        "subscription_id": f"S{i:04d}",
        "user_id": f"U{random.randint(1, 300):04d}",
        "plan_name": random.choice([
            "Basic", "Standard", "Premium"
        ]),
        "start_date": random_date(start_date).date(),
        "end_date": random_date(
            datetime(2025, 1, 1), days=365
        ).date(),
        "monthly_price": random.choice([99, 149, 199, 299, 399])
    }
    for i in range(1, 301)
])

# Add invalid price
subscriptions.loc[2, "monthly_price"] = -99

# ============================================================
# 4. PAYMENTS DATASET
# ============================================================

payments = pd.DataFrame([
    {
        "payment_id": f"P{i:04d}",
        "subscription_id": f"S{random.randint(1, 300):04d}",
        "payment_date": random_date(start_date).date(),
        "amount": random.choice([99, 149, 199, 299, 399]),
        "payment_method": random.choice([
            "UPI", "Card", "Net Banking", "Wallet"
        ]),
        "payment_status": random.choice([
            "Success", "Success", "Success", "Failed"
        ])
    }
    for i in range(1, 401)
])

# Add invalid payment amount
payments.loc[3, "amount"] = -50

# ============================================================
# 5. VIEWING EVENTS DATASET
# ============================================================

viewing_events = pd.DataFrame([
    {
        "viewing_id": f"V{i:04d}",
        "user_id": f"U{random.randint(1, 300):04d}",
        "movie_id": f"M{random.randint(1, 200):04d}",
        "watch_date": random_date(start_date),
        "watch_duration_minutes": random.randint(1, 200),
        "device": random.choice([
            "Mobile", "Smart TV", "Laptop", "Tablet"
        ])
    }
    for i in range(1, 501)
])

# Add invalid viewing duration
viewing_events.loc[4, "watch_duration_minutes"] = -5

# ============================================================
# 6. RATINGS DATASET
# ============================================================

ratings = pd.DataFrame([
    {
        "rating_id": f"R{i:04d}",
        "user_id": f"U{random.randint(1, 300):04d}",
        "movie_id": f"M{random.randint(1, 200):04d}",
        "rating": random.choice([1, 2, 3, 4, 5]),
        "review_date": random_date(start_date).date()
    }
    for i in range(1, 301)
])

# Add invalid rating
ratings.loc[5, "rating"] = 8

# ============================================================
# 7. SAVE RAW DATASETS
# ============================================================

datasets = {
    "users": users,
    "movies": movies,
    "subscriptions": subscriptions,
    "payments": payments,
    "viewing_events": viewing_events,
    "ratings": ratings
}

for name, df in datasets.items():

    # Add one duplicate record for cleaning practice
    df = pd.concat(
        [df, df.iloc[[0]]],
        ignore_index=True
    )

    output_file = RAW_DIR / f"{name}.csv"

    df.to_csv(output_file, index=False)

    print(f"{name}.csv created: {len(df)} rows")

print("\nAll six raw datasets generated successfully!")
