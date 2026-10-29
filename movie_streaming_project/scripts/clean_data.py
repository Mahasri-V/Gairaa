
from pathlib import Path
import pandas as pd

RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


def load_csv(filename):
    """Load a raw CSV dataset."""
    path = RAW_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    return pd.read_csv(path)


def clean_dates(df, columns):
    """Convert selected columns to datetime."""
    for column in columns:
        df[column] = pd.to_datetime(df[column], errors="coerce")
    return df


def save_csv(df, filename):
    """Save a cleaned dataset."""
    output_path = PROCESSED_DIR / filename
    df.to_csv(output_path, index=False)
    print(f"Saved {filename}: {len(df)} rows")


def remove_duplicates(df):
    """Remove exact duplicate records."""
    return df.drop_duplicates().copy()

print("\nCleaning users...")

users = load_csv("users.csv")
users = remove_duplicates(users)

users["signup_date"] = pd.to_datetime(
    users["signup_date"], errors="coerce"
)

users["user_id"] = users["user_id"].astype("string").str.strip()
users["email"] = users["email"].astype("string").str.strip()

users = users.dropna(subset=["user_id", "name", "signup_date"])
users = users[users["user_id"] != ""]

users["email"] = users["email"].replace("", pd.NA)
users["email"] = users["email"].fillna("unknown@example.com")

users = users.drop_duplicates(subset=["user_id"])

save_csv(users, "users.csv")


print("\nCleaning movies...")

movies = load_csv("movies.csv")
movies = remove_duplicates(movies)

movies["release_year"] = pd.to_numeric(
    movies["release_year"], errors="coerce"
)
movies["duration_minutes"] = pd.to_numeric(
    movies["duration_minutes"], errors="coerce"
)

movies = movies.dropna(
    subset=["movie_id", "title", "release_year", "duration_minutes"]
)

movies = movies[
    (movies["duration_minutes"] > 0)
    & (movies["release_year"].between(1888, 2026))
]

movies = movies.drop_duplicates(subset=["movie_id"])

save_csv(movies, "movies.csv")

print("\nCleaning subscriptions...")

subscriptions = load_csv("subscriptions.csv")
subscriptions = remove_duplicates(subscriptions)

subscriptions = clean_dates(
    subscriptions, ["start_date", "end_date"]
)

subscriptions["monthly_price"] = pd.to_numeric(
    subscriptions["monthly_price"], errors="coerce"
)

subscriptions = subscriptions.dropna(
    subset=[
        "subscription_id", "user_id", "start_date",
        "end_date", "monthly_price"
    ]
)

subscriptions = subscriptions[
    (subscriptions["monthly_price"] > 0)
    & (subscriptions["end_date"] >= subscriptions["start_date"])
]

subscriptions = subscriptions.drop_duplicates(
    subset=["subscription_id"]
)

subscriptions = subscriptions[
    subscriptions["user_id"].isin(users["user_id"])
]

save_csv(subscriptions, "subscriptions.csv")

print("\nCleaning payments...")

payments = load_csv("payments.csv")
payments = remove_duplicates(payments)

payments = clean_dates(payments, ["payment_date"])

payments["amount"] = pd.to_numeric(
    payments["amount"], errors="coerce"
)

payments = payments.dropna(
    subset=["payment_id", "subscription_id", "payment_date", "amount"]
)

payments = payments[payments["amount"] >= 0]

payments = payments.drop_duplicates(subset=["payment_id"])

payments = payments[
    payments["subscription_id"].isin(
        subscriptions["subscription_id"]
    )
]

save_csv(payments, "payments.csv")


print("\nCleaning viewing events...")

viewing_events = load_csv("viewing_events.csv")
viewing_events = remove_duplicates(viewing_events)

viewing_events = clean_dates(viewing_events, ["watch_date"])

viewing_events["watch_duration_minutes"] = pd.to_numeric(
    viewing_events["watch_duration_minutes"], errors="coerce"
)

viewing_events = viewing_events.dropna(
    subset=[
        "viewing_id", "user_id", "movie_id",
        "watch_date", "watch_duration_minutes"
    ]
)

viewing_events = viewing_events[
    viewing_events["watch_duration_minutes"].between(0, 1440)
]

viewing_events = viewing_events.drop_duplicates(
    subset=["viewing_id"]
)

viewing_events = viewing_events[
    viewing_events["user_id"].isin(users["user_id"])
    & viewing_events["movie_id"].isin(movies["movie_id"])
]

save_csv(viewing_events, "viewing_events.csv")


print("\nCleaning ratings...")

ratings = load_csv("ratings.csv")
ratings = remove_duplicates(ratings)

ratings = clean_dates(ratings, ["review_date"])

ratings["rating"] = pd.to_numeric(
    ratings["rating"], errors="coerce"
)

ratings = ratings.dropna(
    subset=[
        "rating_id", "user_id", "movie_id",
        "rating", "review_date"
    ]
)

ratings = ratings[ratings["rating"].between(1, 5)]

ratings = ratings.drop_duplicates(subset=["rating_id"])

ratings = ratings[
    ratings["user_id"].isin(users["user_id"])
    & ratings["movie_id"].isin(movies["movie_id"])
]

save_csv(ratings, "ratings.csv")


print("FINAL DATA VALIDATION")
cleaned_datasets = {
    "users": users,
    "movies": movies,
    "subscriptions": subscriptions,
    "payments": payments,
    "viewing_events": viewing_events,
    "ratings": ratings,
}

for name, df in cleaned_datasets.items():
    print(f"\n{name.upper()}")
    print("Rows:", len(df))
    print("Columns:", len(df.columns))
    print("Duplicate rows:", df.duplicated().sum())
    print("Missing values:", int(df.isna().sum().sum()))

print("\nData cleaning completed successfully!")
print(f"Cleaned files saved in: {PROCESSED_DIR}")
