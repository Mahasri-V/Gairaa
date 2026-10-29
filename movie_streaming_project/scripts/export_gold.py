
import os
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv


# Project paths
PROJECT_DIR = Path(__file__).resolve().parent.parent
GOLD_DIR = PROJECT_DIR / "data" / "gold"

# Load database configuration
load_dotenv(PROJECT_DIR / ".env")

DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": int(os.getenv("DB_PORT", "5432")),
}

GOLD_TABLES = [
    "movie_analytics",
    "subscription_analytics",
    "user_engagement",
    "daily_viewing_trends",
]


def main():
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    print("EXPORTING GOLD TABLES TO CSV")

    with psycopg.connect(**DB_CONFIG) as connection:
        for table in GOLD_TABLES:
            query = f"SELECT * FROM gold.{table} ORDER BY 1"

            df = pd.read_sql_query(query, connection)

            output_file = GOLD_DIR / f"{table}.csv"
            df.to_csv(output_file, index=False)

            print(f"{table}: {len(df)} rows exported")
            print(f"Saved to: {output_file}")

    print("\nGold CSV export completed successfully!")


if __name__ == "__main__":
    main()
