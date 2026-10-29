
from pathlib import Path
import pandas as pd

DATA_DIR = Path("data/processed")

REQUIRED_COLUMNS = {
    "users": ["user_id", "name", "signup_date"],
    "movies": ["movie_id", "title", "release_year"],
    "subscriptions": ["subscription_id", "user_id"],
    "payments": ["payment_id", "subscription_id"],
    "viewing_events": ["viewing_id", "user_id", "movie_id"],
    "ratings": ["rating_id", "user_id", "movie_id", "rating"],
}

def main():
    print("PROCESSED DATA VALIDATION")

    total_rows = 0

    for name, required_columns in REQUIRED_COLUMNS.items():
        file_path = DATA_DIR / f"{name}.csv"

        if not file_path.exists():
            raise FileNotFoundError(f"Missing file: {file_path}")

        df = pd.read_csv(file_path)

        missing_columns = [
            column for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"{name}: missing columns {missing_columns}"
            )

        if df.empty:
            raise ValueError(f"{name}: dataset is empty")

        if df[required_columns].isnull().any().any():
            raise ValueError(
                f"{name}: required columns contain missing values"
            )

        id_column = required_columns[0]

        if df[id_column].duplicated().any():
            raise ValueError(
                f"{name}: duplicate IDs found in {id_column}"
            )

        print(f"{name}: PASS ({len(df)} rows)")
        total_rows += len(df)

    print("-" * 60)
    print(f"All six datasets passed validation.")
    print(f"Total processed rows: {total_rows}")

if __name__ == "__main__":
    main()
