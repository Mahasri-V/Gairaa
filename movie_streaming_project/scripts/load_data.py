
import os
from pathlib import Path

import pandas as pd
import psycopg
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path("data/processed")

TABLES = [
    ("users", "users.csv"),
    ("movies", "movies.csv"),
    ("subscriptions", "subscriptions.csv"),
    ("payments", "payments.csv"),
    ("viewing_events", "viewing_events.csv"),
    ("ratings", "ratings.csv"),
]

def main():
    connection = psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    try:
        with connection:
            with connection.cursor() as cursor:
                for table_name, filename in TABLES:
                    file_path = DATA_DIR / filename

                    if not file_path.exists():
                        raise FileNotFoundError(
                            f"CSV file not found: {file_path}"
                        )

                    df = pd.read_csv(file_path)

                   
                    cursor.execute(
                        f"SELECT COUNT(*) FROM {table_name}"
                    )
                    existing_count = cursor.fetchone()[0]

                    if existing_count > 0:
                        raise RuntimeError(
                            f"Table {table_name} is not empty "
                            f"({existing_count} rows). Load cancelled."
                        )

                    columns = list(df.columns)
                    column_sql = ", ".join(
                        '"' + column.replace('"', '""') + '"'
                        for column in columns
                    )
                    placeholders = ", ".join(
                        ["%s"] * len(columns)
                    )

                    insert_sql = (
                        f'INSERT INTO "{table_name}" '
                        f"({column_sql}) VALUES ({placeholders})"
                    )
                    records = []
                    for row in df.itertuples(index=False, name=None):
                        records.append(tuple(
                            None if pd.isna(value) else
                            value.to_pydatetime()
                            if hasattr(value, "to_pydatetime") else value
                            for value in row
                        ))

                    cursor.executemany(insert_sql, records)

                    print(
                        f"Loaded {len(records)} rows into {table_name}"
                    )

        print("\nAll datasets loaded successfully!")

    finally:
        connection.close()


if __name__ == "__main__":
    main()
