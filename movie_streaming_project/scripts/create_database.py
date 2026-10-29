import os
import psycopg
from dotenv import load_dotenv
from psycopg import sql

load_dotenv()

db_name = os.getenv("DB_NAME", "movie_streaming_db")

try:
    with psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname="postgres",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (db_name,)
            )

            if cur.fetchone():
                print(f"Database '{db_name}' already exists.")
            else:
                cur.execute(
                    sql.SQL("CREATE DATABASE {}").format(
                        sql.Identifier(db_name)
                    )
                )
                print(f"Database '{db_name}' created successfully!")

except Exception as e:
    print("Database creation failed.")
    print(type(e).__name__ + ":", e)