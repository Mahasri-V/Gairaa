import os
import psycopg
from dotenv import load_dotenv

load_dotenv()

try:
    with psycopg.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname="postgres",
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        connect_timeout=5,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            print("PostgreSQL connection successful!")
            print(cur.fetchone()[0])

except Exception as e:
    print("PostgreSQL connection failed.")
    print(type(e).__name__ + ":", e)
