from datetime import datetime
import csv
import psycopg2

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator   

CSV_FILE = "/opt/airflow/data/sales.csv"
TRANSFORMED_FILE = "/opt/airflow/data/transformed_sales.csv"

def clean_price(value):
    """
    Convert values such as:
    ₹399
    ₹1,099
    399
    into float values.
    """

    if not value or value.strip() in ("|", "nan", "NaN"):
        return None

    value = value.replace("₹", "")
    value = value.replace(",", "")
    value = value.strip()

    try:
        return float(value)
    except ValueError:
        return None


def clean_percentage(value):
    """
    Convert values such as:
    64%
    43%
    into float values.
    """

    if not value or value.strip() in ("|", "nan", "NaN"):
        return None

    value = value.replace("%", "")
    value = value.strip()

    try:
        return float(value)
    except ValueError:
        return None


def clean_rating(value):
    """
    Convert rating values such as:
    4.2
    4.0
    into float values.

    Invalid values such as | are converted to None.
    """

    if not value or value.strip() in ("|", "nan", "NaN"):
        return None

    value = value.strip()

    try:
        return float(value)
    except ValueError:
        return None


def clean_rating_count(value):
    """
    Convert values such as:
    24,269
    43,994
    into integers.
    """

    if not value or value.strip() in ("|", "nan", "NaN"):
        return None

    value = value.replace(",", "")
    value = value.strip()

    try:
        return int(value)
    except ValueError:
        return None


def extract_and_transform():

    transformed_data = []

    with open(
        CSV_FILE,
        "r",
        encoding="utf-8"
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:

            transformed_data.append({

                "product_id": row["product_id"].strip(),

                "product_name": row["product_name"].strip(),

                "category": row["category"].strip(),

                "discounted_price": clean_price(
                    row["discounted_price"]
                ),

                "actual_price": clean_price(
                    row["actual_price"]
                ),

                "discount_percentage": clean_percentage(
                    row["discount_percentage"]
                ),

                "rating": clean_rating(
                    row["rating"]
                ),

                "rating_count": clean_rating_count(
                    row["rating_count"]
                ),

                "user_name": row["user_name"].strip(),

                "review_id": row["review_id"].strip(),

                "review_title": row["review_title"].strip()
            })


    # -----------------------------------------------------
    # Write transformed CSV
    # -----------------------------------------------------

    with open(
        TRANSFORMED_FILE,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        fieldnames = [

            "product_id",
            "product_name",
            "category",
            "discounted_price",
            "actual_price",
            "discount_percentage",
            "rating",
            "rating_count",
            "user_name",
            "review_id",
            "review_title"

        ]

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()

        writer.writerows(transformed_data)


    print(
        "Extract and Transform completed successfully."
    )

    print(
        f"Rows processed: {len(transformed_data)}"
    )


# ---------------------------------------------------------
# Load into PostgreSQL
# ---------------------------------------------------------

def load_to_postgres():

    connection = psycopg2.connect(

        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow"

    )

    cursor = connection.cursor()


    # -----------------------------------------------------
    # Create table
    # -----------------------------------------------------

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS amazon_products (

            product_id VARCHAR(50) PRIMARY KEY,

            product_name TEXT,

            category TEXT,

            discounted_price NUMERIC(12,2),

            actual_price NUMERIC(12,2),

            discount_percentage NUMERIC(5,2),

            rating NUMERIC(3,2),

            rating_count INTEGER,

            user_name TEXT,

            review_id TEXT,

            review_title TEXT

        )
    """)


    # -----------------------------------------------------
    # Read transformed CSV
    # -----------------------------------------------------

    with open(
        TRANSFORMED_FILE,
        "r",
        encoding="utf-8"
    ) as file:

        reader = csv.DictReader(file)

        for row in reader:

            cursor.execute("""

                INSERT INTO amazon_products (

                    product_id,
                    product_name,
                    category,
                    discounted_price,
                    actual_price,
                    discount_percentage,
                    rating,
                    rating_count,
                    user_name,
                    review_id,
                    review_title

                )

                VALUES (

                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s,
                    %s

                )

                ON CONFLICT (product_id)

                DO UPDATE SET

                    product_name =
                        EXCLUDED.product_name,

                    category =
                        EXCLUDED.category,

                    discounted_price =
                        EXCLUDED.discounted_price,

                    actual_price =
                        EXCLUDED.actual_price,

                    discount_percentage =
                        EXCLUDED.discount_percentage,

                    rating =
                        EXCLUDED.rating,

                    rating_count =
                        EXCLUDED.rating_count,

                    user_name =
                        EXCLUDED.user_name,

                    review_id =
                        EXCLUDED.review_id,

                    review_title =
                        EXCLUDED.review_title

            """, (

                row["product_id"],

                row["product_name"],

                row["category"],

                row["discounted_price"] or None,

                row["actual_price"] or None,

                row["discount_percentage"] or None,

                row["rating"] or None,

                row["rating_count"] or None,

                row["user_name"],

                row["review_id"],

                row["review_title"]

            ))


    # -----------------------------------------------------
    # Commit changes
    # -----------------------------------------------------

    connection.commit()

    cursor.close()

    connection.close()


    print(
        "Load to PostgreSQL completed successfully."
    )


# ---------------------------------------------------------
# Airflow DAG
# ---------------------------------------------------------

with DAG(

    dag_id="sales_csv_postgres_etl",

    start_date=datetime(2026, 10, 6),

    schedule="@daily",

    catchup=False,

    tags=[
        "ETL",
        "Amazon",
        "CSV",
        "PostgreSQL"
    ]

 ) as dag:


    transform_task = PythonOperator(

        task_id="extract_and_transform",

        python_callable=extract_and_transform

    )


    load_task = PythonOperator(

        task_id="load_to_postgres",

        python_callable=load_to_postgres

    )


    # Transform first, then load
    transform_task >> load_task
