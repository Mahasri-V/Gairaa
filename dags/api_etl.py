from datetime import datetime
import csv
import json
import urllib.request

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


API_URL = "https://dummyjson.com/products"
POSTGRES_CONN_ID = "postgres_project2"


def extract_api():
    print("Starting API extraction...")

    request = urllib.request.Request(
        API_URL,
        headers={"User-Agent": "Mozilla/5.0"}
    )

    with urllib.request.urlopen(request, timeout=30) as response:
        data = json.loads(response.read().decode("utf-8"))

    output_file = "/opt/airflow/data/raw_products.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(data, file, indent=4)

    print("API extraction completed.")
    print(f"Products received: {len(data['products'])}")
    print(f"Raw data saved to: {output_file}")


def clean_transform():
    print("Starting data cleaning and transformation...")

    input_file = "/opt/airflow/data/raw_products.json"
    output_file = "/opt/airflow/data/transformed_products.csv"

    with open(input_file, "r", encoding="utf-8") as file:
        data = json.load(file)

    products = data["products"]

    cleaned_data = []

    for product in products:

        price = product.get("price", 0)

        # Temporary SCD Type 2 test
        if product.get("id") == 1:
            price = 15.99
            print(
                "SCD2 TEST: Product 1 price changed "
                "from 9.99 to 15.99"
            )

        cleaned_data.append({
            "product_id": product.get("id"),
            "product_name": product.get("title", "").strip(),
            "category": product.get("category", "").strip(),
            "price": price,
            "discount_percentage": product.get(
                "discountPercentage", 0
            ),
            "rating": product.get("rating", 0),
            "stock": product.get("stock", 0),
            "brand": product.get("brand", "").strip(),
            "sku": product.get("sku", "").strip()
        })

    fieldnames = [
        "product_id",
        "product_name",
        "category",
        "price",
        "discount_percentage",
        "rating",
        "stock",
        "brand",
        "sku"
    ]

    with open(
        output_file,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )

        writer.writeheader()
        writer.writerows(cleaned_data)

    print("Data cleaning completed.")
    print(f"Products cleaned: {len(cleaned_data)}")
    print(f"Clean CSV saved to: {output_file}")


def load_scd_type_2():
    print("Starting SCD Type 2 loading...")

    csv_file = "/opt/airflow/data/transformed_products.csv"

    hook = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    connection = hook.get_conn()
    cursor = connection.cursor()

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS product_scd2 (
        product_id INTEGER,
        product_name TEXT,
        category TEXT,
        price NUMERIC(10,2),
        discount_percentage NUMERIC(10,2),
        rating NUMERIC(10,2),
        stock INTEGER,
        brand TEXT,
        sku TEXT,
        valid_from TIMESTAMP,
        valid_to TIMESTAMP,
        is_current BOOLEAN
    );
    """

    cursor.execute(create_table_sql)
    connection.commit()

    print("SCD Type 2 table checked/created.")

    with open(csv_file, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        products = list(reader)

    print(f"Products received from CSV: {len(products)}")

    current_time = datetime.now()

    inserted_count = 0
    updated_count = 0
    unchanged_count = 0

    for product in products:

        product_id = int(product["product_id"])
        product_name = product["product_name"]
        category = product["category"]
        price = float(product["price"])
        discount_percentage = float(
            product["discount_percentage"]
        )
        rating = float(product["rating"])
        stock = int(product["stock"])
        brand = product["brand"]
        sku = product["sku"]

        select_sql = """
        SELECT
            product_name,
            category,
            price,
            discount_percentage,
            rating,
            stock,
            brand,
            sku
        FROM product_scd2
        WHERE product_id = %s
        AND is_current = TRUE
        """

        cursor.execute(
            select_sql,
            (product_id,)
        )

        existing = cursor.fetchone()

        if existing is None:

            insert_sql = """
            INSERT INTO product_scd2 (
                product_id,
                product_name,
                category,
                price,
                discount_percentage,
                rating,
                stock,
                brand,
                sku,
                valid_from,
                valid_to,
                is_current
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, NULL, TRUE
            )
            """

            cursor.execute(
                insert_sql,
                (
                    product_id,
                    product_name,
                    category,
                    price,
                    discount_percentage,
                    rating,
                    stock,
                    brand,
                    sku,
                    current_time
                )
            )

            inserted_count += 1

        else:

            (
                old_product_name,
                old_category,
                old_price,
                old_discount_percentage,
                old_rating,
                old_stock,
                old_brand,
                old_sku
            ) = existing

            changed = (
                old_product_name != product_name
                or old_category != category
                or float(old_price) != price
                or float(old_discount_percentage)
                != discount_percentage
                or float(old_rating) != rating
                or old_stock != stock
                or old_brand != brand
                or old_sku != sku
            )

            if changed:

                print(
                    f"Change detected for product {product_id}"
                )

                update_sql = """
                UPDATE product_scd2
                SET
                    valid_to = %s,
                    is_current = FALSE
                WHERE product_id = %s
                AND is_current = TRUE
                """

                cursor.execute(
                    update_sql,
                    (
                        current_time,
                        product_id
                    )
                )

                insert_sql = """
                INSERT INTO product_scd2 (
                    product_id,
                    product_name,
                    category,
                    price,
                    discount_percentage,
                    rating,
                    stock,
                    brand,
                    sku,
                    valid_from,
                    valid_to,
                    is_current
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, NULL, TRUE
                )
                """

                cursor.execute(
                    insert_sql,
                    (
                        product_id,
                        product_name,
                        category,
                        price,
                        discount_percentage,
                        rating,
                        stock,
                        brand,
                        sku,
                        current_time
                    )
                )

                updated_count += 1

            else:
                unchanged_count += 1

    connection.commit()

    cursor.close()
    connection.close()

    print("SCD Type 2 loading completed.")
    print(f"New products inserted: {inserted_count}")
    print(f"Changed products versioned: {updated_count}")
    print(f"Unchanged products: {unchanged_count}")


with DAG(
    dag_id="api_product_etl",
    start_date=datetime(2026, 10, 7),
    schedule="@daily",
    catchup=False,
    tags=[
        "API",
        "ETL",
        "Products",
        "SCD2"
    ]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_api",
        python_callable=extract_api
    )

    clean_task = PythonOperator(
        task_id="clean_transform",
        python_callable=clean_transform
    )

    scd_task = PythonOperator(
        task_id="load_scd_type_2",
        python_callable=load_scd_type_2
    )

    extract_task >> clean_task >> scd_task