import csv
import os

CSV_FILE = "target.csv"


def create_table():

    if not os.path.exists(CSV_FILE):

        with open(CSV_FILE, "w", newline="", encoding="utf-8") as file:

            writer = csv.writer(file)

            writer.writerow([
                "product_id",
                "product_name",
                "price"
            ])


def get_all_products():

    products = []

    if not os.path.exists(CSV_FILE):
        return products

    with open(CSV_FILE, "r", newline="", encoding="utf-8") as file:

        reader = csv.DictReader(file)

        for row in reader:

            products.append({
                "product_id": int(row["product_id"]),
                "product_name": row["product_name"],
                "price": float(row["price"])
            })

    products.sort(key=lambda x: x["product_id"])

    return products