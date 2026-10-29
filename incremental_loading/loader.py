import csv
import os

CSV_FILE = "target.csv"


def read_target_data():

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

    return products


def write_target_data(products):

    with open(CSV_FILE, "w", newline="", encoding="utf-8") as file:

        writer = csv.DictWriter(
            file,
            fieldnames=[
                "product_id",
                "product_name",
                "price"
            ]
        )

        writer.writeheader()
        writer.writerows(products)


def full_load(source_data):

    # Replace the complete target data
    write_target_data(source_data)

    target_data = read_target_data()

    return {
        "load_type": "FULL LOAD",
        "source_records": len(source_data),
        "loaded_records": len(source_data),
        "target_records": len(target_data),
        "new_records": len(source_data),
        "updated_records": 0,
        "status": "SUCCESS"
    }


def incremental_load(source_data):

    target_data = read_target_data()

    target_dict = {
        product["product_id"]: product
        for product in target_data
    }

    new_records = 0
    updated_records = 0

    for product in source_data:

        product_id = product["product_id"]

        if product_id not in target_dict:

            target_dict[product_id] = product
            new_records += 1

        else:

            existing = target_dict[product_id]

            if (
                existing["product_name"] != product["product_name"]
                or existing["price"] != product["price"]
            ):

                target_dict[product_id] = product
                updated_records += 1

    final_data = list(target_dict.values())

    write_target_data(final_data)

    return {
        "load_type": "INCREMENTAL LOAD",
        "source_records": len(source_data),
        "loaded_records": new_records + updated_records,
        "target_records": len(final_data),
        "new_records": new_records,
        "updated_records": updated_records,
        "status": "SUCCESS"
    }