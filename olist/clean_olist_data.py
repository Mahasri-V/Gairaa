import pandas as pd
import psycopg2
from psycopg2.extras import execute_values


# LOAD CSV FILES

customers = pd.read_csv(
    "data/olist_customers_dataset.csv"
)
orders = pd.read_csv(
    "data/olist_orders_dataset.csv"
)
order_items = pd.read_csv(
    "data/olist_order_items_dataset.csv"
)
order_payments = pd.read_csv(
    "data/olist_order_payments_dataset.csv"
)
order_reviews = pd.read_csv(
    "data/olist_order_reviews_dataset.csv"
)
products = pd.read_csv(
    "data/olist_products_dataset.csv"
)
sellers = pd.read_csv(
    "data/olist_sellers_dataset.csv"
)
geolocation = pd.read_csv(
    "data/olist_geolocation_dataset.csv"
)
category_translation = pd.read_csv(
    "data/product_category_name_translation.csv"
)
print("\nAll CSV files loaded successfully!")
#  CREATE TABLE DICTIONARY
tables = {
    "customers": customers,
    "orders": orders,
    "order_items": order_items,
    "order_payments": order_payments,
    "order_reviews": order_reviews,
    "products": products,
    "sellers": sellers,
    "geolocation": geolocation,
    "category_translation": category_translation
}


#  BASIC DATA INSPECTION

print("\nDATASET SIZE")

for name, df in tables.items():

    print("\n" + name.upper())
    print("Rows:", df.shape[0])
    print("Columns:", df.shape[1])
#  MISSING VALUES
print("\nMISSING VALUES")
for name, df in tables.items():
    print("\n" + name.upper())
    missing = df.isnull().sum()
    print(missing[missing > 0])

#  DUPLICATE ROW CHECK
print("\nDUPLICATE ROW CHECK")
for name, df in tables.items():
    print(
        name.upper(),
        "Duplicate rows:",
        df.duplicated().sum()
    )


#  PRIMARY KEY VALIDATION
print("\nPRIMARY KEY VALIDATION")
print(
    "Customers duplicate customer_id:",
    customers["customer_id"].duplicated().sum()
)
print(
    "Orders duplicate order_id:",
    orders["order_id"].duplicated().sum()
)
print(
    "Products duplicate product_id:",
    products["product_id"].duplicated().sum()
)
print(
    "Sellers duplicate seller_id:",
    sellers["seller_id"].duplicated().sum()
)
print(
    "Reviews duplicate review_id:",
    order_reviews["review_id"].duplicated().sum()
)
#  COMPOSITE KEY VALIDATION

print("\nCOMPOSITE KEY VALIDATION")

print(
    "Order Items duplicate primary key:",
    order_items.duplicated(
        subset=[
            "order_id",
            "order_item_id"
        ]
    ).sum()
)

print(
    "Order Payments duplicate primary key:",
    order_payments.duplicated(
        subset=[
            "order_id",
            "payment_sequential"
        ]
    ).sum()
)


# DUPLICATE REVIEW INVESTIGATION

print("\nDUPLICATE REVIEW ID INVESTIGATION")

duplicate_reviews = order_reviews[
    order_reviews["review_id"].duplicated(
        keep=False
    )
].sort_values("review_id")

print(
    "Duplicate review rows:",
    duplicate_reviews.shape[0]
)

print(
    "Exact duplicate rows:",
    duplicate_reviews.duplicated().sum()
)


print("\nSample duplicate reviews:")

print(
    duplicate_reviews[
        [
            "review_id",
            "order_id",
            "review_score"
        ]
    ].head(30)
)


# REMOVE DUPLICATE REVIEW IDs

print("\nREMOVING DUPLICATE REVIEW IDs")

order_reviews = order_reviews.drop_duplicates(
    subset=["review_id"],
    keep="first"
)
print(
    "Duplicate review_id after cleaning:",
    order_reviews["review_id"].duplicated().sum()
)
print(
    "Final review rows:",
    len(order_reviews)
)
#  CONVERT ORDER DATE COLUMNS

print("\nCONVERTING DATE COLUMNS")

order_date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for column in order_date_columns:

    orders[column] = pd.to_datetime(
        orders[column],
        errors="coerce"
    )

print("Order date columns converted successfully.")


print("\nDATE COLUMN DATA TYPES")

print(
    orders[order_date_columns].dtypes
)


#  CONVERT ORDER ITEM DATE

order_items["shipping_limit_date"] = pd.to_datetime(
    order_items["shipping_limit_date"],
    errors="coerce"
)
#  CONVERT REVIEW DATE COLUMNS

order_reviews["review_creation_date"] = pd.to_datetime(
    order_reviews["review_creation_date"],
    errors="coerce"
)
order_reviews["review_answer_timestamp"] = pd.to_datetime(
    order_reviews["review_answer_timestamp"],
    errors="coerce"
)
# STEP 3.10: CONVERT NUMERIC COLUMNS
print("\nCONVERTING NUMERIC COLUMNS")
numeric_columns = {
    "order_items": [
        "order_item_id",
        "price",
        "freight_value"
    ],

    "order_payments": [
        "payment_sequential",
        "payment_installments",
        "payment_value"
    ],

    "order_reviews": [
        "review_score"
    ],

    "products": [
        "product_name_lenght",
        "product_description_lenght",
        "product_photos_qty",
        "product_weight_g",
        "product_length_cm",
        "product_height_cm",
        "product_width_cm"
    ]
}


for table_name, columns in numeric_columns.items():
    df = tables[table_name]
    for column in columns:
        if column in df.columns:
            df[column] = pd.to_numeric(
                df[column],
                errors="coerce"
            )
print("Numeric columns converted successfully.")


# REVIEW SCORE VALIDATION
print("\nREVIEW SCORE VALIDATION")
invalid_reviews = order_reviews[
    ~order_reviews["review_score"].between(1, 5)
]
print(
    "Invalid review scores:",
    len(invalid_reviews)
)
#  PRICE AND FREIGHT VALIDATION
print("\nPRICE AND FREIGHT VALIDATION")
negative_prices = order_items[
    order_items["price"] < 0
]
negative_freight = order_items[
    order_items["freight_value"] < 0
]
print(
    "Negative price values:",
    len(negative_prices)
)
print(
    "Negative freight values:",
    len(negative_freight)
)

#  PAYMENT VALIDATION
print("\nPAYMENT VALIDATION")
negative_payments = order_payments[
    order_payments["payment_value"] < 0
]
invalid_installments = order_payments[
    order_payments["payment_installments"] <= 0
]
print(
    "Negative payment values:",
    len(negative_payments)
)
print(
    "Invalid payment installments:",
    len(invalid_installments)
)

#  FIX INVALID PAYMENT INSTALLMENTS

order_payments.loc[
    order_payments["payment_installments"] <= 0,
    "payment_installments"
] = 1

print(
    "Invalid payment installments after cleaning:",
    (
        order_payments["payment_installments"] <= 0
    ).sum()
)


#  FOREIGN KEY VALIDATION

print("\nFOREIGN KEY VALIDATION")
# Orders → Customers
invalid_customer_ids = orders[
    ~orders["customer_id"].isin(
        customers["customer_id"]
    )
]
print(
    "Orders with invalid customer_id:",
    len(invalid_customer_ids)
)
# Order Items → Orders
invalid_order_ids = order_items[
    ~order_items["order_id"].isin(
        orders["order_id"]
    )
]
print(
    "Order items with invalid order_id:",
    len(invalid_order_ids)
)
# Order Items → Products
invalid_product_ids = order_items[
    ~order_items["product_id"].isin(
        products["product_id"]
    )
]
print(
    "Order items with invalid product_id:",
    len(invalid_product_ids)
)

# Order Items → Sellers
invalid_seller_ids = order_items[
    ~order_items["seller_id"].isin(
        sellers["seller_id"]
    )
]
print(
    "Order items with invalid seller_id:",
    len(invalid_seller_ids)
)
# Payments → Orders
invalid_payment_orders = order_payments[
    ~order_payments["order_id"].isin(
        orders["order_id"]
    )
]
print(
    "Payments with invalid order_id:",
    len(invalid_payment_orders)
)
# Reviews → Orders
invalid_review_orders = order_reviews[
    ~order_reviews["order_id"].isin(
        orders["order_id"]
    )
]
print(
    "Reviews with invalid order_id:",
    len(invalid_review_orders)
)
#  PRODUCT CATEGORY VALIDATION
print("\nPRODUCT CATEGORY VALIDATION")
invalid_categories = products[
    ~products["product_category_name"].isin(
        category_translation["product_category_name"]
    )
]
print(
    "Product categories without translation:",
    len(invalid_categories)
)
#  UPDATE TABLE DICTIONARY

tables = {
    "customers": customers,
    "orders": orders,
    "order_items": order_items,
    "order_payments": order_payments,
    "order_reviews": order_reviews,
    "products": products,
    "sellers": sellers,
    "geolocation": geolocation,
    "category_translation": category_translation
}


#  FINAL CLEANED DATASET SIZE

print("\nFINAL CLEANED DATASET SIZE")
for name, df in tables.items():
    print(
        name.upper(),
        "Rows:",
        df.shape[0],
        "| Columns:",
        df.shape[1]
    )


#  CONNECT TO POSTGRESQL
print("\nPOSTGRESQL CONNECTION")
connection = psycopg2.connect(
    host="localhost",
    port="5432",
    database="olist_ecommerce",
    user="postgres",
    password="mahasri"
)
cursor = connection.cursor()
print("Connected to PostgreSQL successfully!")
# CREATE POSTGRESQL TABLES
print("\nCreating PostgreSQL tables...")
# CUSTOMERS
cursor.execute("""
CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INTEGER,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10)
);
""")
# PRODUCTS
cursor.execute("""
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_lenght DOUBLE PRECISION,
    product_description_lenght DOUBLE PRECISION,
    product_photos_qty DOUBLE PRECISION,
    product_weight_g DOUBLE PRECISION,
    product_length_cm DOUBLE PRECISION,
    product_height_cm DOUBLE PRECISION,
    product_width_cm DOUBLE PRECISION
);
""")
# SELLERS
cursor.execute("""
CREATE TABLE IF NOT EXISTS sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INTEGER,
    seller_city VARCHAR(100),
    seller_state VARCHAR(10)
);
""")
# CATEGORY TRANSLATION
cursor.execute("""
CREATE TABLE IF NOT EXISTS category_translation (
    product_category_name VARCHAR(100) PRIMARY KEY,
    product_category_name_english VARCHAR(100)
);
""")
# ORDERS
cursor.execute("""
CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    order_status VARCHAR(50),
    order_purchase_timestamp TIMESTAMP,
    order_approved_at TIMESTAMP,
    order_delivered_carrier_date TIMESTAMP,
    order_delivered_customer_date TIMESTAMP,
    order_estimated_delivery_date TIMESTAMP,
    CONSTRAINT fk_orders_customer
        FOREIGN KEY (customer_id)
        REFERENCES customers(customer_id)
);
""")
# ORDER ITEMS
cursor.execute("""
CREATE TABLE IF NOT EXISTS order_items (
    order_id VARCHAR(50),
    order_item_id INTEGER,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date TIMESTAMP,
    price DOUBLE PRECISION,
    freight_value DOUBLE PRECISION,

    PRIMARY KEY (order_id, order_item_id),

    CONSTRAINT fk_items_order
        FOREIGN KEY (order_id)
        REFERENCES orders(order_id),

    CONSTRAINT fk_items_product
        FOREIGN KEY (product_id)
        REFERENCES products(product_id),

    CONSTRAINT fk_items_seller
        FOREIGN KEY (seller_id)
        REFERENCES sellers(seller_id)
);
""")
# ORDER PAYMENTS
cursor.execute("""
CREATE TABLE IF NOT EXISTS order_payments (
    order_id VARCHAR(50),
    payment_sequential INTEGER,
    payment_type VARCHAR(50),
    payment_installments INTEGER,
    payment_value DOUBLE PRECISION,

    PRIMARY KEY (order_id, payment_sequential),

    CONSTRAINT fk_payments_order
        FOREIGN KEY (order_id)
        REFERENCES orders(order_id),

    CONSTRAINT chk_payment_installments
        CHECK (payment_installments > 0),

    CONSTRAINT chk_payment_value
        CHECK (payment_value >= 0)
);
""")
# ORDER REVIEWS
cursor.execute("""
CREATE TABLE IF NOT EXISTS order_reviews (
    review_id VARCHAR(50) PRIMARY KEY,
    order_id VARCHAR(50),
    review_score INTEGER,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date TIMESTAMP,
    review_answer_timestamp TIMESTAMP,

    CONSTRAINT fk_reviews_order
        FOREIGN KEY (order_id)
        REFERENCES orders(order_id),

    CONSTRAINT chk_review_score
        CHECK (review_score BETWEEN 1 AND 5)
);
""")
# GEOLOCATION
cursor.execute("""
CREATE TABLE IF NOT EXISTS geolocation (
    geolocation_id BIGSERIAL PRIMARY KEY,
    geolocation_zip_code_prefix INTEGER,
    geolocation_lat DOUBLE PRECISION,
    geolocation_lng DOUBLE PRECISION,
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(10)
);
""")
connection.commit()
print("All PostgreSQL tables created successfully!")
# Converts pandas NaN and NaT into PostgreSQL NULL
def prepare_data_for_postgresql(df, columns):
    temp_df = df[columns].copy()
    temp_df = temp_df.astype(object)
    temp_df = temp_df.where(
        pd.notna(temp_df),
        None
    )
    return list(
        temp_df.itertuples(
            index=False,
            name=None
        )
    )
#  LOAD CUSTOMERS
print("\nLoading CUSTOMERS...")
customer_columns = [
    "customer_id",
    "customer_unique_id",
    "customer_zip_code_prefix",
    "customer_city",
    "customer_state"
]
customer_data = prepare_data_for_postgresql(
    customers,
    customer_columns
)
execute_values(
    cursor,
    """
    INSERT INTO customers (
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    )
    VALUES %s
    """,
    customer_data
)

connection.commit()

print(
    "CUSTOMERS loaded:",
    len(customer_data)
)
#  LOAD PRODUCTS
print("\nLoading PRODUCTS...")
product_columns = [
    "product_id",
    "product_category_name",
    "product_name_lenght",
    "product_description_lenght",
    "product_photos_qty",
    "product_weight_g",
    "product_length_cm",
    "product_height_cm",
    "product_width_cm"
]

product_data = prepare_data_for_postgresql(
    products,
    product_columns
)
execute_values(
    cursor,
    """
    INSERT INTO products (
        product_id,
        product_category_name,
        product_name_lenght,
        product_description_lenght,
        product_photos_qty,
        product_weight_g,
        product_length_cm,
        product_height_cm,
        product_width_cm
    )
    VALUES %s
    """,
    product_data
)
connection.commit()
print(
    "PRODUCTS loaded:",
    len(product_data)
)
#  LOAD SELLERS
print("\nLoading SELLERS...")
seller_columns = [
    "seller_id",
    "seller_zip_code_prefix",
    "seller_city",
    "seller_state"
]
seller_data = prepare_data_for_postgresql(
    sellers,
    seller_columns
)
execute_values(
    cursor,
    """
    INSERT INTO sellers (
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state
    )
    VALUES %s
    """,
    seller_data
)
connection.commit()
print(
    "SELLERS loaded:",
    len(seller_data)
)
#  LOAD CATEGORY TRANSLATION
print("\nLoading CATEGORY TRANSLATION...")
category_columns = [
    "product_category_name",
    "product_category_name_english"
]
category_data = prepare_data_for_postgresql(
    category_translation,
    category_columns
)
execute_values(
    cursor,
    """
    INSERT INTO category_translation (
        product_category_name,
        product_category_name_english
    )
    VALUES %s
    """,
    category_data
)
connection.commit()
print(
    "CATEGORY TRANSLATION loaded:",
    len(category_data)
)
#  LOAD ORDERS
print("\nLoading ORDERS...")
order_columns = [
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]
order_data = prepare_data_for_postgresql(
    orders,
    order_columns
)
execute_values(
    cursor,
    """
    INSERT INTO orders (
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date
    )
    VALUES %s
    """,
    order_data
)

connection.commit()
print(
    "ORDERS loaded:",
    len(order_data)
)
#  LOAD ORDER ITEMS
print("\nLoading ORDER ITEMS...")
order_items_columns = [
    "order_id",
    "order_item_id",
    "product_id",
    "seller_id",
    "shipping_limit_date",
    "price",
    "freight_value"
]
order_items_data = prepare_data_for_postgresql(
    order_items,
    order_items_columns
)
execute_values(
    cursor,
    """
    INSERT INTO order_items (
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value
    )
    VALUES %s
    """,
    order_items_data
)
connection.commit()
print(
    "ORDER ITEMS loaded:",
    len(order_items_data)
)
#  LOAD ORDER PAYMENTS
print("\nLoading ORDER PAYMENTS...")
order_payments_columns = [
    "order_id",
    "payment_sequential",
    "payment_type",
    "payment_installments",
    "payment_value"
]
order_payments_data = prepare_data_for_postgresql(
    order_payments,
    order_payments_columns
)
execute_values(
    cursor,
    """
    INSERT INTO order_payments (
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
    )
    VALUES %s
    """,
    order_payments_data
)
connection.commit()
print(
    "ORDER PAYMENTS loaded:",
    len(order_payments_data)
)
# LOAD ORDER REVIEWS
print("\nLoading ORDER REVIEWS...")
order_reviews_columns = [
    "review_id",
    "order_id",
    "review_score",
    "review_comment_title",
    "review_comment_message",
    "review_creation_date",
    "review_answer_timestamp"
]
order_reviews_data = prepare_data_for_postgresql(
    order_reviews,
    order_reviews_columns
)
execute_values(
    cursor,
    """
    INSERT INTO order_reviews (
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
    )
    VALUES %s
    """,
    order_reviews_data
)
connection.commit()
print(
    "ORDER REVIEWS loaded:",
    len(order_reviews_data)
)
#  LOAD GEOLOCATION
print("\nLoading GEOLOCATION...")
geolocation_columns = [
    "geolocation_zip_code_prefix",
    "geolocation_lat",
    "geolocation_lng",
    "geolocation_city",
    "geolocation_state"
]
geolocation_data = prepare_data_for_postgresql(
    geolocation,
    geolocation_columns
)
execute_values(
    cursor,
    """
    INSERT INTO geolocation (
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state
    )
    VALUES %s
    """,
    geolocation_data
)
connection.commit()
print(
    "GEOLOCATION loaded:",
    len(geolocation_data)
)
#  POSTGRESQL ROW COUNT VERIFICATION
print("\nPOSTGRESQL ROW COUNT VERIFICATION")
table_names = [
    "customers",
    "orders",
    "order_items",
    "order_payments",
    "order_reviews",
    "products",
    "sellers",
    "geolocation",
    "category_translation"
]

for table_name in table_names:
    cursor.execute(
        f"SELECT COUNT(*) FROM {table_name};"
    )
    count = cursor.fetchone()[0]
    print(
        f"{table_name.upper():25} Rows: {count}"
    )
cursor.close()
connection.close()
print("\nPostgreSQL connection closed.")