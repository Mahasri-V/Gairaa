import requests
import pandas as pd
import psycopg2
PRODUCTS_API = "https://fakestoreapi.com/products"
USERS_API = "https://fakestoreapi.com/users"
CARTS_API = "https://fakestoreapi.com/carts"
CATEGORIES_API = "https://fakestoreapi.com/products/categories"

def get_api_data(url):
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            print("API accessed successfully:", url)
            return response.json()
        else:
            print("API access failed:", url)
            print("Status code:", response.status_code)
            return None
    except Exception as e:
        print("Error accessing API:", url)
        print(e)


products_data = get_api_data(PRODUCTS_API)
users_data = get_api_data(USERS_API)
carts_data = get_api_data(CARTS_API)
categories_data = get_api_data(CATEGORIES_API)

if products_data is None:
    raise Exception("Products API failed")
if users_data is None:
    raise Exception("Users API failed")
if carts_data is None:
    raise Exception("Carts API failed")
if categories_data is None:
    raise Exception("Categories API failed")


products = []
for item in products_data:
    products.append({
        "product_id": item["id"],
        "title": item["title"],
        "price": item["price"],
        "category": item["category"],
        "rating": item["rating"]["rate"],
        "rating_count": item["rating"]["count"]
    })
products_df = pd.DataFrame(products)


users = []
for item in users_data:
    users.append({
        "user_id": item["id"],
        "first_name": item["name"]["firstname"],
        "last_name": item["name"]["lastname"],
        "email": item["email"],
        "username": item["username"],
        "phone": item["phone"],
        "city": item["address"]["city"],
        "zipcode": item["address"]["zipcode"]
    })
users_df = pd.DataFrame(users)


carts = []
for cart in carts_data:
    for product in cart["products"]:
        carts.append({
            "cart_id": cart["id"],
            "user_id": cart["userId"],
            "date": cart["date"],
            "product_id": product["productId"],
            "quantity": product["quantity"]
        })
carts_df = pd.DataFrame(carts)


categories_df = pd.DataFrame(
    categories_data,
    columns=["category"]
)


print("\n========== PRODUCTS ==========")
print(products_df.head())
print("\n========== USERS ==========")
print(users_df.head())
print("\n========== CARTS ==========")
print(carts_df.head())
print("\n========== CATEGORIES ==========")
print(categories_df)


products_df = products_df.drop_duplicates(
    subset="product_id"
)

products_df["price"] = pd.to_numeric(
    products_df["price"],
    errors="coerce"
)
products_df["rating"] = pd.to_numeric(
    products_df["rating"],
    errors="coerce"
)
products_df["rating_count"] = pd.to_numeric(
    products_df["rating_count"],
    errors="coerce"
)

products_df = products_df.dropna(
    subset=[
        "product_id",
        "title",
        "price",
        "category",
        "rating"
    ]
)

products_df = products_df[
    products_df["price"] >= 0
]

products_df = products_df[
    (products_df["rating"] >= 0) &
    (products_df["rating"] <= 5)
]

products_df["title"] = (
    products_df["title"]
    .str.strip()
)
products_df["category"] = (
    products_df["category"]
    .str.strip()
    .str.lower()
)

products_df["price"] = (
    products_df["price"]
    .round(2)
)
products_df["rating"] = (
    products_df["rating"]
    .round(2)
)

users_df = users_df.drop_duplicates(
    subset="user_id"
)

users_df["first_name"] = (
    users_df["first_name"]
    .str.strip()
)
users_df["last_name"] = (
    users_df["last_name"]
    .str.strip()
)
users_df["email"] = (
    users_df["email"]
    .str.strip()
    .str.lower()
)
users_df["username"] = (
    users_df["username"]
    .str.strip()
    .str.lower()
)
users_df["phone"] = (
    users_df["phone"]
    .astype(str)
    .str.strip()
)
users_df["city"] = (
    users_df["city"]
    .str.strip()
    .str.lower()
)
users_df["zipcode"] = (
    users_df["zipcode"]
    .astype(str)
    .str.strip()
)


carts_df = carts_df.drop_duplicates()

carts_df["cart_id"] = pd.to_numeric(
    carts_df["cart_id"],
    errors="coerce"
)
carts_df["user_id"] = pd.to_numeric(
    carts_df["user_id"],
    errors="coerce"
)
carts_df["product_id"] = pd.to_numeric(
    carts_df["product_id"],
    errors="coerce"
)
carts_df["quantity"] = pd.to_numeric(
    carts_df["quantity"],
    errors="coerce"
)

carts_df = carts_df.dropna(
    subset=[
        "cart_id",
        "user_id",
        "product_id",
        "quantity"
    ]
)

carts_df = carts_df[
    carts_df["quantity"] > 0
]

carts_df["cart_id"] = (
    carts_df["cart_id"]
    .astype(int)
)
carts_df["user_id"] = (
    carts_df["user_id"]
    .astype(int)
)
carts_df["product_id"] = (
    carts_df["product_id"]
    .astype(int)
)
carts_df["quantity"] = (
    carts_df["quantity"]
    .astype(int)
)

carts_df["date"] = pd.to_datetime(
    carts_df["date"],
    errors="coerce",
    utc=True
).dt.tz_localize(None)

categories_df["category"] = (
    categories_df["category"]
    .str.strip()
    .str.lower()
)
categories_df = categories_df.drop_duplicates()
categories_df = categories_df.dropna(
    subset=["category"]
)

print("CLEANED DATA")
print("Products:", len(products_df))
print("Users:", len(users_df))
print("Carts:", len(carts_df))
print("Categories:", len(categories_df))

products_df.to_csv(
    "products.csv",
    index=False
)
users_df.to_csv(
    "users.csv",
    index=False
)
carts_df.to_csv(
    "carts.csv",
    index=False
)
categories_df.to_csv(
    "categories.csv",
    index=False
)
print("CSV FILES CREATED")

try:
    conn = psycopg2.connect(
        host="localhost",
        port="5432",
        database="data_pipeline_db",
        user="postgres",
        password="mahasri"
    )
    cursor = conn.cursor()
    print("\nPostgreSQL connected successfully")
except Exception as e:
    print("PostgreSQL connection failed")
    print(e)
    raise SystemExit

cursor.execute(
    "DROP TABLE IF EXISTS order_items CASCADE"
)
cursor.execute(
    "DROP TABLE IF EXISTS carts CASCADE"
)
cursor.execute(
    "DROP TABLE IF EXISTS products CASCADE"
)
cursor.execute(
    "DROP TABLE IF EXISTS users CASCADE"
)
cursor.execute(
    "DROP TABLE IF EXISTS categories CASCADE"
)
conn.commit()
print("Old tables removed successfully")


cursor.execute("""
CREATE TABLE categories (
    category TEXT PRIMARY KEY

);
""")

cursor.execute("""
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY,
    title TEXT NOT NULL,
    price NUMERIC(10,2),
    category TEXT,
    rating NUMERIC(3,2),
    rating_count INTEGER
);
""")

cursor.execute("""
CREATE TABLE users (
    user_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    email TEXT,
    username TEXT,
    phone TEXT,
    city TEXT,
    zipcode TEXT
);
""")

cursor.execute("""
CREATE TABLE carts (
    cart_id INTEGER,
    user_id INTEGER,
    date TIMESTAMP,
    product_id INTEGER,
    quantity INTEGER
);
""")
conn.commit()
print("All PostgreSQL tables created successfully")

category_query = """
INSERT INTO categories (
    category
)
VALUES (%s)
"""
for _, row in categories_df.iterrows():
    cursor.execute(
        category_query,
        (
            row["category"],
        )
    )

product_query = """
INSERT INTO products (
    product_id,
    title,
    price,
    category,
    rating,
    rating_count
)
VALUES (%s, %s, %s, %s, %s, %s)
"""
for _, row in products_df.iterrows():
    cursor.execute(
        product_query,
        (
            int(row["product_id"]),
            row["title"],
            float(row["price"]),
            row["category"],
            float(row["rating"]),
            int(row["rating_count"])
        )
    )

user_query = """
INSERT INTO users (
    user_id,
    first_name,
    last_name,
    email,
    username,
    phone,
    city,
    zipcode
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
"""
for _, row in users_df.iterrows():
    cursor.execute(
        user_query,
        (
            int(row["user_id"]),
            row["first_name"],
            row["last_name"],
            row["email"],
            row["username"],
            row["phone"],
            row["city"],
            row["zipcode"]
        )
    )
cart_query = """
INSERT INTO carts (
    cart_id,
    user_id,
    date,
    product_id,
    quantity
)
VALUES (%s, %s, %s, %s, %s)
"""
for _, row in carts_df.iterrows():
    cursor.execute(
        cart_query,
        (
            int(row["cart_id"]),
            int(row["user_id"]),
            row["date"].to_pydatetime(),
            int(row["product_id"]),
            int(row["quantity"])
        )
    )

conn.commit()
print("DATA INSERTED SUCCESSFULLY")
print("Products inserted:", len(products_df))
print("Users inserted:", len(users_df))
print("Carts inserted:", len(carts_df))
print("Categories inserted:", len(categories_df))


cursor.execute(
    "SELECT COUNT(*) FROM products"
)
product_count = cursor.fetchone()[0]
cursor.execute(
    "SELECT COUNT(*) FROM users"
)
user_count = cursor.fetchone()[0]
cursor.execute(
    "SELECT COUNT(*) FROM carts"
)
cart_count = cursor.fetchone()[0]
cursor.execute(
    "SELECT COUNT(*) FROM categories"
)
category_count = cursor.fetchone()[0]
print("POSTGRESQL TABLE COUNTS")

print("Products:", product_count)
print("Users:", user_count)
print("Carts:", cart_count)
print("Categories:", category_count)

cursor.close()
conn.close()
print("POSTGRESQL CONNECTION CLOSED")
