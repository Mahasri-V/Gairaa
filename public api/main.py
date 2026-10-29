from fastapi import FastAPI
import psycopg2

app = FastAPI()
def get_connection():
    return psycopg2.connect(
        host="localhost",
        port="5432",
        database="data_pipeline_db",
        user="postgres",
        password="mahasri"
    )
@app.get("/")
def home():
    return {
        "message": "API is working"
    }
@app.get("/products")
def get_products():
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            product_id,
            title,
            price,
            category,
            rating,
            rating_count
        FROM products
        ORDER BY product_id
    """)

    rows = cursor.fetchall()

    products = []

    for row in rows:
        products.append({
            "product_id": row[0],
            "title": row[1],
            "price": float(row[2]) if row[2] is not None else None,
            "category": row[3],
            "rating": float(row[4]) if row[4] is not None else None,
            "rating_count": row[5]
        })

    cursor.close()
    conn.close()

    return products