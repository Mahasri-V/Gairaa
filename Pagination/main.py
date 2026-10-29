from fastapi import FastAPI, Query
from fastapi.responses import FileResponse
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
    return FileResponse("index.html")
@app.get("/test")
def test():
    return {
        "message": "FastAPI is working"
    }
@app.get("/products")
def get_products(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    offset = (page - 1) * limit
    connection = get_connection()
    cursor = connection.cursor()
    query = """
    SELECT *
    FROM products
    ORDER BY product_id
    LIMIT %s OFFSET %s
"""
    cursor.execute(query, (limit, offset))
    rows = cursor.fetchall()
    columns = [description[0] for description in cursor.description]
    records = [
        dict(zip(columns, row))
        for row in rows
    ]
    cursor.close()
    connection.close()
    return {
        "page": page,
        "limit": limit,
        "offset": offset,
        "records": records
    }