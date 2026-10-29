from fastapi import FastAPI, Query, HTTPException
import json
import logging
import os
app = FastAPI(
    title="Product Pagination API",
    description="FastAPI service for paginated product dataset",
    version="1.0"
)
DATA_FILE = "data/products.json"
logging.basicConfig(
    filename="logs/app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def load_products():
    if not os.path.exists(DATA_FILE):
        raise FileNotFoundError(
            "products.json not found. Run fetch_data.py first."
        )
    with open(
        DATA_FILE,
        "r",
        encoding="utf-8"
    ) as file:
        return json.load(file)

@app.get("/")
def home():
    return {
        "message": "Product Pagination API is running"
    }

@app.get("/products")
def get_products(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
):
    try:
        products = load_products()
        total = len(products)
        # Calculate positions
        start = (page - 1) * limit
        end = start + limit
        # Page doesn't exist
        if start >= total:
            return {
                "page": page,
                "limit": limit,
                "total": total,
                "count": 0,
                "has_next": False,
                "has_previous": page > 1,
                "data": []
            }
        page_data = products[start:end]
        has_next = end < total
        has_previous = page > 1
        logging.info(
            f"API request: page={page}, limit={limit}"
        )
        return {
            "page": page,
            "limit": limit,
            "total": total,
            "count": len(page_data),
            "has_next": has_next,
            "has_previous": has_previous,
            "data": page_data
        }
    except FileNotFoundError as error:
        logging.error(str(error))
        raise HTTPException(
            status_code=500,
            detail=str(error)
        )
    except Exception as error:
        logging.error(
            f"Unexpected error: {error}"
        )
        raise HTTPException(
            status_code=500,
            detail="Internal server error"
        )