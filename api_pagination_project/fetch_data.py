import requests
import csv
import json
import time
import logging
import os

API_URL = "https://dummyjson.com/products"
TIMEOUT = 10
MAX_RETRIES = 3
PAGE_SIZE = 20

REQUEST_DELAY = 1

MAX_REQUESTS = 5
TIME_WINDOW = 1

request_times = []

os.makedirs("data", exist_ok=True)
os.makedirs("logs", exist_ok=True)

logging.basicConfig(
    filename="logs/app.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

def rate_limit():
    global request_times
    current_time = time.time()
    request_times = [
        request_time
        for request_time in request_times
        if current_time - request_time < TIME_WINDOW
    ]
    if len(request_times) >= MAX_REQUESTS:
        wait_time = TIME_WINDOW - (
            current_time - request_times[0]
        )
        if wait_time > 0:
            logging.info(
                f"Rate limit reached. "
                f"Waiting {wait_time:.2f} seconds"
            )
            time.sleep(wait_time)
        current_time = time.time()
        request_times = [
            request_time
            for request_time in request_times
            if current_time - request_time < TIME_WINDOW
        ]
    request_times.append(time.time())
    logging.info(
        f"Request allowed. "
        f"Requests in current window: {len(request_times)}"
    )

def fetch_page(skip, limit):
    params = {
        "limit": limit,
        "skip": skip
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            logging.info(
                f"Requesting page: "
                f"skip={skip}, "
                f"limit={limit}, "
                f"attempt={attempt}"
            )
            # Apply rate limiting
            rate_limit()
            response = requests.get(
                API_URL,
                params=params,
                timeout=TIMEOUT
            )
            # Handle HTTP errors
            response.raise_for_status()
            data = response.json()
            logging.info(
                f"Successfully received "
                f"{len(data.get('products', []))} records"
            )
            return data
        
        except requests.exceptions.Timeout:
            logging.warning(
                f"Timeout on attempt {attempt}"
            )
       
        except requests.exceptions.ConnectionError:

            logging.warning(
                f"Connection error on attempt {attempt}"
            )
        
        except requests.exceptions.HTTPError as error:
            logging.error(
                f"HTTP error: {error}"
            )
            
            if response.status_code < 500 and response.status_code != 429:
                return None
        
        except requests.exceptions.RequestException as error:
            logging.error(
                f"Request error: {error}"
            )
     
        except ValueError:
            logging.error(
                "Invalid JSON response"
            )
            return None
        
        if attempt < MAX_RETRIES:
            wait_time = 2 ** (attempt - 1)
            logging.info(
                f"Waiting {wait_time} seconds before retry"
            )
            time.sleep(wait_time)
    # -----------------------------
    # Maximum retries reached
    # ----------------------------
    logging.error(
        f"Maximum retries exceeded for skip={skip}"
    )
    return None

def validate_product(product):
    required_fields = [
        "id",
        "title",
        "category",
        "price",
        "stock"
    ]
    for field in required_fields:
        if field not in product:
            logging.warning(
                f"Missing field '{field}' in product"
            )
            return False
    if not isinstance(product["id"], int):
        return False
    if not isinstance(product["title"], str):
        return False
    if not isinstance(product["price"], (int, float)):
        return False
    if not isinstance(product["stock"], int):
        return False
    return True

def fetch_all_products():
    all_products = []
    skip = 0
    seen_ids = set()
    while True:
        logging.info(
            f"Fetching data: "
            f"skip={skip}, "
            f"limit={PAGE_SIZE}"
        )
        data = fetch_page(
            skip=skip,
            limit=PAGE_SIZE
        )
        if data is None:
            logging.error(
                "Stopping because page request failed"
            )
            break
        products = data.get(
            "products",
            []
        )
        total = data.get(
            "total",
            0
        )
       
        if not products:
            logging.info(
                "No more products available"
            )
            break
      
        for product in products:
            product_id = product.get("id")
            
            if product_id in seen_ids:
                logging.warning(
                    f"Duplicate product ignored: "
                    f"{product_id}"
                )
                continue
            
            if not validate_product(product):
                logging.warning(
                    f"Invalid product ignored: "
                    f"{product_id}"
                )
                continue
            seen_ids.add(product_id)
            all_products.append({
                "id": product["id"],
                "title": product["title"],
                "category": product["category"],
                "price": product["price"],
                "stock": product["stock"]
            })
        logging.info(
            f"Total collected products: "
            f"{len(all_products)}"
        )
       
        if skip + PAGE_SIZE >= total:
            logging.info(
                "All available products have been fetched"
            )
            break
        skip += PAGE_SIZE
       
        logging.info(
            f"Waiting {REQUEST_DELAY} "
            f"second before next request"
        )
        time.sleep(REQUEST_DELAY)
    return all_products

def save_json(products):
    with open(
        "data/products.json",
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            products,
            file,
            indent=4
        )
    logging.info(
        "JSON dataset created successfully"
    )

def save_csv(products):
    if not products:
        return
    with open(
        "data/products.csv",
        "w",
        newline="",
        encoding="utf-8"
    ) as file:
        fieldnames = [
            "id",
            "title",
            "category",
            "price",
            "stock"
        ]
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames
        )
        writer.writeheader()
        writer.writerows(products)
    logging.info(
        "CSV dataset created successfully"
    )

if __name__ == "__main__":
    logging.info(
        "========== DATA COLLECTION STARTED =========="
    )
    products = fetch_all_products()
    save_json(products)
    save_csv(products)
    print(
        "Dataset creation completed."
    )
    print(
        f"Total unique valid products: "
        f"{len(products)}"
    )
    print("Created:")
    print(
        "data/products.json"
    )
    print(
        "data/products.csv"
    )
    logging.info(
        "========== DATA COLLECTION COMPLETED =========="
    )