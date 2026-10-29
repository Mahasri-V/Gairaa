from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from database import create_table, get_all_products
from loader import full_load, incremental_load


app = FastAPI(title="Full and Incremental Loading Dashboard")

templates = Jinja2Templates(directory="templates")

create_table()
source_data = []

@app.get("/", response_class=HTMLResponse)
def home(request: Request):

    target_data = get_all_products()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "source_data": source_data,
            "target_data": target_data,
            "result": None
        }
    )

@app.post("/add")
async def add_data(request: Request):

    global source_data

    form = await request.form()

    product_id = int(form["product_id"])
    product_name = form["product_name"]
    price = float(form["price"])

    for product in source_data:

        if product["product_id"] == product_id:

            target_data = get_all_products()

            result = {
                "load_type": "ADD DATA",
                "source_records": len(source_data),
                "loaded_records": 0,
                "target_records": len(target_data),
                "new_records": 0,
                "updated_records": 0,
                "status": "Product ID already exists. Use Update Data."
            }

            return templates.TemplateResponse(
                "index.html",
                {
                    "request": request,
                    "source_data": source_data,
                    "target_data": target_data,
                    "result": result
                }
            )

    product = {
        "product_id": product_id,
        "product_name": product_name,
        "price": price
    }

    source_data.append(product)

    target_data = get_all_products()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "source_data": source_data,
            "target_data": target_data,
            "result": None
        }
    )


@app.post("/update")
async def update_data(request: Request):

    global source_data

    form = await request.form()

    product_id = int(form["product_id"])
    product_name = form["product_name"]
    price = float(form["price"])

    updated = False
    for product in source_data:

        if product["product_id"] == product_id:

            product["product_name"] = product_name
            product["price"] = price

            updated = True
            break

    target_data = get_all_products()

    if updated:

        result = {
            "load_type": "UPDATE SOURCE",
            "source_records": len(source_data),
            "loaded_records": 0,
            "target_records": len(target_data),
            "new_records": 0,
            "updated_records": 0,
            "status": "Source data updated successfully."
        }

    else:

        result = {
            "load_type": "UPDATE SOURCE",
            "source_records": len(source_data),
            "loaded_records": 0,
            "target_records": len(target_data),
            "new_records": 0,
            "updated_records": 0,
            "status": "Product ID not found in source data."
        }

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "source_data": source_data,
            "target_data": target_data,
            "result": result
        }
    )

@app.post("/full-load")
def run_full_load(request: Request):

    global source_data

    result = full_load(source_data)

    target_data = get_all_products()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "source_data": source_data,
            "target_data": target_data,
            "result": result
        }
    )

@app.post("/incremental-load")
def run_incremental_load(request: Request):

    global source_data

    result = incremental_load(source_data)

    target_data = get_all_products()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "source_data": source_data,
            "target_data": target_data,
            "result": result
        }
    )
