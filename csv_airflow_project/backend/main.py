
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
from datetime import datetime, timezone
import shutil
import urllib.request
import urllib.error
import json


app = FastAPI(title="CSV/TSV Upload API")


app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Upload folder shared with Airflow
UPLOAD_DIR = Path("/opt/airflow/data/uploads")
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


# Airflow configuration
AIRFLOW_URL = "http://csv_airflow:8080"

AIRFLOW_USERNAME = "admin"
AIRFLOW_PASSWORD = "hGbzwCmxTBnaNNEd"

DAG_ID = "generic_sales_pipeline"


@app.get("/")
def home():
    return {
        "message": "CSV/TSV Upload API is running"
    }


# ---------------------------------------------------------
# Get JWT token from Airflow
# ---------------------------------------------------------

def get_airflow_token():

    token_url = f"{AIRFLOW_URL}/auth/token"

    login_data = json.dumps({
        "username": AIRFLOW_USERNAME,
        "password": AIRFLOW_PASSWORD
    }).encode("utf-8")

    request = urllib.request.Request(
        token_url,
        data=login_data,
        headers={
            "Content-Type": "application/json"
        },
        method="POST"
    )

    with urllib.request.urlopen(
        request,
        timeout=10
    ) as response:

        response_data = json.loads(
            response.read().decode("utf-8")
        )

    return response_data["access_token"]


# ---------------------------------------------------------
# Trigger Airflow DAG
# ---------------------------------------------------------

def trigger_airflow_dag(token):

    dag_url = (
        f"{AIRFLOW_URL}/api/v2/dags/"
        f"{DAG_ID}/dagRuns"
    )

    # Airflow 3 requires logical_date
    logical_date = datetime.now(
        timezone.utc
    ).isoformat()

    dag_data = json.dumps({
        "logical_date": logical_date
    }).encode("utf-8")

    request = urllib.request.Request(
        dag_url,
        data=dag_data,
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {token}"
        },
        method="POST"
    )

    with urllib.request.urlopen(
        request,
        timeout=10
    ) as response:

        return json.loads(
            response.read().decode("utf-8")
        )


# ---------------------------------------------------------
# Upload CSV / TSV
# ---------------------------------------------------------

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(...)
):

    # Check file name
    if not file.filename:
        raise HTTPException(
            status_code=400,
            detail="No file selected"
        )

    # Check file extension
    extension = Path(
        file.filename
    ).suffix.lower()

    if extension not in [".csv", ".tsv"]:
        raise HTTPException(
            status_code=400,
            detail="Only CSV and TSV files are allowed"
        )

    # Save uploaded file
    file_path = UPLOAD_DIR / file.filename

    with file_path.open("wb") as buffer:
        shutil.copyfileobj(
            file.file,
            buffer
        )

    print(
        f"File uploaded successfully: "
        f"{file.filename}"
    )

    # -----------------------------------------------------
    # Automatically trigger Airflow
    # -----------------------------------------------------

    try:

        # Step 1: Get JWT token
        token = get_airflow_token()

        print(
            "Airflow JWT token received"
        )

        # Step 2: Trigger DAG
        airflow_response = trigger_airflow_dag(
            token
        )

        print(
            "Airflow DAG triggered successfully"
        )

        return {
            "message": (
                "File uploaded and Airflow "
                "DAG triggered successfully"
            ),
            "filename": file.filename,
            "dag_id": DAG_ID,
            "airflow": airflow_response
        }

    except urllib.error.HTTPError as error:

        error_body = error.read().decode(
            "utf-8",
            errors="ignore"
        )

        print(
            f"Airflow HTTP error: "
            f"{error.code} - {error_body}"
        )

        raise HTTPException(
            status_code=500,
            detail=(
                f"Airflow error {error.code}: "
                f"{error_body}"
            )
        )

    except Exception as error:

        print(
            f"Airflow trigger failed: {error}"
        )

        raise HTTPException(
            status_code=500,
            detail=(
                "File uploaded, but Airflow "
                f"could not be triggered: {error}"
            )
        )

