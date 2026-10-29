from datetime import datetime
from pathlib import Path

import pandas as pd
import psycopg2

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


UPLOAD_DIR = Path("/opt/airflow/data/uploads")
PROCESSED_DIR = Path("/opt/airflow/data/processed")

XCOM_KEY = "processed_file"

POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow",
}

def extract_file(**context):

    files = (
        list(UPLOAD_DIR.glob("*.csv"))
        + list(UPLOAD_DIR.glob("*.tsv"))
    )

    if not files:
        raise FileNotFoundError(
            "No CSV or TSV file found in upload folder."
        )
    input_file = max(
        files,
        key=lambda file: file.stat().st_mtime
    )

    print(
        f"Extracting file: {input_file.name}"
    )
    if input_file.suffix.lower() == ".csv":

        df = pd.read_csv(input_file)

    else:

        df = pd.read_csv(
            input_file,
            sep="\t"
        )
    if df.empty:
        raise ValueError(
            "Uploaded file is empty."
        )

    print(
        f"Extracted rows: {len(df)}"
    )

    print(
        f"Extracted columns: "
        f"{list(df.columns)}"
    )
    context["ti"].xcom_push(
        key="input_file",
        value=str(input_file)
    )

def transform_file(**context):

    input_file = Path(
        context["ti"].xcom_pull(
            task_ids="extract_file",
            key="input_file"
        )
    )

    print(
        f"Transforming file: "
        f"{input_file.name}"
    )

    if input_file.suffix.lower() == ".csv":

        df = pd.read_csv(input_file)

    else:

        df = pd.read_csv(
            input_file,
            sep="\t"
        )

   
    df = df.dropna(
        how="all"
    )
    before_duplicates = len(df)

    df = df.drop_duplicates()

    duplicates_removed = (
        before_duplicates - len(df)
    )

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
        .str.replace("-", "_")
    )

    for column in df.select_dtypes(
        include="object"
    ).columns:

        df[column] = df[column].apply(
            lambda value:
            value.strip()
            if isinstance(value, str)
            else value
        )

    PROCESSED_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    
    output_file = (
        PROCESSED_DIR
        / f"cleaned_{input_file.stem}.csv"
    )

    df.to_csv(
        output_file,
        index=False
    )

    print(
        f"Rows after transformation: "
        f"{len(df)}"
    )

    print(
        f"Duplicates removed: "
        f"{duplicates_removed}"
    )

    print(
        f"Cleaned CSV created: "
        f"{output_file}"
    )
    context["ti"].xcom_push(
        key=XCOM_KEY,
        value=str(output_file)
    )
def validate_file(**context):

    processed_file = Path(
        context["ti"].xcom_pull(
            task_ids="transform_file",
            key=XCOM_KEY
        )
    )

    print(
        f"Validating file: "
        f"{processed_file.name}"
    )

    if not processed_file.exists():

        raise FileNotFoundError(
            "Cleaned CSV file does not exist."
        )

    df = pd.read_csv(
        processed_file
    )

    if df.empty:

        raise ValueError(
            "Validation failed: CSV is empty."
        )

    if len(df.columns) == 0:

        raise ValueError(
            "Validation failed: "
            "No columns found."
        )

    duplicate_count = (
        df.duplicated().sum()
    )

    if duplicate_count > 0:

        raise ValueError(
            f"Validation failed: "
            f"{duplicate_count} duplicate "
            f"rows found."
        )

    missing_values = (
        df.isnull().sum()
    )

    print(
        "Missing values:"
    )

    print(
        missing_values[
            missing_values > 0
        ]
    )

    print(
        "Validation successful."
    )

    print(
        f"Rows validated: {len(df)}"
    )

    print(
        f"Columns validated: "
        f"{len(df.columns)}"
    )

def load_to_postgresql(**context):

    processed_file = Path(
        context["ti"].xcom_pull(
            task_ids="transform_file",
            key=XCOM_KEY
        )
    )

    print(
        f"Loading file: "
        f"{processed_file.name}"
    )
    df = pd.read_csv(
        processed_file
    )

    if df.empty:

        raise ValueError(
            "Cannot load empty CSV."
        )
    table_name = processed_file.stem



    if table_name.startswith(
        "cleaned_"
    ):

        table_name = table_name.replace(
            "cleaned_",
            "",
            1
        )
    table_name = (
        table_name
        .lower()
        .strip()
        .replace(" ", "_")
        .replace("-", "_")
    )

    print(
        f"PostgreSQL table: "
        f"{table_name}"
    )

    conn = psycopg2.connect(
        **POSTGRES_CONFIG
    )

    cursor = conn.cursor()

    try:
        columns = []

        for column in df.columns:

            safe_column = (
                column
                .lower()
                .strip()
                .replace(" ", "_")
                .replace("-", "_")
            )

            columns.append(
                f'"{safe_column}" TEXT'
            )
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS
            "{table_name}" (
                {", ".join(columns)}
            )
        """

        cursor.execute(
            create_table_sql
        )

        cursor.execute(
            f'TRUNCATE TABLE "{table_name}"'
        )

        with open(
            processed_file,
            "r",
            encoding="utf-8"
        ) as file:

            cursor.copy_expert(
                f"""
                COPY "{table_name}"
                FROM STDIN
                WITH (
                    FORMAT CSV,
                    HEADER TRUE
                )
                """,
                file
            )

        conn.commit()

        print(
            f"Successfully loaded "
            f"{len(df)} rows into "
            f"PostgreSQL table "
            f"'{table_name}'."
        )

    except Exception:

        conn.rollback()

        raise

    finally:

        cursor.close()
        conn.close()



with DAG(

    dag_id="generic_sales_pipeline",

    start_date=datetime(
        2026,
        10,
        7
    ),

    schedule="@daily",

    catchup=False,

    tags=[
        "csv",
        "tsv",
        "etl",
        "sales"
    ],

) as dag:

    extract = PythonOperator(
        task_id="extract_file",
        python_callable=extract_file,
    )
    transform = PythonOperator(
        task_id="transform_file",
        python_callable=transform_file,
    )
    validate = PythonOperator(
        task_id="validate_file",
        python_callable=validate_file,
    )
    load = PythonOperator(
        task_id="load_to_postgresql",
        python_callable=load_to_postgresql,
    )

    extract >> transform >> validate >> load