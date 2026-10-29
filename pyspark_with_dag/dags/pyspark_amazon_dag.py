
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

CSV_FILE = "/opt/airflow/data/amazon.csv"

EXTRACTED_PATH = "/opt/airflow/output/extracted_amazon"
VALIDATED_PATH = "/opt/airflow/output/validated_amazon"
TRANSFORMED_PATH = "/opt/airflow/output/transformed_amazon_staging"
FINAL_PATH = "/opt/airflow/output/amazon_transformed"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("AmazonSalesETL")
        .master("local[*]")
        .getOrCreate()
    )


def extract_data():
    spark = create_spark_session()

    try:
        print("Starting Amazon CSV extraction...")

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", False)
            .option("encoding", "UTF-8")
            .option("multiLine", True)
            .option("escape", '"')
            .csv(CSV_FILE)
        )

        row_count = df.count()

        if row_count == 0:
            raise ValueError("The Amazon CSV file is empty.")

        df.write.mode("overwrite").parquet(EXTRACTED_PATH)

        print(f"Extraction successful: {row_count} rows")
        print(f"Extracted data saved to: {EXTRACTED_PATH}")

    finally:
        spark.stop()


def validate_data():
    spark = create_spark_session()

    try:
        print("Starting Amazon data validation...")

        df = spark.read.parquet(EXTRACTED_PATH)

        required_columns = [
            "product_id",
            "discounted_price",
            "actual_price",
            "discount_percentage",
            "rating",
            "rating_count",
        ]

        missing_columns = [
            column
            for column in required_columns
            if column not in df.columns
        ]

        if missing_columns:
            raise ValueError(
                f"Required columns missing: {missing_columns}"
            )

        original_count = df.count()

        df = df.filter(
            F.col("product_id").isNotNull()
            & (F.trim(F.col("product_id")) != "")
        )

        df = df.dropDuplicates(["product_id"])

        valid_count = df.count()

        if valid_count == 0:
            raise ValueError(
                "No valid products remain after validation."
            )

        df.write.mode("overwrite").parquet(VALIDATED_PATH)

        print(f"Original rows: {original_count}")
        print(f"Validated unique products: {valid_count}")
        print(f"Validation successful: {VALIDATED_PATH}")

    finally:
        spark.stop()



def transform_data():
    spark = create_spark_session()

    try:
        print("Starting Amazon data transformation...")

        df = spark.read.parquet(VALIDATED_PATH)
        numeric_columns = [
            "discounted_price",
            "actual_price",
            "discount_percentage",
            "rating",
        ]

        for column_name in numeric_columns:
            temporary_column = f"__{column_name}_numeric"

            cleaned_text = F.regexp_extract(
                F.regexp_replace(
                    F.trim(F.col(column_name)),
                    ",",
                    "",
                ),
                r"([0-9]+(?:\.[0-9]+)?)",
                1,
            )

            df = df.withColumn(
                temporary_column,
                cleaned_text,
            )

            df = df.withColumn(
                column_name,
                F.expr(
                    f"try_cast(`{temporary_column}` AS DOUBLE)"
                ),
            )

            df = df.drop(temporary_column)

        rating_count_text = F.regexp_extract(
            F.regexp_replace(
                F.trim(F.col("rating_count")),
                ",",
                "",
            ),
            r"([0-9]+)",
            1,
        )

        df = df.withColumn(
            "__rating_count_numeric",
            rating_count_text,
        )

        df = df.withColumn(
            "rating_count",
            F.expr(
                "try_cast(`__rating_count_numeric` AS BIGINT)"
            ),
        )

        df = df.drop("__rating_count_numeric")

        df = df.filter(
            (F.col("discounted_price") > 0)
            & (F.col("actual_price") > 0)
            & F.col("discount_percentage").between(0, 100)
        )

        df = df.withColumn(
            "savings",
            F.round(
                F.col("actual_price")
                - F.col("discounted_price"),
                2,
            ),
        )

        row_count = df.count()

        if row_count == 0:
            raise ValueError(
                "No valid products remain after transformation. "
                "Check the source CSV values."
            )
        df.write.mode("overwrite").parquet(TRANSFORMED_PATH)

        print(f"Transformation successful: {row_count} rows")
        print(f"Transformed data saved to: {TRANSFORMED_PATH}")

        df.select(
            "product_id",
            "product_name",
            "discounted_price",
            "actual_price",
            "discount_percentage",
            "rating",
            "rating_count",
            "savings",
        ).show(10, truncate=40)

    finally:
        spark.stop()


def load_data():
    spark = create_spark_session()

    try:
        print("Starting final data loading...")

        df = spark.read.parquet(TRANSFORMED_PATH)

        row_count = df.count()

        if row_count == 0:
            raise ValueError(
                "No transformed data available to load."
            )

        df.write.mode("overwrite").parquet(FINAL_PATH)

        final_df = spark.read.parquet(FINAL_PATH)
        final_count = final_df.count()

        if final_count != row_count:
            raise ValueError(
                f"Load verification failed. "
                f"Expected {row_count}, found {final_count}."
            )

        print(f"Load successful: {final_count} rows")
        print(f"Final dataset saved to: {FINAL_PATH}")

        final_df.show(5, truncate=40)

    finally:
        spark.stop()

with DAG(
    dag_id="pyspark_amazon_etl",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["amazon", "pyspark", "etl"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    validate_task = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_task = PythonOperator(
        task_id="load_data",
        python_callable=load_data,
    )

    extract_task >> validate_task >> transform_task >> load_task
