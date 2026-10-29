
import csv
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_CSV = os.path.join(BASE_DIR, "sample_sales_dirty.csv")
OUTPUT_CSV = os.path.join(BASE_DIR, "cleaned_sales.csv")

spark = (
    SparkSession.builder
    .appName("SalesDataCleaningAndDAG")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("PYSPARK SALES DATA CLEANING PROJECT")

print("\nSpark application started.")
print("Spark UI: http://localhost:4040")
print("Input file:", INPUT_CSV)
print("Output file:", OUTPUT_CSV)


try:
   
    if not os.path.exists(INPUT_CSV):
        raise FileNotFoundError(
            f"Input CSV file not found: {INPUT_CSV}\n"
            "Keep sample_sales_dirty.csv in the same folder "
            "as spark_ui.py."
        )

    dirty_df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("mode", "PERMISSIVE")
        .csv(INPUT_CSV)
    )
    dirty_df = dirty_df.toDF(
        *[column.strip() for column in dirty_df.columns]
    )

    print("\nSTEP 1: ORIGINAL DIRTY DATA")
    dirty_df.show(truncate=False)

    original_count = dirty_df.count()
    print("Original row count:", original_count)

    print("\nOriginal schema:")
    dirty_df.printSchema()

  
    print("\nSTEP 2: CLEAN EMPTY VALUES")

    for column in dirty_df.columns:
        dirty_df = dirty_df.withColumn(
            column,
            F.when(
                F.trim(F.col(f"`{column}`")) == "",
                F.lit(None)
            ).otherwise(F.trim(F.col(f"`{column}`")))
        )

    dirty_df.show(truncate=False)

    
    print("\nSTEP 3: CONVERT DATA TYPES")

    typed_df = dirty_df.select(
        F.expr("try_cast(order_id AS INT)").alias("order_id"),
        F.col("product").alias("product"),
        F.col("category").alias("category"),
        F.expr("try_cast(price AS DOUBLE)").alias("price"),
        F.expr("try_cast(quantity AS INT)").alias("quantity"),
        F.col("region").alias("region")
    )

    typed_df.show(truncate=False)

    print("\nConverted schema:")
    typed_df.printSchema()

   
    print("\nSTEP 4: REMOVE INVALID RECORDS")
    valid_df = typed_df.filter(
        F.col("order_id").isNotNull()
        & F.col("product").isNotNull()
        & (F.trim(F.col("product")) != "")
        & F.col("category").isNotNull()
        & (F.trim(F.col("category")) != "")
        & F.col("price").isNotNull()
        & (F.col("price") > 0)
        & F.col("quantity").isNotNull()
        & (F.col("quantity") > 0)
        & F.col("region").isNotNull()
        & (F.trim(F.col("region")) != "")
    )

    valid_count = valid_df.count()
    invalid_removed = original_count - valid_count

    valid_df.show(truncate=False)

    print("Rows before validation:", original_count)
    print("Rows after validation:", valid_count)
    print("Invalid rows removed:", invalid_removed)


    print("\nSTEP 5: REMOVE DUPLICATES")

    before_deduplication = valid_df.count()

    deduplicated_df = valid_df.dropDuplicates(["order_id"])

    final_count = deduplicated_df.count()
    duplicates_removed = before_deduplication - final_count

    deduplicated_df.show(truncate=False)

    print("Rows before duplicate removal:", before_deduplication)
    print("Rows after duplicate removal:", final_count)
    print("Duplicate rows removed:", duplicates_removed)

    print("\nSTEP 6: CALCULATE TOTAL AMOUNT")
    cleaned_df = deduplicated_df.withColumn(
        "total_amount",
        F.round(F.col("price") * F.col("quantity"), 2)
    )

    cleaned_df.show(truncate=False)


    print("\nSTEP 7: CATEGORY-WISE SALES SUMMARY")

    category_summary = (
        cleaned_df
        .groupBy("category")
        .agg(
            F.count("*").alias("total_orders"),
            F.round(
                F.sum("total_amount"), 2
            ).alias("total_sales"),
            F.round(
                F.avg("total_amount"), 2
            ).alias("average_order_amount")
        )
        .orderBy("category")
    )

    category_summary.show(truncate=False)

    print("\nSTEP 8: SPARK EXECUTION PLAN")
    print(
        "The following plan shows the operations "
        "Spark uses to process the data."
    )
    cleaned_df.explain(mode="formatted")

    print("\nFinal cleaned row count:", cleaned_df.count())


    print("\nSTEP 9: SAVE CLEANED DATA")

    with open(
        OUTPUT_CSV,
        mode="w",
        newline="",
        encoding="utf-8"
    ) as output_file:

        writer = csv.writer(output_file)

        writer.writerow(cleaned_df.columns)
        for row in cleaned_df.toLocalIterator():
            writer.writerow(list(row))

    print("\nSUCCESS: Cleaned data saved!")
    print("Saved file:", OUTPUT_CSV)

    print("\nFinal cleaned data:")
    cleaned_df.show(truncate=False)



    print("DATA CLEANING SUMMARY")

    print("Original rows:", original_count)
    print("Invalid rows removed:", invalid_removed)
    print("Duplicate rows removed:", duplicates_removed)
    print("Final cleaned rows:", final_count)
    print("Output CSV:", OUTPUT_CSV)

    print("\nOpen http://localhost:4040 to inspect Spark UI.")
    print("You can inspect Jobs, Stages, and DAG visualizations.")

    input("\nPress Enter to stop Spark and close the Spark UI...")

finally:
    spark.stop()
    print("\nSpark session stopped.")
