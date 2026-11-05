
import os
import sys


PYTHON_PATH = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

INPUT_FILE = "amazon.csv"

spark = (
    SparkSession.builder
    .appName("Advanced_PySpark_Project")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")


print("\n========== STEP 1: READ DATASET ==========")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("multiLine", True)
    .option("mode", "PERMISSIVE")
    .csv(INPUT_FILE)
)

print("Total Records:", df.count())
print("Total Columns:", len(df.columns))

print("\nColumn Names:")

for column in df.columns:
    print("-", column)

print("\nDataset Schema:")
df.printSchema()

print("\nFirst 5 Records:")
df.show(5, truncate=30)


print("\n========== STEP 2: DATA INSPECTION ==========")

print("Total Rows:", df.count())

print("\nMissing Values:")

missing_values = df.select([
    F.count(
        F.when(
            F.col(column).isNull() |
            (F.trim(F.col(column).cast("string")) == ""),
            column
        )
    ).alias(column)
    for column in df.columns
])

missing_values.show(truncate=25)



print("\n========== STEP 3: DATA CLEANING ==========")

df_clean = (
    df
    .withColumn(
        "discounted_price",
        F.regexp_replace(
            F.col("discounted_price").cast("string"),
            r"[^0-9.]",
            ""
        ).cast("double")
    )
    .withColumn(
        "actual_price",
        F.regexp_replace(
            F.col("actual_price").cast("string"),
            r"[^0-9.]",
            ""
        ).cast("double")
    )
    .withColumn(
        "discount_percentage",
        F.regexp_replace(
            F.col("discount_percentage").cast("string"),
            r"[^0-9.]",
            ""
        ).cast("double")
    )
    .withColumn(
        "rating",
        F.expr("try_cast(rating AS DOUBLE)")
    )
    .withColumn(
        "rating_count",
        F.regexp_replace(
            F.col("rating_count").cast("string"),
            r"[^0-9]",
            ""
        ).cast("long")
    )
)

print("Data cleaning completed.")
print("Cleaned records:", df_clean.count())

print("\nCleaned Dataset Sample:")

df_clean.select(
    "product_id",
    "product_name",
    "category",
    "discounted_price",
    "actual_price",
    "rating"
).show(5, truncate=30)



print("\n========== STEP 4: WHERE CONDITION ==========")

filtered_df = df_clean.filter(
    (F.col("rating") >= 3.0) &
    (F.col("discounted_price") > 0) &
    (F.col("discounted_price").isNotNull())
)

print("Records before filtering:", df_clean.count())
print("Records after filtering:", filtered_df.count())

filtered_df.select(
    "product_id",
    "product_name",
    "discounted_price",
    "rating"
).show(5, truncate=30)


print("\n========== STEP 5: WINDOW FUNCTIONS ==========")

window_spec = (
    Window
    .partitionBy("category")
    .orderBy(F.col("rating").desc())
)

window_df = (
    filtered_df
    .withColumn(
        "row_number",
        F.row_number().over(window_spec)
    )
    .withColumn(
        "rank",
        F.rank().over(window_spec)
    )
    .withColumn(
        "dense_rank",
        F.dense_rank().over(window_spec)
    )
)

print("\nROW_NUMBER, RANK and DENSE_RANK:")

window_df.select(
    "product_id",
    "category",
    "rating",
    "row_number",
    "rank",
    "dense_rank"
).show(10, truncate=25)


print("\n========== STEP 6: PREPARE JOIN DATA ==========")

products = (
    filtered_df
    .select(
        "product_id",
        "product_name",
        "category",
        "rating",
        "discounted_price"
    )
    .dropDuplicates(["product_id"])
)

reviews = (
    filtered_df
    .select(
        F.col("product_id").alias("review_product_id"),
        F.col("rating").alias("review_rating")
    )
    .dropDuplicates(["review_product_id"])
)

print("Products count:", products.count())
print("Reviews count:", reviews.count())

print("\n========== STEP 7: INNER JOIN ==========")

inner_df = products.join(
    reviews,
    products.product_id == reviews.review_product_id,
    "inner"
)

print("Inner Join records:", inner_df.count())

inner_df.select(
    "product_id",
    "product_name",
    "rating",
    "review_rating"
).show(5, truncate=25)



print("\n========== STEP 8: LEFT OUTER JOIN ==========")

left_df = products.join(
    reviews,
    products.product_id == reviews.review_product_id,
    "left"
)

print("Left Join records:", left_df.count())
left_df.show(5, truncate=25)



print("\n========== STEP 9: RIGHT OUTER JOIN ==========")

right_df = products.join(
    reviews,
    products.product_id == reviews.review_product_id,
    "right"
)

print("Right Join records:", right_df.count())
right_df.show(5, truncate=25)



print("\n========== STEP 10: FULL OUTER JOIN ==========")

full_df = products.join(
    reviews,
    products.product_id == reviews.review_product_id,
    "full"
)

print("Full Outer Join records:", full_df.count())
full_df.show(5, truncate=25)



print("\n========== STEP 11: CROSS JOIN ==========")

products_small = products.limit(5)
reviews_small = reviews.limit(5)

cross_df = products_small.crossJoin(reviews_small)

print("Cross Join records:", cross_df.count())
cross_df.show(5, truncate=25)



print("\n========== STEP 12: SELF JOIN ==========")

product_a = products.alias("A")
product_b = products.alias("B")

self_df = product_a.join(
    product_b,
    (F.col("A.category") == F.col("B.category")) &
    (F.col("A.product_id") < F.col("B.product_id")),
    "inner"
)

print("Self Join records:", self_df.count())

self_df.select(
    F.col("A.product_id").alias("product_1"),
    F.col("A.product_name").alias("name_1"),
    F.col("B.product_id").alias("product_2"),
    F.col("B.product_name").alias("name_2"),
    F.col("A.category").alias("category")
).show(5, truncate=25)


print("\n========== STEP 13: SHUFFLE OPERATIONS ==========")

print("\n1. GROUP BY - Category Count")

group_df = (
    filtered_df
    .groupBy("category")
    .count()
    .orderBy(F.col("count").desc())
)

group_df.show(10, truncate=25)


print("\n2. REPARTITION")

repartition_df = filtered_df.repartition(
    8,
    "category"
)

print(
    "Partitions after repartition:",
    repartition_df.rdd.getNumPartitions()
)


print("\n3. ORDER BY")

order_df = filtered_df.orderBy(
    F.col("rating").desc()
)

order_df.select(
    "product_id",
    "product_name",
    "rating"
).show(5, truncate=25)


print("\n========== PROJECT SUMMARY ==========")

print("Original Records:", df.count())
print("Cleaned Records:", df_clean.count())
print("Filtered Records:", filtered_df.count())

print("Window Functions: Completed")
print("Inner Join: Completed")
print("Left Outer Join: Completed")
print("Right Outer Join: Completed")
print("Full Outer Join: Completed")
print("Cross Join: Completed")
print("Self Join: Completed")
print("Shuffle Operations: Executed")

print("\nADVANCED PYSPARK OPERATIONS COMPLETED!")



print("\n========== SPARK WEB UI ==========")

print("Spark UI URL:", spark.sparkContext.uiWebUrl)

input("\nKeep this CMD window open. Press ENTER after viewing Spark UI...")

spark.stop()

print("Spark application stopped successfully.")
