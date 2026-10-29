
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    regexp_replace,
    trim,
    when,
    split,
    element_at,
    round as spark_round
)


spark = (
    SparkSession.builder
    .appName("Amazon Product Data Analysis")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

print("\n========== AMAZON DATA ANALYSIS ==========")


CSV_FILE = "amazon.csv"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("mode", "PERMISSIVE")
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(CSV_FILE)
)

print("\n========== ORIGINAL DATA ==========")

print("Original row count:", df.count())
print("Original column count:", len(df.columns))

df.show(5, truncate=60)
df.printSchema()


print("\n========== REMOVE DUPLICATES ==========")

if "product_id" in df.columns:
    df = df.dropDuplicates(["product_id"])

print("Rows after removing duplicate product IDs:", df.count())


print("\n========== REMOVE INVALID ROWS ==========")

required_columns = [
    "product_id",
    "product_name",
    "category"
]

for column_name in required_columns:
    if column_name in df.columns:
        df = df.filter(
            col(column_name).isNotNull()
            & (trim(col(column_name)) != "")
        )

print("Rows after removing invalid records:", df.count())


print("\n========== CLEAN PRICE DATA ==========")

for column_name in ["discounted_price", "actual_price"]:
    if column_name in df.columns:
        df = df.withColumn(
            column_name,
            regexp_replace(
                trim(col(column_name)),
                r"[^0-9.]",
                ""
            ).try_cast("double")
        )


print("\n========== CLEAN RATING DATA ==========")

if "rating" in df.columns:
    df = df.withColumn(
        "rating",
        regexp_replace(
            trim(col("rating")),
            r"[^0-9.]",
            ""
        ).try_cast("double")
    )

    df = df.withColumn(
        "rating",
        when(
            (col("rating") >= 0) & (col("rating") <= 5),
            col("rating")
        ).otherwise(None)
    )


print("\n========== CLEAN RATING COUNT ==========")
if "rating_count" in df.columns:
    df = df.withColumn(
        "rating_count",
        regexp_replace(
            trim(col("rating_count")),
            ",",
            ""
        ).try_cast("long")
    )



print("\n========== CLEAN DISCOUNT PERCENTAGE ==========")

if "discount_percentage" in df.columns:
    df = df.withColumn(
        "discount_percentage",
        regexp_replace(
            trim(col("discount_percentage")),
            r"[^0-9.]",
            ""
        ).try_cast("double")
    )


print("\n========== CLEAN TEXT DATA ==========")
text_columns = [
    "product_name",
    "category",
    "about_product",
    "user_name",
    "review_title",
    "review_content"
]

for column_name in text_columns:
    if column_name in df.columns:
        df = df.withColumn(
            column_name,
            trim(col(column_name))
        )


print("\n========== CATEGORY ANALYSIS ==========")
if "category" in df.columns:
    df = df.withColumn(
        "category_leaf",
        trim(
            element_at(
                split(col("category"), r"\|"),
                -1
            )
        )
    )

    category_summary = df.groupBy("category_leaf").agg(
        count("*").alias("total_products"),
        spark_round(
            avg("discounted_price"), 2
        ).alias("average_discounted_price"),
        spark_round(
            avg("actual_price"), 2
        ).alias("average_actual_price"),
        spark_round(
            avg("rating"), 2
        ).alias("average_rating")
    )

    category_summary = category_summary.orderBy(
        col("total_products").desc()
    )

    category_summary.show(20, truncate=False)

else:
    category_summary = None
    print("Category column was not found.")


print("\n========== CLEANED DATA ==========")
print("Cleaned row count:", df.count())
print("Cleaned column count:", len(df.columns))

df.show(10, truncate=60)
df.printSchema()


print("\n========== MISSING VALUE REPORT ==========")

for column_name in df.columns:
    missing_count = df.filter(
        col(column_name).isNull()
        | (trim(col(column_name).cast("string")) == "")
    ).count()

    print(f"{column_name}: {missing_count} missing values")


print("\n========== TOP-RATED PRODUCTS ==========")

if "rating" in df.columns:
    df.filter(
        col("rating").isNotNull()
    ).orderBy(
        col("rating").desc(),
        col("rating_count").desc_nulls_last()
        if "rating_count" in df.columns
        else col("rating").desc()
    ).select(
        *[
            column_name
            for column_name in [
                "product_id",
                "product_name",
                "category_leaf",
                "discounted_price",
                "rating",
                "rating_count"
            ]
            if column_name in df.columns
        ]
    ).show(10, truncate=60)


print("\n========== SAVE CLEANED DATA ==========")

CLEANED_OUTPUT = "output/amazon_cleaned"

(
    df.write
    .mode("overwrite")
    .parquet(CLEANED_OUTPUT)
)

print("Cleaned data saved to:", CLEANED_OUTPUT)



if category_summary is not None:
    SUMMARY_OUTPUT = "output/category_summary"

    (
        category_summary.write
        .mode("overwrite")
        .parquet(SUMMARY_OUTPUT)
    )

    print("Category summary saved to:", SUMMARY_OUTPUT)


print("\n========== PROJECT SUMMARY ==========")
print("Original row count:", "See original data section above")
print("Final cleaned row count:", df.count())
print("Final column count:", len(df.columns))
print("Output format: Parquet")
print("Project completed successfully!")

spark.stop()
