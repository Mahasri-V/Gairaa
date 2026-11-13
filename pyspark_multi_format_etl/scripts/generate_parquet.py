
from pyspark.sql import SparkSession

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("Employee_CSV_to_Parquet") \
    .getOrCreate()

print("Starting CSV to Parquet conversion...")

# 2. Read CSV file
csv_path = "/app/input/csv/batch1.csv"

df = spark.read \
    .option("header", True) \
    .option("inferSchema", False) \
    .csv(csv_path)

# 3. Display original data
print("Original CSV records:")
df.show(truncate=False)

# 4. Check record count
total_records = df.count()
print("Total CSV records:", total_records)

if total_records != 10:
    raise ValueError(
        f"Expected 10 records, but found {total_records}"
    )

# 5. Generate genuine Parquet file
parquet_path = "/app/input/parquet/batch1_parquet"

df.write \
    .mode("overwrite") \
    .parquet(parquet_path)

print("Parquet file generated successfully!")

# 6. Read Parquet file again to verify
parquet_df = spark.read.parquet(parquet_path)

print("Parquet schema:")
parquet_df.printSchema()

print("Parquet records:")
parquet_df.show(truncate=False)

print("Total Parquet records:", parquet_df.count())

spark.stop()
