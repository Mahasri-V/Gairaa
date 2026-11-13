
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# 1. Create Spark session
spark = SparkSession.builder \
    .appName("Employee_Data_Cleaning") \
    .getOrCreate()

print("\n========== EMPLOYEE ETL CLEANING ==========\n")

# Common employee columns
columns = [
    "employee_id",
    "first_name",
    "last_name",
    "email",
    "department",
    "salary",
    "city",
    "joining_date"
]

# 2. Read CSV
print("Reading CSV data...")

csv_df = spark.read \
    .option("header", True) \
    .csv("/app/input/csv/batch1.csv")

csv_df = csv_df.select(
    *[F.col(c).cast("string").alias(c) for c in columns]
)

# 3. Read Parquet
print("Reading Parquet data...")

parquet_df = spark.read.parquet(
    "/app/input/parquet/batch1_parquet"
)

parquet_df = parquet_df.select(
    *[F.col(c).cast("string").alias(c) for c in columns]
)

# 4. Read JSON
print("Reading JSON data...")

json_df = spark.read \
    .option("multiLine", True) \
    .json("/app/input/json/batch1.json")

json_df = json_df.select(
    *[F.col(c).cast("string").alias(c) for c in columns]
)

# 5. Combine all three input formats
print("Combining CSV, Parquet and JSON...")

combined_df = csv_df.unionByName(parquet_df) \
                    .unionByName(json_df)

print("Total input records:", combined_df.count())

# 6. Remove duplicate employee IDs
clean_df = combined_df.dropDuplicates(["employee_id"])

print("Records after duplicate removal:", clean_df.count())

# 7. Preserve original salary and date values
clean_df = clean_df \
    .withColumn("salary_raw", F.col("salary")) \
    .withColumn("joining_date_raw", F.col("joining_date")) \
    .withColumn("employee_id_raw", F.col("employee_id"))

# 8. Trim spaces and standardize values
for col_name in ["first_name", "last_name", "email",
                 "department", "city"]:
    clean_df = clean_df.withColumn(
        col_name,
        F.when(
            F.trim(F.col(col_name)) == "",
            None
        ).otherwise(F.trim(F.col(col_name)))
    )

clean_df = clean_df \
    .withColumn("email", F.lower(F.col("email"))) \
    .withColumn("department", F.upper(F.col("department")))

# 9. Convert data types safely
clean_df = clean_df \
    .withColumn(
        "employee_id",
        F.expr("try_cast(employee_id AS INT)")
    ) \
    .withColumn(
        "salary",
        F.expr("try_cast(salary AS DECIMAL(12,2))")
    ) \
    .withColumn(
        "joining_date",
        F.expr(
            "try_to_timestamp(joining_date, 'yyyy-MM-dd')"
        ).cast("date")
    )

# 10. Validate employee records
email_pattern = r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"

validation_errors = [
    F.when(
        F.col("employee_id").isNull() |
        (F.col("employee_id") <= 0),
        "Invalid employee ID"
    ),

    F.when(
        F.col("first_name").isNull(),
        "Missing first name"
    ),

    F.when(
        F.col("last_name").isNull(),
        "Missing last name"
    ),

    F.when(
        F.col("email").isNull() |
        (~F.col("email").rlike(email_pattern)),
        "Missing or invalid email"
    ),

    F.when(
        F.col("department").isNull(),
        "Missing department"
    ),

    F.when(
        F.col("salary").isNull(),
        "Missing or invalid salary"
    ),

    F.when(
        F.col("salary") <= 0,
        "Salary must be positive"
    ),

    F.when(
        F.col("joining_date").isNull(),
        "Invalid joining date"
    )
]

clean_df = clean_df.withColumn(
    "validation_errors",
    F.concat_ws("; ", *validation_errors)
)

# 11. Separate valid and rejected records
valid_df = clean_df.filter(
    F.col("validation_errors") == ""
)

rejected_df = clean_df.filter(
    F.col("validation_errors") != ""
)

# 12. Display results
print("\n========== CLEANING RESULTS ==========")

print("Total unique records:", clean_df.count())
print("Valid records:", valid_df.count())
print("Rejected records:", rejected_df.count())

print("\nVALID EMPLOYEES:")
valid_df.select(*columns).show(truncate=False)

print("\nREJECTED EMPLOYEES:")
rejected_df.select(
    "employee_id_raw",
    "first_name",
    "email",
    "salary_raw",
    "joining_date_raw",
    "validation_errors"
).show(truncate=False)

# 13. Save cleaned records
valid_output = "/app/output/cleaned_employees"

valid_df.select(*columns) \
    .write.mode("overwrite") \
    .parquet(valid_output)

# 14. Save rejected records
rejected_output = "/app/output/rejected_employees"

rejected_df.write \
    .mode("overwrite") \
    .parquet(rejected_output)

print("\nCleaned employee data saved successfully!")
print("Rejected employee data saved successfully!")

spark.stop()
