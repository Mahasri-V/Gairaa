
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("Employee_Data_Validation") \
    .getOrCreate()

print("\n========== EMPLOYEE DATA VALIDATION ==========\n")

# Read cleaned and rejected Parquet data
cleaned_df = spark.read.parquet(
    "/app/output/cleaned_employees"
)

rejected_df = spark.read.parquet(
    "/app/output/rejected_employees"
)

# Total records
total_records = cleaned_df.count()
rejected_records = rejected_df.count()

print("Cleaned records:", total_records)
print("Rejected records:", rejected_records)

# 1. Check duplicate employee IDs
duplicate_count = (
    total_records -
    cleaned_df.select("employee_id").distinct().count()
)

# 2. Check missing mandatory fields
missing_count = cleaned_df.filter(
    F.col("employee_id").isNull() |
    F.col("first_name").isNull() |
    F.col("last_name").isNull() |
    F.col("email").isNull() |
    F.col("department").isNull() |
    F.col("salary").isNull() |
    F.col("city").isNull() |
    F.col("joining_date").isNull()
).count()

# 3. Check invalid email addresses
email_pattern = (
    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
)

invalid_email_count = cleaned_df.filter(
    ~F.col("email").rlike(email_pattern)
).count()

# 4. Check non-positive salaries
invalid_salary_count = cleaned_df.filter(
    F.col("salary").isNull() |
    (F.col("salary") <= 0)
).count()

# 5. Check invalid employee IDs
invalid_id_count = cleaned_df.filter(
    F.col("employee_id").isNull() |
    (F.col("employee_id") <= 0)
).count()

# 6. Check invalid joining dates
invalid_date_count = cleaned_df.filter(
    F.col("joining_date").isNull()
).count()

# Display validation report
print("\n========== VALIDATION REPORT ==========\n")

checks = {
    "Duplicate employee IDs": duplicate_count,
    "Missing mandatory fields": missing_count,
    "Invalid email addresses": invalid_email_count,
    "Invalid salaries": invalid_salary_count,
    "Invalid employee IDs": invalid_id_count,
    "Invalid joining dates": invalid_date_count
}

for check_name, error_count in checks.items():
    status = "PASS" if error_count == 0 else "FAIL"

    print(
        f"{check_name}: {error_count} | {status}"
    )

# Final validation result
failed_checks = sum(
    1 for error_count in checks.values()
    if error_count > 0
)

print("\n========== FINAL RESULT ==========")

if failed_checks == 0:
    print("ALL VALIDATION CHECKS PASSED!")
else:
    print("VALIDATION FAILED!")
    raise ValueError(
        f"{failed_checks} validation checks failed."
    )

print("\nCleaned records:", total_records)
print("Rejected records:", rejected_records)

spark.stop()
