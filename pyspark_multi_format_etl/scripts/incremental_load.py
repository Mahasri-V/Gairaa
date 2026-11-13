
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from psycopg2.extras import execute_values
from db_connection import get_connection


# ------------------------------------------------
# 1. CREATE SPARK SESSION
# ------------------------------------------------

spark = SparkSession.builder \
    .appName("Employee_Incremental_Load") \
    .getOrCreate()

print("\n========== INCREMENTAL ETL PIPELINE ==========\n")

connection = None

try:

    # ------------------------------------------------
    # 2. READ CLEANED PARQUET DATA
    # ------------------------------------------------

    parquet_path = "/app/output/cleaned_employees"

    df = spark.read.parquet(parquet_path)

    print("Cleaned Parquet records:", df.count())

    # ------------------------------------------------
    # 3. GENERATE SHA-256 ROW HASH
    # ------------------------------------------------

    hash_columns = [
        "first_name",
        "last_name",
        "email",
        "department",
        "salary",
        "city",
        "joining_date"
    ]

    df = df.withColumn(
        "row_hash",
        F.sha2(
            F.to_json(
                F.struct(*[F.col(c) for c in hash_columns])
            ),
            256
        )
    )

    print("\nSHA-256 hashes generated successfully!")

    # ------------------------------------------------
    # 4. PREPARE DATA FOR POSTGRESQL
    # ------------------------------------------------

    columns = [
        "employee_id",
        "first_name",
        "last_name",
        "email",
        "department",
        "salary",
        "city",
        "joining_date",
        "row_hash"
    ]

    df = df.select(*columns)

    # ------------------------------------------------
    # 5. CONNECT TO POSTGRESQL
    # ------------------------------------------------

    connection = get_connection()
    cursor = connection.cursor()

    print("\nConnected to PostgreSQL!")

    # ------------------------------------------------
    # 6. INSERT OR UPDATE RECORDS
    # ------------------------------------------------

    insert_query = """
        INSERT INTO employees (
            employee_id,
            first_name,
            last_name,
            email,
            department,
            salary,
            city,
            joining_date,
            row_hash
        )
        VALUES %s

        ON CONFLICT (employee_id)
        DO UPDATE SET
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            email = EXCLUDED.email,
            department = EXCLUDED.department,
            salary = EXCLUDED.salary,
            city = EXCLUDED.city,
            joining_date = EXCLUDED.joining_date,
            row_hash = EXCLUDED.row_hash,
            updated_at = CURRENT_TIMESTAMP

        WHERE employees.row_hash
        IS DISTINCT FROM EXCLUDED.row_hash;
    """

    inserted_count = 0
    updated_count = 0
    unchanged_count = 0

    batch_size = 500
    batch = []

    def load_batch(records):

        global inserted_count, updated_count, unchanged_count

        employee_ids = [record[0] for record in records]

        cursor.execute(
            """
            SELECT employee_id, row_hash
            FROM employees
            WHERE employee_id = ANY(%s)
            """,
            (employee_ids,)
        )

        existing_records = dict(cursor.fetchall())

        new_records = 0
        changed_records = 0
        same_records = 0

        for record in records:

            employee_id = record[0]
            new_hash = record[8]

            old_hash = existing_records.get(employee_id)

            if old_hash is None:
                new_records += 1

            elif old_hash != new_hash:
                changed_records += 1

            else:
                same_records += 1

        execute_values(
            cursor,
            insert_query,
            records
        )

        connection.commit()

        return new_records, changed_records, same_records

    for row in df.toLocalIterator():

        row_data = tuple(row[c] for c in columns)

        batch.append(row_data)

        if len(batch) >= batch_size:

            new, changed, same = load_batch(batch)

            inserted_count += new
            updated_count += changed
            unchanged_count += same

            batch = []

    if batch:

        new, changed, same = load_batch(batch)

        inserted_count += new
        updated_count += changed
        unchanged_count += same

    # ------------------------------------------------
    # 7. PRINT INCREMENTAL LOAD RESULTS
    # ------------------------------------------------

    print("\n========== LOAD SUMMARY ==========\n")

    print("New records inserted:", inserted_count)
    print("Existing records updated:", updated_count)
    print("Unchanged records skipped:", unchanged_count)

    cursor.execute(
        "SELECT COUNT(*) FROM employees"
    )

    total_records = cursor.fetchone()[0]

    print("Total PostgreSQL records:", total_records)

    print("\nIncremental loading completed successfully!")

    cursor.close()

except Exception as error:

    if connection is not None:
        connection.rollback()

    print("\nETL pipeline failed:", error)

    raise

finally:

    if connection is not None:
        connection.close()

    spark.stop()
