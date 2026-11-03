
import os
import sys
import csv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PYTHON_PATH = sys.executable

os.environ["PYSPARK_PYTHON"] = PYTHON_PATH
os.environ["PYSPARK_DRIVER_PYTHON"] = PYTHON_PATH



spark = (
    SparkSession.builder
    .appName("PySpark_Shuffle_and_Partitioning")
    .master("local[4]")
    .config("spark.pyspark.python", PYTHON_PATH)
    .config("spark.pyspark.driver.python", PYTHON_PATH)
    .config("spark.sql.shuffle.partitions", "4")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.ui.port", "4040")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("PYSPARK SHUFFLE AND PARTITIONING")

print("Spark Version:", spark.version)
print("Default Parallelism:", spark.sparkContext.defaultParallelism)


employee_data = [
    (1, "Arun", "IT", 60000),
    (2, "Bala", "HR", 35000),
    (3, "Charan", "IT", 55000),
    (4, "Divya", "Finance", 48000),
    (5, "Ezhil", "HR", 36000),
    (6, "Farhan", "Finance", 50000),
    (7, "Gokul", "IT", 65000),
    (8, "Harini", "HR", 37000),
    (9, "Irfan", "Finance", 51000),
    (10, "Janani", "IT", 65000),
    (11, "Kavin", "HR", 39000),
    (12, "Lavanya", "Finance", 51000)
]

columns = ["emp_id", "emp_name", "department", "salary"]


print("\nSTEP 1: CREATE RDD")

rdd = spark.sparkContext.parallelize(employee_data, 4)

print("Number of RDD Partitions:", rdd.getNumPartitions())


print("\n STEP 2: RDD PARTITION DETAILS ")

def inspect_partition(index, iterator):
    records = list(iterator)
    yield (index, records)

partition_details = rdd.mapPartitionsWithIndex(
    inspect_partition
).collect()

for partition_id, records in partition_details:
    print(f"\nPartition {partition_id}")
    for record in records:
        print(record)


print("\nSTEP 3: CREATE DATAFRAME")

df = spark.createDataFrame(employee_data, columns)

df.show()

print("DataFrame Partitions:", df.rdd.getNumPartitions())


print("\n STEP 4: REPARTITION BY DEPARTMENT")

df_department = df.repartition(4, "department")

print("Number of Partitions:", df_department.rdd.getNumPartitions())

df_department.withColumn(
    "partition_id",
    F.spark_partition_id()
).select(
    "emp_id",
    "department",
    "partition_id"
).orderBy(
    "emp_id"
).show()


print("\n STEP 5: GROUP BY DEPARTMENT")

grouped_df = df.groupBy("department").agg(
    F.count("emp_id").alias("employee_count"),
    F.sum("salary").alias("total_salary"),
    F.avg("salary").alias("average_salary")
)

grouped_df.show()

print("\nGROUP BY PHYSICAL PLAN")

grouped_df.explain()


print("\n STEP 6: JOIN OPERATION")

department_data = [
    ("IT", "Chennai"),
    ("HR", "Coimbatore"),
    ("Finance", "Bangalore")
]

department_columns = ["department", "location"]

department_df = spark.createDataFrame(
    department_data,
    department_columns
)

joined_df = df.join(
    department_df,
    on="department",
    how="inner"
)

joined_df.show()

print("\nJOIN PHYSICAL PLAN")

joined_df.explain()

print("\n STEP 7: DISTINCT DEPARTMENTS ")

distinct_df = df.select(
    "department"
).distinct()

distinct_df.show()

print("\nDISTINCT PHYSICAL PLAN")

distinct_df.explain()


print("\n STEP 8: REPARTITION FROM 4 TO 6")

df_repartition = df.repartition(6)

print("Before Repartition:", df.rdd.getNumPartitions())
print("After Repartition:", df_repartition.rdd.getNumPartitions())

df_repartition.withColumn(
    "partition_id",
    F.spark_partition_id()
).select(
    "emp_id",
    "department",
    "partition_id"
).orderBy(
    "partition_id",
    "emp_id"
).show()


print("\nSTEP 9: COALESCE FROM 4 TO 2 ")

df_coalesce = df.coalesce(2)

print("Before Coalesce:", df.rdd.getNumPartitions())
print("After Coalesce:", df_coalesce.rdd.getNumPartitions())

df_coalesce.withColumn(
    "partition_id",
    F.spark_partition_id()
).select(
    "emp_id",
    "department",
    "partition_id"
).orderBy(
    "partition_id",
    "emp_id"
).show()


print("\nSTEP 10: HASH PARTITIONING EXPERIMENT")

print("\n1. PARTITION BY DEPARTMENT")

df_department = df.repartition(4, "department")

df_department.withColumn(
    "partition_id",
    F.spark_partition_id()
).groupBy(
    "partition_id"
).count().orderBy(
    "partition_id"
).show()


print("\n2. PARTITION BY EMPLOYEE ID")

df_employee = df.repartition(4, "emp_id")

df_employee.withColumn(
    "partition_id",
    F.spark_partition_id()
).groupBy(
    "partition_id"
).count().orderBy(
    "partition_id"
).show()



print("\n3. ROUND-ROBIN PARTITIONING")

df_round_robin = df.repartition(4)

df_round_robin.withColumn(
    "partition_id",
    F.spark_partition_id()
).groupBy(
    "partition_id"
).count().orderBy(
    "partition_id"
).show()


print("\nSTEP 11: RECORD-LEVEL PARTITION DISTRIBUTION")
df_round_robin.withColumn(
    "partition_id",
    F.spark_partition_id()
).select(
    "emp_id",
    "department",
    "partition_id"
).orderBy(
    "partition_id",
    "emp_id"
).show(50)



print("\nSTEP 12: SAVE GROUPED DATA")
output_folder = "output/grouped_sales"
os.makedirs(output_folder, exist_ok=True)
output_file = os.path.join(
    output_folder,
    "grouped_sales.csv"
)

rows = grouped_df.orderBy("department").collect()
with open(output_file, "w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file)
    writer.writerow([
        "department",
        "employee_count",
        "total_salary",
        "average_salary"
    ])
    for row in rows:
        writer.writerow([
            row["department"],
            row["employee_count"],
            row["total_salary"],
            row["average_salary"]
        ])
print("Grouped data saved successfully!")
print("Output file:", output_file)


print("\n FINAL SHUFFLE PHYSICAL PLAN")
df.repartition(4, "department").groupBy(
    "department"
).count().explain()



print("EXECUTION COMPLETED")
print("\nSpark UI: http://localhost:4040")
input("\nPress ENTER after inspecting Spark UI...")
spark.stop()
print("\nSpark Session Stopped Successfully!")
