import pandas as pd
import psycopg2


# ============================================================
# 1. FETCH / LOAD DATASET
# ============================================================

file_path = "data/sales_data.csv"

df = pd.read_csv(file_path)

print("=" * 60)
print("DATASET LOADED SUCCESSFULLY")
print("=" * 60)


# ============================================================
# 2. INSPECT RAW DATA
# ============================================================

print("\nFIRST 5 RECORDS")
print("-" * 60)
print(df.head())

print("\nNUMBER OF RECORDS")
print("-" * 60)
print("Records:", df.shape[0])

print("\nNUMBER OF COLUMNS")
print("-" * 60)
print("Columns:", df.shape[1])

print("\nCOLUMN NAMES")
print("-" * 60)
print(df.columns.tolist())

print("\nDATA TYPES")
print("-" * 60)
print(df.dtypes)

print("\nMISSING VALUES")
print("-" * 60)
print(df.isnull().sum())

print("\nDUPLICATE RECORDS")
print("-" * 60)
print("Duplicates:", df.duplicated().sum())


# ============================================================
# 3. CLEAN DATA
# ============================================================

print("\n" + "=" * 60)
print("DATA CLEANING")
print("=" * 60)


# Handle missing values
print("\nMissing values before cleaning:")
print(df.isnull().sum())

df = df.dropna()

print("\nMissing values after cleaning:")
print(df.isnull().sum())


# Remove duplicate records
print("\nDuplicate records before cleaning:")
print(df.duplicated().sum())

df = df.drop_duplicates()

print("Duplicate records after cleaning:")
print(df.duplicated().sum())


# ============================================================
# 4. CONVERT DATA TYPES
# ============================================================

print("\n" + "=" * 60)
print("DATA TYPE CONVERSION")
print("=" * 60)


# Convert date
df["order_date"] = pd.to_datetime(
    df["order_date"],
    errors="coerce"
)


# Convert numeric columns
numeric_columns = [
    "order_id",
    "customer_id",
    "quantity",
    "unit_price",
    "discount",
    "delivery_days",
    "customer_rating",
    "revenue"
]

for column in numeric_columns:
    df[column] = pd.to_numeric(
        df[column],
        errors="coerce"
    )


print("\nData types after conversion:")
print(df.dtypes)


# ============================================================
# 5. HANDLE INVALID RECORDS
# ============================================================

print("\n" + "=" * 60)
print("REMOVING INVALID RECORDS")
print("=" * 60)


# Remove invalid dates
df = df.dropna(subset=["order_date"])


# Quantity must be greater than 0
df = df[df["quantity"] > 0]


# Unit price must be greater than 0
df = df[df["unit_price"] > 0]


# Discount must be between 0 and 1
df = df[
    (df["discount"] >= 0) &
    (df["discount"] <= 1)
]


# Delivery days must be greater than 0
df = df[df["delivery_days"] > 0]


# Customer rating must be between 1 and 5
df = df[
    (df["customer_rating"] >= 1) &
    (df["customer_rating"] <= 5)
]


# Revenue must be greater than 0
df = df[df["revenue"] > 0]


# Convert integer columns after removing invalid values
df["order_id"] = df["order_id"].astype("int64")
df["customer_id"] = df["customer_id"].astype("int64")
df["quantity"] = df["quantity"].astype("int64")
df["delivery_days"] = df["delivery_days"].astype("int64")


print("Invalid records removed successfully.")


# ============================================================
# 6. TRANSFORM DATA
# ============================================================

print("\n" + "=" * 60)
print("DATA TRANSFORMATION")
print("=" * 60)


# Create year
df["year"] = df["order_date"].dt.year


# Create month number
df["month"] = df["order_date"].dt.month


# Create month name
df["month_name"] = df["order_date"].dt.month_name()


# Calculate gross sales
df["gross_sales"] = (
    df["quantity"] * df["unit_price"]
)


# Calculate discount amount
df["discount_amount"] = (
    df["gross_sales"] * df["discount"]
)


# Calculate net sales
df["net_sales"] = (
    df["gross_sales"] - df["discount_amount"]
)


print("\nTransformed data:")
print(df.head())


# ============================================================
# 7. AGGREGATE DATA
# ============================================================

print("\n" + "=" * 60)
print("SALES AGGREGATION")
print("=" * 60)


# Sales by product category
category_summary = (
    df.groupby("product_category")
      .agg(
          total_orders=("order_id", "count"),
          total_quantity=("quantity", "sum"),
          total_revenue=("revenue", "sum"),
          average_rating=("customer_rating", "mean")
      )
      .reset_index()
)


print("\nSales by Product Category:")
print(category_summary)


# Sales by region
region_summary = (
    df.groupby("region")
      .agg(
          total_orders=("order_id", "count"),
          total_quantity=("quantity", "sum"),
          total_revenue=("revenue", "sum")
      )
      .reset_index()
)


print("\nSales by Region:")
print(region_summary)


# ============================================================
# 8. FINAL DATA CHECK
# ============================================================

print("\n" + "=" * 60)
print("FINAL CLEAN DATA")
print("=" * 60)

print("Final number of records:", len(df))
print("Final number of columns:", len(df.columns))

print("\nMissing values:")
print(df.isnull().sum())

print("\nDuplicate records:")
print(df.duplicated().sum())

print("\nFinal data types:")
print(df.dtypes)

print("\nFirst 5 cleaned records:")
print(df.head())


# ============================================================
# 9. SAVE CLEANED DATASET
# ============================================================

output_file = "data/cleaned_sales_data.csv"

df.to_csv(
    output_file,
    index=False
)

print("\nCleaned dataset saved successfully!")
print("File:", output_file)


# ============================================================
# 10. CONNECT TO POSTGRESQL
# ============================================================

print("\n" + "=" * 60)
print("POSTGRESQL CONNECTION")
print("=" * 60)


conn = psycopg2.connect(
    host="localhost",
    database="myproject",
    user="postgres",
    password="mahasri",
    port="5432"
)

cursor = conn.cursor()

print("Connected to PostgreSQL successfully!")


# ============================================================
# 11. CREATE TABLE
# ============================================================

print("\n" + "=" * 60)
print("CREATING SALES TABLE")
print("=" * 60)


cursor.execute("""
    CREATE TABLE IF NOT EXISTS sales (
        order_id BIGINT,
        order_date DATE,
        customer_id BIGINT,
        product_category VARCHAR(100),
        region VARCHAR(50),
        quantity INTEGER,
        unit_price NUMERIC(10,2),
        discount NUMERIC(5,2),
        payment_method VARCHAR(50),
        delivery_days INTEGER,
        customer_rating NUMERIC(3,1),
        revenue NUMERIC(12,2),
        year INTEGER,
        month INTEGER,
        month_name VARCHAR(20),
        gross_sales NUMERIC(12,2),
        discount_amount NUMERIC(12,2),
        net_sales NUMERIC(12,2)
    )
""")

conn.commit()

print("Sales table created successfully!")


# ============================================================
# 12. INSERT CLEANED RECORDS WITH TRANSACTION
# ============================================================

print("\n" + "=" * 60)
print("INSERTING CLEANED RECORDS")
print("=" * 60)


try:

    insert_query = """
        INSERT INTO sales (
            order_id,
            order_date,
            customer_id,
            product_category,
            region,
            quantity,
            unit_price,
            discount,
            payment_method,
            delivery_days,
            customer_rating,
            revenue,
            year,
            month,
            month_name,
            gross_sales,
            discount_amount,
            net_sales
        )
        VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
    """


    for _, row in df.iterrows():

        cursor.execute(
            insert_query,
            (
                int(row["order_id"]),
                row["order_date"].date(),
                int(row["customer_id"]),
                str(row["product_category"]),
                str(row["region"]),
                int(row["quantity"]),
                float(row["unit_price"]),
                float(row["discount"]),
                str(row["payment_method"]),
                int(row["delivery_days"]),
                float(row["customer_rating"]),
                float(row["revenue"]),
                int(row["year"]),
                int(row["month"]),
                str(row["month_name"]),
                float(row["gross_sales"]),
                float(row["discount_amount"]),
                float(row["net_sales"])
            )
        )


    # Commit transaction
    conn.commit()

    print("Cleaned records inserted successfully!")
    print("Transaction committed successfully!")
    print("Records inserted:", len(df))


except Exception as e:

    # Rollback if error occurs
    conn.rollback()

    print("Error occurred!")
    print("Transaction rolled back!")
    print(e)


# ============================================================
# 13. VERIFY DATA IN POSTGRESQL
# ============================================================

print("\n" + "=" * 60)
print("VERIFYING POSTGRESQL DATA")
print("=" * 60)


# Check total records
cursor.execute("""
    SELECT COUNT(*)
    FROM sales
""")

count = cursor.fetchone()[0]

print("Total records in PostgreSQL:", count)


# Display first 5 records
cursor.execute("""
    SELECT *
    FROM sales
    ORDER BY order_id
    LIMIT 5
""")

rows = cursor.fetchall()


print("\nFirst 5 records from PostgreSQL:")

for row in rows:
    print(row)


# ============================================================
# 14. CLOSE DATABASE CONNECTION
# ============================================================

cursor.close()
conn.close()

print("\n" + "=" * 60)
print("PROJECT COMPLETED SUCCESSFULLY")
print("=" * 60)