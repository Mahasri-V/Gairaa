import pandas as pd
import os
import time
df = pd.read_csv("data/sales_data1.csv")
print("\nFirst 5 rows:")
print(df.head())

print("\nShape:")
print(df.shape)

print("\nColumns:")
print(df.columns.tolist())

print("\nData Types:")
print(df.dtypes)

print("\nDataset Information:")
df.info()

df["order_date"] = pd.to_datetime(
    df["order_date"],
    format="%d-%m-%Y",
    errors="coerce"
)
# Fill invalid/missing dates with the most common date
df["order_date"] = df["order_date"].fillna(df["order_date"].mode()[0])
print("\nOrder Date Data Type:")
print(df["order_date"].dtype)
print("\nFirst 5 Order Dates:")
print(df["order_date"].head())


df["discount"] = pd.to_numeric(df["discount"], errors="coerce")
df["discount"] = df["discount"].fillna(df["discount"].median())
df["discount"] = df["discount"].astype(int)
print("Discount Data Type:")
print(df["discount"].dtype)

print("\nMissing Values:")
print(df.isnull().sum())

print("\nTotal Missing Values:")
print(df.isnull().sum().sum())

print("\nDuplicate Rows:")
print(df.duplicated().sum())

df = df.dropna(how="all")

df = df.drop_duplicates()

df.columns = df.columns.str.strip()

text_columns = df.select_dtypes(include="object").columns

for column in text_columns:
    df[column] = df[column].str.strip()

print("\nFilling missing values...")
numeric_columns = df.select_dtypes(include="number").columns
for column in numeric_columns:
    df[column] = df[column].fillna(df[column].median())
text_columns = df.select_dtypes(include="object").columns
for column in text_columns:
    df[column] = df[column].fillna(df[column].mode()[0])
df = df.drop_duplicates()

print("\nMissing Values After Cleaning:")
print(df.isnull().sum())

print("\nTotal Missing Values:")
print(df.isnull().sum().sum())

print("\nDuplicate Rows After Cleaning:")
print(df.duplicated().sum())

print("\nData Types After Cleaning:")
print(df.dtypes)

print("\nNegative Numeric Values:")
numeric_columns = df.select_dtypes(include="number").columns
for column in numeric_columns:
    negative_count = (df[column] < 0).sum()
    print(column, ":", negative_count)


print("\nFinal Shape:")
print(df.shape)
print("\nFinal Data Types:")
print(df.dtypes)


output_file = "data/sales_data_cleaned.csv"
df.to_csv(output_file, index=False)
print("Cleaned dataset shape:", df.shape)
print("Cleaned CSV saved as:", output_file)
print("\nFirst 5 rows of cleaned data:")
print(df.head())

csv_file = "data/sales_data_cleaned.csv"
parquet_file = "data/sales_data_cleaned.parquet"
df.to_csv(csv_file, index=False)
df.to_parquet(
    parquet_file,
    index=False,
    engine="pyarrow"
)
print("Cleaned CSV saved as:", csv_file)
print("Parquet saved as:", parquet_file)
# File sizes in KB
csv_size = os.path.getsize(csv_file) / 1024
parquet_size = os.path.getsize(parquet_file) / 1024

print(f"CSV Size: {csv_size:.2f} KB")
print(f"Parquet Size: {parquet_size:.2f} KB")
if parquet_size < csv_size:
    reduction = ((csv_size - parquet_size) / csv_size) * 100
    print(f"Parquet is {reduction:.2f}% smaller than CSV")
else:
    print("Parquet is not smaller for this dataset.")

start_time = time.perf_counter()
csv_data = pd.read_csv(csv_file)
csv_time = time.perf_counter() - start_time

start_time = time.perf_counter()
parquet_data = pd.read_parquet(parquet_file)
parquet_time = time.perf_counter() - start_time

print(f"CSV Read Time: {csv_time:.6f} seconds")
print(f"Parquet Read Time: {parquet_time:.6f} seconds")

print("CSV Shape:", csv_data.shape)
print("Parquet Shape:", parquet_data.shape)
print("CSV Columns:", list(csv_data.columns))
print("Parquet Columns:", list(parquet_data.columns))