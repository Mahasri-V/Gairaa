# Sales Data Cleaning and PostgreSQL Data Pipeline

## 1. Project Title

**Sales Data Cleaning, Transformation, Analysis and PostgreSQL Integration Pipeline**

---

## 2. Project Overview

This project demonstrates a complete data processing pipeline using **Python, Pandas, and PostgreSQL**.

The main objective of this project is to take a raw sales dataset, inspect and clean the data, transform the dataset into a useful format, perform basic sales analysis using aggregation, and finally store the cleaned data in a PostgreSQL database.

The pipeline follows the complete data processing workflow:

**Raw Dataset → Data Inspection → Data Cleaning → Data Validation → Data Transformation → Data Aggregation → CSV Output → PostgreSQL → Data Verification**

The project is designed to demonstrate practical data engineering and data analysis concepts, including handling missing values, removing duplicate records, correcting invalid data, converting data types, performing calculations, connecting Python to PostgreSQL, inserting records using database transactions, and verifying the stored data.

---

# 3. Project Objectives

The main objectives of this project are:

1. Obtain and load a publicly accessible sales dataset.
2. Load the dataset into a Pandas DataFrame.
3. Inspect the raw dataset.
4. Identify missing values.
5. Identify duplicate records.
6. Handle missing values.
7. Remove duplicate records.
8. Convert columns to appropriate data types.
9. Identify and remove invalid records.
10. Transform the dataset by creating new calculated columns.
11. Perform sales aggregations.
12. Save the cleaned dataset as a CSV file.
13. Connect Python to PostgreSQL.
14. Create a PostgreSQL table based on the cleaned dataset.
15. Insert cleaned records into PostgreSQL.
16. Use database transactions with `COMMIT` and `ROLLBACK`.
17. Verify that the records were successfully inserted.
18. Document the complete pipeline and execution procedure.

---

# 4. Dataset Description

The dataset used in this project contains sales transaction information.

Each row represents a sales order and contains information about the order, customer, product category, region, quantity, pricing, discount, payment method, delivery time, customer rating, and revenue.

The dataset contains:

* **5,000 records**
* **12 original columns**

Additional columns are created during the transformation process.

After transformation, the dataset contains **18 columns**.

---

# 5. Dataset Columns

The original dataset contains the following columns:

| Column             | Description                                  |
| ------------------ | -------------------------------------------- |
| `order_id`         | Unique identifier for the sales order        |
| `order_date`       | Date on which the order was placed           |
| `customer_id`      | Identifier of the customer                   |
| `product_category` | Category of the purchased product            |
| `region`           | Geographical sales region                    |
| `quantity`         | Number of units purchased                    |
| `unit_price`       | Price of one unit of the product             |
| `discount`         | Discount percentage represented as a decimal |
| `payment_method`   | Payment method used for the order            |
| `delivery_days`    | Number of days required to deliver the order |
| `customer_rating`  | Customer rating for the order                |
| `revenue`          | Revenue generated from the order             |

---

# 6. Data Source

The dataset is a publicly accessible sales dataset.

**Source:** Kaggle / GitHub / Public Dataset

**Dataset URL:**
Add the exact URL of the dataset used for this project here.

> Note: The source URL should be replaced with the actual dataset source used for the project.

---

# 7. Raw Dataset Characteristics

Before cleaning, the raw dataset is inspected to understand its quality.

The following characteristics are checked:

* Number of records
* Number of columns
* Column names
* Data types
* Missing values
* Duplicate records
* Invalid values

The inspection is performed using Pandas functions such as:

```python
df.shape
df.columns
df.dtypes
df.isnull().sum()
df.duplicated().sum()
df.head()
```

---

# 8. Data Loading

The dataset is stored as a CSV file.

The Python program loads the dataset using Pandas:

```python
df = pd.read_csv("data/sales_data.csv")
```

After loading, the program displays a confirmation message indicating that the dataset was loaded successfully.

The first five records are displayed using:

```python
df.head()
```

This helps verify that the dataset has been loaded correctly.

---

# 9. Raw Data Inspection

The program performs several inspection operations.

## 9.1 Number of Records

The number of rows in the dataset is identified using:

```python
df.shape[0]
```

The current dataset contains:

**5,000 records**

---

## 9.2 Number of Columns

The number of columns is identified using:

```python
df.shape[1]
```

The original dataset contains:

**12 columns**

---

## 9.3 Column Names

The program displays all column names using:

```python
df.columns.tolist()
```

This helps identify the fields available for analysis and cleaning.

---

## 9.4 Data Types

The program checks the data type of every column using:

```python
df.dtypes
```

For example:

* Integer columns are identified as integer types.
* Price and rating columns are identified as floating-point values.
* Text columns are identified as object/string types.
* The order date is initially loaded as a text/object column and is later converted to datetime.

---

# 10. Missing Value Handling

Missing values are identified using:

```python
df.isnull().sum()
```

The number of missing values in every column is displayed.

Rows containing missing values are removed using:

```python
df = df.dropna()
```

After cleaning, the program checks the dataset again to confirm that missing values have been handled.

```python
df.isnull().sum()
```

The final cleaned dataset should contain no missing values.

---

# 11. Duplicate Record Handling

Duplicate records are identified using:

```python
df.duplicated().sum()
```

Duplicate rows are removed using:

```python
df = df.drop_duplicates()
```

The program checks the duplicate count before and after the cleaning process.

This ensures that the final dataset does not contain duplicate records.

---

# 12. Data Type Conversion

The raw dataset may contain columns that are not stored using the appropriate data types.

The project converts the columns into suitable formats.

## Date Conversion

The `order_date` column is converted into datetime format:

```python
df["order_date"] = pd.to_datetime(
    df["order_date"],
    errors="coerce"
)
```

Invalid dates are converted to missing values and later removed.

## Numeric Conversion

The following columns are converted to numeric values:

* `order_id`
* `customer_id`
* `quantity`
* `unit_price`
* `discount`
* `delivery_days`
* `customer_rating`
* `revenue`

This is performed using:

```python
pd.to_numeric()
```

Appropriate numeric data types are required for calculations and database storage.

---

# 13. Invalid Data Handling

The project also checks for logically invalid values.

The following validation rules are applied.

## Order Date

Invalid dates are removed.

```python
df = df.dropna(subset=["order_date"])
```

## Quantity

Quantity must be greater than zero.

```python
df = df[df["quantity"] > 0]
```

## Unit Price

Unit price must be greater than zero.

```python
df = df[df["unit_price"] > 0]
```

## Discount

Discount must be between 0 and 1.

```python
df = df[
    (df["discount"] >= 0) &
    (df["discount"] <= 1)
]
```

For example:

* `0.10` represents a 10% discount.
* `0.25` represents a 25% discount.

## Delivery Days

Delivery days must be greater than zero.

```python
df = df[df["delivery_days"] > 0]
```

## Customer Rating

Customer rating must be between 1 and 5.

```python
df = df[
    (df["customer_rating"] >= 1) &
    (df["customer_rating"] <= 5)
]
```

## Revenue

Revenue must be greater than zero.

```python
df = df[df["revenue"] > 0]
```

These validation rules ensure that invalid records do not enter the final dataset.

---

# 14. Data Transformation

After cleaning, additional columns are created to make the dataset more useful for analysis.

## Year

The year is extracted from `order_date`.

```python
df["year"] = df["order_date"].dt.year
```

## Month

The numerical month is extracted:

```python
df["month"] = df["order_date"].dt.month
```

## Month Name

The name of the month is created:

```python
df["month_name"] = df["order_date"].dt.month_name()
```

---

# 15. Gross Sales Calculation

Gross sales are calculated before applying the discount.

Formula:

**Gross Sales = Quantity × Unit Price**

Python implementation:

```python
df["gross_sales"] = (
    df["quantity"] * df["unit_price"]
)
```

This creates a new `gross_sales` column.

---

# 16. Discount Amount Calculation

The discount amount is calculated using:

**Discount Amount = Gross Sales × Discount**

Python implementation:

```python
df["discount_amount"] = (
    df["gross_sales"] * df["discount"]
)
```

This creates the `discount_amount` column.

---

# 17. Net Sales Calculation

Net sales are calculated after applying the discount.

Formula:

**Net Sales = Gross Sales − Discount Amount**

Python implementation:

```python
df["net_sales"] = (
    df["gross_sales"] - df["discount_amount"]
)
```

This creates the `net_sales` column.

---

# 18. Data Aggregation

After transformation, the project performs sales aggregation to obtain useful business insights.

## 18.1 Sales by Product Category

The dataset is grouped by `product_category`.

The following metrics are calculated:

* Total orders
* Total quantity
* Total revenue
* Average customer rating

Example:

```python
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
```

This provides a summary of sales performance for each product category.

---

# 19. Sales by Region

The dataset is also grouped by `region`.

The following metrics are calculated:

* Total orders
* Total quantity
* Total revenue

Example:

```python
region_summary = (
    df.groupby("region")
      .agg(
          total_orders=("order_id", "count"),
          total_quantity=("quantity", "sum"),
          total_revenue=("revenue", "sum")
      )
      .reset_index()
)
```

This provides a regional sales performance summary.

---

# 20. Final Data Quality Check

After cleaning and transformation, the program performs a final validation.

The following are checked:

```python
len(df)
len(df.columns)
df.isnull().sum()
df.duplicated().sum()
df.dtypes
```

The expected final results are:

* Missing values: **0**
* Duplicate records: **0**
* Original columns: **12**
* Final columns after transformation: **18**

The first five cleaned records are also displayed.

---

# 21. Cleaned Dataset Output

The cleaned and transformed dataset is saved as:

```text
data/cleaned_sales_data.csv
```

The file is generated using:

```python
df.to_csv(
    "data/cleaned_sales_data.csv",
    index=False
)
```

The cleaned file contains the original columns along with the newly created transformation columns.

---

# 22. PostgreSQL Integration

The cleaned dataset is stored in PostgreSQL for database processing and verification.

The Python program uses the `psycopg2` library to connect Python with PostgreSQL.

The connection contains:

* Host
* Database name
* Username
* Password
* Port

Example:

```python
conn = psycopg2.connect(
    host="localhost",
    database="myproject",
    user="postgres",
    password="YOUR_POSTGRES_PASSWORD",
    port="5432"
)
```

The PostgreSQL database used for this project is:

```text
myproject
```

---

# 23. PostgreSQL Table Creation

A table named `sales` is created in PostgreSQL.

The table contains the cleaned and transformed fields:

* order_id
* order_date
* customer_id
* product_category
* region
* quantity
* unit_price
* discount
* payment_method
* delivery_days
* customer_rating
* revenue
* year
* month
* month_name
* gross_sales
* discount_amount
* net_sales

The table is created using SQL executed from Python.

---

# 24. Database Transactions

Database transactions are used while inserting records.

The program uses:

```python
conn.commit()
```

to permanently save successful changes.

If an error occurs during insertion, the program uses:

```python
conn.rollback()
```

This prevents an incomplete transaction from being committed to the database.

The transaction process is:

**Start Transaction → Insert Records → Success → COMMIT**

or

**Start Transaction → Insert Records → Error → ROLLBACK**

This provides safer database operations.

---

# 25. Inserting Cleaned Records

Each cleaned record from the Pandas DataFrame is inserted into the PostgreSQL `sales` table.

The Python program uses a parameterized SQL `INSERT` query.

Parameterized queries are used instead of directly constructing SQL strings with data values.

This allows the cleaned Pandas data to be transferred into PostgreSQL safely.

---

# 26. PostgreSQL Data Verification

After inserting the records, the program verifies the database contents.

The total number of records is checked using:

```sql
SELECT COUNT(*) FROM sales;
```

The program also retrieves the first five records:

```sql
SELECT *
FROM sales
ORDER BY order_id
LIMIT 5;
```

This confirms that the data has been successfully stored in PostgreSQL.

---

# 27. Project Technologies

The project uses the following technologies:

### Python

Used to develop the complete data processing pipeline.

### Pandas

Used for:

* Loading CSV files
* Data inspection
* Cleaning
* Transformation
* Aggregation
* Saving cleaned data

### PostgreSQL

Used to store the cleaned and transformed dataset.

### psycopg2

Used to connect Python with PostgreSQL and execute SQL queries.

### CSV

Used as the input and cleaned-data file format.

---

# 28. Required Software

Before running the project, install:

1. Python 3.x
2. PostgreSQL
3. pgAdmin 4
4. Required Python libraries

Install the Python libraries using:

```bash
pip install pandas psycopg2-binary
```

---

# 29. Project Structure

The recommended project structure is:

```text
sales_data_project/
│
├── data/
│   ├── sales_data.csv
│   └── cleaned_sales_data.csv
│
├── source_code/
│   └── sales_pipeline.py
│
└── README.md
```

### `data/sales_data.csv`

Contains the original input dataset.

### `data/cleaned_sales_data.csv`

Contains the cleaned and transformed dataset generated by the Python program.

### `source_code/sales_pipeline.py`

Contains the complete Python data processing and PostgreSQL pipeline.

### `README.md`

Contains the project documentation, dataset description, cleaning process, database information, and execution instructions.

---

# 30. PostgreSQL Database Setup

Open PostgreSQL using pgAdmin 4.

Create a database named:

```text
myproject
```

The Python program connects to this database and creates the `sales` table automatically.

The PostgreSQL server should be running before executing the Python program.

---

# 31. How to Run the Project

## Step 1: Open the Project

Open the project folder in VS Code or another Python IDE.

---

## Step 2: Check the Dataset

Make sure the input dataset exists at:

```text
data/sales_data.csv
```

---

## Step 3: Install Required Libraries

Open the terminal and run:

```bash
pip install pandas psycopg2-binary
```

---

## Step 4: Create the PostgreSQL Database

Open pgAdmin 4 and create:

```text
Database: myproject
```

---

## Step 5: Configure PostgreSQL Connection

Open:

```text
source_code/sales_pipeline.py
```

Update the PostgreSQL connection details:

```python
conn = psycopg2.connect(
    host="localhost",
    database="myproject",
    user="postgres",
    password="YOUR_POSTGRES_PASSWORD",
    port="5432"
)
```

Replace `YOUR_POSTGRES_PASSWORD` with the PostgreSQL password configured on the local computer.

For security, the password should not be uploaded to GitHub or included in a publicly shared project.

---

## Step 6: Run the Python Pipeline

From the project directory, run:

```bash
python source_code/sales_pipeline.py
```

---

# 32. Expected Pipeline Output

The program displays information similar to:

```text
DATASET LOADED SUCCESSFULLY

NUMBER OF RECORDS
Records: 5000

NUMBER OF COLUMNS
Columns: 12

MISSING VALUES
...

DUPLICATE RECORDS
...

DATA CLEANING
...

DATA TYPE CONVERSION
...

REMOVING INVALID RECORDS
...

DATA TRANSFORMATION
...

SALES AGGREGATION
...

Cleaned dataset saved successfully!

Connected to PostgreSQL successfully!

Sales table created successfully!

Cleaned records inserted successfully!
Transaction committed successfully!

Total records in PostgreSQL: ...
```

The exact record count after cleaning depends on the contents of the raw dataset.

---

# 33. Important Data Quality Checks

The pipeline verifies that:

* The dataset can be loaded successfully.
* Missing values are identified and handled.
* Duplicate records are identified and removed.
* Dates are converted correctly.
* Numeric columns are converted correctly.
* Invalid quantities are removed.
* Invalid prices are removed.
* Invalid discounts are removed.
* Invalid delivery days are removed.
* Invalid customer ratings are removed.
* Invalid revenue values are removed.
* Calculated sales columns are generated.
* Aggregated sales summaries are produced.
* Cleaned data is saved to CSV.
* PostgreSQL connection works.
* Database table is created.
* Cleaned records are inserted.
* Transactions are committed successfully.
* Database records are verified.

---

# 34. Expected Final Deliverables

The following files should be submitted:

### 1. Source Code

```text
source_code/sales_pipeline.py
```

This contains the complete Python pipeline.

### 2. Input Source Dataset

```text
data/sales_data.csv
```

This is the original raw dataset used as input.

### 3. Cleaned Dataset

```text
data/cleaned_sales_data.csv
```

This is the output generated by the pipeline.

### 4. README Documentation

```text
README.md
```

This document explains the project, dataset, cleaning process, transformation, PostgreSQL integration, and execution instructions.

---

# 35. Complete Workflow

The complete workflow of the project is:

```text
                RAW CSV DATASET
                       |
                       ↓
                LOAD USING PANDAS
                       |
                       ↓
                DATA INSPECTION
                       |
                       ↓
              HANDLE MISSING VALUES
                       |
                       ↓
              REMOVE DUPLICATES
                       |
                       ↓
             CONVERT DATA TYPES
                       |
                       ↓
             VALIDATE DATA VALUES
                       |
                       ↓
             TRANSFORM THE DATA
                       |
                       ↓
             AGGREGATE SALES DATA
                       |
                       ↓
             SAVE CLEANED CSV
                       |
                       ↓
              CONNECT TO POSTGRESQL
                       |
                       ↓
                CREATE SALES TABLE
                       |
                       ↓
              INSERT CLEANED RECORDS
                       |
                       ↓
                  COMMIT
                       |
                       ↓
              VERIFY DATABASE DATA
                       |
                       ↓
                FINAL OUTPUT
```

---

# 36. Conclusion

This project demonstrates a complete end-to-end data processing pipeline.

The raw sales dataset is loaded into Pandas and inspected for data quality issues. Missing values and duplicate records are handled, invalid records are removed, and columns are converted into appropriate data types.

The cleaned data is then transformed by creating year, month, month name, gross sales, discount amount, and net sales columns. Sales aggregations are performed by product category and region to provide useful summaries.

Finally, the cleaned dataset is saved as a CSV file and loaded into PostgreSQL using Python and `psycopg2`. Database transactions are used to ensure that records are committed successfully or rolled back when an error occurs.

The project therefore demonstrates the complete process of:

**Data Loading → Data Cleaning → Data Transformation → Data Analysis → Database Integration → Transaction Management → Data Verification**

This pipeline provides a practical example of using Python and PostgreSQL together for real-world data processing and analysis.
