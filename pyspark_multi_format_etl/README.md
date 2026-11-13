# PySpark Multi-Format ETL Pipeline

A Dockerized PySpark ETL pipeline that processes employee data from CSV, Parquet, and JSON files, performs data cleaning and validation, and loads the processed records into PostgreSQL using hash-based incremental loading.

## Project Overview

This project demonstrates a multi-format data engineering workflow using Apache Spark and PostgreSQL.

The pipeline reads employee records from multiple file formats, standardizes the data, removes duplicate employee IDs, validates records, separates valid and rejected records, and loads valid records into PostgreSQL.

It uses SHA-256 row hashing to identify new, modified, and unchanged employee records during incremental loading.

## Architecture

```text
       CSV Input
           |
       JSON Input
           |
     Parquet Input
           |
           v
   PySpark Data Ingestion
           |
           v
    Data Cleaning
           |
   Duplicate Removal
           |
   Data Type Conversion
           |
           v
    Data Validation
           |
      +----+----+
      |         |
      v         v
  Valid Data  Rejected Data
      |
      v
  Parquet Output
      |
      v
  SHA-256 Hashing
      |
      v
 PostgreSQL Incremental Load
      |
      v
 New / Updated / Unchanged Records
```

## Technologies Used

- Python
- Apache PySpark 4.0.1
- PostgreSQL 16
- Docker
- Docker Compose
- psycopg2
- python-dotenv
- Parquet

## Project Structure

```text
pyspark-multi-format-etl/
│
├── config/
│
├── input/
│   ├── csv/
│   │   └── batch1.csv
│   ├── json/
│   │   └── batch1.json
│   └── parquet/
│       └── Generated during execution
│
├── scripts/
│   ├── clean_data.py
│   ├── db_connection.py
│   ├── generate_parquet.py
│   ├── incremental_load.py
│   └── validate_data.py
│
├── output/
│   ├── cleaned_employees/
│   └── rejected_employees/
│
├── logs/
│
├── .env.example
├── .gitignore
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── README.md
```

## Key Features

### 1. Multi-Format Data Ingestion
- Reads employee records from CSV.
- Reads nested or multiline JSON records supported by the input schema.
- Reads Parquet files generated from CSV input.

### 2. Data Cleaning
- Combines records from all three formats.
- Removes duplicate employee IDs.
- Trims unnecessary whitespace.
- Converts email addresses to lowercase.
- Standardizes department names.
- Converts employee IDs, salaries, and joining dates into appropriate data types.

### 3. Data Validation
- Checks employee IDs.
- Validates email addresses.
- Checks mandatory fields.
- Validates positive salary values.
- Checks joining dates.
- Separates valid and rejected records with validation error details.

### 4. PostgreSQL Integration
- Creates the employee table if it does not exist.
- Uses employee ID as the primary key.
- Stores employee information and SHA-256 row hashes.

### 5. Incremental Loading
- Generates SHA-256 hashes for employee records.
- Identifies new employee records.
- Detects modified employee records.
- Skips unchanged records.
- Uses PostgreSQL `ON CONFLICT` upsert logic.

## Prerequisites

Install the following:

- Docker Desktop
- Git
- GitHub account

Docker Desktop should be running before executing the pipeline.

## Installation and Setup

### 1. Clone the repository

```bash
git clone https://github.com/Mahasri-V/Gairaa.git
cd Gairaa/pyspark_multi_format_etl
```

### 2. Configure environment variables

Copy the example configuration:

**Windows PowerShell:**

```powershell
Copy-Item .env.example .env
```

Update `.env` with your local PostgreSQL credentials.

Never commit the `.env` file.

### 3. Build and start Docker services

```bash
docker compose up -d --build
```

Check running containers:

```bash
docker compose ps
```

### 4. Generate Parquet input

```bash
docker compose exec spark python scripts/generate_parquet.py
```

### 5. Execute data cleaning

```bash
docker compose exec spark python scripts/clean_data.py
```

### 6. Execute data validation

```bash
docker compose exec spark python scripts/validate_data.py
```

### 7. Create PostgreSQL employee table

```bash
docker compose exec spark python scripts/db_connection.py
```

### 8. Execute incremental loading

```bash
docker compose exec spark python scripts/incremental_load.py
```

## PostgreSQL Configuration

| Property | Value |
|---|---|
| Database | employee_db |
| Table | employees |
| Primary Key | employee_id |
| PostgreSQL Container Port | 5432 |
| Local Host Port | 5433 |

## Incremental Loading Logic

The pipeline generates a SHA-256 hash using employee attributes:

- First name
- Last name
- Email
- Department
- Salary
- City
- Joining date

The pipeline compares incoming hashes with existing database records.

| Record Type | Operation |
|---|---|
| New employee | INSERT |
| Modified employee | UPDATE |
| Unchanged employee | SKIP |

## Generated Outputs

The pipeline generates:

- `output/cleaned_employees/` – validated employee records in Parquet format.
- `output/rejected_employees/` – rejected employee records with validation errors.
- PostgreSQL `employees` table – incrementally loaded employee records.

These outputs are generated locally and are not committed to the repository.

## Stop Docker Services

```bash
docker compose down
```

To remove the database volume as well, use `docker compose down -v` only when you intentionally want to delete its stored data.

## Learning Outcomes

- Multi-format data ingestion using PySpark.
- Data cleaning and validation.
- Working with structured and semi-structured data.
- Parquet file generation.
- PostgreSQL database integration.
- Hash-based incremental loading.
- Docker containerization.
- ETL pipeline development.

## Author

**Mahasri V**

GitHub: [Mahasri-V](https://github.com/Mahasri-V)

---

*Built as a hands-on data engineering project using PySpark, PostgreSQL, and Docker.*
