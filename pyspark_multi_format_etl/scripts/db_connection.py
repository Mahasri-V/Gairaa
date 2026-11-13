
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()


def get_connection():
    try:
        connection = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "postgres"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            database=os.getenv("POSTGRES_DB"),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD")
        )

        print("PostgreSQL connection successful!")
        return connection

    except Exception as error:
        print("Database connection failed:", error)
        raise


def create_employee_table():
    connection = get_connection()

    try:
        cursor = connection.cursor()

        create_table_query = """
        CREATE TABLE IF NOT EXISTS employees (
            employee_id INTEGER PRIMARY KEY,
            first_name VARCHAR(100) NOT NULL,
            last_name VARCHAR(100) NOT NULL,
            email VARCHAR(255) NOT NULL,
            department VARCHAR(100) NOT NULL,
            salary NUMERIC(12, 2) NOT NULL,
            city VARCHAR(100) NOT NULL,
            joining_date DATE NOT NULL,
            row_hash VARCHAR(64) NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """

        cursor.execute(create_table_query)
        connection.commit()
        print("Employees table created successfully!")

        cursor.close()

    except Exception as error:
        connection.rollback()
        print("Table creation failed:", error)
        raise

    finally:
        connection.close()


if __name__ == "__main__":
    create_employee_table()
