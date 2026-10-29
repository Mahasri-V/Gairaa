FROM apache/airflow:3.3.2

USER airflow

RUN pip install --no-cache-dir \
    pandas \
    "psycopg[binary]" \
    python-dotenv \
    pytest
