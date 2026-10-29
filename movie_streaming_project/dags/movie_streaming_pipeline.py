
from datetime import datetime, timedelta
import logging
import subprocess
import sys

from airflow.sdk import dag, task

PROJECT_DIR = "/opt/airflow"
SCRIPTS_DIR = f"{PROJECT_DIR}/scripts"

logger = logging.getLogger(__name__)

default_args = {
    "owner": "mahasri",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}


def run_script(script_name):
    """Execute a project script with logging and error handling."""

    script_path = f"{SCRIPTS_DIR}/{script_name}"

    logger.info("Starting script: %s", script_name)

    try:
        result = subprocess.run(
            [sys.executable, script_path],
            cwd=PROJECT_DIR,
            check=True,
            capture_output=True,
            text=True,
        )

        if result.stdout:
            logger.info(
                "Output from %s:\n%s",
                script_name,
                result.stdout,
            )

        if result.stderr:
            logger.warning(
                "Standard error from %s:\n%s",
                script_name,
                result.stderr,
            )

        logger.info("Script completed successfully: %s", script_name)

    except subprocess.CalledProcessError as error:
        logger.error(
            "Script failed: %s. Exit code: %s",
            script_name,
            error.returncode,
        )

        if error.stdout:
            logger.error("Script output:\n%s", error.stdout)

        if error.stderr:
            logger.error("Script error:\n%s", error.stderr)

        # Re-raise the exception so Airflow marks the task as failed
        # and applies the configured retry settings.
        raise

@dag(
    dag_id="movie_streaming_pipeline",
    description="Inspect, clean, validate, and build movie streaming analytics",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["movie-streaming", "data-engineering"],
)
def movie_streaming_pipeline():

    @task
    def inspect_raw_data():
        logger.info("TASK 1: Inspecting raw movie streaming data")
        run_script("inspect_data.py")
        logger.info("TASK 1 completed successfully")


    @task
    def clean_data():
        logger.info("TASK 2: Cleaning raw data")
        run_script("clean_data.py")
        logger.info("TASK 2 completed successfully")

  
    @task
    def validate_processed_data():
        logger.info("TASK 3: Validating processed data")
        run_script("validate_data.py")
        logger.info("TASK 3 completed successfully")


    @task
    def build_gold_analytics():
        logger.info("TASK 4: Building Gold analytics tables")
        run_script("build_gold.py")
        logger.info("TASK 4 completed successfully")


    inspection = inspect_raw_data()
    cleaning = clean_data()
    validation = validate_processed_data()
    gold = build_gold_analytics()

    inspection >> cleaning >> validation >> gold

movie_streaming_pipeline()
