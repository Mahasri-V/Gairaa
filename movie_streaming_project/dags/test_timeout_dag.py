
from datetime import datetime, timedelta
import time

from airflow.sdk import dag, task


@dag(
    dag_id="test_timeout_mechanism",
    description="Test Airflow task execution timeout",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["testing", "task-7"],
)
def test_timeout_mechanism():

    @task(
        execution_timeout=timedelta(seconds=10),
        retries=0,
    )
    def test_timeout():
        print("Timeout test started.")
        print("The task will sleep for 30 seconds.")

        time.sleep(30)

        print("Timeout test completed.")

    test_timeout()


test_timeout_mechanism()
