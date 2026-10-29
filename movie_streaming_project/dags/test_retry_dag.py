
from datetime import datetime, timedelta

from airflow.sdk import dag, task, get_current_context


@dag(
    dag_id="test_retry_mechanism",
    description="Test Airflow retries and error handling",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["testing", "task-7"],
)
def test_retry_mechanism():

    @task(
        retries=2,
        retry_delay=timedelta(seconds=10),
    )
    def test_retry():

        context = get_current_context()
        task_instance = context["ti"]

        logger_message = (
            f"Running attempt number: {task_instance.try_number}"
        )
        print(logger_message)

        # Intentionally fail the first attempt only.
        if task_instance.try_number == 1:
            print("Intentional test failure: retry should happen.")
            raise RuntimeError("Testing Airflow retry mechanism")

        print("Retry test succeeded!")

    test_retry()


test_retry_mechanism()
