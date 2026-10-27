import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="retail_platform_smoke_test",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["retail", "training"],
)
def retail_platform_smoke_test():

    @task
    def extract() -> int:
        print("Simulating extraction...")
        return 100

    @task
    def validate(record_count: int) -> int:
        print(f"Validating {record_count} records...")

        if record_count <= 0:
            raise ValueError("No records were extracted")

        print("Validation passed!")
        return 101

    @task
    def publish(validate_response: int) -> None:
        print("Data successfully published!")

    publish(validate(extract()))


retail_platform_smoke_test()