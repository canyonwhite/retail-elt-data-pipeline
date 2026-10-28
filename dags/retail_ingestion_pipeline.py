import json
import os
from datetime import timedelta
from pathlib import Path

import boto3
import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import dag, task

from retail_pipeline.csv_warehouse import load_csv_snapshot
from retail_pipeline.exchange_rates import (
    create_http_session,
    prepare_exchange_rate_batch,
)
from retail_pipeline.ingestion import (
    CSV_FILES,
    DATASETS,
    inspect_csv,
    save_manifest,
)
from retail_pipeline.storage import (
    upload_batch,
    upload_exchange_rate_batch,
)
from retail_pipeline.validation import DataQualityError
from retail_pipeline.warehouse import (
    create_snowflake_connection,
    load_exchange_rate_batch,
    load_orders_batch,
)

DATA_DIR = Path("/opt/airflow/data")

ORDERS_BATCH_ID = "0002"
FX_BATCH_ID = "0001"
CSV_BATCH_ID = "0001"


def get_s3_client():
    session = boto3.Session(
        profile_name=os.getenv("AWS_PROFILE") or None
    )
    return session.client("s3")


@dag(
    dag_id="retail_ingestion_pipeline",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["retail", "ingestion", "elt"],
)
def retail_ingestion_pipeline():

    @task
    def prepare_orders() -> dict:
        source = DATA_DIR / "raw" / "olist_orders_dataset.csv"

        manifest = inspect_csv(
            source,
            DATASETS["orders"],
            ORDERS_BATCH_ID,
        )

        manifest_path = (
            DATA_DIR
            / "manifests"
            / "orders"
            / f"batch_id={ORDERS_BATCH_ID}.json"
        )

        save_manifest(manifest, manifest_path)

        return {
            "source_path": str(source),
            "manifest_path": str(manifest_path),
        }

    @task
    def upload_orders(batch: dict) -> dict:
        result = upload_batch(
            s3_client=get_s3_client(),
            bucket=os.environ["S3_BUCKET_NAME"],
            source_path=Path(batch["source_path"]),
            manifest_path=Path(batch["manifest_path"]),
        )

        return {
            **batch,
            "s3_raw_key": result["raw_key"],
        }

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
    )
    def load_orders(batch: dict) -> dict:
        from airflow.sdk import get_current_context

        from retail_pipeline.logging_utils import log_event

        context = get_current_context()

        manifest = json.loads(
            Path(batch["manifest_path"]).read_text(
                encoding="utf-8"
            )
        )

        log_event(
            "orders_load_started",
            dataset="orders",
            batch_id=manifest["batch_id"],
            dag_run_id=context["run_id"],
        )

        connection = create_snowflake_connection()

        try:
            try:
                result = load_orders_batch(
                    connection=connection,
                    s3_client=get_s3_client(),
                    bucket=os.environ["S3_BUCKET_NAME"],
                    manifest=manifest,
                )
            except DataQualityError as exc:
                raise AirflowFailException(
                    f"Orders batch failed validation: {exc}"
                ) from exc

            log_event(
                "orders_load_completed",
                dataset="orders",
                batch_id=manifest["batch_id"],
                dag_run_id=context["run_id"],
                source_rows=result["source_rows"],
                inserted_rows=result["inserted_rows"],
                status=result["status"],
            )

            return result

        finally:
            connection.close()

    @task
    def prepare_csv(dataset: str) -> dict:
        source = DATA_DIR / "raw" / CSV_FILES[dataset]

        manifest = inspect_csv(
            source,
            DATASETS[dataset],
            CSV_BATCH_ID,
        )

        manifest_path = (
            DATA_DIR
            / "manifests"
            / dataset
            / f"batch_id={CSV_BATCH_ID}.json"
        )

        save_manifest(manifest, manifest_path)

        return {
            "source_path": str(source),
            "manifest_path": str(manifest_path),
        }

    @task
    def upload_csv(batch: dict) -> dict:
        result = upload_batch(
            s3_client=get_s3_client(),
            bucket=os.environ["S3_BUCKET_NAME"],
            source_path=Path(batch["source_path"]),
            manifest_path=Path(batch["manifest_path"]),
        )

        return {
            **batch,
            "s3_raw_key": result["raw_key"],
        }

    @task(
        retries=2,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
    )
    def load_csv(batch: dict) -> dict:
        manifest = json.loads(
            Path(batch["manifest_path"]).read_text(
                encoding="utf-8"
            )
        )

        connection = create_snowflake_connection()

        try:
            try:
                return load_csv_snapshot(
                    connection,
                    manifest,
                    batch["s3_raw_key"],
                )
            except DataQualityError as exc:
                raise AirflowFailException(
                    f"{manifest['dataset']} batch failed "
                    f"validation: {exc}"
                ) from exc

        finally:
            connection.close()

    @task
    def prepare_fx() -> dict:
        with create_http_session() as session:
            raw_path, manifest_path, _ = (
                prepare_exchange_rate_batch(
                    session=session,
                    start_date="2018-01-01",
                    end_date="2018-01-10",
                    batch_id=FX_BATCH_ID,
                    data_dir=DATA_DIR,
                )
            )

        return {
            "raw_path": str(raw_path),
            "manifest_path": str(manifest_path),
        }

    @task
    def upload_fx(batch: dict) -> dict:
        result = upload_exchange_rate_batch(
            s3_client=get_s3_client(),
            bucket=os.environ["S3_BUCKET_NAME"],
            raw_path=Path(batch["raw_path"]),
            manifest_path=Path(batch["manifest_path"]),
        )

        return {
            **batch,
            "s3_raw_key": result["raw_key"],
        }

    @task
    def load_fx(batch: dict) -> dict:
        manifest = json.loads(
            Path(batch["manifest_path"]).read_text(
                encoding="utf-8"
            )
        )

        connection = create_snowflake_connection()

        try:
            return load_exchange_rate_batch(
                connection,
                manifest,
            )
        finally:
            connection.close()

    @task
    def validate_pipeline(
        orders_result: dict,
        fx_result: dict,
        csv_results: list[dict],
    ) -> None:
        allowed_statuses = {
            "success",
            "already_processed",
        }

        if orders_result["status"] not in allowed_statuses:
            raise ValueError("Orders ingestion failed")

        if fx_result["status"] not in allowed_statuses:
            raise ValueError("FX ingestion failed")

        for result in csv_results:
            if result["status"] not in allowed_statuses:
                raise ValueError(
                    f"{result['dataset']} ingestion failed"
                )

        connection = create_snowflake_connection()

        try:
            cursor = connection.cursor()

            try:
                cursor.execute(
                    """
                    SELECT
                        COUNT(*) - COUNT(DISTINCT ORDER_ID)
                    FROM RETAIL_DB.RAW.ORDERS
                    """
                )

                duplicate_or_null_count = cursor.fetchone()[0]

                if duplicate_or_null_count != 0:
                    raise ValueError(
                        "RAW orders contain duplicate "
                        "or null order IDs"
                    )
            finally:
                cursor.close()

        finally:
            connection.close()

        print("All ingestion branches completed.")
        print("Orders status:", orders_result["status"])
        print("FX status:", fx_result["status"])

        for result in csv_results:
            print(
                f"{result['dataset']} status:",
                result["status"],
            )

    orders_result = load_orders(
        upload_orders(
            prepare_orders()
        )
    )

    fx_result = load_fx(
        upload_fx(
            prepare_fx()
        )
    )

    csv_results = []

    for dataset in sorted(CSV_FILES):
        prepared = prepare_csv.override(
            task_id=f"prepare_{dataset}"
        )(dataset)

        uploaded = upload_csv.override(
            task_id=f"upload_{dataset}"
        )(prepared)

        loaded = load_csv.override(
            task_id=f"load_{dataset}"
        )(uploaded)

        csv_results.append(loaded)

    validate_pipeline(
        orders_result,
        fx_result,
        csv_results,
    )


retail_ingestion_pipeline()