import re

from retail_pipeline.ingestion import CSV_FILES, DATASETS
from retail_pipeline.validation import DataQualityError


def load_csv_snapshot(
    connection,
    manifest: dict,
    s3_raw_key: str,
) -> dict:
    """Validate a staged CSV and atomically replace its raw snapshot."""

    dataset = manifest["dataset"]

    if dataset not in CSV_FILES:
        raise ValueError(
            f"Unsupported CSV dataset: {dataset}"
        )

    columns = DATASETS[dataset].required_columns

    # COPY maps fields by position, so header order must match.
    if manifest["columns"] != list(columns):
        raise DataQualityError(
            f"{dataset}: unexpected CSV columns or column order"
        )

    if manifest["source_filename"] != CSV_FILES[dataset]:
        raise DataQualityError(
            f"{dataset}: unexpected source filename"
        )

    expected_rows = manifest["row_count"]

    if (
        not isinstance(expected_rows, int)
        or isinstance(expected_rows, bool)
        or expected_rows <= 0
    ):
        raise DataQualityError(
            f"{dataset}: expected a positive integer row count"
        )

    # The key is embedded in SQL; restrict it to our object-key format.
    if not re.fullmatch(r"[A-Za-z0-9_./=-]+", s3_raw_key):
        raise ValueError("Unsupported S3 object key")

    table = f"RETAIL_DB.RAW.{dataset.upper()}"
    staging = f"RETAIL_DB.RAW._LOAD_{dataset.upper()}"

    column_sql = ", ".join(
        f'"{column.upper()}"'
        for column in columns
    )

    definition = ", ".join(
        f'"{column.upper()}" VARCHAR'
        for column in columns
    )

    cursor = connection.cursor()

    try:
        # Preserve text values, including leading zeros in identifiers.
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table}
            ({definition})
            """
        )

        cursor.execute(
            f"""
            CREATE TEMPORARY TABLE {staging}
            LIKE {table}
            """
        )

        # RAW_S3 must point to the root of the upload bucket.
        cursor.execute(
            f"""
            COPY INTO {staging}
            FROM @RETAIL_DB.RAW.RAW_S3
            FILES = ('{s3_raw_key}')
            FILE_FORMAT = (
                TYPE = CSV
                SKIP_HEADER = 1
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                EMPTY_FIELD_AS_NULL = TRUE
                ERROR_ON_COLUMN_COUNT_MISMATCH = TRUE
            )
            ON_ERROR = ABORT_STATEMENT
            FORCE = TRUE
            """
        )

        cursor.execute(
            f"SELECT COUNT(*) FROM {staging}"
        )
        loaded_rows = cursor.fetchone()[0]

        if loaded_rows != expected_rows:
            raise DataQualityError(
                f"{dataset}: expected {expected_rows} rows, "
                f"loaded {loaded_rows}"
            )

        # Keep the current snapshot until its replacement is validated.
        cursor.execute("BEGIN")

        try:
            cursor.execute(
                f"DELETE FROM {table}"
            )

            cursor.execute(
                f"""
                INSERT INTO {table} ({column_sql})
                SELECT {column_sql}
                FROM {staging}
                """
            )

            connection.commit()

        except Exception:
            connection.rollback()
            raise

        return {
            "dataset": dataset,
            "batch_id": manifest["batch_id"],
            "source_rows": loaded_rows,
            "status": "success",
        }

    finally:
        cursor.close()