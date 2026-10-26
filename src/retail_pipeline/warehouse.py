import os
import re

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()


def create_snowflake_connection():
    """Create a Snowflake connection using local configuration."""

    config = {
        "account": os.environ["SNOWFLAKE_ACCOUNT"],
        "user": os.environ["SNOWFLAKE_USER"],
        "authenticator": os.getenv("SNOWFLAKE_AUTHENTICATOR"),
        "private_key_file": os.getenv("SNOWFLAKE_KEY_FOLDER"),
        "private_key_file_pwd": os.environ["SNOWFLAKE_KEY_PASSPHRASE"],
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        "database": "RETAIL_DB",
        "schema": "RAW",
        "role": os.getenv("SNOWFLAKE_ROLE"),
    }

    return snowflake.connector.connect(**config)


ORDER_COLUMNS = (
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
)


def verify_s3_batch(s3_client, bucket: str, manifest: dict) -> str:
    """Verify the uploaded orders file matches its manifest."""

    if manifest.get("dataset") != "orders":
        raise ValueError("This loader supports orders only")

    batch_id = manifest.get("batch_id")

    if not isinstance(batch_id, str) or not re.fullmatch(
        r"[0-9]+", batch_id
    ):
        raise ValueError("Invalid batch ID")

    if tuple(manifest.get("columns", [])) != ORDER_COLUMNS:
        raise ValueError("Unexpected source schema or column order")

    if manifest.get("row_count", 0) <= 0:
        raise ValueError("Batch must contain records")

    key = f"raw/olist/orders/batch_id={batch_id}/orders.csv"

    obj = s3_client.head_object(
        Bucket=bucket,
        Key=key,
    )

    if (
        obj["ContentLength"] != manifest["file_size_bytes"]
        or obj.get("Metadata", {}).get("sha256")
        != manifest["sha256"]
    ):
        raise ValueError(
            "S3 source object does not match the manifest"
        )

    return key


def load_orders_batch(
    connection,
    s3_client,
    bucket: str,
    manifest: dict,
) -> dict:
    """Load one orders batch and merge new records into RAW."""

    source_key = verify_s3_batch(
        s3_client, bucket, manifest
    )

    batch_id = manifest["batch_id"]
    source_count = manifest["row_count"]
    checksum = manifest["sha256"]

    cursor = connection.cursor()

    try:
        # 1. Check whether this batch previously succeeded.
        cursor.execute(
            """
            SELECT SOURCE_SHA256, SOURCE_ROW_COUNT,
                   SOURCE_FILE, STATUS
            FROM RETAIL_DB.RAW.INGESTION_BATCHES
            WHERE DATASET = %s AND BATCH_ID = %s
            """,
            ("orders", batch_id),
        )

        previous = cursor.fetchall()

        if previous:
            if len(previous) != 1 or previous[0] != (
                checksum,
                source_count,
                source_key,
                "SUCCESS",
            ):
                raise ValueError(
                    "Batch ID conflicts with ingestion history"
                )

            return {
                "status": "already_processed",
                "source_rows": source_count,
                "inserted_rows": 0,
            }

        # 2. Create an empty, session-scoped landing table.
        cursor.execute(
            """
            CREATE OR REPLACE TEMPORARY TABLE
                RETAIL_DB.RAW.ORDERS_INGEST_TMP
            LIKE RETAIL_DB.RAW.ORDERS
            """
        )

        # 3. Load the precise S3 object into the landing table.
        # batch_id is validated as ASCII digits before interpolation.
        cursor.execute(
            f"""
            COPY INTO RETAIL_DB.RAW.ORDERS_INGEST_TMP
            FROM @RETAIL_DB.RAW.OLIST_ORDERS_STAGE
            FILES = ('batch_id={batch_id}/orders.csv')
            FILE_FORMAT = (
                FORMAT_NAME = 'RETAIL_DB.RAW.OLIST_CSV_FORMAT'
            )
            ON_ERROR = 'ABORT_STATEMENT'
            FORCE = TRUE
            """
        )

        copy_results = cursor.fetchall()

        if (
            len(copy_results) != 1
            or copy_results[0][1] != "LOADED"
        ):
            raise ValueError(
                f"Unexpected COPY result: {copy_results}"
            )

        # 4. Validate row counts and business keys.
        cursor.execute(
            """
            SELECT
                COUNT(*),
                COUNT(DISTINCT ORDER_ID),
                COUNT_IF(ORDER_ID IS NULL)
            FROM RETAIL_DB.RAW.ORDERS_INGEST_TMP
            """
        )

        loaded, distinct_orders, null_keys = cursor.fetchone()

        if (
            loaded != source_count
            or distinct_orders != loaded
            or null_keys != 0
        ):
            raise ValueError(
                "Landing table failed row-count or key validation"
            )

        # 5. Reject conflicting values for existing order IDs.
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.ORDERS_INGEST_TMP AS src
            JOIN RETAIL_DB.RAW.ORDERS AS tgt
                ON src.ORDER_ID = tgt.ORDER_ID
            WHERE
                src.CUSTOMER_ID
                    IS DISTINCT FROM tgt.CUSTOMER_ID
                OR src.ORDER_STATUS
                    IS DISTINCT FROM tgt.ORDER_STATUS
                OR src.ORDER_PURCHASE_TIMESTAMP
                    IS DISTINCT FROM tgt.ORDER_PURCHASE_TIMESTAMP
                OR src.ORDER_APPROVED_AT
                    IS DISTINCT FROM tgt.ORDER_APPROVED_AT
                OR src.ORDER_DELIVERED_CARRIER_DATE
                    IS DISTINCT FROM tgt.ORDER_DELIVERED_CARRIER_DATE
                OR src.ORDER_DELIVERED_CUSTOMER_DATE
                    IS DISTINCT FROM tgt.ORDER_DELIVERED_CUSTOMER_DATE
                OR src.ORDER_ESTIMATED_DELIVERY_DATE
                    IS DISTINCT FROM tgt.ORDER_ESTIMATED_DELIVERY_DATE
            """
        )

        conflict_count = cursor.fetchone()[0]

        if conflict_count:
            raise ValueError(
                f"{conflict_count} conflicting existing orders"
            )

        # 6. Count records that are not yet in RAW.
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.ORDERS_INGEST_TMP AS src
            LEFT JOIN RETAIL_DB.RAW.ORDERS AS tgt
                ON src.ORDER_ID = tgt.ORDER_ID
            WHERE tgt.ORDER_ID IS NULL
            """
        )

        new_count = cursor.fetchone()[0]

        # 7. Apply records and audit metadata together.
        cursor.execute("BEGIN")

        try:
            cursor.execute(
                """
                MERGE INTO RETAIL_DB.RAW.ORDERS AS tgt
                USING RETAIL_DB.RAW.ORDERS_INGEST_TMP AS src
                    ON tgt.ORDER_ID = src.ORDER_ID
                WHEN NOT MATCHED THEN INSERT ALL BY NAME
                """
            )

            cursor.execute(
                """
                INSERT INTO RETAIL_DB.RAW.INGESTION_BATCHES (
                    DATASET,
                    BATCH_ID,
                    SOURCE_FILE,
                    SOURCE_SHA256,
                    SOURCE_ROW_COUNT,
                    INSERTED_ROW_COUNT,
                    STATUS
                )
                VALUES (%s, %s, %s, %s, %s, %s, 'SUCCESS')
                """,
                (
                    "orders",
                    batch_id,
                    source_key,
                    checksum,
                    source_count,
                    new_count,
                ),
            )

            cursor.execute("COMMIT")

        except Exception:
            cursor.execute("ROLLBACK")
            raise

        return {
            "status": "success",
            "source_rows": source_count,
            "inserted_rows": new_count,
        }

    finally:
        cursor.close()

def load_exchange_rate_batch(
    connection,
    manifest: dict,
) -> dict:
    """Load one exchange-rate batch into Snowflake RAW."""

    import re

    if manifest.get("dataset") != "exchange_rates":
        raise ValueError("Unexpected dataset")

    batch_id = manifest["batch_id"]

    if (
        not isinstance(batch_id, str)
        or not re.fullmatch(r"[0-9]+", batch_id)
    ):
        raise ValueError("Invalid batch ID")

    expected_count = manifest["record_count"]

    if expected_count <= 0:
        raise ValueError("Empty exchange-rate batch")

    suffix = f"%/batch_id={batch_id}/rates.json"

    cursor = connection.cursor()

    try:
        # Check whether this source file has already
        # been loaded into the RAW table.
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.EXCHANGE_RATES_API
            WHERE SOURCE_FILE LIKE %s
            """,
            (suffix,),
        )

        existing_count = cursor.fetchone()[0]

        if existing_count == expected_count:
            return {
                "status": "already_processed",
                "source_rows": expected_count,
            }

        if existing_count != 0:
            raise ValueError(
                "Partially loaded or conflicting FX batch"
            )

        # The batch ID has been validated as digits.
        cursor.execute(
            f"""
            COPY INTO RETAIL_DB.RAW.EXCHANGE_RATES_API (
                RAW_RECORD,
                SOURCE_FILE
            )
            FROM (
                SELECT
                    t.$1,
                    METADATA$FILENAME
                FROM
                    @RETAIL_DB.RAW.FX_RATES_STAGE/
                    batch_id={batch_id}/rates.json t
            )
            ON_ERROR = 'ABORT_STATEMENT'
            """
        )

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM RETAIL_DB.RAW.EXCHANGE_RATES_API
            WHERE SOURCE_FILE LIKE %s
            """,
            (suffix,),
        )

        loaded_count = cursor.fetchone()[0]

        if loaded_count != expected_count:
            raise ValueError(
                "Exchange-rate load count mismatch"
            )

        return {
            "status": "success",
            "source_rows": loaded_count,
        }

    finally:
        cursor.close()