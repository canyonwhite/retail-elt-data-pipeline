from unittest.mock import MagicMock

import pytest

from retail_pipeline.warehouse import (
    ORDER_COLUMNS,
    publish_fx_landing,
    verify_s3_batch,
)


def test_fx_audit_failure_triggers_rollback():
    cursor = MagicMock()

    def execute_with_failure(sql, params=None):
        if (
            "INSERT INTO RETAIL_DB.RAW.INGESTION_BATCHES"
            in sql
        ):
            raise RuntimeError("Simulated audit failure")

    cursor.execute.side_effect = execute_with_failure

    manifest = {
        "batch_id": "0003",
        "sha256": "a" * 64,
        "record_count": 5,
    }

    with pytest.raises(
        RuntimeError,
        match="Simulated audit failure",
    ):
        publish_fx_landing(
            cursor,
            manifest,
            "raw/exchange_rates/batch_id=0003/rates.json",
        )

    executed = [
        call.args[0].strip()
        for call in cursor.execute.call_args_list
    ]

    assert executed[0] == "BEGIN"
    assert executed[-1] == "ROLLBACK"
    assert "COMMIT" not in executed


class FakeS3:
    def __init__(self, size, checksum):
        self.size = size
        self.checksum = checksum

    def head_object(self, Bucket, Key):
        return {
            "ContentLength": self.size,
            "Metadata": {
                "sha256": self.checksum,
            },
        }


@pytest.fixture
def manifest():
    return {
        "dataset": "orders",
        "batch_id": "0002",
        "columns": list(ORDER_COLUMNS),
        "row_count": 2,
        "file_size_bytes": 100,
        "sha256": "a" * 64,
    }


def test_valid_s3_batch(manifest):
    s3 = FakeS3(
        size=100,
        checksum="a" * 64,
    )

    key = verify_s3_batch(
        s3,
        "test-bucket",
        manifest,
    )

    assert key == (
        "raw/olist/orders/batch_id=0002/orders.csv"
    )


def test_reject_checksum_mismatch(manifest):
    s3 = FakeS3(
        size=100,
        checksum="b" * 64,
    )

    with pytest.raises(ValueError, match="does not match"):
        verify_s3_batch(
            s3,
            "test-bucket",
            manifest,
        )


def test_reject_invalid_batch_id(manifest):
    manifest["batch_id"] = "../bad-batch"

    s3 = FakeS3(
        size=100,
        checksum="a" * 64,
    )

    with pytest.raises(ValueError, match="Invalid batch ID"):
        verify_s3_batch(
            s3,
            "test-bucket",
            manifest,
        )