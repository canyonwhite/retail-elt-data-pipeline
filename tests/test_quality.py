import pytest

from retail_pipeline.validation import (
    DataQualityError,
    validate_orders_landing,
)


def test_valid_orders_batch():
    result = validate_orders_landing(
        expected_rows=100,
        loaded_rows=100,
        distinct_order_ids=100,
        null_order_ids=0,
    )

    assert result["validation_status"] == "passed"


def test_missing_records():
    with pytest.raises(
        DataQualityError,
        match="Record count mismatch",
    ):
        validate_orders_landing(100, 95, 95, 0)


def test_duplicate_order_ids():
    with pytest.raises(
        DataQualityError,
        match="Duplicate",
    ):
        validate_orders_landing(100, 100, 99, 0)


def test_null_order_ids():
    with pytest.raises(
        DataQualityError,
        match="null order IDs",
    ):
        validate_orders_landing(100, 100, 99, 1)


def test_zero_record_batch():
    with pytest.raises(
        DataQualityError,
        match="at least one",
    ):
        validate_orders_landing(0, 0, 0, 0)