from collections.abc import Iterable


class DataQualityError(ValueError):
    """Source or warehouse data violates an expected rule."""

REQUIRED_ORDER_COLUMNS = {
    "order_id",
    "customer_id",
    "order_status",
    "order_purchase_timestamp"
}


def validate_order_columns(columns: Iterable[str], allow_extra: bool = True) -> None:
    """ Verify that all required order columns exist."""
    missing = REQUIRED_ORDER_COLUMNS - set(columns)
    extra_col = set(columns) - REQUIRED_ORDER_COLUMNS

    if missing:
        raise ValueError(
            f"Missing required columns: {sorted(missing)}"
        )
    if not allow_extra and extra_col:
        raise ValueError(
            f"Unexpected column provided: {sorted(extra_col)}"
        )

def validate_orders_landing(
    expected_rows: int,
    loaded_rows: int,
    distinct_order_ids: int,
    null_order_ids: int,
) -> dict:
    """Validate a loaded orders batch before publication."""

    if expected_rows <= 0:
        raise DataQualityError(
            "Orders batch must contain at least one record"
        )

    if loaded_rows != expected_rows:
        raise DataQualityError(
            f"Record count mismatch: expected={expected_rows}, "
            f"loaded={loaded_rows}"
        )

    if null_order_ids > 0:
        raise DataQualityError(
            f"Found {null_order_ids} null order IDs"
        )

    if distinct_order_ids != loaded_rows:
        raise DataQualityError(
            "Duplicate order IDs detected"
        )

    return {
        "expected_rows": expected_rows,
        "loaded_rows": loaded_rows,
        "distinct_order_ids": distinct_order_ids,
        "null_order_ids": null_order_ids,
        "validation_status": "passed",
    }