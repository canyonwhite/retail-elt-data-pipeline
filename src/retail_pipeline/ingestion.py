import csv
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path


@dataclass(frozen=True)
class DatasetSpec:
    name: str
    required_columns: tuple[str, ...]


DATASETS = {
    "orders": DatasetSpec(
        name="orders",
        required_columns=(
            "order_id",
            "customer_id",
            "order_status",
            "order_purchase_timestamp",
            "order_approved_at",
            "order_delivered_carrier_date",
            "order_delivered_customer_date",
            "order_estimated_delivery_date",
        ),
    ),
    "payments": DatasetSpec(
        name="payments",
        required_columns=(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        ),
    ),
    "order_items": DatasetSpec(
        name="order_items",
        required_columns=(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "shipping_limit_date",
            "price",
            "freight_value",
        ),
    ),
    "customers": DatasetSpec(
        name="customers",
        required_columns=(
            "customer_id",
            "customer_unique_id",
            "customer_zip_code_prefix",
            "customer_city",
            "customer_state",
        ),
    ),
    "products": DatasetSpec(
        name="products",
        required_columns=(
            "product_id",
            "product_category_name",
            "product_name_lenght",
            "product_description_lenght",
            "product_photos_qty",
            "product_weight_g",
            "product_length_cm",
            "product_height_cm",
            "product_width_cm",
        ),
    ),
    "sellers": DatasetSpec(
        name="sellers",
        required_columns=(
            "seller_id",
            "seller_zip_code_prefix",
            "seller_city",
            "seller_state",
        ),
    ),
}

# Orders retain their existing dedicated ingestion branch.
CSV_FILES = {
    "payments": "olist_order_payments_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "customers": "olist_customers_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
}


def inspect_csv(
    file_path: Path,
    spec: DatasetSpec,
    batch_id: str,
) -> dict:
    """Validate a CSV file and collect batch metadata."""

    if not file_path.is_file():
        raise FileNotFoundError(file_path)

    if not batch_id.isascii() or not batch_id.isdigit():
        raise ValueError("Batch ID must contain ASCII digits")

    with file_path.open(
        "r",
        encoding="utf-8-sig",
        newline="",
    ) as file:
        reader = csv.reader(file)
        columns = next(reader, None)

        if not columns:
            raise ValueError("CSV file has no header")

        if len(columns) != len(set(columns)):
            raise ValueError("Duplicate column names found")

        missing = set(spec.required_columns) - set(columns)

        if missing:
            raise ValueError(
                f"Missing required columns: {sorted(missing)}"
            )

        row_count = 0

        for row in reader:
            if len(row) != len(columns):
                raise ValueError(
                    f"Invalid field count near line "
                    f"{reader.line_num}"
                )

            row_count += 1

        if row_count == 0:
            raise ValueError(
                "No rows detected in this file. Row count: 0"
            )

    checksum = hashlib.sha256()

    with file_path.open("rb") as file:
        for chunk in iter(
            lambda: file.read(1024 * 1024),
            b"",
        ):
            checksum.update(chunk)

    return {
        "dataset": spec.name,
        "batch_id": batch_id,
        "source_filename": file_path.name,
        "row_count": row_count,
        "file_size_bytes": file_path.stat().st_size,
        "sha256": checksum.hexdigest(),
        "columns": columns,
        "created_at": datetime.now(UTC).isoformat(),
    }


def save_manifest(
    manifest: dict,
    destination: Path,
) -> bool:
    """Save batch metadata without silently replacing batches."""

    identifying_fields = (
        "dataset",
        "batch_id",
        "source_filename",
        "row_count",
        "file_size_bytes",
        "sha256",
        "columns",
    )

    if destination.exists():
        existing = json.loads(
            destination.read_text(encoding="utf-8")
        )

        if any(
            existing.get(field) != manifest.get(field)
            for field in identifying_fields
        ):
            raise ValueError(
                "Batch ID already exists with different metadata"
            )

        return False

    destination.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    destination.write_text(
        json.dumps(manifest, indent=2),
        encoding="utf-8",
    )

    return True