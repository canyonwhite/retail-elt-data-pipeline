import base64
import hashlib
import json
from pathlib import Path

from botocore.exceptions import ClientError

from retail_pipeline.ingestion import DATASETS


def put_immutable_object(
    s3_client,
    bucket: str,
    key: str,
    body: bytes,
    content_type: str,
) -> str:
    """Upload an object without replacing existing contents."""

    digest = hashlib.sha256(body).digest()
    checksum_hex = digest.hex()

    status = "uploaded"

    try:
        s3_client.put_object(
            Bucket=bucket,
            Key=key,
            Body=body,
            ContentType=content_type,
            Metadata={"sha256": checksum_hex},
            ChecksumSHA256=base64.b64encode(
                digest
            ).decode("ascii"),
            IfNoneMatch="*",
        )

    except ClientError as exc:
        error_code = exc.response["Error"]["Code"]

        if error_code not in ("PreconditionFailed", "412"):
            raise

        status = "already_exists"

    # Check the stored object after either outcome.
    stored = s3_client.head_object(
        Bucket=bucket,
        Key=key,
    )

    if (
        stored["ContentLength"] != len(body)
        or stored.get("Metadata", {}).get("sha256")
        != checksum_hex
    ):
        raise ValueError(
            f"S3 object exists with different or "
            f"unverifiable contents: s3://{bucket}/{key}"
        )

    return status

def upload_batch(
    s3_client,
    bucket: str,
    source_path: Path,
    manifest_path: Path,
) -> dict:
    """Upload a validated CSV and its batch manifest."""

    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    dataset = manifest["dataset"]
    batch_id = manifest["batch_id"]

    if dataset not in DATASETS:
        raise ValueError("Unsupported dataset")

    if (
        not isinstance(batch_id, str)
        or not batch_id.isascii()
        or not batch_id.isdigit()
    ):
        raise ValueError("Invalid batch ID")

    if manifest["source_filename"] != source_path.name:
        raise ValueError("Source filename does not match manifest")

    # This lesson uses small files and an in-memory snapshot.
    max_file_bytes = 100 * 1024 * 1024

    if source_path.stat().st_size > max_file_bytes:
        raise ValueError(
            "File exceeds the single-upload lesson limit"
        )

    source_bytes = source_path.read_bytes()

    source_sha256 = hashlib.sha256(
        source_bytes
    ).hexdigest()

    if (
        source_sha256 != manifest["sha256"]
        or len(source_bytes) != manifest["file_size_bytes"]
    ):
        raise ValueError(
            "Source file no longer matches its manifest"
        )

    raw_key = (
        f"raw/olist/{dataset}/"
        f"batch_id={batch_id}/{dataset}.csv"
    )

    manifest_key = (
        f"metadata/ingestion_manifests/"
        f"{dataset}/batch_id={batch_id}.json"
    )

    # Upload the source first.
    raw_status = put_immutable_object(
        s3_client,
        bucket,
        raw_key,
        source_bytes,
        "text/csv",
    )

    # Upload the manifest after the source succeeds.
    manifest_bytes = manifest_path.read_bytes()

    manifest_status = put_immutable_object(
        s3_client,
        bucket,
        manifest_key,
        manifest_bytes,
        "application/json",
    )

    return {
        "raw_key": raw_key,
        "manifest_key": manifest_key,
        "raw_status": raw_status,
        "manifest_status": manifest_status,
    }

def upload_exchange_rate_batch(
    s3_client,
    bucket: str,
    raw_path: Path,
    manifest_path: Path,
) -> dict:
    """Upload a prepared exchange-rate batch."""

    manifest = json.loads(
        manifest_path.read_text(encoding="utf-8")
    )

    if manifest.get("dataset") != "exchange_rates":
        raise ValueError("Unexpected dataset")

    batch_id = manifest["batch_id"]

    if (
        not isinstance(batch_id, str)
        or not batch_id.isascii()
        or not batch_id.isdigit()
    ):
        raise ValueError("Invalid batch ID")

    raw_bytes = raw_path.read_bytes()

    if (
        hashlib.sha256(raw_bytes).hexdigest()
        != manifest["sha256"]
        or len(raw_bytes) != manifest["file_size_bytes"]
    ):
        raise ValueError("Source does not match manifest")

    raw_key = (
        f"raw/exchange_rates/"
        f"batch_id={batch_id}/rates.json"
    )

    manifest_key = (
        "metadata/ingestion_manifests/"
        f"exchange_rates/batch_id={batch_id}.json"
    )

    raw_status = put_immutable_object(
        s3_client,
        bucket,
        raw_key,
        raw_bytes,
        "application/json",
    )

    manifest_status = put_immutable_object(
        s3_client,
        bucket,
        manifest_key,
        manifest_path.read_bytes(),
        "application/json",
    )

    return {
        "raw_key": raw_key,
        "manifest_key": manifest_key,
        "raw_status": raw_status,
        "manifest_status": manifest_status,
    }