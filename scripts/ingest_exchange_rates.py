import argparse
import hashlib

import boto3

from retail_pipeline.exchange_rates import (
    create_http_session,
    prepare_exchange_rate_batch,
)
from retail_pipeline.storage import put_immutable_object


def main():
    parser = argparse.ArgumentParser(
        description="Extract and upload historical exchange rates"
    )

    parser.add_argument("--from-date", required=True)
    parser.add_argument("--to-date", required=True)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--profile", default=None)

    args = parser.parse_args()

    with create_http_session() as http_session:
        raw_path, manifest_path, manifest = (
            prepare_exchange_rate_batch(
                session=http_session,
                start_date=args.from_date,
                end_date=args.to_date,
                batch_id=args.batch_id,
            )
        )

    raw = raw_path.read_bytes()
    manifest_bytes = manifest_path.read_bytes()

    if (
        hashlib.sha256(raw).hexdigest() != manifest["sha256"]
        or len(raw) != manifest["file_size_bytes"]
    ):
        raise ValueError("Saved response differs from manifest")

    aws_session = boto3.Session(
        profile_name=args.profile
    )

    s3 = aws_session.client("s3")

    raw_key = (
        f"raw/exchange_rates/"
        f"batch_id={args.batch_id}/rates.json"
    )

    manifest_key = (
        "metadata/ingestion_manifests/"
        f"exchange_rates/batch_id={args.batch_id}.json"
    )

    raw_status = put_immutable_object(
        s3, args.bucket, raw_key, raw, "application/json"
    )

    manifest_status = put_immutable_object(
        s3,
        args.bucket,
        manifest_key,
        manifest_bytes,
        "application/json",
    )

    print(f"Records: {manifest['record_count']}")
    print(f"Raw upload: {raw_status}")
    print(f"Manifest upload: {manifest_status}")
    print(f"S3 source: s3://{args.bucket}/{raw_key}")


if __name__ == "__main__":
    main()