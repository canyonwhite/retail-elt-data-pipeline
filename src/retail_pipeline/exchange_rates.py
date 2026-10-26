import hashlib
import json
import re
from datetime import UTC, date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

API_URL = "https://api.frankfurter.dev/v2/rates"


def create_http_session() -> requests.Session:
    """Create an HTTP session with retry handling."""

    retry_policy = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods={"GET"},
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry_policy)

    session = requests.Session()
    session.mount("https://", adapter)

    return session

def validate_exchange_rates(
    raw_json: bytes,
    start_date: date,
    end_date: date,
) -> list[dict]:
    """Validate historical BRL/USD rate observations."""

    try:
        records = json.loads(raw_json)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError("Invalid JSON response") from exc

    if not isinstance(records, list) or not records:
        raise ValueError("API returned no rate records")

    seen_dates = set()

    for index, record in enumerate(records):
        if not isinstance(record, dict):
            raise TypeError(
                f"Record {index} is not a JSON object"
            )

        if record.get("base") != "BRL":
            raise ValueError("Unexpected base currency")

        if record.get("quote") != "USD":
            raise ValueError("Unexpected quote currency")

        try:
            rate_date = date.fromisoformat(record["date"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Invalid observation date") from exc

        if not start_date <= rate_date <= end_date:
            raise ValueError(
                "Observation date outside requested range"
            )

        if rate_date in seen_dates:
            raise ValueError(
                f"Duplicate exchange-rate date: {rate_date}"
            )

        seen_dates.add(rate_date)

        value = record.get("rate")

        if isinstance(value, bool):
            raise TypeError("Invalid exchange-rate value")

        try:
            rate = Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError) as exc:
            raise ValueError("Invalid exchange-rate value") from exc

        if not rate.is_finite() or rate <= 0:
            raise ValueError(
                "Exchange rate must be positive and finite"
            )

    return records

def fetch_exchange_rates(
    session: requests.Session,
    start_date: str,
    end_date: str,
) -> bytes:
    """Retrieve and validate historical BRL/USD rates."""

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if start > end:
        raise ValueError(
            "Start date must not exceed end date"
        )

    response = session.get(
        API_URL,
        params={
            "from": start.isoformat(),
            "to": end.isoformat(),
            "base": "BRL",
            "quotes": "USD",
        },
        timeout=(5, 30),
    )

    response.raise_for_status()

    raw_json = response.content

    if len(raw_json) > 5_000_000:
        raise ValueError("API response exceeds lesson size limit")

    validate_exchange_rates(
        raw_json,
        start,
        end,
    )

    return raw_json

def save_immutable_file(path: Path, content: bytes) -> None:
    """Create a file without overwriting different contents."""

    path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with path.open("xb") as file:
            file.write(content)
    except FileExistsError:
        if path.read_bytes() != content:
            raise ValueError(
                f"Existing file has different contents: {path}"
            )


def prepare_exchange_rate_batch(
    session: requests.Session,
    start_date: str,
    end_date: str,
    batch_id: str,
    data_dir: Path = Path("data"),
) -> tuple[Path, Path, dict]:
    """Prepare immutable API response and manifest files."""

    if not re.fullmatch(r"[0-9]+", batch_id):
        raise ValueError("Batch ID must contain ASCII digits")

    start = date.fromisoformat(start_date)
    end = date.fromisoformat(end_date)

    if start > end:
        raise ValueError("Invalid date range")

    response_path = (
        data_dir
        / "raw"
        / "exchange_rates"
        / f"batch_id={batch_id}"
        / "rates.json"
    )

    manifest_path = (
        data_dir
        / "manifests"
        / "exchange_rates"
        / f"batch_id={batch_id}.json"
    )

    if response_path.exists():
        raw = response_path.read_bytes()
    else:
        raw = fetch_exchange_rates(
            session, start_date, end_date
        )
        save_immutable_file(response_path, raw)

    records = validate_exchange_rates(raw, start, end)

    observed_dates = [item["date"] for item in records]

    manifest = {
        "dataset": "exchange_rates",
        "batch_id": batch_id,
        "source_system": "frankfurter_v2",
        "source_endpoint": API_URL,
        "requested_from": start_date,
        "requested_to": end_date,
        "base_currency": "BRL",
        "quote_currency": "USD",
        "record_count": len(records),
        "first_observation_date": min(observed_dates),
        "last_observation_date": max(observed_dates),
        "file_size_bytes": len(raw),
        "sha256": hashlib.sha256(raw).hexdigest(),
    }

    if manifest_path.exists():
        existing = json.loads(
            manifest_path.read_text(encoding="utf-8")
        )

        for key, value in manifest.items():
            if existing.get(key) != value:
                raise ValueError(
                    f"Conflicting manifest field: {key}"
                )

        return response_path, manifest_path, existing

    manifest["retrieved_at_utc"] = datetime.now(
        UTC
    ).isoformat()

    encoded = json.dumps(
        manifest, indent=2, sort_keys=True
    ).encode("utf-8")

    save_immutable_file(manifest_path, encoded)

    return response_path, manifest_path, manifest