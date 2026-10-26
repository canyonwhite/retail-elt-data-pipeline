import json
from datetime import date

import pytest

from retail_pipeline.exchange_rates import (
    fetch_exchange_rates,
    validate_exchange_rates,
)


@pytest.fixture
def sample_rates():
    return [
        {
            "date": "2018-01-03",
            "base": "BRL",
            "quote": "USD",
            "rate": 0.31,
        },
        {
            "date": "2018-01-04",
            "base": "BRL",
            "quote": "USD",
            "rate": 0.30,
        },
    ]


def test_valid_rates(sample_rates):
    raw = json.dumps(sample_rates).encode("utf-8")

    result = validate_exchange_rates(
        raw,
        date(2018, 1, 1),
        date(2018, 1, 10),
    )

    assert len(result) == 2


def test_empty_response():
    with pytest.raises(ValueError, match="no rate records"):
        validate_exchange_rates(
            b"[]",
            date(2018, 1, 1),
            date(2018, 1, 10),
        )


def test_invalid_currency_pair(sample_rates):
    sample_rates[0]["base"] = "USD"

    raw = json.dumps(sample_rates).encode("utf-8")

    with pytest.raises(ValueError, match="base currency"):
        validate_exchange_rates(
            raw,
            date(2018, 1, 1),
            date(2018, 1, 10),
        )


def test_duplicate_observation_date(sample_rates):
    sample_rates[1]["date"] = "2018-01-03"

    raw = json.dumps(sample_rates).encode("utf-8")

    with pytest.raises(ValueError, match="Duplicate"):
        validate_exchange_rates(
            raw,
            date(2018, 1, 1),
            date(2018, 1, 10),
        )

class FakeResponse:
    def __init__(self, payload):
        self.content = json.dumps(payload).encode("utf-8")

    def raise_for_status(self):
        pass


class FakeSession:
    def __init__(self, payload):
        self.payload = payload
        self.params = None

    def get(self, url, params, timeout):
        self.params = params
        return FakeResponse(self.payload)


def test_api_request_parameters(sample_rates):
    session = FakeSession(sample_rates)

    result = fetch_exchange_rates(
        session,
        "2018-01-01",
        "2018-01-10",
    )

    assert result
    assert session.params["base"] == "BRL"
    assert session.params["quotes"] == "USD"
    assert session.params["from"] == "2018-01-01"
    assert session.params["to"] == "2018-01-10"