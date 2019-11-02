import pytest

import datums_warehouse.broker.source as module_under_test
from datums_warehouse.broker.datums import CsvDatums
from datums_warehouse.broker.source import KrakenSource


class GetRequest:
    def __init__(self, url, params):
        self.url = url
        self.params = params

    def __repr__(self):
        return f"GetRequest(url={self.url}, params={self.params})"

    def __eq__(self, other):
        return self.url == other.url and self.params == other.params


class GetResponse:
    def __init__(self, json=None):
        self._json = json or {}

    def json(self):
        return self._json


class Adapted:
    def __init__(self, json, with_interval):
        self.json = json
        self.interval = with_interval

    def __repr__(self):
        return f"Adapted(json={self.json}, with_interval={self.interval})"

    def __eq__(self, other):
        return self.json == other.json and self.interval == other.interval


class Validated:
    def __init__(self, data):
        self.data = data

    def __repr__(self):
        return f"Validated(data={self.data})"

    def __eq__(self, other):
        return self.data.csv == other.data.csv and self.data.interval == other.data.interval


class RequestStub:
    def __init__(self):
        self.get_response = GetResponse()

    def set_get_response(self, json):
        self.get_response = GetResponse(json)

    def get(self, url, params):
        return self.get_response


class RequestsSpy(RequestStub):
    def __init__(self):
        super().__init__()
        self.received_get = None

    def get(self, url, params):
        self.received_get = GetRequest(url, params)
        return super().get(url, params)


class AdapterStub:
    def __init__(self, interval):
        self._interval = interval

    def __call__(self, data):
        return Adapted(data, self._interval)


@pytest.fixture
def requests(monkeypatch):
    s = RequestsSpy()
    monkeypatch.setattr(module_under_test, 'requests', s)
    return s


@pytest.fixture
def source_interval():
    return 30


@pytest.fixture
def source(source_interval):
    return KrakenSource(pair="xbtusd", interval=source_interval)


@pytest.fixture(autouse=True)
def validation(monkeypatch):
    monkeypatch.setattr(module_under_test, 'validate', lambda d, *args, **kwargs: Validated(d))


@pytest.fixture(autouse=True)
def adapter(monkeypatch):
    monkeypatch.setattr(module_under_test, 'KrakenAdapter', AdapterStub)


def test_kraken_source_builds_correct_url(source, requests):
    source.query(since=1559347200)
    assert requests.received_get == make_get("https://api.kraken.com/0/public/Trades", pair="xbtusd",
                                             since=1559347200000000000)


def make_get(url, **params):
    return GetRequest(url, params)


def test_kraken_source_returns_validated_and_adapted_data(source, source_interval, requests):
    requests.set_get_response(json={'some': 'data'})
    adapted = Adapted(json={'some': 'data'}, with_interval=source_interval)
    assert source.query(since=1559347200) == Validated(csv_datums_from(adapted))


def csv_datums_from(adapted):
    return CsvDatums(adapted.interval, adapted)
