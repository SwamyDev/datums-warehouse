import logging
import random
from itertools import repeat

import pytest

import datums_warehouse.broker.source as module_under_test
from datums_warehouse.broker.datums import CsvDatums
from datums_warehouse.broker.source import KrakenSource, to_nano_sec, LEDGER_FREQUENCY, KrakenServerTime, \
    InvalidFormatError, ResponseError
from datums_warehouse.broker.validation import DataError

START_TIME_S = 1559347200
START_TIME_NS = to_nano_sec(START_TIME_S)


class GetRequest:
    def __init__(self, url, params=None):
        self.url = url
        self.params = params

    def __repr__(self):
        return f"GetRequest(url={self.url}, params={self.params})"

    def __eq__(self, other):
        return self.url == other.url and self.params == other.params


class GetResponse:
    def __init__(self, json=None):
        self._json = make_default_json_response() if json is None else json

    def json(self):
        return self._json


def make_default_json_response():
    return {'result': {'pair': [[0] * 6], 'last': str(to_nano_sec(START_TIME_S * 2))}, 'error': []}


class AdaptedData:
    def __init__(self, trades, with_interval):
        self.trades = trades
        self.interval = with_interval

    def __repr__(self):
        return f"Adapted(trades={self.trades}, with_interval={self.interval})"

    def __eq__(self, other):
        return self.trades == other.trades and self.interval == other.interval


class RequestStub:
    def __init__(self):
        self.data_responses = [GetResponse()]
        self.response_iter = None

    def set_get_response(self, json):
        self.data_responses = [GetResponse(json)]

    def set_get_responses(self, *jsons):
        self.data_responses = [GetResponse(j) for j in jsons]

    def get(self, url, params=None):
        self.response_iter = self.response_iter or iter(self.data_responses)
        return next(self.response_iter)


class RequestsSpy(RequestStub):
    def __init__(self):
        super().__init__()
        self.received_get = None

    def get(self, url, params=None):
        self.received_get = GetRequest(url, params or {})
        return super().get(url, params)


class AdapterStub:
    def __init__(self, interval):
        self._interval = interval

    def __call__(self, data):
        return AdaptedData(data, self._interval)


class LocalTimeSpy:
    def __init__(self):
        self.received_sleeps = []

    def sleep(self, seconds):
        self.received_sleeps.append(seconds)

    def time(self):
        return 0


class ServerTimeStub:
    def __init__(self):
        self.current_time = None

    def set_current_time(self, t):
        self.current_time = t

    def now(self):
        return self.current_time


class RandomnessSpy:
    def __init__(self):
        self.history = list()

    def uniform(self, a, b):
        self.history.append(random.uniform(a, b))
        return self.history[-1]


class ValidationSpy:
    def __init__(self):
        self.data = None
        self.raises = None

    def set_raises(self, raises):
        self.raises = raises

    def __call__(self, d, *args, **kwargs):
        if self.raises:
            raise self.raises("mocked data error")
        self.data = d
        return self.data


@pytest.fixture
def requests(monkeypatch):
    s = RequestsSpy()
    monkeypatch.setattr(module_under_test, 'requests', s)
    return s


@pytest.fixture
def source_interval():
    return 30


@pytest.fixture
def source(source_interval, tmp_path):
    return KrakenSource(trades_storage=tmp_path, pair="xbtusd", interval=source_interval, max_results=10)


@pytest.fixture(autouse=True)
def validation(monkeypatch):
    s = ValidationSpy()
    monkeypatch.setattr(module_under_test, 'validate', s)
    return s


@pytest.fixture(autouse=True)
def adapter(monkeypatch):
    monkeypatch.setattr(module_under_test, 'KrakenAdapter', AdapterStub)


@pytest.fixture(autouse=True)
def local_time(monkeypatch, source_interval):
    s = LocalTimeSpy()
    monkeypatch.setattr(module_under_test, 'time', s)
    return s


@pytest.fixture
def server_time(monkeypatch, source_interval):
    s = ServerTimeStub()
    s.set_current_time(START_TIME_S + source_interval + 1)
    monkeypatch.setattr(module_under_test, 'KrakenServerTime', lambda: s)
    return s


@pytest.fixture
def make_json(server_time, source_interval):
    def factory(results, last=None, with_error=None):
        res = make_default_json_response()
        res['result'] = results
        res['error'] = with_error or []
        last = last or to_nano_sec(server_time.current_time + source_interval + 1)
        res['result']['last'] = str(last)
        return res

    return factory


@pytest.fixture
def randomness(monkeypatch):
    s = RandomnessSpy()
    monkeypatch.setattr(module_under_test, 'random', s)
    return s


def make_get(url, **params):
    return GetRequest(url, params)


def csv_datums_from(adapted):
    return CsvDatums(adapted.interval, adapted)


def expand_to_trades(*values, ts_range=None):
    ts_range = ts_range or repeat(START_TIME_S)
    return [[v, v, ts, v, v, v] for v, ts in zip(values, ts_range)]


@pytest.mark.usefixtures("server_time")
class TestKrakenSource:
    def test_kraken_source_builds_correct_url(self, source, requests):
        source.query(since=START_TIME_S)
        assert requests.received_get == make_get("https://api.kraken.com/0/public/Trades", pair="xbtusd",
                                                 since=START_TIME_NS)

    @pytest.mark.parametrize("invalid", [dict(), dict(result={'pair': []}), dict(error=[])])
    def test_invalid_kraken_api_format(self, requests, source, invalid):
        requests.set_get_response(json=invalid)
        with pytest.raises(InvalidFormatError):
            source.query(since=START_TIME_S)

    def test_error_in_response(self, requests, source, make_json):
        requests.set_get_response(json=make_json({'pair': expand_to_trades(0)}, with_error=['some error']))
        with pytest.raises(ResponseError):
            source.query(since=START_TIME_S)

    def test_kraken_source_returns_validated_and_adapted_data(self, source, source_interval, requests, validation,
                                                              make_json):
        requests.set_get_response(json=make_json({'pair': expand_to_trades(1)}))
        adapted = AdaptedData(trades=[[1, 1, START_TIME_S]], with_interval=source_interval)
        assert source.query(since=START_TIME_S) == csv_datums_from(adapted)
        assert validation.data == csv_datums_from(adapted)

    def test_kraken_source_queries_data_up_to_last_incomplete_interval(self, source, source_interval, requests,
                                                                       server_time, make_json):
        server_time.set_current_time(START_TIME_S + source_interval * 3 * 60 + 10)
        last1 = START_TIME_S + source_interval * 60
        last2 = START_TIME_S + source_interval * 1.5 * 60
        last3 = START_TIME_S + source_interval * 3 * 60
        last4 = START_TIME_S + source_interval * 3 * 60 + 1
        requests.set_get_responses(
            make_json({'pair': expand_to_trades(1, ts_range=[START_TIME_S])}, last=to_nano_sec(last1)),
            make_json({'pair': expand_to_trades(2, ts_range=[last1 + 1])}, last=to_nano_sec(last2)),
            make_json({'pair': expand_to_trades(3, ts_range=[last2 + 1])}, last=to_nano_sec(last3)),
            make_json({'pair': expand_to_trades(4, ts_range=[last3 + 1])}, last=to_nano_sec(last4)),
        )

        adapted = AdaptedData(trades=[[1, 1, START_TIME_S], [2, 2, last1 + 1], [3, 3, last2 + 1]],
                              with_interval=source_interval)
        assert source.query(since=START_TIME_S) == csv_datums_from(adapted)

    def test_subsequent_queries_are_paused_with_frequency(self, source, source_interval, requests, server_time,
                                                          local_time, make_json, randomness):
        server_time.set_current_time(START_TIME_S + source_interval * 2 * 60 + 10)
        requests.set_get_responses(
            make_json({'pair': expand_to_trades(1)}, last=to_nano_sec(START_TIME_S + source_interval * 60)),
            make_json({'pair': expand_to_trades(2)}, last=to_nano_sec(START_TIME_S + source_interval * 2.5 * 60)),
            make_json({'pair': expand_to_trades(3)}, last=to_nano_sec(START_TIME_S + source_interval * 3 * 60)),
        )

        source.query(since=START_TIME_S)
        assert local_time.received_sleeps == [
            LEDGER_FREQUENCY + randomness.history[-2],
            LEDGER_FREQUENCY + randomness.history[-1]
        ]

    def test_subsequent_queries_use_cache_instead_of_remote(self, source, source_interval, requests, server_time,
                                                            local_time, make_json):
        server_time.set_current_time(START_TIME_S + source_interval * 2 * 60 + 10)
        requests.set_get_responses(
            make_json({'pair': expand_to_trades(1, ts_range=[1559347201])},
                      last=to_nano_sec(START_TIME_S + source_interval * 60)),
            make_json({'pair': expand_to_trades(2, ts_range=[START_TIME_S + source_interval + 10])},
                      last=to_nano_sec(START_TIME_S + source_interval * 2.5 * 60)),
            make_json({'pair': expand_to_trades(3, ts_range=[START_TIME_S + source_interval * 2])},
                      last=to_nano_sec(START_TIME_S + source_interval * 3 * 60)),
        )

        source.query(since=START_TIME_S)
        source.query(since=START_TIME_S)
        source.query(since=START_TIME_S)
        assert len(local_time.received_sleeps) == 2

    def test_subsequent_queries_retrieve_since_the_last_time_stamp(self, source, source_interval, requests, server_time,
                                                                   make_json):
        server_time.set_current_time(START_TIME_S + source_interval * 2 * 60 + 10)
        requests.set_get_responses(
            make_json({'pair': expand_to_trades(1)}, last=to_nano_sec(START_TIME_S + source_interval * 60)),
            make_json({'pair': expand_to_trades(2)}, last=to_nano_sec(START_TIME_S + source_interval * 2.5 * 60))
        )

        source.query(since=START_TIME_S)
        assert requests.received_get == make_get("https://api.kraken.com/0/public/Trades", pair="xbtusd",
                                                 since=to_nano_sec(START_TIME_S + source_interval * 60))

    def test_stop_queries_after_configured_max_amount(self, source, source_interval, requests, server_time, make_json):
        server_time.set_current_time(START_TIME_S + source_interval * 3 * 60 + 10)
        requests.set_get_responses(
            make_json({'pair': expand_to_trades(1, 2, 3, 4, 5, 6)},
                      last=to_nano_sec(START_TIME_S + source_interval * 60)),
            make_json({'pair': expand_to_trades(7, 8, 9, 10, 11, 12)},
                      last=to_nano_sec(START_TIME_S + source_interval * 2.5 * 60)),
            make_json({'pair': expand_to_trades(13, 14)}, last=to_nano_sec(START_TIME_S + source_interval * 3 * 60)),
        )

        adapted = AdaptedData(
            trades=self.take_price_volume_time(),
            with_interval=source_interval)
        assert source.query(since=START_TIME_S) == csv_datums_from(adapted)

    @staticmethod
    def take_price_volume_time():
        return [[p, v, t] for p, v, t, _, _, _ in expand_to_trades(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)]

    def test_logs_warning_on_invalid_data(self, source, validation, caplog):
        caplog.set_level(logging.WARNING)
        validation.set_raises(DataError)
        source.query(since=START_TIME_S)
        assert "mocked data error" in caplog.text


def test_kraken_server_time_queries_url_correctly(requests):
    requests.set_get_response(
        json={"error": [], "result": {"unixtime": 1572715612, "rfc1123": "Sat,  2 Nov 19 17:26:52 +0000"}})
    time = KrakenServerTime()
    time.now()
    assert requests.received_get == make_get("https://api.kraken.com/0/public/Time")


def test_kraken_server_time_returns_unix_time(requests):
    requests.set_get_response(
        json={"error": [], "result": {"unixtime": 1572715612, "rfc1123": "Sat,  2 Nov 19 17:26:52 +0000"}})
    time = KrakenServerTime()
    assert time.now() == 1572715612
