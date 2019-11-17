import pytest

import datums_warehouse.broker.source as module_under_test
from datums_warehouse.broker.datums import CsvDatums
from datums_warehouse.broker.source import KrakenSource, to_nano_sec, LEDGER_FREQUENCY, KrakenServerTime, \
    InvalidFormatError, ResponseError


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
    return {'result': {'pair': [[]], 'last': str(to_nano_sec(1559347200 * 2))}, 'error': []}


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
    class _Spy:
        def __init__(self):
            self.data = None

        def __call__(self, d, *args, **kwargs):
            self.data = d
            return self.data

    s = _Spy()
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
    s.set_current_time(1559347200 + source_interval + 1)
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


def make_get(url, **params):
    return GetRequest(url, params)


def csv_datums_from(adapted):
    return CsvDatums(adapted.interval, adapted)


@pytest.mark.usefixtures("server_time")
class TestKrakenSource:
    def test_kraken_source_builds_correct_url(self, source, requests):
        source.query(since=1559347200)
        assert requests.received_get == make_get("https://api.kraken.com/0/public/Trades", pair="xbtusd",
                                                 since=1559347200000000000)

    @pytest.mark.parametrize("invalid", [dict(), dict(result={'pair': []}), dict(error=[])])
    def test_invalid_kraken_api_format(self, requests, source, invalid):
        requests.set_get_response(json=invalid)
        with pytest.raises(InvalidFormatError):
            source.query(since=1559347200)

    def test_error_in_response(self, requests, source, make_json):
        requests.set_get_response(json=make_json({'pair': [['data']]}, with_error=['some error']))
        with pytest.raises(ResponseError):
            source.query(since=1559347200)

    def test_kraken_source_returns_validated_and_adapted_data(self, source, source_interval, requests, validation,
                                                              make_json):
        requests.set_get_response(json=make_json({'pair': [['data']]}))
        adapted = AdaptedData(trades=[['data']], with_interval=source_interval)
        assert source.query(since=1559347200) == csv_datums_from(adapted)
        assert validation.data == csv_datums_from(adapted)

    def test_kraken_source_queries_data_up_to_last_incomplete_interval(self, source, source_interval, requests,
                                                                       server_time, make_json):
        server_time.set_current_time(1559347200 + source_interval * 3 * 60 + 10)
        requests.set_get_responses(
            make_json({'pair': [1]}, last=to_nano_sec(1559347200 + source_interval * 60)),
            make_json({'pair': [2]}, last=to_nano_sec(1559347200 + source_interval * 1.5 * 60)),
            make_json({'pair': [3]}, last=to_nano_sec(1559347200 + source_interval * 3 * 60)),
            make_json({'pair': [4]}, last=to_nano_sec(1559347200 + source_interval * 3 * 60 + 1)),
        )

        adapted = AdaptedData(trades=[1, 2, 3], with_interval=source_interval)
        assert source.query(since=1559347200) == csv_datums_from(adapted)

    def test_subsequent_queries_are_paused_with_frequency(self, source, source_interval, requests, server_time,
                                                          local_time, make_json):
        server_time.set_current_time(1559347200 + source_interval * 2 * 60 + 10)
        requests.set_get_responses(
            make_json({'pair': [1]}, last=to_nano_sec(1559347200 + source_interval * 60)),
            make_json({'pair': [2]}, last=to_nano_sec(1559347200 + source_interval * 2.5 * 60)),
            make_json({'pair': [3]}, last=to_nano_sec(1559347200 + source_interval * 3 * 60)),
        )

        source.query(since=1559347200)
        assert local_time.received_sleeps == [LEDGER_FREQUENCY, LEDGER_FREQUENCY]

    def test_subsequent_queries_use_cache_instead_of_remote(self, source, source_interval, requests, server_time,
                                                            local_time, make_json):
        server_time.set_current_time(1559347200 + source_interval * 2 * 60 + 10)
        requests.set_get_responses(
            make_json({'pair': [[1, 0, 1559347201]]},
                      last=to_nano_sec(1559347200 + source_interval * 60)),
            make_json({'pair': [[2, 0, 1559347200 + source_interval + 10]]},
                      last=to_nano_sec(1559347200 + source_interval * 2.5 * 60)),
            make_json({'pair': [[3, 0, 1559347200 + source_interval * 2]]},
                      last=to_nano_sec(1559347200 + source_interval * 3 * 60)),
        )

        source.query(since=1559347200)
        source.query(since=1559347200)
        source.query(since=1559347200)
        assert local_time.received_sleeps == [LEDGER_FREQUENCY, LEDGER_FREQUENCY]

    def test_subsequent_queries_retrieve_since_the_last_time_stamp(self, source, source_interval, requests, server_time,
                                                                   make_json):
        server_time.set_current_time(1559347200 + source_interval * 2 * 60 + 10)
        requests.set_get_responses(
            make_json({'pair': [1]}, last=to_nano_sec(1559347200 + source_interval * 60)),
            make_json({'pair': [2]}, last=to_nano_sec(1559347200 + source_interval * 2.5 * 60))
        )

        source.query(since=1559347200)
        assert requests.received_get == make_get("https://api.kraken.com/0/public/Trades", pair="xbtusd",
                                                 since=to_nano_sec(1559347200 + source_interval * 60))

    def test_stop_queries_after_configured_max_amount(self, source, source_interval, requests, server_time, make_json):
        server_time.set_current_time(1559347200 + source_interval * 3 * 60 + 10)
        requests.set_get_responses(
            make_json({'pair': [1, 2, 3, 4, 5, 6]}, last=to_nano_sec(1559347200 + source_interval * 60)),
            make_json({'pair': [7, 8, 9, 10, 11, 12]}, last=to_nano_sec(1559347200 + source_interval * 2.5 * 60)),
            make_json({'pair': [13, 14]}, last=to_nano_sec(1559347200 + source_interval * 3 * 60)),
        )

        adapted = AdaptedData(trades=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
                              with_interval=source_interval)
        assert source.query(since=1559347200) == csv_datums_from(adapted)


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
