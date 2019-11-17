import logging
import time
from pathlib import Path

import requests
from more_itertools import first

from datums_warehouse.broker.adapters import KrakenAdapter
from datums_warehouse.broker.cache import TradesCache
from datums_warehouse.broker.datums import CsvDatums, floor_to_interval
from datums_warehouse.broker.validation import validate, DataError

LEDGER_FREQUENCY = 6
logger = logging.getLogger(__name__)


def to_nano_sec(t):
    return int(t * 1e9)


class KrakenServerTime:
    _TIME_URL = "https://api.kraken.com/0/public/Time"

    def now(self):
        res = requests.get(self._TIME_URL).json()
        return res['result']['unixtime']


class KrakenTrades:
    _TRADE_URL = "https://api.kraken.com/0/public/Trades"
    _RESULT_KEY = "result"
    _ERROR_KEY = "error"

    def __init__(self, cache_dir, pair, max_results):
        self._pair = pair
        cache_file = Path(cache_dir) / self._pair / "kraken_cache"
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        self._cache = TradesCache(cache_file)
        self._max_results = max_results

    def get(self, since, until):
        len_results = 0
        while self._cache.last_timestamp() < to_nano_sec(until) and len_results < self._max_results:
            time.sleep(LEDGER_FREQUENCY)
            res = self._query_remote_trades(self._cache.last_timestamp() or to_nano_sec(since))
            trades = get_trades(res)
            self._cache.update(trades, get_last(res))
            len_results += len(trades)
            logger.info(f" <<< received total: {len_results}")

        return self._cache.get(since, until)

    def _query_remote_trades(self, since):
        logger.info(f" >>> querying {self._TRADE_URL}, pair={self._pair}, since={since}")
        res = requests.get(self._TRADE_URL, params=dict(pair=self._pair, since=since)).json()
        self._validate(res)
        return res

    def _validate(self, res):
        if self._ERROR_KEY not in res:
            raise InvalidFormatError(f"The Kraken API response is not in an expected format:\n {res}")
        if len(res[self._ERROR_KEY]) != 0:
            raise ResponseError(f"The Kraken API returned an error:\n {res}")
        if self._RESULT_KEY not in res:
            raise InvalidFormatError(f"The Kraken API response is not in an expected format:\n {res}")


def get_last(trades):
    if trades is None:
        return 0
    return int(trades['result']['last'])


def get_trades(res):
    pair = get_pair(res)
    pair_data = res['result'][pair]
    return [[p, v, t] for p, v, t, _, _, _m in pair_data]


def get_pair(trades):
    return first([k for k in trades['result'].keys() if k != 'last'])


class KrakenSource:
    def __init__(self, trades_storage, pair, interval, max_results=1e6):
        self._trades = KrakenTrades(trades_storage, pair, max_results)
        self._interval = interval
        self._adapter = KrakenAdapter(self._interval)
        self._server_time = KrakenServerTime()

    def query(self, since, exclude_outliers=None, z_score_threshold=10):
        last_itv = floor_to_interval(self._server_time.now(), self._interval * 60)
        res = self._trades.get(since, last_itv)
        datums = CsvDatums(self._interval, self._adapter(res))
        try:
            validate(datums, exclude_outliers, z_score_threshold)
        except DataError as e:
            logger.warning("invalid data found:\n", e.args[0])
        return datums


class InvalidFormatError(TypeError):
    pass


class ResponseError(ValueError):
    pass
