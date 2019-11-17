import gzip
import json
import logging
import time
from pathlib import Path

import requests
from more_itertools import first

from datums_warehouse.broker.adapters import KrakenAdapter
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
        self._cache = Path(cache_dir) / self._pair / "kraken_cache.gz"
        self._max_results = max_results

    def get(self, since, until):
        # TODO: Refactor caching and querying removing the code duplication - maybe by using an index and extracting
        # the cache into its own class
        res = self._get_cached(since, until)
        len_results = get_len(res)
        while get_last(res) < until and len_results < self._max_results:
            time.sleep(LEDGER_FREQUENCY)
            next_trades = self._query_remote_trades(get_last(res) or since)
            self._update_cache(next_trades)
            res = combine_trades(res, next_trades)
            len_results = get_len(res)
            logger.info(f" <<< received total: {len_results}")

        return res

    def _get_cached(self, since, until):
        if not self._cache.exists():
            return None

        with gzip.open(self._cache, mode='rb') as file:
            combined = None
            start = time.time()
            for line in file:
                trades = json.loads(line, encoding='utf-8')
                if get_last(trades) >= since:
                    combined = combine_trades(combined, trades)
                if get_last(combined) >= until or get_len(combined) >= self._max_results:
                    break
            duration = time.time() - start
            logger.debug(f"[performance] cache lookup took: {duration}s")

        logger.info(f" <<< cached, pair={self._pair}, since={since}, until={until}")
        return combined

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

    def _update_cache(self, trades):
        logger.info(f" >>> cache update, pair={self._pair}, len={get_len(trades)}")
        self._cache.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(self._cache, mode='ab') as file:
            file.write(f"{json.dumps(trades)}\n".encode())


def get_last(trades):
    if trades is None:
        return 0
    return int(trades['result']['last'])


def get_len(trades):
    if trades is None:
        return 0
    trades = get_trades(trades)
    return len(trades)


def get_trades(trades):
    pair = get_pair(trades)
    trades = trades['result'][pair]
    return trades


def combine_trades(lhs, rhs):
    if lhs is None:
        return rhs
    if rhs is None:
        return lhs

    lhs['result']['last'] = str(get_last(rhs))
    pair = get_pair(lhs)
    lhs['result'][pair] += rhs['result'][pair]
    return lhs


def get_pair(trades):
    return first([k for k in trades['result'].keys() if k != 'last'])


class KrakenSource:
    def __init__(self, trades_storage, pair, interval, max_results=1e6):
        self._trades = KrakenTrades(trades_storage, pair, max_results)
        self._interval = interval
        self._adapter = KrakenAdapter(self._interval)
        self._server_time = KrakenServerTime()

    def query(self, since, exclude_outliers=None, z_score_threshold=10):
        last_itv = to_nano_sec(floor_to_interval(self._server_time.now(), self._interval * 60))
        res = self._trades.get(to_nano_sec(since), last_itv)
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
