import requests

from datums_warehouse.broker.adapters import KrakenAdapter
from datums_warehouse.broker.datums import CsvDatums
from datums_warehouse.broker.validation import validate


def to_nano_sec(t):
    return int(t * 1e9)


class KrakenSource:
    _TRADE_URL = "https://api.kraken.com/0/public/Trades"

    def __init__(self, pair, interval):
        self._pair = pair
        self._interval = interval
        self._adapter = KrakenAdapter(self._interval)

    def query(self, since, exclude_outliers=None, z_score_threshold=10):
        res = requests.get(self._TRADE_URL, params=dict(pair=self._pair, since=to_nano_sec(since)))
        datums = CsvDatums(self._interval, self._adapter(res.json()))
        return validate(datums, exclude_outliers, z_score_threshold)
