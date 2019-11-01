import requests

from datums_warehouse.adapters import KrakenAdapter
from datums_warehouse.datums import CsvDatums
from datums_warehouse.validation import validate


def to_nano_sec(t):
    return t * 1e9


class KrakenSource:
    _TRADE_URL = "https://api.kraken.com/0/public/Trades"

    def __init__(self, pair, interval):
        self._pair = pair
        self._interval = interval
        self._adapter = KrakenAdapter(self._interval)

    def query(self, since):
        res = requests.get(self._TRADE_URL, params=dict(pair=self._pair, since=to_nano_sec(since)))
        return validate(CsvDatums(self._interval, self._adapter(res.json())))
