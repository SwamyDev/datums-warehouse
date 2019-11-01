from pathlib import Path

from datums_warehouse.broker.source import KrakenSource
from datums_warehouse.broker.storage import Storage
from datums_warehouse.broker.validation import validate


def make_storage(storage, pair):  # pragma: no cover simple factory function
    return Storage(Path(storage) / pair)


def make_source(src_type, interval, pair):  # pragma: no cover simple factory function
    if src_type == 'Kraken':
        return KrakenSource(interval, pair)

    raise NotImplementedError(type)


class Warehouse:
    _STORAGE_KEY = 'storage'
    _INTERVAL_KEY = 'interval'
    _PAIR_KEY = 'pair'
    _SOURCE_KEY = 'source'
    _EXCLUDE_OUTLIERS_KEY = 'exclude_outliers'
    _Z_THRESHOLD_KEY = 'z_score_threshold'

    def __init__(self, config):
        self._config = config

    def retrieve(self, pkt_id, since=None, until=None):
        self._validate_packet(pkt_id)
        pkt_cfg = self._config[pkt_id]
        storage = make_storage(pkt_cfg[self._STORAGE_KEY], pkt_cfg[self._PAIR_KEY])
        datums = storage.get(pkt_cfg[self._INTERVAL_KEY], since, until)
        validate(datums, pkt_cfg.get(self._EXCLUDE_OUTLIERS_KEY, None), pkt_cfg.get(self._Z_THRESHOLD_KEY, 10))
        return datums

    def _validate_packet(self, pkt_id):
        if pkt_id not in self._config:
            raise MissingPacketError(f"the requested packet {pkt_id} has not been configured.")

    def all_packets(self):
        return set(self._config.keys())

    def update(self, pkt_id):
        self._validate_packet(pkt_id)
        pkt_cfg = self._config[pkt_id]
        interval = pkt_cfg[self._INTERVAL_KEY]
        pair = pkt_cfg[self._PAIR_KEY]
        src = make_source(pkt_cfg[self._SOURCE_KEY], interval, pair)
        storage = make_storage(pkt_cfg[self._STORAGE_KEY], pair)
        src.query(storage.last_time_of(interval) + interval)


class MissingPacketError(IOError):
    pass
