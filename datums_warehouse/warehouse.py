from datums_warehouse.source import KrakenSource
from datums_warehouse.storage import Storage


def make_storage(directory):
    return Storage(directory)


# noinspection PyShadowingBuiltins
def make_source(type, **kwargs):
    if type == 'Kraken':
        return KrakenSource(**kwargs)

    raise NotImplementedError(type)


class Warehouse:
    _STORAGE_KEY = 'storage'
    _INTERVAL_KEY = 'interval'
    _SOURCE_KEY = 'source'

    def __init__(self, config):
        self._config = config

    def retrieve(self, pkt_id, since=None, until=None):
        self._validate_packet(pkt_id)
        pkt_cfg = self._config[pkt_id]
        storage = make_storage(pkt_cfg[self._STORAGE_KEY])
        return storage.get(pkt_cfg[self._INTERVAL_KEY], since, until)

    def _validate_packet(self, pkt_id):
        if pkt_id not in self._config:
            raise MissingPacketError(f"the requested packet {pkt_id} has not been configured.")

    def all_packets(self):
        return set(self._config.keys())

    def update(self, pkt_id):
        self._validate_packet(pkt_id)
        pkt_cfg = self._config[pkt_id]
        src_cfg = pkt_cfg[self._SOURCE_KEY]
        interval = pkt_cfg[self._INTERVAL_KEY]
        src_cfg['interval'] = interval
        src = make_source(**src_cfg)
        storage = make_storage(pkt_cfg[self._STORAGE_KEY])
        src.query(storage.last_time_of(interval) + interval)


class MissingPacketError(IOError):
    pass
