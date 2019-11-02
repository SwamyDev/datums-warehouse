from pathlib import Path

import pytest

from datums_warehouse.broker.warehouse import Warehouse, MissingPacketError


class Data:
    def __init__(self, from_dir, with_interval, with_since=None, with_until=None):
        self.from_dir = Path(from_dir)
        self.with_interval = with_interval
        self.with_since = with_since
        self.with_until = with_until

    def __repr__(self):
        optionals = []
        if self.with_since:
            optionals.append(f"with_since={self.with_since}")
        if self.with_until:
            optionals.append(f"with_since={self.with_until}")

        return f"Data(from_dir={self.from_dir}, with_interval={self.with_interval}{', '.join(optionals)})"

    def __eq__(self, other):
        return self.from_dir == other.from_dir and self.with_interval == other.with_interval and \
               self.with_since == other.with_since and self.with_until == other.with_until


class StorageStub:
    class StorageAPI:
        def __init__(self, owner, storage, pair):
            self.owner = owner
            self.storage = storage
            self.pair = pair

        def exists(self, interval):
            return self.owner.exists

        def get(self, interval, since=None, until=None):
            directory = Path(self.storage) / self.pair
            return Data(from_dir=directory, with_interval=interval, with_since=since, with_until=until)

        def last_time_of(self, interval):
            return self.owner.last_times.get(self.owner.make_key_for(self.storage, interval, self.pair), 0)

        def store(self, datums):
            self.owner.received_datums = datums

    def __init__(self):
        self.last_times = dict()
        self.exists = True
        self.received_datums = None

    def __call__(self, storage, pair):
        return self.StorageAPI(self, storage, pair)

    def last_time_of(self, storage, interval, pair):
        class _Proxy:
            def __init__(self, owner, key):
                self.owner = owner
                self.key = key

            def set(self, time):
                self.owner.last_times[self.key] = time

        return _Proxy(self, self.make_key_for(storage, interval, pair))

    @staticmethod
    def make_key_for(storage, interval, pair):
        return "".join(sorted(map(str, [storage, interval, pair])))

    def set_not_existent(self):
        self.exists = False

    def stored(self, datums):
        return self.received_datums == datums


@pytest.fixture(autouse=True)
def storage(monkeypatch):
    import datums_warehouse.broker.warehouse as module_under_test
    s = StorageStub()
    monkeypatch.setattr(module_under_test, 'make_storage', s)
    return s


class SourceSpy:
    class SourceAPI:
        def __init__(self, owner):
            self.owner = owner

        def query(self, since, exclude_outliers=None, z_score_threshold=10):
            self.owner.received_query_since = since
            self.owner.received_validation_cfg = dict(exclude_outliers=exclude_outliers,
                                                      z_score_threshold=z_score_threshold)
            self.owner.returned_datums = Data(from_dir='remote_source', with_interval=30, with_since=since)
            return self.owner.returned_datums

    def __init__(self):
        self.type_created = None
        self.with_interval = None
        self.with_pair = None
        self.received_query_since = None
        self.received_validation_cfg = None
        self.returned_datums = None

    def __call__(self, source_type, pair, interval):
        self.type_created = source_type
        self.with_interval = interval
        self.with_pair = pair
        return self.SourceAPI(self)

    def updated_from(self, t, with_interval, with_pair):
        return self.type_created == t and self.with_interval == with_interval and self.with_pair == with_pair

    def __repr__(self):
        return f"SourceSpy(): type_created={self.type_created}, with_interval={self.with_interval}, with_pair={self.with_pair}"


@pytest.fixture
def source(monkeypatch):
    import datums_warehouse.broker.warehouse as module_under_test
    s = SourceSpy()
    monkeypatch.setattr(module_under_test, 'make_source', s)
    return s


class ValidatorSpy:
    def __init__(self):
        self.received = None

    def __call__(self, datums, exclude_outliers=None, z_score_threshold=10):
        self.received = dict(datums=datums, exclude_outliers=exclude_outliers, z_score_threshold=z_score_threshold)
        return datums


@pytest.fixture(autouse=True)
def validator(monkeypatch):
    import datums_warehouse.broker.warehouse as module_under_test
    s = ValidatorSpy()
    monkeypatch.setattr(module_under_test, 'validate', s)
    return s


def test_warehouse_raises_an_error_when_accessing_non_existent_packet():
    warehouse = Warehouse({})
    with pytest.raises(MissingPacketError):
        warehouse.retrieve("some_packet_id")
    with pytest.raises(MissingPacketError):
        warehouse.update("some_packet_id")


def test_warehouse_retrieves_specified_packet():
    warehouse = Warehouse({'packet_id': {'storage': "some/directory", 'interval': 30, 'pair': 'SMNPAR'},
                           'other_pkt': {'storage': "some/directory", 'interval': 60, 'pair': 'SMNPAR'}})
    assert warehouse.retrieve('packet_id') == Data(from_dir="some/directory/SMNPAR", with_interval=30)


def test_warehouse_retrieves_specified_packet_range():
    warehouse = Warehouse(
        {'packet_id': {'storage': "some/directory", 'interval': 30, 'pair': 'SMNPAR'},
         'other_pkt': {'storage': "some/directory", 'interval': 60, 'pair': 'SMNPAR'}})
    assert warehouse.retrieve('packet_id', since=1500000000, until=1500001000) == Data(from_dir="some/directory/SMNPAR",
                                                                                       with_interval=30,
                                                                                       with_since=1500000000,
                                                                                       with_until=1500001000)


def test_warehouse_validates_packet_with_specified_config(validator):
    warehouse = Warehouse({'packet_id': {'storage': "some/directory", 'interval': 30, 'pair': 'SMNPAR',
                                         'exclude_outliers': ['vwap'], 'z_score_threshold': 5}})
    datums = warehouse.retrieve('packet_id')
    assert validator.received == dict(datums=datums, exclude_outliers=['vwap'], z_score_threshold=5)


@pytest.mark.parametrize('config,packets', [({}, set()),
                                            ({'packet_id': {'storage': "some/directory", 'interval': 30,
                                                            'pair': 'SMNPAR'},
                                              'other_pkt': {'storage': "some/directory", 'interval': 60,
                                                            'pair': 'SMNPAR'}},
                                             {'packet_id', 'other_pkt'})])
def test_warehouse_reports_all_packets(config, packets):
    warehouse = Warehouse(config)
    assert warehouse.all_packets() == packets


def test_warehouse_updating_with_unknown_source():
    warehouse = Warehouse({'packet_id': {'storage': "some/directory", 'interval': 30, 'pair': 'SMNPAR',
                                         'source': "some_source"}})
    with pytest.raises(NotImplementedError):
        warehouse.update('packet_id')


def test_warehouse_invokes_configured_query(source):
    warehouse = Warehouse({'packet_id': {'storage': "some/directory", 'interval': 30, 'pair': 'SMNPAR',
                                         'source': "some_source"},
                           'other_pkt': {'storage': "some/directory", 'interval': 60, 'pair': 'SMNPAR', }})
    warehouse.update('packet_id')
    assert source.updated_from('some_source', with_interval=30, with_pair="SMNPAR")


def test_warehouse_passes_validation_config_along_to_query(source):
    warehouse = Warehouse({'packet_id': {'storage': "some/directory", 'interval': 30, 'pair': 'SMNPAR',
                                         'source': "some_source", 'exclude_outliers': ['vwap'],
                                         'z_score_threshold': 5}})
    warehouse.update('packet_id')
    assert source.received_validation_cfg == dict(exclude_outliers=['vwap'], z_score_threshold=5)


def test_warehouse_stores_queried_updates(source, storage):
    warehouse = Warehouse({'packet_id': {'storage': "some/directory", 'interval': 30, 'pair': 'SMNPAR',
                                         'source': "some_source"},
                           'other_pkt': {'storage': "some/directory", 'interval': 60, 'pair': 'SMNPAR', }})
    warehouse.update('packet_id')
    storage.stored(source.returned_datums)


def test_warehouse_updates_queries_source(source, storage):
    cfg = {'packet_id': {'storage': "some/directory", 'interval': 30, 'pair': 'SMNPAR',
                         'source': {'type': "some_source"}},
           'other_pkt': {'storage': "some/directory", 'interval': 60, 'pair': 'SMNPAR'}}
    warehouse = Warehouse(cfg)
    storage.last_time_of("some/directory", interval=30, pair='SMNPAR').set(15000)
    warehouse.update('packet_id')
    assert source.received_query_since == 15000 + 30


def test_warehouse_updates_sources_from_zero_if_they_do_not_exist_yet(source, storage):
    cfg = {'packet_id': {'storage': "some/directory", 'interval': 30, 'pair': 'SMNPAR',
                         'source': {'type': "some_source"}},
           'other_pkt': {'storage': "some/directory", 'interval': 60, 'pair': 'SMNPAR'}}
    warehouse = Warehouse(cfg)
    storage.set_not_existent()
    warehouse.update('packet_id')
    assert source.received_query_since == 0
