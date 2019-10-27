import pytest

from datums_warehouse.warehouse import Warehouse, MissingPacketError


class Data:
    def __init__(self, from_dir, with_interval, with_since=None, with_until=None):
        self.from_dir = from_dir
        self.with_interval = with_interval
        self.with_since = with_since
        self.with_until = with_until
        self.last_time = 1500001000

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
    def __init__(self, directory):
        self.directory = directory

    def get(self, interval, since=None, until=None):
        return Data(from_dir=self.directory, with_interval=interval, with_since=since,
                    with_until=until)

    def last_time_of(self, interval):
        d = self.get(interval)
        return d.last_time


@pytest.fixture(autouse=True)
def storage_factory(monkeypatch):
    import datums_warehouse.warehouse as module_under_test
    monkeypatch.setattr(module_under_test, 'Storage', StorageStub)


class SourceSpy:
    class SourceAPI:
        def __init__(self, owner):
            self.owner = owner

        def query(self, since):
            self.owner.received_query_since = since

    def __init__(self):
        self.type_created = None
        self.with_ctr_kwargs = None
        self.received_query_since = None

    # noinspection PyShadowingBuiltins
    def __call__(self, type, **kwargs):
        self.type_created = type
        self.with_ctr_kwargs = kwargs
        return self.SourceAPI(self)

    def updated_from(self, t, with_kwargs):
        return self.type_created == t and self.with_ctr_kwargs == with_kwargs

    def __repr__(self):
        return f"SourceSpy(): type_created={self.type_created}, with_ctr_kwargs={self.with_ctr_kwargs}"


@pytest.fixture
def source(monkeypatch):
    import datums_warehouse.warehouse as module_under_test
    s = SourceSpy()
    monkeypatch.setattr(module_under_test, 'make_source', s)
    return s


def test_warehouse_raises_an_error_when_accessing_non_existent_packet():
    warehouse = Warehouse({})
    with pytest.raises(MissingPacketError):
        warehouse.retrieve("some_packet_id")
    with pytest.raises(MissingPacketError):
        warehouse.update("some_packet_id")


def test_warehouse_retrieves_specified_packet():
    warehouse = Warehouse({'packet_id': {'storage': "some/directory", 'interval': 30},
                           'other_pkt': {'storage': "some/directory", 'interval': 60}})
    assert warehouse.retrieve('packet_id') == Data(from_dir="some/directory", with_interval=30)


def test_warehouse_retrieves_specified_packet_range():
    warehouse = Warehouse(
        {'packet_id': {'storage': "some/directory", 'interval': 30},
         'other_pkt': {'storage': "some/directory", 'interval': 60}})
    assert warehouse.retrieve('packet_id', since=1500000000, until=1500001000) == Data(from_dir="some/directory",
                                                                                       with_interval=30,
                                                                                       with_since=1500000000,
                                                                                       with_until=1500001000)


@pytest.mark.parametrize('config,packets', [({}, set()),
                                            ({'packet_id': {'storage': "some/directory", 'interval': 30},
                                              'other_pkt': {'storage': "some/directory", 'interval': 60}},
                                             {'packet_id', 'other_pkt'})])
def test_warehouse_reports_all_packets(config, packets):
    warehouse = Warehouse(config)
    assert warehouse.all_packets() == packets


def test_warehouse_updating_with_unknown_source():
    warehouse = Warehouse({'packet_id': {'storage': "some/directory", 'interval': 30,
                                         'source': {'type': "some_source", 'ctr': "args", 'more': 10}}})
    with pytest.raises(NotImplementedError):
        warehouse.update('packet_id')


def test_warehouse_invokes_configured_query(source):
    warehouse = Warehouse({'packet_id': {'storage': "some/directory", 'interval': 30,
                                         'source': {'type': "some_source", 'ctr': "args", 'more': 10}},
                           'other_pkt': {'storage': "some/directory", 'interval': 60}})
    warehouse.update('packet_id')
    assert source.updated_from('some_source', with_kwargs={'interval': 30, 'ctr': "args", 'more': 10})


def test_warehouse_updates_queries_source(source):
    warehouse = Warehouse({'packet_id': {'storage': "some/directory", 'interval': 30,
                                         'source': {'type': "some_source"}},
                           'other_pkt': {'storage': "some/directory", 'interval': 60}})
    last_time = warehouse.retrieve('packet_id').last_time
    warehouse.update('packet_id')
    assert source.received_query_since == last_time + 30
