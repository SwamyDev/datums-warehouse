import pytest

from datums_warehouse.broker.cache import TradesCache


@pytest.fixture
def cache_file(tmp_path):
    return tmp_path / 'cache'


@pytest.fixture
def cache(cache_file):
    with TradesCache(cache_file) as c:
        yield c


def seconds_to_ns(sec):
    return int(sec * 1e9)


def test_update_and_query(cache):
    cache.update([
        [10.0, 0.1, 1500000000.0],
        [10.0, 0.1, 1500000001.3],
    ], seconds_to_ns(1500000002))
    assert cache.get(0, 1500000003.0) == [(10.0, 0.1, 1500000000.0), (10.0, 0.1, 1500000001.3)]


@pytest.mark.parametrize('since, until, expected', [
    (0, 1500000002.0, [(10.0, 0.1, 1500000000.0), (11.0, 0.2, 1500000001.3)]),
    (1500000002.0, 1500000005.0, [(12.0, 0.3, 1500000002.5), (13.0, 0.4, 1500000003.5)]),
    (1500000001.3, 1500000002.5, [(11.0, 0.2, 1500000001.3), (12.0, 0.3, 1500000002.5)]),
    (1500000002.5, 1500000002.5, [(12.0, 0.3, 1500000002.5)]),
])
def test_select_query(cache, since, until, expected):
    cache.update([
        [10.0, 0.1, 1500000000.0],
        [11.0, 0.2, 1500000001.3],
        [12.0, 0.3, 1500000002.5],
        [13.0, 0.4, 1500000003.5],
    ], seconds_to_ns(1500000004))
    assert cache.get(since, until) == expected


def test_update_last_timestamp(cache):
    cache.update([
        [10.0, 0.1, 1500000000.0],
        [10.0, 0.1, 1500000001.3],
    ], seconds_to_ns(1500000002))
    assert cache.last_timestamp() == seconds_to_ns(1500000002)


def test_the_cache_is_persistent(cache_file):
    with TradesCache(cache_file) as cache:
        cache.update([[10.0, 0.1, 1500000000.0], [11.0, 0.2, 1500000001.3]], seconds_to_ns(1500000002))
    with TradesCache(cache_file) as cache:
        cache.update([[12.0, 0.3, 1500000002.5], [13.0, 0.4, 1500000003.5]], seconds_to_ns(1500000004))
    with TradesCache(cache_file) as cache:
        assert cache.last_timestamp() == seconds_to_ns(1500000004)
        assert cache.get(0, 1500000004) == [(10.0, 0.1, 1500000000.0), (11.0, 0.2, 1500000001.3),
                                            (12.0, 0.3, 1500000002.5), (13.0, 0.4, 1500000003.5)]
