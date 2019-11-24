import pytest

from datums_warehouse.scripts.update import update_pairs


class WarehouseSpy:
    def __init__(self):
        self.received_pairs = []

    def update(self, pair):
        import time
        import random
        time.sleep(random.uniform(0.01, 0.1))
        self.received_pairs.append(pair)


@pytest.fixture
def warehouse():
    return WarehouseSpy()


@pytest.fixture(autouse=True)
def inject_warehouse(monkeypatch, warehouse):
    import datums_warehouse.scripts.update as mut
    monkeypatch.setattr(mut, 'make_warehouse', lambda cfg: warehouse)
    return warehouse


def test_update_single_pair(warehouse):
    update_pairs(warehouse, ['pair'])
    assert warehouse.received_pairs == ['pair']


def test_update_multiple_pairs(warehouse):
    update_pairs(warehouse, ['A', 'B'])
    assert {'A', 'B'} == set(warehouse.received_pairs)
