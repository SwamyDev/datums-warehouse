import pytest

from datums_warehouse.scripts.update import update_pairs


class WarehouseSpy:
    def __init__(self):
        self.received_pair = None

    def update(self, pair):
        import time
        import random
        time.sleep(random.uniform(0.01, 0.1))
        self.received_pair = pair


@pytest.fixture
def warehouse():
    return WarehouseSpy()


def test_update_single_pair(warehouse):
    update_pairs(warehouse, ['pair'])
    assert warehouse.received_pair == 'pair'


def test_update_multiple_pairs(warehouse):
    update_pairs(warehouse, ['pair'])
    assert warehouse.received_pair == 'pair'
