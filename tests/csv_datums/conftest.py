import pytest

from datums_warehouse.broker.datums import CsvDatums


@pytest.fixture
def make_csv_datums():
    def csv_datums_fac(itv, csv):
        return CsvDatums(itv, csv)

    return csv_datums_fac
