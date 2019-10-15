import logging

import pytest

from datums_warehouse.storage import InvalidDatumError, Storage


@pytest.fixture
def datum_path(tmp_path):
    return tmp_path / "DTN_NME"


@pytest.fixture
def storage(datum_path):
    datum_path.mkdir(exist_ok=True)
    return Storage(datum_path)


@pytest.mark.parametrize("empty", ["", "timestamp,c1,c2,c3\n"])
def test_empty_or_single_line_datums_raise_error(storage, empty, make_csv_datums):
    with pytest.raises(InvalidDatumError):
        storage.store(make_csv_datums(1, empty))


@pytest.mark.parametrize("csv", ["timestamp,c1,c2\n0,1,1\n1,2,2\n",
                                 "timestamp,c1,c2\n0,1,1\n1,2,3\n"])
def test_store_new_datums_in_file(storage, datum_path, csv, make_csv_datums):
    storage.store(make_csv_datums(1, csv))
    assert read_gz(datum_path / "1__0_1.gz") == csv


def read_gz(gz):
    import gzip
    with gzip.open(gz, mode='rt') as f:
        return f.read()


@pytest.mark.parametrize("freq", [1, 2])
def test_datums_are_stored_with_freq_in_file_name(storage, datum_path, freq, make_csv_datums):
    storage.store(make_csv_datums(freq, f"timestamp,c1,c2\n0,1,1\n{freq},2,2"))
    assert (datum_path / f"{freq}__0_{freq}.gz").exists()


def test_datums_are_stored_with_last_time_stamp_in_name(storage, datum_path, make_csv_datums):
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n"))
    assert (datum_path / "1__0_2.gz").exists()


def test_update_existing_datum(storage, datum_path, make_csv_datums):
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n"))
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n3,2,2\n"))
    assert read_gz(datum_path / "1__0_3.gz") == "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n3,2,2\n"


def test_create_new_file_for_different_frequency(storage, datum_path, make_csv_datums):
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n"))
    storage.store(make_csv_datums(60, "timestamp,c1,c2\n3,2,2\n"))
    assert (datum_path / "1__0_2.gz").exists()
    assert (datum_path / "60__3_3.gz").exists()


def test_datums_are_merged_without_duplicates(storage, datum_path, make_csv_datums):
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n"))
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n2,3,4\n3,2,2\n"))
    assert read_gz(datum_path / "1__0_3.gz") == "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n3,2,2\n"


def test_start_new_file_when_there_is_a_gap_in_frequency(storage, datum_path, make_csv_datums):
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n"))
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n4,2,2\n5,1,1\n"))
    assert read_gz(datum_path / "1__0_2.gz") == "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n"
    assert read_gz(datum_path / "1__4_5.gz") == "timestamp,c1,c2\n4,2,2\n5,1,1\n"


def test_append_to_correct_file(storage, datum_path, make_csv_datums):
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n"))
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n4,2,2\n5,1,1\n"))
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n3,2,2\n"))
    assert read_gz(datum_path / "1__0_3.gz") == "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n3,2,2\n"


def test_log_if_new_file_is_created(storage, caplog, make_csv_datums):
    caplog.set_level(logging.INFO)
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n"))
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n4,2,2\n5,1,1\n"))
    assert "1__0_2.gz" in caplog.text and "1__4_5.gz" in caplog.text


def test_do_not_log_when_no_new_file_is_created(storage, caplog, make_csv_datums):
    caplog.set_level(logging.INFO)
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n"))
    caplog.clear()
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n2,3,4\n3,2,2\n"))
    assert len(caplog.records) == 0


def test_delete_old_file_when_appended(storage, datum_path, make_csv_datums):
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n0,1,1\n1,2,2\n2,3,3\n"))
    storage.store(make_csv_datums(1, "timestamp,c1,c2\n2,3,4\n3,2,2\n"))
    assert not (datum_path / "1__0_2.gz").exists()
