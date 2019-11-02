from collections import namedtuple

import pytest
from more_itertools import first


def test_requesting_non_existent_data(query):
    assert "unknown" in query.symbol("unknown").json['error']


def test_request_existing_data(query, valid_datums):
    for datums in valid_datums:
        assert query.symbol(*parameters(datums)).json['csv'] == datums['csv']


def parameters(datum):
    return datum['pair'], datum['interval']


@pytest.fixture
def fst_datum(valid_datums):
    return first(valid_datums)


@pytest.fixture
def fst_range(fst_datum):
    return fst_datum['range']


@pytest.fixture(params=[
    (300, -300),
    (-60, 0),
    (0, 60),
])
def ranges(request, fst_range):
    return namedtuple('RangeCpy', ['min', 'max'])(fst_range.min + request.param[0], fst_range.max + request.param[0])


def test_request_data_range(query, fst_datum, fst_range, ranges):
    csv = query.symbol(*parameters(fst_datum), since=ranges.min, until=ranges.max).json['csv']
    assert csv == slice_csv(fst_datum['csv'], start=max(ranges.min, fst_range.min), stop=min(ranges.max, fst_range.max))


def slice_csv(csv, start, stop):
    lines = csv.split()
    header = lines[0] + "\n"

    def in_range(line):
        t = int(line.split(',')[0])
        return start <= t <= stop

    return header + "\n".join([l for l in lines[1:] if in_range(l)]) + "\n"


def test_request_only_since(query, fst_datum, fst_range):
    csv = query.symbol(*parameters(fst_datum), since=fst_range.min + 300).json['csv']
    assert csv == slice_csv(fst_datum['csv'], start=fst_range.min + 300, stop=fst_range.max)


def test_retrieving_invalid_csv_data(query, fragmented_datums):
    assert 'gap' in query.symbol(*parameters(first(fragmented_datums))).json['error']
