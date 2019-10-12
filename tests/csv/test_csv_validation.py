import random
from io import StringIO

import pytest

from datums_warehouse.validation import DataError, validate


def test_validate_empty_csv():
    with pytest.raises(DataError) as e:
        validate(StringIO("c1,c2\n"))
    assert "no data" in exception_msg(e)


def exception_msg(e):
    return e.value.args[0]


@pytest.mark.parametrize("missing", [[(2, 1)], [(1, 3)], [(1, 1), (2, 2)]])
def test_contains_missing_elements(missing):
    with pytest.raises(DataError) as e:
        validate(make_csv_with(missing, ""))
    assert line_from(missing) in exception_msg(e) and column_from(missing) in exception_msg(e)


def make_csv_with(locations, value, shape=None):
    lines = make_csv_lines(shape=shape)
    for r, c in locations:
        lines[r][c] = value
    return make_csv(lines)


def make_csv_lines(shape=None):
    if shape is None:
        return [["timestamp", "c1", "c2", "c3"], ["0", "1", "2", "3"], ["1", "2", "3", "4"]]

    header = ["timestamp"] + [f"c{i + 1}" for i in range(shape[0])]
    return [header] + [[str(ts)] + [str(random.random()) for _ in range(shape[0])] for ts in range(shape[1])]


def make_csv(lines):
    return StringIO("\n".join([",".join(r) for r in lines]) + "\n")


def line_from(missing):
    return ", ".join([str(m[0] + 1) for m in missing])


def column_from(missing):
    csv = make_csv_lines()
    return ", ".join([csv[0][m[1]] for m in missing])


@pytest.mark.parametrize("nan", [[(2, 1)], [(1, 3)], [(1, 1), (2, 2)]])
def test_contains_nan_elements(nan):
    with pytest.raises(DataError) as e:
        validate(make_csv_with(nan, "NaN"))
    assert line_from(nan) in exception_msg(e) and column_from(nan) in exception_msg(e)


def test_valid_data():
    validate(make_csv(make_csv_lines()))


@pytest.mark.parametrize("outlier", [[(2, 1)], [(1, 3)], [(1, 1), (2, 2)]])
def test_statistical_outliers(outlier):
    with pytest.raises(DataError) as e:
        validate(make_csv_with(outlier, "5", shape=(3, 1000)))
    assert line_from(outlier) in exception_msg(e) and column_from(outlier) in exception_msg(e)


def test_ignore_configured_statistical_outliers():
    loc = [(2, 1)]
    validate(make_csv_with(loc, "5", shape=(3, 1000)), exclude_outliers=[column_from(loc)])


def test_gap_in_series():
    with pytest.raises(DataError) as e:
        validate(make_csv([["timestamp", "c1", "c2", "c3"],
                           ["0", "1", "2", "3"],
                           ["120", "2", "3", "4"],
                           ["180", "3", "4", "5"],
                           ["300", "4", "5", "6"]]), interval=60)
    assert "2, 4" in exception_msg(e)


def test_z_score_threshold_can_be_configured():
    loc = [(2, 1)]
    with pytest.raises(DataError) as e:
        validate(make_csv_with(loc, "2", shape=(3, 1000)), z_score_threshold=3)
    assert line_from(loc) in exception_msg(e) and column_from(loc) in exception_msg(e)
