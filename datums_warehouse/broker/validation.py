import numpy as np
import pandas as pd


def validate(datums, exclude_outliers=None, z_score_threshold=10):
    df = pd.read_csv(datums.csv)
    if df.empty:
        raise DataError("no data has been found")

    _check_elements(df, 'missing', df.isnull())
    _check_elements(df, 'outlier', _make_outlier_mask(df, exclude_outliers or [], z_score_threshold))
    _check_index_interval(df, datums.interval)


def _check_elements(df, label, elements):
    if not any(elements.any()):
        return

    rws, cls = _find_location(df, elements)
    lines = _indices_to_lines(rws.values)
    raise DataError(f"{label} data found at lines {', '.join(lines)}, columns {', '.join(cls.values)}")


def _find_location(df, elements):
    cls = df.columns[elements.any()]
    rws = df[elements.any(axis=1)][cls].index
    return rws, cls


def _indices_to_lines(idc):
    return [str(v + 2) for v in idc]


def _make_outlier_mask(df, exclude, threshold):
    mask = pd.DataFrame()
    exclude.append('timestamp')
    for col in df.columns:
        if col in exclude:
            mask[col] = False
        else:
            mask[col] = abs((df[col] - df[col].mean()) / df[col].std(ddof=0))
            mask[col] = mask[col].apply(lambda x: x > threshold)

    return mask


def _check_index_interval(df, interval):
    diff = df.timestamp.values[1:] - df.timestamp.values[:-1]
    lines = _indices_to_lines(np.where(diff != interval)[0])
    if len(lines) > 0:
        raise DataError(f"gap in the time series found at lines {', '.join(lines)}")


class DataError(ValueError):
    pass
