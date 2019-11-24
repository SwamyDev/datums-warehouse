import logging
from io import StringIO
from pathlib import Path

import pandas as pd
from more_itertools import first

from datums_warehouse.broker.datums import CsvDatums

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, directory):
        self._directory = Path(directory)

    def exists(self, interval):
        if not self._directory.exists():
            return False
        return first(self._all_of(interval), None) is not None

    def store(self, datums):
        self._directory.mkdir(parents=True, exist_ok=True)
        df = self._read_csv(datums.csv)
        df, prv = self._maybe_prepend_existing(df, datums.interval)
        self._write_csv(df, datums.interval, prv)

    @staticmethod
    def _read_csv(csv):
        if len(csv) == 0:
            raise InvalidDatumError(f"the datum values are empty")
        df = pd.read_csv(StringIO(csv))
        if df.empty:
            raise InvalidDatumError(f"the datum string has no values:\n {csv}")
        return df

    def _maybe_prepend_existing(self, new_df, itv):
        for prv, file in self._all_of(itv):
            if self._can_concatenate(new_df, prv, itv):
                new_df = pd.concat([prv, new_df]).drop_duplicates(subset='timestamp', keep='last') \
                    .reset_index(drop=True)
                return new_df, file
        return new_df, None

    def _all_of(self, interval):
        for file in self._directory.glob(f"{interval}__*.gz"):
            yield pd.read_csv(file), file

    @staticmethod
    def _can_concatenate(new_df, prv_df, itv):
        fst = new_df.timestamp.iloc[0]
        lst = prv_df.timestamp.iloc[-1]
        frq_connect = fst <= lst or (fst - lst) == itv
        is_after = fst > prv_df.timestamp.iloc[0]
        return frq_connect and is_after

    def _write_csv(self, df, itv, prv):
        first = df.timestamp.iloc[0]
        last = df.timestamp.iloc[-1]
        file = self._directory / f"{itv}__{first}_{last}.gz"
        df.to_csv(file, index=False, compression="infer")
        if prv is None:
            logger.info(f"creating new csv storage: {file}")
        elif file != prv:
            prv.unlink()

    def last_time_of(self, interval):
        _, last_time = self._get_last_of(interval, until=None)
        return last_time

    def _get_last_of(self, interval, until):
        def only_df(tp):
            return tp[0]

        def starts_before_until(df):
            return until is None or df.timestamp.iloc[0] <= until

        def last_ts(df):
            return df.timestamp.iloc[-1]

        all_including_until = filter(starts_before_until, map(only_df, self._all_of(interval)))
        last_df = max(all_including_until, key=last_ts)
        return last_df, last_df.timestamp.iloc[-1]

    def get(self, interval, since=None, until=None):
        df = self._get_in_range(interval, since, until)
        return CsvDatums(interval, df.to_csv(index=False))

    def _get_in_range(self, interval, since, until):
        selected_df, _ = self._get_last_of(interval, until)
        if since and since > selected_df.timestamp.iloc[0]:
            selected_df = selected_df[selected_df.timestamp >= since]
        if until and until < selected_df.timestamp.iloc[-1]:
            selected_df = selected_df[selected_df.timestamp <= until]
        return selected_df


class InvalidDatumError(ValueError):
    pass
