import logging
from io import StringIO
from pathlib import Path

import pandas as pd

from datums_warehouse.datums import CsvDatums

logger = logging.getLogger(__name__)


class Storage:
    def __init__(self, directory):
        self._directory = Path(directory)

    def store(self, datums):
        df = self._read_csv(datums.csv)
        df, prv = self._maybe_prepend_existing(df, datums.interval)
        self._write_csv(df, datums.interval, prv)

    @staticmethod
    def _read_csv(csv):
        if len(csv) == 0:
            raise InvalidDatumError(f"the datum values are empty")
        df = pd.read_csv(StringIO(csv))
        if df.empty:
            raise InvalidDatumError(f"the datum string has now values:\n {csv}")
        return df

    def _maybe_prepend_existing(self, new_df, itv):
        for prv, file in self._all_of(itv):
            if self._can_concatenate(new_df, prv, itv):
                new_df = pd.concat([prv, new_df]).drop_duplicates(subset='timestamp') \
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
        else:
            prv.unlink()

    def last_time_of(self, interval):
        _, last_time = self._get_last_of(interval, until=None)
        return last_time

    def _get_last_of(self, interval, until):
        last_time = None
        last_df = None
        for df, _ in self._all_of(interval):
            if until and df.timestamp.iloc[0] > until:
                continue

            lt = df.timestamp.iloc[-1]
            last_time = last_time or lt
            if lt >= last_time:
                last_time = lt
                last_df = df

        return last_df, last_time

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
