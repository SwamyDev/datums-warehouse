from io import StringIO
from pathlib import Path

import logging
import pandas as pd


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
        for file in self._directory.glob(f"{itv}__*.csv"):
            prv = pd.read_csv(file)
            if self._can_concatenate(new_df, prv, itv):
                new_df = pd.concat([prv, new_df]).drop_duplicates(subset='timestamp')\
                    .reset_index(drop=True)
                return new_df, file
        return new_df, None

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
        file = self._directory / f"{itv}__{first}_{last}.csv"   # TODO: allow for compression
        df.to_csv(file, index=False)
        if prv is None:
            logger.info(f"creating new csv storage: {file}")
        else:
            prv.unlink()


class InvalidDatumError(ValueError):
    pass
