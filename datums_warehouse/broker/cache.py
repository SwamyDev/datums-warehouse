import struct
from pathlib import Path


class TradesCache:
    _TRADE = struct.Struct('<ddd')

    def __init__(self, file):
        self._file = Path(file)
        self._last_file = self._file.with_name('cache_last')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def update(self, trades, last):
        with open(self._file, mode='ab') as file:
            for trade in trades:
                file.write(self._TRADE.pack(*trade))

        with open(self._last_file, mode='w') as file:
            file.write(str(last))

    def get(self, since, until):
        if not self._file.exists():
            return []

        with open(self._file, mode='rb') as file:
            return [trade for trade in self._read_trades(file) if since <= trade[2] <= until]

    def _read_trades(self, file):
        n = self._TRADE.size
        buf = file.read(n)
        while buf:
            yield list(self._TRADE.unpack(buf))
            buf = file.read(n)

    def last_timestamp(self):
        if not self._file.exists():
            return 0

        with open(self._last_file, mode='r') as file:
            return int(file.read())
