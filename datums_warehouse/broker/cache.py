import io
import math
import struct
import zlib
from pathlib import Path

from more_itertools import flatten


class TradesCache:
    _TRADE = struct.Struct('<ddd')
    _HEADER = struct.Struct('<II')

    def __init__(self, file):
        self._file = Path(file)
        self._last_file = self._file.with_name('cache_last')

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def update(self, trades, last):
        if len(trades) > 0:
            with open(self._file, mode='ab') as file:
                raw = struct.pack(f'<{len(trades) * 3}d', *flatten(trades))
                raw = zlib.compress(raw)
                file.write(self._HEADER.pack(len(raw), int(math.ceil(trades[-1][2]))))
                file.write(raw)

        with open(self._last_file, mode='w') as file:
            file.write(str(last))

    def get(self, since, until):
        if not self._file.exists():
            return []

        with open(self._file, mode='rb') as file:
            return [trade for trade in self._read_trades(file, since) if since <= trade[2] <= until]

    def _read_trades(self, file, since):
        for size, last in self._read_headers(file):
            if last < since:
                file.seek(size, io.SEEK_CUR)
            else:
                raw = zlib.decompress(file.read(size))
                for cnk in _chunked(raw, self._TRADE.size):
                    yield list(self._TRADE.unpack(cnk))

    def _read_headers(self, file):
        buf = file.read(self._HEADER.size)
        while buf:
            yield self._HEADER.unpack(buf)
            buf = file.read(self._HEADER.size)

    def last_timestamp(self):
        if not self._last_file.exists():
            return 0

        with open(self._last_file, mode='r') as file:
            return int(file.read())


def _chunked(buffer, n):
    for i in range(0, len(buffer), n):
        yield buffer[i:i + n]
