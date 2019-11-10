from more_itertools import one

from datums_warehouse.broker.datums import floor_to_interval


def truncate(x, digits):
    return float(int(x * (10 ** digits))) / float(10 ** digits)


class KrakenAdapter:
    _HEADER = "timestamp,open,high,low,close,vwap,volume,count"
    _RESULT_KEY = "result"
    _ERROR_KEY = "error"
    _LAST_KEY = "last"

    def __init__(self, interval):
        self._interval = interval * 60

    def __call__(self, raw):
        self._validate(raw)
        trades = self._get_trades(raw)
        return "\n".join(self._make_csv_lines(trades))

    def _validate(self, raw):
        if self._ERROR_KEY not in raw:
            raise InvalidFormatError(f"The Kraken API response is not in an expected format:\n {raw}")
        if len(raw[self._ERROR_KEY]) != 0:
            raise ResponseError(f"The Kraken API returned an error:\n {raw}")
        if self._RESULT_KEY not in raw:
            raise InvalidFormatError(f"The Kraken API response is not in an expected format:\n {raw}")

    def _get_trades(self, raw):
        res = raw[self._RESULT_KEY]
        pair = one((k for k in res.keys() if k != self._LAST_KEY))
        trades = res[pair]
        return trades

    def _make_csv_lines(self, trades):
        csv = [self._HEADER]
        if len(trades) == 0:
            return csv

        for itv in self._chunked_by_interval(trades):
            ps, vs, ts = self._transpose([[float(p), float(v), int(t)] for p, v, t, _, _, _ in itv])
            csv.append(self._make_line(ps, ts, vs))
        return csv

    def _chunked_by_interval(self, trades):
        prev_itv = None
        buffer = []
        for trade in trades:
            ts = self._floor_to_interval(int(trade[2]))
            if prev_itv is None:
                prev_itv = self._floor_to_interval(ts)
            if (ts - prev_itv) >= self._interval:
                prev_itv = ts
                if len(buffer) > 0:
                    yield list(buffer)
                    buffer.clear()

            buffer.append(trade)

    def _floor_to_interval(self, ts):
        interval = self._interval
        return floor_to_interval(ts, interval)

    @staticmethod
    def _transpose(trs):
        return map(list, zip(*trs))

    def _make_line(self, ps, ts, vs):
        ttl_v = sum(vs)
        vwap = truncate(sum((p * v for p, v in zip(ps, vs))) / ttl_v, 1)
        ttl_v = round(ttl_v, 8)
        t = self._floor_to_interval(min(ts))
        line = f"{t},{ps[0]},{max(ps)},{min(ps)},{ps[-1]},{vwap},{ttl_v},{len(ts)}"
        return line


class InvalidFormatError(TypeError):
    pass


class ResponseError(ValueError):
    pass
