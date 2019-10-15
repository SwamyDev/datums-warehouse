from more_itertools import split_before, one


def truncate(x, digits):
    return float(int(x * (10 ** digits))) / float(10**digits)


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

    def _check_errors(self, raw):
        if len(raw[self._ERROR_KEY]) != 0:
            raise ResponseError(f"The Kraken API returned an error:\n {raw}")

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
        split = None

        def next_interval(trd):
            nonlocal split
            ts = int(trd[2])
            split = split or self._floor_to_interval(ts)
            if ts >= split:
                split += self._interval
                return True
            return False

        return list(split_before(trades, next_interval))[:-1]

    def _floor_to_interval(self, ts):
        return ts - (ts % self._interval)

    @staticmethod
    def _transpose(trs):
        return map(list, zip(*trs))

    def _make_line(self, ps, ts, vs):
        ttl_v = sum(vs)
        vwap = truncate(sum((p * v for p, v in zip(ps, vs))) / ttl_v, 1)
        ttl_v = round(ttl_v, 8)
        line = f"{self._floor_to_interval(min(ts))},{ps[0]},{max(ps)},{min(ps)},{ps[-1]},{vwap},{ttl_v},{len(ts)}"
        return line


class InvalidFormatError(TypeError):
    pass


class ResponseError(ValueError):
    pass
