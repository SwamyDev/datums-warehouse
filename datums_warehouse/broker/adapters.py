from datums_warehouse.broker.datums import floor_to_interval


def truncate(x, digits):
    return float(int(x * (10 ** digits))) / float(10 ** digits)


class KrakenAdapter:
    _HEADER = "timestamp,open,high,low,close,vwap,volume,count"

    def __init__(self, interval):
        self._interval = interval * 60

    def __call__(self, trades):
        return "\n".join(self._make_csv_lines(trades))

    def _make_csv_lines(self, trades):
        csv = [self._HEADER]
        if len(trades) == 0:
            return csv

        for itv in self._chunked_by_interval(trades):
            ps, vs, ts = self._transpose([[float(p), float(v), int(t)] for p, v, t in itv])
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
