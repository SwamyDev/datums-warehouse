import sqlite3

cache_schema = """
CREATE TABLE last (
   timestamp INTEGER PRIMARY KEY
);

CREATE TABLE trades (
   timestamp REAL PRIMARY KEY,
   price REAL NOT NULL,
   volume REAL NOT NULL
);

INSERT INTO last (timestamp) VALUES (0);
"""

all_tables = """
SELECT COUNT(*) FROM sqlite_master WHERE type = 'table'
"""

insert_trade = """
INSERT INTO trades (price, volume, timestamp) VALUES (?, ?, ?);
"""
query_trades = """
SELECT price, volume, timestamp
FROM trades
where timestamp >= ? and timestamp <= ?
"""
update_last = """
UPDATE last SET timestamp = ?
"""
query_last = """
SELECT timestamp from last
"""


class TradesCache:
    def __init__(self, file):
        self._file = file
        self._db = None

    def __enter__(self):
        self._db = sqlite3.connect(self._file, detect_types=sqlite3.PARSE_DECLTYPES)
        num_tables, = self._db.execute(all_tables).fetchone()
        if num_tables == 0:
            self._db.executescript(cache_schema)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._db.commit()
        self._db.close()

    def update(self, trades, last):
        for trade in trades:
            try:
                self._db.execute(insert_trade, trade)
            except sqlite3.IntegrityError:
                print(trade)
        self._db.execute(update_last, [last])
        self._db.commit()

    def get(self, since, until):
        return self._db.execute(query_trades, (since, until)).fetchall()

    def last_timestamp(self):
        last, = self._db.execute(query_last).fetchone()
        return last
