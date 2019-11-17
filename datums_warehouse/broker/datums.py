class CsvDatums:
    def __init__(self, interval, csv):
        self.interval = interval
        self.csv = csv

    def __repr__(self):
        return f"CsvDatums(interval={self.interval}, csv={self.csv})"

    def __eq__(self, other):
        return self.interval == other.interval and self.csv == other.csv


def floor_to_interval(timestamp, interval):
    return int(timestamp - (timestamp % interval))
