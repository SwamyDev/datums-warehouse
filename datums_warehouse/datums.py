class CsvDatums:
    def __init__(self, interval, csv):
        self.interval = interval
        self.csv = csv

    def __repr__(self):
        return f"CsvDatums(interval={self.interval}, csv={self.csv})"
