from datetime import datetime, timedelta

class AggResultNode:
    def __init__(self, time_start: datetime, time_range: timedelta, time_delta: timedelta):
        self.time_start = time_start
        self.time_range = time_range
        self.time_delta = time_delta
        self.queues = dict()

    def add(self, name, values):
        self.queues[name] = values

    def print(self):
        print(f'  ts={self.time_start} tr={self.time_range} td={self.time_delta}')
        for key in self.queues:
            print(f'    {key}: {self.queues[key]}')

class AggResult(dict):
    def __init__(self, *argw, **kwargw):
        super(AggResult, self).__init__(*argw, **kwargw)

    def add(self, name, time_start: datetime, time_range: timedelta, time_delta: timedelta):
        self[name] = AggResultNode(time_start, time_range, time_delta)

    def print(self):
        for key in self:
            print(key)
            self[key].print()