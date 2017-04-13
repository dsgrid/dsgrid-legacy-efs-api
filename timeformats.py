import pandas as pd
from itertools import product

# TODO: Determine whether 8784s are needed since weather
# year is 2012, and set timeseries() / constructors accordingly.

class TimeFormat:

    def __init__(self, periods):
        self.periods = periods

    def timeindex(self):
        raise NotImplementedError("Abstract Method")

    def timeseries(self):
        raise NotImplementedError("Abstract Method")


class HourOfYear(TimeFormat):

    def __init__(self):
        TimeFormat.__init__(self, 8760)

    def timeindex(self):
        return pd.Index(range(8760))

    def timeseries(self, data):
        return data


class HourOfDayBy(TimeFormat):

    def __init__(self, daymapping):
        assert(set(xrange(max(daymapping)+1)) == set(daymapping))
        self.daymapping = daymapping
        TimeFormat.__init__(self, (max(daymapping) + 1) * 24)

    def timeindex(self):
        idxs = product(xrange(max(self.daymapping)+1), xrange(24))
        return pd.Index(idxs)


class HourOfDayByDayOfWeek(HourOfDayBy):

    def __init__(self, daymapping):
        assert(len(daymapping) == 7)
        HourOfDayBy.__init__(self, daymapping)

    def timeseries(self, data):
        pass # TODO
        # mapping = ...
        # return data[mapping]


hourofyear = HourOfYear()
hourofweekdayweekend = HourOfDayByDayOfWeek([0,0,0,0,0,1,1])
