import numpy as np
import pandas as pd
from itertools import product

weatheryear = pd.DatetimeIndex(start='1/1/2012', freq='H', periods=8784)

class TimeFormat:

    def __init__(self, periods):
        self.periods = periods

    def timeindex(self):
        raise NotImplementedError("Abstract Method")

    def timeseries(self):
        raise NotImplementedError("Abstract Method")


class HourOfYear(TimeFormat):

    def __init__(self):
        TimeFormat.__init__(self, 8784)

    def timeindex(self):
        return pd.Index(range(self.periods))

    def timeseries(self, data):
        result = data.copy()
        result.index = weatheryear
        return result


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
        self.hourmapping = (24 * np.array(self.daymapping)[weatheryear.dayofweek]
                                + weatheryear.hour)

    def timeseries(self, data):
        result = data.loc[self.hourmapping, :]
        result.index = weatheryear
        return result


hourofyear = HourOfYear()
hourofweekdayweekend = HourOfDayByDayOfWeek([0,0,0,0,0,1,1])
