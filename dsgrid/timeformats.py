import numpy as np
import pandas as pd
from itertools import product
import h5py

weatheryear = pd.DatetimeIndex(start='1/1/2012', freq='H', periods=8784)

# Class hierarchy

class TimeFormat:

    def __init__(self, name, periods):
        self.name = name
        self.periods = periods

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            self.name == other.name and
            self.periods == other.periods)

    def to_hdf5_attributes(self):
        return {"timeformat_name": self.name}

    def timeindex(self):
        raise NotImplementedError("Abstract Method")

    def timeseries(self):
        raise NotImplementedError("Abstract Method")


class HourOfYear(TimeFormat):

    def __init__(self):
        TimeFormat.__init__(self, "HourOfYear", 8784)

    def timeindex(self):
        return pd.Index(range(self.periods))

    def timeseries(self, data):
        result = data.copy()
        result.index = weatheryear
        return result


class HourOfDayBy(TimeFormat):

    def __init__(self, name, daymapping):
        assert(set(xrange(max(daymapping)+1)) == set(daymapping))
        TimeFormat.__init__(self, name, (max(daymapping) + 1) * 24)
        self.daymapping = np.array(daymapping)

    def __eq__(self, other):
        return (TimeFormat.__eq__(self, other) and
                    (self.daymapping == other.daymapping).all())

    def to_hdf5_attributes(self):
        attrs = TimeFormat.to_hdf5_attributes(self)
        attrs["timeformat_daymapping"] = self.daymapping
        return attrs

    def timeindex(self):
        idxs = product(xrange(max(self.daymapping)+1), xrange(24))
        return pd.Index(idxs)


class HourOfDayByDayOfWeek(HourOfDayBy):

    def __init__(self, daymapping):
        assert(len(daymapping) == 7)
        HourOfDayBy.__init__(self, "HourOfDayByDayOfWeek", daymapping)
        self.hourmapping = (24 * np.array(self.daymapping)[weatheryear.dayofweek]
                                + weatheryear.hour)

    def timeseries(self, data):
        result = data.loc[self.hourmapping, :]
        result.index = weatheryear
        return result

hourofyear = HourOfYear()
hourofweekdayweekend = HourOfDayByDayOfWeek([0,0,0,0,0,1,1])


# Parse HDF5 attributes to timeformat classes

def parse_timeformat(attributes):

    if attributes["timeformat_name"] == "HourOfYear":
        return hourofyear

    elif attributes["timeformat_name"] == "HourOfDayByDayOfWeek":
        return HourOfDayByDayOfWeek(attributes["timeformat_daymapping"])
