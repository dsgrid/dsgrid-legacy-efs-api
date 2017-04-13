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
        return range(8760)

    def timeseries(self, data):
        return data


class HourOfDayBy(TimeFormat):

    def __init__(self, daymapping):
        assert(set(xrange(max(daymapping)+1)) == set(daymapping))
        self.daymapping = daymapping
        TimeFormat.__init__(self, len(daymapping) * 24)


class HourOfDayByDayOfWeek(HourOfDayBy):

    def __init__(self, daymapping):
        assert(len(daymapping) == 7)
        HourOfDayBy.__init__(self, daymapping)

    def timeindex(self):
        pass # TODO

    def timeseries(self, data):
        pass # TODO


hourofweekdayweekend = HourOfDayByDayOfWeek([0,0,0,0,0,1,1])
