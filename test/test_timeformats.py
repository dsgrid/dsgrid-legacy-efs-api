from timeformats import hourofyear, hourofweekdayweekend, weatheryear, parse_timeformat
import numpy as np
import pandas as pd
import h5py

testfilepath = "test_timeformats.h5"
enduses = ["End-Use A", "End-Use B", "End-Use C"]


# Hour of year time format

hoy_index = hourofyear.timeindex()

assert(hoy_index[0] == 0)
assert(hoy_index[-1] == 8783)
assert(len(hoy_index) == 8784)
assert(len(hoy_index) == hourofyear.periods)

df = pd.DataFrame(np.random.randn(8784, 3), columns=enduses)
hoy_timeseries = hourofyear.timeseries(df)

assert(len(hoy_timeseries.index) == 8784)
assert((hoy_timeseries.columns == enduses).all())
df.index = weatheryear
assert((df == hoy_timeseries).all().all())

with h5py.File(testfilepath) as testfile:

    for attr, val in hourofyear.to_hdf5_attributes().iteritems():
        testfile.attrs[attr] = val

    h5timeformat = parse_timeformat(testfile.attrs)

assert(h5timeformat == hourofyear)


# Hour of weekday/weekend time format

howw_index = hourofweekdayweekend.timeindex()

assert(howw_index[0] == (0, 0))
assert(howw_index[-1] == (1, 23))
assert(len(howw_index) == 48)
assert(len(howw_index) == hourofweekdayweekend.periods)

df = pd.DataFrame(np.random.randn(48, 3), columns=enduses)
howw_timeseries = hourofweekdayweekend.timeseries(df)

assert((howw_timeseries.index == weatheryear).all())
assert((howw_timeseries.columns == enduses).all())
assert(howw_timeseries.loc["1/1/2012 17:00", "End-Use A"] ==
           df.loc[41, "End-Use A"])
assert(howw_timeseries.loc["2/1/2012 23:00", "End-Use B"] ==
           df.loc[23, "End-Use B"])
assert(howw_timeseries.loc["31/12/2012 20:00", "End-Use C"] ==
           df.loc[20, "End-Use C"])

with h5py.File(testfilepath) as testfile:

    for attr, val in hourofweekdayweekend.to_hdf5_attributes().iteritems():
        testfile.attrs[attr] = val

    h5timeformat = parse_timeformat(testfile.attrs)

assert(h5timeformat == hourofweekdayweekend)
