from timeformats import hourofyear, hourofweekdayweekend

# Hour of year time format

hoy_index = hourofyear.timeindex()

assert(hoy_index[0] == 0)
assert(hoy_index[-1] == 8759)
assert(len(hoy_index) == 8760)
assert(len(hoy_index) == hourofyear.periods)

# hoy_timeseries = hourofyear.timeseries(...)

# assert(len(hoy_timeseries.index) == 8760)
# TODO: Spot check number - date mappings

# Hour of weekday/weekend time format

howw_index = hourofweekdayweekend.timeindex()

assert(howw_index[0] == (0, 0))
assert(howw_index[-1] == (1, 23))
assert(len(howw_index) == 48)
assert(len(howw_index) == hourofweekdayweekend.periods)

# howw_timeseries = hourofweekdayweekend.timeseries(...)

# assert(len(howw_timeseries.index) == 8760)
# TODO: Spot check number - date mappings
