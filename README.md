# dsgrid Data Marshalling

This package provides functionality for marshaling sector-based electricity demand data (stored in a Pandas DataFrame) into the common dsgrid HDF5 data format and persisting it to disk.

## Getting Started

This is an overview of the basics of using the package. If desired, more extensive examples can be found throughout the `tests` folder.

To begin, create an empty `DSGridFile` object and add a sector to it. Adding the sector returns the newly created `Sector` object, which can be assigned to a variable if desired:

```python
f = DSGridFile()

# Provide both a short and long name
mysector = f.add_sector("mysectorshortname", "My Sector Long Name")
```

The `Sector` object can also be referenced via its short name as an attribute on the `DSGridFile` object:

```python
assert(f.mysectorshortname is mysector)
```

`Subsector` objects can be accessed analogously via attributes on a `Sector` object. Creating a new subsector requires providing a short name, long name, time format, and a list of end-uses associated with the data to be provided. The `hourofyear` (8784 sequential hourly values) and `hourofweekdayweekend` (24 typical weekday + 24 typical weekend values) time formats are provided out of the box, although others can be defined as necessary by subclassing `TimeFormat` and implementing the required abstract methods.

```python
subsector = mysector.add_subsector("mysubsec", "My Subsector Long Name",
                                   hourofyear, ["End-Use 1", "End-Use 2"])
```

Simulation data can now be assigned to the subsector. The data should be in the form of a Pandas DataFrame with rows representing the timestamps of the supplied time format and columns representing the supplied subsector enduses. A list (or single) tuple provding a (State FIPS code, county FIPS code) pair is required to define the geographic extent of the dataset.

```python
subsector.add_data(mydata, (8, 59))
```

Finally, the stored data can be written out to an HDF5 file:

```python
f.write("file.h5")
```
