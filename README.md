__Note that package development has now moved to https://github.com/dsgrid/dataformat__

<hr/>

# dsgrid Data Marshalling

This package provides functionality for marshaling sector-based electricity demand data (stored in a Pandas DataFrame) into the common dsgrid HDF5 data format and persisting it to disk. The package should work on both Python 2 and Python 3.

## Installation

Download the latest source distribution tarball from the repo [releases page](https://github.com/dsgrid/dataformat/releases) (e.g. `dsgrid-x.y.z.tar.gz`), and run:

```
pip install /filepath/to/dsgrid.x.y.z.tar.gz
```

The `pandas`, `numpy`, and `h5py` packages should be added automatically during the installation process, if they're not already available.

## Getting Started

This is an overview of the basics of using the package. If desired, more extensive examples can be found throughout the [`tests` folder](tests).

### Creating a new data file

To begin, create an empty `Datafile` object. This involves providing a file path for the HDF5 file that will be created, and a set of master lists of valid sectors, geographies, enduses, and times (these master lists are referred to as `Enumeration`s in the package). The package includes predefined `Enumeration`s for sector model data. An `Enumeration` includes both a list of unique IDs identifying individual allowed values, as well as a matching list of more descriptive names.

```python
from dsgrid.datafile import Datafile
from dsgrid.enumeration import (
    sectors_subsectors, counties, enduses, hourly2012
)

f = Datafile("hdf5filename.h5", sectors_subsectors, counties, enduses, hourly2012)

```

A `SectorDataset` can now be added to the `Datafile`. Note that here "sector" refers to both levels of the sector/subsector hierarchy. This is for extensibility of the format to support less resolved datasets where data may only be available by aggregate sector, or even just economy-wide.

The following would create a sector dataset that spans all enduses and time periods, assuming the provided sector ID exists in `f`'s `SectorEnumeration`:

```python
f.add_sector("res__SingleFamilyDetached")
```

However, it's likely that a single sector/subsector will not be drawing load for all possible end uses. In that case, to save space on disk, the sector can be defined to use only a subset of the end-uses listed in the `DataFile`'s `EndUseEnumeration` ID list:

```python
singlefamilydetached = f.add_sector("res__SingleFamilyDetached",
                                    enduses=["heating", "cooling", "interior_lights"])
```

One could restrict the dataset to a subset of times in a similar fashion.

Simulation data can now be assigned to the sector (subsector). The data should be in the form of a Pandas DataFrame with rows indices corresponding to IDs in the `Datafile`'s `TimeEnumeration` and column names corresponding to enduse IDs in the `Datafile`'s `EndUseEnumeration` (or the predetermined subset discussed immediately above). Each DataFrame is assigned to at least one geography, which are represented by IDs in the `Datafile`'s `GeographyEnumeration`. In this case, `"08059"` is the ID and FIPS code for Jefferson County, Colorado:

```python
singlefamilydetached["08059"] = jeffco_sfd_data
singlefamilydetached[["08001", "08003", "08005"]] = same_sfd_data_in_many_counties
```

Individual geographies can be associated with a scaling factor to be applied to their corresponding data, although this requires a method call rather than the nicer indexing syntax. This is most useful when load shapes are shared between counties but magnitudes differ:

```python
singlefamilydetached.add_data(same_sfd_shape_different_magnitudes,
                              ["01001", "01003", "01005"], [1.1, 2.3, 6.7])
```


All data is persisted to disk (not stored in memory) as soon as it is assigned, so after adding data no further steps are required to save out the file.

### Reading in an existing data file

If a dsgrid-formatted HDF5 file already exists, it can be read in to a Python object by passing the file name as a constructor argument. In that case, any `Enumeration`s passed to the constructor will be ignored (with a warning).

```python
f2 = Datafile("hdf5filename.h5")
```

All of the data will then be accessible to Python just as it was when the file was first created, for example:

```python
sfd = f2["res__SingleFamilyDetached"]
jeffco_sfd = sfd["08059"]
```
