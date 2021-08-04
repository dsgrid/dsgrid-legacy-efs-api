Getting Started
---------------

This is an overview of the basics of using the package. If desired, more
extensive examples can be found throughout the
`notebooks <https://github.com/dsgrid/dsgrid-load/tree/eh/create-docs/notebooks>`__
and `tests <https://github.com/dsgrid/dsgrid-load/tree/master/tests>`__.

Installation
~~~~~~~~~~~~

Download the latest source distribution tarball from the repo `releases
page <https://github.com/dsgrid/dsgrid-load/releases>`__ (e.g.
``dsgrid-x.y.z.tar.gz``), and run:

::

    pip install /filepath/to/dsgrid.x.y.z.tar.gz

The ``pandas``, ``numpy``, and ``h5py`` packages should be added
automatically during the installation process, if they’re not already
available.

Creating a new data file
~~~~~~~~~~~~~~~~~~~~~~~~

To begin, create an empty ``Datafile`` object. This involves providing a
file path for the HDF5 file that will be created, and a set of master
lists of valid sectors, geographies, enduses, and times (these master
lists are referred to as ``Enumeration``\ s in the package). The package
includes predefined ``Enumeration``\ s for sector model data. An
``Enumeration`` includes both a list of unique IDs identifying
individual allowed values, as well as a matching list of more
descriptive names.

.. code:: python

    from dsgrid.datafile import Datafile
    from dsgrid.enumeration import (
        sectors_subsectors, counties, enduses, hourly2012
    )

    f = Datafile("hdf5filename.h5", sectors_subsectors, counties, enduses, hourly2012)

A ``SectorDataset`` can now be added to the ``Datafile``. Note that here
“sector” refers to both levels of the sector/subsector hierarchy. This
is for extensibility of the format to support less resolved datasets
where data may only be available by aggregate sector, or even just
economy-wide.

The following would create a sector dataset that spans all enduses and
time periods, assuming the provided sector ID exists in ``f``\ ’s
``SectorEnumeration``:

.. code:: python

    f.add_sector("res__SingleFamilyDetached")

However, it’s likely that a single sector/subsector will not be drawing
load for all possible end uses. In that case, to save space on disk, the
sector can be defined to use only a subset of the end-uses listed in the
``DataFile``\ ’s ``EndUseEnumeration`` ID list:

.. code:: python

    singlefamilydetached = f.add_sector("res__SingleFamilyDetached",
                                        enduses=["heating", "cooling", "interior_lights"])

One could restrict the dataset to a subset of times in a similar
fashion.

Simulation data can now be assigned to the sector (subsector). The data
should be in the form of a Pandas DataFrame with rows indices
corresponding to IDs in the ``Datafile``\ ’s ``TimeEnumeration`` and
column names corresponding to enduse IDs in the ``Datafile``\ ’s
``EndUseEnumeration`` (or the predetermined subset discussed immediately
above). Each DataFrame is assigned to at least one geography, which are
represented by IDs in the ``Datafile``\ ’s ``GeographyEnumeration``. In
this case, ``"08059"`` is the ID and FIPS code for Jefferson County,
Colorado:

.. code:: python

    singlefamilydetached["08059"] = jeffco_sfd_data
    singlefamilydetached[["08001", "08003", "08005"]] = same_sfd_data_in_many_counties

Individual geographies can be associated with a scaling factor to be
applied to their corresponding data, although this requires a method
call rather than the nicer indexing syntax. This is most useful when
load shapes are shared between counties but magnitudes differ:

.. code:: python

    singlefamilydetached.add_data(same_sfd_shape_different_magnitudes,
                                  ["01001", "01003", "01005"], [1.1, 2.3, 6.7])

All data is persisted to disk (not stored in memory) as soon as it is
assigned, so after adding data no further steps are required to save out
the file.

Reading in an existing data file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If a dsgrid-formatted HDF5 file already exists, it can be read in to a
Python object by passing the file name as a constructor argument. In
that case, any ``Enumeration``\ s passed to the constructor will be
ignored (with a warning).

.. code:: python

    f2 = Datafile("hdf5filename.h5")

All of the data will then be accessible to Python just as it was when
the file was first created, for example:

.. code:: python

    sfd = f2["res__SingleFamilyDetached"]
    jeffco_sfd = sfd["08059"]

For easier data manipulation, the full contents of the ``Datafile`` can
also be read into memory in a tabular format by creating a ``Datatable``
object:

.. code:: python

    from dsgrid.datatable import Datatable
    dt = Datatable(f2)

A ``Datatable`` is just a thin wrapper around a Pandas ``Series`` with a
four-level ``MultiIndex``. The ``Datatable`` can be indexed into for
quick access to a relevant subset of the data, or the underlying
``Series`` can be accessed and manipulated directly.

.. code:: python

    # Accessing a single value
    dt["res__SingleFamilyDetached", "08059", "heating", "2012-04-28 02:00:00-05:00"]

    # Accessing a Series slice
    dt["res__SingleFamilyDetached", "08059", "heating", :]

    # Working directly with the underlying Series
    sector_enduse_totals = dt.data.groupby(levels=["sector", "enduse"]).sum()
