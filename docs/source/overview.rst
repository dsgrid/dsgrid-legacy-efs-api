Getting Started
---------------

This is a basic overview of how to use the dsgrid-legacy-efs-api
``dsgrid`` package. If desired, more extensive examples can be found
throughout the
`notebooks <https://github.com/dsgrid/dsgrid-load/tree/eh/create-docs/notebooks>`__
and `tests <https://github.com/dsgrid/dsgrid-load/tree/master/tests>`__.

TODO: Update links after changing repository name

For accessing the dsgrid EFS data, `reading data
files <#reading-in-an-existing-data-file>`__ and `working with dsgrid
models <#working-with-a-dsgrid-model-collection-of-data-files>`__ is
probably most of interest. However, the `brief primer on how the data
files were created <#creating-a-new-data-file>`__ may be useful
background information. Also note that while a ``.dsg`` extension is
used for the dsgrid EFS data files, the underlying format is basic HDF5
and can be browsed with a basic viewer like
`HDFView <https://www.hdfgroup.org/downloads/hdfview/>`__.

Installation
~~~~~~~~~~~~

Download the latest source distribution tarball from the repo `releases
page <https://github.com/dsgrid/dsgrid-load/releases>`__ (e.g.
``dsgrid-x.y.z.tar.gz``), and run:

::

    pip install /filepath/to/dsgrid.x.y.z.tar.gz

TODO: Update when publish on PyPI. Document how to install optional
dependencies.

Creating a new data file
~~~~~~~~~~~~~~~~~~~~~~~~

To begin, create an empty
:class:`~dsgrid.dataformat.datafile.Datafile` object. This involves
providing a file path for the HDF5 file that will be created, and a set
of valid
:class:`sector <dsgrid.dataformat.enumeration.SectorEnumeration>`,
:class:`geography <dsgrid.dataformat.enumeration.GeographyEnumeration>`,
:class:`enduse <dsgrid.dataformat.enumeration.EndUseEnumerationBase>`,
and :class:`time <dsgrid.dataformat.enumeration.TimeEnumeration>`
:class:`enumerations <dsgrid.dataformat.enumeration.Enumeration>`. An
:class:`~dsgrid.dataformat.enumeration.Enumeration` includes both a
list of unique IDs identifying individual allowed values, as well as a
matching list of more descriptive names. The package includes predefined
:class:`Enumerations <dsgrid.dataformat.enumeration.Enumeration>` for
sector model data.

.. code:: python

    from dsgrid.datafile import Datafile
    from dsgrid.enumeration import (
        sectors_subsectors, counties, enduses, hourly2012
    )

    f = Datafile("data.dsg", sectors_subsectors, counties, enduses, hourly2012)

A :class:`~dsgrid.dataformat.sectordataset.SectorDataset` can now be
added to the :class:`~dsgrid.dataformat.datafile.Datafile`. Note that
here “sector” refers to both levels of the sector/subsector hierarchy.
This is for extensibility of the format to support less resolved
datasets where data may only be available by aggregate sector, or even
just economy-wide.

The following would create a sector dataset that spans all enduses and
time periods, assuming the provided sector ID exists in ``f``\ ’s
:class:`~dsgrid.dataformat.enumeration.SectorEnumeration`:

.. code:: python

    f.add_sector("res__SingleFamilyDetached")

However, it’s likely that a single sector/subsector will not be drawing
load for all possible end uses. In that case, to save space on disk, the
sector can be defined to use only a subset of the end-uses listed in the
:class:`Datafile's <dsgrid.dataformat.datafile.Datafile>`
:class:`~dsgrid.dataformat.enumeration.EndUseEnumerationBase` ID list:

.. code:: python

    singlefamilydetached = f.add_sector("res__SingleFamilyDetached",
                                        enduses=["heating", "cooling", "interior_lights"])

One could restrict the dataset to a subset of times in a similar
fashion.

Simulation data can now be assigned to the sector (subsector). The data
should be in the form of a Pandas DataFrame with rows indices
corresponding to IDs in the
:class:`Datafile's <dsgrid.dataformat.datafile.Datafile>`
``TimeEnumeration`` and column names corresponding to enduse IDs in the
:class:`Datafile's <dsgrid.dataformat.datafile.Datafile>`
:class:`EndUseEnumeration <dsgrid.dataformat.enumeration.EndUseEnumerationBase>`
(or the predetermined subset discussed immediately above). Each
DataFrame is assigned to at least one geography, which are represented
by IDs in the
:class:`Datafile's <dsgrid.dataformat.datafile.Datafile>`
:class:`~dsgrid.dataformat.enumeration.GeographyEnumeration`. In this
case, ``"08059"`` is the ID and FIPS code for Jefferson County,
Colorado:

.. code:: python

    singlefamilydetached["08059"] = jeffco_sfd_data
    singlefamilydetached[["08001", "08003", "08005"]] = same_sfd_data_in_many_counties

Individual geographies can be associated with a scaling factor to be
applied to their corresponding data, although this feature is not
accessible through the indexed assignment syntax and instead requires a
method call. This is most useful when load shapes are shared between
counties but magnitudes differ:

.. code:: python

    singlefamilydetached.add_data(same_sfd_shape_different_magnitudes,
                                  ["01001", "01003", "01005"], [1.1, 2.3, 6.7])

All data is persisted to disk (not stored in memory) as soon as it is
assigned, so after adding data no further steps are required to save out
the file.

Additional classes and methods useful for creating new data:

-  :class:`~dsgrid.dataformat.enumeration.SingleFuelEndUseEnumeration`
-  :class:`~dsgrid.dataformat.enumeration.FuelEnumeration`
-  :class:`~dsgrid.dataformat.enumeration.MultiFuelEndUseEnumeration`
-  :meth:`~dsgrid.sectordataset.SectorDataset.add_data_batch`

Reading in an existing data file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If a dsgrid-formatted HDF5 file already exists, it can be read into a
:class:`~dsgrid.dataformat.datafile.Datafile` object:

.. code:: python

    f2 = Datafile.load("data.dsg")

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

Additional methods useful for accessing data:

-  :meth:`dsgrid.dataformat.sectordataset.SectorDataset.get_data`

Working with a dsgrid model (collection of data files)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

TODO: Document a few basic operations using code snippets from notebooks

Classes, methods and objects useful for working with the dsgrid EFS
dataset:

-  :class:`dsgrid.model.LoadModel`
-  :class:`dsgrid.model.LoadModelComponent`
-  :class:`dsgrid.dataformat.dimmap.Mappings`
-  :class:`dsgrid.dataformat.dimmap.FullAggregationMap`
-  :class:`dsgrid.dataformat.dimmap.FilterToSubsetMap`
-  :class:`dsgrid.dataformat.dimmap.FilterToSingleFuelMap`
-  :class:`dsgrid.dataformat.dimmap.ExplicitAggregation`
-  :class:`dsgrid.dataformat.dimmap.UnitConversionMap`
-  :data:`dsgrid.dataformat.dimmap.mappings`
-  :meth:`dsgrid.dataformat.datafile.Datafile.map_dimension`
-  :meth:`dsgrid.dataformat.datafile.Datafile.scale_data`
