Getting Started
---------------

This is a basic overview of how to use the dsgrid-legacy-efs-api
``dsgrid`` package. If desired, more extensive examples can be found
throughout the
`notebooks <https://github.com/dsgrid/dsgrid-legacy-efs-api/tree/main/notebooks>`__
and `tests <https://github.com/dsgrid/dsgrid-legacy-efs-api/tree/main/tests>`__.

For accessing the dsgrid EFS data, `reading data
files <#reading-in-an-existing-data-file>`__ and `working with dsgrid
models <#working-with-a-dsgrid-model-collection-of-data-files>`__ is
probably most of interest. However, the `brief primer on how to create new
data files <#creating-a-new-data-file>`__ may be useful
background information. Also note that while a ``.dsg`` extension is
used for the dsgrid EFS data files, the underlying format is basic HDF5
and can be browsed with a basic viewer like
`HDFView <https://www.hdfgroup.org/downloads/hdfview/>`__.

Installation
~~~~~~~~~~~~

To get the basic package, run:

::

    pip install dsgrid-legacy-efs-api

If you would like to run the example notebooks and browse the files available 
through the Open Energy Data Initiative (OEDI), install the required extra 
dependencies:

::

    pip install dsgrid-legacy-efs-api[ntbks,oedi]

and also clone the repository. Then you should be able to run the .ipynb files 
in the dsgrid-legacy-efs-api/notebooks folder, which include functionality for 
directly browsing the OEDI `oedi-data-lake/dsgrid-2018-efs 
<https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=dsgrid-2018-efs%2F>`__ 
data files. If you would like to use the HSDS service, please see the 
configuration instructions at `https://github.com/NREL/hsds-examples/ <https://github.com/NREL/hsds-examples/>`__.

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
:class:`~dsgrid.dataformat.enumeration.TimeEnumeration` and column names corresponding to enduse IDs in the
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
-  :meth:`~dsgrid.dataformat.sectordataset.SectorDataset.add_data_batch`

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

For easier data manipulation, the full contents of the :class:`~dsgrid.dataformat.datafile.Datafile` can
also be read into memory in a tabular format by creating a :class:`~dsgrid.dataformat.datatable.Datatable`
object:

.. code:: python

    from dsgrid.dataformat.datatable import Datatable
    dt = Datatable(f2)

A :class:`~dsgrid.dataformat.datatable.Datatable` is just a thin wrapper around 
a Pandas ``Series`` with a four-level ``MultiIndex``. The 
:class:`~dsgrid.dataformat.datatable.Datatable` can be indexed into for quick 
access to a relevant subset of the data, or the underlying ``Series`` can be 
accessed and manipulated directly.

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

A :class:`dsgrid.model.LoadModel` holds a collection of related datafiles and 
tag each one with its :class:`~dsgrid.model.ComponentType` and an optional color 
(for plotting). For example, a :class:`~dsgrid.model.LoadModel` can be formed 
just from the ComponentType.BOTTOMUP components:

.. code:: python

    from dsgrid.model import ComponentType, LoadModelComponent, LoadModel

    bottomup_components_list = [
        ('Residential','#F7A11A','residential.dsg'),
        ('Commercial','#5D9732','commercial.dsg'),
        ('Industrial','#D9531E','industrial.dsg')]
    
    # Let datadir be a pathlib.Path pointing to a folder containing .dsg files ...
    components = []
    for name, color, filename in bottomup_components_list:
        components.append(LoadModelComponent(name, component_type=ComponentType.BOTTOMUP, color=color))
        components[-1].load_datafile(datadir / filename)
    model = LoadModel.create(components)

Dimension mappings can be applied to individual :class:`Datafiles <dsgrid.dataformat.Datafile>`,
individual :class:`LoadModelComponents <dsgrid.model.LoadModelComponent>`, or to 
an entire :class:`LoadModel`. For example, this code would aggregate the model 
defined above to the census division level: 

.. code:: python

    from dsgrid.dataformat.enumeration import census_divisions
    from dsgrid.dataformat.dimmap import mappings

    model.map_dimension(datadir / ".." / "aggregated_to_census_division", census_divisions, mappings)

See `notebooks/Visualize dsgrid model.ipynb` for more examples.

Classes, methods and objects useful for working with the dsgrid EFS
dataset:

-  :class:`dsgrid.model.LoadModel`
-  :class:`dsgrid.model.LoadModelComponent`
-  :class:`dsgrid.dataformat.dimmap.Mappings` (Also scroll to the bottom of the source code file to see the mappings module attribute and how it is defined.)
-  :class:`dsgrid.dataformat.dimmap.FullAggregationMap`
-  :class:`dsgrid.dataformat.dimmap.FilterToSubsetMap`
-  :class:`dsgrid.dataformat.dimmap.FilterToSingleFuelMap`
-  :class:`dsgrid.dataformat.dimmap.ExplicitAggregation`
-  :class:`dsgrid.dataformat.dimmap.UnitConversionMap`
-  :meth:`dsgrid.dataformat.datafile.Datafile.map_dimension`
-  :meth:`dsgrid.dataformat.datafile.Datafile.scale_data`
