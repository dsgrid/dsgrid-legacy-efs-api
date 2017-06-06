import os
import csv
import numpy as np
import pandas as pd
import h5py
from collections import namedtuple
from warnings import warn

from timeformats import *

# NumPy <--> HDF5 bit types

county_dtype = np.dtype([
    ('state_fips', 'u1'),
    ('county_fips', 'u2'),
    ('state', 'S2'),
    ('county', 'S30')
    ])

enduse_dtype = np.dtype([
    ('name', 'S30')
])

EndUse = namedtuple("EndUse", "name")

counties_filepath = os.path.join(os.path.dirname(__file__), 'counties.csv')

def load_counties():

    countymap = dict()
    statefipss = []
    countyfipss = []
    statenames = []
    countynames = []
    i = 0

    with open(counties_filepath) as counties_file:

        for county in csv.DictReader(counties_file):

            statefips = int(county["StateFIPS"])
            countyfips = int(county["CountyFIPS"])

            countymap[(statefips, countyfips)] = i
            statefipss.append(statefips)
            countyfipss.append(countyfips)
            statenames.append(county["State"])
            countynames.append(county["County"])

            i += 1

    counties = np.empty(i, dtype=county_dtype)
    counties["state_fips"] = statefipss
    counties["county_fips"] = countyfipss
    counties["state"] = statenames
    counties["county"] = countynames

    return countymap, counties

standard_fipstoindex, standard_counties = load_counties()

def fips_to_countyindex(fips_list, fips_to_index):
    return map(lambda x: fips_to_index[x], fips_list)


# Data standardization

def to_standard_array(dataframe, timeformat, enduses):

    if not (dataframe.index == timeformat.timeindex()).all():
        ValueError(
            "Input row indices must match the subsector time format")

    enduses = map(lambda enduse: enduse.name, enduses)
    set_enduses = set(enduses)
    set_cols = set(dataframe.columns)
    if not set_enduses.issubset(set_cols):
        ValueError(
            "Input columns must represent all subsector enduses.",
            "\nInputs were", dataframe.columns,
            "\nSubsector end-uses are", enduses)

    extracols = list(set_cols.difference(set_enduses))
    if extracols:
        warn(
            "\nExtra end-use columns discarded: " + str(extracols) +
            "\nSubsector end-uses are: " + str(enduses))

    return np.array(dataframe.loc[:, enduses])

# HDF5 File Manipulation

## Counties

def read_counties(h5file):
    h5_counties = h5file['counties']
    h5_fips_list = list(zip(h5_counties['state_fips'],
                          h5_counties['county_fips']))
    return h5_fips_list, h5_counties

def write_counties(h5file, counties):
    if 'counties' in h5file:
        del h5file['counties']
    h5file['counties'] = counties
    return None

## End-Uses

def read_enduses(h5file):
    enduses = [EndUse(enduse['name']) for enduse in h5file['enduses'][:]]
    return enduses

def write_enduses(h5file, enduses):

    h5enduses = np.empty(len(enduses), dtype=enduse_dtype)
    h5enduses['name'] = map(lambda x: x.name, enduses)

    if 'enduses' in h5file:
        del h5file['enduses']
    h5file['enduses'] = h5enduses

    return None


# Sectors / subsectors

def read_sectors(h5file):

    h5_county_fips, h5_counties = read_counties(h5file)
    h5_to_standard_mapping = np.array(
        fips_to_countyindex(h5_county_fips, standard_fipstoindex))
    enduses = read_enduses(h5file)

    return {slug: load_sector(sector_group, h5_to_standard_mapping, enduses)
                         for slug, sector_group in h5file.iteritems()
                         if isinstance(sector_group, h5py.Group)}

def load_sector(sector_group, h5_to_standard_mapping, enduses):

    sector = Sector(sector_group.attrs["slug"], sector_group.attrs["name"])
    sector.subsectors = {slug: load_subsector(
        subsector_dataset, h5_to_standard_mapping, enduses)
        for slug, subsector_dataset in sector_group.iteritems()}
    return sector

def load_subsector(subsector_dataset, h5_to_standard_mapping, enduses):

    subsector_enduses = [enduse[0] for enduse
                             in np.array(enduses)[subsector_dataset.attrs["enduses"]]]
    subsector = Subsector(subsector_dataset.attrs["slug"],
                              subsector_dataset.attrs["name"],
                              parse_timeformat(subsector_dataset.attrs),
                              subsector_enduses)

    n_regiondatasets = subsector_dataset.shape[2]
    countymap = subsector_dataset.attrs["countymap"]
    subsector.counties_data = [
        (h5_to_standard_mapping[np.where(countymap == i)],
             subsector_dataset[:, :, i])
        for i in range(n_regiondatasets)]

    return subsector


def write_sectors(h5file, sectors, enduses=None, county_check=True):

    if not enduses:
        enduses = read_enduses(h5file)

    for sector in sectors.values():

        if sector.slug not in h5file:
            h5file.create_group(sector.slug)

        sector_group = h5file[sector.slug]
        sector_group.attrs["slug"] = sector.slug
        sector_group.attrs["name"] = sector.name

        for subsector in sector.subsectors.values():

            county_missing = np.ones(len(standard_counties), dtype=bool)
            county_mapping = np.zeros(len(standard_counties), dtype='u2')
            allcounties_data = np.empty((
                subsector.timeformat.periods,
                len(subsector.enduses),
                len(subsector.counties_data)))
            enduse_mapping = np.array(
                map(lambda enduse: enduses.index(enduse),
                        subsector.enduses), dtype='u1')

            for i, county_data in enumerate(subsector.counties_data):
                county_mapping[county_data[0]] = i
                county_missing[county_data[0]] = False
                allcounties_data[:, :, i] = county_data[1]

            if subsector.slug in sector_group:
                del sector_group[subsector.slug]
 
            sector_group[subsector.slug] = allcounties_data
            sector_group[subsector.slug].attrs["slug"] = subsector.slug
            sector_group[subsector.slug].attrs["name"] = subsector.name
            sector_group[subsector.slug].attrs["countymap"] = county_mapping
            sector_group[subsector.slug].attrs["enduses"] = enduse_mapping

            for attr, val in subsector.timeformat.to_hdf5_attributes().iteritems():
                sector_group[subsector.slug].attrs[attr] = val

            if county_check and county_missing.any():
                missing_county_info = [county[3] + " " + county[2]
                                           for county
                                           in standard_counties[county_missing]]
                warn(sector.name + " " + subsector.name +
                         " data is missing for some counties.\n" +
                         "Counties assignments will default to the" +
                         "first dataset supplied.\n" +
                         "Missing counties: " + str(missing_county_info))


    return None

def collect_enduses(sectors):
    enduses = set()
    for sector in sectors.values():
        for subsector in sector.subsectors.values():
            enduses.update(subsector.enduses)
    return list(enduses)

# Classes

class DSGridFile:

    def __init__(self, filepath=None):

        self.filepath = filepath

        if filepath:
            with h5py.File(filepath, 'r') as hdf5file:
                self.sectors = read_sectors(hdf5file)

        else:
            self.sectors = {}

    def __getattr__(self, slug):
        return self.sectors[slug]

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            self.sectors == other.sectors
            )

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def __str__(self):
        return self.__repr__()

    def add_sector(self, slug, name):
        sector = Sector(slug, name)
        self.sectors[slug] = sector
        return sector

    def write(self, filepath=None, county_check=True):

        if not filepath:
            filepath = self.filepath

        with h5py.File(filepath, 'a') as hdf5file:
            write_counties(hdf5file, standard_counties)
            write_enduses(hdf5file, collect_enduses(self.sectors))
            write_sectors(hdf5file, self.sectors,
                              county_check=county_check)

        return None


class Sector:

    def __init__(self, slug, name):
        self.slug = slug
        self.name = name
        self.subsectors = dict()

    def __getattr__(self, slug):
        return self.subsectors[slug]

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            self.slug == other.slug and
            self.name == other.name and
            self.subsectors == other.subsectors)

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def __str__(self):
        return self.__repr__()

    def add_subsector(self, slug, name, timeformat, enduses):
        subsector = Subsector(slug, name, timeformat, enduses)
        self.subsectors[slug] = subsector
        return subsector


class Subsector:

    def __init__(self, slug, name, timeformat, enduses):

        if type(enduses[0]) is not EndUse:
            enduses = map(EndUse, enduses)

        self.slug = slug
        self.name = name
        self.timeformat = timeformat
        self.enduses = enduses
        self.counties_data = []

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            self.slug == other.slug and
            self.name == other.name and
            self.timeformat == other.timeformat and
            self.enduses == other.enduses and
            all(map(lambda cd1, cd2:
                    (cd1[0] == cd2[0]).all() and (cd1[1] == cd2[1]).all(),
                    self.counties_data, other.counties_data)))

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def __str__(self):
        return self.__repr__()

    def add_data(self, dataframe, county_assignments=[]):

        if type(county_assignments) is not list:
            county_assignments = [county_assignments]

        self.counties_data.append((
            np.array(fips_to_countyindex(
                county_assignments, standard_fipstoindex), dtype='u2'),
            to_standard_array(dataframe, self.timeformat, self.enduses)
            ))

        return None