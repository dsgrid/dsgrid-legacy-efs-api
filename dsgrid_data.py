import csv
import numpy as np
import pandas as pd
import h5py
from collections import namedtuple

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

# Named Tuples

EndUse = namedtuple("EndUse", "name")

def load_counties():

    countymap = dict()
    statefipss = []
    countyfipss = []
    statenames = []
    countynames = []
    i = 0

    with open("data/counties.csv") as counties_file:

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

countymap, counties = load_counties()
def lookup_counties(county_list):
    return map(lambda x: countymap[x], county_list)


# Data standardization

def to_standard_array(dataframe, timeformat, enduses):
    pass # TODO

# HDF5 File Manipulation

## Counties

def read_counties(h5file):
    counties = h5file['counties']
    county_ids = list(zip(counties['state_fips'],
                          counties['county_fips']))
    return county_ids, counties

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
    # read_subsectors(h5file, sector)
    return sectors

def write_sectors(h5file, sectors):
    # write_subsectors(h5file, sector)
    return sectors

def read_subsectors(h5file, sector):
    return subsectors

def write_subsectors(h5file, sector, subsectors):
    return subsectors

# Classes

class DSGridFile:

    def __init__(self, filepath=None):

        self.filepath = filepath

        if filepath:
            hdf5file = h5py.File(filepath, 'r')
            self.counties = read_counties(hdf5file)
            self.enduses = read_enduses(hdf5file)
            self.sectors = read_sectors(hdf5file)

        else:
            self.counties = load_counties()
            self.enduses = []
            self.sectors = []

    def add_sector(self, name):
        sector = Sector(name)
        self.sectors.append(sector)
        return sector

    def write(self, filepath=None):

        if not filepath:
            filepath = self.filepath

        with h5py.File(filepath, 'a') as hdf5file:
            write_counties(hdf5file, self.counties)
            write_enduses(hdf5file, self.enduses)
            write_sectors(hdf5file, self.sectors)

        return None


class Sector:

    def __init__(self, name):
        self.name = name
        self.subsectors = []

    def add_subsector(self, name, timeformat, enduses):
        self.subsectors.append(
            Subsector(name, timeformat, enduses, self))


class Subsector:

    def __init__(self, name, timeformat, enduses, sector):
        self.name = name
        self.timeformat = timeformat
        self.enduses = enduses
        self.counties_data = []

    def add_data(self, data, county_assignments=[]):

        if type(county_assignments) is not list:
            county_assignments = [county_assignments]

        self.counties_data.append((
            lookup_counties(county_assignments),
            to_standard_array(dataframe, self.timeformat, self.enduses)
            ))

        return None
