import dsgrid_data
import h5py
from random import sample
import os

testfilename = "countytest.h5"

with h5py.File(testfilename, 'w') as testfile:

    # Test reading in counties from CSV, confirm matching data

    countymap, counties = dsgrid_data.load_counties()

    assert(len(countymap) == 3109)
    assert(countymap[(1, 1)] == 0)
    assert(countymap[(8, 59)] == 245)
    assert(countymap[(20, 173)] == 940)
    assert(countymap[(51, 840)] == 2919)
    assert(countymap[(56, 45)] == 3108)

    assert(len(counties) == 3109)
    assert(list(counties[0]) == [1, 1, 'AL', 'Autauga County'])
    assert(list(counties[245]) == [8, 59, 'CO', 'Jefferson County'])
    assert(list(counties[940]) == [20, 173, 'KS', 'Sedgwick County'])
    assert(list(counties[2919]) == [51, 840, 'VA', 'Winchester city'])
    assert(list(counties[-1]) == [56, 45, 'WY', 'Weston County'])

    # Test writing out counties to HDF5 and reading back in

    dsgrid_data.write_counties(testfile, counties)
    indices, h5counties = dsgrid_data.read_counties(testfile)

    assert(indices[0] == (1,1))
    assert(indices[245] == (8,59))
    assert(indices[940] == (20,173))
    assert(indices[3108] == (56,45))
    assert((counties == h5counties).all())

    # Test reading in non-standard order / incomplete counties
    # from file, map on FIPS codes and check match

    random_county_subset = counties[sample(range(len(counties)), 1500)]
    dsgrid_data.write_counties(testfile, random_county_subset)

    h5fips, h5counties = dsgrid_data.read_counties(testfile)
    countymapping = dsgrid_data.fips_to_countyindex(h5fips, countymap)
    assert((counties[countymapping] == h5counties).all())
    assert((random_county_subset == h5counties).all())

os.remove(testfilename)
