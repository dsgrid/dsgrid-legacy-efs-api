import pandas as pd
import numpy as np
import os

from dsgrid.dataformat import DSGridFile, Sector, standard_counties
from dsgrid.timeformats import hourofyear, hourofweekdayweekend

testfilepath = "integration_test.h5"

othercounties = [(county.state_fips,county.county_fips) for county in standard_counties[3:]]


def test_dsgridfile():

    from py.test import raises

    enduses = ['Space Heating',
               'Space Cooling',
               'Water Heating',
               'Other']

    # Create data and write to file

    f = DSGridFile()

    # Add and populate a sector

    residential = f.add_sector("residential","Residential")
    assert residential is f.residential
    assert residential == Sector("residential","Residential")

    tfmt = hourofweekdayweekend
    timestamps = tfmt.timeindex()

    df1 = pd.DataFrame(10 + np.random.randn(48, 4),
                        columns=enduses,
                        index=timestamps)
    df2 = pd.DataFrame(10 + np.random.randn(48, 4),
                        columns=enduses,
                        index=timestamps)

    residential.add_subsector("sfd","Single Family Detached", tfmt, enduses)
    residential.sfd.add_data(df1, (1,1))
    residential.sfd.add_data(df2, [(1,3), (1,5)] + othercounties)


    # Add another sector

    commercial = f.add_sector("commercial","Commercial")
    assert commercial is f.commercial
    assert commercial == Sector("commercial","Commercial")

    enduses = ['Space Heating',
               'Space Cooling',
               'Water Heating',
               'Refrigeration',
               'Other']
    df3 = df1 + 10
    df3["Refrigeration"] = 20 + np.random.randn(48)
    df4 = df2 + 10
    df4["Refrigeration"] = 20 + np.random.randn(48)

    commercial.add_subsector("retail", "Retail", tfmt, enduses)
    commercial.retail.add_data(df3, [(1,1), (1,5)])
    commercial.retail.add_data(df4, [(1,3)] + othercounties)

    tfmt = hourofyear
    timestamps = tfmt.timeindex()
    df5 = pd.DataFrame(10 + np.random.randn(8784, 5),
                        columns=enduses,
                        index=timestamps)
    df6 = pd.DataFrame(10 + np.random.randn(8784, 5),
                        columns=enduses,
                        index=timestamps)

    commercial.add_subsector("office", "Office", tfmt, enduses)
    commercial.office.add_data(df5, (1,5))
    commercial.office.add_data(df6, [(1,1), (1,3)] + othercounties)

    raises(ValueError, commercial.add_subsector, "test", "Test", tfmt,
    ["__________________________Too long enduse name____________________"])

    assert(set(f.sectors.keys()) == set(["residential", "commercial"]))

    f.write(testfilepath)

    # Read data back in and check consistency

    f_h5 = DSGridFile(testfilepath)

    assert(f == f_h5)

    os.remove(testfilepath)
