import numpy as np
import pandas as pd
import h5py
import os

import dsgrid_data
from dsgrid_data import Sector, write_sectors, read_sectors, counties
import timeformats

testfilepath = "sector_test.h5"

othercounties = zip(
    counties[3:]["state_fips"],
    counties[3:]["county_fips"])

# Add and populate a sector

tfmt = timeformats.hourofweekdayweekend
timestamps = tfmt.timeindex()
enduses = ["Space Heating", "Space Cooling",
               "Water Heating", "Other"]

df1 = pd.DataFrame(10 + np.random.randn(48, 4),
                       columns=enduses,
                       index=timestamps)
df2 = pd.DataFrame(10 + np.random.randn(48, 4),
                       columns=enduses,
                       index=timestamps)

residential = Sector("residential", "Residential")
residential.add_subsector("sfd", "Single Family Detached", tfmt, enduses)
residential.sfd.add_data(df1, (1,1))
residential.sfd.add_data(df2, [(1,3), (1,5)] + othercounties)


# Add another sector

enduses = ["Space Heating", "Space Cooling",
               "Water Heating", "Refrigeration", "Other"]
df3 = df1 + 10
df3["Refrigeration"] = 20 + np.random.randn(48)
df3["Random Extra Column"] = 20 + np.random.randn(48)
df4 = df2 + 10
df4["Refrigeration"] = 20 + np.random.randn(48)

commercial = Sector("commercial", "Commercial")
commercial.add_subsector("retail", "Retail", tfmt, enduses)
commercial.retail.add_data(df3, [(1,1), (1,5)])
commercial.retail.add_data(df4, [(1,3)] + othercounties)

tfmt = timeformats.hourofyear
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


# Write sectors out and read back in

sectors = {}
for sector in [residential, commercial]:
    sectors[sector.slug] = sector

with h5py.File(testfilepath, 'w') as testfile:

    dsgrid_data.write_counties(testfile, dsgrid_data.counties)
    dsgrid_data.write_enduses(testfile, map(dsgrid_data.EndUse, enduses))
    write_sectors(testfile, sectors, county_check=False)

    h5sectors = read_sectors(testfile)

    assert(h5sectors == sectors)

os.remove(testfilepath)
