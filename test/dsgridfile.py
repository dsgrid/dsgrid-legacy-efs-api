import dsgrid_data

testfilepath = "integration_test.h5"

# Create data and write to file

f = DSGridFile()

enduses = map(EndUse, ["Space Heating", "Space Cooling",
               "Water Heating", "Other"])

residential = f.add_sector("Residential")

residential.add_subsector("sfd", "Single Family Detached",
                                    tfmt, enduses)
residential.sfd.add_data(df1, County(1,1))
residential.sfd.add_data(df2, [County(1,2), County(1,3)])

residential.add_subsector("sfa", "Single Family Attached",
                                    tfmt, enduses)
residential.sfa.add_data()
residential.sfa.add_data()


enduses.append("Refrigeration")
commercial = f.add_sector("Commercial")

commercial.add_subsector("retail", "Retail", tfmt, enduses)
commercial.retail.add_data()
commercial.retail.add_data()

commercial.add_subsector("office", "Office", tfmt, enduses)
commerical.office.add_data()
commerical.office.add_data()

f.write(testfilepath)

# Read data back in and check consistency

f2 = DSGridFile(testfilepath)

assert(f.counties == f2.counties)
assert(f.enduses == f2.enduses)
assert(f.sectors == f2.sectors)
