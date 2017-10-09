from py.test import raises
from .temphdf5 import TempHDF5File
from dsgrid.enumeration import (
    Enumeration, SectorEnumeration, GeographyEnumeration,
    EndUseEnumeration, TimeEnumeration,
    sectors, counties, enduses, hourly2012
)

def test_enumeration_prepackaged():

    assert(len(counties.ids) == 3108)
    assert(counties.ids[245] == "08059")
    assert(counties.names[245] == "Jefferson County, CO")
    assert(counties.ids[940] == "20173")
    assert(counties.names[940] == "Sedgwick County, KS")


def test_enumeration_validation():

    mismatchedvaluecount = ["abc", "def"]
    raises(ValueError,
        Enumeration.checkvalues, mismatchedvaluecount, ["ABC", "DEF", "GHI"])

    toomanyvalues = [str(x) for x in range(1,70000)]
    raises(ValueError,
        Enumeration.checkvalues, toomanyvalues, toomanyvalues)

    repeatedvalue = ["abc", "def", "ghi", "abc"]
    unrepeatedvalue = ["A B C", "D E F", "G H I", "J K L"]
    raises(ValueError,
        Enumeration.checkvalues, repeatedvalue, unrepeatedvalue)
    # Repeated names are ok
    Enumeration.checkvalues(unrepeatedvalue, repeatedvalue)

    toolongvalue = ["a", "b", "abcdef"*15]
    raises(ValueError,
        Enumeration.checkvalues, toolongvalue, ["a", "b", "c"])
    raises(ValueError,
           Enumeration.checkvalues, ["a", "b", "c"], toolongvalue)

def test_enumeration_io():

    with TempHDF5File() as f:

        sectors.persist(f)
        counties.persist(f)
        enduses.persist(f)
        hourly2012.persist(f)

        assert(sectors == SectorEnumeration.load(f))
        assert(counties == GeographyEnumeration.load(f))
        assert(enduses == EndUseEnumeration.load(f))
        assert(hourly2012 == TimeEnumeration.load(f))
