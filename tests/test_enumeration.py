from py.test import raises
from .temphdf5 import TempHDF5File
from dsgrid import DSGridValueError
from dsgrid.dataformat.enumeration import (
    Enumeration, SectorEnumeration, GeographyEnumeration, EndUseEnumeration, 
    EndUseEnumerationBase, TimeEnumeration, 
    allsectors, sectors, sectors_subsectors, conus, states, counties, 
    allenduses, enduses, annual, hourly2012
)

def test_enumeration_prepackaged():

    assert(len(allsectors) == 1)
    assert(len(sectors) == 4)
    assert(len(sectors_subsectors) == 161)
    assert(sectors_subsectors.ids[20] == "com__Laboratory")
    assert(sectors_subsectors.names[20] == "Commercial: Laboratory")
    assert(sectors_subsectors.ids[152] == "ind__3364")
    assert(sectors_subsectors.names[152] ==
           "Industry: Aerospace Product and Parts Manufacturing")

    assert(len(conus) == 1)

    assert(len(states) == 51)
    assert(states.ids[5] == "CO")
    assert(states.names[42] == "Tennessee")

    assert(len(counties) == 3108)
    assert(counties.ids[245] == "08059")
    assert(counties.names[245] == "Jefferson County, CO")
    assert(counties.ids[940] == "20173")
    assert(counties.names[940] == "Sedgwick County, KS")

    assert(len(enduses) == 30)
    assert(len(allenduses) == 1)

    assert(len(annual) == 1)
    assert(len(hourly2012) == 8784)

def test_enumeration_validation():

    mismatchedvaluecount = ["abc", "def"]
    raises(DSGridValueError,
        Enumeration, 'enum', mismatchedvaluecount, ["ABC", "DEF", "GHI"])

    toomanyvalues = [str(x) for x in range(1,70000)]
    raises(DSGridValueError,
        Enumeration, 'enum', toomanyvalues, toomanyvalues)

    repeatedvalue = ["abc", "def", "ghi", "abc"]
    unrepeatedvalue = ["A B C", "D E F", "G H I", "J K L"]
    raises(DSGridValueError,
        Enumeration, 'enum', repeatedvalue, unrepeatedvalue)
    # Repeated names are ok
    Enumeration('enum', unrepeatedvalue, repeatedvalue)

    toolongvalue = ["a", "b", "abcdef"*30]
    raises(DSGridValueError,
        Enumeration, 'enum', toolongvalue, ["a", "b", "c"])
    raises(DSGridValueError,
           Enumeration, 'enum', ["a", "b", "c"], toolongvalue)

def test_enumeration_io():

    with TempHDF5File() as f:

        sectors_subsectors.persist(f)
        counties.persist(f)
        enduses.persist(f)
        hourly2012.persist(f)

        assert(sectors_subsectors == SectorEnumeration.load(f))
        assert(counties == GeographyEnumeration.load(f))
        assert(enduses == EndUseEnumerationBase.load(f))
        assert(hourly2012 == TimeEnumeration.load(f))
