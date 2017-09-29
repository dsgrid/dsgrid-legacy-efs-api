from .temphdf5 import TempHDF5

from dsgrid.dataformat import EndUse, read_enduses, write_enduses

# Write end-uses to file and read back

def test_endusereadwrite():

    enduses = [
        EndUse('Space Heating'),
        EndUse('Space Cooling'),
        EndUse('Water Heating'),
        EndUse('Other')
    ]

    with TempHDF5() as testfile:
        write_enduses(testfile, enduses)
        h5enduses = read_enduses(testfile)
        assert(h5enduses == enduses)
