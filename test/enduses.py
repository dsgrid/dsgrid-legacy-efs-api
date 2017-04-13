from dsgrid_data import EndUse, read_enduses, write_enduses
import h5py
import os

testfilepath = "enduse_test.h5"

with h5py.File(testfilepath, "w") as testfile:

    # Write end-uses to file and read back

    enduses = [
        EndUse("Space Heating"),
        EndUse("Space Cooling"),
        EndUse("Water Heating"),
        EndUse("Other")
    ]

    write_enduses(testfile, enduses)

    h5enduses = read_enduses(testfile)
    assert(h5enduses == enduses)

os.remove(testfilepath)
