from .temphdf5 import TempHDF5Filepath
from py.test import raises
from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.sectordataset import SectorDataset
from dsgrid.dataformat.enumeration import (
    sectors_subsectors, counties, enduses, hourly2012
)

# Python2 doesn't have a FileNotFoundError
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

def test_datafile_io():

    with TempHDF5Filepath() as filepath:

        try:
            raises(FileNotFoundError, Datafile.load, filepath)
        except:
            # monkeycup with Anaconda 3 throws OSError instead
            raises(OSError, Datafile.load, filepath)

        datafile = Datafile(filepath,sectors_subsectors,counties,enduses,hourly2012)
        sector = datafile.add_sector("res__SingleFamilyDetached")
        assert(sector is datafile["res__SingleFamilyDetached"])

        # 20171106 - Changed loading syntax to require explicit call to Datafile.load
        datafile2 = Datafile.load(filepath)

        assert(datafile == datafile2)
