from .temphdf5 import TempHDF5Filepath
from py.test import raises
from dsgrid.datafile import Datafile
from dsgrid.sectordataset import SectorDataset
from dsgrid.enumeration import (
    sectors_subsectors, counties, enduses, hourly2012
)

def test_datafile_io():

    with TempHDF5Filepath() as filepath:

        raises(FileNotFoundError, Datafile, filepath)

        datafile = Datafile(filepath, sectors_subsectors, counties, enduses, hourly2012)
        datafile.add_sector("res__SingleFamilyDetached")

        datafile2 = Datafile(filepath)

        assert(datafile == datafile2)
