from .temphdf5 import TempHDF5Filepath
from py.test import raises
from dsgrid.datafile import Datafile
from dsgrid.sectordataset import SectorDataset
from dsgrid.enumeration import (
    sectors, counties, enduses, hourly2012
)

def test_datafile_validation():

    with TempHDF5Filepath() as filepath:

        datafile = Datafile(filepath, sectors, counties, enduses, hourly2012)

        raises(ValueError, datafile.add_sector, "bogus_sector")
        raises(ValueError, datafile.add_sector,
            "residential__singlefamilydetached", enduses=["bogus_enduse"])
        raises(ValueError, datafile.add_sector,
            "residential__singlefamilydetached", times=["bogus_time"])

def test_datafile_io():

    with TempHDF5Filepath() as filepath:

        raises(FileNotFoundError, Datafile, filepath)

        datafile = Datafile(filepath, sectors, counties, enduses, hourly2012)
        datafile.add_sector("res__sfd")

        assert(datafile.sectordata["res__sfd"] ==
            SectorDataset("res__sfd", datafile))
        assert(datafile == Datafile(filepath))
