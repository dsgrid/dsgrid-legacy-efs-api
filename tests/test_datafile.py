import os
from py.test import raises
from dsgrid import __version__ as VERSION
from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.sectordataset import SectorDataset
from dsgrid.dataformat.enumeration import (
    sectors_subsectors, counties, enduses, hourly2012,
    enumdata_folder, MultiFuelEndUseEnumeration
)
from .temphdf5 import TempHDF5Filepath

# Python2 doesn't have a FileNotFoundError
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

def verify_file_not_there(filepath):
    try:
        raises(FileNotFoundError, Datafile.load, filepath)
    except:
        # monkeycup with Anaconda 3 throws OSError instead
        raises(OSError, Datafile.load, filepath)


def test_datafile_io():

    with TempHDF5Filepath() as filepath:

        verify_file_not_there(filepath)

        datafile = Datafile(filepath,sectors_subsectors,counties,enduses,hourly2012)
        assert(datafile.version == VERSION)

        sector = datafile.add_sector("res__SingleFamilyDetached")
        assert(sector is datafile["res__SingleFamilyDetached"])

        # 20171106 - Changed loading syntax to require explicit call to Datafile.load
        datafile2 = Datafile.load(filepath)

        assert(datafile == datafile2)


def test_datafile_io_fancy_enduses():
    with TempHDF5Filepath() as filepath:

        verify_file_not_there(filepath)

        comstock_enduses = MultiFuelEndUseEnumeration.read_csv(
            os.path.join(enumdata_folder,'comstock_enduses.csv'),
            'ComStock Enduses')

        datafile = Datafile(filepath,sectors_subsectors,counties,comstock_enduses,hourly2012)
        subset_enduses = []
        for i, _id in enumerate(comstock_enduses.ids):
            if _id[0].startswith('facility'):
                subset_enduses.append(_id)
        sector = datafile.add_sector("com__Hotel",enduses=subset_enduses)
        assert(sector is datafile["com__Hotel"])

        datafile2 = Datafile.load(filepath)
        assert(datafile == datafile2)
