import logging
import os
from py.test import raises

from dsgrid import __version__ as VERSION
from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.sectordataset import SectorDataset
from dsgrid.dataformat.enumeration import (
    sectors_subsectors, counties, enduses, hourly2012,
    enumdata_folder, MultiFuelEndUseEnumeration
)
from .temppaths import TempFilepath

logger = logging.getLogger(__name__)

here = os.path.dirname(__file__)


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

    with TempFilepath() as filepath:

        verify_file_not_there(filepath)

        datafile = Datafile(filepath,sectors_subsectors,counties,enduses,hourly2012)
        assert(datafile.version == VERSION)

        sector = datafile.add_sector("res__SingleFamilyDetached")
        assert(sector is datafile["res__SingleFamilyDetached"])

        # 20171106 - Changed loading syntax to require explicit call to Datafile.load
        datafile2 = Datafile.load(filepath)

        assert(datafile == datafile2)


def test_datafile_io_fancy_enduses():
    with TempFilepath() as filepath:

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


def test_backward_compatible():
    for p, dirs, _junk1 in os.walk(os.path.join(here,'data')):
        for version_dir in dirs:
            for pp, _junk2, files in os.walk(os.path.join(p,version_dir)):
                for filename in files:
                    filepath = os.path.join(pp,filename)
                    datafile1 = Datafile.load(filepath,upgrade=False)
                    assert datafile1.version == version_dir[1:]
                    with TempFilepath() as new_filepath:
                        datafile2 = Datafile.load(os.path.join(pp,filename),new_filepath=new_filepath)
                        assert datafile2.version == VERSION, filepath

                        # check that data seems about the same
                        assert len(datafile1.sectordata) == len(datafile2.sectordata)
                        assert len(datafile2.sectordata) > 0

                        # as of v0.2.0, sectordata enduses, times, and n_geos 
                        # should be the same
                        for sector_id, upgraded in datafile2.sectordata.items():
                            assert upgraded.enduses == datafile1[sector_id].enduses
                            assert upgraded.times == datafile1[sector_id].times
                            assert upgraded.n_geos == datafile1[sector_id].n_geos

                        # check that datafile2 is browsable
                        for sector_id, sectordataset in datafile2.sectordata.items():
                            assert sectordataset.n_geos > 0
                            
                            df, geo_ids, scalings = sectordataset.get_data(0)
                            assert len(df.index) > 0, f"{sector_id}, {geo_ids}, {scalings}, \n{df}"
                break
        break
