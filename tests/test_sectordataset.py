from .temphdf5 import TempHDF5Filepath
from py.test import raises
from dsgrid.datafile import Datafile
from dsgrid.sectordataset import SectorDataset
from dsgrid.enumeration import (
    sectors, counties, enduses, hourly2012
)
import pandas as pd

def test_sectordataset_validation():

    with TempHDF5Filepath() as filepath:

        datafile = Datafile(filepath, sectors, counties, enduses, hourly2012)

        raises(ValueError, SectorDataset, "bogus_sector", datafile)
        raises(ValueError, SectorDataset, "res__sfd", datafile,
               enduses=["bogus_enduse"])
        raises(ValueError, SectorDataset, "res__sfd", datafile,
               times=["bogus_time"])

        dataset = SectorDataset("res__sfd", datafile)
        data = pd.DataFrame(columns=["water_heating"], index=["hour1"])

        raises(ValueError, dataset.add_data, data, ["01001"], [2.3, 4.5])
        raises(ValueError, dataset.add_data, data, "bogus_geography")

        data2 = pd.DataFrame(columns=["water_heating", "other_enduse"],
                             index=["hour1"])
        raises(ValueError, dataset.add_data, data2, ["01001"])

        data2 = pd.DataFrame(columns=["water_heating"],
                             index=["hour1", "hourX"])
        raises(ValueError, dataset.add_data, data2, ["01001"])


def test_sectordataset_io():

    data = pd.DataFrame(0, columns=["water_heating"], index=["hour1"])

    with TempHDF5Filepath() as filepath:

        datafile = Datafile(filepath, sectors, counties, enduses, hourly2012)
        dataset = datafile.add_sector("res__sfd")
        dataset.add_data(data, ["01001"])
