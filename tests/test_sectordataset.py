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

    zerodata = pd.DataFrame(0, columns=["water_heating"], index=["hour1"], dtype='float32')
    data = pd.DataFrame(123, columns=["water_heating"], index=["hour1"], dtype='float32')

    with TempHDF5Filepath() as filepath:

        datafile = Datafile(filepath, sectors, counties, enduses, hourly2012)
        dataset = datafile.add_sector("res__sfd")

        dataset.add_data(data, ["01001", "01003"], [2.3, 4.5])
        pd.testing.assert_frame_equal(dataset["01001"], data*2.3,
                                      check_like=True)
        pd.testing.assert_frame_equal(dataset["01003"], data*4.5,
                                      check_like=True)
        pd.testing.assert_frame_equal(dataset["01005"], zerodata,
                                      check_like=True)
