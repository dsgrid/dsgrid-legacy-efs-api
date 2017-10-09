from .temphdf5 import TempHDF5Filepath
from py.test import raises
from dsgrid.datafile import Datafile
from dsgrid.sectordataset import SectorDataset
from dsgrid.enumeration import (
    sectors_subsectors, counties, enduses, hourly2012
)
import numpy as np
import pandas as pd

def test_sectordataset_validation():

    with TempHDF5Filepath() as filepath:

        datafile = Datafile(filepath, sectors_subsectors, counties, enduses, hourly2012)

        raises(ValueError, SectorDataset, "bogus_sector", datafile)
        raises(ValueError, SectorDataset, "ind__22", datafile,
               enduses=["bogus_enduse"])
        raises(ValueError, SectorDataset, "ind__22", datafile,
               times=["bogus_time"])

        dataset = SectorDataset("ind__22", datafile,
                                ["heating", "cooling"], persist=True)
        data = pd.DataFrame(columns=["heating", "cooling"],
                            index=hourly2012.ids, dtype="float32")

        raises(ValueError, dataset.add_data, data, ["01001"], [2.3, 4.5])
        raises(ValueError, dataset.add_data, data, ["bogus_geography"])

        data2 = pd.DataFrame(columns=["heating", "bogus_enduse"],
                             index=hourly2012.ids, dtype="float32")
        raises(ValueError, dataset.add_data, data2, ["01001"])

        data2 = pd.DataFrame(columns=["heating", "cooling"],
                             index=hourly2012.ids[:-1] + ["bogus_time"],
                             dtype="float32")
        raises(ValueError, dataset.add_data, data2, ["01001"])

        dataset.add_data(data, ["01001"])

def test_sectordataset_io():

    zerodata = pd.DataFrame(0, dtype='float32',
                            columns=enduses.ids, index=hourly2012.ids)

    data = pd.DataFrame(np.random.rand(len(hourly2012), len(enduses)),
                        dtype='float32',
                        columns=enduses.ids, index=hourly2012.ids)
    data23 = pd.DataFrame(np.array(data)*2.3, dtype='float32',
                        columns=enduses.ids, index=hourly2012.ids)
    data45 = pd.DataFrame(np.array(data)*4.5, dtype='float32',
                        columns=enduses.ids, index=hourly2012.ids)

    with TempHDF5Filepath() as filepath:

        datafile = Datafile(filepath, sectors_subsectors, counties, enduses, hourly2012)
        dataset = datafile.add_sector("res__SingleFamilyDetached")

        dataset.add_data(data, ["01001", "01003"], [2.3, 4.5])
        dataset["56045"] = data

        pd.testing.assert_frame_equal(dataset["01001"], data23, check_like=True)
        pd.testing.assert_frame_equal(dataset["01003"], data45, check_like=True)
        pd.testing.assert_frame_equal(dataset["01005"], zerodata, check_like=True)
        pd.testing.assert_frame_equal(dataset["56043"], zerodata, check_like=True)
        pd.testing.assert_frame_equal(dataset["56045"], data, check_like=True)
