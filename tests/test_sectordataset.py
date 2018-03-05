import os
from py.test import raises

import numpy as np
import pandas as pd

from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.sectordataset import SectorDataset
from dsgrid.dataformat.enumeration import (
    sectors_subsectors, counties, enduses, hourly2012,
    enumdata_folder, MultiFuelEndUseEnumeration
)
from .temphdf5 import TempHDF5Filepath

def test_sectordataset_validation():

    with TempHDF5Filepath() as filepath:

        datafile = Datafile(filepath, sectors_subsectors, counties, enduses, hourly2012)

        raises(ValueError, SectorDataset.new, datafile, "bogus_sector")
        raises(ValueError, SectorDataset.new, datafile, "ind__22",
               enduses=["bogus_enduse"])
        raises(ValueError, SectorDataset.new, datafile, "ind__22",
               times=["bogus_time"])

        dataset = SectorDataset.new(datafile, "ind__22",
                                ["heating", "cooling"])
        data = pd.DataFrame(columns=["heating", "cooling"],
                            index=hourly2012.ids, dtype="float32")

        raises(ValueError, dataset.add_data, data, ["01001"], [2.3, 4.5])
        raises(ValueError, dataset.add_data, data, ["bogus_geography"])

        baddata = pd.DataFrame(columns=["heating", "bogus_enduse"],
                             index=hourly2012.ids, dtype="float32")
        raises(ValueError, dataset.add_data, baddata, ["01001"])

        baddata = pd.DataFrame(columns=["heating", "cooling"],
                             index=hourly2012.ids[:-1] + ["bogus_time"],
                             dtype="float32")
        raises(ValueError, dataset.add_data, baddata, ["01001"])

        baddata = pd.DataFrame(columns=["heating", "heating"],
                             index=hourly2012.ids, dtype="float32")
        raises(ValueError, dataset.add_data, baddata, ["01001"])

        baddata = pd.DataFrame(columns=["heating", "cooling"],
                               index=["2012-04-28 02:00:00-05:00",
                                      "2012-04-28 02:00:00-05:00"],
                               dtype="float32")
        raises(ValueError, dataset.add_data, baddata, ["01001"])

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

def test_sectordataset_io_fancy_enduses():
    comstock_enduses = MultiFuelEndUseEnumeration.read_csv(
            os.path.join(enumdata_folder,'comstock_enduses.csv'),
            'ComStock Enduses')

    subset_enduses = []
    for i, _id in enumerate(comstock_enduses.ids):
        if _id[0].startswith('facility'):
            subset_enduses.append(_id)

    # column names can be subset of MultiFuelEndUseEnumeration.ids ...
    zerodata = pd.DataFrame(0, dtype='float32', columns=subset_enduses,
                            index=hourly2012.ids)

    # ... OR a MultiIndex made from those tuples
    cols = pd.MultiIndex.from_tuples(subset_enduses)
    data = pd.DataFrame(np.random.rand(len(hourly2012), len(subset_enduses)),
                        dtype='float32',
                        columns=cols, index=hourly2012.ids)
    data23 = pd.DataFrame(np.array(data)*2.3, dtype='float32',
                        columns=cols, index=hourly2012.ids)
    data45 = pd.DataFrame(np.array(data)*4.5, dtype='float32',
                        columns=cols, index=hourly2012.ids)

    with TempHDF5Filepath() as filepath:

        datafile = Datafile(filepath, sectors_subsectors, counties, comstock_enduses, hourly2012)
        dataset = datafile.add_sector("com__Hotel", enduses=subset_enduses)

        dataset.add_data(data, ["01001", "01003"], [2.3, 4.5])
        dataset["56045"] = data

        pd.testing.assert_frame_equal(dataset["01001"], data23, check_like=True)
        pd.testing.assert_frame_equal(dataset["01003"], data45, check_like=True)
        pd.testing.assert_frame_equal(dataset["01005"], zerodata, check_like=True)
        pd.testing.assert_frame_equal(dataset["56043"], zerodata, check_like=True)
        pd.testing.assert_frame_equal(dataset["56045"], data, check_like=True)

        datafile2 = Datafile.load(filepath)
        assert(datafile == datafile2)
