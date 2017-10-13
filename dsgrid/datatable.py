import numpy as np
import pandas as pd
import h5py
from dsgrid.sectordataset import ZERO_IDX

class Datatable(object):

    def __init__(self, datafile):

        self.sector_enum = datafile.sector_enum
        self.geo_enum = datafile.geo_enum
        self.enduse_enum = datafile.enduse_enum
        self.time_enum = datafile.time_enum

        self.data = pd.Series(dtype="float32")

        with h5py.File(datafile.h5path, "r") as f:

            for sectorname, sectordataset in datafile.sectordata.items():

                h5dset = f["data/" + sectorname]

                geo_idxs = np.nonzero(h5dset.attrs["geo_mappings"][:] != ZERO_IDX)
                geo_ids = np.array(self.geo_enum.ids)[geo_idxs]
                geo_dset_idxs = h5dset.attrs["geo_mappings"][geo_idxs]
                geo_scales = h5dset.attrs["geo_scalings"][geo_idxs]

                sectors = pd.CategoricalIndex(
                    data = [sectorname],
                    categories=self.sector_enum.ids,
                    name="Sector")

                geos = pd.CategoricalIndex(
                    data = geo_ids,
                    categories = self.geo_enum.ids,
                    name = "Geography")

                enduses = pd.CategoricalIndex(
                    data = sectordataset.enduses,
                    categories = self.enduse_enum.ids,
                    name = "End Use")

                times = pd.CategoricalIndex(
                    data = sectordataset.times,
                    categories = self.time_enum.ids,
                    name = "Time")

                index = pd.MultiIndex.from_product([sectors, geos, enduses, times])

                data = h5dset[:, :, :]
                data = data[geo_dset_idxs, :, :]
                #TODO: Scale data by geo_scales

                self.data = self.data.append(
                    pd.Series(data.reshape(-1), index=index),
                    verify_integrity=True)
