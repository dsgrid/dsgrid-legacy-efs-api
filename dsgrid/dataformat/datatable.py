import numpy as np
import pandas as pd
import h5py
from dsgrid.dataformat.sectordataset import ZERO_IDX

class Datatable(object):

    def __init__(self, datafile):

        self.sector_enum = datafile.sector_enum
        self.geo_enum = datafile.geo_enum
        self.enduse_enum = datafile.enduse_enum
        self.time_enum = datafile.time_enum

        self.data = pd.Series(
            index=self._categoricalmultiindex([], [], [], []),
            dtype="float32")

        with h5py.File(datafile.h5path, "r") as f:

            for sectorname, sectordataset in datafile.sectordata.items():

                h5dset = f["data/" + sectorname]

                geo_idxs = np.nonzero(h5dset.attrs["geo_mappings"][:] != ZERO_IDX)
                geo_ids = np.array(self.geo_enum.ids)[geo_idxs]
                geo_dset_idxs = h5dset.attrs["geo_mappings"][geo_idxs]
                geo_scales = h5dset.attrs["geo_scalings"][geo_idxs]

                index = self._categoricalmultiindex(
                    [sectorname], geo_ids,
                    sectordataset.enduses, sectordataset.times)

                data = h5dset[:, :, :]
                data = data[geo_dset_idxs, :, :] * geo_scales.reshape(-1,1,1)

                self.data = self.data.append(
                    pd.Series(data.reshape(-1), index=index, dtype="float32"),
                    verify_integrity=True)

        self.data.sort_index(inplace=True)

    def __getitem__(self, idxs):

        if len(idxs) != 4:
            raise KeyError("Indexing into a Datatable requires indices " +
                           "for all four dimensions")

        return self.data.loc[idxs]

    def _categoricalmultiindex(self, sectors, geos, enduses, times):

        sectors = pd.CategoricalIndex(
            data = sectors,
            categories=self.sector_enum.ids)

        geos = pd.CategoricalIndex(
            data = geos,
            categories = self.geo_enum.ids)

        enduses = pd.CategoricalIndex(
            data = enduses,
            categories = self.enduse_enum.ids)

        times = pd.CategoricalIndex(
            data = times,
            categories = self.time_enum.ids)

        return pd.MultiIndex.from_product(
            [sectors, geos, enduses, times],
            names=["sector", "geography", "enduse", "time"])
