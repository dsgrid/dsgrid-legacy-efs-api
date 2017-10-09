import h5py
import numpy as np

ZERO_IDX = 65535

class SectorDataset(object):

    def __init__(self, sector_id, datafile, enduses=None, times=None):

        if sector_id not in datafile.sector_enum.ids:
            raise ValueError("Sector ID " + sector_id + " is not in " +
                                "the Datafile's SectorEnumeration")

        if enduses:
            if not set(enduses).issubset(set(datafile.enduse_enum.ids)):
                raise ValueError("Supplied enduses are not a subset of the " +
                                 "Datafile's EndUseEnumeration")
        else:
            enduses = datafile.enduse_enum.ids

        if times:
            if not set(times).issubset(set(datafile.time_enum.ids)):
                raise ValueError("Supplied times are not a subset of the " +
                             "Datafile's TimeEnumeration")
        else:
            times = datafile.time_enum.ids

        self.sector_id = sector_id
        self.datafile = datafile
        self.enduses = enduses
        self.times = times

        # Initialize geography metadata
        n_total_geos = len(datafile.geo_enum.ids)
        self.geo_ids = []
        self.n_geos = 0
        self.geo_mappings = np.full(n_total_geos, ZERO_IDX, dtype="u2")
        self.geo_scalings = np.ones(shape=n_total_geos)

        # Initialize enduse metadata
        self.n_enduses = len(self.enduses)
        self.enduse_mappings = np.array([
            datafile.enduse_enum.ids.index(enduse)
            for enduse in self.enduses], dtype="u2")

        # Initialize time metadata
        self.n_times = len(self.times)
        self.time_mappings = np.array([
            datafile.time_enum.ids.index(time)
            for time in self.times], dtype="u2")

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            self.__dict__ == self.__dict__
            )

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def __str__(self):
        return self.__repr__()

    def h5init(self, h5group):

        shape = (0, self.n_enduses, self.n_times)
        max_shape = (None, self.n_enduses, self.n_times)
        chunk_shape = (1, self.n_enduses, self.n_times)

        dset = h5group.create_dataset(
            self.sector_id,
            shape=shape,
            maxshape=max_shape,
            chunks=chunk_shape,
            compression="gzip")

        dset.attrs["geo_mappings"] = self.geo_mappings
        dset.attrs["geo_scalings"] = self.geo_scalings

        dset.attrs["enduse_mappings"] = self.enduse_mappings

        dset.attrs["time_mappings"] = self.time_mappings


    def add_data(self, dataframe, geo_ids, scale=[]):

        if type(geo_ids) is not list:
            geo_ids = [geo_ids]

        if type(scale) is not list:
            scale = [scale]

        if len(scale) == 0:
            scales = [1 for x in geo_ids]

        elif len(scale) != len(geo_ids):
            raise ValueError("Geography ID and scale factor " +
                             "list lengths must match")

        for geo_id in geo_ids:
            if geo_id not in self.datafile.geo_enum.ids:
                raise ValueError("Geography ID must be in the " +
                                 "DataFile's GeographyEnumeration")

        for time in dataframe.index:
            if time not in self.times:
                raise ValueError("All time IDs (DataFrame row indices) must be " +
                                 "in the DataFile's TimeEnumeration")

        for enduse in dataframe.columns:
            if enduse not in self.enduses:
                raise ValueError("All end-use IDs (DataFrame column names) must be " +
                                 "in the DataFile's EndUseEnumeration")

        data = np.array(dataframe.loc[self.times, self.enduses])
        np.nan_to_num(data, copy=False)

        with h5py.File(self.datafile.h5path, "r+") as f:

            dset = f["data/" + self.sector_id]
            new_idx = self.n_geos
            dset.resize(new_idx+1, 0)
            dset[new_idx, :, :] = data
            self.n_geos += 1

    @classmethod
    def loadall(cls, datafile, h5group):

        enduses = np.array(datafile.enduse_enum.ids)
        times = np.array(datafile.time_enum.ids)

        sectors = {}

        for dset_id, dset in h5group.items():

            if isinstance(dset, h5py.Dataset):

                sectors[dset_id] = cls(
                    dset_id,
                    datafile,
                    list(enduses[dset.attrs["enduse_mappings"][:]]),
                    list(times[dset.attrs["time_mappings"][:]])
                )

        return sectors
