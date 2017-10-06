import numpy as np

ZERO_IDX = 65535

class SectorDataset(object):

    def __init__(self, sector_id, datafile, enduse_ids=None, time_ids=None):

        if not enduse_ids:
            enduse_ids = datafile.enduse_enum.ids

        if not time_ids:
            time_ids = datafile.time_enum.ids

        self.sector_id = sector_id
        self.datafile = datafile

        # Initialize geography metadata
        n_total_geos = len(datafile.geo_enum.ids)
        self.geo_ids = []
        self.n_geos = 0
        self.geo_mappings = np.full(n_total_geos, ZERO_IDX, dtype="u2")
        self.geo_scalings = np.ones(shape=n_total_geos)

        # Initialize enduse metadata
        n_total_enduses = len(datafile.enduse_enum.ids)
        self.enduse_ids = enduse_ids
        self.n_enduses = len(self.enduses)
        self.enduse_mappings = np.full(n_total_enduses, ZERO_IDX, dtype="u2")
        self.enduse_scalings = np.ones(shape=n_total_enduses)

        for i, enduse_id in enumerate(self.enduse_ids):
            enduse_idx = datafile.enduse_enum.ids.index(enduse_id)
            self.enduse_mappings[enduse_idx] = i

        # Initialize time metadata
        n_total_times = len(datafile.time_enum.ids)
        self.time_ids = time_ids
        self.n_times = len(self.times)
        self.time_mappings = np.full(n_total_times, ZERO_IDX, dtype="u2")
        self.time_scalings = np.ones(shape=n_total_times)

        for i, time_id in enumerate(self.time_ids):
            time_idx = datafile.time_enum.ids.index(time_id)
            self.time_mappings[time_idx] = i

    def h5init(self, h5group):

        shape = (0, self.n_enduses, self.n_times)
        max_shape = (None, self.n_enduses, self.n_times)
        chunk_shape = (1, self.n_enduses, self.n_times)

        dset = h5group.create_dataset(
            self.sector_id,
            shape=shape
            maxshape=max_shape,
            chunks=chunk_shape,
            compression="gzip")

        dset.attrs["geo_mappings"] = self.geo_mappings
        dset.attrs["geo_scalings"] = self.geo_scalings

        dset.attrs["enduse_mappings"] = self.enduse_mappings
        dset.attrs["enduse_scalings"] = self.enduse_scalings

        dset.attrs["time_mappings"] = self.time_mappings
        dset.attrs["time_scalings"] = self.time_scalings


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

        for time_id in dataframe.index:
            if time_id not in self.time_ids:
                raise ValueError("All time IDs (DataFrame row indices) must be " +
                                 "in the DataFile's TimeEnumeration")

        for enduse_id in dataframe.columns:
            if enduse_id not in self.enduse_ids:
                raise ValueError("All end-use IDs (DataFrame column names) must be " +
                                 "in the DataFile's EndUseEnumeration")

        data = np.array(dataframe[self.times, self.enduse])
        np.nan_to_num(data, copy=False)

        with h5py.File(self.datafile.h5path, "w-") as f:

            dset = f["data/"+self.sector_id]
            new_idx = self.n_geos
            dset.resize(new_idx+1, 1)
            dset[new_idx, :, :] = data
            n_geos += 1
