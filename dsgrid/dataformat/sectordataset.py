from collections import OrderedDict
import h5py
import numpy as np
import pandas as pd

from dsgrid.dataformat.enumeration import (
    SectorEnumeration, GeographyEnumeration,
    EndUseEnumeration, TimeEnumeration)

ZERO_IDX = 65535

class SectorDataset(object):

    def __init__(self,sector_id,datafile,enduses=None,times=None,loading=False):
        """
        Initialize the SectorDataset within datafile. If not loading, then are
        creating a new dataset and start persisting the new data.
        """

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

        n_total_geos = len(datafile.geo_enum.ids)
        self.n_geos = 0

        if not loading:
            with h5py.File(self.datafile.h5path, "r+") as f:
                n_enduses = len(self.enduses)
                n_times = len(self.times)

                shape = (0, n_enduses, n_times)
                max_shape = (None, n_enduses, n_times)
                chunk_shape = (1, n_enduses, n_times)

                dset = f["data"].create_dataset(
                    self.sector_id,
                    shape=shape,
                    maxshape=max_shape,
                    chunks=chunk_shape,
                    compression="gzip")

                dset.attrs["geo_mappings"] = np.full(n_total_geos, ZERO_IDX, dtype="u2")
                dset.attrs["geo_scalings"] = np.ones(shape=n_total_geos)

                dset.attrs["enduse_mappings"] = np.array([
                    datafile.enduse_enum.ids.index(enduse)
                    for enduse in self.enduses], dtype="u2")

                dset.attrs["time_mappings"] = np.array([
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


    def add_data(self,dataframe,geo_ids,scalings=[]):
        """
        Add new data to this SectorDataset, as part of the self.datafile HDF5.
        """

        if type(geo_ids) is not list:
            geo_ids = [geo_ids]

        if type(scalings) is not list:
            scalings = [scalings]

        if len(scalings) == 0:
            scalings = [1 for x in geo_ids]

        elif len(scalings) != len(geo_ids):
            raise ValueError("Geography ID and scale factor " +
                             "list lengths must match")

        for geo_id in geo_ids:
            if geo_id not in self.datafile.geo_enum.ids:
                raise ValueError("Geography ID must be in the " +
                                 "DataFile's GeographyEnumeration")

        if len(dataframe.index.unique()) != len(dataframe.index):
            raise ValueError("DataFrame row indices must be unique")

        for time in dataframe.index:
            if time not in self.times:
                raise ValueError("All time IDs (DataFrame row indices) must be " +
                                 "in the DataFile's TimeEnumeration")

        if len(dataframe.columns.unique()) != len(dataframe.columns):
            raise ValueError("DataFrame column names must be unique")

        for enduse in dataframe.columns:
            if enduse not in self.enduses:
                raise ValueError("All end-use IDs (DataFrame column names) must be " +
                                 "in the DataFile's EndUseEnumeration")

        id_idxs = np.array([
            self.datafile.geo_enum.ids.index(geo_id)
            for geo_id in geo_ids])
        scalings = np.array(scalings)
        data = np.array(dataframe.loc[self.times, self.enduses]).T
        data = np.nan_to_num(data)

        with h5py.File(self.datafile.h5path, "r+") as f:

            dset = f["data/" + self.sector_id]
            new_idx = self.n_geos

            dset.resize(new_idx+1, 0)
            dset[new_idx, :, :] = data
            self.n_geos += 1

            geo_mappings = dset.attrs["geo_mappings"][:]
            geo_mappings[id_idxs] = new_idx
            dset.attrs["geo_mappings"] = geo_mappings

            geo_scalings = dset.attrs["geo_scalings"][:]
            geo_scalings[id_idxs] = scalings
            dset.attrs["geo_scalings"] = geo_scalings


    def __setitem__(self, geo_ids, dataframe):
        self.add_data(dataframe, geo_ids)

    def __getitem__(self, geo_id):

        id_idx = self.datafile.geo_enum.ids.index(geo_id)

        with h5py.File(self.datafile.h5path, "r") as f:
            dset = f["data/" + self.sector_id]

            geo_idx = dset.attrs["geo_mappings"][id_idx]
            geo_scale = dset.attrs["geo_scalings"][id_idx]

            if geo_idx == ZERO_IDX:
                data = 0
            else:
                data = dset[geo_idx, :, :].T * geo_scale

        return pd.DataFrame(data,
                            index=self.times,
                            columns=self.enduses,
                            dtype="float32")

    def is_empty(self,geo_id):
        id_idx = self.datafile.geo_enum.ids.index(geo_id)
        with h5py.File(self.datafile.h5path, "r") as f:
            dset = f["data/" + self.sector_id]

            geo_idx = dset.attrs["geo_mappings"][id_idx]
            return geo_idx == ZERO_IDX

    @classmethod
    def loadall(cls,datafile,h5group):
        enduses = np.array(datafile.enduse_enum.ids)
        times = np.array(datafile.time_enum.ids)
        sectors = {}
        for dset_id, dset in h5group.items():
            if isinstance(dset, h5py.Dataset):
                sectors[dset_id] = cls(
                    dset_id,
                    datafile,
                    list(enduses[dset.attrs["enduse_mappings"][:]]),
                    list(times[dset.attrs["time_mappings"][:]]),
                    loading=True)

        return sectors

    def aggregate(self,agg_datafile,mapping):
        result = self.__class__(self.sector_id,agg_datafile,
            None if isinstance(mapping.to_enum,EndUseEnumeration) else self.enduses,
            None if isinstance(mapping.to_enum,TimeEnumeration) else self.times)
        if isinstance(mapping.to_enum,GeographyEnumeration):
            # Geographic mapping. Aggregation proceeds alltogether.
            data = OrderedDict()
            for geo_id in self.datafile.geo_enum.ids:
                if self.is_empty(geo_id):
                    # No data--ignore
                    continue
                new_geo_id = mapping.map(geo_id)
                if new_geo_id is None:
                    # No mapping--filter out
                    continue
                if new_geo_id in data:
                    data[new_geo_id] = data[new_geo_id].add(self[geo_id],fill_value=0.0)
                else:
                    data[new_geo_id] = self[geo_id]
            for new_geo_id in data:
                result.add_data(data[new_geo_id],new_geo_id)
        elif isinstance(mapping.to_enum,TimeEnumeration):
            # Aggregate dataframe indices
            for geo_id in self.datafile.geo_enum.ids:
                if self.is_empty(geo_id):
                    # No data--ignore
                    continue
                df = self[geo_id]
                cols = df.columns
                df[mapping.to_enum.name] = df.index.to_series().apply(mapping.map)
                # Filter out unmapped items
                df = df[~(df[mapping.to_enum.name] == None)]
                df = df.pivot_table(index=mapping.to_enum.name,
                                    values=cols,
                                    aggfunc=np.sum,
                                    fill_value=0.0)
                result.add_data(df,geo_id)
        elif isinstance(mapping.to_enum,EndUseEnumeration):
            # Aggregate dataframe columns
            for geo_id in self.datafile.geo_enum.ids:
                if self.is_empty(geo_id):
                    # No data--ignore
                    continue
                df = self[geo_id]
                df.columns = [mapping.map(col) for col in df.columns]
                # Filter out unmapped items
                df = df[[col for col in df.columns if col is not None]]
                df = df.groupby(df.columns,axis=1).sum()
                result.add_data(df,geo_id)
        else:
            raise DSGridError("SectorDataset is not able to aggregate to {}.".format(mapping.to_enum))
        return result



