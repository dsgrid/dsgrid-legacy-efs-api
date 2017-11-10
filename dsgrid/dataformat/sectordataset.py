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

        if loading:
            # determine how many geos
            with h5py.File(self.datafile.h5path,'r') as f:
                dset = f["data/" + self.sector_id]
                geo_ptrs = [x for x in dset.attrs["geo_mappings"] if not (x == ZERO_IDX)]
                self.n_geos = len(set(geo_ptrs))
        else:
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


    def add_data(self,dataframe,geo_ids,scalings=[],full_validation=True):
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

        if full_validation:
            for geo_id in geo_ids:
                if geo_id not in self.datafile.geo_enum.ids:
                    raise ValueError("Geography ID must be in the " +
                                     "DataFile's GeographyEnumeration")

        if len(dataframe.index.unique()) != len(dataframe.index):
            raise ValueError("DataFrame row indices must be unique")

        if full_validation:
            for time in dataframe.index:
                if time not in self.times:
                    raise ValueError("All time IDs (DataFrame row indices) must be " +
                                     "in the DataFile's TimeEnumeration")

        if len(dataframe.columns.unique()) != len(dataframe.columns):
            raise ValueError("DataFrame column names must be unique")

        if full_validation:
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

    def get_data(self, dataset_geo_index):
        """
        Get data in this file's native format. 

        Arguments:
            - dataset_geo_index (int) - Index into the geography dimension of 
                  this dataset. Is an integer in the range [0,self.n_geos) that
                  corresponds to the values in this dataset's geo_mappings that 
                  are not equal to ZERO_IDX.

        Returns dataframe, geo_ids, scalings that are needed to add_data.
        """
        if (dataset_geo_index < 0) or (not dataset_geo_index < self.n_geos):
            raise ValueError("dataset_geo_index must be in the range [0,{})".format(self.n_geos))

        with h5py.File(self.datafile.h5path, "r") as f:

            dset = f["data/" + self.sector_id]
            data = dset[dataset_geo_index, :, :]
            data = data.T
            df = pd.DataFrame(data,
                              index=self.times,
                              columns=self.enduses,
                              dtype="float32")

            geo_mappings = dset.attrs["geo_mappings"][:]
            geo_idxs = [i for i, val in enumerate(geo_mappings) if val == dataset_geo_index]
            geo_ids = [self.datafile.geo_enum.ids[i] for i in geo_idxs]

            geo_scalings = dset.attrs["geo_scalings"][:]
            scalings = [geo_scalings[i] for i in geo_idxs]
        return df, geo_ids, scalings


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

    def map_dimension(self,new_datafile,mapping):
        result = self.__class__(self.sector_id,new_datafile,
            None if isinstance(mapping.to_enum,EndUseEnumeration) else self.enduses,
            None if isinstance(mapping.to_enum,TimeEnumeration) else self.times)
        if isinstance(mapping.to_enum,GeographyEnumeration):
            # Geographic mapping. Aggregation proceeds alltogether
            # TODO: See if this can be sped up using the get_data method
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
                result.add_data(data[new_geo_id],[new_geo_id],full_validation=False)

        elif isinstance(mapping.to_enum,TimeEnumeration):
            # Map dataframe indices
            for i in range(self.n_geos):
                # pull data
                df, geo_ids, scalings = self.get_data(i)

                # apply the mapping
                cols = df.columns
                df[mapping.to_enum.name] = df.index.to_series().apply(mapping.map)
                # filter out unmapped items
                df = df[~(df[mapping.to_enum.name] == None)]
                df = df.pivot_table(index=mapping.to_enum.name,
                                    values=cols,
                                    aggfunc=np.sum,
                                    fill_value=0.0)

                # add the mapped data to the new file
                result.add_data(df,geo_ids,scalings=scalings,full_validation=False)

        elif isinstance(mapping.to_enum,EndUseEnumeration):
            # Map dataframe columns
            for i in range(self.n_geos):
                # pull data
                df, geo_ids, scalings = self.get_data(i)

                # apply the mapping
                df.columns = [mapping.map(col) for col in df.columns]
                # filter out unmapped items
                df = df[[col for col in df.columns if col is not None]]
                df = df.groupby(df.columns,axis=1).sum()

                # add the mapped data to the new file
                result.add_data(df,geo_ids,scalings=scalings,full_validation=False)

        else:
            raise DSGridError("SectorDataset is not able to map to {}.".format(mapping.to_enum))
        return result


    def scale_data(self,new_datafile,factor=0.001):
        """
        Scale all the data in self by factor, creating a new HDF5 file and 
        corresponding Datafile. 

        Arguments:
            - filepath (str) - Location for the new HDF5 file to be created
            - factor (float) - Factor by which all the data in the file is to be
                  multiplied. The default value of 0.001 corresponds to converting
                  the bottom-up data from kWh to MWh.
        """
        result = self.__class__(self.sector_id,new_datafile,self.enduses,self.times)
        # pull data
        for i in range(self.n_geos):
            df, geo_ids, scalings = self.get_data(i)

            # apply the scaling
            scalings = [x * factor for x in scalings]

            # add the mapped data to the new file
            result.add_data(df,geo_ids,scalings=scalings,full_validation=False)

        return result

