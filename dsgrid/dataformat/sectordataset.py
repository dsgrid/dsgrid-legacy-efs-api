from collections import defaultdict, OrderedDict
import h5py
import logging
import numpy as np
import pandas as pd

from dsgrid import DSGridError, DSGridNotImplemented
from dsgrid.dataformat.enumeration import (
    SectorEnumeration, GeographyEnumeration,
    EndUseEnumeration, TimeEnumeration)

logger = logging.getLogger(__name__)

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
                geo_ptrs = [x for x in f["data/geo_mappings"][:] if not (x == ZERO_IDX)]
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

                dset = f["data"].create_dataset("geo_mappings",
                    data=np.full(n_total_geos, ZERO_IDX, dtype="u2"))
                dset = f["data"].create_dataset("geo_scalings",
                    data=np.ones(shape=n_total_geos))

                dset = f["data/" + self.sector_id]
                dset.attrs["enduse_mappings"] = np.array([
                    list(datafile.enduse_enum.ids).index(enduse)
                    for enduse in self.enduses], dtype="u2")

                dset.attrs["time_mappings"] = np.array([
                    list(datafile.time_enum.ids).index(time)
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

        if len(geo_ids) == 0:
            logger.info("Skipping call to add_data because geo_ids is empty.")
            if not dataframe.empty:
                logger.warn("Although geo_ids is empty, dataframe is not:\n{}".format(dataframe))
            return

        if not isinstance(scalings,(list,np.ndarray)):
            scalings = [scalings]

        if len(scalings) == 0:
            scalings = [1 for x in geo_ids]

        elif len(scalings) != len(geo_ids):
            raise ValueError("Geography ID and scale factor list lengths must " +
                "match, but len(geo_ids) = {} and len(scalings) = {}, ".format(len(geo_ids),len(scalings)) +
                "where geo_ids = {}, scalings = {}.".format(geo_ids,scalings))

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

            geo_mappings = f["data/geo_mappings"][:]
            try:
                geo_mappings[id_idxs] = new_idx
            except:
                logger.error("Unable to set geo_mappings[id_idxs] = new_idx with geo_ids: {}, id_idx: {}, geo_mappings: {}".format(repr(geo_ids), repr(id_idxs), repr(geo_mappings)))
                raise
            dset = f["data/geo_mappings"]
            dset[:] = geo_mappings

            geo_scalings = f["data/geo_scalings"][:]
            geo_scalings[id_idxs] = scalings
            dset = f["data/geo_scalings"]
            dset[:] = geo_scalings

    def __setitem__(self, geo_ids, dataframe):
        self.add_data(dataframe, geo_ids)

    def __getitem__(self, geo_id):

        id_idx = self.datafile.geo_enum.ids.index(geo_id)

        with h5py.File(self.datafile.h5path, "r") as f:
            dset = f["data/" + self.sector_id]

            geo_idx = f["data/geo_mappings"][id_idx]
            geo_scale = f["data/geo_scalings"][id_idx]

            if geo_idx == ZERO_IDX:
                data = 0
            else:
                data = dset[geo_idx, :, :].T * geo_scale

        return pd.DataFrame(data,
                            index=self.times,
                            columns=self.enduses,
                            dtype="float32")

    def get_data(self, dataset_geo_index, return_geo_ids_scales=True):
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
            raise ValueError("dataset_geo_index must be in the range [0,{}), but is {}.".format(self.n_geos,dataset_geo_index))

        geo_ids = None; scalings = None
        with h5py.File(self.datafile.h5path, "r") as f:

            dset = f["data/" + self.sector_id]
            data = dset[dataset_geo_index, :, :]
            data = data.T
            df = pd.DataFrame(data,
                              index=self.times,
                              columns=self.enduses,
                              dtype="float32")

            if return_geo_ids_scales:
                geo_mappings = f["data/geo_mappings"][:]
                geo_idxs = [i for i, val in enumerate(geo_mappings) if val == dataset_geo_index]
                geo_ids = [self.datafile.geo_enum.ids[i] for i in geo_idxs]

                geo_scalings = f["data/geo_scalings"][:]
                scalings = [geo_scalings[i] for i in geo_idxs]

        return df, geo_ids, scalings


    def copy_data(self,other_sectordataset,full_validation=True):
        for i in range(self.n_geos):
            # pull data
            df, geo_ids, scalings = self.get_data(i)
            other_sectordataset.add_data(df,geo_ids,scalings=scalings,full_validation=full_validation)


    def get_geo_map(self):
        """
        Returns ordered dict of dataset_geo_index: (geo_ids, scalings)
        """
        geo_mappings = None; geo_scalings = None
        with h5py.File(self.datafile.h5path,"r") as f:
            dset = f["data/" + self.sector_id]
            geo_mappings = f["data/geo_mappings"][:]
            geo_scalings = f["data/geo_scalings"][:]
        temp_dict = defaultdict(lambda: ([],[]))
        for i, val in enumerate(geo_mappings):
            if val == ZERO_IDX:
                continue
            temp_dict[val][0].append(self.datafile.geo_enum.ids[i])
            temp_dict[val][1].append(geo_scalings[i])
        result = OrderedDict()
        for val in sorted(temp_dict.keys()):
            result[val] = temp_dict[val]
        return result


    def is_empty(self,geo_id):
        id_idx = self.datafile.geo_enum.ids.index(geo_id)
        with h5py.File(self.datafile.h5path, "r") as f:
            dset = f["data/" + self.sector_id]

            geo_idx = f["data/geo_mappings"][id_idx]
            return geo_idx == ZERO_IDX

    @classmethod
    def loadall(cls,datafile,h5group):
        enduses = list(datafile.enduse_enum.ids)
        times = np.array(datafile.time_enum.ids)
        sectors = {}
        for dset_id, dset in h5group.items():
            if isinstance(dset, h5py.Dataset):
                if dset_id in ["geo_mappings", "geo_scalings"]:
                    continue
                sectors[dset_id] = cls(
                    dset_id,
                    datafile,
                    [enduses[i] for i in dset.attrs["enduse_mappings"][:]],
                    list(times[dset.attrs["time_mappings"][:]]),
                    loading=True)

        return sectors

    def map_dimension(self,new_datafile,mapping):
        # import ExplicitDisaggregation here to avoid circular import but be
        # able to test and raise DSGridNotImplemented as needed
        from dsgrid.dataformat.dimmap import ExplicitDisaggregation

        result = self.__class__(self.sector_id,new_datafile,
            None if isinstance(mapping.to_enum,EndUseEnumeration) else self.enduses,
            None if isinstance(mapping.to_enum,TimeEnumeration) else self.times)
        if isinstance(mapping.to_enum,GeographyEnumeration):
            # 1. Figure out how geography is mapped now, where it needs to get
            # mapped to, and what the new scalings should be on a
            # per-dataset_geo_index basis
            from_geo_map = self.get_geo_map() # dataset_geo_index: (geo_ids, scalings) in THIS dataset
            to_geo_map = OrderedDict()        # DITTO, but for mapped dataset, ignoring aggregation for now
            # new_geo_id: [dataset_geo_indices] so can see what aggregation needs to be done
            new_geo_ids_to_dataset_geo_index_map = defaultdict(lambda: [])
            for dataset_geo_index in from_geo_map:
                geo_ids, scalings = from_geo_map[dataset_geo_index]
                new_geo_ids = []
                new_scalings = []
                for i, geo_id in enumerate(geo_ids):
                    new_geo_id = mapping.map(geo_id)
                    if new_geo_id is None:
                        # filtering out; moving on
                        continue
                    if isinstance(new_geo_id,list):
                        # disaggregating
                        new_geo_ids += new_geo_id
                        new_scaling = mapping.get_scalings(new_geo_id)
                        logger.debug("Disaggregating.\n  new_geo_id: {}".format(new_geo_id) +
                                     "\n  new_scaling: {}".format(new_scaling))
                        new_scalings = np.concatenate((new_scalings, (new_scaling * scalings[i])))
                        for newid in new_geo_id:
                            new_geo_ids_to_dataset_geo_index_map[newid].append(dataset_geo_index)
                    else:
                        new_geo_ids.append(new_geo_id)
                        new_scalings.append(scalings[i])
                        new_geo_ids_to_dataset_geo_index_map[new_geo_id].append(dataset_geo_index)
                to_geo_map[dataset_geo_index] = (new_geo_ids,new_scalings)

            # 2. Step through via new_geo_ids_to_dataset_geo_index_map. Pull out
            # new_geo_ids that are aggregations across dataset_geo_indices. Then
            # process those new_geo_ids first.
            pulled_data = OrderedDict() # dataset_geo_index: df
            def get_df(dataset_geo_index,new_geo_id):
                if not (dataset_geo_index in pulled_data):
                    # pull the original data
                    df, geo_ids, scalings = self.get_data(dataset_geo_index)
                    pulled_data[dataset_geo_index] = df
                # pull the scaling factor
                ind = to_geo_map[dataset_geo_index][0].index(new_geo_id)
                scale = to_geo_map[dataset_geo_index][1][ind]
                # calculate the result
                result = pulled_data[dataset_geo_index] * scale
                # delete the items in to_geo_map that have now been handled
                to_geo_map[dataset_geo_index] = (
                    [x for i, x in enumerate(to_geo_map[dataset_geo_index][0]) if not (i == ind)],
                    [x for i, x in enumerate(to_geo_map[dataset_geo_index][1]) if not (i == ind)])
                # if this to_geo_map entry is empty, delete it, and delete the
                # pulled_data too
                assert len(to_geo_map[dataset_geo_index][0]) == len(to_geo_map[dataset_geo_index][1])
                if len(to_geo_map[dataset_geo_index][0]) == 0:
                    del to_geo_map[dataset_geo_index]
                    del pulled_data[dataset_geo_index]
                return result

            require_aggregation = []
            for new_geo_id in new_geo_ids_to_dataset_geo_index_map:
                if len(new_geo_ids_to_dataset_geo_index_map[new_geo_id]) > 1:
                    require_aggregation.append(new_geo_id)
            for new_geo_id in require_aggregation:
                aggdf = None
                for dataset_geo_index in new_geo_ids_to_dataset_geo_index_map[new_geo_id]:
                    tempdf = get_df(dataset_geo_index,new_geo_id)
                    if aggdf is None:
                        aggdf = tempdf
                    else:
                        aggdf = aggdf.add(tempdf,fill_value=0.0)
                result.add_data(aggdf,[new_geo_id],full_validation=False)

            # 3. Now add data that did not need to be aggregated
            for dataset_geo_index in to_geo_map:
                geo_ids, scalings = to_geo_map[dataset_geo_index]
                if len(geo_ids) == 0:
                    logger.debug("All data from dataset_geo_index {} is being dropped.".format(dataset_geo_index))
                    continue
                if dataset_geo_index in pulled_data:
                    df = pulled_data[dataset_geo_index]
                    del pulled_data[dataset_geo_index]
                else:
                    df, junk1, junk2 = self.get_data(dataset_geo_index,return_geo_ids_scales=False)
                result.add_data(df,geo_ids,scalings=scalings,full_validation=False)

        elif isinstance(mapping.to_enum,TimeEnumeration):
            if isinstance(mapping,ExplicitDisaggregation):
                raise DSGridNotImplemented("Temporal disaggregations have not been implemented.")
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
            if isinstance(mapping,ExplicitDisaggregation):
                raise DSGridNotImplemented("End-use disaggregations have not been implemented.")
            # Map dataframe columns
            for i in range(self.n_geos):
                # pull data
                df, geo_ids, scalings = self.get_data(i)

                # apply the mapping
                df.columns = [mapping.map(col) for col in df.columns]
                # filter out unmapped items
                df = df.iloc[:,[i for i, col in enumerate(df.columns) if col is not None]]
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
