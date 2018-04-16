from collections import defaultdict, OrderedDict
from distutils.version import StrictVersion
from itertools import repeat
import h5py
import logging
import numpy as np
import pandas as pd

from dsgrid import __version__ as VERSION
from dsgrid import DSGridError, DSGridNotImplemented
from dsgrid.dataformat.enumeration import (
    SectorEnumeration, GeographyEnumeration,
    EndUseEnumerationBase, TimeEnumeration)

logger = logging.getLogger(__name__)

NULL_IDX = 2**32 - 1
enum_datamap_dtype = np.dtype([
    ("idx", "u4"), # 32-bit unsigned integer index
    ("scale", "f4") # 32-bit float scaling factor
])

class Datamap(object):
    """
    Map between Datafile-level enumeration (enum) and Sectordataset-level 
    sub-enumeration (enum_ids). Sub-enumeration may also have non-unity scaling 
    factors. Per Sectordataset, these Datamaps link each enumeration value with:

      a) a particular index in the dataset along the Enumeration's
         dimension; and
      b) a scaling parameter to apply to the associated underlying
         data.

    Multiple enumeration values can refer to the same index in the
    dataset's enumeration dimension, with the option to apply different
    scaling factors. The index is represented as a 32-bit unsigned
    integer, which limits dataset size to 2^32 - 2 in each dimension,
    with NULL_IDX (2^32 - 1) serving as the sentinel value assigned to
    enumeration values not described in the dataset (looking up data
    associated with such an enumeration value will simply return zeros)

    Attributes
    ----------
    value : numpy.ndarray
        datamap vector of length len(enum.ids) with 'idx' and 'scale' 
        dimensions. For the example of j, scale = datamap[i], 
        - i = position of enum_id in enum.ids (datafile-level enum)
        - j = position of enum_id in enum_ids (sectordataset-level sub-enum)
        - scale = scaling factor to apply to this enumeration element
    """

    def __init__(self,value):
        assert isinstance(value,np.ndarray), type(value)
        self.value = value

    @classmethod
    def create(cls, enum, enum_ids, enum_scales=None):
        """
        Parameters
        ----------
        enum : dsgrid.enumeration.Enumeration
        enum_ids : list
            List of items in enum.ids
        enum_scales : None or list
            if list, is list of floats the same length as enum_ids

        Returns
        -------
        Datamap
        """
        datamap = np.empty(len(enum.ids), enum_datamap_dtype)
        datamap["idx"] = NULL_IDX
        datamap["scale"] = 0.

        if enum_scales:
            if len(enum_ids) != len(enum_scales):
                raise ValueError(enum.dimension + " id list has length " +
                                 len(enum_ids) + ", but scaling factor list " +
                                 "has length " + len(enum_scales))

        else:
            enum_scales = repeat(1.0, len(enum_ids))

        for i, (enum_id, enum_scale) in enumerate(zip(enum_ids, enum_scales)):
            enum_idx = list(enum.ids).index(enum_id)
            datamap[enum_idx] = (i, enum_scale)

        return cls(datamap)

    @classmethod
    def load(cls,dataset):
        """
        Parameters
        ----------
        dataset : h5py.Dataset
            a Datamap serialized to h5py

        Returns
        -------
        Datamap
        """
        assert isinstance(dataset,h5py.Dataset)
        idx = dataset[:,"idx"]
        scale = dataset[:,"scale"]
        datamap = np.empty(len(idx), enum_datamap_dtype)
        datamap["idx"] = idx
        datamap["scale"] = scale
        return cls(datamap)

    def update(self,dataset):
        """
        Updates dataset with this Datamap's value. Overwrites current 
        dataset[:,'idx'] and dataset[:,'scale'].

        Parameters
        ----------
        dataset : h5py.Dataset
            a Datamap serialized to h5py
        """
        dataset[:,"idx"] = self.value["idx"]
        dataset[:,"scale"] = self.value["scale"]

    @property
    def num_entries(self):
        """
        The number of non-null idx values in this Datamap's value. Corresponds, 
        e.g. to the number of number of distinct (up to a scaling factor) 
        entries along a dimension.
        """
        original_idxes = np.flatnonzero(self.value["idx"] != NULL_IDX)
        return len(set(self.value["idx"][original_idxes]))

    def get_subenum(self,enum):
        """
        Parameters
        ----------
        enum : dsgrid.dataformat.enumeration.Enumeration
            Datafile-level enumeration

        Returns
        -------
        list of enum.ids
            In the correct order for this Sectordataset
        """
        full_enum_ids = list(enum.ids)
        original_idxes = np.flatnonzero(self.value["idx"] != NULL_IDX)
        original_idxes = sorted(original_idxes,key=lambda x: self.value[x]['idx'])
        return [full_enum_ids[i] for i in original_idxes]

    def is_empty(self,enum_id,enum):
        """
        Parameters
        ----------
        enum_id : str
            element of enum.ids
        enum : dsgrid.dataformat.enumeration.Enumeration
            Datafile-level enumeration

        Returns
        -------
        bool
            True if the dataset has no data for enum_id, False otherwise
        """
        id_idx = list(enum.ids).index(enum_id)
        return self.value[id_idx]['idx'] == NULL_IDX

    def get_map(self,enum):
        """
        Get the data in this map in ordered, hashed form, with the dataset-level 
        idx as the key.

        Parameters
        ----------
        enum : dsgrid.dataformat.enumeration.Enumeration
            Datafile-level enumeration

        Returns
        -------
        OrderedDict
            idx: (list of enum.ids, scales)
        """
        result = OrderedDict()
        original_idxes = np.flatnonzero(self.value["idx"] != NULL_IDX)
        unique_idxes = sorted(list(set(self.value["idx"][original_idxes])))
        for i in unique_idxes:
            result[i] = (self.ids(i,enum), self.scales(i))
        return result   

    def ids(self,idx,enum):
        """
        Returns the enum.ids for that are mapped to the dataset idx

        Parameters
        ----------
        idx : int
            dataset-level sub-enumeration index
        enum : dsgrid.dataformat.Enumeration
            datafile-level enumeration the sub-enum is based on

        Returns
        -------
        list of enum.ids
            in particular, the ones that are mapped to idx, in the order 
            specified by enum.ids
        """
        orig_idxs = [i for i, val in enumerate(self.value['idx']) if val == idx]
        all_ids = list(enum.ids)
        return [all_ids[i] for i in orig_idxs]

    def scales(self,idx):
        """
        Returns the scaling factors that correspond to the dataset idx

        Parameters
        ----------
        idx : int
            dataset-level sub-enumeration index

        Returns
        -------
        list of float
            one for each of the .ids(idx,enum), and in the same order
        """
        orig_idxs = [i for i, val in enumerate(self.value['idx']) if val == idx]
        return [self.value[i]['scale'] for i in orig_idxs]

    def append_element(self,new_elem_idx,enum_ids,enum,scalings=[]):
        """
        Appends a new non-null element for this Datamap that defines the index 
        (new_elem_idx) for data that corresponds to enum_ids in enum.

        Parameters
        ----------
        new_elem_idx : int
            index value for the new element
        enum_ids : list
            list of distinct elements in enum.ids
        enum : dsgrid.dataformat.enumeration.Enumeration
            should be same Enumeration originally used to .create this Datamap
        scalinges : list of numeric
            if empty, will be defaulted to 1.0. otherwise should be the same 
            length as enum_ids
        """
        if not isinstance(scalings,(list,np.ndarray)):
            scalings = [scalings]

        if len(scalings) == 0:
            scalings = [1 for x in enum_ids]

        elif len(scalings) != len(enum_ids):
            raise ValueError("Enum id and scale factor list lengths must " +
                "match, but len(enum_ids) = {} and len(scalings) = {}, ".format(len(enum_ids),len(scalings)) +
                "where enum_ids = {}, scalings = {}.".format(enum_ids,scalings))

        id_idxs = np.array([enum.ids.index(enum_id) for enum_id in enum_ids])
        scalings = np.array(scalings)

        try:
            self.value["idx"][id_idxs] = new_elem_idx
        except:
            logger.error("Unable to set the selected enumeration ids to new_elem_idx. " + 
                "enum_ids: {}, id_idxs: {}, new_elem_ids: {}, datamap: {}".format(repr(enum_ids), repr(id_idxs), repr(new_elem_idx), repr(self.value)))
            raise
        self.value["scale"][id_idxs] = scalings
        return


def append_element_to_dataset_dimension(dataset,new_elem_idx,enum_ids,enum,scalings=[]):
    """
    Helper method to do all the work of adding a new element to a SectorDataset 
    dimenstion.

    Parameters
    ----------
    dataset : h5py.Dataset
        a Datamap serialized to h5py
    new_elem_idx : int
        index value for the new element
    enum_ids : list
        list of distinct elements in enum.ids
    enum : dsgrid.dataformat.enumeration.Enumeration
        should be same Enumeration originally used to .create this Datamap
    scalinges : list of numeric
        if empty, will be defaulted to 1.0. otherwise should be the same 
        length as enum_ids
    """
    datamap = Datamap.load(dataset)
    datamap.append_element(new_elem_idx,enum_ids,enum,scalings=scalings)
    datamap.update(dataset)


class SectorDataset(object):

    def __init__(self,datafile,sector_id,enduses,times):
        """
        Creates a SectorDataset object. Note that this does not read from
        or write to datafile in any way, and should generally not be called directly.
        Instead, use the SectorDataset.load or SectorDataset.new class methods.
        """

        if sector_id not in datafile.sector_enum.ids:
            raise ValueError("Sector ID " + sector_id + " is not in " +
                                "the Datafile's SectorEnumeration")

        if not set(enduses).issubset(set(datafile.enduse_enum.ids)):
            raise ValueError("Supplied enduses (" + str(enduses) +
                             ") are not a subset of the " +
                             "Datafile's EndUseEnumeration ids (" +
                             str(datafile.enduse_enum.ids) + ")")

        if not set(times).issubset(set(datafile.time_enum.ids)):
            raise ValueError("Supplied times are not a subset of the " +
                             "Datafile's TimeEnumeration")

        self.sector_id = sector_id
        self.datafile = datafile
        self.enduses = enduses
        self.times = times

        self.n_geos = 0 # data is inserted by geography

    @classmethod
    def new(cls,datafile,sector_id,enduses=None,times=None):

        if not enduses:
            enduses = datafile.enduse_enum.ids

        if not times:
            times = datafile.time_enum.ids

        sdset = cls(datafile,sector_id,enduses,times)

        n_enduses = len(enduses)
        n_times = len(times)
        shape = (0, n_enduses, n_times)
        max_shape = (None, n_enduses, n_times)
        chunk_shape = (1, n_enduses, n_times)

        with h5py.File(datafile.h5path, "r+") as f:

            dgroup = f["data"].create_group(sector_id)

            dset = dgroup.create_dataset(
                "data",
                shape=shape,
                maxshape=max_shape,
                chunks=chunk_shape,
                compression="gzip")

            dgroup["geographies"] = Datamap.create(datafile.geo_enum, []).value
            dgroup["enduses"] = Datamap.create(datafile.enduse_enum, enduses).value
            dgroup["times"] = Datamap.create(datafile.time_enum, times).value

        return sdset


    @classmethod
    def load(cls,datafile,f,sector_id):
        dgroup = f["data/" + sector_id]

        datamap = Datamap.load(dgroup["enduses"])
        enduses = datamap.get_subenum(datafile.enduse_enum)

        datamap = Datamap.load(dgroup["times"])
        times = datamap.get_subenum(datafile.time_enum)

        result = cls(datafile,sector_id,enduses,times)

        datamap = Datamap.load(dgroup["geographies"])
        result.n_geos = datamap.num_entries

        return result


    @classmethod
    def loadall(cls,datafile,f,_upgrade_class=None):

        for sector_id, sector_group in f["data"].items():
            if _upgrade_class is not None:
                yield sector_id, _upgrade_class.load_sectordataset(datafile,f,sector_id)
                continue
            assert isinstance(sector_group, h5py.Group)
            yield sector_id, SectorDataset.load(datafile,f,sector_id)


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

        Parameters
        ----------
        dataframe : pandas.DataFrame
           Data to add, indexed by times, and with columns equal to enduses.
        geo_ids : id or list of ids
           Ids map to datafile.geo_enum
        scalings : list of float
           If non-empty, must be same length as geo_ids and represents the 
           scaling factors for the geo_ids in order. Otherwise, a uniform value
           of 1.0 is assumed for all geo_ids.
        full_validation : bool
           If true, checks that all enumeration ids (time, enduse, and geography)
           are valid.
        """

        if type(geo_ids) is not list:
            geo_ids = [geo_ids]

        if len(geo_ids) == 0:
            logger.info("Skipping call to add_data because geo_ids is empty.")
            if not dataframe.empty:
                logger.warn("Although geo_ids is empty, dataframe is not:\n{}".format(dataframe))
            return

        if full_validation:
            for geo_id in geo_ids:
                if geo_id not in self.datafile.geo_enum.ids:
                    raise ValueError("Geography ID must be in the DataFile's " + 
                        "GeographyEnumeration, but is {!r}".format(geo_id))

        if len(dataframe.index.unique()) != len(dataframe.index):
            raise ValueError("DataFrame row indices must be unique")

        if full_validation:
            for time in dataframe.index:
                if time not in self.times:
                    raise ValueError("Time ID (DataFrame row index) {!r}".format(time) +
" is invalid: time IDs must both exist in the DataFile's TimeEnumeration and "
"have been defined as a sector-relevant time during the SectorDataset's creation.")

        if len(dataframe.columns.unique()) != len(dataframe.columns):
            raise ValueError("DataFrame column names must be unique")

        if full_validation:
            for enduse in dataframe.columns:
                if enduse not in self.enduses:
                    raise ValueError("End-use ID (DataFrame column name) " + enduse +
" is invalid: end-use IDs must both exist in the DataFile's EndUseEnumeration and "
"have been defined as a sector-relevant end-use during the SectorDataset's creation.")

        data = np.array(dataframe.loc[self.times, self.enduses]).T
        data = np.nan_to_num(data)

        with h5py.File(self.datafile.h5path, "r+") as f:

            dgroup = f["data/" + self.sector_id]
            dset = dgroup["data"]

            new_geo_idx = self.n_geos
            self.n_geos += 1

            dset.resize(self.n_geos, 0)
            dset[new_geo_idx, :, :] = data

            append_element_to_dataset_dimension(dgroup["geographies"],
                new_geo_idx,geo_ids,self.datafile.geo_enum,scalings=scalings)

    def __setitem__(self, geo_ids, dataframe):
        self.add_data(dataframe, geo_ids)

    def __getitem__(self, geo_id):

        id_idx = self.datafile.geo_enum.ids.index(geo_id)

        with h5py.File(self.datafile.h5path, "r") as f:
            dgroup = f["data/" + self.sector_id]
            dset = dgroup["data"]

            geo_idx, geo_scale = dgroup["geographies"][id_idx]

            if geo_idx == NULL_IDX:
                data = 0
            else:
                data = dset[geo_idx, :, :].T * geo_scale

        return pd.DataFrame(data,
                            index=self.times,
                            columns=self.enduses,
                            dtype="float32")

    def get_datamap(self,dim_key):
        with h5py.File(self.datafile.h5path, "r") as f:
            dgroup = f["data"][self.sector_id]
            result = Datamap.load(dgroup[dim_key])
        return result

    def get_data(self, dataset_geo_index):
        """
        Get data in this file's native format.

        Parameters
        ----------
        dataset_geo_index : int
            Index into the geography dimension of this dataset. Is an integer 
            in the range [0,self.n_geos) that corresponds to the values in this 
            dataset's geographies[:,'idx'] that are not equal to NULL_IDX

        Returns
        -------
        pandas.DataFrame
            data indexed by time and differentiated by enduse (as columns)
        list of .datafile.geo_enum.ids
            geographic enum values this data applies to
        list of float
            one scaling factor for each geographic enum value
        """
        if (dataset_geo_index < 0) or (not dataset_geo_index < self.n_geos):
            raise ValueError("dataset_geo_index must be in the range [0,{}), but is {}.".format(self.n_geos,dataset_geo_index))

        with h5py.File(self.datafile.h5path, "r") as f:

            dgroup = f["data"][self.sector_id]
            dset = dgroup["data"]
            data = dset[dataset_geo_index, :, :]
            data = data.T
            df = pd.DataFrame(data,
                              index=self.times,
                              columns=self.enduses,
                              dtype="float32")

            geo_datamap = Datamap.load(dgroup["geographies"])
            geo_ids = geo_datamap.ids(dataset_geo_index,self.datafile.geo_enum)
            scalings = geo_datamap.scales(dataset_geo_index)

        return df, geo_ids, scalings

    def copy_data(self,other_sectordataset,full_validation=True):
        """
        Copy data from this SectorDataset into other_sectordataset.

        Parameters
        ----------
        other_sectordataset : SectorDataset
            target for this SectorDataset's data to be copied into
        full_validation : bool
            flag for SectorDataset.add_data
        """
        for i in range(self.n_geos):
            # pull data
            df, geo_ids, scalings = self.get_data(i)
            other_sectordataset.add_data(df,geo_ids,scalings=scalings,full_validation=full_validation)

    def map_dimension(self,new_datafile,mapping):
        # import ExplicitDisaggregation here to avoid circular import but be
        # able to test and raise DSGridNotImplemented as needed
        from dsgrid.dataformat.dimmap import ExplicitDisaggregation

        result = self.__class__.new(new_datafile,self.sector_id,
            enduses=None if isinstance(mapping.to_enum,EndUseEnumerationBase) else self.enduses,
            times=None if isinstance(mapping.to_enum,TimeEnumeration) else self.times)
        if isinstance(mapping.to_enum,GeographyEnumeration):
            # 1. Figure out how geography is mapped now, where it needs to get
            # mapped to, and what the new scalings should be on a
            # per-dataset_geo_index basis
            geo_datamap = self.get_datamap('geographies')
            from_geo_map = geo_datamap.get_map(self.datafile.geo_enum) # dataset_geo_index: (geo_ids, scalings) in THIS dataset
            to_geo_map = OrderedDict()                                 # DITTO, but for mapped dataset, ignoring aggregation for now
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
                    df, junk1, junk2 = self.get_data(dataset_geo_index)
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

        elif isinstance(mapping.to_enum,EndUseEnumerationBase):
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

        Parameters
        ----------
        filepath : str
            Location for the new HDF5 file to be created
        factor : float
            Factor by which all the data in the file is to be multiplied. The 
            default value of 0.001 corresponds to converting the bottom-up data 
            from kWh to MWh.
        """
        result = self.__class__.new(new_datafile,self.sector_id,self.enduses,self.times)
        # pull data
        for i in range(self.n_geos):
            df, geo_ids, scalings = self.get_data(i)

            # apply the scaling
            scalings = [x * factor for x in scalings]

            # add the mapped data to the new file
            result.add_data(df,geo_ids,scalings=scalings,full_validation=False)

        return result
