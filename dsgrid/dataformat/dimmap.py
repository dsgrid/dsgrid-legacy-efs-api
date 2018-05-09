from collections import defaultdict
import os

import numpy as np
import pandas as pd

from dsgrid import DSGridError
from dsgrid.dataformat.datatable import Datatable
from dsgrid.dataformat.enumeration import (
    SectorEnumeration, GeographyEnumeration, EndUseEnumerationBase, TimeEnumeration,
    allenduses,allsectors,annual,census_divisions,census_regions,conus,counties,
    enduses,enumdata_folder,fuel_types,hourly2012,loss_state_groups,sectors,
    sectors_subsectors,states)

class DimensionMap(object):
    def __init__(self,from_enum,to_enum):
        self.from_enum = from_enum
        self.to_enum = to_enum

    def map(self,from_id):
        """
        Returns the appropriate to_id.
        """
        return None


class TautologyMapping(DimensionMap):
    def __init__(self,from_to_enum):
        super().__init__(from_to_enum,from_to_enum)

    def map(self,from_id):
        return from_id


class FullAggregationMap(DimensionMap):

    def __init__(self,from_enum,to_enum,exclude_list=[]):
        """
        Arguments:
            - to_enum (Enumeration) - an enumeration with exactly one element
            - exclude_list (list of from_enum.ids) - from_enum values that should 
                  be dropped from the aggregation
        """
        super().__init__(from_enum,to_enum)
        if len(to_enum.ids) > 1:
            raise DSGridError("FullAggregationMaps are aggregates that may exclude " + 
                "some items, but otherwise aggretate up to one quantity. " + 
                "to_enum {} contains too many items.".format(repr(to_enum)))
        self.to_id = to_enum.ids[0]

        self.exclude_list = exclude_list
        for exclude_item in self.exclude_list:
            if exclude_item not in from_enum.ids:
                raise DSGridError("exclude_list must contain ids in from_enum " + 
                    "that are to be exluded from the overall aggregation. "
                    "Found {} in exclude list, which is not in {}.".format(exclude_item,from_enum))

    def map(self,from_id):
        if from_id in self.exclude_list:
            return None
        return self.to_id


class FilterToSubsetMap(DimensionMap):
    def __init__(self,from_enum,to_enum):
        """
        Arguments:
            - to_enum (Enumeration) - should be a subset of from_enum
        """
        super().__init__(from_enum,to_enum)
        for to_id in to_enum.ids:
            if not to_id in from_enum.ids:
                raise DSGridError("to_enum should be a subset of from_enum")

    def map(self,from_id):
        if from_id in self.to_enum.ids:
            return from_id
        return None


class ExplicitMap(DimensionMap):
    def __init__(self,from_enum,to_enum,dictmap):
        super().__init__(from_enum,to_enum)

    def map(self,from_id):
        return self._dictmap[from_id]

    @classmethod
    def create_from_csv(cls,from_enum,to_enum,filepath):
        mapdata = pd.read_csv(filepath,dtype=str)
        return cls(from_enum,to_enum,cls._make_dictmap(mapdata))


class ExplicitDisaggregation(ExplicitMap):
    def __init__(self,from_enum,to_enum,dictmap,scaling_datafile=None):
        """
        If no scaling_datafile, scaling factors are assumed to be 1.0.
        """
        super().__init__(from_enum,to_enum,dictmap)
        self._dictmap = defaultdict(lambda: [])
        for from_id, to_ids in dictmap.items():
            if from_id not in self.from_enum.ids:
                raise DSGridError("Id {} is not in from_enum {}.".format(from_id,self.from_enum))
            for to_id in to_ids:
                if to_id not in self.to_enum.ids:
                    raise DSGridError("Id {} is not in to_enum {}.".format(to_id,self.to_enum))
            self._dictmap[from_id] = to_ids
        # scaling_datafile must have to_enum as one of its dimensions
        if (scaling_datafile is not None) and (not scaling_datafile.contains(to_enum)):
            raise DSGridError("Datafile {} cannot be used to scale this map ".format(repr(scaling_datafile)) + 
                "because it does not contain to_enum {}.".format(repr(to_enum)))
        self._scaling_datafile = scaling_datafile
        self._scaling_datatable = None

    @property
    def default_scaling(self):
        return self._scaling_datafile is None

    @property
    def scaling_datatable(self):
        assert not self.default_scaling
        if self._scaling_datatable is None:
            self._scaling_datatable = Datatable(self._scaling_datafile)
        return self._scaling_datatable

    def get_scalings(self,to_ids):
        """
        Return an array of scalings for to_ids.
        """
        if self.default_scaling:
            return np.array([1.0 for to_id in to_ids]) 

        if isinstance(self.to_enum,SectorEnumeration):
            temp = self.scaling_datatable[to_ids,:,:,:]
            temp = temp.groupby(level='sector').sum()
        elif isinstance(self.to_enum,GeographyEnumeration):
            temp = self.scaling_datatable[:,to_ids,:,:]
            temp = temp.groupby(level='geography').sum()
        elif isinstance(self.to_enum,EndUseEnumerationBase):
            temp = self.scaling_datatable[:,:,to_ids,:]
            temp = temp.groupby(level='enduse').sum()
        else:
            assert isinstance(self.to_enum,TimeEnumeration)
            temp = self.scaling_datatable[:,:,:,to_ids]
            temp = temp.groupby(level='time').sum()

        # fraction of from_id that should go to each to_id
        temp = temp / temp.sum()
        result = np.array([temp[to_id] for to_id in to_ids])
        return result

    @classmethod
    def create_from_csv(cls,from_enum,to_enum,filepath,scaling_datafile=None):
        mapdata = pd.read_csv(filepath,dtype=str)
        return cls(from_enum,to_enum,cls._make_dictmap(mapdata),scaling_datafile=scaling_datafile)

    @classmethod
    def _make_dictmap(cls,mapdata):
        result = defaultdict(lambda: [])
        for from_id, to_id in zip(mapdata.from_id,mapdata.to_id):
            result[from_id].append(to_id)
        return result


class ExplicitAggregation(ExplicitMap):
    def __init__(self,from_enum,to_enum,dictmap):
        super().__init__(from_enum,to_enum,dictmap)
        self._dictmap = defaultdict(lambda: None)
        for from_id, to_id in dictmap.items():
            if from_id not in self.from_enum.ids:
                raise DSGridError("Id {} is not in from_enum {}.".format(from_id,self.from_enum))
            if to_id not in self.to_enum.ids:
                raise DSGridError("Id {} is not in to_enum {}.".format(to_id,self.to_enum))
            self._dictmap[from_id] = to_id

    @classmethod
    def _make_dictmap(cls,mapdata):
        result = {}
        from_fuel_enduse = ('from_fuel_id' in mapdata.columns)
        to_fuel_enduse = ('to_fuel_id' in mapdata.columns)
        for row in mapdata.itertuples(index=False):
            from_key = (row.from_id, row.from_fuel_id) if from_fuel_enduse else row.from_id
            to_key = (row.to_id, row.to_fuel_id) if to_fuel_enduse else row.to_id
            result[from_key] = to_key
        return result


class Mappings(object):

    def __init__(self):
        self._mappings = defaultdict(lambda: None)

    def add_mapping(self,mapping):
        self._mappings[(mapping.from_enum.name,mapping.to_enum.name)] = mapping

    def get_mapping(self,datafile,to_enum):
        
        from_enum = None
        if isinstance(to_enum,SectorEnumeration):
            from_enum = datafile.sector_enum
        elif isinstance(to_enum,GeographyEnumeration):
            from_enum = datafile.geo_enum
        elif isinstance(to_enum,EndUseEnumerationBase):
            from_enum = datafile.enduse_enum
        elif isinstance(to_enum,TimeEnumeration):
            from_enum = datafile.time_enum
        else:
            raise DSGridError("to_enum {} is not a recognized enumeration type.".format(repr(to_enum)))

        key = (from_enum.name,to_enum.name)
        if key in self._mappings:
            return self._mappings[key]

        # No immediate match
        # Is the requested mapping a tautology?
        if from_enum == to_enum:
            return TautologyMapping(to_enum)
        if from_enum.is_subset(to_enum):
            return TautologyMapping(to_enum)
        # Are elements in from_enum a subset of a stored mapping.from_enum?
        candidates = [mapping for key, mapping in self._mappings.items() if key[1] == to_enum.name]
        for candidate in candidates:
            okay = True
            for from_id in from_enum.ids:
                if from_id not in candidate.from_enum.ids:
                    okay = False
                    break
            if okay:
                return candidate
        return None

mappings = Mappings()
mappings.add_mapping(ExplicitAggregation.create_from_csv(counties,states,os.path.join(enumdata_folder,'counties_to_states.csv')))
conus_states = pd.read_csv(os.path.join(enumdata_folder,'conus_to_states.csv'),dtype=str)['to_id'].tolist()
mappings.add_mapping(FullAggregationMap(states,conus,exclude_list=[state_id for state_id in states.ids if state_id not in conus_states]))
mappings.add_mapping(FullAggregationMap(census_regions,conus))
mappings.add_mapping(FullAggregationMap(hourly2012,annual))
mappings.add_mapping(FullAggregationMap(sectors,allsectors))
mappings.add_mapping(FullAggregationMap(sectors_subsectors,allsectors))
mappings.add_mapping(FullAggregationMap(enduses,allenduses))
mappings.add_mapping(ExplicitAggregation.create_from_csv(enduses,fuel_types,os.path.join(enumdata_folder,'enduses_to_fuel_types.csv')))
mappings.add_mapping(ExplicitAggregation.create_from_csv(states,loss_state_groups,os.path.join(enumdata_folder,'states_to_loss_state_groups.csv')))
# Sneaky trick here--dropping non-CONUS states
mappings.add_mapping(ExplicitAggregation.create_from_csv(states,census_divisions,os.path.join(enumdata_folder,'states_to_census_divisions_conus_only.csv')))
mappings.add_mapping(ExplicitAggregation.create_from_csv(census_divisions,census_regions,os.path.join(enumdata_folder,'census_divisions_to_census_regions.csv')))
