from collections import defaultdict
from os import path
import numpy as np
import pandas as pd

from dsgrid import DSGridError

ENCODING = "utf-8"

class Enumeration(object):

    max_id_len = 64
    max_name_len = 128
    enum_dtype = np.dtype([
        ("id", "S" + str(max_id_len)),
        ("name", "S" + str(max_name_len))
    ])

    dimension = None

    @classmethod
    def checkvalues(cls, ids, names):

        n_ids = len(ids)
        n_names = len(names)

        if n_ids != n_names:
            raise ValueError("Number of ids (" + str(n_ids) +
                             ") must match number of names (" + str(n_names) + ")")

        if n_ids > 65535:
            raise ValueError("Enumeration cannot contain more than 65535 values: " +
                             str(n_ids) + " provided")
        # 0 to 2^16-2 for indices (2^16-1 slots)
        # 2^16-1 for zero-sentinel

        if len(set(ids)) != n_ids:
            raise ValueError("Enumeration ids must be unique")

        if max(len(value) for value in ids) > cls.max_id_len:
            raise ValueError("Enumeration ids cannot exceed " +
                             str(cls.max_id_len) + " characters")

        if max(len(value) for value in names) > cls.max_name_len:
            raise ValueError("Enumeration names cannot exceed " +
                             str(cls.max_name_len) + " characters")


    def __init__(self, name, ids, names):

        Enumeration.checkvalues(ids, names)

        self.name = name
        self.ids = ids
        self.names = names

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            self.__dict__ == other.__dict__
        )

    def __len__(self):
        return len(self.ids)

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def __str__(self):
        return self.__repr__()


    def persist(self, h5group):

        dset = h5group.create_dataset(
            self.dimension,
            dtype=self.enum_dtype,
            shape=(len(self),))

        dset.attrs["name"] = self.name

        dset["id"] = np.array(self.ids)
        dset["name"] = np.array(self.names)

        return dset

    @classmethod
    def load(cls, h5group):
        h5dset = h5group[cls.dimension]
        return cls(
            h5dset.attrs["name"],
            [vid.decode(ENCODING) for vid in h5dset["id"]],
            [vname.decode(ENCODING) for vname in h5dset["name"]]
        )

    @classmethod
    def read_csv(cls, filepath, name):
        enum = pd.read_csv(filepath , dtype=str)
        return cls(name, list(enum.id), list(enum.name))


class SectorEnumeration(Enumeration):
    dimension = "sector"

class GeographyEnumeration(Enumeration):
    dimension = "geography"

class EndUseEnumeration(Enumeration):
    dimension = "enduse"

class TimeEnumeration(Enumeration):
    dimension = "time"

# Define standard enumerations

enumdata_folder = path.join(path.dirname(__file__), "enumeration_data/")

## Sectors
sectors_subsectors = SectorEnumeration.read_csv(
    enumdata_folder + "sectors_subsectors.csv", "standard_sector_subsectors")

sectors = SectorEnumeration.read_csv(
    enumdata_folder + "sectors.csv", "standard_sectors")

allsectors = SectorEnumeration("all_sectors", ["All"], ["All Sectors"])

## Geographies
counties = GeographyEnumeration.read_csv(
    enumdata_folder + "counties.csv", "counties")

states = GeographyEnumeration.read_csv(
    enumdata_folder + "states.csv", "states")

census_divisions = GeographyEnumeration.read_csv(
    enumdata_folder + "census_divisions.csv", "census_divisions")

res_state_groups = GeographyEnumeration.read_csv(
    enumdata_folder + "res_state_groups.csv", "state_groups")

census_regions = GeographyEnumeration.read_csv(
    enumdata_folder + "census_regions.csv", "census_regions")

conus = GeographyEnumeration("conus", ["conus"], ["Continental United States"])

## End Uses
enduses = EndUseEnumeration.read_csv(
    enumdata_folder + "enduses.csv", "standard_enduses")

allenduses = EndUseEnumeration("all_enduses", ["All"], ["All End-uses"])

# Time
hourly2012 = TimeEnumeration.read_csv(
    enumdata_folder + "hourly2012.csv", "standard_2012_hourly")

annual = TimeEnumeration("annual", ["Annual"], ["Annual"])


class AggregationMap(object):
    def __init__(self,from_enum,to_enum):
        self.from_enum = from_enum
        self.to_enum = to_enum

    def map(self,from_id):
        """
        Returns the appropriate to_id.
        """
        return None


class TautologyMapping(AggregationMap):
    def __init__(self,from_to_enum):
        super().__init__(from_to_enum,from_to_enum)

    def map(self,from_id):
        return from_id


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
        elif isinstance(to_enum,EndUseEnumeration):
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


class FilterOnlyMap(AggregationMap):

    def __init__(self,from_enum,to_enum,exclude_list=[]):
        super().__init__(from_enum,to_enum)
        if len(to_enum.ids) > 1:
            raise DSGridError("FilterOnlyMaps are aggregates that may exclude " + 
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


mappings = Mappings()
mappings.add_mapping(FilterOnlyMap(states,conus,exclude_list=['AK','HI']))
mappings.add_mapping(FilterOnlyMap(hourly2012,annual))
mappings.add_mapping(FilterOnlyMap(sectors,allsectors))
mappings.add_mapping(FilterOnlyMap(sectors_subsectors,allsectors))
mappings.add_mapping(FilterOnlyMap(enduses,allenduses))
