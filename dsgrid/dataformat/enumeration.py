from os import path
import numpy as np
import pandas as pd

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

mecs_subsectors = SectorEnumeration.read_csv(
    enumdata_folder + "mecs_subsectors.csv", "mecs_subsectors")

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

gaps_enduses = EndUseEnumeration.read_csv(
    enumdata_folder + "gaps_enduses.csv", "gaps_enduses")

fuel_types = EndUseEnumeration.read_csv(
    enumdata_folder + "fuel_types.csv", "fuel_types")

allenduses = EndUseEnumeration("all_enduses", ["All"], ["All End-uses"])

# Time
hourly2012 = TimeEnumeration.read_csv(
    enumdata_folder + "hourly2012.csv", "standard_2012_hourly")

annual = TimeEnumeration("annual", ["Annual"], ["Annual"])
