import os

import numpy as np
import pandas as pd

from dsgrid import DSGridValueError
from dsgrid.dataformat import ENCODING


class Enumeration(object):

    max_id_len = 64
    max_name_len = 128
    enum_dtype = np.dtype([
        ("id", "S" + str(max_id_len)),
        ("name", "S" + str(max_name_len))
    ])

    dimension = None

    def __init__(self, name, ids, names):
        self.name = name
        self.ids = ids
        self.names = names

        self.checkvalues()
        return

    def checkvalues(self):

        ids = list(self.ids); names = list(self.names)
        n_ids = len(ids); n_names = len(names)

        if n_ids != n_names:
            raise DSGridValueError("Number of ids (" + str(n_ids) +
                ") must match number of names (" + str(n_names) + ")")

        if len(set(ids)) != n_ids:
            raise DSGridValueError("Enumeration ids must be unique")

        if max(len(value) for value in ids) > self.max_id_len:
            raise DSGridValueError("Enumeration ids cannot exceed " +
                "{} characters".format(self.max_id_len))

        if max(len(value) for value in names) > self.max_name_len:
            raise DSGridValueError("Enumeration names cannot exceed " +
                             "{} characters".format(self.max_name_len))

    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            self.__dict__ == other.__dict__
        )

    def __len__(self):
        return len(list(self.ids))

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def __str__(self):
        return self.__repr__()

    def get_name(self,id):
        ind = list(self.ids).index(id)
        return self.names[ind]

    def is_subset(self,other_enum):
        """
        Returns true if this Enumeration is a subset of other_enum.
        """
        if not isinstance(other_enum,self.__class__):
            return False
        for my_id in self.ids:
            if not (my_id in other_enum.ids):
                return False
        return True

    def persist(self, h5group):

        dset = h5group.create_dataset(
            self.dimension,
            dtype=self.enum_dtype,
            shape=(len(self),))

        dset.attrs["name"] = self.name

        dset["id"] = np.array(self.ids)
        dset["name"] = np.array([name.encode(ENCODING) for name in self.names])

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


# Define standard dimensions

class SectorEnumeration(Enumeration):
    dimension = "sector"

class GeographyEnumeration(Enumeration):
    dimension = "geography"

class EndUseEnumerationBase(Enumeration):
    dimension = "enduse"

    def fuel(self,id): pass

    def units(self,id): pass

    @classmethod
    def load(cls, h5group):
        # Create correct type of EndUseEnumerationBase depending on auxillary data
        if FuelEnumeration.dimension in h5group:
            return MultiFuelEndUseEnumeration.load(h5group)

        h5dset = h5group[cls.dimension]
        name = h5dset.attrs["name"]
        ids = [vid.decode(ENCODING) for vid in h5dset["id"]]
        names = [vname.decode(ENCODING) for vname in h5dset["name"]]

        if 'fuel' in h5dset.attrs:
            return SingleFuelEndUseEnumeration(name, ids, names,
                fuel=h5dset.attrs['fuel'],
                units=h5dset.attrs['units'])
        else:
            return EndUseEnumeration(name,ids,names)


class TimeEnumeration(Enumeration):
    dimension = "time"


# Define data units -- these are ultimately associated with end-uses

class EndUseEnumeration(EndUseEnumerationBase):
    """
    Provided for backward compatibility with dsgrid v0.1.0 datasets.
    """
    def fuel(self,id):
        logger.warn("Deprecated: Fuel type has not been explicitly specified. Returning default value.")
        return 'Electricity'

    def units(self,id):
        logger.warn("Deprecated: Units have not been explicitly specified. Returning default value.")
        return 'MWh'


class SingleFuelEndUseEnumeration(EndUseEnumerationBase):
    """
    If the end-use enumeration only applies to a single fuel type, and all the
    data is in the same units, just give the fuel and units.
    """

    def __init__(self, name, ids, names, fuel='Electricity', units='MWh'):
        super(SingleFuelEndUseEnumeration, self).__init__(name,ids,names)
        self._fuel = fuel
        self._units = units

    def fuel(self,id):
        return self._fuel

    def units(self,id):
        return self._units

    def persist(self, h5group):
        dset = super(SingleFuelEndUseEnumeration, self).persist(h5group)

        dset.attrs["fuel"] = self._fuel
        dset.attrs["units"] = self._units

        return dset

    @classmethod
    def read_csv(cls, filepath, name, fuel='Electricity', units='MWh'):
        enum = pd.read_csv(filepath , dtype=str)
        return cls(name, list(enum.id), list(enum.name), fuel=fuel, units=units)


class FuelEnumeration(Enumeration):
    dimension = "fuel"

    enum_dtype = np.dtype([
        ("id", "S" + str(Enumeration.max_id_len)),
        ("name", "S" + str(Enumeration.max_name_len)),
        ("units", "S" + str(Enumeration.max_id_len))
    ])

    def __init__(self, name, ids, names, units):
        self.units = units
        super(FuelEnumeration, self).__init__(name,ids,names)

    def checkvalues(self):
        super(FuelEnumeration, self).checkvalues()

        # make sure units is as long as ids
        ids = list(self.ids); units = list(self.units)
        n_ids = len(ids); n_units = len(units)

        if n_ids != n_units:
            raise DSGridValueError("Number of units (" + str(n_units) +
                ") must match number of ids (" + str(n_ids) + ")")

        if max(len(unit) for unit in units) > self.max_id_len:
            raise DSGridValueError("Enumeration units cannot exceed " +
                "{} characters".format(self.max_id_len))

    def persist(self, h5group):
        dset = super(FuelEnumeration, self).persist(h5group)
        dset["units"] = np.array(self.units)
        return dset

    @classmethod
    def load(cls, h5group):
        h5dset = h5group[cls.dimension]
        return cls(
            h5dset.attrs["name"],
            [vid.decode(ENCODING) for vid in h5dset["id"]],
            [vname.decode(ENCODING) for vname in h5dset["name"]],
            [vunits.decode(ENCODING) for vunits in h5dset["units"]]
        )

    @classmethod
    def read_csv(cls, filepath, name):
        enum = pd.read_csv(filepath , dtype=str)
        return cls(name, list(enum.id), list(enum.name), list(enum.units))


class MultiFuelEndUseEnumeration(EndUseEnumerationBase):

    enum_dtype = np.dtype([
        ("id", "S" + str(Enumeration.max_id_len)),
        ("name", "S" + str(Enumeration.max_name_len)),
        ("fuel_id", "S" + str(Enumeration.max_id_len))
    ])

    def __init__(self, name, ids, names, fuel_enum, fuel_ids):
        self.name = name
        self._ids = ids
        self._names = names
        self.fuel_enum = fuel_enum
        self._fuel_ids = fuel_ids

        self.checkvalues()
        return

    def checkvalues(self):
        ids = self._ids; fuel_ids = self._fuel_ids; fuel_enum = self.fuel_enum
        n_ids = len(ids); n_fuel_ids = len(fuel_ids)

        # make sure fuel_ids is as long as ids
        if n_fuel_ids != n_ids:
            raise DSGridValueError("Number of fuel ids (" + str(n_fuel_ids) +
                ") must match number of ids (" + str(n_ids) + ")")

        if not isinstance(fuel_enum,FuelEnumeration):
            raise DSGridValueError("The fuel_enum must be of type " +
                "{}, but is instead of type {}".format(FuelEnumeration.__class__,
                                                       type(fuel_enum)))

        # make sure fuel_ids are in fuel enum
        for fuel_id in set(fuel_ids):
            if fuel_id not in fuel_enum.ids:
                raise DSGridValueError("The fuel_ids must each be an id in the fuel_enum." +
                    "fuel_id: {}, fuel_enum.ids: {}".format(fuel_id,fuel_enum.ids))

        super(MultiFuelEndUseEnumeration, self).checkvalues()

        return

    @property
    def ids(self):
        return list(zip(self._ids,self._fuel_ids))

    @property
    def names(self):
        for i, _id in enumerate(self._ids):
            yield "{} ({})".format(self._names[i],self.fuel((_id,self._fuel_ids[i])))

    def fuel(self,id):
        assert isinstance(id,tuple) & (len(id) == 2), "The ids for MultiFuelEndUseEnumerations are (enduse_id, fuel_id)."
        return self.fuel_enum.names[self.fuel_enum.ids.index(id[1])]

    def units(self,id):
        assert isinstance(id,tuple) & len(id) == 2, "The ids for MultiFuelEndUseEnumerations are (enduse_id, fuel_id)."
        return self.fuel_enum.units[self.fuel_enum.ids.index(id[1])]

    def persist(self, h5group):
        dset = h5group.create_dataset(
            self.dimension,
            dtype=self.enum_dtype,
            shape=(len(self),))

        dset.attrs["name"] = self.name

        dset["id"] = np.array(self._ids)
        dset["name"] = np.array([name.encode(ENCODING) for name in self._names])
        dset["fuel_id"] = np.array(self._fuel_ids)

        fuel_dset = self.fuel_enum.persist(h5group)

        return dset

    @classmethod
    def load(cls, h5group):
        fuel_enum = FuelEnumeration.load(h5group)

        h5dset = h5group[cls.dimension]
        return cls(
            h5dset.attrs["name"],
            [vid.decode(ENCODING) for vid in h5dset["id"]],
            [vname.decode(ENCODING) for vname in h5dset["name"]],
            fuel_enum,
            [vfuel_id.decode(ENCODING) for vfuel_id in h5dset["fuel_id"]]
        )

    @classmethod
    def read_csv(cls, filepath, name, fuel_enum=None):
        """
        id, name, fuel_id + pass in file_enum

        or

        id, name, fuel_id, fuel_name, units

        or 

        id, name, fuel_id, units (and fuel_name will be guessed from fuel_id)
        """
        enum = pd.read_csv(filepath , dtype=str)
        if fuel_enum is None:
            fuel_enum_name = name + ' Fuels'
            if 'fuel_name' in enum.columns:
                # fuel enum fully defined in this file
                fuel_enum = enum[["fuel_id","fuel_name","units"]].drop_duplicates()
                fuel_enum = FuelEnumeration(
                    fuel_enum_name,
                    list(fuel_enum.fuel_id),
                    list(fuel_enum.fuel_name),
                    list(fuel_enum.units))
            else:
                # create fuel enum names from fuel enum ids
                fuel_enum = enum[["fuel_id","units"]].drop_duplicates()
                fuel_ids = list(fuel_enum.fuel_id)
                fuel_names = [fuel_id.replace("_"," ").title() for fuel_id in fuel_ids]
                fuel_enum = FuelEnumeration(
                    fuel_enum_name,
                    fuel_ids,
                    fuel_names,
                    list(fuel_enum.units))

        assert fuel_enum is not None
        return cls(name, list(enum.id), list(enum.name), fuel_enum, list(enum.fuel_id))


# Define standard enumerations

enumdata_folder = os.path.join(os.path.dirname(__file__), "enumeration_data/")

## Sectors
sectors_subsectors = SectorEnumeration.read_csv(
    enumdata_folder + "sectors_subsectors.csv", "standard_sector_subsectors")

mecs_subsectors = SectorEnumeration.read_csv(
    enumdata_folder + "mecs_subsectors.csv", "mecs_subsectors")

sectors = SectorEnumeration.read_csv(
    enumdata_folder + "sectors.csv", "standard_sectors")

sectors_eia_extended = SectorEnumeration.read_csv(
    enumdata_folder + "sectors_eia_extended.csv", "sectors_eia_extended")

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

loss_state_groups = GeographyEnumeration.read_csv(
    enumdata_folder + "loss_state_groups.csv", "loss_state_groups")

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
