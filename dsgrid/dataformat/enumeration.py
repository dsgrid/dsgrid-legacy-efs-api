import datetime as dt
from enum import Enum
import os
import logging
import pytz
import re

import numpy as np
import pandas as pd

from dsgrid import DSGridRuntimeError, DSGridValueError
from dsgrid.dataformat import ENCODING, get_str

logger = logging.getLogger(__name__)


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
            get_str(h5dset.attrs["name"]),
            [get_str(vid) for vid in h5dset["id"]],
            [get_str(vname) for vname in h5dset["name"]]
        )

    @classmethod
    def read_csv(cls, filepath, name=None):
        enum = pd.read_csv(filepath, dtype=str)
        name = cls._name_from_filepath(filepath) if name is None else name
        return cls(name, list(enum.id), list(enum.name))

    def to_csv(self,filepath=None,overwrite=False):
        p = self._default_filepath() if filepath is None else filepath
        if not overwrite and os.path.exists(p):
            msg = "{} already exists".format(p)
            logger.error(msg)
            raise DSGridRuntimeError(msg)
        df = pd.DataFrame(list(zip(self.ids,self.names)),columns=['id','name'])
        df.to_csv(p,index=False)

    @classmethod
    def _name_from_filepath(cls,filepath):
        return os.path.splitext(os.path.basename(filepath))[0].replace("_"," ").title()

    def _default_filepath(self):
        return os.path.join(enumdata_folder,self.name.lower().replace(' ','_') + '.csv')

    
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
        name = get_str(h5dset.attrs["name"])
        ids = [get_str(vid) for vid in h5dset["id"]]
        names = [get_str(vname) for vname in h5dset["name"]]

        if 'fuel' in h5dset.attrs:
            return SingleFuelEndUseEnumeration(name, ids, names,
                fuel=h5dset.attrs['fuel'],
                units=h5dset.attrs['units'])
        else:
            return EndUseEnumeration(name,ids,names)

    @classmethod
    def read_csv(cls, filepath, name=None):
        """
        Infer and read into the correct derived class.
        """
        enum = pd.read_csv(filepath , dtype=str)
        if 'fuel' in enum.columns:
            return SingleFuelEndUseEnumeration.read_csv(filepath,name=name)
        if 'fuel_id' in enum.columns:
            return MultiFuelEndUseEnumeration.read_csv(filepath,name=name)
        return EndUseEnumeration.read_csv(filepath,name=name)


class TimeEnumeration(Enumeration):
    dimension = "time"

    TIMESTAMP_POSITION = Enum('TIMESTAMP_POSITION',
                              ['period_beginning',
                               'period_midpoint',
                               'period_ending'])

    TIMEZONE_DISPLAY_NAMES = {
        'Etc/GMT+5': 'EST',
        'Etc/GMT+6': 'CST',
        'Etc/GMT+7': 'MST',
        'Etc/GMT+8': 'PST' }

    TIMEZONE_LOOKUP = {val: key for key, val in TIMEZONE_DISPLAY_NAMES.items()}

    @classmethod
    def create(cls,enum_name,start,duration,resolution,
               extent_timezone=pytz.timezone('UTC'),
               store_timezone=None,
               timestamp_position=TIMESTAMP_POSITION['period_ending']):
        """
        Create a new time enumeration based on the specified temporal extents,
        resolution, and timezone.

        Parameters
        ----------
        enum_name : str
            name for this enumeration, ideally descriptive of the parameters
            used for creation
        start : datetime.datetime
            beginning of the time period to be represented by the timestamps
        duration : datetime.timedelta
            total length of time to be covered
        resolution : datetime.timedelta
            timestep for the enumeration
        extent_timezone : pytz.timezone
            timezone that should be used to interpret the extent parameters
        store_timezone : None or pytz.timezone
            timezone to write the ids and names in. If None, extent_timezone is
            used.
        timestamp_position : TimeEnumeration.TIMESTAMP_POSITION or convertable str
            whether timestamps are placed at the beginning, ending, or midpoint 
            of the time period being described

        Returns
        -------
        TimeEnumeration
        """
        num_steps = duration / resolution
        if not (num_steps == int(num_steps)):
            logger.warn("Duration {} is not divided cleanly into steps of size {}".format(duration,resolution))
        
        extent_timezone = cls._timezone_object(extent_timezone)
        store_timezone = cls._timezone_object(store_timezone,extent_timezone)

        end = start + duration
        ts_pos = timestamp_position if isinstance(timestamp_position,cls.TIMESTAMP_POSITION) else cls.TIMESTAMP_POSITION[timestamp_position]
        
        next_stamp = start
        if ts_pos == cls.TIMESTAMP_POSITION['period_ending']:
            next_stamp = start + resolution
        elif ts_pos == cls.TIMESTAMP_POSITION['period_midpoint']:
            next_stamp = start + (resolution / 2)

        ids = []
        while next_stamp <= end:
            ids.append(str(extent_timezone.localize(next_stamp).astimezone(store_timezone)))
            next_stamp = next_stamp + resolution

        return cls(enum_name,ids,ids)

    @classmethod
    def _timezone_object(cls,timezone,default=None):
        result = timezone
        if timezone is None:
            result = default
        if result in cls.TIMEZONE_LOOKUP:
            result = cls.TIMEZONE_LOOKUP[result]
        if isinstance(result,str):
            result = pytz.timezone(result)
        return result

    @property
    def store_timezone(self):
        """
        Examines the first id to determine what timezone this TimeEnumeration
        is stored in. Assumes the usage of datetime, pytz, and the "standard" 
        timezones, e.g., 
        
            - pytz.timezone('Etc/GMT+5') = EST
            - pytz.timezone('Etc/GMT+6') = CST
            - pytz.timezone('Etc/GMT+7') = MST
            - pytz.timezone('Etc/GMT+8') = PST
        """
        if not self.ids:
            raise DSGridValueError('No instances in this {}. Cannot determine a timezone.'.format(type(self)))
        m = re.match(r'[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}([+-][0-9]{2}:[0-9]{2})?',self.ids[0])
        if not m:
            raise DSGridValueError('Not able to interpret {} as a timestamp'.format(self.ids[0]))
        if m.group(1) is None:
            logger.warn('Explicit timezone not found in timestamp {}, assuming UTC'.format(self.ids[0]))
            return pytz.timezone('UTC')
        assert m.group(1)[3:] == ':00', m.group(1)
        tz_str = 'Etc/GMT'
        tz_str += '+' if m.group(1)[0] == '-' else '-'
        tz_str += str(int(m.group(1)[1:3]))
        return pytz.timezone(tz_str)

    @property
    def store_timezone_display_name(self):
        """
        Interprets self.ids[0] to report what timezone this enumeration is 
        stored in. Converts from pytz strings to what we typically use, namely
        EST, CST, MST, or PST.

        Returns
        -------
        str
            timezone this TimeEnumeration is stored in, per self.store_timezone 
            and self.TIMEZONE_DISPLAY_NAMES
        """
        result = str(self.store_timezone)
        if result in self.TIMEZONE_DISPLAY_NAMES:
            result = self.TIMEZONE_DISPLAY_NAMES[result]
        return result

    @property
    def resolution(self):
        """
        The resolution of this TimeEnumeration.

        Returns
        -------
        dt.timedelta or array of dt.timedelta
            Returns a single value if the intervals are all of the same length.
            Returns a vector of values if they are different.
        """
        ind = self.to_datetime_index()
        result = (ind[1:] - ind[:-1])
        unique_vals = result.unique()
        if len(unique_vals) == 1:
            return unique_vals.to_pytimedelta()[0]
        return result.to_pytimedelta()

    def get_extents(self,report_timezone=None,
                    timestamp_position=TIMESTAMP_POSITION['period_ending']):
        """
        Returns the inclusive temporal extents represented in this 
        TimeEnumeration. That interpretation requires knowledge of the 
        timestamp_postion--beginning, end, or midpoint of the period being 
        described.

        Parameters
        ----------
        report_timezone : pytz.timezone
            Timezone in which to report out the result

        Returns
        -------
        (datetime.datetime,datetime.datetime)
            Tuple of start and end times, inclusive of all time represented 
            based on the timestamp position, and in report_timezone. 
        """
        ind = self.to_datetime_index(return_timezone=report_timezone)
        res = self.resolution
        bres = res; eres = res
        if not isinstance(res,dt.timedelta):
            logger.warn("Temporal resolution is not uniform. Reported extents may be inaccurate.")
            bres = res[0]; eres = res[-1]
        start = ind[0].to_pydatetime(); end = ind[-1].to_pydatetime()

        ts_pos = timestamp_position if isinstance(timestamp_position,self.TIMESTAMP_POSITION) else self.TIMESTAMP_POSITION[timestamp_position]
        if ts_pos == self.TIMESTAMP_POSITION['period_beginning']:
            end = end + eres
        elif ts_pos == self.TIMESTAMP_POSITION['period_midpoint']:
            start = start - (bres / 2)
            end = end + (eres / 2)
        elif ts_pos == self.TIMESTAMP_POSITION['period_ending']:
            start = start - bres

        return (start, end)        

    def to_datetime_index(self,return_timezone=None):
        """
        Return a Pandas DatetimeIndex corresponding to this TimeEnumeration. 
        By default, localizes the timestamps to the timezone inferred based on 
        the text of the first enumeration id. If return_timezpone is None, this 
        is what is returned. If return_timezone is not None, the index is 
        converted to that timezone before being returned.

        Parameters
        ----------
        return_timezone : None or pytz.timezone
            timezone of the returned index. If None, this is inferred from 
            self.ids[0]
        
        Returns
        -------
        pandas.DatetimeIndex
            same length as self.ids, but strings are converted to 
            datetime.datetime objects and localized to a timezone.
        """
        df = pd.DataFrame([],index=self.ids)
        return_timezone = self._timezone_object(return_timezone,self.store_timezone)
        df.index = pd.to_datetime(df.index).tz_localize('UTC').tz_convert(return_timezone)
        return df.index


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

    @classmethod
    def read_csv(cls, filepath, name=None):
        enum = pd.read_csv(filepath, dtype=str)
        name = cls._name_from_filepath(filepath) if name is None else name
        return cls(name, list(enum.id), list(enum.name))


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
    def read_csv(cls, filepath, name=None):
        enum = pd.read_csv(filepath , dtype=str)
        assert 'fuel' in enum.columns, "Fuel must be specified."
        assert 'units' in enum.columns, "Units must be specified."
        assert len(enum['fuel'].unique()) == 1, "There must be exactly 1 fuel, but {} are listed".format(len(enum.fuel.unique()))
        assert len(enum['units'].unique()) == 1, "There must be exactly 1 units, but {} are listed".format(len(enum.units.unique()))
        fuel = enum['fuel'].unique()[0]
        units = enum['units'].unique()[0]
        name = cls._name_from_filepath(filepath) if name is None else name
        return cls(name, list(enum.id), list(enum.name), fuel=fuel, units=units)

    def to_csv(self, filepath=None, overwrite=False):
        p = self._default_filepath() if filepath is None else filepath
        if not overwrite and os.path.exists(p):
            msg = "{} already exists".format(p)
            logger.error(msg)
            raise DSGridRuntimeError(msg)
        data = [list(x) + [self._fuel, self._units] for x in zip(self.ids,self.names)]
        df = pd.DataFrame(data,columns=['id','name','fuel','units'])
        df.to_csv(p,index=False)


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

    def get_units(self,id):
        ind = list(self.ids).index(id)
        return self.units[ind]

    def persist(self, h5group):
        dset = super(FuelEnumeration, self).persist(h5group)
        dset["units"] = np.array(self.units)
        return dset

    @classmethod
    def load(cls, h5group):
        h5dset = h5group[cls.dimension]
        return cls(
            get_str(h5dset.attrs["name"]),
            [get_str(vid) for vid in h5dset["id"]],
            [get_str(vname) for vname in h5dset["name"]],
            [get_str(vunits) for vunits in h5dset["units"]]
        )

    @classmethod
    def read_csv(cls, filepath, name=None):
        enum = pd.read_csv(filepath , dtype=str)
        name = cls._name_from_filepath(filepath) if name is None else name
        return cls(name, list(enum.id), list(enum.name), list(enum.units))

    def to_csv(self, filepath=None, overwrite=False):
        p = self._default_filepath() if filepath is None else filepath
        if not overwrite and os.path.exists(p):
            msg = "{} already exists".format(p)
            logger.error(msg)
            raise DSGridRuntimeError(msg)
        df = pd.DataFrame(list(zip(self.ids,self.names,self.units)),
                          columns=['id','name','units'])
        df.to_csv(p,index=False)


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
        assert isinstance(id,tuple) & (len(id) == 2), "The ids for MultiFuelEndUseEnumerations are (enduse_id, fuel_id). Got {!r}".format(id)
        return self.fuel_enum.names[self.fuel_enum.ids.index(id[1])]

    def units(self,id):
        assert isinstance(id,tuple) & (len(id) == 2), "The ids for MultiFuelEndUseEnumerations are (enduse_id, fuel_id). Got {!r}".format(id)
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
            get_str(h5dset.attrs["name"]),
            [get_str(vid) for vid in h5dset["id"]],
            [get_str(vname) for vname in h5dset["name"]],
            fuel_enum,
            [get_str(vfuel_id) for vfuel_id in h5dset["fuel_id"]]
        )

    @classmethod
    def read_csv(cls, filepath, name=None, fuel_enum=None):
        """
        id, name, fuel_id + pass in file_enum

        or

        id, name, fuel_id, fuel_name, units

        or 

        id, name, fuel_id, units (and fuel_name will be guessed from fuel_id)
        """
        enum = pd.read_csv(filepath , dtype=str)
        name = cls._name_from_filepath(filepath) if name is None else name
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

    def to_csv(self, filepath=None, overwrite=False):
        p = self._default_filepath() if filepath is None else filepath
        if not overwrite and os.path.exists(p):
            msg = "{} already exists".format(p)
            logger.error(msg)
            raise DSGridRuntimeError(msg)
        simple_fuel_name = True
        for fuel_id in self.fuel_enum.ids:
            if not (fuel_id.replace("_"," ").title() == self.fuel_enum.get_name(fuel_id)):
                simple_fuel_name = False
                break
        data = list(zip(self._ids,self._names,self._fuel_ids))
        cols = ['id','name','fuel_id']
        if not simple_fuel_name:
            data = [list(x) + [self.fuel_enum.get_name(x[2])] for x in data]
            cols += ['fuel_name']
        data = [list(x) + [self.fuel_enum.get_units(x[2])] for x in data]
        cols += ['units']
        df = pd.DataFrame(data,columns=cols)
        df.to_csv(p,index=False)


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

conus_counties = GeographyEnumeration.read_csv(
    os.path.join(enumdata_folder,'conus_counties.csv'))

states = GeographyEnumeration.read_csv(
    enumdata_folder + "states.csv", "states")

conus_states = GeographyEnumeration.read_csv(
    os.path.join(enumdata_folder,'conus_states.csv'))

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

deprecated_allenduses = EndUseEnumeration("all_enduses", ["All"], ["All End-uses"])
allenduses = SingleFuelEndUseEnumeration("all_enduses", ["All"], ["All End-uses"])

loss_factor = SingleFuelEndUseEnumeration('Loss Factor',['loss_factor'],
    ['Loss Factor'],fuel='N/A',units='dimensionless')

# Time
hourly2012 = TimeEnumeration.read_csv(
    os.path.join(enumdata_folder,'hourly2012.csv'))

daily2012 = TimeEnumeration.read_csv(
    os.path.join(enumdata_folder,'daily2012.csv'))

weekdays = TimeEnumeration.read_csv(
    os.path.join(enumdata_folder,'weekdays.csv'))

daytypes = TimeEnumeration.read_csv(
    os.path.join(enumdata_folder,'day_types.csv'))

weekly2012 = TimeEnumeration.read_csv(
    os.path.join(enumdata_folder,'weekly2012.csv'))

seasons = TimeEnumeration.read_csv(
    os.path.join(enumdata_folder,'seasons.csv'))

annual = TimeEnumeration("annual", ["Annual"], ["Annual"])
