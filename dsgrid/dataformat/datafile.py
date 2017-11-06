import h5py
from os.path import exists
from warnings import warn
from dataformat.enumeration import (
    SectorEnumeration, GeographyEnumeration,
    EndUseEnumeration, TimeEnumeration)
from dataformat.sectordataset import SectorDataset

# Python2 doesn't have a FileNotFoundError
try:
    FileNotFoundError
except NameError:
    FileNotFoundError = IOError

class Datafile(object):

    def __init__(self, h5path,
                 sector_enum=None, geography_enum=None,
                 enduse_enum=None, time_enum=None):

        self.h5path = h5path

        if exists(h5path):

            if sector_enum or geography_enum or time_enum or enduse_enum:
                warn("File already exists at " + h5path +
                        " and will be loaded: provided Enumerations will be ignored.")

            self._load()

        elif sector_enum and geography_enum and time_enum and enduse_enum:
            self._new(sector_enum, geography_enum, enduse_enum, time_enum)

        else:
            raise FileNotFoundError("No file exists at " + h5path +
                                    " - to create a new datafile, provide " +
                                    "sector/subsector, geography, " +
                                    "time, and enduse Enumerations.")


    def __eq__(self, other):
        return (
            isinstance(other, self.__class__) and
            self.__dict__ == other.__dict__
        )

    def __repr__(self):
        return "%s(%r)" % (self.__class__, self.__dict__)

    def __str__(self):
        return self.__repr__()

    def __getitem__(self, sector_id):
        return self.sectordata[sector_id]

    def _new(self, sector_enum, geo_enum, enduse_enum, time_enum):

        with h5py.File(self.h5path, "w-") as f:

            enum_group = f.create_group("enumerations")
            data_group = f.create_group("data")

            sector_enum.persist(enum_group)
            geo_enum.persist(enum_group)
            time_enum.persist(enum_group)
            enduse_enum.persist(enum_group)

        self.sector_enum = sector_enum
        self.geo_enum = geo_enum
        self.enduse_enum = enduse_enum
        self.time_enum = time_enum
        self.sectordata = {}


    def _load(self):

        with h5py.File(self.h5path, "r") as f:

            enum_group = f["enumerations"]
            self.sector_enum = SectorEnumeration.load(enum_group)
            self.geo_enum = GeographyEnumeration.load(enum_group)
            self.enduse_enum = EndUseEnumeration.load(enum_group)
            self.time_enum = TimeEnumeration.load(enum_group)

            self.sectordata = SectorDataset.loadall(self, f["data"])


    def add_sector(self, sector_id, enduses=None, times=None):

        sector = SectorDataset(sector_id, self,
                               enduses, times, persist=True)
        self.sectordata[sector_id] = sector

        return sector
