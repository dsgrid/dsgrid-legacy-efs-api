import h5py
from os.path import exists
from Enumerations import enums

class DataFile(object):

    def __init__(self, h5path,
                 sector_enum=None, geography_enum=None,
                 time_enum=None, enduse_enum=None):

        self.h5path = h5path

        if exists(h5path):
            self._load()

        elif sector_enum and geography_enum and time_enum and enduse_enum:
            self._new(sector_enum, geography_enum, time_enum, enduse_enum)

        else:
            raise FileNotFoundError("No file exists at " + h5path +
                                    " - to create a new datafile, provide " +
                                    "sector/subsector, geography, " +
                                    "time, and enduse Enumerations.")


    def _new(self, sector_enum, geo_enum, time_enum, enduse_enum):

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
        self.sectors = {}

    def _load(self):

        with h5py.File(h5path, "r") as f:

            self.sectors = {dset_id: SectorDataset(dset_id,
                                   dset.attrs.enduse_idxs,
                                   dset.attrs.time_idxs)
                                   for dset_id, dset in f["data"]
                                   where isinstance(dset, h5py.Dataset)}

            #TODO: Move these out to static Enumeration methods
            enum_group = f["enumerations"]

            enum_dset = enum_group["sector"]
            self.sector_enum = GeographyEnumeration(
                    enum_dset.attrs["name"],
                    list(enum_dset["id"]),
                    list(enum_dset["name"])
                )

            enum_dset = enum_group["geography"]
            self.geo_enum = GeographyEnumeration(
                    enum_dset.attrs["name"],
                    list(enum_dset["id"]),
                    list(enum_dset["name"])
                )

            enum_dset = enum_group["enduse"]
            self.enduse_enum = EndUseEnumeration(
                    enum_dset.attrs["name"],
                    list(enum_dset["id"]),
                    list(enum_dset["name"])
                )

            enum_dset = enum_group["time"]
            self.time_enum = TimeEnumeration(
                    enum_dset.attrs["name"],
                    list(enum_dset["id"]),
                    list(enum_dset["name"])
                )


        def add_sector(self, sector_id, enduse_coverage, time_coverage):

            if sector_id not in self.sector_enum.ids:
                raise ValueError("Sector ID " + sector_id + " is not in " +
                                 "the Datafile's SectorEnumeration")

            if set(enduse_coverage) not subset of self.enduse_enum.values:
                raise ValueError("Supplied enduses are not a subset of the " +
                                 "Datafile's EndUseEnumeration")

            if set(time_coverage) not subset of self.time_enum.values:
                raise ValueError("Supplied times are not a subset of the " +
                                 "Datafile's TimeEnumeration")

            sector = SectorDataset(sector_id,
                                   self.enduse_enum, enduse_coverage,
                                   self.time_enum, time_coverage)

            with h5py.File(self.h5path, "w-") as f:
                sector.h5init(f["data"])

            self.sectors[sector_id] = sector

            return sector
