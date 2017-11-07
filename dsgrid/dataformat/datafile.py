import h5py
import logging
from os.path import exists
from shutil import copyfile
from warnings import warn

from dsgrid import DSGridNotImplemented
from dsgrid.dataformat.enumeration import (
    SectorEnumeration, GeographyEnumeration,
    EndUseEnumeration, TimeEnumeration)
from dsgrid.dataformat.sectordataset import SectorDataset

logger = logging.getLogger(__name__)


class Datafile(object):

    def __init__(self,h5path,sector_enum,geography_enum,enduse_enum,time_enum,
                 loading=False):
        """
        Create a new Datafile object.
        """
        self.h5path = h5path
        self.sector_enum = sector_enum
        self.geo_enum = geography_enum
        self.enduse_enum = enduse_enum
        self.time_enum = time_enum
        self.sectordata = {}
        if not loading:
            with h5py.File(self.h5path,mode="w-") as f:
                enum_group = f.create_group("enumerations")
                data_group = f.create_group("data")

                self.sector_enum.persist(enum_group)
                self.geo_enum.persist(enum_group)
                self.time_enum.persist(enum_group)
                self.enduse_enum.persist(enum_group)
                logger.debug("Saved enums to {}".format(self.h5path))

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

    @classmethod
    def load(cls,filepath):
        with h5py.File(filepath, "r") as f:
            enum_group = f["enumerations"]
            result = cls(filepath,
                         SectorEnumeration.load(enum_group),
                         GeographyEnumeration.load(enum_group),
                         EndUseEnumeration.load(enum_group),
                         TimeEnumeration.load(enum_group),
                         loading=True)
            for sector_id, sector_dataset in SectorDataset.loadall(result,f["data"]).items():
                result.sectordata[sector_id] = sector_dataset
        return result

    def save(self,filepath):
        """
        Save self to filepath and return newly created Datafile
        """
        copyfile(self.h5path,filepath)
        return self.__class__.load(filepath)

    def add_sector(self,sector_id,enduses=None,times=None):
        """
        Adds a SectorDataset to this file and returns it.
        """

        sector = SectorDataset(sector_id,self,enduses=enduses,times=times)
        self.sectordata[sector_id] = sector

        return sector

    def aggregate(self,filepath,mapping): 
        if isinstance(mapping.to_enum,SectorEnumeration):
            raise DSGridNotImplemented("Aggregating subsectors at the Datafile level has not yet been implemented. Datatables can be used to aggregate subsectors.")
        result = self.__class__(filepath,
            mapping.to_enum if isinstance(mapping.to_enum,SectorEnumeration) else self.sector_enum,
            mapping.to_enum if isinstance(mapping.to_enum,GeographyEnumeration) else self.geo_enum,
            mapping.to_enum if isinstance(mapping.to_enum,EndUseEnumeration) else self.enduse_enum,
            mapping.to_enum if isinstance(mapping.to_enum,TimeEnumeration) else self.time_enum)
        for sector_id, sectordataset in self.sectordata.items():
            assert sectordataset is not None, "sector_id {} in file {} contains no data".format(sector_id,self.h5path)
            result.sectordata[sector_id] = sectordataset.aggregate(result,mapping)
        return result
