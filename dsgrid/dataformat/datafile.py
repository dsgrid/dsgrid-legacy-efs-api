from collections import defaultdict
import logging
from os.path import exists
from shutil import copyfile
from warnings import warn

import h5py


from dsgrid import __version__ as VERSION
from dsgrid import DSGridNotImplemented
from dsgrid.dataformat.enumeration import (
    SectorEnumeration, GeographyEnumeration,
    EndUseEnumerationBase, TimeEnumeration)
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
                f.attrs["dsgrid"] = VERSION
                self.version = VERSION
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

    def contains(self, an_enum):
        if isinstance(an_enum,SectorEnumeration):
            return an_enum == self.sector_enum
        elif isinstance(an_enum,GeographyEnumeration):
            return an_enum == self.geo_enum
        elif isinstance(an_enum,EndUseEnumerationBase):
            return an_enum == self.enduse_enum
        assert isinstance(an_enum,TimeEnumeration)
        return an_enum == self.time_enum

    @classmethod
    def load(cls,filepath):
        with h5py.File(filepath, "r") as f:
            enum_group = f["enumerations"]
            result = cls(filepath,
                         SectorEnumeration.load(enum_group),
                         GeographyEnumeration.load(enum_group),
                         EndUseEnumerationBase.load(enum_group),
                         TimeEnumeration.load(enum_group),
                         loading=True)
            result.version = f.attrs.get("dsgrid", "v0.1.0")
            for sector_id, sector_dataset in SectorDataset.loadall(result,f).items():
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

        sector = SectorDataset.new(self,sector_id,enduses,times)
        self.sectordata[sector_id] = sector

        return sector

    def map_dimension(self,filepath,mapping):
        result = self.__class__(filepath,
            mapping.to_enum if isinstance(mapping.to_enum,SectorEnumeration) else self.sector_enum,
            mapping.to_enum if isinstance(mapping.to_enum,GeographyEnumeration) else self.geo_enum,
            mapping.to_enum if isinstance(mapping.to_enum,EndUseEnumerationBase) else self.enduse_enum,
            mapping.to_enum if isinstance(mapping.to_enum,TimeEnumeration) else self.time_enum)
        data = defaultdict(lambda: [])
        if isinstance(mapping.to_enum,SectorEnumeration):
            for sector_id, sectordataset in self.sectordata.items():
                new_sector_id = mapping.map(sector_id)
                if isinstance(new_sector_id,list):
                    raise DSGridNotImplemented("Disaggregating sectors at the Datafile level has not yet been implemented.")
                data[new_sector_id].append(sectordataset)
            for new_sector_id, datasets in data.items():
                if new_sector_id == None:
                    # This data is not to be kept
                    continue
                new_sector_dataset = None
                for i, dataset in enumerate(datasets):
                    if i == 0:
                        # for first, create and add_data
                        new_sector_dataset = result.add_sector(new_sector_id,enduses=dataset.enduses,times=dataset.times)
                        for i in range(dataset.n_geos):
                            # pull data
                            df, geo_ids, scalings = dataset.get_data(i)
                            # push data
                            new_sector_dataset.add_data(df,geo_ids,scalings=scalings,full_validation=False)
                    else:
                        # add to what you already have
                        raise DSGridNotImplemented("Aggregating subsectors at the Datafile level has not yet been implemented.")
                        # new_sector_dataset.add_inplace(dataset)
        else:
            for sector_id, sectordataset in self.sectordata.items():
                assert sectordataset is not None, "sector_id {} in file {} contains no data".format(sector_id,self.h5path)
                print("Mapping data for {} in {}".format(sector_id,self.h5path))
                result.sectordata[sector_id] = sectordataset.map_dimension(result,mapping)
        return result

    def scale_data(self,filepath,factor=0.001):
        """
        Scale all the data in self by factor, creating a new HDF5 file and
        corresponding Datafile.

        Arguments:
            - filepath (str) - Location for the new HDF5 file to be created
            - factor (float) - Factor by which all the data in the file is to be
                  multiplied. The default value of 0.001 corresponds to converting
                  the bottom-up data from kWh to MWh.
        """
        result = self.__class__(filepath,self.sector_enum,self.geo_enum,
                                self.enduse_enum,self.time_enum)
        for sector_id, sectordataset in self.sectordata.items():
            assert sectordataset is not None, "sector_id {} in file {} contains no data".format(sector_id,self.h5path)
            print("Scaling data for {} in {}".format(sector_id,self.h5path))
            result.sectordata[sector_id] = sectordataset.scale_data(result,factor=factor)
        return result
