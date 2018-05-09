from collections import defaultdict, OrderedDict
from distutils.version import StrictVersion
import logging
import os
from shutil import copyfile

try:
    from collections.abc import Mapping
except ImportError:
    # Python 2
    from collections import Mapping


import h5py

from dsgrid import __version__ as VERSION
from dsgrid import DSGridNotImplemented, DSGridValueError
from dsgrid.dataformat import ENCODING
from dsgrid.dataformat.enumeration import (
    SectorEnumeration, GeographyEnumeration,
    EndUseEnumerationBase, TimeEnumeration)
from dsgrid.dataformat.sectordataset import SectorDataset

logger = logging.getLogger(__name__)


class Datafile(Mapping):

    def __init__(self,h5path,sector_enum,geography_enum,enduse_enum,time_enum,
                 loading=False,version=VERSION):
        """
        Create a new Datafile object. Use Datafile.load to open existing files.

        Parameters
        ----------
        h5path : str
            file to create. typically has a .dsg file extension.
        sector_enum : dsgrid.dataformat.enumeration.SectorEnumeration
            enumeration of sectors to be stored in this Datafile
        geography_enum : dsgrid.dataformat.enumeration.GeographyEnumeration
            enumeration of geographies for this Datafile. typically these are 
            geographical units at the same level of resolution. the Datafile 
            does not have to specify values for every geography.
        enduse_enum : dsgrid.dataformat.enumeration.EndUseEnumerationBase
            enumeration of end-uses. there are mutiple EndUseEnumerationBase 
            class types. typically one would use SingeFuelEndUseEnumeration or 
            MultiFuelEndUseEnumeration.
        time_enum : dsgrid.dataformat.enumeration.TimeEnumeration
            enumeration specifying the time resolution of this Datafile
        loading : bool
            NOT FOR GENERAL USE -- Use Datafile.load to open existing files.
        version : str
            NOT FOR GENERAL USE -- New file are marked with the current VERSION.
            The load and update methods are used to manage version indicators 
            for backward compatibility.
        """
        self.h5path = h5path
        self.sector_enum = sector_enum
        self.geo_enum = geography_enum
        self.enduse_enum = enduse_enum
        self.time_enum = time_enum
        self.version = version
        self.sectordata = OrderedDict()
        if not loading:
            assert StrictVersion(version) == VERSION, "New Datafiles must be created at the current version"
            with h5py.File(self.h5path,mode="w-") as f:
                f.attrs["dsgrid"] = version
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

    def __iter__(self):
        for k in self.sectordata:
            yield k

    def __len__(self):
        return len(self.sectordata)

    def contains(self, an_enum):
        if isinstance(an_enum,SectorEnumeration):
            return an_enum == self.sector_enum
        elif isinstance(an_enum,GeographyEnumeration):
            return an_enum == self.geo_enum
        elif isinstance(an_enum,EndUseEnumerationBase):
            return an_enum == self.enduse_enum
        assert isinstance(an_enum,TimeEnumeration), "Unknown enum type {}".format(type(an_enum))
        return an_enum == self.time_enum

    @classmethod
    def load(cls,filepath,upgrade=True,overwrite=False,new_filepath=None,**kwargs):
        # Version Handling
        with h5py.File(filepath, "r") as f:
            version = f.attrs.get("dsgrid", "0.1.0")
            if isinstance(version,bytes):
                version = version.decode(ENCODING)

        if StrictVersion(version) > StrictVersion(VERSION):
            raise DSGridValueError("File at {} is of version {}. ".format(filepath,version) + 
                "It cannot be opened by this older version {} codebase.".format(VERSION))

        if StrictVersion(version) < StrictVersion(VERSION):
            from dsgrid.dataformat.upgrade import OLD_VERSIONS
            if version in OLD_VERSIONS:
                upgrade_class = OLD_VERSIONS[version]
                result = upgrade_class.load_datafile(filepath)
                if upgrade:
                    return result.upgrade(OLD_VERSIONS,overwrite=overwrite,new_filepath=new_filepath)
                elif not '__saving' in kwargs:
                    logger.warn("Not upgrading Datafile from version " + 
                        "{} (current version is {}).".format(result.version,VERSION) + 
                        " This package may not run properly on the loaded data.")
                return result

        # Current Version
        with h5py.File(filepath, "r") as f:
            enum_group = f["enumerations"]
            result = cls(filepath,
                         SectorEnumeration.load(enum_group),
                         GeographyEnumeration.load(enum_group),
                         EndUseEnumerationBase.load(enum_group),
                         TimeEnumeration.load(enum_group),
                         loading=True,
                         version=version)

            for sector_id, sector_dataset in SectorDataset.loadall(result,f):
                result.sectordata[sector_id] = sector_dataset

            return result

    def upgrade(self,OLD_VERSIONS,overwrite=False,new_filepath=None):
        # determine where to put upgraded Datafile
        filepath = self.h5path
        fp = filepath if overwrite else new_filepath
        if fp is None:
            # make up a filename
            filedir = os.path.dirname(filepath)
            filename = os.path.splitext(os.path.basename(filepath))[0]
            version_suffix = '_' + VERSION.replace('.','_')
            fp = os.path.join(filedir,filename + version_suffix + '.dsg')
            while os.path.exists(fp):
                fp = os.path.join(filedir,os.path.splitext(os.path.basename(fp))[0] + version_suffix + '.dsg')
            logger.info("Saving upgraded Datafile to {}".format(fp))

        # if we are not overwriting, start by making a new copy of the current 
        # Datafile
        result = self
        if fp != filepath:
            result = self.save(fp)

        # now loop through the upgrade classes
        with h5py.File(fp,mode="a") as f:
            for old_version, upgrade_class in OLD_VERSIONS.items():
                if result.version == upgrade_class.from_version:
                    result = upgrade_class.upgrade(result,f)
                    assert result.version == upgrade_class.to_version

        return result

    def save(self,filepath):
        """
        Save self to filepath and return newly created Datafile
        """
        copyfile(self.h5path,filepath)
        return self.__class__.load(filepath,upgrade=False,__saving=True)

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
                    os.remove(filepath)
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
                        os.remove(filepath)
                        raise DSGridNotImplemented("Aggregating subsectors at the Datafile level has not yet been implemented.")
                        # new_sector_dataset.add_inplace(dataset)
        else:
            for sector_id, sectordataset in self.sectordata.items():
                assert sectordataset is not None, "sector_id {} in file {} contains no data".format(sector_id,self.h5path)
                logger.info("Mapping data for {} in {}".format(sector_id,self.h5path))
                result.sectordata[sector_id] = sectordataset.map_dimension(result,mapping)
        return result

    def scale_data(self,filepath,factor=0.001):
        """
        Scale all the data in self by factor, creating a new HDF5 file and
        corresponding Datafile.

        Parameters
        ----------
        filepath | str
            Location for the new HDF5 file to be created
        factor  | float
            Factor by which all the data in the file is to be multiplied. The 
            default value of 0.001 corresponds to converting the bottom-up data 
            from kWh to MWh
        """
        result = self.__class__(filepath,self.sector_enum,self.geo_enum,
                                self.enduse_enum,self.time_enum)
        for sector_id, sectordataset in self.sectordata.items():
            assert sectordataset is not None, "sector_id {} in file {} contains no data".format(sector_id,self.h5path)
            logger.info("Scaling data for {} in {}".format(sector_id,self.h5path))
            result.sectordata[sector_id] = sectordataset.scale_data(result,factor=factor)
        return result

    def convert_units(self,filepath,from_units=[],to_units=[]):
        """
        Convert this Datafile to to_units.
        """
        raise DSGridNotImplemented("Placeholder method to implement soon")
