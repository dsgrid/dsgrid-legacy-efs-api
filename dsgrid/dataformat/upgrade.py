from collections import OrderedDict
from distutils.version import StrictVersion

import h5py
import numpy as np

from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.enumeration import (SectorEnumeration, 
    GeographyEnumeration, EndUseEnumerationBase, TimeEnumeration)
from dsgrid.dataformat.sectordataset import SectorDataset

class UpgradeDatafile(object):
    from_version = None
    to_version = None

    @classmethod
    def upgrade(cls,datafile,f):
        assert StrictVersion(datafile.version) == StrictVersion(cls.from_version)

        cls._transform(datafile,f)

        f.attrs["dsgrid"] = cls.to_version
        datafile.version = cls.to_version
        return datafile

    @classmethod
    def _transform(cls,datafile,f):
        pass

    @classmethod
    def load_datafile(cls,filepath):
        """
        Load enough to return a Datafile object. Object should not be 
        expected to be fully functional.
        """
        with h5py.File(filepath, "r") as f:
            enum_group = f["enumerations"]
            result = Datafile(filepath,
                              SectorEnumeration.load(enum_group),
                              GeographyEnumeration.load(enum_group),
                              EndUseEnumerationBase.load(enum_group),
                              TimeEnumeration.load(enum_group),
                              loading=True,
                              version=cls.from_version)

            for sector_id, sector_dataset in SectorDataset.loadall(result,f,_upgrade_class=cls):
                result.sectordata[sector_id] = sector_dataset

            return result

    def load_sectordataset(cls,datafile,f,sector_id):
        """
        Load enough to return a SectorDataset object. Object should not be 
        expected to be fully functional.
        """
        pass


class DSG_0_1_0(UpgradeDatafile):
    from_version = '0.1.0'
    to_version = '0.2.0'

    ZERO_IDX = 65535

    @classmethod
    def _transform(cls,datafile,f):
        pass

    @classmethod
    def load_sectordataset(cls,datafile,f,sector_id):
        dset = f["data/" + sector_id]

        enduses = list(datafile.enduse_enum.ids)
        times = np.array(datafile.time_enum.ids)
        enduses = [enduses[i] for i in dset.attrs["enduse_mappings"][:]]
        times = list(times[dset.attrs["time_mappings"][:]])

        result = SectorDataset(datafile,sector_id,enduses,times)

        geo_ptrs = [x for x in dset.attrs["geo_mappings"] if not (x == cls.ZERO_IDX)]
        result.n_geos = len(set(geo_ptrs))

        return result


OLD_VERSIONS = OrderedDict()
OLD_VERSIONS[DSG_0_1_0.from_version] = DSG_0_1_0
