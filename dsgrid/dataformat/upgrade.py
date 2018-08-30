from collections import OrderedDict
from distutils.version import StrictVersion
import logging

import h5py
import numpy as np

from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.enumeration import (SectorEnumeration,
    GeographyEnumeration, EndUseEnumeration, EndUseEnumerationBase, 
    SingleFuelEndUseEnumeration, TimeEnumeration)
from dsgrid.dataformat.sectordataset import Datamap, SectorDataset, NULL_IDX

logger = logging.getLogger(__name__)

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

        Parameters
        ----------
        filepath : str
            path to Datafile

        Returns
        -------
        dsgrid.dataformat.datafile.Datafile
            (partially) loaded Datafile in old format
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

    @classmethod
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
        # from v0.1.0 to v0.2.0
        #   - f['data'][sector_id] is no longer a h5py.Dataset with attributes
        #   - f['data'][sector_id] is now a h5py.Group containing
        #       - f['data'][sector_id]['data']
        #       - f['data'][sector_id]['geographies']
        #       - f['data'][sector_id]['enduses']
        #       - f['data'][sector_id]['times']
        for sector_id, sectordataset in datafile.sectordata.items():
            tmp_name = sector_id + '_temp'
            f['data'][tmp_name] = f['data'][sector_id]
            orig_dset = f['data'][tmp_name]
            del f['data'][sector_id]
            dgroup = f['data'].create_group(sector_id)

            dgroup['geographies'] = Datamap.create(datafile.geo_enum,[]).value
            try:
                geo_map = orig_dset.attrs['geo_mappings'].astype("u4")
            except:
                geo_map = f['geo_mappings'][()].astype("u4")
            if np.any(geo_map == cls.ZERO_IDX):
                null_pos = (geo_map == cls.ZERO_IDX)
                geo_map[null_pos] = NULL_IDX

            dgroup['geographies'][:,'idx'] = geo_map
            try:
                scalings = orig_dset.attrs['geo_scalings']
            except:
                # Handle Joe's format
                scalings = f['geo_scalings'][()]
            dgroup['geographies'][:,'scale'] = scalings

            # already loaded these sub-enums as part of the backward compatible
            # load process
            dgroup['enduses'] = Datamap.create(datafile.enduse_enum,sectordataset.enduses).value
            dgroup['times'] = Datamap.create(datafile.time_enum,sectordataset.times).value

            for attr_name in ['geo_mappings', 'geo_scalings', 'enduse_mappings', 'time_mappings']:
                if attr_name in orig_dset.attrs:
                    del orig_dset.attrs[attr_name]
            assert len(orig_dset.attrs) == 0, "There are attrs left in orig_dset: {}".format(orig_dset.attrs)

            dgroup['data'] = orig_dset
            del f['data'][tmp_name]

    @classmethod
    def load_sectordataset(cls,datafile,f,sector_id):
        dset = f["data/" + sector_id]

        enduses = list(datafile.enduse_enum.ids)
        times = np.array(datafile.time_enum.ids)
        enduses = [enduses[i] for i in dset.attrs['enduse_mappings'][:]]
        times = [times[i] for i in dset.attrs['time_mappings'][:]]

        result = SectorDataset(datafile,sector_id,enduses,times)
                
        try:
            geo_mappings = dset.attrs["geo_mappings"]
        except:
            # handles Joe's files
            geo_mappings = f["geo_mappings"]

        geo_ptrs = [x for x in geo_mappings if not (x == cls.ZERO_IDX)]
        
        result.n_geos = len(set(geo_ptrs))

        return result


OLD_VERSIONS = OrderedDict()
OLD_VERSIONS[DSG_0_1_0.from_version] = DSG_0_1_0


def make_fuel_and_units_explicit(datafile, filepath, fuel='Electricity', units='MWh'):
    old_enduse_enum = datafile.enduse_enum
    assert isinstance(old_enduse_enum,EndUseEnumeration), "This upgrade method is for datafiles with old-style EndUseEnumerations"
    new_enduse_enum = SingleFuelEndUseEnumeration(
        old_enduse_enum.name,
        old_enduse_enum.ids,
        old_enduse_enum.names,
        fuel=fuel,
        units=units)
    
    result = Datafile(filepath,
                      datafile.sector_enum,
                      datafile.geo_enum,
                      new_enduse_enum,
                      datafile.time_enum)
    for sector_id in datafile:
        old_sector = datafile[sector_id]
        new_sector = result.add_sector(sector_id,enduses=old_sector.enduses,times=old_sector.times)
        for i in range(old_sector.n_geos):
            df, geo_ids, scalings = old_sector.get_data(i)
            new_sector.add_data(df,geo_ids,scalings=scalings,full_validation=False)

    return result
