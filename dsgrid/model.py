
from collections import OrderedDict
from collections.abc import MutableMapping
import copy
from enum import Enum, auto
import logging
import os
import shutil

from dsgrid import DSGridError
from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.datatable import Datatable
from dsgrid.dataformat.dimmap import TautologyMapping

logger = logging.getLogger(__name__)


class LoadModelStatus(Enum):
    RAW = auto()


class ComponentType(Enum):
    BOTTOMUP = auto()
    GAP = auto()
    DG = auto()
    TOPDOWN = auto()
    DERIVED = auto()

    UNKNOWN = 9999


class LoadModelComponent(object):
    def __init__(self,name,component_type=ComponentType.UNKNOWN,color=None):
        self.component_type = ComponentType(component_type)
        self.name = name
        self.color = color
        self._datafile = None
        self._datatable = None

    @property
    def key(self):
        return (self.component_type,self.name)

    @property
    def datafile(self):
        return self._datafile

    def __str__(self):
        return "{}, {}".format(self.name,self.component_type.name)

    def load_datafile(self,filepath):
        self._datafile = Datafile.load(filepath)

    @classmethod
    def clone(cls,original,filepath=None):
        result = cls(original.component_type,original.name,color=original.color)
        if filepath is not None:
            result.load_datafile(filepath)
        return result

    def get_datatable(self,sort=True,**kwargs):
        if self._datatable is None and self._datafile is not None:
            self._datatable = Datatable(self._datafile,sort=sort,**kwargs)
        if sort and (self._datatable is not None) and (not self._datatable.sorted):
            self._datatable.sort()
        return self._datatable

    def save(self,dirpath):
        """
        Saves this component to a new directory, returning the new component.
        """
        result = LoadModelComponent(self.name,component_type=self.component_type,color=self.color)
        if self._datafile:
            p = os.path.join(dirpath,os.path.basename(self.datafile.h5path))
            result._datafile = self._datafile.save(p)
        return result

    def map_dimension(self,dirpath,to_enum,mappings,filename_prefix=''):
        result = LoadModelComponent(self.name,component_type=self.component_type,color=self.color)
        if self._datafile:
            mapping = mappings.get_mapping(self._datafile,to_enum)
            if mapping is None:
                logger.warn("Unable to map Component {} to {}".format(self.name,to_enum.name))
                return None
            p = os.path.join(dirpath,filename_prefix + os.path.basename(self.datafile.h5path))
            if isinstance(mapping,TautologyMapping):
                result._datafile = self._datafile.save(p)
            else:
                result._datafile = self._datafile.map_dimension(p,mapping)
        return result

    def scale_data(self,dirpath,factor=0.001):
        """
        Scale all the data in self.datafile by factor, creating a new HDF5 file 
        and corresponding LoadModelComponent. 

        Arguments:
            - dirpath (str) - Folder the new version of the data should be 
                  placed in. This LoadModelComponent's filename will be retained.
            - factor (float) - Factor by which all the data in the file is to be
                  multiplied. The default value of 0.001 corresponds to converting
                  the bottom-up data from kWh to MWh.
        """
        result = LoadModelComponent(self.name,component_type=self.component_type,color=self.color)
        if self._datafile:
            p = os.path.join(dirpath,os.path.basename(self.datafile.h5path))
            result._datafile = self._datafile.scale_data(p,factor=factor)
        return result


class LoadModel(MutableMapping):

    def __init__(self):
        self.status = LoadModelStatus.RAW
        self.components = OrderedDict()

    def __getitem__(self,key):
        return self.components[key]

    def __setitem__(self,key,value):
        if not isinstance(value,LoadModelComponent):
            raise DSGridError("Expected a LoadModelComponent, got a {}.".format(type(value)))
        if not key == value.key:
            raise DSGridError("Expected the key to match the LoadModelComponent.key, " + \
                "but key = {} and LoadModelComponent.key = {}".format(key,value.key))
        result.components[value.key] = value

    def __delitem__(self,key):
        del self.components[key]

    def __iter__(self):
        for key in self.components:
            yield key

    def __len__(self):
        return len(self.components)

    @classmethod
    def create(cls,components):
        result = LoadModel()
        for component in components:
            result.components[component.key] = component
        return result

    def save(self,dirpath,clean=False):
        if clean and os.path.exists(dirpath):
            shutil.rmtree(dirpath)
        os.mkdir(dirpath)
        return self.create([component.save(dirpath) for key, component in self.components.items()])

    def map_dimension(self,dirpath,to_enum,mappings):
        if os.path.exists(dirpath):
            raise DSGridError("Must map_dimension to a new location")
        os.mkdir(dirpath)
        result = LoadModel()
        for key, component in self.components.items():
            agg_component = component.map_dimension(dirpath,to_enum,mappings)
            result.components[key] = agg_component
        return result

    def move_sectors(self,dirpath,from_component_key,to_component_key,sectors_to_move):
        """
        Moves sectors_to_move from_component_key to_component_key. If 
        to_component_key is None, simply drops those sectors.
        """
        if os.path.exists(dirpath):
            raise DSGridError("Must move_sectors to a new location")
        os.mkdir(dirpath)
        result = LoadModel()

        # First find the from_ and to_ components, and transfer all other 
        # components as-is. ALSO, transfer to_component as-is now. It will be
        # appended to later.
        from_component = None; to_component = None
        for key, component in self.components.items():
            if key == from_component_key:
                from_component = component
            elif key == to_component_key:
                to_component = component
            else:
                logger.info("Transferring {}".format(key))
                result.components[key] = component.save(dirpath)

        if from_component is None:
            logger.error("Cannot move sectors {} from component {}, because it was not found. Available components are: {}".format(
                sectors_to_move,from_component_key,list(self.components.keys())))
            shutil.rmtree(dirpath)
            return None

        if (to_component is None) and (to_component_key is not None):
            logger.error("Cannot move sectors {} to component {}, because it was not found. Available components are: {}".format(
                sectors_to_move,to_component_key,list(self.components.keys())))
            shutil.rmtree(dirpath)
            return None

        # Now loop through the from_component, moving what should stay and 
        # temporarily caching what needs to move
        new_from_file = os.path.join(dirpath,os.path.basename(from_component.datafile.h5path))
        new_from_h5 = Datafile(new_from_file,
                               from_component.datafile.sector_enum,
                               from_component.datafile.geo_enum,
                               from_component.datafile.enduse_enum,
                               from_component.datafile.time_enum)
        data_to_move = {}
        logger.info("Transferring part of {}".format(from_component_key))
        for sector_id, sectordataset in from_component.datafile.sectordata.items():
            if sector_id in sectors_to_move:
                data_to_move[sector_id] = sectordataset
            else:
                new_sector = new_from_h5.add_sector(sector_id,enduses=sectordataset.enduses,times=sectordataset.times)
                sectordataset.copy_data(new_sector,full_validation=False) # there should be no new validation issues
        new_from_component = LoadModelComponent.clone(from_component,filepath=new_from_file)
        result.components[from_component_key] = new_from_component

        # Now transfer current data in to_component
        def append_enum_value(current_enum,enum_id_to_check,source_enum):
            if enum_id_to_check not in current_enum.ids:
                enum_name = source_enum.names[source_enum.ids.index(enum_id_to_check)]
                current_enum = copy.deepcopy(current_enum)
                current_enum.ids.append(enum_id_to_check)
                current_enum.names.append(enum_name)
            return current_enum

        if to_component_key is not None:
            assert to_component is not None
            new_to_file = os.path.join(dirpath,os.path.basename(to_component.datafile.h5path))
            sector_enum = to_component.datafile.sector_enum
            for sector_id in data_to_move:
                sector_enum = append_enum_value(sector_enum,sector_id,from_component.datafile.sector_enum)
            enduse_enum = to_component.datafile.enduse_enum
            for sector_id, sectordataset in data_to_move.items():
                for enduse_id in sectordataset.enduses:
                    enduse_enum = append_enum_value(enduse_enum,enduse_id,from_component.datafile.enduse_enum)
            new_to_h5 = Datafile(new_to_file,
                                 sector_enum,
                                 to_component.datafile.geo_enum,
                                 enduse_enum,
                                 to_component.datafile.time_enum)
            logger.info("Transferring {}".format(to_component_key))
            for sector_id, sectordataset in to_component.datafile.sectordata.items():
                new_sector = new_to_h5.add_sector(sector_id,enduses=sectordataset.enduses,times=sectordataset.times)
                sectordataset.copy_data(new_sector,full_validation=False) # there should be no validation issues

            # Mow add to-be-moved data to to_component
            logger.info("Appending part of {} to {}".format(from_component_key,to_component_key))
            for sector_id, sectordataset in data_to_move.items():
                new_sector = new_to_h5.add_sector(sector_id,enduses=sectordataset.enduses,times=sectordataset.times)
                sectordataset.copy_data(new_sector) # validate because the data being transfered come from somewhere else

            new_to_component = LoadModelComponent.clone(to_component,filepath=new_to_file)
            result.components[to_component_key] = new_to_component

        assert len(result.components) == len(self.components)
        return result
