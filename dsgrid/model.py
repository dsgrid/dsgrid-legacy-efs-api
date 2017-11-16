
from enum import Enum, auto
import logging
import os

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
    TOPDOWN = auto()
    DERIVED = auto()


class LoadModelComponent(object):
    def __init__(self,component_type,name,color=None):
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

    def get_datatable(self,sort=True,**kwargs):
        if self._datatable is None and self._datafile is not None:
            self._datatable = Datatable(self._datafile,sort=sort,**kwargs)
        if sort and (self._datatable is not None) and (not self._datatable.sorted):
            self._datatable.sort()
        return self._datatable

    def map_dimension(self,dirpath,to_enum,mappings,filename_prefix=''):
        result = LoadModelComponent(self.component_type,self.name,color=self.color)
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
        result = LoadModelComponent(self.component_type,self.name,color=self.color)
        if self._datafile:
            p = os.path.join(dirpath,os.path.basename(self.datafile.h5path))
            result._datafile = self._datafile.scale_data(p,factor=factor)
        return result


class LoadModel(object):

    def __init__(self):
        self.status = LoadModelStatus.RAW
        self.components = {}

    @classmethod
    def create(cls,components):
        result = LoadModel()
        for component in components:
            if not isinstance(component,LoadModelComponent):
                raise DSGridError("Expected a LoadModelComponent, got a {}.".format(type(component)))
            result.components[component.key] = component
        return result

    def map_dimension(self,dirpath,to_enum,mappings):
        if os.path.exists(dirpath):
            raise DSGridError("Must map_dimension to a new location")
        os.mkdir(dirpath)
        result = LoadModel()
        for key, component in self.components.items():
            agg_component = component.map_dimension(dirpath,to_enum,mappings)
            result.components[key] = agg_component
        return result
