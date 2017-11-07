
from enum import Enum, auto

from dsgrid import DSGridError
from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.datatable import Datatable


class LoadModelStatus(Enum):
    RAW = auto()


class ComponentType(Enum):
    BOTTOMUP = auto()
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
        if sort and not self._datatable.sorted:
            self._datatable.sort()
        return self._datatable

    def aggregate(self,dirpath,mapping):
        result = LoadModelComponent(self.component_type,self.name,color=self.color)
        if self._datafile:
            result._datafile = self._datafile.aggregate(os.path.join(dirpath,os.path.basename(self.datafile.h5path)),mapping)
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

    def aggregate(self,dirpath,mapping):
        if os.path.exists(dirpath):
            raise DSGridError("Must aggregate to a new location")
        os.mkdir(dirpath)
        result = LoadModel()
        for key, component in self.components.items():
            agg_component = component.aggregate(dirpath,mapping)
            result.components[key] = agg_component
        return result
