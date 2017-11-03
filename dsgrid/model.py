
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

    @property
    def key(self):
        return (self.component_type,self.name)

    def __str__(self):
        return "{}, {}".format(self.name,self.component_type.name)

    def load_datafile(self,filepath):
        self._datafile = Datafile(filepath)

    @property
    def datatable(self):
        if self._datafile is not None:
            return Datatable(self._datafile)
        return None


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
