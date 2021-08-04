"""
A dsgrid LoadModel consists of a number of different LoadModelComponents. Each
LoadModelComponent is a BOTTOMUP, GAP, DG, TOPDOWN, or DERIVED ComponentType. 
Load is represented by the BOTTOMUP (detailed) and GAP (coarser) models. The 
other components allow for computation of variants other than total site load 
(e.g. net site load, system load), as well as validation/calibration/
reconciliation with historical data.
"""


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


# TODO: Losses should be their own component type. And maybe DERIVED should be
# replaced by RESIDUALS/ERRORS?

class ComponentType(Enum):
    BOTTOMUP = auto()
    GAP = auto()
    DG = auto()
    TOPDOWN = auto()
    DERIVED = auto()

    UNKNOWN = 9999


class LoadModelComponent(object):

    def __init__(self,name,component_type=ComponentType.UNKNOWN,color=None):
        """
        A LoadModelComponent is generally associated with a 
        dsgrid.dataformat.Datafile, and is therefore data of a particular type,
        represented at a certain geographic, temporal, sectoral, and end-use 
        resolution. A LoadModelComponent is also commonly part of a LoadModel. 
        Based on the dimension-mapping capabilities of dsgrid, the LoadModel 
        likely has uniform geographical and temporal extents and resolution.

        Attributes
        ----------
        name : str
            The name of the LoadModelComponent
        component_type : ComponentType
            The type of the LoadModelComponent, defaults to ComponentType.UNKNOWN
        color : str
            Color to use when plotting this LoadModelComponent, in Hexadecimal.
            Otherwise None.
        """
        self.component_type = ComponentType(component_type)
        self.name = name
        self.color = color
        self._datafile = None
        self._datatable = None

    @property
    def key(self):
        """
        The (hash) key used by LoadModel to identify this LoadModelComponent.

        Returns
        -------
        tuple
            (self.component_type,self.name)
        """
        return (self.component_type,self.name)

    @property
    def datafile(self):
        """
        Returns
        -------
        None or dsgrid.dataformat.Datafile
            Returns a Datafile if self.load_datafile has been called successfully.
        """
        return self._datafile

    def __str__(self):
        """
        Returns
        -------
        str
            name, component_type name
        """
        return "{}, {}".format(self.name,self.component_type.name)

    def load_datafile(self,filepath):
        """
        Parameters
        ----------
        filepath : str
            Path to dsgrid.dataformat.Datafile corresponding to this component
        """
        self._datafile = Datafile.load(filepath)

    @classmethod
    def clone(cls,original,filepath=None):
        """
        Creates a new instance of a LoadModelComponent, perhaps pointing to a 
        different dsgrid.dataformat.Datafile.

        Parameters
        ----------
        original : LoadModelComponent
            Only the metadata (e.g. name, type, color) are used, and orignial 
            is not modified in any way
        filepath : str or None
            If not None, path to the dsgrid.dataformat.Datafile corresponding to 
            the new LoadModelComponent created by this method. In most cases, 
            should not be the same as original.datafile.h5path.

        Returns
        -------
        LoadModelComponent
            Clone of original, with the appropriate dsgrid.dataformat.Datafile
            loaded if filepath was not None.
        """
        result = cls(original.name,component_type=original.component_type,color=original.color)
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
        Saves this component (primarily its datafile) to a new directory.

        Parameters
        ----------
        dirpath : str
            The folder to which to save this component's datafile (using the 
            same filename)
        
        Returns
        -------
        LoadModelComponent
            A new component with the copy of this component's datafile that has 
            been saved to dirpath already loaded
        """
        result = LoadModelComponent(self.name,component_type=self.component_type,color=self.color)
        if self._datafile:
            p = os.path.join(dirpath,os.path.basename(self.datafile.h5path))
            result._datafile = self._datafile.save(p)
        return result

    def map_dimension(self,dirpath,to_enum,mappings,filename_prefix=''):
        """
        If there is an appropriate mapping, map the dimension specified by 
        to_enum's class to match to_enum. If there is not, simply create a copy
        of this LoadModelComponent in the new location.

        Parameters
        ----------
        dirpath : str
            Directory in which to save the result of mapping this LoadModelComponent
        to_enum : dsgrid.dataformat.enumeration.Enumeration
            This enumeration's class (e.g. SectorEnumeration, TimeEnumeration) 
            determines which dimension is being mapped. The enumeration 
            specifies the target resolution and particular naming convention 
            that is desired.
        mappings : dsgrid.dataformat.dimmap.Mappings
            Container of dsgrid.dataformat.dimmap.DimensionMaps. If for the 
            dimension in question this component's enumeration (the from_enum), 
            there is either a TautologyMapping to to_enum, or there is an 
            explicit mapping to to_enum, then we say that we are able to map 
            this component, and proceed to do that by either simply saving this 
            component to the new location or by calling 
            dsgrid.dataformat.Datafile.map_dimension.
        filename_prefix : str
            Default is the empty string, ''. If not empty, the new 
            dsgrid.dataformat.Datafile created by this method is saved using the 
            filename created by placing this string before the current 
            datafile's filename.

        Returns
        -------
        LoadModelComponent
            New component with either a copy of this component's datafile loaded,
            or a new, mapped datafile loaded. In either case the new datafile is
            located at os.path.join(dirpath,filename_prefix + os.path.basename(self.datafile.h5path)).
        """
        result = LoadModelComponent(self.name,component_type=self.component_type,color=self.color)
        if self._datafile:
            mapping = mappings.get_mapping(self._datafile,to_enum)
            p = os.path.join(dirpath,filename_prefix + os.path.basename(self.datafile.h5path))
            if mapping is None:
                logger.warn("Unable to map Component {} to {}".format(self.name,to_enum.name))
                result._datafile = self._datafile.save(p)
                return result
            if isinstance(mapping,TautologyMapping):
                result._datafile = self._datafile.save(p)
            else:
                result._datafile = self._datafile.map_dimension(p,mapping)
        else:
            logger.warn("Asked to map LoadModelComponent {} even though no Datafile is loaded.".format(self))
        return result

    def scale_data(self,dirpath,factor=0.001):
        """
        Scale all the data in self.datafile by factor, creating a new HDF5 file 
        and corresponding LoadModelComponent. 

        Parameters
        ----------
        dirpath : str
            Folder the new version of the data should be placed in. This 
            LoadModelComponent's filename will be retained.
        factor : float
            Factor by which all the data in the file is to be multiplied. The 
            default value of 0.001 corresponds to converting the bottom-up data 
            from kWh to MWh.
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
        self.components[value.key] = value

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
        """
        Map the appropriate dimension to_enum for all possible components. Will 
        only map those components for which the given dimension (determined by 
        to_enum's class) is resolved by a from_enum for which a map between 
        from_enum and to_enum is in mappings.

        Parameters
        ----------
        dirpath : str
            Folder for new model created by doing this mapping
        to_enum : dsgrid.dataformat.Enumeration
            Enumeration to which to map the appropriate dimension
        mappings : dsgrid.dimmap.Mappings
            collection of dsgrid.dimmap.DimensionMap objects. Only maps 
            registered in this object will be applied.
        """
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
