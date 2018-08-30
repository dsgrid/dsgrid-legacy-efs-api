from __future__ import print_function, division, absolute_import

from builtins import super
import logging
from uuid import UUID

from layerstack.args import Arg, Kwarg
from dsgrid.layerstack import DSGridDatafileLayer

from dsgrid.dataformat.dimmap import FullAggregationMap
from dsgrid.dataformat.enumeration import (allsectors, allenduses, 
    GeographyEnumeration, MultiFuelEndUseEnumeration, SectorEnumeration, 
    SingleFuelEndUseEnumeration, TimeEnumeration)

logger = logging.getLogger('layerstack.layers.AggregateOneDimension')


class AggregateOneDimension(DSGridDatafileLayer):
    name = "Aggregate One Dimension"
    uuid = UUID("3fa11d00-cade-4942-a03c-23f69aac3f6f")
    version = '0.1.0'
    desc = None

    DIMENSIONS = ['sector','enduse','geography','time']

    @classmethod
    def args(cls, model=None):
        arg_list = super().args()
        arg_list.append(Arg('dimension', 
                            description='Which dimension to aggregate',
                            choices=cls.DIMENSIONS))
        return arg_list

    @classmethod
    def kwargs(cls, model=None):
        kwarg_dict = super().kwargs()
        kwarg_dict['aggregation_id'] = Kwarg(
            default=None, 
            description='Id for aggregation enumeration. Must be provided for geography and time aggregations.',
            parser=str)
        kwarg_dict['aggregation_name'] = Kwarg(
            default=None, 
            description='Name for aggregation enumeration. Only used if aggregation_id provided. Defaults to aggregation_id if aggregation_name is None.',
            parser=str)
        kwarg_dict['out_filepath'] = Kwarg(
            default=None, 
            description="Location for downselected Datafile. If not provided, " + 
                        "will append aggregation_id.lower() to the " +
                        "current filename.",
            parser=str)
        kwarg_dict['exclude_list'] = Kwarg(
            default = [],
            description = "List of enumeration ids (in current enumeration " + 
                "for dimension being aggregated) to exclude from the aggregation.",
            parser = str,
            nargs = '*'
        )
        return kwarg_dict

    @classmethod
    def apply(cls, stack, model, dimension, aggregation_id = None, 
              aggregation_name = None, out_filepath = None, exclude_list = []):
        
        # figure out from_enum and to_enum

        # helper to create new enumeration to map to
        def get_to_enum(enum_cls,**kwargs):
            if aggregation_id is None:
                logger.error("Must provide aggregation_id for dimension '{}'".format(dimension))
                raise ValueError("aggregation_id cannot be None")
            if aggregation_name is None:
                aggregation_name = aggregation_id
            return enum_cls(aggregation_name,[aggregation_id],[aggregation_name],**kwargs)

        if dimension == 'sector':
            from_enum = model.sector_enum
            if aggregation_id is None:
                to_enum = allsectors
            else:
                to_enum = get_to_enum(SectorEnumeration)
        elif dimension == 'enduse':
            from_enum = model.enduse_enum
            if isinstance(from_enum,MultiFuelEndUseEnumeration):
                logger.error("Cannot aggregate across multiple fuels. Please filter to single fuel first.")
                raise ValueError("If dimension is 'enduse' Datafile.enduse_enum cannot be a MultiFuelEndUseEnumeration")
            if not from_enum.ids:
                logger.error("Current end-use enum is empty. Cannot aggregate.")
                raise ValueError("Cannot determine fuel and units from {}.".format(from_enum))
            test_id = list(from_enum.ids)[0]
            fuel_kwargs = {
                'fuel': from_enum.fuel(test_id),
                'units': from_enum.units(test_id)
            }
            if aggregation_id is None:
                to_enum = SingleFuelEndUseEnumeration(
                    allenduses.name,allenduses.ids,allenduses.names,
                    **fuel_kwargs)
            else:
                to_enum = get_to_enum(SingleFuelEndUseEnumeration,**fuel_kwargs)
        elif dimension == 'geography':
            from_enum = model.geo_enum
            to_enum = get_to_enum(GeographyEnumeration)
        elif dimension == 'time':
            from_enum = model.time_enum
            to_enum = get_to_enum(TimeEnumeration)
        else:
            logger.error("Unknown dimension {}".format(dimension))
            raise ValueError("dimension must be one of {}".format(cls.DIMENSIONS))

        # now actually map
        new_filepath = cls.new_filepath(model,to_enum.ids[0].lower(),out_filepath=out_filepath)
        aggregation_map = FullAggregationMap(from_enum,to_enum,exclude_list=exclude_list)        
        model = model.map_dimension(new_filepath, aggregation_map)
        return model


if __name__ == '__main__':
    # Single-layer command-line interface entry point.

    # Parameters
    # ----------
    # log_format : str
    #     custom logging format to use with the logging package via 
    #     layerstack.start_console_log
    # 
    AggregateOneDimension.main()

    