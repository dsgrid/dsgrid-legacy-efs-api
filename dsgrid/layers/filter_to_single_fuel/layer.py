from __future__ import print_function, division, absolute_import

from builtins import super
import logging
import os
from uuid import UUID

from layerstack.args import Arg, Kwarg
from dsgrid.layerstack import DSGridDatafileLayer

from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.dimmap import FilterToSingleFuelMap
from dsgrid.dataformat.enumeration import MultiFuelEndUseEnumeration

logger = logging.getLogger('layerstack.layers.FilterToSingleFuel')


class FilterToSingleFuel(DSGridDatafileLayer):
    name = "Filter to Single Fuel"
    uuid = UUID("a8477042-73dc-4482-8528-60d49ced41c9")
    version = '0.1.0'
    desc = None

    @classmethod
    def args(cls, model=None):
        arg_list = super().args()
        choices = {}
        if model is not None:
            if isinstance(model.enduse_enum,MultiFuelEndUseEnumeration):
                choices = { 'choices': list(model.enduse_enum.fuel_enum.ids) }
        arg_list.append(Arg(
            'fuel_id', 
            description='dsgrid.dataformat.enumeration.FuelEnumeration.id to keep', 
            parser=str,
            **choices))
        return arg_list

    @classmethod
    def kwargs(cls, model=None):
        kwarg_dict = super().kwargs()
        kwarg_dict['out_filepath'] = Kwarg(
            default=None, 
            description="Location for downselected Datafile. If not provided, " + 
                        "will append the suffix fuel_id + '_only' to the " +
                        "current filename.",
            parser=str)
        return kwarg_dict

    @classmethod
    def apply(cls, stack, model, fuel_id, out_filepath=None):
        super().apply(stack,model)
        to_fuel_map = FilterToSingleFuelMap(model.enduse_enum,fuel_id)
        new_filepath = cls.new_filepath(model,fuel_id + '_only',out_filepath=out_filepath)
        model = model.map_dimension(new_filepath,to_fuel_map)
        return model


if __name__ == '__main__':
    # Single-layer command-line interface entry point.

    # Parameters
    # ----------
    # log_format : str
    #     custom logging format to use with the logging package via 
    #     layerstack.start_console_log
    # 
    FilterToSingleFuel.main()

    