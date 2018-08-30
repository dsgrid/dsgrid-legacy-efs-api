from __future__ import print_function, division, absolute_import

from builtins import super
import logging
from uuid import UUID

from layerstack.args import Arg, Kwarg
from layerstack.layer import LayerBase

from dsgrid.dataformat.datafile import Datafile

logger = logging.getLogger('layerstack.layers.UpdateDsgridDatafile')


class UpdateDsgridDatafile(LayerBase):
    name = "Update dsgrid Datafile"
    uuid = UUID("e92ab9fd-1d1c-4be9-a38c-3ecb50836131")
    version = '0.1.0'
    desc = "Update a dsgrid Datafile to the current version"

    @classmethod
    def args(cls):
        arg_list = super().args()
        arg_list.append(Arg('in_filepath', 
                            description='dsgrid Datafile to upgrade', 
                            parser=str))
        return arg_list

    @classmethod
    def kwargs(cls):
        kwarg_dict = super().kwargs()
        kwarg_dict['overwrite'] = Kwarg(
            default=False, 
            description='Set to true if update should be in-place',
            parser=bool,
            action='store_true')
        kwarg_dict['out_filepath'] = Kwarg(
            default=None, 
            description="Where to save the upgraded file. A new filename will "
                        "be constructed by appending the version number to the "
                        "original filename if not overwrite and out_filepath "
                        "is None",
            parser=str)
        return kwarg_dict

    @classmethod
    def apply(cls, stack, in_filepath, overwrite=False, out_filepath=None):
        stack.model = Datafile.load(in_filepath,overwrite=overwrite,new_filepath=out_)
        return True


if __name__ == '__main__':
    # Single-layer command-line interface entry point.

    # Parameters
    # ----------
    # log_format : str
    #     custom logging format to use with the logging package via 
    #     layerstack.start_console_log
    # 
    UpdateDsgridDatafile.main()

    