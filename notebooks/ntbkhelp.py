
from dsgrid.dataformat.enumeration import EndUseEnumerationBase

import pandas as pd


class OptionPresenter(object):

    def __init__(self, alist):
        self._list = alist
        self._n = len(self._list)
        self._fmt_str = '{: ' + str(len(str(self._n))+1) + '}: {}'

    def present_options(self):
        for i, val in enumerate(self._list, 1):
            print(self._fmt_str.format(i, val))

    def get_option(self, choice):
        return self._list[int(choice)-1]

    def get_options(self, choices):
        return [self._list[int(choice) - 1] for choice in choices]
    
    
def show_enum(an_enum):
    if isinstance(an_enum, EndUseEnumerationBase):
        return show_enduse_enum(an_enum)
    return show_basic_enum(an_enum)
    
def show_basic_enum(an_enum):
    print(an_enum.name)
    return pd.DataFrame(zip(an_enum.ids, an_enum.names), columns = ["id", "name"])

def show_enduse_enum(an_enum):
    print(an_enum.name)
    return pd.DataFrame([[_id, an_enum.get_name(_id), an_enum.fuel(_id), an_enum.units(_id)] for _id in an_enum.ids],
                        columns = ['id', 'name', 'fuel', 'units'])

def show_elements_with_data(an_enum, ids_with_data):
    print(f"Subset of {an_enum.name} with data:")
    df = pd.DataFrame(zip(an_enum.ids, an_enum.names), columns = ["id", "name"])
    return df[df.id.isin(ids_with_data)]
