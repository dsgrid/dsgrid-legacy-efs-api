import h5py
from os import remove
from uuid import uuid4

class TempHDF5File():

    def __init__(self,ext='.dsg'):
        self.filename = str(uuid4()) + ext
        self.file = h5py.File(self.filename, 'w')

    def __enter__(self):
        return self.file

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        self.file.close()
        remove(self.filename)


class TempFilepath():

    def __init__(self,ext='.dsg'):
        self.filename = str(uuid4()) + ext

    def __enter__(self):
        return self.filename

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        remove(self.filename)
