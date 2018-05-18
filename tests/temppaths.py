import h5py
import os
import shutil
from uuid import uuid4


class TempHDF5File():

    def __init__(self,ext='.dsg'):
        self.filename = str(uuid4()) + ext
        self.file = h5py.File(self.filename, 'w')

    def __enter__(self):
        return self.file

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        self.file.close()
        os.remove(self.filename)


class TempFilepath():

    def __init__(self,ext='.dsg'):
        self.filename = str(uuid4()) + ext

    def __enter__(self):
        return self.filename

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        os.remove(self.filename)


class TempDir():
    def __init__(self):
        self.dirname = str(uuid4())

    def __enter__(self):
        os.mkdir(self.dirname)
        return self.dirname

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        shutil.rmtree(self.dirname)
