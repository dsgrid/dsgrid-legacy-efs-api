import h5py
import os
from uuid import uuid4

class TempHDF5():

    def __init__(self):
        self.filename = str(uuid4()) + ".h5"
        self.file = h5py.File(self.filename, 'w')

    def __enter__(self):
        return self.file

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        self.file.close()
        os.remove(self.filename)
