import h5py
import logging
import os
import shutil
from uuid import uuid4

logger = logging.getLogger(__name__)

class TempHDF5File():

    def __init__(self,ext='.dsg'):
        self.filename = str(uuid4()) + ext
        self.file = h5py.File(self.filename, 'w')

    def __enter__(self):
        return self.file

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        if not os.path.exists(self.filename):
            logger.info(f"{self.filename} does not exist")
            return
        self.file.close()
        os.remove(self.filename)


class TempFilepath():

    def __init__(self,ext='.dsg'):
        self.filename = str(uuid4()) + ext

    def __enter__(self):
        return self.filename

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        if not os.path.exists(self.filename):
            logger.info(f"{self.filename} does not exist")
            return
        os.remove(self.filename)


class TempDir():
    def __init__(self):
        self.dirname = str(uuid4())

    def __enter__(self):
        os.mkdir(self.dirname)
        return self.dirname

    def __exit__(self, ctx_type, ctx_value, ctx_traceback):
        if not os.path.exists(self.dirname):
            logger.info(f"{self.dirname} does not exist")
            return
        shutil.rmtree(self.dirname)
