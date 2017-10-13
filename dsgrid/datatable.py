import pandas as pd

class Datatable(object):

    def __init__(datafile):

        self.sector_enum = datafile.sector_enum
        self.geo_enum = datafile.geo_enum
        self.enduse_enum = datafile.enduse_enum
        self.time_enum = datafile.time_enum

        self.dataframe = pd.DataFrame()

        #TODO: Categorical multiindex based on enums

        #TODO: Individual sector dataset to tabular dataframe conversion

        with h5py.File(self.h5path, "r") as f:

            for sectorname, sectordataset in datafile.sectordata.items():

                #TODO: Convert each sector dataset and append to self.dataframe
                h5dset = f["data/" + sectorname]
