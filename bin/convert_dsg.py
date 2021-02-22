"""Converts a .dsg file to a directory of parquet files"""

# Users: you need to install pyspark, h5py, numpy, pandas as well as have the
# dsgrid-load repo in your Python path.
# Installing pyspark will add spark-submit to your system path.
# The recommended way to run this script is through spark-submit. Refer to the
# instructions at the bottom of the file.


import json
import logging
import multiprocessing
import os
import shutil
import sys
import time
from functools import reduce

import numpy as np
import pandas as pd
import h5py
from pyspark.sql import Row, SparkSession
from pyspark.sql.dataframe import DataFrame

from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.sectordataset import SectorDataset


logger = None


class ConvertDsg:
    """Converts a .dsg file to a directory of parquet files"""
    def __init__(self, output_dir):
        self._output_dir = output_dir
        self._load_data_lookup = []
        self._id = 0
        self._data_df = None
        self._data_dfs = []
        self._num_buckets = 0
        self._basename = os.path.basename(output_dir)
        self._timestamps = None  # There is one array of timestamps per file.
                                 # Initialize on the first occurrence.

        self._spark = SparkSession.builder \
            .master('local') \
            .appName('convert_dsg') \
            .getOrCreate()
        # Optimize for converting pandas DataFrames to spark.
        self._spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    def convert(self, filename, num_buckets=0):
        """Convert the dsg file to parquet files.

        Parameters
        ----------
        output_dir : str
            output_dir directory
        num_buckets : int
            Number of Spark buckets to create. Use 0 for no buckets.
            This should only be set if there is no way to reasonably partition
            the data by columns (partitioning would make too many directories).
            Determine the expected size of the generated parquet file(s).
            Set this number to size / 128 MiB.

        """
        logger.info("Converting %s to parquet num_buckets=%s", filename, num_buckets)
        self._num_buckets = num_buckets
        start = time.time()

        # This can likely be removed. Useful for now.
        dimensions_filename = os.path.join(self._output_dir, "dimensions.json")
        with h5py.File(filename, "r") as h5f:
            data = {
                "dimension_types": {x: h5f["enumerations"][x].attrs["name"] for x in h5f["enumerations"].keys()},
                "end_uses": [x[0].decode("utf-8") for x in h5f['enumerations']['enduse'][:]],
                "end_use_attributes": {},
            }
            for attr in h5f["enumerations/enduse"].attrs.keys():
                data["end_use_attributes"][attr] = h5f["enumerations/enduse"].attrs[attr]

            with open(dimensions_filename, "w") as f_out:
                json.dump(data, f_out, indent=2)
            for enumeration in h5f["enumerations"].keys():
                df = pd.DataFrame(h5f[f"enumerations/{enumeration}"][:])
                df.to_csv(os.path.join(self._output_dir, enumeration + ".csv"))
            self._convert(filename, h5f)

        duration = time.time() - start
        logger.info("Created %s duration=%s seconds", self._output_dir, duration)

    def _next_id(self):
        self._id += 1
        return self._id

    def _append_data_dataframe(self, df, load_id):
        df["id"] = np.int32(load_id)
        spark_df = self._spark.createDataFrame(df)
        # If bucketing is not being used then collect all dfs in a list that
        # can be concatenated at the end.
        # If bucketing is being used then write them to temporary, individual
        # files and use spark to bucket them at the end.
        if self._num_buckets == 0:
            self._data_dfs.append(spark_df)
        else:
            os.makedirs(self._output_dir + "/data", exist_ok=True)
            data_filename = os.path.join(self._output_dir, f"data/data_{load_id}.parquet")
            spark_df.write.parquet(data_filename, mode="overwrite")
            logger.info("Created temporary file %s", data_filename)

    def _convert(self, filename, h5f):
        datafile = Datafile.load(filename)
        for _, sector_dataset in SectorDataset.loadall(datafile, h5f):
            for geo_idx in range(sector_dataset.n_geos):
                self._convert_geo_id(sector_dataset, geo_idx)

        record_filename = os.path.join(self._output_dir, "load_data_lookup.parquet")
        df = self._spark.createDataFrame(Row(**x) for x in self._load_data_lookup)
        df.write.parquet(record_filename, mode="overwrite")

        data_filename = os.path.join(self._output_dir, "data.parquet")
        if self._num_buckets == 0:
            start = time.time()
            df = reduce(DataFrame.union, self._data_dfs).coalesce(1)
            df.write.parquet(data_filename, mode="overwrite")
            logger.info("Time to coalesce dataframes %s seconds", time.time() - start)
        else:
            data_dir = os.path.join(self._output_dir, "data")
            t1 = time.time()
            df = self._spark.read.parquet(data_dir, mergeSchema=True, recursiveFileLookup=True)
            df = df.repartition(1)
            t2 = time.time()
            logger.info("Time to repartition(1) = %s seconds", t2 - t1)
            name = self._basename + "_bucketed"
            tmp_path = os.path.join("spark-warehouse", name)
            if os.path.exists(tmp_path):
                shutil.rmtree(tmp_path)
            df.write.format("parquet") \
                .bucketBy(self._num_buckets, "id") \
                .mode("overwrite") \
                .saveAsTable(name)
            t3 = time.time()
            logger.info("Time to bucket with num_buckets=%s = %s seconds", self._num_buckets, t3 - t2)
            shutil.rmtree(data_dir)
            if os.path.exists(data_filename):
                shutil.rmtree(data_filename)
            os.rename(tmp_path, data_filename)

    def _convert_geo_id(self, sector_dataset, geo_idx):
        df, geos, scales = sector_dataset.get_data(geo_idx)
        df.reset_index(inplace=True)
        if self._timestamps is None:
            self._timestamps = pd.to_datetime(df["index"])
        df.insert(0, "timestamp", self._timestamps)
        df.drop("index", inplace=True, axis=1)
        load_id = self._next_id()
        self._append_data_dataframe(df, load_id)
        sector_id = sector_dataset.sector_id

        for geo, scale_factor in zip(geos, scales):
            self._load_data_lookup.append(
                {
                    "geography": geo,
                    "subsector": sector_id,
                    "scale_factor": float(scale_factor),
                    "data_id": load_id,
                }
            )


def setup_logging(filename, file_level=logging.INFO, console_level=logging.INFO):
    global logger
    logger = logging.getLogger("DSG")
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler(filename)
    fh.setLevel(file_level)
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)


def main(dsg_file, output_dir, num_buckets=0):
    os.makedirs(output_dir, exist_ok=True)
    base_dir = os.path.join(output_dir, os.path.basename(dsg_file.replace(".dsg", "")))
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    os.makedirs(base_dir, exist_ok=True)
    log_file = os.path.join(base_dir, "convert_dsg.log")
    setup_logging(log_file)
    logger.info("CLI args: %s", " ".join(sys.argv))
    ConvertDsg(base_dir).convert(dsg_file, num_buckets)


if __name__ == "__main__":
    # Here is how to run the script through spark-submit:
    # spark-submit --total-executor-cores=8 \
    #     --driver-memory=8G \
    #     --executor-memory=16G \
    #     --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" \
    #     --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" \
    #     convert_dsg.py data/filename.dsg output_dir

    if len(sys.argv) not in (3, 4):
        # Refer to ConvertDsg.convert() docstring for help on NUM_BUCKETS
        print(f"Usage: {sys.argv[0]} DSG_FILE OUTPUT_DIR [NUM_BUCKETS]")
        sys.exit(1)

    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        main(sys.argv[1], sys.argv[2], int(sys.argv[3]))
