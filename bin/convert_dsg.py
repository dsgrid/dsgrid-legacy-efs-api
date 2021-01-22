"""Converts a .dsg file to a directory of parquet files"""

import json
import logging
import multiprocessing
import os
import shutil
import sys
from functools import reduce

import numpy as np
import pandas as pd
import h5py
from pyspark.sql import Row, SparkSession
from pyspark.sql.dataframe import DataFrame

from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.sectordataset import SectorDataset


class ConvertDsg:
    """Converts a .dsg file to a directory of parquet files"""
    def __init__(self, filename):
        self._filename = filename
        self._output = None
        self._datafile = Datafile.load(filename)
        self._load_data_table = []
        self._load_geo_table = []
        self._basename = os.path.splitext(os.path.basename(filename))[0]
        self._id = 0
        self._data_df = None
        self._data_dfs = []
        self._num_buckets = 0

        self._spark = SparkSession.builder \
            .master('local') \
            .appName('convertDsg') \
            .config('spark.executor.memory', '20gb') \
            .config("spark.cores.max", str(multiprocessing.cpu_count())) \
            .getOrCreate()
        # Optimize for converting pandas DataFrames to spark.
        self._spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        self._spark.sql("set spark.sql.parquet.compression.codec=gzip")

        with h5py.File(filename, "r") as f:
            self._dimension_types = {
                x: f["enumerations"][x].attrs["name"] for x in f["enumerations"].keys()
            }

    def convert(self, output, num_buckets=0):
        """Convert the dsg file to parquet files.

        Parameters
        ----------
        output : str
            output directory
        num_buckets : int
            Number of Spark buckets to create. Use 0 for no buckets.
            This should only be set if there is no way to reasonably partition
            the data by columns (partitioning would make too many directories).
            Determine the expected size of the generated parquet file(s).
            Set this number to size / 128 MiB.

        """
        self._num_buckets = num_buckets
        self._output = os.path.join(output, self._basename)
        os.makedirs(self._output, exist_ok=True)

        # This can likely be removed. Useful for now.
        dimensions_filename = os.path.join(self._output, "dimensions.json")
        with h5py.File(self._filename, "r") as h5f:
            data = {
                "dimension_types": self._dimension_types,
                "end_uses": [x[0].decode("utf-8") for x in h5f['enumerations']['enduse'][:]]
            }
            with open(dimensions_filename, "w") as f_out:
                json.dump(data, f_out, indent=2)
            self._convert(h5f)

    def _next_id(self):
        self._id += 1
        return self._id

    def _append_data_dataframe(self, df, load_id):
        df["id"] = np.int32(load_id)
        spark_df = self._spark.createDataFrame(df)
        if self._num_buckets == 0:
            self._data_dfs.append(spark_df)
        else:
            os.makedirs(self._output + "/data", exist_ok=True)
            data_filename = os.path.join(self._output, f"data/data_{load_id}.parquet")
            spark_df.write.parquet(data_filename, mode="overwrite")

    def _convert(self, h5f):
        for _, sector_dataset in SectorDataset.loadall(self._datafile, h5f):
            for geo_idx in range(sector_dataset.n_geos):
                self._convert_geo_id(sector_dataset, geo_idx)

        record_filename = os.path.join(self._output, "load_records.parquet")
        df = self._spark.createDataFrame(Row(**x) for x in self._load_geo_table)
        df.write.parquet(record_filename, mode="overwrite")

        ld_table_filename = os.path.join(self._output, "load_data_table.parquet")
        df = self._spark.createDataFrame(Row(**x) for x in self._load_data_table)
        df.write.parquet(ld_table_filename, mode="overwrite")

        data_filename = os.path.join(self._output, "data.parquet")
        if self._num_buckets == 0:
            df = reduce(DataFrame.union, self._data_dfs).coalesce(1)
            df.write.parquet(data_filename, mode="overwrite")
        else:
            data_dir = os.path.join(self._output, "data")
            df = self._spark.read.parquet(data_dir, mergeSchema=True, recursiveFileLookup=True)
            df = df.repartition(1)
            name = self._basename + "_bucketed"
            tmp_path = os.path.join("spark-warehouse", name)
            if os.path.exists(tmp_path):
                shutil.rmtree(tmp_path)
            df.write.format("parquet") \
                .bucketBy(self._num_buckets, "id") \
                .mode("overwrite") \
                .saveAsTable(name)
            shutil.rmtree(data_dir)
            if os.path.exists(data_filename):
                shutil.rmtree(data_filename)
            os.rename(tmp_path, data_filename)

    def _convert_geo_id(self, sector_dataset, geo_idx):
        df, geos, scales = sector_dataset.get_data(geo_idx)
        df.reset_index(inplace=True)
        df.insert(0, "timestamp", pd.to_datetime(df["index"]))
        df.drop("index", inplace=True, axis=1)
        load_id = self._next_id()
        self._append_data_dataframe(df, load_id)
        sector_id = sector_dataset.sector_id

        self._load_data_table.append({
            "id": load_id,
            "name": str(load_id),
            "geo_index": geo_idx,
            "sector_id": sector_id,
        })
        for geo, scale_factor in zip(geos, scales):
            self._load_geo_table.append(
                {
                    "id": geo,
                    "sector_id": sector_id,
                    "scale_factor": float(scale_factor),
                    "data_id": load_id,
                }
            )


def main(dsg_file, output, num_buckets=0):
    logging.basicConfig(level=logging.INFO)
    ConvertDsg(dsg_file).convert(output, num_buckets)


if __name__ == "__main__":
    # Here is how to run the script through spark-submit:
    # spark-submit --total-executor-cores=8 \
    #     --driver-memory=8G \
    #     --executor-memory=16G \
    #     --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" \
    #     --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" \
    #     convert_dsg.py data/filename.dsg output

    if len(sys.argv) not in (3, 4):
        # Refer to ConvertDsg.convert() docstring for help on NUM_BUCKETS
        print(f"Usage: {sys.argv[0]} DSG_FILE OUTPUT [NUM_BUCKETS]")
        sys.exit(1)

    if len(sys.argv) == 3:
        main(sys.argv[1], sys.argv[2])
    else:
        main(sys.argv[1], sys.argv[2], int(sys.argv[3]))
