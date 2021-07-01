#!/usr/bin/env python

"""Converts a .dsg file to a directory of parquet files"""

# Users: you need to install pyspark, h5py, numpy, pandas as well as have the
# dsgrid-load repo in your Python path.
# Installing pyspark will add spark-submit to your system path.
# The recommended way to run this script is through spark-submit. Refer to the
# instructions at the bottom of the file.


import json
import logging
import os
import shutil
import sys
import time
from collections import defaultdict
from functools import reduce

import click
import numpy as np
import pandas as pd
import h5py
from pyspark.sql import Row, SparkSession
from pyspark.sql.dataframe import DataFrame

from dsgrid.dataformat.datafile import Datafile
from dsgrid.dataformat.sectordataset import SectorDataset


LOGGER_NAME = "convert_dsg"

logger = logging.getLogger(LOGGER_NAME)


class ConvertDsg:
    """Converts a .dsg file to a directory of parquet files"""

    def __init__(self, output_dir, data_id_offset):
        self._output_dir = output_dir
        self._load_data_lookup = []
        self._data_id = data_id_offset  # Each unique dataframe gets assigned a unique ID.
        self._data_df = None
        self._data_dfs = []
        self._num_buckets = 0
        self._timestamps = None  # There is one array of timestamps per file.
        # Initialize on the first occurrence.
        self._spark = SparkSession.builder.appName("convert_dsg").getOrCreate()

        # Optimize for converting pandas DataFrames to spark.
        self._spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

    def convert(self, filename, auto_scale_factor=True, num_buckets=0):
        """Convert the dsg file to parquet files.

        Parameters
        ----------
        filename : str
            existing .dsg file
        auto_scale_factor : bool
        num_buckets : int
            Number of Spark buckets to create. Use 0 for no buckets.
            This should only be set if there is no way to reasonably partition
            the data by columns (partitioning would make too many directories).
            Determine the expected size of the generated parquet file(s).
            Set this number to size / 128 MiB.

            Setting num_buckets to 0 means that the code will accumulate all
            dataframes from the .dsg file in memory. This can easily consume
            all system memory for large files. Set num_buckets to something
            greater than 0 to avoid this.

        """
        logger.info(
            "Converting %s to parquet num_buckets=%s auto_scale_factor=%s",
            filename,
            num_buckets,
            auto_scale_factor,
        )
        self._num_buckets = num_buckets
        start = time.time()

        # This can likely be removed. Useful for now.
        dimensions_filename = os.path.join(self._output_dir, "dimensions.json")
        with h5py.File(filename, "r") as h5f:
            data = {
                "dimension_types": {
                    x: h5f["enumerations"][x].attrs["name"] for x in h5f["enumerations"].keys()
                },
                "end_uses": [x[0].decode("utf-8") for x in h5f["enumerations"]["enduse"][:]],
                "end_use_attributes": {},
            }
            for attr in h5f["enumerations/enduse"].attrs.keys():
                data["end_use_attributes"][attr] = h5f["enumerations/enduse"].attrs[attr]

            with open(dimensions_filename, "w") as f_out:
                json.dump(data, f_out, indent=2)
            for enumeration in h5f["enumerations"].keys():
                df = pd.DataFrame(h5f[f"enumerations/{enumeration}"][:])
                df.to_csv(os.path.join(self._output_dir, enumeration + ".csv"))
            self._convert(filename, h5f, auto_scale_factor)

        duration = time.time() - start
        logger.info("Last data_id=%s", self._data_id)
        logger.info("Created %s duration=%s seconds", self._output_dir, duration)

    def _next_id(self):
        self._data_id += 1
        return self._data_id

    def _append_data_dataframe(self, df, load_id):
        df["id"] = np.int32(load_id)
        spark_df = self._spark.createDataFrame(df)
        # If bucketing is not being used then collect all dfs in a list that
        # can be concatenated at the end.
        # If bucketing is being used then write them to temporary, individual
        # files and use spark to bucket them at the end.
        if self._num_buckets == 0:
            pass
            self._data_dfs.append(spark_df)
        else:
            os.makedirs(self._output_dir + "/data", exist_ok=True)
            data_filename = os.path.join(self._output_dir, f"data/data_{load_id}.parquet")
            spark_df.write.parquet(data_filename, mode="overwrite")
            logger.info("Created temporary file %s", data_filename)

    def _convert(self, filename, h5f, auto_scale_factor):
        scale_factors = defaultdict(dict)
        datafile = Datafile.load(filename)
        for _, sector_dataset in SectorDataset.loadall(datafile, h5f):
            for geo_idx in range(sector_dataset.n_geos):
                self._convert_geo_id(sector_dataset, geo_idx, auto_scale_factor, scale_factors)

        record_filename = os.path.join(self._output_dir, "load_data_lookup.parquet")
        df = self._spark.createDataFrame(Row(**x) for x in self._load_data_lookup)
        df.repartition(1).write.parquet(record_filename, mode="overwrite")

        data_filename = os.path.join(self._output_dir, "load_data.parquet")
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
            name = os.path.basename(self._output_dir) + "_bucketed"
            tmp_path = os.path.join("spark-warehouse", name)
            if os.path.exists(tmp_path):
                shutil.rmtree(tmp_path)
            df.write.format("parquet").bucketBy(self._num_buckets, "id").mode(
                "overwrite"
            ).saveAsTable(name)
            t3 = time.time()
            logger.info(
                "Time to bucket with num_buckets=%s = %s seconds",
                self._num_buckets,
                t3 - t2,
            )
            shutil.rmtree(data_dir)
            if os.path.exists(data_filename):
                shutil.rmtree(data_filename)
            os.rename(tmp_path, data_filename)

        scale_factor_filename = os.path.join(self._output_dir, "scale_factors.json")
        with open(scale_factor_filename, "w") as f_out:
            json.dump(scale_factors, f_out, indent=2)

    def _convert_geo_id(self, sector_dataset, geo_idx, auto_scale_factor, scale_factors):
        df, geos, scales = sector_dataset.get_data(geo_idx)
        if auto_scale_factor and (len(scales) == 1 or len(set(scales)) == 1):
            keep_scaling_factor = False
            df = df * scales[0]
        else:
            keep_scaling_factor = True

        df.reset_index(inplace=True)
        if self._timestamps is None:
            self._timestamps = pd.to_datetime(df["index"])
        df.insert(0, "timestamp", self._timestamps)
        df.drop("index", inplace=True, axis=1)

        load_id = self._next_id()
        self._append_data_dataframe(df, load_id)
        sector_id = sector_dataset.sector_id

        for geo, scale_factor in zip(geos, scales):
            lookup_data = {
                "geography": geo,
                "subsector": sector_id,
                "id": load_id,
            }
            if keep_scaling_factor:
                lookup_data["scale_factor"] = float(scale_factor)
            else:
                scale_factors[sector_dataset.sector_id][str(geo)] = float(scales[0])
            self._load_data_lookup.append(lookup_data)


def setup_logging(filename, file_level=logging.INFO, console_level=logging.INFO):
    my_logger = logging.getLogger(LOGGER_NAME)
    my_logger.setLevel(logging.INFO)
    fh = logging.FileHandler(filename)
    fh.setLevel(file_level)
    ch = logging.StreamHandler()
    ch.setLevel(console_level)
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    # add the handlers to the logger
    my_logger.addHandler(fh)
    my_logger.addHandler(ch)


@click.command()
@click.argument("dsg_file")
@click.argument("output_dir")
@click.option(
    "--auto-scale-factor/--no-auto-scale-factor",
    default=True,
    show_default=True,
    help="Apply the scale factor to the data and drop the column if dataframes are not shared. "
    "If False, never apply the scale factor.",
)
@click.option(
    "-n",
    "--num-buckets",
    default=0,
    show_default=True,
    help="Enable Spark bucketing with this number of buckets.",
)
@click.option(
    "-o",
    "--data-id-offset",
    default=0,
    show_default=True,
    type=click.IntRange(
        0,
    ),
    help="Offset the dataframe ID by this value",
)
@click.option(
    "--verbose",
    is_flag=True,
    default=False,
    show_default=True,
    help="Enable verbose log output.",
)
def convert_dsg(
    dsg_file,
    output_dir,
    auto_scale_factor=True,
    num_buckets=0,
    data_id_offset=0,
    verbose=False,
):
    """Convert a DSG file to Parquet.

    Run the command through spark-submit as in this example. Set driver-memory
    to the maximum amount that your system can spare.

    \b
    spark-submit --driver-memory=16G \\
        --conf spark.driver.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" \\
        --conf spark.executor.extraJavaOptions="-Dio.netty.tryReflectionSetAccessible=true" \\
        convert_dsg.py data/filename.dsg output_dir

    Alternatively, set these parameters in $SPARK_HOME/conf/spark-defaults.conf

    spark.driver.memory 16g
    spark.driver.extraJavaOptions -Dio.netty.tryReflectionSetAccessible=true
    spark.executor.extraJavaOptions -Dio.netty.tryReflectionSetAccessible=true

    Spark logging is quite verbose. Suppress INFO messages by setting this line in
    $SPARK_HOME/conf/log4j.properties:

    log4j.rootCategory=WARN, console

    """
    os.makedirs(output_dir, exist_ok=True)
    base_dir = os.path.join(output_dir, os.path.basename(dsg_file.replace(".dsg", "")))
    if os.path.exists(base_dir):
        shutil.rmtree(base_dir)
    os.makedirs(base_dir, exist_ok=True)
    log_file = os.path.join(base_dir, "convert_dsg.log")
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging(log_file, file_level=level, console_level=level)
    logger.info("CLI args: %s", " ".join(sys.argv))
    ConvertDsg(base_dir, data_id_offset).convert(dsg_file, auto_scale_factor, num_buckets)


if __name__ == "__main__":
    convert_dsg()
