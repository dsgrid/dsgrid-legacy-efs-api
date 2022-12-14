#!/usr/bin/env python

"""Converts a .dsg file to a directory of parquet files"""

# Users: you need to install pyspark, h5py, numpy, pandas as well as have the
# dsgrid-legacy-efs-apid repo in your Python path.
# Installing pyspark will add spark-submit to your system path.
# The recommended way to run this script is through spark-submit. Refer to the
# instructions at the bottom of the file.


import enum
import json
import logging
import math
import os
import shutil
import sys
import time
from collections import defaultdict
from pathlib import Path

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

    def __init__(self, output_dir, data_id_offset, sector_id_type):
        self._output_dir = Path(output_dir)
        self._load_data_lookup = []
        self._data_id = data_id_offset  # Each unique dataframe gets assigned a unique ID.
        self._data_df = None
        self._data_dfs = []
        self._sector_id_type = sector_id_type
        self._timestamps = None  # There is one array of timestamps per file.
        # Initialize on the first occurrence.
        self._spark = SparkSession.builder.appName("convert_dsg").getOrCreate()
        self._cached_dfs = []

        # Optimize for converting pandas DataFrames to spark.
        self._spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        self._tmp_data_dir = self._output_dir / "data"
        if self._tmp_data_dir.exists():
            shutil.rmtree(self._tmp_data_dir)
        self._tmp_data_dir.mkdir()

    def convert(self, filename, auto_scale_factor=True, num_partitions=None):
        """Convert the dsg file to parquet files.

        Parameters
        ----------
        filename : str
            existing .dsg file
        auto_scale_factor : bool
        num_partitions : int | None
            Number of partitions to create in the final Parquet file.
            If not set, the code will calculate the number by counting the
            number of rows and columns with estimated sizes and compression
            while trying to produce 128 MB partitions.

        """
        logger.info(
            "Converting %s to parquet auto_scale_factor=%s",
            filename,
            auto_scale_factor,
        )
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
            self._convert(filename, h5f, auto_scale_factor, num_partitions)

        duration = time.time() - start
        logger.info("Last data_id=%s", self._data_id)
        logger.info("Created %s duration=%s seconds", self._output_dir, duration)

    def _next_id(self):
        self._data_id += 1
        return self._data_id

    def _append_data_dataframe(self, df, load_id):
        df["id"] = np.int32(load_id)
        self._cached_dfs.append(df)
        # Creating many small dataframes is very slow. This code caches them in memory and
        # concatenates them.
        # Since each dataframe is only 8784 in length, we could cache more. However, if we go
        # much bigger than this value, we exceed the max Spark RPC message size.
        # This seems to be good enough to avoid creating too many small files.
        if len(self._cached_dfs) >= 100:
            self._concatenate_cached_dfs()

    def _concatenate_cached_dfs(self):
        spark_df = self._spark.createDataFrame(pd.concat(self._cached_dfs))
        data_filename = self._tmp_data_dir / f"data_{self._data_id}.parquet"
        assert not data_filename.exists(), data_filename
        spark_df.coalesce(1).write.parquet(str(data_filename))
        logger.info("Created temporary file %s", data_filename)
        self._cached_dfs.clear()

    def _convert(self, filename, h5f, auto_scale_factor, num_partitions=None):
        scale_factors = defaultdict(dict)
        datafile = Datafile.load(filename)
        for _, sector_dataset in SectorDataset.loadall(datafile, h5f):
            for geo_idx in range(sector_dataset.n_geos):
                self._convert_geo_id(sector_dataset, geo_idx, auto_scale_factor, scale_factors)

        if self._cached_dfs:
            self._concatenate_cached_dfs()

        record_filename = os.path.join(self._output_dir, "load_data_lookup.parquet")
        df = self._spark.createDataFrame(Row(**x) for x in self._load_data_lookup)
        df.repartition(1).write.parquet(record_filename, mode="overwrite")

        data_filename = self._output_dir / "load_data.parquet"
        t1 = time.time()
        df = self._spark.read.parquet(*[str(x) for x in self._tmp_data_dir.iterdir()])
        if num_partitions is None:
            num_partitions = get_optimal_number_of_files(df)
            logger.info("Estimated the optimal number of partitions to be %s", num_partitions)
        else:
            logger.info("Apply user-specified num_partitions=%s", num_partitions)
        df.coalesce(num_partitions).write.mode("overwrite").parquet(str(data_filename))
        t2 = time.time()
        logger.info("Time to coalesce and write: %s seconds", t2 - t1)

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

        sector_id = None
        subsector_id = None
        if self._sector_id_type == SectorIdType.SECTOR:
            sector_id = sector_dataset.sector_id
        elif self._sector_id_type == SectorIdType.SECTOR_SUBSECTOR:
            if sector_dataset.sector_id == "com__MidriseApartment":
                sector_id = "res"
                subsector_id = "MidriseApartment"
            else:
                sector_id, subsector_id = sector_dataset.sector_id.split("__")
        elif self._sector_id_type == SectorIdType.SUBSECTOR:
            subsector_id = sector_dataset.sector_id
        else:
            assert False, self._sector_id_type

        for geo, scale_factor in zip(geos, scales):
            lookup_data = {
                "geography": geo,
                "id": load_id,
            }
            if sector_id is not None:
                lookup_data["sector"] = sector_id
            if subsector_id is not None:
                lookup_data["subsector"] = subsector_id

            if keep_scaling_factor:
                lookup_data["scale_factor"] = float(scale_factor)
            else:
                scale_factors[sector_dataset.sector_id][str(geo)] = float(scales[0])
            self._load_data_lookup.append(lookup_data)


class SectorIdType(enum.Enum):
    """Defines how sector_id in the .dsg file should be interpreted."""

    SECTOR = "sector"
    SECTOR_SUBSECTOR = "sector_subsector"
    SUBSECTOR = "subsector"


# The next two functions are copied from the new version of dsgrid
# dsgrid/utils/spark_partition.py


def get_data_size(df, bytes_per_cell=8):
    """approximate dataset size

    Parameters
    ----------
    df : DataFrame
    bytes_per_cell : [float, int]
        Estimated number of bytes per cell in a dataframe.
        * 4-bytes = 32-bit = Single-precision Float = pyspark.sql.types.FloatType,
        * 8-bytes = 64-bit = Double-precision float = pyspark.sql.types.DoubleType,

    Returns
    -------
    n_rows : int
        Number of rows in df
    n_cols : int
        Number of columns in df
    data_MB : float
        Estimated size of df in memory in MB

    """
    n_rows = df.count()
    n_cols = len(df.columns)
    data_MB = n_rows * n_cols * bytes_per_cell / 1e6  # MB
    return n_rows, n_cols, data_MB


def get_optimal_number_of_files(df, MB_per_cmp_file=128, cmp_ratio=0.18):
    """calculate *optimal* number of files
    Parameters
    ----------
    df : DataFrame
    MB_per_cmp_file : float
        Desired size of compressed file on disk in MB
    cmp_ratio : float
        Ratio of file size after and before compression

    Returns
    -------
    n_files : int
        Number of files
    """
    _, _, data_MB = get_data_size(df)
    MB_per_file = MB_per_cmp_file / cmp_ratio
    n_files = math.ceil(data_MB / MB_per_file)

    logger.info(
        f"Dataframe is approximately {data_MB:.02f} MB in size, "
        f"ideal to split into {n_files} file(s) at {MB_per_file:.1f} MB compressed on disk. "
        f"({MB_per_file:.1f} MB uncompressed in memory, {cmp_ratio} compression ratio)."
    )
    return n_files


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


def _apply_sector_id_type(_, __, val):
    return SectorIdType(val)


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
    "--num-partitions",
    type=int,
    help="Create this number of partitions. Default is to calculate based on number of rows "
    "and columns with estimated sizes and compression.",
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
    "-s",
    "--sector-id-type",
    required=True,
    type=click.Choice([x.value for x in SectorIdType]),
    callback=_apply_sector_id_type,
    help="Defines how to interpret the sector_id field in the .dsg file",
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
    num_partitions=None,
    data_id_offset=0,
    sector_id_type=None,
    verbose=False,
):
    """Convert a DSG file to Parquet.

    Run the command through spark-submit in local mode as in this example. Set driver-memory
    to the maximum amount that your system can spare. Running through a cluster may be helpful
    for large datasets, but likely isn't necessary.

    \b
    spark-submit --driver-memory=16G convert_dsg.py -s sector data/filename.dsg output_dir

    Alternatively, set the parameter in $SPARK_HOME/conf/spark-defaults.conf

    spark.driver.memory 16g
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
    ConvertDsg(base_dir, data_id_offset, sector_id_type).convert(
        dsg_file, auto_scale_factor, num_partitions
    )


if __name__ == "__main__":
    convert_dsg()
