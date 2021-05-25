"""Converts a .dsg file to a directory of parquet files"""

import json
import logging
import multiprocessing
import os
import shutil
import sys
import time
from functools import reduce

from pyspark.sql import Row, SparkSession
from pyspark.sql.dataframe import DataFrame
import pyspark.sql.functions as F


LOGGER_NAME = "DSG"

logger = logging.getLogger(LOGGER_NAME)


def filter_midrise_apartment(src_dir):
    """Filters midrise apartment subsector data from the parquet files.

    Returns
    -------
    tuple
        spark DataFrames for load_data and load_data_lookup
    """
    spark = SparkSession.getActiveSession()
    load_data = spark.read.parquet(os.path.join(src_dir, "load_data.parquet"))
    load_data.createOrReplaceTempView("load_data")
    lookup = spark.read.parquet(os.path.join(src_dir, "load_data_lookup.parquet"))
    lookup.createOrReplaceTempView("lookup")

    df = spark.sql(
        """
        select timestamp,fans,pumps,heating,cooling,interior_lights,
        exterior_lights,water_systems,interior_equipment,heat_rejection,load_data.id as id,lookup.subsector as subsector
        from load_data
        join lookup
        on lookup.data_id = load_data.id"""
    ).filter("subsector='com__MidriseApartment'")

    filename = os.path.join(src_dir, f"load_data_only_midrise_apartment.parquet")
    load_data_reduced_df = df.drop("subsector")
    load_data_reduced_df.write.parquet(filename)

    lookup_reduced_df = lookup.filter("subsector='com__MidriseApartment'")
    filename = os.path.join(src_dir, f"load_data_lookup_only_midrise_apartment.parquet")
    lookup_reduced_df.write.parquet(filename)
    return load_data_reduced_df, lookup_reduced_df


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
    my_logger.addHandler(fh)
    my_logger.addHandler(ch)


def main(commercial_dir, residential_dir):
    """Removes com__MidriseApartment subsector data from residential and inserts it
    in the commercial data.

    """
    spark = SparkSession.builder.master("local").appName("DSG").getOrCreate()

    log_file = os.path.join(residential_dir, "dsg.log")
    setup_logging(log_file)
    logger.info("CLI args: %s", " ".join(sys.argv))
    load_data_reduced_df, lookup_reduced_df = filter_midrise_apartment(residential_dir)

    main_lookup = spark.read.parquet(os.path.join(commercial_dir, "load_data_lookup.parquet"))
    final_lookup = main_lookup.union(lookup_reduced_df)
    lookup_filename = os.path.join(commercial_dir, "load_data_lookup_with_midrise.parquet")
    final_lookup.write.parquet(lookup_filename)
    logger.info("Added MidriseApartment data to %s", lookup_filename)

    main_load_data = spark.read.parquet(os.path.join(commercial_dir, "load_data.parquet"))
    start = time.time()
    final_load_data = main_load_data.union(load_data_reduced_df)
    load_data_filename = os.path.join(commercial_dir, "load_data_with_midrise.parquet")
    final_load_data.write.parquet(load_data_filename)
    dur = time.time() - start
    logger.info(
        "Added MidriseApartment data to %s. Duration=%s seconds",
        load_data_filename,
        dur,
    )


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} COMMERCIAL_DIR RESIDENTIAL_DIR")
        sys.exit(1)

    main(sys.argv[1], sys.argv[2])
