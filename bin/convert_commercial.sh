#!/bin/bash

# Example usage:
# DSG_INPUTS=./data OUTPUT_DIR=output ~/repos/dsgrid-load/bin/convert_commercial.sh

# If running on Eagle, make sure you change to /tmp/scratch and run from there.

BIN_DIR=`dirname $0`
OUTPUT_DIR=converted_output
SPARK_SETTINGS="--conf spark.driver.extraJavaOptions=\"-Dio.netty.tryReflectionSetAccessible=true\" --conf spark.executor.extraJavaOptions=\"-Dio.netty.tryReflectionSetAccessible=true\""

export SPARK_LOCAL_DIRS=.

if [ -z $DSG_INPUTS ]; then
	echo "The path to the .dsg files must be defined in the env variable DSG_INPUTS"
	exit 1
fi

if [ -z $OUTPUT_DIR ]; then
	echo "The output directory must be defined in the env variable OUTPUT_DIR"
	exit 1
fi

spark-submit ${SPARK_SETTINGS} ${BIN_DIR}/convert_dsg.py --num-buckets=6 ${DSG_INPUTS}/commercial.dsg ${OUTPUT_DIR}

if [ $? -ne 0 ]; then
	echo "Creation of commercial parquet files failed: $?"
	exit 1
fi

# Bucketing is not required here.
# There are 16704 dataframes in commercial.dsg; start at the next one.
spark-submit ${SPARK_SETTINGS} ${BIN_DIR}/convert_dsg.py --data-id-offset=16704 ${DSG_INPUTS}/residential.dsg ${OUTPUT_DIR}

if [ $? -ne 0 ]; then
	echo "Creation of residential parquet files failed: $?"
	exit 1
fi

spark-submit ${SPARK_SETTINGS} ${BIN_DIR}/add_midrise_apartment_to_commercial.py ${OUTPUT_DIR}/commercial ${OUTPUT_DIR}/residential

if [ $? -ne 0 ]; then
	echo "Creation of residential parquet files failed: $?"
	exit 1
fi

echo "Successfully converted commercial.dsg to parquet format."
exit 0
