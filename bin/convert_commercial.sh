#!/bin/bash

# Example usage:
# DSG_INPUTS=./data OUTPUT_DIR=output ~/repos/dsgrid-load/bin/convert_commercial.sh

# If running on Eagle:
# - Make sure you change to /tmp/scratch and run from there.
# - If $SPARK_HOME is not set then set it to the pyspark directory and create $SPARK_HOME/conf.
# - Example:
#       export SPARK_HOME=$HOME/.conda/envs/dsgrid/lib/python3.8/site-packages/pyspark
# - Set this parameter in $SPARK_HOME/conf/spark-defaults.conf
#       spark.driver.memory 80
# - Set this parameter in $SPARK_HOME/conf/log4j.properties:
#       log4j.rootCategory=WARN, console

BIN_DIR=`dirname $0`
OUTPUT_DIR=converted_output
SPARK_SETTINGS="--conf spark.driver.extraJavaOptions=\"-Dio.netty.tryReflectionSetAccessible=true\" --conf spark.executor.extraJavaOptions=\"-Dio.netty.tryReflectionSetAccessible=true\""
export SPARK_LOCAL_DIRS=`pwd`

if [ -z $DSG_INPUTS ]; then
	echo "The path to the .dsg files must be defined in the env variable DSG_INPUTS"
	exit 1
fi

if [ -z $OUTPUT_DIR ]; then
	echo "The output directory must be defined in the env variable OUTPUT_DIR"
	exit 1
fi

spark-submit ${SPARK_SETTINGS} ${BIN_DIR}/convert_dsg.py -s sector_subsector --num-buckets=6 ${DSG_INPUTS}/commercial.dsg ${OUTPUT_DIR}

if [ $? -ne 0 ]; then
	echo "Creation of commercial parquet files failed: $?"
	exit 1
fi

# Bucketing is not required here.
# There are 16704 dataframes in commercial.dsg; start at the next one.
grep "Last data_id=16704" ${OUTPUT_DIR}/commercial/convert_dsg.log >> /dev/null

if [ $? -ne 0 ]; then
	echo "Did not find Last data_id=16704 in commercial log file."
	exit 1
fi

spark-submit ${SPARK_SETTINGS} ${BIN_DIR}/convert_dsg.py -s sector_subsector --data-id-offset=16704 ${DSG_INPUTS}/residential.dsg ${OUTPUT_DIR}

if [ $? -ne 0 ]; then
	echo "Creation of residential parquet files failed: $?"
	exit 1
fi

spark-submit ${SPARK_SETTINGS} ${BIN_DIR}/add_midrise_apartment_to_commercial.py ${OUTPUT_DIR}/commercial ${OUTPUT_DIR}/residential

if [ $? -ne 0 ]; then
	echo "Creation of residential parquet files failed: $?"
	exit 1
fi

rm -rf ${OUTPUT_DIR}/commercial/load_data_lookup.parquet
if [ $? -ne 0 ]; then
	echo "rm of load_data_lookup failed: $?"
	exit 1
fi

mv ${OUTPUT_DIR}/commercial/load_data_lookup_with_midrise.parquet ${OUTPUT_DIR}/commercial/load_data_lookup.parquet
if [ $? -ne 0 ]; then
	echo "mv of load_data_lookup failed: $?"
	exit 1
fi

rm -rf ${OUTPUT_DIR}/commercial/load_data.parquet
if [ $? -ne 0 ]; then
	echo "rm of load_data failed: $?"
	exit 1
fi

mv ${OUTPUT_DIR}/commercial/load_data_with_midrise.parquet ${OUTPUT_DIR}/commercial/load_data.parquet
if [ $? -ne 0 ]; then
	echo "mv of load_data failed: $?"
	exit 1
fi

echo "Successfully converted commercial.dsg to parquet format."
exit 0
