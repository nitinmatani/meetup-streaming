#!/bin/bash

if [ "$1" == "" ]; then
    echo "need to provide the parameters:"
    echo "  1. Home folder of your local Spark 2.3 runtime, downloaded end extracted "
    echo "example:"
    echo "      ./$( basename $0 ) ~/Downloads/spark-2.3.2-bin-hadoop2.7/"
    exit 1
fi

SPARK_HOME=$1
export SPARK_HOME=$SPARK_HOME

echo "Running program for 'Meetup Experiment' ..."

$SPARK_HOME/bin/spark-submit \
    --master=local[*] \
    --deploy-mode client \
    --driver-memory 2G \
    --executor-memory 8G \
    --conf "spark.driver.memoryOverhead=2G spark.executor.memoryOverhead=2G spark.serializer=org.apache.spark.serializer.KryoSerializer spark.kryoserializer.buffer.max=200M" \
    --class com.reuthlinger.meetup.MeetupExperiment \
    meetup-experiment-1.0.0-jar-with-dependencies.jar

echo "Program done"

# clean up a little
rm -rf ./spark-warehouse/

exit 0