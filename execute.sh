#!/bin/bash

./clean.sh
sbt package
#$SPARK_HOME/bin/spark-submit --class "ExplanationTableApp" \
#    --master yarn-cluster \
#    --executor-cores 4 target/scala-2.10/explanation-table_2.10-1.0.jar 2>&1

$SPARK_HOME/bin/spark-submit --class "ExplanationTableApp" \
    --master yarn-cluster \
    target/scala-2.10/explanation-table_2.10-1.0.jar 2>&1

#$SPARK_HOME/bin/spark-submit --class org.apache.spark.examples.SparkPi \
#    --master yarn-cluster \
#    --num-executors 1 \
#    --driver-memory 4g \
#    --executor-memory 2g \
#    --executor-cores 4 \
#    $SPARK_HOME/examples/target/scala-2.10/spark-examples-1.1.0-SNAPSHOT-hadoop2.2.0.jar \
#    10