#!/bin/bash

./clean.sh
sbt package
$SPARK_HOME/bin/spark-submit --class "Main" --master yarn-cluster target/scala-2.10/explanation-table_2.10-1.0.jar 2>&1