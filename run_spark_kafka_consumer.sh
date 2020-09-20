#!/bin/bash

spark2-submit  \
--master yarn \
--deploy-mode client \
--conf spark.ui.port=12901 \
--class SparkKafkaConsumer \
--packages org.apache.spark:spark-streaming-kafka-0-10_2.11:2.3.0,\
org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 \
/home/sparkdatabox/git/SparkKafkaConsumer/target/scala-2.11/sparkkafkaconsumer_2.11-0.1.jar
