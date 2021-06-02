#!/bin/bash

APPNAME=$1
STREAMNAME=$2

REGION=`curl --silent http://169.254.169.254/latest/dynamic/instance-identity/document | jq -r .region`
spark-submit --packages org.apache.spark:spark-streaming-kinesis-asl_2.12:3.1.1 --class com.amazonaws.awslabs.sparkstreaming.SparkKinesisExample /home/hadoop/streaming-example/sparkkinesisexample_2.12-0.2.jar $APPNAME $STREAMNAME $REGION

